package ufes.soe.bgp;

import java.math.BigDecimal;
import java.time.Duration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Topologia pura de D1 e D2: entra JSON como String, sai JSON como String.
 *
 * <p>Uma aplicacao Streams com duas fontes. D1 le {@code bgp.alertas} e so olha S1 — a linha de
 * base prefixo -> AS legitimo e conhecimento do detector, e recalcula-la aqui poria regra de
 * dominio em duas linguagens. D2 le {@code bgp.updates}, que e onde anuncio e retirada aparecem.
 *
 * <p>Chave e valor sao String nas duas pontas e o JSON e lido com Jackson dentro dos lambdas: um
 * Serde de POJO seria abstracao sem ganho para tres campos lidos.
 */
public final class Topologia {

    public static final String TOPICO_UPDATES = "bgp.updates";
    public static final String TOPICO_ALERTAS = "bgp.alertas";
    public static final String TOPICO_DERIVADOS = "bgp.derivados";

    /**
     * ponytail: janela fixa (tumbling), nao deslizante. Teto conhecido: um incidente que cai em
     * cima da borda da janela fica partido em duas metades e pode nao juntar os 3 coletores de
     * D1 nem as 4 alternancias de D2. Upgrade: trocar por SlidingWindows, que mantem o resto da
     * topologia igual e so multiplica o custo de estado.
     */
    static final Duration JANELA = Duration.ofMinutes(5);

    /** D1 so confirma quando 3 coletores independentes concordam. */
    static final int MIN_COLETORES = 3;

    /** D2 so acende a partir da 4a alternancia anuncio/retirada na janela. */
    static final int MIN_ALTERNANCIAS = 4;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TimestampExtractor TEMPO_DO_EVENTO = new TempoDoEvento();

    private Topologia() {
    }

    public static Topology construir() {
        StreamsBuilder construtor = new StreamsBuilder();
        sequestroConfirmado(construtor);
        rotaInstavel(construtor);
        return construtor.build();
    }

    // ------------------------------------------------------------ D1

    private static void sequestroConfirmado(StreamsBuilder construtor) {
        KStream<String, String> s1 = construtor
                .stream(TOPICO_ALERTAS, Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(TEMPO_DO_EVENTO))
                .filter((chave, alerta) -> "S1".equals(ler(alerta).path("situacao").asText()))
                .selectKey((chave, alerta) -> {
                    JsonNode a = ler(alerta);
                    return a.path("prefixo").asText() + "|" + a.path("origem_as").asText();
                });

        s1.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(JANELA))
                .aggregate(Topologia::coletoresVazio, Topologia::acumularColetor,
                        Materialized.with(Serdes.String(), Serdes.String()))
                .toStream()
                .filter((janela, acumulado) -> ler(acumulado).path("emitir").asBoolean())
                .map(Topologia::montarSequestro)
                .to(TOPICO_DERIVADOS, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static String coletoresVazio() {
        ObjectNode vazio = MAPPER.createObjectNode();
        vazio.putArray("coletores");
        vazio.put("emitido", false);
        vazio.put("emitir", false);
        return vazio.toString();
    }

    private static String acumularColetor(String chave, String alerta, String acumulado) {
        JsonNode a = ler(alerta);
        ObjectNode estado = (ObjectNode) ler(acumulado);
        ArrayNode coletores = (ArrayNode) estado.get("coletores");

        String coletor = a.path("coletor").asText();
        boolean inedito = true;
        for (JsonNode visto : coletores) {
            inedito &= !visto.asText().equals(coletor);
        }
        if (inedito) {
            coletores.add(coletor);
        }

        estado.put("prefixo", a.path("prefixo").asText());
        estado.put("as_suspeito", a.path("origem_as").asLong());
        estado.put("as_legitimo", a.path("as_esperado").asLong());

        // "emitir" e transitorio (so o registro que cruza o limiar o carrega); "emitido" e pegajoso,
        // e e o que garante um evento por janela sem precisar esperar a janela fechar.
        boolean emitir = !estado.path("emitido").asBoolean() && coletores.size() >= MIN_COLETORES;
        estado.put("emitir", emitir);
        if (emitir) {
            estado.put("emitido", true);
        }
        return estado.toString();
    }

    private static KeyValue<String, String> montarSequestro(Windowed<String> janela, String acumulado) {
        JsonNode estado = ler(acumulado);
        ObjectNode d = MAPPER.createObjectNode();
        d.put("tipo", "sequestro_confirmado");
        d.put("prefixo", estado.path("prefixo").asText());
        d.put("as_legitimo", estado.path("as_legitimo").asLong());
        d.put("as_suspeito", estado.path("as_suspeito").asLong());
        d.set("coletores", estado.get("coletores"));
        d.put("confianca", confianca(estado.get("coletores").size()));
        datarJanela(d, janela);
        // A chave da saida e o prefixo, nunca a chave composta da janela: e o que mantem todo
        // evento do mesmo bloco na mesma particao.
        return KeyValue.pair(estado.path("prefixo").asText(), d.toString());
    }

    /** Confianca = 1 - 1/n_coletores: 0.67 com tres, 0.83 com seis. Duas casas. */
    static double confianca(int coletores) {
        return Math.round((1.0 - 1.0 / coletores) * 100) / 100.0;
    }

    // ------------------------------------------------------------ D2

    private static void rotaInstavel(StreamsBuilder construtor) {
        construtor
                .stream(TOPICO_UPDATES, Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(TEMPO_DO_EVENTO))
                // Agrupar por peer separa flapping de verdade de dois coletores enxergando
                // estados diferentes do mesmo prefixo.
                .selectKey((chave, evento) -> {
                    JsonNode e = ler(evento);
                    return e.path("prefixo").asText() + "|" + e.path("peer_as").asText();
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(JANELA))
                .aggregate(Topologia::alternanciasVazio, Topologia::acumularAlternancia,
                        Materialized.with(Serdes.String(), Serdes.String()))
                .toStream()
                .filter((janela, acumulado) -> ler(acumulado).path("emitir").asBoolean())
                .map(Topologia::montarRotaInstavel)
                .to(TOPICO_DERIVADOS, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static String alternanciasVazio() {
        ObjectNode vazio = MAPPER.createObjectNode();
        vazio.putNull("ultimo_tipo");
        vazio.put("alternancias", 0);
        vazio.put("emitido", false);
        vazio.put("emitir", false);
        return vazio.toString();
    }

    private static String acumularAlternancia(String chave, String evento, String acumulado) {
        JsonNode e = ler(evento);
        ObjectNode estado = (ObjectNode) ler(acumulado);

        String tipo = e.path("tipo").asText();
        String ultimo = estado.path("ultimo_tipo").isNull() ? null : estado.path("ultimo_tipo").asText();
        int alternancias = estado.path("alternancias").asInt();
        if (ultimo != null && !ultimo.equals(tipo)) {
            alternancias++;
        }
        estado.put("ultimo_tipo", tipo);
        estado.put("alternancias", alternancias);
        estado.put("prefixo", e.path("prefixo").asText());
        estado.put("peer_as", e.path("peer_as").asLong());

        boolean emitir = !estado.path("emitido").asBoolean() && alternancias >= MIN_ALTERNANCIAS;
        estado.put("emitir", emitir);
        if (emitir) {
            estado.put("emitido", true);
        }
        return estado.toString();
    }

    private static KeyValue<String, String> montarRotaInstavel(Windowed<String> janela, String acumulado) {
        JsonNode estado = ler(acumulado);
        ObjectNode d = MAPPER.createObjectNode();
        d.put("tipo", "rota_instavel");
        d.put("prefixo", estado.path("prefixo").asText());
        d.put("peer_as", estado.path("peer_as").asLong());
        d.put("alternancias", estado.path("alternancias").asInt());
        datarJanela(d, janela);
        return KeyValue.pair(estado.path("prefixo").asText(), d.toString());
    }

    // --------------------------------------------------------- apoio

    private static void datarJanela(ObjectNode d, Windowed<String> janela) {
        // BigDecimal com escala 3 em vez de double: sai "1203878700.000" no topico, e nao a
        // notacao cientifica que Double.toString produz para epoch em segundos.
        d.put("janela_inicio", BigDecimal.valueOf(janela.window().startTime().toEpochMilli(), 3));
        d.put("janela_fim", BigDecimal.valueOf(janela.window().endTime().toEpochMilli(), 3));
    }

    static JsonNode ler(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalArgumentException("JSON invalido no topico: " + json, e);
        }
    }

    /**
     * O tempo da janela vem do campo {@code timestamp} do evento (segundos com fracao), nunca do
     * relogio de parede. Sem isso o replay de 2008 cairia todo numa janela de "agora".
     */
    static final class TempoDoEvento implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> registro, long tempoAnterior) {
            Object valor = registro.value();
            if (valor instanceof String json) {
                JsonNode t = ler(json).path("timestamp");
                if (t.isNumber()) {
                    return (long) (t.asDouble() * 1000);
                }
            }
            // Evento sem timestamp utilizavel nao pode parar o fluxo nem viajar no tempo:
            // herda o tempo do ultimo registro valido da particao.
            return tempoAnterior >= 0 ? tempoAnterior : registro.timestamp();
        }
    }
}
