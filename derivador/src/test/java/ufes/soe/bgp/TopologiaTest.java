package ufes.soe.bgp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * D1 e D2 rodando inteiros em memoria, sem broker e sem Docker.
 *
 * <p>Os timestamps sao explicitos em todo teste: a janela e do tempo do evento, nunca do relogio.
 */
class TopologiaTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Path DADOS = Path.of("..", "testes", "dados");

    /** Inicio de uma janela de 5 min do caso de 2008, para os testes de janela unica. */
    private static final double T0 = 1203878700.0;

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> alertas;
    private TestInputTopic<String, String> updates;
    private TestOutputTopic<String, String> derivados;

    @BeforeEach
    void montar() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "teste-derivador");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        driver = new TopologyTestDriver(Topologia.construir(), config);
        alertas = driver.createInputTopic(
                Topologia.TOPICO_ALERTAS, new StringSerializer(), new StringSerializer());
        updates = driver.createInputTopic(
                Topologia.TOPICO_UPDATES, new StringSerializer(), new StringSerializer());
        derivados = driver.createOutputTopic(
                Topologia.TOPICO_DERIVADOS, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void desmontar() {
        if (driver != null) {
            driver.close();
        }
    }

    // ------------------------------------------------------------------ D1

    @Test
    void tres_coletores_distintos_na_janela_confirmam_o_sequestro() {
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc00", T0 + 1));
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc01", T0 + 2));
        assertTrue(derivados.isEmpty(), "dois coletores ainda nao confirmam");

        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc03", T0 + 3));

        List<KeyValue<String, String>> saida = derivados.readKeyValuesToList();
        assertEquals(1, saida.size());
        JsonNode d = ler(saida.get(0).value);
        assertEquals("sequestro_confirmado", d.get("tipo").asText());
        assertEquals("208.65.153.0/24", d.get("prefixo").asText());
        assertEquals(36561, d.get("as_legitimo").asInt());
        assertEquals(17557, d.get("as_suspeito").asInt());
        assertEquals(3, d.get("coletores").size());
        assertEquals(T0, d.get("janela_inicio").asDouble());
        assertEquals(T0 + 300, d.get("janela_fim").asDouble());
    }

    @Test
    void o_mesmo_coletor_repetido_nao_confirma_o_sequestro() {
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc00", T0 + 1));
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc00", T0 + 2));
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc00", T0 + 3));

        assertTrue(derivados.isEmpty());
    }

    @Test
    void coletores_espalhados_alem_da_janela_nao_confirmam_o_sequestro() {
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc00", T0 + 1));
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc01", T0 + 400));
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc03", T0 + 800));

        assertTrue(derivados.isEmpty());
    }

    @Test
    void seis_coletores_na_mesma_janela_emitem_um_unico_evento() {
        String[] coletores = {"rrc00", "rrc01", "rrc03", "rrc12", "route-views2", "route-views.linx"};
        for (int i = 0; i < coletores.length; i++) {
            alertas.pipeInput("208.65.153.0/24", alertaS1(coletores[i], T0 + i));
        }

        assertEquals(1, derivados.readKeyValuesToList().size());
    }

    @Test
    void a_chave_do_sequestro_confirmado_e_o_prefixo_e_nao_a_chave_composta() {
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc00", T0 + 1));
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc01", T0 + 2));
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc03", T0 + 3));

        assertEquals("208.65.153.0/24", derivados.readKeyValue().key);
    }

    @Test
    void a_confianca_e_um_menos_um_sobre_o_numero_de_coletores() {
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc00", T0 + 1));
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc01", T0 + 2));
        alertas.pipeInput("208.65.153.0/24", alertaS1("rrc03", T0 + 3));

        assertEquals(0.67, ler(derivados.readValue()).get("confianca").asDouble());
    }

    @Test
    void alertas_que_nao_sao_s1_nao_alimentam_o_sequestro_confirmado() {
        alertas.pipeInput("208.65.153.0/24", alerta("S2", "rrc00", T0 + 1));
        alertas.pipeInput("208.65.153.0/24", alerta("S2", "rrc01", T0 + 2));
        alertas.pipeInput("208.65.153.0/24", alerta("S2", "rrc03", T0 + 3));

        assertTrue(derivados.isEmpty());
    }

    @Test
    void a_fixture_de_2008_confirma_o_sequestro_do_youtube_por_tres_coletores() throws IOException {
        empurrar(alertas, DADOS.resolve("alertas_2008.jsonl"));

        List<JsonNode> sequestros = derivadosDoTipo("sequestro_confirmado");
        assertTrue(sequestros.size() >= 1, "nenhum sequestro_confirmado saiu da fixture de 2008");
        JsonNode d = sequestros.get(0);
        assertEquals("208.65.153.0/24", d.get("prefixo").asText());
        assertEquals(17557, d.get("as_suspeito").asInt());
        assertEquals(36561, d.get("as_legitimo").asInt());
        assertTrue(d.get("coletores").size() >= 3, "coletores: " + d.get("coletores"));
    }

    // ------------------------------------------------------------------ D2

    @Test
    void alternancia_acima_do_limiar_acende_rota_instavel_com_a_contagem() {
        String p = "102.221.15.0/24";
        updates.pipeInput(p, anuncio(p, 199524, T0 + 1));
        updates.pipeInput(p, retirada(p, 199524, T0 + 2));
        updates.pipeInput(p, anuncio(p, 199524, T0 + 3));
        updates.pipeInput(p, retirada(p, 199524, T0 + 4));
        assertTrue(derivados.isEmpty(), "tres alternancias ainda estao abaixo do limiar");

        updates.pipeInput(p, anuncio(p, 199524, T0 + 5));

        List<KeyValue<String, String>> saida = derivados.readKeyValuesToList();
        assertEquals(1, saida.size());
        JsonNode d = ler(saida.get(0).value);
        assertEquals("rota_instavel", d.get("tipo").asText());
        assertEquals(p, d.get("prefixo").asText());
        assertEquals(199524, d.get("peer_as").asInt());
        assertEquals(4, d.get("alternancias").asInt());
        assertEquals(T0, d.get("janela_inicio").asDouble());
        assertEquals(T0 + 300, d.get("janela_fim").asDouble());
    }

    @Test
    void anuncios_repetidos_sem_retirada_nao_acendem_rota_instavel() {
        String p = "102.221.15.0/24";
        for (int i = 0; i < 10; i++) {
            updates.pipeInput(p, anuncio(p, 199524, T0 + i));
        }

        assertTrue(derivados.isEmpty());
    }

    @Test
    void peers_diferentes_no_mesmo_prefixo_nao_somam_alternancias() {
        String p = "102.221.15.0/24";
        updates.pipeInput(p, anuncio(p, 199524, T0 + 1));
        updates.pipeInput(p, retirada(p, 209823, T0 + 2));
        updates.pipeInput(p, anuncio(p, 199524, T0 + 3));
        updates.pipeInput(p, retirada(p, 209823, T0 + 4));
        updates.pipeInput(p, anuncio(p, 199524, T0 + 5));
        updates.pipeInput(p, retirada(p, 209823, T0 + 6));
        updates.pipeInput(p, anuncio(p, 199524, T0 + 7));
        updates.pipeInput(p, retirada(p, 209823, T0 + 8));

        assertTrue(derivados.isEmpty(), "dois coletores vendo coisas diferentes nao e flapping");
    }

    @Test
    void a_chave_da_rota_instavel_e_o_prefixo_e_nao_a_chave_composta() {
        String p = "102.221.15.0/24";
        updates.pipeInput(p, anuncio(p, 199524, T0 + 1));
        updates.pipeInput(p, retirada(p, 199524, T0 + 2));
        updates.pipeInput(p, anuncio(p, 199524, T0 + 3));
        updates.pipeInput(p, retirada(p, 199524, T0 + 4));
        updates.pipeInput(p, anuncio(p, 199524, T0 + 5));

        assertEquals(p, derivados.readKeyValue().key);
    }

    @Test
    void a_fixture_ao_vivo_acende_duas_rotas_instaveis() throws IOException {
        empurrar(updates, DADOS.resolve("updates_live.jsonl"));

        List<JsonNode> instaveis = derivadosDoTipo("rota_instavel");
        assertEquals(2, instaveis.size());
        assertTrue(instaveis.stream().anyMatch(d -> "102.221.15.0/24".equals(d.get("prefixo").asText())),
                "esperado 102.221.15.0/24 entre as rotas instaveis");
        assertTrue(instaveis.stream().allMatch(d -> d.get("alternancias").asInt() >= 4));
    }

    // -------------------------------------------------------------- apoio

    private void empurrar(TestInputTopic<String, String> topico, Path arquivo) throws IOException {
        try (Stream<String> linhas = Files.lines(arquivo)) {
            linhas.filter(l -> !l.isBlank())
                    .forEach(l -> topico.pipeInput(ler(l).get("prefixo").asText(), l));
        }
    }

    private List<JsonNode> derivadosDoTipo(String tipo) {
        List<JsonNode> achados = new ArrayList<>();
        for (KeyValue<String, String> kv : derivados.readKeyValuesToList()) {
            JsonNode d = ler(kv.value);
            if (tipo.equals(d.get("tipo").asText())) {
                achados.add(d);
            }
        }
        return achados;
    }

    private static String alertaS1(String coletor, double timestamp) {
        return alerta("S1", coletor, timestamp);
    }

    private static String alerta(String situacao, String coletor, double timestamp) {
        return """
                {"situacao":"%s","severidade":"media","prefixo":"208.65.153.0/24",\
                "origem_as":17557,"as_esperado":36561,"prefixo_base":"208.65.152.0/22",\
                "as_path":[6762,3491,17557],"coletor":"%s","peer_as":6762,"timestamp":%s,\
                "motivo":"origem 17557 difere da esperada 36561"}"""
                .formatted(situacao, coletor, timestamp);
    }

    private static String anuncio(String prefixo, int peerAs, double timestamp) {
        return """
                {"tipo":"anuncio","prefixo":"%s","origem_as":267106,"as_path":[%d,267106],\
                "coletor":"rrc15","peer_as":%d,"timestamp":%s}"""
                .formatted(prefixo, peerAs, peerAs, timestamp);
    }

    private static String retirada(String prefixo, int peerAs, double timestamp) {
        return """
                {"tipo":"retirada","prefixo":"%s","coletor":"rrc15","peer_as":%d,"timestamp":%s}"""
                .formatted(prefixo, peerAs, timestamp);
    }

    private static JsonNode ler(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (IOException e) {
            throw new IllegalArgumentException(json, e);
        }
    }
}
