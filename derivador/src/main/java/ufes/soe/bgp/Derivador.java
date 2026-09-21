package ufes.soe.bgp;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Casca do derivador: so configuracao e ciclo de vida. Toda a decisao mora em {@link Topologia},
 * que roda sem broker no TopologyTestDriver.
 *
 * <p>Broker em {@code KAFKA_BOOTSTRAP}, ou {@code localhost:9092}.
 */
public final class Derivador {

    public static void main(String[] args) {
        String broker = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bgp-derivador");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Sem cache: o registro que cruza o limiar tem que sair na hora. O cache so encaminha o
        // ultimo valor de cada chave a cada flush, e engoliria justamente esse registro.
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        KafkaStreams streams = new KafkaStreams(Topologia.construir(), config);
        CountDownLatch parada = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            parada.countDown();
        }));

        System.err.println("derivador: " + broker + " | " + Topologia.TOPICO_ALERTAS + " (D1) + "
                + Topologia.TOPICO_UPDATES + " (D2) -> " + Topologia.TOPICO_DERIVADOS);
        streams.start();
        try {
            parada.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Derivador() {
    }
}
