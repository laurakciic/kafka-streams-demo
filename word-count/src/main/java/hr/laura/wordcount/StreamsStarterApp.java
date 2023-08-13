package hr.laura.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;

public class StreamsStarterApp {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(APPLICATION_ID_CONFIG, "word-count");
        config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsStarterApp app = new StreamsStarterApp();

        KafkaStreams streams = new KafkaStreams(app.createTopology(), config);
        streams.start();
        System.out.println(streams);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // 1 - stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        KTable<String, Long> wordCounts = wordCountInput
                .mapValues(value -> value.toLowerCase()) // or .mapValues(String::toLowerCase)
                .flatMapValues(value -> Arrays.asList(value.split(" "))) // flatmap values split by space
                .selectKey((key, value) -> value) // select key to apply a key (we discard the old key)
                .groupByKey() // group by key before aggregation
                .count(Materialized.as("Counts")); // count occurences

        // 7 - to in order to write the results back to kafka
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}
