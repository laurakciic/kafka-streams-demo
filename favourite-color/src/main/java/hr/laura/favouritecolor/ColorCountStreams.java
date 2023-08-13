package hr.laura.favouritecolor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

public class ColorCountStreams {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(APPLICATION_ID_CONFIG, "fav-colors");
        config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        config.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // disable cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        // Step 1: create topic of user keys to colours
        // message example: laura,blue
        KStream<String, String> favColors = builder.stream("favorite-colors");

        KStream<String, String> favColorsFiltered = favColors
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((user, color) -> Arrays.asList("red", "green", "blue").contains(color));

        favColorsFiltered.to("user-keys-and-colors");

        // step 2 - read that topic as a KTable so that updates are read correctly
        KTable<String, String> usersAndColoursTable = builder.table("user-keys-and-colors");

        // step 3 - count the occurrences of colours
        KTable<String, Long> favColorsCount = usersAndColoursTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count(Materialized.as("color-count"));

        // output results to a Kafka Topic
        favColorsCount.toStream().to("favorite-colors-count", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        // print topology
        streams.localThreadsMetadata().forEach(System.out::println);

        // shutdown hook to correctly close streams app
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
