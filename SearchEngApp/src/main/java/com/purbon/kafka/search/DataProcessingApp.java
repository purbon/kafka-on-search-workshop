package com.purbon.kafka.search;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

public class DataProcessingApp {

  public static Properties config() {

    final Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "enrichment-test-v4");
    config.put(StreamsConfig.CLIENT_ID_CONFIG, "enrichment-test-client");

    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
    config
        .put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    //config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
    config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return config;

  }

  public static void main(String[] args) {

    StreamsBuilder builder = new StreamsBuilder();

    builder.stream("incoming-messages-from-cars")
        .map( (key, value) -> enrichtmentFunction(key, value))
        .to("enriched-messages-with-insurance");

    final KafkaStreams streams = new KafkaStreams(builder.build(), config());
    streams.cleanUp();
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

  }

}
