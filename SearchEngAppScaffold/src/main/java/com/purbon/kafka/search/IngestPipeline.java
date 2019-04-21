package com.purbon.kafka.search;

import com.purbon.kafka.search.models.Customer;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public abstract class IngestPipeline {

  public static final String CUSTOMERS_TOPIC = "asgard.demo.CUSTOMERS";
  public static final String CUSTOMERS_PROFILES_TOPIC = "customers-profiles";
  public static final String CONSUMERS_PER_GENDER_TOPIC = "consumers-per-gender";
  public static final String INVOICES_TOPIC = "asgard.demo.invoices";

  public static Properties config(String clientId) {
    return  config(null, clientId);
  }

  public static Properties config(Serde<Customer> serde, String clientId) {

    final Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "search-pipeline");
    config.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);

    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    //config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
    config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
    //config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return config;

  }

  public void run(Topology topology, String clientId) {

    final KafkaStreams streams = new KafkaStreams(topology, config(clientId));
    streams.cleanUp();
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
