package com.purbon.kafka.search;

import com.purbon.kafka.search.models.Customer;
import com.purbon.kafka.search.serdes.JsonPOJODeserializer;
import com.purbon.kafka.search.serdes.JsonPOJOSerializer;
import java.util.HashMap;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class CalculateCustomersPerGenre {

  public static Properties config() {
    return  config(null);
  }

  public static Properties config(Serde<Customer> serde) {

    final Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "Search-Ingestion-Pipeline");
    config.put(StreamsConfig.CLIENT_ID_CONFIG, "normalization-client");

    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    //config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
    config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return config;

  }

  public static void main(String[] args) {

    HashMap<String, Object> serdeProps = new HashMap<>();

    StreamsBuilder builder = new StreamsBuilder();

    final Serializer<Customer> customerSerializer = new JsonPOJOSerializer<>();
    serdeProps.put("JsonPOJOClass", Customer.class);
    customerSerializer.configure(serdeProps, false);

    final Deserializer<Customer> customerDeserializer = new JsonPOJODeserializer<>();
    serdeProps.put("JsonPOJOClass", Customer.class);
    customerDeserializer.configure(serdeProps, false);

    final Serde<Customer> customerSerde = Serdes.serdeFrom(customerSerializer, customerDeserializer);

    KStream<String, Customer> customersKStream = builder
        .stream("asgard.demo.CUSTOMERS",
            Consumed.with(Serdes.String(), customerSerde));

    customersKStream
        .groupBy((aString, customer) -> customer.gender, Grouped.with(Serdes.String(), customerSerde))
        .count()
        .toStream()
        .to("consumers-per-gender", Produced.with(Serdes.String(), Serdes.Long()));

    final KafkaStreams streams = new KafkaStreams(builder.build(), config());
    streams.cleanUp();
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

  }

}
