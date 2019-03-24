package com.purbon.kafka.datagen;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SimpleKafkaProducer {

  private final String kafkaServers;
  private ObjectMapper mapper = new ObjectMapper();
  private KafkaProducer<Long, String> producer = new KafkaProducer<>(configure());


  public SimpleKafkaProducer(String kafkaServers) {
    this.kafkaServers = kafkaServers;
  }

  private Properties configure() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");
    //props.put("max.block.ms", 2000);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    return props;
  }

  public void send(String topic, long key, String value) {
    send(topic, key, value, false);
  }

  public Future<RecordMetadata> send(String topic, long key, String value, boolean flush) {

    ProducerRecord<Long, String> record = new ProducerRecord<>(topic, key, value);
    Future<RecordMetadata> metadataFuture = producer.send(record);
    if (flush)
      producer.flush();

    return metadataFuture;
  }

  public void close() {
    producer.close();
  }
}
