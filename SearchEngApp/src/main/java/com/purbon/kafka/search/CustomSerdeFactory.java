package com.purbon.kafka.search;

import com.purbon.kafka.search.serdes.JsonPOJODeserializer;
import com.purbon.kafka.search.serdes.JsonPOJOSerializer;
import java.util.HashMap;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class CustomSerdeFactory<T> {

  public Serde<T> build(Class<T> classObject) {

    HashMap<String, Object> serdeProps = new HashMap<>();

    final Serializer<T> customerSerializer = new JsonPOJOSerializer<>();
    serdeProps.put("JsonPOJOClass", classObject);
    customerSerializer.configure(serdeProps, false);

    final Deserializer<T> customerDeserializer = new JsonPOJODeserializer<>();
    serdeProps.put("JsonPOJOClass", classObject);
    customerDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(customerSerializer, customerDeserializer);
  }

}
