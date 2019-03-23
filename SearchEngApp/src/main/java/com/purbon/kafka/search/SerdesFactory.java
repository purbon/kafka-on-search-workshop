package com.purbon.kafka.search;


import com.purbon.kafka.search.serdes.JsonHybridDeserializer;
import com.purbon.kafka.search.serdes.JsonPOJODeserializer;
import com.purbon.kafka.search.serdes.JsonPOJOSerializer;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class SerdesFactory {

  public static <T> Serde<T> from(Class<T> clazz, boolean isKey) {
    Map<String, Object> serdeProps = new HashMap<>();
    serdeProps.put("JsonPOJOClass", clazz);

    Serializer<T> ser = new JsonPOJOSerializer<>();
    ser.configure(serdeProps, isKey);

    Deserializer<T> de = new JsonPOJODeserializer<>();
    de.configure(serdeProps, isKey);

    return Serdes.serdeFrom(ser, de);
  }
}


