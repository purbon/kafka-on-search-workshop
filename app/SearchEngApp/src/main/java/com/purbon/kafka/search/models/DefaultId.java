package com.purbon.kafka.search.models;


import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.*;

import java.io.IOException;

public class DefaultId {

  @JsonIgnore
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static class IdDeserializer extends KeyDeserializer {

    @Override
    public DefaultId deserializeKey(
        String key,
        DeserializationContext ctx) throws IOException {

      return OBJECT_MAPPER.readValue(key, DefaultId.class);
    }
  }

  public static class IdSerializer extends JsonSerializer<DefaultId> {

    @Override
    public void serialize(DefaultId key,
        JsonGenerator gen,
        SerializerProvider serializers)
        throws IOException {

      gen.writeFieldName(OBJECT_MAPPER.writeValueAsString(key));
    }
  }

  private final String id;

  @JsonCreator
  public DefaultId(@JsonProperty("id") String id) {
    this.id = id;
  }

  @JsonCreator
  public DefaultId(@JsonProperty("id") Integer id) {
    String myId = "";
    try {
      myId = id.toString();
    } catch (Exception ex) {
      myId = "-1";
    }
    this.id = myId;
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DefaultId defaultId = (DefaultId) o;

    return id != null ? id.equals(defaultId.id) : defaultId.id == null;
  }

  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "DefaultId{" +
        "id=" + id +
        '}';
  }

}
