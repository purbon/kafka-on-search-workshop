package com.purbon.kafka.search.models;

import java.util.HashMap;
import java.util.Map;

public class Document {

  public int docId;
  public Map<String, String> headers;
  public String content;

  public Document(String content) {
    this.content = content;
    this.headers = new HashMap<>();
    this.docId = -1;
  }
}
