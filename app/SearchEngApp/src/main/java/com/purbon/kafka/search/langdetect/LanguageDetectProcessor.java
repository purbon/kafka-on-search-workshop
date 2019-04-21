package com.purbon.kafka.search.langdetect;

import com.purbon.kafka.search.models.Document;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.tika.Tika;
import org.apache.tika.langdetect.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageResult;

public class LanguageDetectProcessor implements Transformer<String, Document, KeyValue<String, Document>> {

  private Tika tika;
  private static final OptimaizeLangDetector detect = new OptimaizeLangDetector();
  private ProcessorContext context;

  @Override
  public void init(ProcessorContext context) {
    tika = new Tika();
    this.context = context;
  }

  @Override
  public KeyValue<String, Document> transform(String key, Document doc) {
    LanguageResult result = detect.detect(doc.content);
    doc.headers.put("LANG", result.getLanguage());
    return new KeyValue(key, doc);
  }

  @Override
  public void close() {
    //EMPTY
  }
}
