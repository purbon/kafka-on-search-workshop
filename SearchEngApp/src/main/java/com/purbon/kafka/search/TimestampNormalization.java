package com.purbon.kafka.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.purbon.kafka.search.models.Customer;
import com.purbon.kafka.search.models.DefaultId;
import com.purbon.kafka.search.models.Document;
import com.purbon.kafka.search.models.Invoice;
import com.purbon.kafka.search.models.InvoicesAggregatedTable;
import com.sun.tools.internal.xjc.Language;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.tika.Tika;
import org.apache.tika.langdetect.OptimaizeLangDetector;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;

public class LanguageDetection extends IngestPipeline {


  private static final String RAW_DOCS_TOPIC = "raw.docs";
  private static final String DOCS_WITH_LANGUAGE_TOPIC = "docs";
  private final Tika tika;
  private static final OptimaizeLangDetector detect = new OptimaizeLangDetector();

  public LanguageDetection() {
    tika = new Tika();
    detect.loadModels();
  }

  public static Document serialize(String jsonString) {

    return new Document(jsonString);
  }

  public static void main(String[] args) {

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> docsStream = builder.stream(RAW_DOCS_TOPIC,
        Consumed.with(Serdes.String(), Serdes.String()));

    final Serde<Document> docsSerde = SerdesFactory.from(Document.class);

    docsStream
        .mapValues(raw_doc -> serialize(raw_doc))
        .mapValues(jsonDoc -> {
          LanguageResult result = detect.detect(jsonDoc.content);
          jsonDoc.headers.put("LANG", result.getLanguage());
          return jsonDoc;
        })
        .to(DOCS_WITH_LANGUAGE_TOPIC, Produced.with(Serdes.String(), docsSerde));

    LanguageDetection profilesApp = new LanguageDetection();
    profilesApp.run(builder.build(), "language-detection");

  }

}
