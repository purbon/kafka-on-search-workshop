package com.purbon.kafka.search;

import com.purbon.kafka.search.models.Customer;
import com.purbon.kafka.search.models.DefaultId;
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

  public static void main(String[] args) {

    StreamsBuilder builder = new StreamsBuilder();

    final Serde<DefaultId> defaultIdSerde = SerdesFactory.from(DefaultId.class);
    final Serde<Invoice> invoiceSerde = SerdesFactory.from(Invoice.class);
    final Serde<InvoicesAggregatedTable> totalsSerde = SerdesFactory.from(InvoicesAggregatedTable.class);

    KStream<String, String> docsStream = builder.stream(RAW_DOCS_TOPIC,
        Consumed.with(Serdes.String(), Serdes.String()));

    docsStream
        .mapValues(raw_doc -> {
          // transform to json
          return raw_doc;
        })
        .mapValues(jsonDoc -> {
          LanguageResult result = detect.detect(jsonDoc);
          return jsonDoc;
        })
        .to(DOCS_WITH_LANGUAGE_TOPIC);

    LanguageDetection profilesApp = new LanguageDetection();
    profilesApp.run(builder.build(), "language-detection");

  }

}
