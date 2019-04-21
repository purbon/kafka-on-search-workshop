package com.purbon.kafka.search;

import com.purbon.kafka.search.langdetect.LanguageDetectProcessor;
import com.purbon.kafka.search.models.DefaultId;
import com.purbon.kafka.search.models.Document;
import com.purbon.kafka.search.models.Invoice;
import com.purbon.kafka.search.models.InvoicesAggregatedTable;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.tika.Tika;
import org.apache.tika.langdetect.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageResult;

public class LanguageDetectionWithProcessorAPI extends IngestPipeline {


  private static final String RAW_DOCS_TOPIC = "raw.docs";
  private static final String DOCS_WITH_LANGUAGE_TOPIC = "docs";

  public static void main(String[] args) {

    StreamsBuilder builder = new StreamsBuilder();
    final Serde<Document> docsSerde = SerdesFactory.from(Document.class);

    KStream<String, String> docsStream = builder.stream(RAW_DOCS_TOPIC,
        Consumed.with(Serdes.String(), Serdes.String()));

    docsStream
        .mapValues(raw_doc -> new Document(raw_doc))
        .transform(LanguageDetectProcessor::new)
        .to(DOCS_WITH_LANGUAGE_TOPIC, Produced.with(Serdes.String(), docsSerde));

    LanguageDetectionWithProcessorAPI profilesApp = new LanguageDetectionWithProcessorAPI();
    profilesApp.run(builder.build(), "language-detection");

  }

}
