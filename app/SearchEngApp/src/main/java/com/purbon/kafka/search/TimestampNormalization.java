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
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.tika.Tika;
import org.apache.tika.langdetect.OptimaizeLangDetector;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;

public class TimestampNormalization extends IngestPipeline {


  static class JodaProcessor implements Processor<String, String> {

    private ProcessorContext context;
    final Serde<Customer> customerSerde = SerdesFactory.from(Customer.class);

    @Override
    public void init(ProcessorContext context) {
      this.context = context;
    }

    @Override
    public void process(String key, String customerJson) {

      Customer customer = customerSerde.deserializer().deserialize(key, customerJson.getBytes());

      Instant instant = new Instant(customer.create_ts);
      DateTime newCreateTs = instant.toDateTime(DateTimeZone.forID("Europe/Berlin"));
      customer.create_ts = newCreateTs.toString();

      context.forward(key, customer);

    }

    @Override
    public void close() {
      //EMPTY
    }
  }


  public static void main(String[] args) {

    Topology topology = new Topology();

    topology
        .addSource("customers", CUSTOMERS_TOPIC)
        .addProcessor("joda-timestamp-corrector", JodaProcessor::new)
        .addSink("customers.fixed", "customers.fixed");

    TimestampNormalization profilesApp = new TimestampNormalization();
    profilesApp.run(topology, "timestamp-action");

  }

}
