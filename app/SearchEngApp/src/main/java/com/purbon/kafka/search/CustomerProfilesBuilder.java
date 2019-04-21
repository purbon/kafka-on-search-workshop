package com.purbon.kafka.search;

import com.purbon.kafka.search.models.Customer;
import com.purbon.kafka.search.models.DefaultId;
import com.purbon.kafka.search.models.Invoice;
import com.purbon.kafka.search.models.InvoicesAggregatedTable;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class CustomerProfilesBuilder extends IngestPipeline {


  public CustomerProfilesBuilder() {

  }

  public static void main(String[] args) {

    StreamsBuilder builder = new StreamsBuilder();

    final Serde<DefaultId> defaultIdSerde = SerdesFactory.from(DefaultId.class);
    final Serde<Invoice> invoiceSerde = SerdesFactory.from(Invoice.class);
    final Serde<InvoicesAggregatedTable> totalsSerde = SerdesFactory.from(InvoicesAggregatedTable.class);

    // Stream of invoices
    KStream<DefaultId, Invoice> invoicesKStream = builder.stream(INVOICES_TOPIC,
        Consumed.with(defaultIdSerde, invoiceSerde));

    // group invoices by invoiceNo
    KGroupedStream<DefaultId, Invoice> groupedInvoices = invoicesKStream
        .groupBy((key, invoice) -> {
          try {
            return new DefaultId(invoice.CustomerID);
          } catch (NullPointerException ex) {
            return new DefaultId("-1");
          }
            },
            Grouped.with(defaultIdSerde, invoiceSerde));

    // Sum the total of the invoices and output a table of the form
    // CustomerId - List of [InvoiceNo - InvoiceTotal]
    KTable<DefaultId, InvoicesAggregatedTable> table = groupedInvoices
        .aggregate(() -> new InvoicesAggregatedTable(),
            (key, invoice, data) -> {
              float totalLine = Float.parseFloat(invoice.Quantity) * Float.parseFloat(invoice.UnitPrice);
              data.accountInvoice(invoice.InvoiceNo, totalLine);
              return data;
            },
            Materialized.with(defaultIdSerde, totalsSerde));

    // output (UserId, List<(InvoiceNo, Float)>)
    // [KTABLE-TOSTREAM-0000000007]: 14849, [ (536463 -> 17.400002) (536466 -> 42.9) (536460 -> 295.53998)]

  final Serde<Customer> customerSerde = SerdesFactory.from(Customer.class);

  // pull the customer tables
    KTable<DefaultId, Customer> customersTable = builder
        .table(CUSTOMERS_TOPIC, Consumed.with(defaultIdSerde, customerSerde));

    // KTable - KTable join customer with aggregated invoice totals
    customersTable
        .join(table, (customer, invoices) -> {
          customer.addInvoices(invoices);
          return customer;
        }, Materialized.with(defaultIdSerde, customerSerde))
        .toStream()
        .to(CUSTOMERS_PROFILES_TOPIC, Produced.with(defaultIdSerde, customerSerde));

    CustomerProfilesBuilder profilesApp = new CustomerProfilesBuilder();
    profilesApp.run(builder.build(), "customer-profile-builder");

  }

}
