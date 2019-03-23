package com.purbon.kafka.search.models;

public class Customer {

  public int id;
  public String first_name;
  public String last_name;
  public String email;
  public String gender;
  public String club_status;
  public String comments;
  public String create_ts;
  public String update_ts;
  public String messagetopic;
  public String messagesource;

  public InvoicesAggregatedTable invoices = new InvoicesAggregatedTable();

  public void addInvoices(InvoicesAggregatedTable invoicesAggregated) {

    this.invoices = invoicesAggregated;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("User: <");
    sb.append(id);
    sb.append(",");
    sb.append(email);
    sb.append(">");
    sb.append(" ");
    sb.append(invoices.toString());
    return sb.toString();
  }
}
