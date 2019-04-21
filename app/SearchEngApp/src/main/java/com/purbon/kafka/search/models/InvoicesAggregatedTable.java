package com.purbon.kafka.search.models;

import java.util.HashMap;

public class InvoicesAggregatedTable {

  public HashMap<String, Float> table;

  public InvoicesAggregatedTable() {
    table = new HashMap<>();
  }

  public void accountInvoice(String invoiceNo, float value) {
    if (table.get(invoiceNo) == null) {
      table.put(invoiceNo, 0f);
    }
    table.put(invoiceNo, table.get(invoiceNo)+value);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[ ");
    int i = 0;
    for(String invoiceNo : table.keySet()) {
      if (i > 0) {
        sb.append(" ");
      }
      sb.append("("+ invoiceNo+" -> ");
      sb.append(table.get(invoiceNo));
      sb.append(")");
      i+=1;
    }
    sb.append("]");
    return sb.toString();
  }
}
