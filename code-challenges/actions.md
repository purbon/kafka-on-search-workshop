# Actions

* load customers and invoices data in MySQL database
*


* customers
  * id;
  * first_name;
  * last_name;
  * email;
  * gender;
  * club_status;
  * comments;
  * create_ts;
  * update_ts;
  * messagetopic;
  * messagesource;


* invoices
  * id;
  * InvoiceNo;
  * StockCode;
  * Description;
  * Quantity;
  * InvoiceDate;
  * UnitPrice;
  * CustomerID;
  * Country;


# Build customer profile

* Customer-Profile
  * id
  * first_name
  * last_name
  * email
  * orders [Array]:
    * InvoiceNo
    * TotalValue
    * NumberOfProducts


Process to build this profile:

* Aggregate invoices
* Join with customers
* store in profiles
