# Tutorial Case Study
In this tutorial we will use problems and code examples from the world of commerce: the main entities will be **products**, **purchases** and **customers**. Following is a description of the data model, including data classes to be used throughout the tutorial.

## Product
A product is defined by an id, a category and a price. The set of all existing products will be referred to as **catalog**.

```java
public class Product {
  private final int productId;
  private final String category;
  private final int priceCents;
  
  public Product(int productId, String category, int priceCents) {
    this.productId = productId;
    this.category = category;
    this.priceCents = priceCents;
  }

  public int getProductId() {
    return productId;
  }

  public String getCategory() {
    return category;
  }

  public int getPriceCents() {
    return priceCents;
  }
  
  public String toCSV() {
    // TODO: Not fully CSV compliant
    return productId + "," + category + "," + priceCents;
  }
  
  public static Product fromCSV(String csv) {
    // TODO: Not fully CSV compliant
    String[] parts = csv.split(",");
    return new Product(Integer.parseInt(parts[0]), parts[1], Integer.parseInt(parts[2]));
  }
}
```

## Purchase
A purchase is defined by a customer id, the product id, the timestamp of the transaction and the quantity. Total price can be determined by crossing this with the relevant product record in the catalog.

```java
public class Purchase {
  private final String customerId;
  private final int productId;
  private final int quantity;
  private final long timestamp;
  
  public Purchase(String customerId, int productId, int quantity, long timestamp) {
    this.customerId = customerId;
    this.productId = productId;
    this.quantity = quantity;
    this.timestamp = timestamp;
  }

  public String getCustomerId() {
    return customerId;
  }

  public int getProductId() {
    return productId;
  }

  public int getQuantity() {
    return quantity;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String toCSV() {
    // TODO: For correctness - customer id should be CSV escaped
    return customerId + "," + productId + "," + quantity + "," + timestamp;
  }
  
  public static Purchase fromCSV(String csv) {
    // TODO: Not fully CSV compliant
    String[] parts = csv.split(",");
    return new Purchase(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), Long.parseLong(parts[3]));
  }
}
```

## Customer Summary
A summary of a set of purchases done by a specific customer.
Defined by customer id, total amount spent, first purchase timestamp and last purchase timestamp.

```java
public class CustomerSummary {
  private final String customerId;
  private int totalSpentCents;
  private long firstPurchaseTime;
  private long lastPurchaseTime;

  public CustomerSummary(String customerId, int totalSpentCents, long firstPurchaseTime, long lastPurchaseTime) {
    this.customerId = customerId;
    this.totalSpentCents = totalSpentCents;
    this.firstPurchaseTime = firstPurchaseTime;
    this.lastPurchaseTime = lastPurchaseTime;
  }

  public CustomerSummary(String customerId) {
    this(customerId, 0, Long.MAX_VALUE, 0);
  }

  public String getCustomerId() {
    return customerId;
  }

  public int getTotalSpentCents() {
    return totalSpentCents;
  }

  public long getFirstPurchaseTime() {
    return firstPurchaseTime;
  }

  public long getLastPurchaseTime() {
    return lastPurchaseTime;
  }
  
  public void merge(CustomerSummary otherSummary) {
    this.firstPurchaseTime = Math.min(firstPurchaseTime, otherSummary.firstPurchaseTime);
    this.lastPurchaseTime = Math.max(lastPurchaseTime, otherSummary.lastPurchaseTime);
    this.totalSpentCents += otherSummary.totalSpentCents;
  }
  
  public String toCSV() {
    // TODO: For correctness - customer id should be CSV escaped
    return customerId + "," + totalSpentCents + "," + firstPurchaseTime + "," + lastPurchaseTime;
  }
  
  public static CustomerSummary fromCSV(String csv) {
    // TODO: Not fully CSV compliant
    String[] parts = csv.split(",");
    return new CustomerSummary(parts[0], Integer.parseInt(parts[1]), Long.parseLong(parts[2]), Long.parseLong(parts[3]));
  }
}
```

[<< Prev](introduction.md) [Next >>](linear_pipes.md) 
