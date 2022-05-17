# Encoders/Decoders
In order to make IO operations more flexible, and in order to introduce additional powerful pipe types, we should discuss how encoding/decoding works in pipes framework. Our previous examples were limited to textual file encoding only (specifically CSV), and were using TxtFileReaderPipe/TxtFileWriterPipe combined with MapPipe as decoder/encoder for that purpose. In the following example we will keep the same data representation in files, but this time using the more general **BinInputReaderPipe** and **BinFileWriterPipe**. This choice will make it easier for us to change the encoding when needed, and it also allows the encoding to be dynamically determined at runtime.

In addition to the BinInputReaderPipe, we will use the **HashJoinPipe**, **SortPipe** and **SharderByItemPipe**, all requiring the user to specify encoders/decoders.

## The problem
In the previous section we generated a summary per category. This time we are interested in a summary per customer (see CustomerSummary object), consisting of:

* customer id

* total spent cents

* first purchase timestamp

* last purchase timestamp

In addition, we don't want all output records to go into a single file. Instead, we would like them split between files according to the total spent field. All customers with total spent between x*10 (inclusive) and (x+1)*10 (non-inclusive) USD should be written to the file named {x}.csv.

This time we also relax the conditions on the input pipes, which are not required to have any particular item order anymore.

## EncoderFactory / DecoderFactory
**EncoderFactory** and **DecoderFactory** are the main players in the framework's encoding/decoding solution, and are widely used as parameters of pipes performing IO, either on user defined inputs/outputs, or for temporary disk storage behind the scenes as part of their work. Both are java interfaces, and their combination is the **CodecFactory** interface, which is also used by some APIs.

EncoderFactory is a functional interface, and the method to implement is:

```java
ItemEncoder<T> newEncoder(OutputStream os, FileWriteOptions writeOptions) throws IOException;
```

Similarly, in DecoderFactory we need to implement:

```java
ItemDecoder<T> newDecoder(InputStream is, FileReadOptions readOptions) throws IOException;
```

The motivation for using factories rather than directly working with classes such as ItemEncoder and ItemDecoder is:

1. In construction time the encoder/decoder "sees" the input/output stream, and can do some pre-processing (such as buffering or creating a more convenient and efficient reader based on the provided stream).

2. Receiving a single decoder/encoder as parameter forces the pipe implementation to use the same instance in different contexts. This may arise thread issues when decoders/encoders are stateful (and we don't want to prevent them from being so).

Fortunately, there are some shortcuts. We don't always need to implement ItemDecoder/ItemEncoder and the corresponding DecoderFactory and EncoderFactory from scratch. The framework provides **SimpleDecoderFactory** and **SimpleEncoderFactory** which are useful when the required logic for IO stream reading/writing is simple, not requiring any pre-processing. In addition, there are some ItemDecoder/ItemEncoder implementations ready to use (AVRO, CSV, Protocol Buffers and integer types). Lastly, **TxtEncoderFactory** and **TxtDecoderFactory** are used for any text based encoding of items of some type T. TxtEncoderFactory requires defining itemTextualizer, while TxtDecoderFactory works with an itemDetextualizer.

## The code

```java
public class CustomerSummaryPipeline {
  private static final DecoderFactory<Purchase> PURCHASE_DECODER = new TxtDecoderFactory<>(Purchase::fromCSV);
  private static final EncoderFactory<Purchase> PURCHASE_ENCODER = new TxtEncoderFactory<>(Purchase::toCSV);
  private static final CodecFactory<Purchase> PURCHASE_CODEC = new DelegatingCodecFactory<>(PURCHASE_ENCODER, PURCHASE_DECODER);
  
  private static final DecoderFactory<Product> PRODUCT_DECODER = new TxtDecoderFactory<>(Product::fromCSV);
  private static final EncoderFactory<Product> PRODUCT_ENCODER = new TxtEncoderFactory<>(Product::toCSV);
  private static final CodecFactory<Product> PRODUCT_CODEC = new DelegatingCodecFactory<>(PRODUCT_ENCODER, PRODUCT_DECODER);

  private static final EncoderFactory<CustomerSummary> CUSTOMER_SUMMARY_ENCODER = new TxtEncoderFactory<>(CustomerSummary::toCSV);
  private static final DecoderFactory<CustomerSummary> CUSTOMER_SUMMARY_DECODER = new TxtDecoderFactory<>(CustomerSummary::fromCSV);
  private static final CodecFactory<CustomerSummary> CUSTOMER_SUMMARY_CODEC = new DelegatingCodecFactory<>(CUSTOMER_SUMMARY_ENCODER, CUSTOMER_SUMMARY_DECODER);
  
  public static void computeCustomerSummaries(File catalogFile, File purchaseFilesFolder, Set<String> categories, File outputFolder) 
      throws IOException, PipeException, InterruptedException {
    List<Pipe<Purchase>> purchasePipes = buildPurchasePipes(purchaseFilesFolder);
    
    try (
        Pipe<Product> catReaderP = new BinInputReaderPipe<>(catalogFile, PRODUCT_DECODER);
        Pipe<Product> catFilterP = new FilterPipe<>(catReaderP, c -> categories.contains(c.getCategory()));
        Pipe<JoinRecord<Integer, Product, Purchase>> joinP = new HashJoinPipe<>(
            catFilterP, 
            Product::getProductId,  // Join key extractor on catalog entries
            purchasePipes, 
            Purchase::getProductId, // Join key extractor on purchases
            JoinMode.INNER,
            50,                     // 50 data partitions by product id
            PRODUCT_CODEC, 
            PURCHASE_CODEC, 
            FileUtils.getSystemDefaultTmpFolder());
            
        Pipe<CustomerSummary> prodAmountP = new FlexibleMapPipe<>(joinP, CustomerSummaryPipeline::toIntermediateCustomerSummary);
        Pipe<CustomerSummary> sortP = new SortPipe<>(prodAmountP, 200_000, CUSTOMER_SUMMARY_CODEC, Comparator.comparing(CustomerSummary::getCustomerId));
        Pipe<CustomerSummary> aggrP = new SequenceReductorPipe<>(sortP,
            ReductorConfig.<CustomerSummary,String,CustomerSummary,CustomerSummary>builder()
            .discriminator(CustomerSummary::getCustomerId)
            .aggregatorCreator(CustomerSummary::new)
            .aggregationLogic(CustomerSummary::merge)
            .postProcessor(Functions.identity()).build()
        );
        Pipe<CustomerSummary> progressP = new ProgressPipe<>(aggrP, 1000, pct -> System.out.println("Progress: " + pct));
        TerminalPipe sharderP = new SharderByItemPipe<>(progressP, CUSTOMER_SUMMARY_ENCODER, 
            s -> s.getTotalSpentCents() / 1000 + ".csv", outputFolder);
        ) {
      sharderP.start();
    }
  }

  private static List<Pipe<Purchase>> buildPurchasePipes(File purchaseFilesFolder) {
    List<Pipe<Purchase>> purchasePipes = new ArrayList<>();
    for (File purchaseFile : purchaseFilesFolder.listFiles()) {
      Pipe<Purchase> readerP = new BinInputReaderPipe<>(purchaseFile, PURCHASE_DECODER);
      purchasePipes.add(readerP);
    }
    return purchasePipes;
  }
  
  private static Pipe<CustomerSummary> toIntermediateCustomerSummary(JoinRecord<Integer, Product, Purchase> joinRec) {
    List<CustomerSummary> summaries = new ArrayList<>();
    Product catalogEntry = joinRec.getLeft().get(0); // We expect a single catalog entry match in the join
    for (List<Purchase> purchases : joinRec.getRight().values()) {
      for (Purchase purchase : purchases) {
        long purchaseTime = purchase.getTimestamp();
        int spentCents = purchase.getQuantity() * catalogEntry.getPriceCents();
        CustomerSummary summary = new CustomerSummary(purchase.getCustomerId(), spentCents, purchaseTime, purchaseTime);
        summaries.add(summary);
      }
    }
    return new CollectionReaderPipe<>(summaries);
  }
}
```

This time we use only BinInputReaderPipe to read the input files, and we use different decoder factories for the catalog file and for the purchase files.

Later comes the join, but this time we use **HashJoinPipe** rather than SortedJoinPipe, because inputs aren't sorted. This pipe has similar inputs to the SortedJoinPipe (including all supported join modes) and has the same output data type. The caller should specify a codec for left and right data types, because hash join works by partitioning the data by the join key, storing on local disk, and then loading partitions one by one, doing hashtable lookups to find matches (see [Grace-hash-join algorithm](https://en.wikipedia.org/wiki/Hash_join#Grace_hash_join)). The number of partitions is a very important parameter for protecting your memory usage. The heuristic we recommend for setting it is estimating the total expected number of MB to be read by the join pipe (left+right), and dividing this number by the maximum number of MB you can afford to hold in memory for that purpose. In some cases you'll need to re-tune it after experimenting with real data.

> A few notes about performance differences between SortedJoinPipe and HashJoinPipe: SortedJoinPipe will always be faster than HashJoinPipe, due to the disk usage by the latter. However, it is very likely that when inputs are un-ordered in the first place, sorting left and right and then performing a sorted join will be much slower than doing a hash join. Therefore, when choosing between the two, we recommend following these rules:
> 1. When inputs are already sorted, always use SortedJoinPipe
> 2. When inputs aren't sorted, but there are other benefits of sorting them elsewhere, consider redesigning your system and add a preliminary sorting phase. This will allow using the efficient SortedJoinPipe.
> 3. When inputs aren't sorted, and there's no benefit in having them sorted, use HashJoinPipe.

After the join pipe, each join record encapsulates the whole purchase history related to a single product. This is a memory usage risk to take into consideration; here we assume that a single product will not have tens of millions of associated purchase events, but this assumption is not always true in the general case. The next pipe simply "flattens" this join record, by splitting it into CustomerSummary items, each describing the customer's purchase summary on that particular product. The flattening is performed by the **FlexibleMapPipe**, which is the more flexible version of MapPipe, and can produce 0,1 or more output items per input item.

Now we wish to group together all summaries related to the same customer. The way we do it here is not necessarily the most efficient one, but its useful for introducing new pipes in the context of this tutorial. We start by sorting the customer summary records by customer id, and then we merge sequences of the same customer together. A faster alternative to these two pipes, assuming that sorted output isn’t a requirement, is the **HashReductorPipe**.

The sorting part is performed using the **SortPipe**. Like HashJoinPipe, this pipe also needs temporary disk space, this time for storing sorted chunks of data. Hence the use of the codec. In addition, the user has to specify how many items can be held in memory at once when sorting (in our case 200_000). Using small numbers may create an excessive number of temp files, while using a large number increases the chance of running out of heap space memory.

Later comes the sequence merging, performed by the **SequenceReductorPipe**, which uses a user provided strategy for merging consecutive items into one output item. We specify that item families are defined by the customer id, and we aggregate summaries into a super-summary object by calling the CustomerSummary.merge(..) method. After this pipe, we have the final summaries per customer.

Now we proceed to writing to disk. Since we want outputs to be split on multiple files on local disk, we use the **SharderByItemPipe**. Unlike the previous pipes that read and write from/to disk, this pipe requires only an encoder because it only writes. The third parameter is the shard selector, specifying for every input item the name of the file it should be written to. In our case the shard is determined by the range in which the total spent amount falls.
There are two other file sharder implementations that may be helpful. **SharderByHashPipe** is a subclass with a built in shard selector based on the hash value of the input items. It achieves a nearly uniform (and consistent) splitting of the items into K buckets. **SharderBySeqPipe** can be used for cases when the input is already ordered as contiguous sequences, each sequence designated to a different file. The major benefit of this pipe is that it only maintains one open file at a time. With the other 2 implementations, the user should be aware of the risk of having too many open files (specially when file compression is enabled).

## ByteArrayEncoder / ByteArrayDecoder
EncoderFactory and DecoderFactory are used for stateful encoders/decoders which operate on OutputStream/InputStream. They support variable length message sizes, and need to know where messages start/end. In some cases, we just need a simpler (usually stateless) logic of encoding a single item into a byte array, and decoding an item from a given byte array. This is the purpose of **ByteArrayEncoder** and **ByteArrayDecoder** interfaces, and their bi-directional version - **ByteArrayCodec**.

Any encoder factory includes a default implementation of byte array encoder, and same goes for decoder factories (see EncoderFactory.newByteArrayEncoder() and DecoderFactory.newByteArrayDecoder()). However, this default implementation is a simple reduction from IO stream encoding/decoding to a byte array encoding/decoding, and therefore is rarely the most efficient way to do it. Some pipes require array encoders/decoders, so we recommend overriding these methods where relevant.


## Non textual binary encoding
So far we only saw binary encoders producing textual output. While this is easier to debug and work with, sometimes we have data that can benefit from a more compact and faster encoding/decoding.

The framework provides a few useful building blocks. **AvroCodecFactory** is a good option for a standard, general purpose binary encoder working with reflection. In addition, **IntEncoders** and **IntDecoders** contain a variety of useful integer number encoder/decoder factories.

Anyway, a custom codec factory isn't complicated to implement, specially when using utility functions from org.pipecraft.infra.io.Coding and org.pipecraft.infra.io.FileUtils. Following is a codec factory implementation for the CustomerSummary class, with a variable record length. The encoding scheme is described in the class header. Note that in this example we didn't override newByteArrayEncoder() and newByteArrayDecoder(), so we remain with the default (less efficient) byte array encoder/decoder implementations.

```java
/**
 * Encoding format:
 * - Customer id length [1 byte]
 * - Customer id UTF8
 * - Total spent cents, little-endian int [4 bytes]
 * - First purchase timestamp, little-endian long [8 bytes]
 * - Last purchase timestamp, little-endian long [8 bytes]
 */
public class CustomerSummaryCodecFactory implements CodecFactory<CustomerSummary> {

  @Override
  public ItemEncoder<CustomerSummary> newEncoder(OutputStream os, FileWriteOptions writeOptions) throws IOException {
    return new CustomerSummaryEncoder(os, writeOptions);
  }

  @Override
  public ItemDecoder<CustomerSummary> newDecoder(InputStream is, FileReadOptions readOptions) throws IOException {
    return new CustomerSummaryDecoder(is, readOptions);
  }

  private class CustomerSummaryEncoder implements ItemEncoder<CustomerSummary> {
    private final BufferedOutputStream os;

    public CustomerSummaryEncoder(OutputStream os, FileWriteOptions writeOptions) throws IOException {
      this.os = FileUtils.getOutputStream(os, writeOptions);
    }

    @Override
    public void close() throws IOException {
      os.close();
    }

    @Override
    public void encode(CustomerSummary item) throws IOException {
      byte[] encodedId = item.getCustomerId().getBytes(StandardCharsets.UTF_8);
      os.write(encodedId.length);
      os.write(encodedId);
      Coding.writeLittleEndian32(item.getTotalSpentCents(), os);
      Coding.writeLittleEndian64(item.getFirstPurchaseTime(), os);
      Coding.writeLittleEndian64(item.getLastPurchaseTime(), os);
    }
  }
  
  private class CustomerSummaryDecoder implements ItemDecoder<CustomerSummary> {
    private final BufferedInputStream is;

    public CustomerSummaryDecoder(InputStream is, FileReadOptions readOptions) throws IOException {
      this.is = FileUtils.getInputStream(is, readOptions);
    }

    @Override
    public void close() throws IOException {
      is.close();
    }

    @Override
    public CustomerSummary decode() throws IOException {
      int idLength = is.read();
      byte[] idBytes = Coding.read(is, idLength);
      String id = new String(idBytes, StandardCharsets.UTF_8);
      int totalSpent = Coding.readLittleEndian32(is);
      long firstPurchaseTS = Coding.readLittleEndian64(is);
      long lastPurchaseTS = Coding.readLittleEndian64(is);
      return new CustomerSummary(id, totalSpent, firstPurchaseTS, lastPurchaseTS);
    }
  }
}
```

[<< Prev](tree_flow.md) [Next >>](parallel_pipes.md)
