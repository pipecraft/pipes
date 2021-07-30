# Parallel Pipes

So far we dealt with linear and tree like flows, but all were executed by a single thread (the caller thread). We can definitely speed up things if we use more CPUs, provided that we aren't close to the disk's read/write capacity. This can be done by allowing multiple threads to run different parts of the flow. In this section we present the **parallel pipes** approach, which means that our flow topology includes multiple "branches", and each branch is executed by a specific thread. The next section presents a different multi-threaded approach, which doesn't require replicating parts of the flow.

The problem
Suppose that we now have a huge customer DB, consisting of hundreds of large files, each storing CustomerSummary records. We would like to have some way to query it efficiently. A query consists of a timeframe of interest, and the query should return all ids of customers whose timeframe of activity intersects the given timeframe. The output should be printed to screen, one line per id.

The single threaded solution
Using the tools presented in this tutorial so far, we can implement a single threaded solution as follows:

```java
 private static void query(File dbFolder, long from, long to) throws IOException, PipeException, InterruptedException {
    try (
        Pipe<CustomerSummary> readerP = new MultiFileReaderPipe<>(
            LocalMultiFileReaderConfig.builder(CUSTOMER_SUMMARY_DECODER).paths(dbFolder).build());
        Pipe<CustomerSummary> filterP = new FilterPipe<>(readerP, 
            s -> from <= s.getLastPurchaseTime() && to >= s.getFirstPurchaseTime());
        TerminalPipe outputP = new ConsumerPipe<>(filterP, cs -> System.out.println(cs.getCustomerId()));
        ) {
      outputP.start();
    }
  }
```
Assuming that we can process 100MB per thread per second, a DB of size 20GB will take more than 3 minutes to scan.

## A parallel pipes solution
Instead of producing the linear flow above, we can create multiple copies of the read->filter->print sequence (one per input file), and attach all to a **ParallelConsumerPipe**, responsible for using a predefined number of threads to consume all input pipes. We'll also replace the ConsumerPipe above with a **CallbackPipe**, because we only need to run a callback method per item, and there's no need to drain the pipe at that point anymore - the parallel consumer does it anyway.

```java
public static void queryParallel(File dbFolder, long from, long to) throws IOException, PipeException, InterruptedException {
  List<Pipe<?>> pipes = new ArrayList<>();
  for (File file : dbFolder.listFiles()) {
    Pipe<CustomerSummary> readerP = new BinInputReaderPipe<>(file, CUSTOMER_SUMMARY_DECODER);
    Pipe<CustomerSummary> filterP = new FilterPipe<>(readerP, 
        s -> from <= s.getLastPurchaseTime() && to >= s.getFirstPurchaseTime());
    Pipe<CustomerSummary> outputP = new CallbackPipe<>(filterP, cs -> System.out.println(cs.getCustomerId()));
    pipes.add(outputP);
  }
  try (TerminalPipe parallelP = new ParallelConsumerPipe(8, pipes)) {
    parallelP.start();
  }
}
```

Creating hundreds of pipes is usually not a problem by itself, since by convention pipes should not do any heavy initialization when constructed. Only start() method should "awake" them and start the actual work. The **ParallelConsumerPipe** doesn't start all input pipes at once. In the example above we specify that 8 threads should be used, meaning that at any given time there are at most 8 started pipes which are being consumed. A thread continues to the next pipe only when the current pipe it works on is fully consumed.
The number of threads recommended for use depends on the number of machine CPUs, as well as on the disk IO speed capacity. A machine with multiple cores and SSD disk can highly benefit from parallel pipes.

As an alternative to building a pipe sequence per individual file, one can create fewer copies of **MultiFileReaderPipe**, with automatic sharding enabled . This way the files are being partitioned into K shards, providing K pipes to read from, where K is configurable.

[<< Prev](encoders_decoders.md) [Next >>](async_pipes.md)
