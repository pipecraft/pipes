# General Tips

## Optimizations
* Distinguish between flow assembling code, which occurs only once and may not be extra efficient, and the flow execution itself. Specifically, pay attention to operations performed on every single item passing through the pipes, and try to make them as efficient as possible. Although it's tempting to use features such as regex, java streams, JDK's date parsing or the String.split() method, it's not recommended to use them for item level operations when there is an alternatives, due to CPU costs. Among other things, this tip applies to mapping method passed to the MapPipe, and filter predicate passed to FilterPipe.

* Consider avoiding expensive validations, provided that the code will fail-fast anyway with an exception. Although the error is not necessarily appropriate to be presented as a runtime exception, you may benefit from less CPU usage.

* Avoid repeating parsing operations during the same flow. Redesigning the flow structure or the data types can help avoid that.

* Compression and decompression are expensive, and their effect on CPU is noticeable. Avoid them when possible, and prefer ZSTD when you do use compression.

* When implementing Encoder/Decoder factories, remember to override newByteArrayEncoder/Decoder(..) if relevant, because the default implementation isn't optimal. In addition, make your ItemEncoder/ItemDecoder implementations stateful, re-using objects/buffers where possible. 

* A single threaded flow which involved IO reads and IO writes at the end can benefit from a QueuePipe. This will allow one thread to work on IO reads and decoding, and the other to work on encoding and IO writes. 

## Distributed pipelines
There is no simple answer to when distributed pipelines are needed. As a rough rule, we recommend following the **20GB** and **1TB** as thresholds. Any pipeline reading less than 20GB of data (uncompressed) should be single-machined, and pipelines reading 1TB or more of uncompressed data should probably be distributed. For the intermediate range, it really depends on the pipeline, its complexity and the requirements. One may still find it useful to choose a simple single/multi-threaded solution rather than a distributed solution for pipelines that don't have strict run time requirements, and can take several hours to complete.

Experimentation is the key for achieving the best solution. Start with a single threaded solution on partial data to evaluate the data processing speed. After some possible optimizations, you can extrapolate the result and estimate how long will a full run take. This can help decide whether to use multithreaded pipes, distributed pipes, or maybe both.

## Progress tracking
**ProgressPipe** reports the progress by estimating the portion of items passing through the pipe compared to the total number of items expected to pass there. Usually we would like to position it right before the sink pipe, so that it reports overall progress. However, sum pipe implementations need to accumulate all items before producing outputs, so positioning the progress pipe after them will create a false impression of progress=0% for a long time, and only then fast progress till 100%. Example of these pipes are SortPipe, HashJoinPipe and TopKPipe. When using them, it is recommended to add multiple ProgressPipe instances in strategic places of the flow. When done properly and with descriptive logging messages, it allows tracking the progress in different phases of the flow.

## Shard writers
Two of the shard writer implementations (**SharderByItemPipe** and **SharderByHashPipe**) distribute input items between multiple files, and need to keep all files open until done. It's important to be aware of the expected number of created files, specially when the sharder is configured to compress data. Compressors tend to use a significant amount of memory for their buffers, and hundreds of them active on the same time may cause OOME. Use compression only when the number of outputs is relatively small. Otherwise, when compression is a must, consider doing the compression separately after the flow terminates(See FileUtils.compressAll(..) method, which uses K threads to compresses all files in a given folder).

## Custom pipes
When implementing custom pipes, consider using **DelegatePipe** and **CompoundPipe** as your base class. They can make the implementation simpler in many cases.

## Join pipes
**SortedJoinPipe** will always be faster than **HashJoinPipe**, due to the disk usage by the latter. However, it is very likely that when inputs are un-ordered in the first place, sorting left and right and then performing a sorted join will be much slower than doing directly a hash join. Therefore, when choosing between the two, we recommend following these rules:

1. When inputs are already sorted, always use SortedJoinPipe
2. When inputs aren't sorted, but there are other benefits of sorting them elsewhere, consider redesigning your system and add a preliminary sorting phase. This will allow using the efficient SortedJoinPipe.
3. When inputs aren't sorted, and there's no benefit in having them sorted, use HashJoinPipe.

In addition, keep in mind that a join operation can sometimes be done without an actual join pipe. For example, if your left side of the join has data which can fit entirely in memory, representing it as a HashMap can be a faster approach. With the map in memory, you can scan right side items using MapPipe (or FlexibleMapPipe), and perform hash lookups to find the left side matches.

## Reading from cloud storage
When reading a single text file (compressed or uncompressed) from storage, one can use **StorageTxtFileReaderPipe** and **StorageTxtFileFetcherReaderPipe**. The former streams the file contents, while the latter efficiently downloads the complete file first and then reads. The second option is preferable when the file is large, or when you believe that the data streaming itself is likely to be a bottleneck rather than the data processing itself.

In case of binary file, we currently don't have similar pipes to do the work, so one can either download first (See Bucket.getSliced(..) method) or get a remote input stream and pass it to **BinInputReaderPipe**.

When multiple files are needed, **StorageMultiFileReaderPipe** and **AsyncStorageMultiFileReaderPipe** are the best options, since they provide the maximum flexibility. However, make sure the download is efficient:

1. **StorageMultiFileReaderPipe** - By default uses one thread to stream the remote data, file by file. Consider using the download-first flag.
2. **AsyncStorageMultiFileReaderPipe** - By default streams data using multiple threads, but can still benefit from download-first when there are very few files
3. When auto-sharding is turned on, make sure that shards are as even as possible. In case of few files or when the files are very uneven in their sizes, consider activating the balanced sharding flag, which tries to reach even data volumes across shards.

## Debugging
Debugging pipes isn't always easy, but the following tips may help:

1. Write unit-tests for every flow. If the flow requires working with storage, use **LocalDiskStorage** as a test double, since it's a reliable emulator of the actual service. If the flow is distributed, let the test start multiple instances on the test machine, using different ports.
2. You can add the non-intrusive **CallbackPipe**, with logging or other debug code in it, to specific places of the flow,
3. You can extract a specific part of the flow aside, and test it locally. You can then feed it with artificial data (e.g. **CollectionReaderPipe**, or some local file reader pipe).
4. Placing breakpoints in the next() method of selected pipes, to see what items are produced and why

## Memory
Parallel pipes work by running multiple flow branches in parallel. You should be aware of the resources needed per branch, and set the number of threads in the **ParallelConsumerPipe** accordingly. For example, if every branch uses **SortPipe**, then the memory item limit passed to the sort pipe should take into consideration the fact that there are multiple instance of the same sorter pipe, and T of them may run on the same time (where T is the number of threads configured in the ParallelConsumerPipe).

With async pipes you may also encounter memory usage problems, in case that you choose to store large amounts of data in a [thread-local](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/ThreadLocal.html) way.

## Exceptions
A flow can throw the following exceptions only:

1. PipeException
2. InterruptedException
3. IOException

The first 2 are thrown by the elementary pipe operations (start(), next() and peek()), and the last mainly originates from the [try-with-resources](https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html) statement (which handles the closing of all pipes).

When designing your public APIs, we recommend to:

1. Avoid throwing both PipeException and IOException. You can wrap IOException by IOPipeException instead.
2. Use specific PipeException subclasses. For example, use ValidationPipeException when a pipe encounters data it can't work with. Create your own subclasses if needed.

## Diagrams
Use flow diagrams (see the "tree" section for an example) to make flows easy to understand. These canonical flow descriptions can be helpful in design documents.

[<< Prev](main_pipes_glossary.md)
