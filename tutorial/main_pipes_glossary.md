# Main Pipes Glossary

## Source Pipes
### Local
**CollectionReaderPipe** - Reads a Collection from memory. Can be useful in tests.

---

**IteratorReaderPipe** - Reads the contents of a given iterator.

---

**ErrorPipe** - A pipe that instead of producing items, produces a configurable pipe exception when pulled from. Useful in tests.

---

**EmptyPipe/AsyncEmptyPipe** - A pipe producing no items. Useful in tests.

---

**SeqGenPipe/AsyncSeqGenPipe** - Produces a sequence of items. Can be useful in tests. In the async case it's not strictly a sequence, and should be considered as a set with no predefined order.

---

**QueueReaderPipe** - Reads the contents of a given blocking queue. Can work well with AsyncEnqueuingSharderPipe. Besides standard queue implementations, consider using LockFreeBlockingQueue implementation.

---

**BinInputReaderPipe** - A local binary file reader, working with a given DecoderFactory. Instead of a file, the constructor also accepts InputStream to read from. Supports compression.

---

**MultiTxtFileReaderPipe/AsyncMultiFileReaderPipe** - The preferred and most flexible way of reading multiple files from local disk. Receives a LocalMultiFileReaderConfig as a parameter. This config class is constructed using the builder design pattern. It supports: applying file filters, automatic sharding, recursive reading, determining the read order between files (only sync), and determining the number of reader threads (only async). Automatic sharding is useful when we want to split the files automatically, and read only one shard (assuming that other parts of the pipeline will take care of the other shards). Sharding tries to achieve even number of files per shard by default, but can be configured to approximate even total file sizes.

---

**TxtFileReaderPipe** - Reads a local text file line by line, producing String items. Supports charset encoding and compression.

### Remote
**BQQueryResultsPipe** - Part of google-bq extension. Executes a BigQuery query, and reads the result rows. The query should be specified as BQQuery object. BigQuery client fetches the result set in pages, so there's no risk of OOME. The user can set the page size using QueryExecutionConfig.

---

**DBQueryResultsPipe** - Executes a JDBC query and returns the result rows. Requires a mapper from a Jdbc ResultSet row to the corresponding output object. JDBC fetches results in pages, so there's no risk of OOME. A known weakness of this implementation is that progress isn't really supported, because JDBC doesn't provide a cheap way of determining the result set size without running an additional query.

---

**StorageMultiFileReaderPipe/AsyncStorageMultiFileReaderPipe** - The preferred and most flexible way of reading multiple files from cloud storage. Receives a StorageMultiFileReaderConfig as a parameter. This config class is constructed using the builder design pattern. It supports: applying file filters, streaming/download approach, automatic sharding, recursive reading, determining the read order between files (only sync), and determining the number of reader threads (only async). Automatic sharding is useful when we want to split the files automatically, and read only one shard (assuming that other parts of the pipeline will take care of the other shards). Sharding tries to achieve even number of files per shard by default, but can be configured to approximate even total file sizes.
The configuration class accepts the generic Bucket and storage classes, meaning that it supports any cloud storage implementation. Currently the implemented ones are google-storage (see GoogleStorage and GoogleStorageBucket in google-cs extension) and S3 (see S3 and S3Bucket in amazon-s3 extension). We also support a dummy cloud storage, which is actually local (see LocalDiskStorage and LocalDiskBucket). The latter is the recommended storage implementation for tests, since it can act as a test-double to actual cloud implementation.

---

**StorageMultiTxtFileReaderPipe** - Reads multiple text files from cloud storage (supports different implementations) in a streaming manner. This class is specific to text files, and is very limited in features compared with StorageMultiFileReaderPipe. The class GSMultiTxtFileReaderPipe (from google-cs extension) is a subclass, made specifically for Google Storage.

---

**StorageTxtFileReaderPipe** - Reads a single textual file from storage, in a streaming manner. Works with a generic storage object, meaning that it supports multiple implementations (Google, S3 and local). The subclass GSTxtFileReaderPipe (from google-cs extension) is specific to google storage. Note that streaming of a single file may become a bottleneck in your flow. Consider using StorageTxtFileFetcherReaderPipe for large files. This pipe used multithreaded optimized download of the file, and then starts reading it.

---

**URLTxtReaderPipe** - Reads text content from a remote URL location, line by line. Supports charset encoding and compression, and allows setting connection timeout and read timeout.

## Terminal Pipes
**CollectionWriterPipe/AsyncCollectionWriterPipe** - Adds items to a given Collection object. In the case of async, the caller is responsible for passing a thread safe collection.

---

**AsyncEnqueuingSharderPipe** - A terminal pipe that receives an async pipe as input, and shards the contents of the input pipe into multiple queues according to some sharding criteria based on item values. The queues are supplied at construction time, and any implementation of BlockingQueue is accepted. Pay attention to the caveats at the class documentation.

---

**ConsumerPipe/AsyncConsumerPipe** - Consumes all items from the input pipe. Optionally allows setting an item callback and a successful termination callback.

---

**ParallelConsumerPipe** - The main building block of parallel pipes. Uses a configurable number of threads T to consume items from a set of P >= T input pipes.

---

**SharderByHashPipe/AsyncSharderByHashPipe** - Deterministically distributes input items into N local files, according to the items' hash value. In the sync case, relative item order is maintained. In the async case the order isn't guaranteed. Make sure the number of target files isn't too high, specially in the case of compression, where compressors use a large amount of memory.

---

**SharderByItemPipe/AsyncSharderPipe** - Deterministically distributes input items into N local files, according to some condition on items. In the sync case, relative item order is maintained. In the async case the order isn't guaranteed. Requires setting a shard selector, which assigns a target file name to each item. The file count isn't necessarily known in advance, and files are created if needed. Make sure you don't have too many open files, specially in the case of compression, where compressors use a large amount of memory.

---

**SharderBySeqPipe** - A more resource efficient version of the two other file sharders(SharderByItemPipe and SharderByHashPipe). Suitable for cases where input items are already grouped by the shard they belong to, allowing the sharder to handle file by file.

---

**BinFileWriterPipe** - Encodes items into a local file. Receives an EncoderFactory as parameter. Supports compression. The non-terminal version of this pipe is IntermediateBinFileWriterPipe.

---

**TxtFileWriterPipe** - Writes textual items to a local file. Supports charset encoding and compression. The non-terminal version of this pipe is IntermediateTxtFileWriterPipe.

---

**StorageTxtFileWriterPipe** - Writes textual items to a cloud storage file. GSTxtFileWriterPipe (from google-cs extension) is a version of this pipe specific to Google Storage.

---

**ListFileWriterPipe** - Writes to a local file in list-file format. This is a Ubimo proprietary, general purpose binary format, supporting metadata header, fixed size blocks, minimal size overhead and compression. A list file in its general form holds byte arrays as data objects, therefore a ByteArrayEncoder is required by ListFileWriterPipe. The corresponding reader pipe is ListFileReaderPipe.

---

**ListFileProtoWriterPipe** - Writes to a local file with list-file format, just like ListFileWriterPipe, but this time assuming that data objects should be ProtoBuf encoded. The constructor receives a Descriptor, corresponding to the type of the proto objects we want to write.

---

**WriterPipe** - A general purpose text writer pipe, receiving a Writer in the constructor. Supports buffering.

---

**QueueWriterPipe** - Reads items from a synchronous pipe, and writes them to a blocking queue.

## Intermediate Pipes
### Data/multiplicity/order changing
**FilterPipe/AsyncFilterPipe** - Filters items based on some given predicate.

---

**MapPipe/AsyncMapPipe** - Maps each input item X to an output item Y, possibly with a different type.

---

**FlexibleMapPipe/AsyncFlexibleMapPipe** - Similar to MapPipe/AsyncMapPipe, but without the 1:1 limitation. For each input item it can produce 0 or more output items.

---

**HeadPipe/AsyncHeadPipe** - Keeps only the first K items from the input pipe, and drops the rest. In case of async, the K selected items are arbitrary and non deterministic.

---

**SkipPipe** - Skips the first K items from the input pipe, and keeps the rest.

---

**PortionSamplerPipe** - A special filter acting as a sampler. Each item is kept with a predefined probability p. This means that the expected number of outputs in N*p, where N is the number of items in the input pipe. Supports deterministic sampling by providing a Random object. It is strongly suggested to use com.ubimo.infra.math.FastRandom, which is optimized for single threaded usage.

---

**ExactSamplerPipe** - A special filter acting as a sampler. In contrast to PortionSamplerPipe, here we randomly and uniformly sample exactly M items from the N available items in the input. Can only be used when N is known in advance, and usually this isn't the case. Supports deterministic sampling by providing a Random object. It is strongly suggested to use com.ubimo.infra.math.FastRandom, which is optimized for single threaded usage.

---

**CSVMapperPipe** - parses CSV string items from the input pipe, and generates a for each item a map between column name and column value. The column names can either be given by the caller, or extracted from the first item.

---

**SequenceReductorPipe** - Identifies sequences in the input pipe, and aggregates them into a single output item, possibly with a different type. The logic of identifying “families” of items, aggregating them and transforming the aggregated value into an output item is defined by a ReductorConfig object passed in the constructor. ListReductorPipe is a simplified sequence reductor implementation, which collects sequences into a List, and allows the caller to specify how to convert the list to the target type. This implementation is less memory friendly, and may explode memory when sequences are too long.

---

**HashReductorPipe** - Similar to SequenceReductorPipe, but doesn’t assume anything regarding the item order in the input. Similar to SequenceReductorPipe, this pipe also  receives a ReductorConfig object which defines the aggregation logic, but unlike the former, this pipe makes use of more memory and temporary disk space, and should therefore be configured with attention to memory limits.

---

**DedupPipe** - Identifies duplicates in the input pipe, and outputs only one copy of each family of equal items. Makes use of temporary disk space and requires tuning for protecting memory usage.

---

**GrouperPipe** - Splits the input items into different families, and emits items from the same family sequentially. The order between families is arbitrary, but the ordering of items inside each family is preserved like in the input. Makes use of temporary disk space and requires tuning for protecting memory usage.

---

**SortPipe** - Sorts the items in the input pipe, according to a given Comparator. Works with chunks that are stored in a temp folder. The number of items in each chunk is configurable, and also indicates the maximum number of items we allow in memory at once. One of the constructors can be used for pure in-memory sorting, and is suitable for cases that we know that all input items can fit in memory.

---

**TopKPipe** - Given an input pipe, returns the top K items from that pipe, according to a given Comparator. Items are returned in descending order.

---

**CountPipe** - Counts the items passing through it, generating a single Integer item with that count

### Side effect only
**CallbackPipe/AsyncCallbackPipe** - Runs a callback method per item, once the input pipe terminates successfully, or both.

---

**ProgressPipe/AsyncProgressPipe** - Runs a callback method every time the progress percentage increases (int values between 0 and 100).

---

**AsyncTimeoutPipe** - An async pipe which verifies that the input pipe is fully consumed within a given duration limit.

---

**IntermediateTxtFileWriterPipe** - An intermediate version of the terminal TxtFileWriterPipe.

---

**IntermediateBinFileWriterPipe** - An intermediate version of the terminal BinFileWriterPipe.

---

**OrderValidationPipe** - Validates that items supplied by the input pipe maintain the ordering specified in a given Comparator.

---

**QueuePipe** - A synchronous pipe actively fetching items from the upstream pipe, storing in a queue, and making them available to the caller. Producer side is handled by a new, dedicated thread, while consuming is performed by the caller's thread (the thread calling start() and pull() on this pipe). The pipe breaks the flow into two parts, each handled by a different thread. For better throughput, consider using LockFreeBlockingQueue as the queue implementation.

###Combiner Pipes
**AsyncUnionPipe** - An async pipe producing items from all given async input pipes. This is only a flow structural change, allowing to "collapse" a few pipes into a single one. The items are unchanged. This is useful when we need to pass outputs of different async pipes to another pipe, but that pipe accepts a single async pipe.

---

**SyncToAsyncPipe** - Sync to async convertor. Uses K threads to pull items from P given sync pipes, and publish them asynchronously.

---

**SortedJoinPipe** - Performs a join operation between a left pipe and a list of right pipes. Left and right may have different item data type. The output items are in form of JoinRecord, and they encapsulate all items from left and right sides with the same key. In order to use this efficient join pipe, all input pipes must be sorted consistently.

---

**HashJoinPipe** - Performs a join operation between a left pipe and a list of right pipes. Has a similar API to SortedJoinPipe, but unlike it doesn't require inputs to be sorted. Uses temporary disk storage for performing the join.

---

**ConcatPipe** - Concats a given list of audiences, maintaining item order and pipe order.

---

**SortedIntersectionPipe** - A sorted set operation pipe performing set intersection on a set of input pipes. All pipes must be sorted, and the provided comparator must be consistent with equality. The pipe output is sorted and deduped.

---

**SortedUnionPipe** - A sorted set operation pipe performing set union on a set of input pipes. All pipes must be sorted, and the provided comparator must be consistent with equality. The pipe output is sorted and deduped.

---

**SortedSubtractionPipe** - A sorted set operation pipe performing set subtraction between two pipes. The two pipes must be sorted, and the provided comparator must be consistent with equality. The pipe output is sorted and deduped.

---

**SortedMergePipe** - Sort-merges a set of given pipes, providing a sorted stream of items. All input pipes must be sorted consistently, with ordering matching the given comparator. Unlike the set operation pipes, here duplicates are maintained.

[<< Prev](custom_pipes.md)  [Next >>](general_tips.md) 
