# Sorted Set Operations

Some pipes are designed to work on sorted inputs, allowing them to do an efficient single pass over the input data, without disk usage and without special memory requirements. One of them is SortedJoinPipe, which was covered in one of the previous sections. In this section we will present another family of such pipes, which are simpler to use and designed to do set operations - union, intersection and subtraction.

All inputs of these pipes are required to be sorted (otherwise am OutOfOrderPipeException is thrown), but in return we get an efficient execution and a sorted output. Input pipes may have duplicates, but since inputs are treated as sets duplicates are ignored, and the output is always includes unique items.

All 3 existing implementations (**SortedUnionPipe**, **SortedIntersectionPipe** and **SortedSubtractionPipe**) extend the abstract **MultiSortedBasePipe**, which can be easily subclassed for creating additional sorted set operations.

> When creating set operation pipes, the caller is required to define a Comparator by which input items are ordered. This comparator has stronger requirements than the ones required by SortPipe or SortedJoinPipe. Here we require the comparator to be consistent with equality, meaning that for any two items x and y, comparator.compare(x,y)==0 if and only if x.equals(y). Failing to do so may cause incomplete/duplicate results to be returned.

Following is an example that receives two customer segments (lists of customer ids of interest) and one exclusion segment, and returns all customer ids which are in either of the first two segments, but not in the exclusion set. In set theory terminology, this can be described as "(S1 union S2) minus S3".

```java
 public static void getCustomerSegment(File segment1File, File segment2File, File excludeFile) throws IOException, PipeException, InterruptedException {
    try (
        Pipe<String> seg1P = new TxtFileReaderPipe(segment1File);
        Pipe<String> seg2P = new TxtFileReaderPipe(segment2File);
        Pipe<String> excludeP = new TxtFileReaderPipe(excludeFile);
        Pipe<String> unionP = new SortedUnionPipe<>(String::compareTo, seg1P, seg2P);
        Pipe<String> subtractP = new SortedSubtractionPipe<>(unionP, excludeP, String::compareTo);
        TerminalPipe consumerP = new ConsumerPipe<>(subtractP, c -> System.out.println(c));
        ) {
      consumerP.start();
    }
  }
```

These basic pipes can even be used to build a flow dynamically in order to achieve any given set operation. The resulting flow topology will then be isomorphic to the [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree) representing the set operation.

[<< Prev](distributed_pipes.md) [Next >>](custom_pipes.md)
