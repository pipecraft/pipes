# Linear Pipes

## Generating a random catalog
In this example we will generate a random catalog, with consecutive, increasing product ids. The product categories are chosen at random from 10 possible options ("Cat0" to "Cat9"), and prices are random and uniformly distributed integers between 1000 and 10,000 cents.

```java
private static void generateCatalog(File targetFile) throws IOException, PipeException, InterruptedException {
    Random rnd = ThreadLocalRandom.current();
    try (
        Pipe<Product> genP = new SeqGenPipe<>(
            i -> new Product(i.intValue(), "Cat" + rnd.nextInt(10), rnd.nextInt(9001) + 1000),
            100_000); // Generate 100k items
        Pipe<String> mapP = new MapPipe<>(genP, Product::toCSV);
        TerminalPipe writerP = new TxtFileWriterPipe(mapP, targetFile)) {
      writerP.start();
    }
}
```

This linear pipeline has 3 pipes in a chain. The source pipe is of type **SeqGenPipe** which is used to generate a sequence of user defined items. The user has to define a function from the index (a long starting from 0) to the required item to produce. The second parameter defines the total number of items to produce. When not specified, the sequence is practically infinite. This pipe is classical for tests.

Next is a **MapPipe** which is a very popular pipe used for doing some 1:1 transformation of items passing through it. In this case we simply convert a Product object to its CSV representation.
The last pipe (**TxtFileWriterPipe**) is a terminal pipe, expecting input items to be strings, and writing them to a file on local disk. In this example the file is written with default charset encoding (UTF8), and no compression, but the user can set both if needed. Currently supported compression schemes are Zstd and Gzip.

## Reading the catalog
Now we would like to traverse the catalog in a "manual" manner, i.e. iterator style. 
Here we use **TxtFileReaderPipe**, which is the counterpart of the **TxtFileWriterPipe** used before. This time we use the mapper pipe for decoding items from text rather than encoding them as text. Note that the sink pipe (mapP) isn't a terminal pipe, which means that the caller is responsible for actively pulling items from it, using the next() method.

> As you can see here, a pipe is totally inactive until the start() method is invoked on the sink pipe. No data processing takes place before this call. Pipes can be built, attached to other pipes and moved around, but only the call to start() makes them ready to start supplying items. Trying to pull items from a non started pipe will produce undefined behavior.

```java
private static void walkCatalog(File catalogFile) throws IOException, PipeException, InterruptedException {
    try (
        Pipe<String> readerP = new TxtFileReaderPipe(catalogFile);
        Pipe<Product> mapP = new MapPipe<>(readerP, Product::fromCSV)) {
      mapP.start();
      Product product;
      while ((product = mapP.next()) != null) {
        // Perform any item processing here
      }
    }
}
```

> Unlike iterators which require alternately calling next() and hasNext(), pipes API simplifies this, and requires items to always be non-null. A null value returned by next() always means end of pipe data. Subsequent calls to next() have no effect and also return null.

[<< Prev](tutorial_case_study.md) [Next >>](tree_flow.md)
