# Pipes

Pipes is a simple, lightweight data processing framework for Java.
This repo comes with the core part plus three extensions (For Google Big Query, Google cloud storage and Amazon S3 storage).
For more information please read the [tutorial](tutorial/page0.md).

This project is licensed under the terms of the MIT license.

# Installation

Pipes library jars are released via Maven Central Repository. The current version is 0.91.

Core artifact:

```xml
<dependency>
  <groupId>org.pipecraft.pipes</groupId>
  <artifactId>pipes-core</artifactId>
  <version>0.91</version>
</dependency>
```

Optional extensions:

```xml
<dependency>
  <groupId>org.pipecraft.pipes</groupId>
  <artifactId>pipes-google-cs</artifactId>
  <version>0.91</version>
</dependency>

<dependency>
  <groupId>org.pipecraft.pipes</groupId>
  <artifactId>pipes-google-bq</artifactId>
  <version>0.91</version>
</dependency>

<dependency>
  <groupId>org.pipecraft.pipes</groupId>
  <artifactId>pipes-amazon-s3</artifactId>
  <version>0.91</version>
</dependency>
```

