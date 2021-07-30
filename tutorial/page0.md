![Pipes](images/pipes_main.png "Pipes")

---

Pipes is a data processing framework for Java.  Originally it has been designed to serve specific pipelines in Ubimo (A Quotient Technology Inc. brand), but today it can be considered a general purpose data processing framework. 

**Pipelines** written in Pipes are composed of one or more **flows**, where each flow consists of multiple steps (**pipes**), typically organized in a tree topology. The framework has a relatively rich set of available pipes, but the user can always add custom pipes as needed, or embed plain java processing where the pipes paradigm is less suitable.

Pipes can work on memory data, local disk data or network originated data (e.g. Amazon S3, Google Cloud Storage, DB or BigQuery). It can be used for simple scenarios such as a linear processing of items from some in-memory collection, or for much heavier tasks, such as reading Terabytes of data from the cloud, and processing them in a distributed manner.

Unlike frameworks such as Apache Beam, pipes is relatively low level, meaning that it requires the programmer to be specific when defining the logic, and to be aware of resource usage (disk space, threads or workers in a distributed case). Programmers should also understand their data volumes in order to come up with the optimal pipeline configuration, and should be able to detect weak spots and optimize them (e.g. inefficient data model or some step consuming too much CPU). 

This tutorial covers basic and advanced pipe topics, in increasing level of complexity. For illustration purposes, we use an invented catalog/consumer use case throughout the tutorial. The code examples cover many of the popular pipe implementations, but a more complete list can be found in the Glossary section.

It's recommended to read the tutorial chapter by chapter, but you may also find the following table of contents useful:

* [Itroduction](introduction.md)
* [Tutorial Case Study](tutorial_case_study.md)
* [Linear Pipes](linear_pipes.md)
* [Tree Flow](tree_flow.md)
* [Encoders/Decoders](encoders_decoders.md)
* [Parallel Pipes](parallel_pipes.md)
* [Async Pipes](async_pipes.md)
* [Distributed Pipes](distributed_pipes.md)
* [Sorted Set Operations](sorted_set_operations.md)
* [Custom Pipes](custom_pipes.md)
* [Main Pipes Glossary](main_pipes_glossary.md)
* [General Tips](general_tips.md)

[Next >>](introduction.md)
