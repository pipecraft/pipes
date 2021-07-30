# Introduction

## Overview
We will start with some definitions:

**Pipe** - The basic building block of the framework. A pipe is an "atomic" data processor, performing some action on data items passing through it, such as transforming them, aggregating them, writing them to disk or just updating some memory state without altering them at all. The framework offers many general purpose pipe implementations, and allows users to implement their own pipes. 

**Source/Intermediate/Terminal pipes** - Pipes which aren't designed to accept predecessor pipes and are only responsible for feeding the flow with items are called **source pipes**. Similarly, a pipe which consumes data items and isn't able to produce any item is called a **terminal pipe**. Terminal pipes are typically used for generating outputs (disk, cloud, screen, etc) . Pipes which both consume items and produce items are referred to as **intermediate pipes**.

**Flow** - A collection of pipes organized in a reverse tree topology, where each pipe has 0 or more predecessors. A flow always has 1 or more source pipes feeding data into it, and a single **sink pipe**. The pipe used as sink is usually a terminal pipe, but not necessarily. The user may choose an intermediate/source pipe, if she/he wishes to loop over the sink pipe's output items manually (iterator style). The sink is the "representative pipe" of the flow, and is used for triggering it.

**Flow Topology** - Although in the general case a flow has a tree shape, many simple cases can be implemented as a linear flow.

**Pipeline** - The top level, executable entity doing the data processing as a whole. A pipeline includes one or more flows doing the core part, but can also include plain java code around them. It's common for pipelines to also include CLI classes, data classes and utilities. In addition, pipelines sometimes have a python script above them, doing the necessary environment preparations and triggering them.

**Data Publishing Type** - Non-terminal pipes can be categorized as **synchronous** or **asynchronous**, depending on how they produce their output items. Basically, sync pipes require the caller to pull items from them, while async pipes produce items actively and notify the successor pipe.

## Pipes Advantages
**Wide range of uses** - In its simplest form, one can use Pipe<T> as if it was an [Iterator](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Iterator.html) over items of type T (including small advantages such as checked exceptions support, Pipe.peek() operation and progress reporting, which aren't part of Iterator API). When chaining pipes, one can achieve a linear processing behavior similar to [Java streams](https://www.oracle.com/technical-resources/articles/java/ma14-java-se-8-streams.html), enjoying a richer variety of operators. A more complex usage would be a single/multi threaded pipeline with a tree topology, and in its most powerful configuration, the pipeline can be distributed, running on multiple machines and splitting the work between them.

**Lightweight** - No special runner is needed to execute pipes, and no special resources are needed. A single threaded pipeline can run on the caller's thread, with very small processing overhead.

**Easy transition to distributed form** - Promoting a pipeline to be distributed usually requires adding a shuffler pipe and doing local changes to the existing pipe. No major re-design is required. 

**Testability** - Designing an API which receives inputs in form of pipes makes these APIs easier to test, since pipes can be easily mocked.

**Sorted data** - Pipes supports efficient operations on sorted streams 

**Error handling** - Pipe implementations (and the logic injected to them by the caller) are encouraged to throw PipeException (and subclasses) to indicate a problem which isn't necessarily a bug. This behavior works well with the conventional approach of reserving runtime exceptions for real bugs. 

**Progress monitoring** - Any pipe can be queried for progress (0.0 - 1.0) at any moment, by any thread.

[<< Prev](page0.md) [Next >>](tutorial_case_study.md) 
