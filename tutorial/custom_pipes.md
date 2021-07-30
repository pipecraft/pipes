# Custom Pipes

When no suitable implementation exists in the framework, new sync/async pipes can be implemented. This can be done by directly implementing Pipe or AsyncPipe interfaces, but also by using proper super classes, when available. For example, FilterPipe (and AsyncFilterPipe) may be subclassed for achieving some specific filtering logic, and similarly MapPipe (and AsyncMapPipe) may be subclassed for performing a specific data transformation.

In addition, there are three abstract pipe implementations that can be useful as base pipes for custom implementations:

1. **DelegatePipe** - A sync pipe decorator, delegating all actions to the pipe given at the constructor. This is useful when the custom pipe is an intermediate pipe having one pipe with same item data type as input. The implementation overrides only methods which perform some special logic, rather than delegating the call to the input pipe.

2. **CompoundPipe** - Useful when the custom pipe can be described as composition of existing pipes. This promotes modularity, and simplifies re-use of flow parts.

3. **AsyncCompoundPipe** - Same as CompoundPipe, but for implementing compound async pipes.

Following are important guidelines for implementing sync/async pipes. All are also documented in the BasePipe/Pipe/AsyncPipe javadocs.

## General pipe implementation guidelines
1. constructors aren't allowed to start any IO or data processing. Pre-processing should be done by the start() method only.

2. close() method should dispose of all resources used by the pipe. It should also invoke the close() method on all input pipes. Closing should be an idempotent action.

3. Implement getProgress() properly: should preferably start with 0.0, and once the pipe is fully consumed it should be 1.0. Must report monotonously increasing values. This method should be thread safe - it can be invoked by any thread.

4. Any pipe having custom public getters exposing some state must make these getters thread safe.

5. Resource usage - the vast majority of existing pipes consume memory which is proportional to the number of input pipes. Accumulating data in memory is legitimate if really needed, but try to make the class memory friendly, and if not, at least support parameters setting memory usage bounds (such as with SortPipe). Whenever disk usage is used for internal purposes, specify it in the documentation, and provide a parameter for setting temp folder location.

## Sync pipe implementation
1. Must be fully thread safe.

2. When the pipe is done generating all items successfully, it should invoke the listener.done() method, exactly once (see notifyDone()).

3. When the pipe is done generating items with an error, it should invoke the listener.error(e) method, exactly once (see notifyError()).

4. Once a call to listener.done() or listener.error(e) starts, no other listener method should be invoked.

5. An error event notification received from the predecessor pipe should trigger an error event to next pipe.

6. Implementation of start() method should start up the notification mechanism. The call should exit as soon as possible, leaving all notifications (next, done, error) to other threads. This is important for preventing deadlocks.

7. A call to close() should stop the item generation mechanism as soon as possible (best effort), and block until stopping is complete. After the method exists normally, there should be no more notifications. As with any pipe, the call should also trigger a close operation on the predecessor pipes. For proper lifecycle tracking, the implementation should call the super's close() implementation.

8. For consistency and interoperability with sync pipes - items produced by async pipes should never be null.

9. Async pipes owning threads must add a final barrier protecting each thread from terminating with unexpected unchecked errors. In case of such errors, the pipe should report an error (notifyError()), using InternalPipeException as a wrapper.

## Teminal pipe implementation
1. start() calls should block until all data is consumed successfully, an interruption occurs or an error occurs.

2. start() is declared as interruptible, and is encouraged to be implemented accordingly.

3. Unless specified otherwise, terminal pipes aren't thread safe (can't call close from another thread).

4. Error handling - in case of an error, the start() method should exit as soon as possible and throw the corresponding exception (PipeException/InterruptedException/runtime exception). In case of InternalPipeException, the actual exception to throw is InternalPipeException.getRuntimeException(), since it indicates a bug and not an ordinary PipeException.

[<< Prev](sorted_set_operations.md) [Next >>](main_pipes_glossary.md)
