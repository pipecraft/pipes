package org.pipecraft.pipes.sync.inter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.utils.PipeSupplier;

/**
 * An intermediate pipe consisting of concatenating the contents of a series of pipes
 * 
 * @author Eyal Schneider
 */
public class ConcatPipe<T> implements Pipe<T> {
  private final List<PipeSupplier<T>> inputs;
  private volatile int currPipeIndex;
  private volatile Pipe<T> currPipe;
  private T next;

  /**
   * Constructor
   * 
   * @param inputs A list of pipe generators for the pipes to be concatenated, in the required concatenation order. 
   * The usage of pipe suppliers instead of ready-to-use pipes allows building the pipes one by one, instead of allocating all resources at once.
   */
  public ConcatPipe(List<PipeSupplier<T>> inputs) {
    this.inputs = inputs;
  }

  /**
   * Constructor
   * 
   * @param inputs A list of pipe generators for the pipes to be concatenated, in the required concatenation order.
   * The usage of pipe suppliers instead of ready-to-use pipes allows building the pipes one by one, instead of allocating all resources at once. 
   */
  @SuppressWarnings("unchecked")
  public ConcatPipe(PipeSupplier<T> ... inputs) {
    this(Arrays.asList(inputs));
  }

  @Override
  public void close() throws IOException {
    if (currPipe != null) {
      currPipe.close();
    }
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    T toReturn = next;
    prepareNext();
    return toReturn;
  }

  @Override
  public T peek() {
    return next;
  }

  /**
   * @param inputs A list of pipes to be concatenated, in the required concatenation order. 
   * @return A {@link ConcatPipe} initialized with the given input pipes
   */
  public static <T> ConcatPipe<T> fromPipes(List<Pipe<T>> inputs) {
    return new ConcatPipe<>(inputs.stream().map(p -> (PipeSupplier<T>)(() -> p)).collect(Collectors.toList()));
  }

  /**
   * @param inputs An array of pipes to be concatenated, in the required concatenation order. 
   * @return A {@link ConcatPipe} initialized with the given input pipes
   */
  @SafeVarargs
  public static <T> ConcatPipe<T> fromPipes(Pipe<T> ... inputs) {
    return fromPipes(Arrays.asList(inputs));
  }

  private void prepareNext() throws PipeException, InterruptedException {
    try {
      while (currPipeIndex < inputs.size()) {
        next = currPipe.next();
        if (next == null) {
          currPipe.close();
          if (++currPipeIndex < inputs.size()) {
            currPipe = inputs.get(currPipeIndex).get();
            currPipe.start();  
          }
        } else {
          break;
        }
      }
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }
  
  @Override
  public void start() throws PipeException, InterruptedException {
    try {
      if (inputs.size() > 0) {
        // Start only the first pipe. The rest are started lazily when reaching them.
        currPipe = inputs.get(0).get();
        currPipe.start();
        prepareNext();
      }
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

  @Override
  public float getProgress() {
    if (inputs.size() == 0) {
      return 1.0f;
    }
    return (float) Math.min(1.0, (currPipeIndex + currPipe.getProgress()) / (double)inputs.size());
  }
}
