package org.pipecraft.infra.concurrent;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

/**
 * The future implementation to be used in cases where a class is responsible for setting the Future's outcome (value/exception),
 * and the future should be a {@link ListenableFuture}.
 * 
 * Notes:
 * 1) The implementation should not expose this class through its API, and should expose it as {@link ListenableFuture}.
 * 2) This class should not be used in cases where the ListenableFuture doesn't need to be created and managed (e.g. when using {@link ListeningExecutorService}).
 * 
 * @author Eyal Schneider
 *
 * @param <T> The data type of the Future's value
 */
public class SettableListenableFuture <T> extends AbstractFuture<T> {

  // Raising the access level from protected to public
  @Override
  public boolean set(T value) {
    return super.set(value);
  }

  // Raising the access level from protected to public
  @Override
  public boolean setException(Throwable throwable) {
    return super.setException(throwable);
  }
}
