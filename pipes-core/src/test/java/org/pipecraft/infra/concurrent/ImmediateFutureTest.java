package org.pipecraft.infra.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.Test;

/**
 * Tests {@link ImmediateFuture} 
 * 
 * @author Eyal Schneider
 *
 */
public class ImmediateFutureTest {

  @Test
  public void testValue() {
    CheckedFuture<?, IOException> f = ImmediateFuture.ofError(new IOException("Test"));
    assertThrows(IOException.class, () -> f.checkedGet());
    
    CheckedFuture<?, RuntimeException> f2 = ImmediateFuture.ofError(new RuntimeException("Test"));
    assertThrows(RuntimeException.class, () -> f2.checkedGet());
  }

  @Test
  public void testException() throws Exception {
    CheckedFuture<Integer, Exception> f = ImmediateFuture.ofValue(15);
    assertEquals(15, f.checkedGet());

    CheckedFuture<Integer, Exception> f2 = ImmediateFuture.ofValue(null);
    assertNull(f2.checkedGet());
  }
}
