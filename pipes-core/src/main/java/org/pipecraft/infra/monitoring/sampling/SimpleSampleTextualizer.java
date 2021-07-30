package org.pipecraft.infra.monitoring.sampling;

/**
 * A sample textualizer that uses the items toString().
 *
 * @author Eyal Schneider
 */
public class SimpleSampleTextualizer<T> implements SampleTextualizer<T>{

  @Override
  public String toText(T obj) {    
    return String.valueOf(obj);
  }
}
