package org.pipecraft.infra.monitoring.sampling;

import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * A sample textualizer for throwable objects. Displays the full stack trace and the message.
 *
 * @author Eyal Schneider
 */
public class ThrowableTextualizer implements SampleTextualizer<Throwable>{

  @Override
  public String toText(Throwable e) { 
    if (e == null)
      return "null";
    return ExceptionUtils.getStackTrace(e);
  }
}
