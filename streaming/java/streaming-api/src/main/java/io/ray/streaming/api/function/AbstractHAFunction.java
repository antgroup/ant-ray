package io.ray.streaming.api.function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Basic implementation for {@link HAFunction}. */
public abstract class AbstractHAFunction implements HAFunction {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHAFunction.class);
  private static final String COMMAND_ACTIVE = "active";

  protected boolean active;

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public void activate() {
    LOG.info("Activate the function.");
    active = true;
  }

  @Override
  public void deactivate() {
    LOG.info("Deactivate the function.");
    active = false;
  }

  @Override
  public void forwardCommand(String commandMessage) {
    if (COMMAND_ACTIVE.equals(commandMessage)) {
      activate();
    } else {
      deactivate();
    }
  }
}
