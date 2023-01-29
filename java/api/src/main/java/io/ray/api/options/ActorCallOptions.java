package io.ray.api.options;

public class ActorCallOptions {
  public final boolean ignoreReturn;
  // The variable is class of Boolean, because we shoule distinguish the value of
  // user-set.
  // For example, the value of enable_task_fast_fail is false/true, we should meet user
  // needs. However, user maybe don't konw the variable, system should distinguish false
  // and default value(null) correctly.
  public final Boolean enableTaskFastFail;

  private ActorCallOptions(boolean ignoreReturn, Boolean enableTaskFastFail) {
    this.ignoreReturn = ignoreReturn;
    this.enableTaskFastFail = enableTaskFastFail;
  }

  /** This inner class for building CallOptions. */
  public static class Builder {
    private boolean ignoreReturn;
    public Boolean enableTaskFastFail;

    public ActorCallOptions.Builder setIgnoreReturn(boolean returnVoid) {
      this.ignoreReturn = returnVoid;
      return this;
    }

    public ActorCallOptions.Builder setEnableTaskFastFail(Boolean enableTaskFastFail) {
      this.enableTaskFastFail = enableTaskFastFail;
      return this;
    }

    public ActorCallOptions build() {
      return new ActorCallOptions(ignoreReturn, enableTaskFastFail);
    }
  }
}
