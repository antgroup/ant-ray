package io.ray.api.call;

import io.ray.api.options.ActorCallOptions;

public class BaseActorTaskCaller<T extends BaseActorTaskCaller<T>> {
  private ActorCallOptions.Builder builder = new ActorCallOptions.Builder();

  public T setIgnoreReturn(boolean returnVoid) {
    builder.setIgnoreReturn(returnVoid);
    return self();
  }

  public T setEnableTaskFastFail(Boolean enableTaskFastFail) {
    builder.setEnableTaskFastFail(enableTaskFastFail);
    return self();
  }

  protected ActorCallOptions buildOptions() {
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }
}
