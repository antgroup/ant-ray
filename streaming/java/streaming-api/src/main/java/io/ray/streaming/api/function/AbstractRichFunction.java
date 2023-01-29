package io.ray.streaming.api.function;

import io.ray.streaming.api.context.RuntimeContext;

/**
 * Alipay.com Inc Copyright (c) 2004-2019 All Rights Reserved.
 *
 * @author yangjianzhang on 2020-06-02.
 */
public abstract class AbstractRichFunction implements RichFunction {

  private RuntimeContext runtimeContext;

  @Override
  public void open(RuntimeContext runtimeContext) {
    this.runtimeContext = runtimeContext;
  }

  /**
   * Gets the context that contains information about the function runtime, such as the parallelism
   * of the function, the subtask index of the function, the job config and function config. It is
   * also the entrance to get the state.
   *
   * @return runtime context.
   */
  public RuntimeContext getRuntimeContext() {
    if (null != this.runtimeContext) {
      return this.runtimeContext;
    } else {
      throw new IllegalStateException("The runtime context has not been open.");
    }
  }
}
