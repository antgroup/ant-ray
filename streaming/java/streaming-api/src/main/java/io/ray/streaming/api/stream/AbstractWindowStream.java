package io.ray.streaming.api.stream;

import io.ray.streaming.api.function.impl.AggregateFunction;
import io.ray.streaming.api.function.impl.ReduceFunction;
import io.ray.streaming.api.function.impl.window.WindowFunction;
import io.ray.streaming.api.window.Window;
import io.ray.streaming.api.window.assigner.WindowAssigner;
import io.ray.streaming.api.window.trigger.Trigger;
import io.ray.streaming.operator.AbstractStreamOperator;

/**
 * Abstract window stream. Including: key-window and all-window.
 *
 * @param <T> input data type
 * @param <W> window type
 */
public abstract class AbstractWindowStream<K, T, R, W extends Window> extends DataStream<T> {

  protected final WindowAssigner<K, T, W> windowAssigner;
  protected Trigger<K, T, W> trigger;

  public AbstractWindowStream(
      DataStream<T> input,
      AbstractStreamOperator operator,
      WindowAssigner<K, T, W> windowAssigner) {
    super(input, operator);
    this.windowAssigner = windowAssigner;
  }

  public AbstractWindowStream(DataStream<T> input, WindowAssigner<K, T, W> windowAssigner) {
    this(input, input.getOperator(), windowAssigner);
  }

  public AbstractWindowStream<K, T, R, W> trigger(Trigger<K, T, W> trigger) {
    this.trigger = trigger;
    return this;
  }

  public abstract <K, R> DataStream<R> apply(WindowFunction<K, T, R, W> windowFunction);

  public abstract DataStream<T> reduce(ReduceFunction<T> reduceFunction);

  public abstract <A, R> DataStream<R> aggregate(AggregateFunction<T, A, R> aggregateFunction);
}
