package io.ray.streaming.api.stream;

import io.ray.streaming.api.function.impl.AggregateFunction;
import io.ray.streaming.api.function.impl.KeyFunction;
import io.ray.streaming.api.function.impl.ReduceFunction;
import io.ray.streaming.api.function.impl.window.AggregateApplyKeyWindowFunction;
import io.ray.streaming.api.function.impl.window.ReduceApplyKeyWindowFunction;
import io.ray.streaming.api.function.impl.window.WindowFunction;
import io.ray.streaming.api.window.Window;
import io.ray.streaming.api.window.assigner.WindowAssigner;
import io.ray.streaming.operator.impl.KeyWindowOperator;
import io.ray.streaming.util.TypeResolver;
import java.util.Map;

/**
 * A special kind of key stream when a {@link KeyDataStream} is assigned window by {@link
 * WindowAssigner} using {@link KeyDataStream#window(WindowAssigner)}.
 *
 * @param <K> key type
 * @param <T> input data type
 * @param <R> output data type
 * @param <W> window type
 */
public class KeyWindowStream<K, T, R, W extends Window> extends AbstractWindowStream<K, T, R, W> {

  private final KeyDataStream<K, T> input;

  private Class<K> keyClazz;
  private Class<T> elementClazz;
  private Class<W> windowClazz;

  public KeyWindowStream(KeyDataStream<K, T> input, WindowAssigner<K, T, W> windowAssigner) {
    super(input, windowAssigner);
    this.input = input;
    Class<?>[] keyValueTypes =
        TypeResolver.resolveRawArguments(
            KeyFunction.class, input.getOperator().getFunction().getClass());
    keyClazz = (Class<K>) keyValueTypes[1];
    elementClazz = (Class<T>) keyValueTypes[0];

    Class<?>[] windowTypes =
        TypeResolver.resolveRawArguments(WindowAssigner.class, windowAssigner.getClass());
    windowClazz = (Class<W>) windowTypes[1];
  }

  @Override
  public <K, R> DataStream<R> apply(WindowFunction<K, T, R, W> windowFunction) {
    KeyWindowOperator<K, T, R, W> keyWindowOperator =
        new KeyWindowOperator(windowFunction, windowAssigner, trigger);
    keyWindowOperator.registerClazz(windowClazz, (Class<K>) keyClazz, elementClazz);
    return new DataStream<>(input, keyWindowOperator);
  }

  @Override
  public DataStream<T> reduce(ReduceFunction<T> reduceFunction) {
    return apply(new ReduceApplyKeyWindowFunction<>(reduceFunction));
  }

  @Override
  public <A, R> DataStream<R> aggregate(AggregateFunction<T, A, R> aggFunction) {
    return apply(new AggregateApplyKeyWindowFunction<>(aggFunction));
  }

  @Override
  public KeyWindowStream<K, T, R, W> withResource(String resourceKey, Double resourceValue) {
    super.withResource(resourceKey, resourceValue);
    return this;
  }

  @Override
  public KeyWindowStream<K, T, R, W> withResource(Map<String, Double> resources) {
    super.withResource(resources);
    return this;
  }

  @Override
  public KeyWindowStream<K, T, R, W> withConfig(Map<String, String> config) {
    super.withConfig(config);
    return this;
  }

  @Override
  public KeyWindowStream<K, T, R, W> withConfig(String key, String value) {
    super.withConfig(key, value);
    return this;
  }

  @Override
  public KeyWindowStream<K, T, R, W> setParallelism(int parallelism) {
    super.setParallelism(parallelism);
    return this;
  }
}
