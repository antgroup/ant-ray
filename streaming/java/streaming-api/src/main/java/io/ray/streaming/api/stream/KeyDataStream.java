package io.ray.streaming.api.stream;

import io.ray.streaming.api.function.impl.AggregateFunction;
import io.ray.streaming.api.function.impl.ReduceFunction;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.api.partition.impl.KeyPartition;
import io.ray.streaming.api.window.Window;
import io.ray.streaming.api.window.assigner.WindowAssigner;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.impl.AggregateOperator;
import io.ray.streaming.operator.impl.ReduceOperator;
import io.ray.streaming.python.stream.PythonDataStream;
import io.ray.streaming.python.stream.PythonKeyDataStream;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeResolver;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Represents a DataStream returned by a key-by operation.
 *
 * @param <K> Type of the key.
 * @param <T> Type of the data.
 */
@SuppressWarnings("unchecked")
public class KeyDataStream<K, T> extends DataStream<T> {

  public KeyDataStream(DataStream<T> input, AbstractStreamOperator streamOperator) {
    super(input, streamOperator, (Partition<T>) new KeyPartition<K, T>());
  }

  /**
   * Create a java stream that reference passed python stream. Changes in new stream will be
   * reflected in referenced stream and vice versa
   */
  public KeyDataStream(PythonDataStream referencedStream) {
    super(referencedStream);
  }

  /**
   * Apply a reduce function to this stream.
   *
   * @param reduceFunction The reduce function.
   * @return A new DataStream.
   */
  public DataStream<T> reduce(ReduceFunction<T> reduceFunction) {
    Class<?>[] reduceTypeArguments =
        TypeResolver.resolveRawArguments(ReduceFunction.class, reduceFunction.getClass());
    return new DataStream<>(this, new ReduceOperator(reduceFunction, reduceTypeArguments));
  }

  /**
   * Apply an aggregate Function to this stream.
   *
   * @param aggregateFunction The aggregate function
   * @param <A> The type of aggregated intermediate data.
   * @param <O> The type of result data.
   * @return A new DataStream.
   */
  public <A, O> DataStream<O> aggregate(AggregateFunction<T, A, O> aggregateFunction) {
    return new DataStream<>(this, new AggregateOperator(aggregateFunction));
  }

  /**
   * Assign a window to get a {@link KeyWindowStream}.
   *
   * @param windowAssigner specified winodow
   * @return {@link KeyWindowStream}
   */
  public <R, W extends Window> KeyWindowStream<K, T, R, W> window(
      WindowAssigner<K, T, W> windowAssigner) {
    return new KeyWindowStream<>(this, windowAssigner);
  }

  /**
   * Convert this stream as a python stream. The converted stream and this stream are the same
   * logical stream, which has same stream id. Changes in converted stream will be reflected in this
   * stream and vice versa.
   */
  @Override
  public PythonKeyDataStream asPythonStream() {
    return new PythonKeyDataStream(this);
  }

  @Override
  public KeyDataStream<K, T> disableChain() {
    return withChainStrategy(ChainStrategy.NEVER);
  }

  @Override
  public KeyDataStream<K, T> withChainStrategy(ChainStrategy chainStrategy) {
    super.withChainStrategy(chainStrategy);
    return this;
  }

  @Override
  public KeyDataStream<K, T> forward() {
    super.forward();
    return this;
  }

  @Override
  public KeyDataStream<K, T> setParallelism(int parallelism) {
    super.setParallelism(parallelism);
    return this;
  }

  @Override
  public KeyDataStream<K, T> setPartition(Partition<T> partition) {
    super.setPartition(partition);
    return this;
  }

  @Override
  public KeyDataStream<K, T> withSchema(Schema schema) {
    super.withSchema(schema);
    return this;
  }

  @Override
  public KeyDataStream<K, T> withType(TypeInfo typeInfo) {
    super.withType(typeInfo);
    return this;
  }

  @Override
  public KeyDataStream<K, T> setDynamicDivisionNum(int dynamicDivisionNum) {
    throw new UnsupportedOperationException("This function is undefined");
  }

  @Override
  public KeyDataStream<K, T> withConfig(Map<String, String> config) {
    super.withConfig(config);
    return this;
  }

  @Override
  public KeyDataStream<K, T> withConfig(String key, String value) {
    super.withConfig(key, value);
    return this;
  }

  @Override
  public KeyDataStream<K, T> withResource(String resourceKey, Double resourceValue) {
    super.withResource(resourceKey, resourceValue);
    return this;
  }

  @Override
  public KeyDataStream<K, T> withResource(Map<String, Double> resources) {
    super.withResource(resources);
    return this;
  }
}
