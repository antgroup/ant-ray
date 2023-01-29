package io.ray.streaming.api.stream;

import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.SourceFunction;
import io.ray.streaming.api.function.internal.CollectionSourceFunction;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.impl.SourceOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.Collection;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Represents a source of the DataStream.
 *
 * @param <T> The type of StreamSource data.
 */
public class DataStreamSource<T> extends DataStream<T> implements StreamSource<T> {

  private DataStreamSource(StreamingContext streamingContext, SourceFunction<T> sourceFunction) {
    super(streamingContext, new SourceOperator<>(sourceFunction));
  }

  public static <T> DataStreamSource<T> fromSource(
      StreamingContext context, SourceFunction<T> sourceFunction) {
    return new DataStreamSource<>(context, sourceFunction);
  }

  /**
   * Build a DataStreamSource source from a collection.
   *
   * @param context Stream context.
   * @param values A collection of values.
   * @param <T> The type of source data.
   * @return A DataStreamSource.
   */
  public static <T> DataStreamSource<T> fromCollection(
      StreamingContext context, Collection<T> values) {
    return new DataStreamSource<>(context, new CollectionSourceFunction<>(values));
  }

  @Override
  public DataStreamSource<T> disableChain() {
    return withChainStrategy(ChainStrategy.NEVER);
  }

  @Override
  public DataStreamSource<T> withChainStrategy(ChainStrategy chainStrategy) {
    super.withChainStrategy(chainStrategy);
    return this;
  }

  @Override
  public DataStreamSource<T> forward() {
    super.forward();
    return this;
  }

  @Override
  public DataStreamSource<T> setParallelism(int parallelism) {
    super.setParallelism(parallelism);
    return this;
  }

  @Override
  public DataStreamSource<T> setPartition(Partition<T> partition) {
    super.setPartition(partition);
    return this;
  }

  @Override
  public DataStreamSource<T> withSchema(Schema schema) {
    super.withSchema(schema);
    return this;
  }

  @Override
  public DataStreamSource<T> withType(TypeInfo typeInfo) {
    super.withType(typeInfo);
    return this;
  }

  @Override
  public DataStreamSource<T> setDynamicDivisionNum(int dynamicDivisionNum) {
    super.setDynamicDivisionNum(dynamicDivisionNum);
    return this;
  }

  @Override
  public DataStreamSource<T> withConfig(Map<String, String> config) {
    super.withConfig(config);
    return this;
  }

  @Override
  public DataStreamSource<T> withConfig(String key, String value) {
    super.withConfig(key, value);
    return this;
  }

  @Override
  public DataStreamSource<T> withResource(String resourceKey, Double resourceValue) {
    super.withResource(resourceKey, resourceValue);
    return this;
  }

  @Override
  public DataStreamSource<T> withResource(Map<String, Double> resources) {
    super.withResource(resources);
    return this;
  }
}
