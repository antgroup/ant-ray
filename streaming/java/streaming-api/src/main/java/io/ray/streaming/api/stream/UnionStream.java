package io.ray.streaming.api.stream;

import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.impl.UnionOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Represents a union DataStream.
 *
 * <p>This stream does not create a physical operation, it only affects how upstream data are
 * connected to downstream data.
 *
 * @param <T> The type of union data.
 */
public class UnionStream<T> extends DataStream<T> {
  private List<DataStream<T>> unionStreams;

  public UnionStream(DataStream<T> input, List<DataStream<T>> streams) {
    // Union stream does not create a physical operation, so we don't have to set partition
    // function for it.
    super(input, new UnionOperator(input.getOperator()));
    this.unionStreams = new ArrayList<>();
    streams.forEach(this::addStream);
  }

  void addStream(DataStream<T> stream) {
    if (stream instanceof UnionStream) {
      this.unionStreams.addAll(((UnionStream<T>) stream).getUnionStreams());
    } else {
      this.unionStreams.add(stream);
    }
  }

  public List<DataStream<T>> getUnionStreams() {
    return unionStreams;
  }

  @Override
  public UnionStream<T> disableChain() {
    return withChainStrategy(ChainStrategy.NEVER);
  }

  @Override
  public UnionStream<T> withChainStrategy(ChainStrategy chainStrategy) {
    super.withChainStrategy(chainStrategy);
    return this;
  }

  @Override
  public UnionStream<T> forward() {
    super.forward();
    return this;
  }

  @Override
  public UnionStream<T> setParallelism(int parallelism) {
    super.setParallelism(parallelism);
    return this;
  }

  @Override
  public UnionStream<T> setPartition(Partition<T> partition) {
    super.setPartition(partition);
    return this;
  }

  @Override
  public UnionStream<T> withSchema(Schema schema) {
    super.withSchema(schema);
    return this;
  }

  @Override
  public UnionStream<T> withType(TypeInfo typeInfo) {
    super.withType(typeInfo);
    return this;
  }

  @Override
  public UnionStream<T> setDynamicDivisionNum(int dynamicDivisionNum) {
    super.setDynamicDivisionNum(dynamicDivisionNum);
    return this;
  }

  @Override
  public UnionStream<T> withConfig(Map<String, String> config) {
    super.withConfig(config);
    return this;
  }

  @Override
  public UnionStream<T> withConfig(String key, String value) {
    super.withConfig(key, value);
    return this;
  }

  @Override
  public UnionStream<T> withResource(String resourceKey, Double resourceValue) {
    super.withResource(resourceKey, resourceValue);
    return this;
  }

  @Override
  public UnionStream<T> withResource(Map<String, Double> resources) {
    super.withResource(resources);
    return this;
  }
}
