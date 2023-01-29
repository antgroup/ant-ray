package io.ray.streaming.api.stream;

import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.jobgraph.Language;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.impl.SinkOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Represents a sink of the DataStream.
 *
 * @param <T> Type of the input data of this sink.
 */
public class DataStreamSink<T> extends StreamSink<T> {

  public DataStreamSink(DataStream input, SinkOperator sinkOperator) {
    super(input, sinkOperator);
    getStreamingContext().addSink(this);
  }

  @Override
  public Language getLanguage() {
    return Language.JAVA;
  }

  @Override
  public DataStreamSink<T> disableChain() {
    return withChainStrategy(ChainStrategy.NEVER);
  }

  @Override
  public DataStreamSink<T> withChainStrategy(ChainStrategy chainStrategy) {
    super.withChainStrategy(chainStrategy);
    return this;
  }

  @Override
  public DataStreamSink<T> forward() {
    super.forward();
    return this;
  }

  @Override
  public DataStreamSink<T> setParallelism(int parallelism) {
    super.setParallelism(parallelism);
    return this;
  }

  @Override
  public DataStreamSink<T> setPartition(Partition<T> partition) {
    super.setPartition(partition);
    return this;
  }

  @Override
  public DataStreamSink<T> withSchema(Schema schema) {
    super.withSchema(schema);
    return this;
  }

  @Override
  public DataStreamSink<T> withType(TypeInfo typeInfo) {
    super.withType(typeInfo);
    return this;
  }

  @Override
  public DataStreamSink<T> setDynamicDivisionNum(int dynamicDivisionNum) {
    super.setDynamicDivisionNum(dynamicDivisionNum);
    return this;
  }

  @Override
  public DataStreamSink<T> withConfig(Map<String, String> config) {
    super.withConfig(config);
    return this;
  }

  @Override
  public DataStreamSink<T> withConfig(String key, String value) {
    super.withConfig(key, value);
    return this;
  }

  @Override
  public DataStreamSink<T> withResource(String resourceKey, Double resourceValue) {
    super.withResource(resourceKey, resourceValue);
    return this;
  }

  @Override
  public DataStreamSink<T> withResource(Map<String, Double> resources) {
    super.withResource(resources);
    return this;
  }
}
