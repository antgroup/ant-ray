package io.ray.streaming.python.stream;

import io.ray.streaming.api.stream.StreamSink;
import io.ray.streaming.jobgraph.Language;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.util.TypeInfo;
import org.apache.arrow.vector.types.pojo.Schema;

/** Represents a sink of the PythonStream. */
public class PythonStreamSink extends StreamSink implements PythonStream {
  public PythonStreamSink(PythonDataStream input, PythonOperator sinkOperator) {
    super(input, sinkOperator);
    getStreamingContext().addSink(this);
  }

  @Override
  public Language getLanguage() {
    return Language.PYTHON;
  }

  @Override
  public PythonStreamSink disableChain() {
    return withChainStrategy(ChainStrategy.NEVER);
  }

  @Override
  public PythonStreamSink withChainStrategy(ChainStrategy chainStrategy) {
    super.withChainStrategy(chainStrategy);
    return this;
  }

  @Override
  public PythonStreamSink forward() {
    super.forward();
    return this;
  }

  @Override
  public PythonStreamSink setParallelism(int parallelism) {
    super.setParallelism(parallelism);
    return this;
  }

  @Override
  public PythonStreamSink withSchema(Schema schema) {
    super.withSchema(schema);
    return this;
  }

  @Override
  public PythonStreamSink withType(TypeInfo typeInfo) {
    super.withType(typeInfo);
    return this;
  }

  @Override
  public PythonStreamSink setDynamicDivisionNum(int dynamicDivisionNum) {
    super.setDynamicDivisionNum(dynamicDivisionNum);
    return this;
  }

  @Override
  public PythonStreamSink withConfig(String key, String value) {
    super.withConfig(key, value);
    return this;
  }

  @Override
  public PythonStreamSink withResource(String resourceKey, Double resourceValue) {
    this.getOperator().setResource(resourceKey, resourceValue);
    return this;
  }
}
