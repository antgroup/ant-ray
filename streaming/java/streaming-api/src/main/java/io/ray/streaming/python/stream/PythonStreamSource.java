package io.ray.streaming.python.stream;

import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.stream.StreamSource;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.PythonFunction.FunctionInterface;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

/** Represents a source of the PythonStream. */
public class PythonStreamSource extends PythonDataStream implements StreamSource {

  private PythonStreamSource(StreamingContext streamingContext, PythonFunction sourceFunction) {
    super(streamingContext, new PythonOperator(sourceFunction));
    withChainStrategy(ChainStrategy.HEAD);
  }

  public static PythonStreamSource from(
      StreamingContext streamingContext, PythonFunction sourceFunction) {
    sourceFunction.setFunctionInterface(FunctionInterface.SOURCE_FUNCTION);
    return new PythonStreamSource(streamingContext, sourceFunction);
  }

  @Override
  public PythonStreamSource disableChain() {
    return withChainStrategy(ChainStrategy.NEVER);
  }

  @Override
  public PythonStreamSource withChainStrategy(ChainStrategy chainStrategy) {
    super.withChainStrategy(chainStrategy);
    return this;
  }

  @Override
  public PythonStreamSource forward() {
    super.forward();
    return this;
  }

  @Override
  public PythonStreamSource setParallelism(int parallelism) {
    super.setParallelism(parallelism);
    return this;
  }

  @Override
  public PythonStreamSource withSchema(Schema schema) {
    super.withSchema(schema);
    return this;
  }

  @Override
  public PythonStreamSource withType(TypeInfo typeInfo) {
    super.withType(typeInfo);
    return this;
  }

  @Override
  public PythonStreamSource setDynamicDivisionNum(int dynamicDivisionNum) {
    super.setDynamicDivisionNum(dynamicDivisionNum);
    return this;
  }

  @Override
  public PythonStreamSource withConfig(Map<String, String> config) {
    super.withConfig(config);
    return this;
  }

  @Override
  public PythonStreamSource withConfig(String key, String value) {
    super.withConfig(key, value);
    return this;
  }

  @Override
  public PythonStreamSource withResource(String resourceKey, Double resourceValue) {
    super.withResource(resourceKey, resourceValue);
    return this;
  }

  @Override
  public PythonStreamSource withResource(Map<String, Double> resources) {
    super.withResource(resources);
    return this;
  }
}
