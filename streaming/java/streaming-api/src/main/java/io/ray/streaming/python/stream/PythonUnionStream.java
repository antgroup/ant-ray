package io.ray.streaming.python.stream;

import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.python.PythonOperator;
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
 */
public class PythonUnionStream extends PythonDataStream {
  private List<PythonDataStream> unionStreams;

  public PythonUnionStream(PythonDataStream input, List<PythonDataStream> others) {
    // Union stream does not create a physical operation, so we don't have to set partition
    // function for it.
    super(input, new PythonOperator(PythonOperator.OPERATOR_MODULE, "UnionOperator"));
    this.unionStreams = new ArrayList<>();
    others.forEach(this::addStream);
  }

  void addStream(PythonDataStream stream) {
    if (stream instanceof PythonUnionStream) {
      this.unionStreams.addAll(((PythonUnionStream) stream).getUnionStreams());
    } else {
      this.unionStreams.add(stream);
    }
  }

  public List<PythonDataStream> getUnionStreams() {
    return unionStreams;
  }

  @Override
  public PythonUnionStream disableChain() {
    return withChainStrategy(ChainStrategy.NEVER);
  }

  @Override
  public PythonUnionStream withChainStrategy(ChainStrategy chainStrategy) {
    super.withChainStrategy(chainStrategy);
    return this;
  }

  @Override
  public PythonUnionStream forward() {
    super.forward();
    return this;
  }

  @Override
  public PythonUnionStream setParallelism(int parallelism) {
    super.setParallelism(parallelism);
    return this;
  }

  @Override
  public PythonUnionStream withSchema(Schema schema) {
    super.withSchema(schema);
    return this;
  }

  @Override
  public PythonUnionStream withType(TypeInfo typeInfo) {
    super.withType(typeInfo);
    return this;
  }

  @Override
  public PythonUnionStream withConfig(Map<String, String> config) {
    super.withConfig(config);
    return this;
  }

  @Override
  public PythonUnionStream withConfig(String key, String value) {
    super.withConfig(key, value);
    return this;
  }

  @Override
  public PythonUnionStream setDynamicDivisionNum(int dynamicDivisionNum) {
    super.setDynamicDivisionNum(dynamicDivisionNum);
    return this;
  }

  @Override
  public PythonUnionStream withResource(String resourceKey, Double resourceValue) {
    super.withResource(resourceKey, resourceValue);
    return this;
  }

  @Override
  public PythonUnionStream withResource(Map<String, Double> resources) {
    super.withResource(resources);
    return this;
  }
}
