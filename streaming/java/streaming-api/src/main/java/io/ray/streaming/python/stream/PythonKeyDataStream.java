package io.ray.streaming.python.stream;

import io.ray.streaming.api.partition.impl.PythonPartitionFunction;
import io.ray.streaming.api.stream.DataStream;
import io.ray.streaming.api.stream.KeyDataStream;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.PythonFunction.FunctionInterface;
import io.ray.streaming.python.PythonJoinOperator;
import io.ray.streaming.python.PythonMergeOperator;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents a python DataStream returned by a key-by operation. */
@SuppressWarnings("unchecked")
public class PythonKeyDataStream extends PythonDataStream implements PythonStream {

  private static final Logger LOG = LoggerFactory.getLogger(PythonKeyDataStream.class);

  public PythonKeyDataStream(PythonDataStream input, PythonOperator pythonOperator) {
    super(input, pythonOperator, PythonPartitionFunction.KeyPartition);
  }

  /**
   * Create a python stream that reference passed python stream. Changes in new stream will be
   * reflected in referenced stream and vice versa
   */
  public PythonKeyDataStream(DataStream referencedStream) {
    super(referencedStream);
  }

  public PythonDataStream reduce(String moduleName, String funcName) {
    return reduce(new PythonFunction(moduleName, funcName));
  }

  /**
   * Apply a reduce function to this stream.
   *
   * @param func The reduce function.
   * @return A new DataStream.
   */
  public PythonDataStream reduce(PythonFunction func) {
    func.setFunctionInterface(FunctionInterface.REDUCE_FUNCTION);
    PythonDataStream stream = new PythonDataStream(this, new PythonOperator(func));
    stream.withChainStrategy(ChainStrategy.HEAD);
    return stream;
  }

  /**
   * Apply join transformations to this stream.
   *
   * @param func The python JoinFunction.
   * @param rightStream The right DataStreams to join with.
   * @return A new JoinStream.
   */
  public final PythonDataStream join(PythonFunction func, PythonKeyDataStream rightStream) {
    func.setFunctionInterface(FunctionInterface.JOIN_FUNCTION);
    LOG.info("Invoke the join method in PythonKeyDataStream.");
    return new PythonJoinStream(
        this, rightStream, new PythonJoinOperator(func, this.getId(), rightStream.getId()));
  }

  public final PythonDataStream merge(PythonFunction func, List<PythonKeyDataStream> rightStreams) {
    func.setFunctionInterface(FunctionInterface.MERGE_FUNCTION);
    LOG.info("Invoke the merge method in PythonKeyDataStream.");

    List<Integer> rightInputOperatorIds = new ArrayList<>();
    for (PythonKeyDataStream stream : rightStreams) {
      rightInputOperatorIds.add(stream.getId());
    }
    return new PythonMergeStream(
        this, rightStreams, new PythonMergeOperator(func, this.getId(), rightInputOperatorIds));
  }

  /**
   * Convert this stream as a java stream. The converted stream and this stream are the same logical
   * stream, which has same stream id. Changes in converted stream will be reflected in this stream
   * and vice versa.
   */
  @Override
  public KeyDataStream<Object, Object> asJavaStream() {
    return new KeyDataStream(this);
  }

  @Override
  public PythonKeyDataStream disableChain() {
    return withChainStrategy(ChainStrategy.NEVER);
  }

  @Override
  public PythonKeyDataStream withChainStrategy(ChainStrategy chainStrategy) {
    super.withChainStrategy(chainStrategy);
    return this;
  }

  @Override
  public PythonKeyDataStream forward() {
    super.forward();
    return this;
  }

  @Override
  public PythonKeyDataStream setParallelism(int parallelism) {
    super.setParallelism(parallelism);
    return this;
  }

  @Override
  public PythonKeyDataStream withSchema(Schema schema) {
    super.withSchema(schema);
    return this;
  }

  @Override
  public PythonKeyDataStream withType(TypeInfo typeInfo) {
    super.withType(typeInfo);
    return this;
  }

  @Override
  public PythonKeyDataStream setDynamicDivisionNum(int dynamicDivisionNum) {
    throw new UnsupportedOperationException("This function is undefined");
  }

  @Override
  public PythonKeyDataStream withConfig(Map<String, String> config) {
    super.withConfig(config);
    return this;
  }

  @Override
  public PythonKeyDataStream withConfig(String key, String value) {
    super.withConfig(key, value);
    return this;
  }

  @Override
  public PythonKeyDataStream withResource(String resourceKey, Double resourceValue) {
    super.withResource(resourceKey, resourceValue);
    return this;
  }

  @Override
  public PythonKeyDataStream withResource(Map<String, Double> resources) {
    super.withResource(resources);
    return this;
  }
}
