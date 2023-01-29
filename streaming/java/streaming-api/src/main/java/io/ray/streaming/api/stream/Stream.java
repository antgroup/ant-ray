package io.ray.streaming.api.stream;

import com.google.common.base.Preconditions;
import io.ray.fury.types.TypeInference;
import io.ray.fury.util.Tuple2;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.api.partition.impl.ForwardPartition;
import io.ray.streaming.api.partition.impl.PythonPartitionFunction;
import io.ray.streaming.jobgraph.Language;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeUtils;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class of all stream types.
 *
 * @param <S> Type of stream class.
 * @param <T> Type of the data in the stream.
 */
public abstract class Stream<S extends Stream<S, T>, T> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(Stream.class);

  private final int id;
  private final StreamingContext streamingContext;
  protected Stream inputStream;
  private AbstractStreamOperator operator;
  private int parallelism = 1;
  private Map<String, String> config = new HashMap<>();
  private Partition<T> partition;
  private Stream originalStream;
  private String name;
  // The default value indicates edges grouping by the greatest common divisor.
  private int dynamicDivisionNum = 0;

  public Stream(StreamingContext streamingContext, AbstractStreamOperator streamOperator) {
    this(streamingContext, null, streamOperator, getForwardPartition(streamOperator));
  }

  public Stream(
      StreamingContext streamingContext,
      AbstractStreamOperator streamOperator,
      Partition<T> partition) {
    this(streamingContext, null, streamOperator, partition);
  }

  public Stream(Stream inputStream, AbstractStreamOperator streamOperator) {
    this(
        inputStream.getStreamingContext(),
        inputStream,
        streamOperator,
        getForwardPartition(streamOperator));
  }

  public Stream(Stream inputStream, AbstractStreamOperator streamOperator, Partition<T> partition) {
    this(inputStream.getStreamingContext(), inputStream, streamOperator, partition);
  }

  protected Stream(
      StreamingContext streamingContext,
      Stream inputStream,
      AbstractStreamOperator streamOperator,
      Partition<T> partition) {
    this.streamingContext = streamingContext;
    this.inputStream = inputStream;
    this.operator = streamOperator;
    this.partition = partition;
    this.id = streamingContext.generateId();
    if (inputStream != null) {
      this.parallelism = inputStream.getParallelism();
    }
    if (operator.hasTypeInfo()) {
      if (TypeInference.isBean(operator.getTypeInfo().getType())) {
        Tuple2<Schema, String> either = TypeUtils.tryInferSchema(operator.getTypeInfo().getType());
        if (either.f1 != null) {
          LOG.info("Can't infer schema for data type {}: {}", operator.getTypeInfo(), either.f1);
        } else {
          operator.setSchema(either.f0);
        }
      }
    }
  }

  /**
   * Create a proxy stream of original stream. Changes in new stream will be reflected in original
   * stream and vice versa
   */
  protected Stream(Stream originalStream) {
    this.originalStream = originalStream;
    this.id = originalStream.getId();
    this.streamingContext = originalStream.getStreamingContext();
    this.inputStream = originalStream.getInputStream();
    this.operator = originalStream.getOperator();
    operator.setName(originalStream.getName());
    Preconditions.checkNotNull(operator);
  }

  @SuppressWarnings("unchecked")
  private static <T> Partition<T> getForwardPartition(StreamOperator operator) {
    switch (operator.getLanguage()) {
      case PYTHON:
        return (Partition<T>) PythonPartitionFunction.ForwardPartition;
      case JAVA:
        return new ForwardPartition<>();
      default:
        throw new UnsupportedOperationException("Unsupported language " + operator.getLanguage());
    }
  }

  public int getId() {
    return id;
  }

  public StreamingContext getStreamingContext() {
    return streamingContext;
  }

  public Stream getInputStream() {
    return inputStream;
  }

  public AbstractStreamOperator getOperator() {
    return operator;
  }

  public void setOperator(AbstractStreamOperator operator) {
    this.operator = operator;
  }

  @SuppressWarnings("unchecked")
  private S self() {
    return (S) this;
  }

  public int getParallelism() {
    return originalStream != null ? originalStream.getParallelism() : parallelism;
  }

  public S setParallelism(int parallelism) {
    if (originalStream != null) {
      originalStream.setParallelism(parallelism);
    } else {
      this.parallelism = parallelism;
    }
    return self();
  }

  @SuppressWarnings("unchecked")
  public Partition<T> getPartition() {
    return originalStream != null ? originalStream.getPartition() : partition;
  }

  @SuppressWarnings("unchecked")
  protected S setPartition(Partition<T> partition) {
    if (originalStream != null) {
      originalStream.setPartition(partition);
    } else {
      this.partition = partition;
    }
    return self();
  }

  /**
   * Sets the number of edge groups. It will set a default value at run time based on the greatest
   * common divisor of source vertex's partition and target vertex's partition if this is not set.
   */
  public S setDynamicDivisionNum(int dynamicDivisionNum) {
    Preconditions.checkArgument(
        dynamicDivisionNum > 0, "The number of groups must be greater than 0");
    this.dynamicDivisionNum = dynamicDivisionNum;
    return self();
  }

  public int getDynamicDivisionNum() {
    return this.dynamicDivisionNum;
  }

  public S withConfig(Map<String, String> config) {
    config.forEach(this::withConfig);
    return self();
  }

  public S withConfig(String key, String value) {
    if (isProxyStream()) {
      originalStream.withConfig(key, value);
    } else {
      operator.getOpConfig().put(key, value);
    }
    return self();
  }

  @SuppressWarnings("unchecked")
  public Map<String, String> getConfig() {
    return isProxyStream() ? originalStream.getConfig() : operator.getOpConfig();
  }

  public boolean isProxyStream() {
    return originalStream != null;
  }

  public Stream getOriginalStream() {
    Preconditions.checkArgument(isProxyStream());
    return originalStream;
  }

  /** Set chain strategy for this stream */
  public S withChainStrategy(ChainStrategy chainStrategy) {
    Preconditions.checkArgument(!isProxyStream());
    operator.setChainStrategy(chainStrategy);
    return self();
  }

  /** Disable chain for this stream */
  public S disableChain() {
    return withChainStrategy(ChainStrategy.NEVER);
  }

  /**
   * Set the partition function of this {@link Stream} so that output elements are forwarded to next
   * operator locally.
   */
  public S forward() {
    return setPartition(getForwardPartition(operator));
  }

  public abstract Language getLanguage();

  @SuppressWarnings("unchecked")
  public <S1 extends Stream> S1 asStream(TypeInfo<S1> streamInfo) {
    // TODO(chaokunyang) extract typeinfo from `streamInfo` and set typeinfo using `withType`
    return (S1) this;
  }

  public TypeInfo getType() {
    Preconditions.checkArgument(
        operator.hasTypeInfo(), "Type information can't inferred out, please specify manually.");
    return operator.getTypeInfo();
  }

  public S withType(TypeInfo typeInfo) {
    operator.setTypeInfo(typeInfo);
    if (TypeInference.isBean(typeInfo.getType())) {
      Tuple2<Schema, String> either = TypeUtils.tryInferSchema(typeInfo.getType());
      if (either.f1 != null) {
        LOG.info("Can't infer schema for data type {}: {}", operator.getTypeInfo(), either.f1);
      } else {
        operator.setSchema(either.f0);
      }
    }
    return self();
  }

  public S withName(String name) {
    LOG.info("Set customer name: {} for the stream: {}", name, getId());
    this.name = name;
    operator.setName(name);
    return self();
  }

  public String getName() {
    return name;
  }

  /** @return Schema of data in this stream */
  public Schema getSchema() {
    if (isProxyStream()) {
      return getOriginalStream().getSchema();
    } else {
      if (!operator.hasSchema()) {
        throw new IllegalStateException(
            "Schema information can't be inferred, please specify manually.");
      }
      return operator.getSchema();
    }
  }

  /** Set the schema of data in this stream */
  public S withSchema(Schema schema) {
    this.operator.setSchema(schema);
    return self();
  }

  public S withResource(String resourceKey, Double resourceValue) {
    this.operator.setResource(resourceKey, resourceValue);
    return self();
  }

  public S withResource(Map<String, Double> resources) {
    if (resources != null && !resources.isEmpty()) {
      this.operator.setResource(resources);
    }
    return self();
  }

  public Map getResource() {
    return this.operator.getResource();
  }
}
