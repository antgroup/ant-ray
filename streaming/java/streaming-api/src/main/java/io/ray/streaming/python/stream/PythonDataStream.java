package io.ray.streaming.python.stream;

import static io.ray.streaming.python.PythonFunction.FUNCTION_MODULE;
import static io.ray.streaming.python.PythonOperator.OPERATOR_MODULE;

import com.google.common.base.Preconditions;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.api.partition.impl.PythonPartitionFunction;
import io.ray.streaming.api.stream.DataStream;
import io.ray.streaming.api.stream.Stream;
import io.ray.streaming.jobgraph.Language;
import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.PythonFunction.FunctionInterface;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Represents a stream of data whose transformations will be executed in python. */
public class PythonDataStream extends Stream<PythonDataStream, Object> implements PythonStream {

  protected PythonDataStream(StreamingContext streamingContext, PythonOperator pythonOperator) {
    super(streamingContext, pythonOperator);
  }

  protected PythonDataStream(
      StreamingContext streamingContext,
      PythonOperator pythonOperator,
      Partition<Object> partition) {
    super(streamingContext, pythonOperator, partition);
  }

  public PythonDataStream(PythonDataStream input, PythonOperator pythonOperator) {
    super(input, pythonOperator);
  }

  public PythonDataStream(
      PythonDataStream input, PythonOperator pythonOperator, Partition<Object> partition) {
    super(input, pythonOperator, partition);
  }

  /**
   * Create a python stream that reference passed java stream. Changes in new stream will be
   * reflected in referenced stream and vice versa
   */
  public PythonDataStream(DataStream referencedStream) {
    super(referencedStream);
  }

  public PythonDataStream map(String moduleName, String funcName) {
    return map(new PythonFunction(moduleName, funcName));
  }

  /**
   * Apply a map function to this stream.
   *
   * @param func The python MapFunction.
   * @return A new PythonDataStream.
   */
  public PythonDataStream map(PythonFunction func) {
    func.setFunctionInterface(FunctionInterface.MAP_FUNCTION);
    return new PythonDataStream(this, new PythonOperator(func));
  }

  public PythonDataStream process(String moduleName, String funcName) {
    return process(new PythonFunction(moduleName, funcName));
  }

  public PythonDataStream process(PythonFunction func) {
    func.setFunctionInterface(FunctionInterface.PROCESS_FUNCTION);
    return new PythonDataStream(this, new PythonOperator(func));
  }

  public PythonDataStream flatMap(String moduleName, String funcName) {
    return flatMap(new PythonFunction(moduleName, funcName));
  }

  /**
   * Apply a flat-map function to this stream.
   *
   * @param func The python FlapMapFunction.
   * @return A new PythonDataStream
   */
  public PythonDataStream flatMap(PythonFunction func) {
    func.setFunctionInterface(FunctionInterface.FLAT_MAP_FUNCTION);
    return new PythonDataStream(this, new PythonOperator(func));
  }

  public PythonDataStream filter(String moduleName, String funcName) {
    return filter(new PythonFunction(moduleName, funcName));
  }

  /**
   * Apply a filter function to this stream.
   *
   * @param func The python FilterFunction.
   * @return A new PythonDataStream that contains only the elements satisfying the given filter
   *     predicate.
   */
  public PythonDataStream filter(PythonFunction func) {
    func.setFunctionInterface(FunctionInterface.FILTER_FUNCTION);
    return new PythonDataStream(this, new PythonOperator(func));
  }

  /**
   * Apply union transformations to this stream by merging {@link PythonDataStream} outputs of the
   * same type with each other.
   *
   * @param stream The DataStream to union output with.
   * @param others The other DataStreams to union output with.
   * @return A new UnionStream.
   */
  public final PythonDataStream union(PythonDataStream stream, PythonDataStream... others) {
    List<PythonDataStream> streams = new ArrayList<>();
    streams.add(stream);
    streams.addAll(Arrays.asList(others));
    return union(streams);
  }

  /**
   * Apply union transformations to this stream by merging {@link PythonDataStream} outputs of the
   * same type with each other.
   *
   * @param streams The DataStreams to union output with.
   * @return A new UnionStream.
   */
  public final PythonDataStream union(List<PythonDataStream> streams) {
    if (this instanceof PythonUnionStream) {
      PythonUnionStream unionStream = (PythonUnionStream) this;
      streams.forEach(unionStream::addStream);
      return unionStream;
    } else {
      return new PythonUnionStream(this, streams);
    }
  }

  public PythonStreamSink sink(String moduleName, String funcName) {
    return sink(new PythonFunction(moduleName, funcName));
  }

  /**
   * Apply a sink function and get a StreamSink.
   *
   * @param func The python SinkFunction.
   * @return A new StreamSink.
   */
  public PythonStreamSink sink(PythonFunction func) {
    func.setFunctionInterface(FunctionInterface.SINK_FUNCTION);
    return new PythonStreamSink(this, new PythonOperator(func));
  }

  public PythonKeyDataStream keyBy(String moduleName, String funcName) {
    return keyBy(new PythonFunction(moduleName, funcName));
  }

  /**
   * Apply a key-by function to this stream.
   *
   * @param func the python keyFunction.
   * @return A new KeyDataStream.
   */
  public PythonKeyDataStream keyBy(PythonFunction func) {
    checkPartitionCall();
    func.setFunctionInterface(FunctionInterface.KEY_FUNCTION);
    return new PythonKeyDataStream(this, new PythonOperator(func));
  }

  /**
   * Apply broadcast to this stream.
   *
   * @return This stream.
   */
  public PythonDataStream broadcast() {
    checkPartitionCall();
    return setPartition(PythonPartitionFunction.BroadcastPartition);
  }

  /**
   * Apply a partition to this stream.
   *
   * @param partition The partitioning strategy.
   * @return This stream.
   */
  public PythonDataStream partitionBy(PythonPartitionFunction partition) {
    checkPartitionCall();
    return setPartition(partition);
  }

  /**
   * If parent stream is a python stream, we can't call partition related methods in the java
   * stream.
   */
  private void checkPartitionCall() {
    if (getInputStream() != null && getInputStream().getLanguage() == Language.JAVA) {
      throw new RuntimeException(
          "Partition related methods can't be called on a "
              + "python stream if parent stream is a java stream.");
    }
  }

  /**
   * Convert a stream to arrow record batch stream. Note the data in this stream must have a schema.
   *
   * @param batchSize arrow record batch size if there is no timeout
   * @param timeoutMilliseconds Finish an arrow record batch if there is no enough data in specified
   *     timeout time duration.
   */
  public PythonDataStream toArrowStream(int batchSize, long timeoutMilliseconds) {
    Preconditions.checkArgument(batchSize > 0);
    Preconditions.checkArgument(timeoutMilliseconds > 0);
    return new PythonDataStream(this, new PythonOperator(OPERATOR_MODULE, "ArrowOperator"))
        .withConfig(OPERATOR_MODULE + ".ArrowOperator.batchSize", String.valueOf(batchSize))
        .withConfig(
            OPERATOR_MODULE + ".ArrowOperator.timeoutMilliseconds",
            String.valueOf(timeoutMilliseconds));
  }

  /**
   * Convert input stream to stream of arrow record batch, then convert it to stream of pandas
   * DataFrame.
   *
   * @see #toArrowStream(int, long)
   */
  public PythonDataStream toPandasStream(int batchSize, long timeoutMilliseconds) {
    return toArrowStream(batchSize, timeoutMilliseconds)
        .map(OPERATOR_MODULE, "ArrowToPandasFunction");
  }

  /**
   * Convert a stream to a stream of pandas DataFrame. Input stream needs to be a stream of arrow
   * record batch.
   */
  public PythonDataStream toPandasStream() {
    Preconditions.checkArgument(
        getOperator() instanceof PythonOperator,
        "Input stream needs to be an arrow stream to be converted to stream of " + "DataFrame");
    Preconditions.checkArgument(
        "ArrowOperator".equals(((PythonOperator) getOperator()).getClassName()),
        "Input stream needs to be an arrow stream to be converted to stream of " + "DataFrame");
    return map(FUNCTION_MODULE, "ArrowToPandasFunction");
  }

  /**
   * Convert this stream as a java stream. The converted stream and this stream are the same logical
   * stream, which has same stream id. Changes in converted stream will be reflected in this stream
   * and vice versa.
   */
  public DataStream<Object> asJavaStream() {
    return new DataStream<>(this);
  }

  public <T> DataStream<T> asJavaStream(Class<T> type) {
    return asJavaStream(new TypeInfo<>(type));
  }

  public <T> DataStream<T> asJavaStream(TypeInfo<T> type) {
    DataStream<T> dataStream = new DataStream<>(this);
    dataStream.withType(type);
    return dataStream;
  }

  @Override
  public Language getLanguage() {
    return Language.PYTHON;
  }
}
