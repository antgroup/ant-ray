package io.ray.streaming.api.stream;

import com.google.common.base.Preconditions;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.async.AsyncFunction;
import io.ray.streaming.api.function.impl.FilterFunction;
import io.ray.streaming.api.function.impl.FlatMapFunction;
import io.ray.streaming.api.function.impl.KeyFunction;
import io.ray.streaming.api.function.impl.MapFunction;
import io.ray.streaming.api.function.impl.ProcessFunction;
import io.ray.streaming.api.function.impl.SinkFunction;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.api.partition.impl.BroadcastPartitionFunction;
import io.ray.streaming.jobgraph.Language;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.async.AsyncOperator;
import io.ray.streaming.operator.async.AsyncQueue;
import io.ray.streaming.operator.impl.ArrowOperator;
import io.ray.streaming.operator.impl.FilterOperator;
import io.ray.streaming.operator.impl.FlatMapOperator;
import io.ray.streaming.operator.impl.KeyByOperator;
import io.ray.streaming.operator.impl.MapOperator;
import io.ray.streaming.operator.impl.ProcessOperator;
import io.ray.streaming.operator.impl.SinkOperator;
import io.ray.streaming.python.stream.PythonDataStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

/**
 * Represents a stream of data.
 *
 * <p>This class defines all the streaming operations.
 *
 * @param <T> Type of data in the stream.
 */
public class DataStream<T> extends Stream<DataStream<T>, T> {

  public DataStream(StreamingContext streamingContext, AbstractStreamOperator streamOperator) {
    super(streamingContext, streamOperator);
  }

  public DataStream(
      StreamingContext streamingContext,
      AbstractStreamOperator streamOperator,
      Partition<T> partition) {
    super(streamingContext, streamOperator, partition);
  }

  public <R> DataStream(DataStream<R> input, AbstractStreamOperator streamOperator) {
    super(input, streamOperator);
  }

  public <R> DataStream(
      DataStream<R> input, AbstractStreamOperator streamOperator, Partition<T> partition) {
    super(input, streamOperator, partition);
  }

  /**
   * Create a java stream that reference passed python stream. Changes in new stream will be
   * reflected in referenced stream and vice versa
   */
  public DataStream(PythonDataStream referencedStream) {
    super(referencedStream);
  }

  /**
   * Apply a async function to this stream.
   *
   * @param asyncFunction The async io function.
   * @param type Type of async io which is ordered or unordered.
   * @param capacity Capacity of async queue
   * @param timeout Time of async io in millisecond.
   * @param <R> Type of data returned by the async function.
   * @return A new DataStream.
   */
  public <R> DataStream<R> async(
      AsyncFunction<T, R> asyncFunction, AsyncQueue.OrderType type, int capacity, long timeout) {
    Preconditions.checkArgument(asyncFunction != null, "AsyncFunction must not be null!");
    return new DataStream<>(this, new AsyncOperator(asyncFunction, type, capacity, timeout));
  }

  /**
   * Apply a async function to this stream and guarantee the input record order.
   *
   * @param asyncFunction The async io function.
   * @param capacity Capacity of async queue
   * @param timeout Time of async io in millisecond.
   * @param <R> Type of data returned by the async function.
   * @return A new DataStream.
   */
  public <R> DataStream<R> asyncOrdered(
      AsyncFunction<T, R> asyncFunction, int capacity, long timeout) {
    Preconditions.checkArgument(asyncFunction != null, "AsyncFunction must not be null!");
    return new DataStream<>(
        this, new AsyncOperator(asyncFunction, AsyncQueue.OrderType.ORDERED, capacity, timeout));
  }

  /**
   * Apply a async function to this stream but do not guarantee the input record order.
   *
   * @param asyncFunction The async io function.
   * @param capacity Capacity of async queue
   * @param timeout Time of async io in millisecond.
   * @param <R> Type of data returned by the async function.
   * @return A new DataStream.
   */
  public <R> DataStream<R> asyncUnordered(
      AsyncFunction<T, R> asyncFunction, int capacity, long timeout) {
    Preconditions.checkArgument(asyncFunction != null, "AsyncFunction must not be null!");
    return new DataStream<>(
        this, new AsyncOperator(asyncFunction, AsyncQueue.OrderType.UNORDERED, capacity, timeout));
  }

  /**
   * Apply a map function to this stream.
   *
   * @param mapFunction The map function.
   * @param <R> Type of data returned by the map function.
   * @return A new DataStream.
   */
  public <R> DataStream<R> map(MapFunction<T, R> mapFunction) {
    return new DataStream<>(this, new MapOperator<>(mapFunction));
  }

  /**
   * Apply a flat-map function to this stream.
   *
   * @param flatMapFunction The FlatMapFunction
   * @param <R> Type of data returned by the flatmap function.
   * @return A new DataStream
   */
  public <R> DataStream<R> flatMap(FlatMapFunction<T, R> flatMapFunction) {
    return new DataStream<>(this, new FlatMapOperator<>(flatMapFunction));
  }

  public DataStream<T> filter(FilterFunction<T> filterFunction) {
    return new DataStream<>(this, new FilterOperator<>(filterFunction));
  }

  /**
   * Apply union transformations to this stream by merging {@link DataStream} outputs of the same
   * type with each other.
   *
   * @param stream The DataStream to union output with.
   * @param others The other DataStreams to union output with.
   * @return A new UnionStream.
   */
  @SafeVarargs
  public final DataStream<T> union(DataStream<T> stream, DataStream<T>... others) {
    List<DataStream<T>> streams = new ArrayList<>();
    streams.add(stream);
    streams.addAll(Arrays.asList(others));
    return union(streams);
  }

  /**
   * Apply union transformations to this stream by merging {@link DataStream} outputs of the same
   * type with each other.
   *
   * @param streams The DataStreams to union output with.
   * @return A new UnionStream.
   */
  public final DataStream<T> union(List<DataStream<T>> streams) {
    if (this instanceof UnionStream) {
      UnionStream<T> unionStream = (UnionStream<T>) this;
      streams.forEach(unionStream::addStream);
      return unionStream;
    } else {
      return new UnionStream<>(this, streams);
    }
  }

  /**
   * Apply a join transformation to this stream, with another stream.
   *
   * @param other Another stream.
   * @param <O> The type of the other stream data.
   * @param <R> The type of the data in the joined stream.
   * @return A new JoinStream.
   */
  public <O, R> JoinStream<T, O, R> join(DataStream<O> other) {
    return new JoinStream<>(this, other);
  }

  public <R> DataStream<R> process(ProcessFunction<T, R> processFunction) {
    return new DataStream<>(this, new ProcessOperator<>(processFunction));
  }

  /**
   * Apply a sink function and get a StreamSink.
   *
   * @param sinkFunction The sink function.
   * @return A new StreamSink.
   */
  public DataStreamSink<T> sink(SinkFunction<T> sinkFunction) {
    return new DataStreamSink<>(this, new SinkOperator<>(sinkFunction));
  }

  /**
   * Apply a key-by function to this stream.
   *
   * @param keyFunction the key function.
   * @param <K> The type of the key.
   * @return A new KeyDataStream.
   */
  public <K> KeyDataStream<K, T> keyBy(KeyFunction<T, K> keyFunction) {
    checkPartitionCall();
    return new KeyDataStream<>(this, new KeyByOperator<>(keyFunction));
  }

  /**
   * Apply broadcast to this stream.
   *
   * @return This stream.
   */
  public DataStream<T> broadcast() {
    checkPartitionCall();
    return setPartition(new BroadcastPartitionFunction<>());
  }

  /**
   * Apply a partition to this stream.
   *
   * @param partition The partitioning strategy.
   * @return This stream.
   */
  public DataStream<T> partitionBy(Partition<T> partition) {
    checkPartitionCall();
    return setPartition(partition);
  }

  /**
   * If parent stream is a python stream, we can't call partition related methods in the java
   * stream.
   */
  private void checkPartitionCall() {
    if (getInputStream() != null && getInputStream().getLanguage() == Language.PYTHON) {
      throw new RuntimeException(
          "Partition related methods can't be called on a "
              + "java stream if parent stream is a python stream.");
    }
  }

  /**
   * Convert a stream to arrow record batch stream. Note the data type in this stream must be a
   * `POJO`
   *
   * @param batchSize arrow record batch size if there is no timeout
   * @param timeoutMilliseconds Finish an arrow record batch if there is no enough data in specified
   *     timeout time duration.
   */
  public DataStream<ArrowRecordBatch> toArrowStream(int batchSize, long timeoutMilliseconds) {
    return new DataStream<>(this, new ArrowOperator(getOperator(), batchSize, timeoutMilliseconds));
  }

  /**
   * Convert this stream as a python stream. The converted stream and this stream are the same
   * logical stream, which has same stream id. Changes in converted stream will be reflected in this
   * stream and vice versa.
   */
  public PythonDataStream asPythonStream() {
    return new PythonDataStream(this);
  }

  @Override
  public Language getLanguage() {
    return Language.JAVA;
  }
}
