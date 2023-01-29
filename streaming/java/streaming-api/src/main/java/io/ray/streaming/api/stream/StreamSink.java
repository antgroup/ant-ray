package io.ray.streaming.api.stream;

import io.ray.streaming.operator.AbstractStreamOperator;

/**
 * Represents a sink of the Stream.
 *
 * @param <T> Type of the input data of this sink.
 */
public abstract class StreamSink<T> extends Stream<StreamSink<T>, T> {
  public StreamSink(Stream inputStream, AbstractStreamOperator streamOperator) {
    super(inputStream, streamOperator);
  }
}
