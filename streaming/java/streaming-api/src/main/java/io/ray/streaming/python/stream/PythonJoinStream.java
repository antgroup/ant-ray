package io.ray.streaming.python.stream;

import io.ray.streaming.python.PythonOperator;

/** Represents a python DataStream returned by a join operation. */
@SuppressWarnings("unchecked")
public class PythonJoinStream extends PythonDataStream implements PythonStream {
  private PythonKeyDataStream rightStream;

  public PythonJoinStream(
      PythonDataStream inputStream,
      PythonKeyDataStream rightStream,
      PythonOperator pythonOperator) {
    super(inputStream, pythonOperator);
    this.rightStream = rightStream;
  }

  public PythonKeyDataStream getRightStream() {
    return rightStream;
  }
}
