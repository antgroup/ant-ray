package io.ray.streaming.python.stream;

import io.ray.streaming.python.PythonOperator;
import java.util.List;

/** Represents a python DataStream returned by a merge operation. */
@SuppressWarnings("unchecked")
public class PythonMergeStream extends PythonDataStream implements PythonStream {
  private List<PythonKeyDataStream> rightStreams;

  public PythonMergeStream(
      PythonDataStream inputStream,
      List<PythonKeyDataStream> rightStreams,
      PythonOperator pythonOperator) {
    super(inputStream, pythonOperator);
    this.rightStreams = rightStreams;
  }

  public List<PythonKeyDataStream> getRightStreams() {
    return rightStreams;
  }
}
