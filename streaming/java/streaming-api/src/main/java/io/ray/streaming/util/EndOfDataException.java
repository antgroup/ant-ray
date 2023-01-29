package io.ray.streaming.util;

/** EndOfDataException will be thrown when the source has no data to fetch and emit. */
public class EndOfDataException extends RuntimeException {
  public EndOfDataException() {
    super("EndOfData");
  }
}
