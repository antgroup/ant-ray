package io.ray.streaming.api.window;

public class WindowUtil {

  /**
   * Get the window start time by time and offset.
   *
   * @param timestamp epoch millisecond to get the window start
   * @param offset the offset which window start would be shifted by
   * @param windowSize the time size of the window
   * @return window start time
   */
  public static long getWindowStartTimeByOffset(long timestamp, long offset, long windowSize) {
    return timestamp - (timestamp - offset + windowSize) % windowSize;
  }

  /**
   * Get the window start time by time and start time.
   *
   * @param timestamp input time
   * @param windowSize size of the window
   * @return window start time
   */
  public static long getWindowStartTimeByStartTime(
      long timestamp, long startTime, long windowSize) {
    long windowNum = (timestamp - startTime) / windowSize;
    if (timestamp >= startTime) {
      return startTime + windowNum * windowSize;
    }
    return startTime + (Math.abs(windowNum) - 1) * windowSize;
  }
}
