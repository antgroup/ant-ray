package io.ray.streaming.api.window.timer;

/** Interface for processing-time task's callback function. */
public interface ProcessingTimeCallback {

  /**
   * Invoke when the specified processing time is triggered.
   *
   * @param time specified processing time
   */
  void onProcessingTime(long time) throws Exception;
}
