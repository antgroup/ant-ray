package io.ray.performancetest.test.latency;

import java.io.Serializable;

public class LatencyParameters implements Serializable {
  private boolean enable;
  private int tps;
  private int timeSliceMs;
  private int tpsPerTimeSlice;
  private long threshold;
  private long waitNanoTime;

  public LatencyParameters(boolean enable, int tps, int timeSliceMs) {
    this.enable = enable;
    this.tps = tps;
    this.timeSliceMs = timeSliceMs;
    if (this.enable) {
      this.tpsPerTimeSlice = this.tps / (1000 / this.timeSliceMs);
      if (this.tpsPerTimeSlice == 0) {
        this.tpsPerTimeSlice = 1;
      }

      this.waitNanoTime = (long) (1000000000 / this.tps);
    } else {
      this.tpsPerTimeSlice = 0;
    }

    this.threshold = 5L;
  }

  public LatencyParameters(boolean enable, int tps, int timeSliceMs, long threshold) {
    this(enable, tps, timeSliceMs);
    this.threshold = threshold;
  }

  public boolean isEnable() {
    return this.enable;
  }

  public int getTps() {
    return this.tps;
  }

  public int getTimeSliceMs() {
    return this.timeSliceMs;
  }

  public int getTpsPerTimeSlice() {
    return this.tpsPerTimeSlice;
  }

  public long getThreshold() {
    return this.threshold;
  }

  public long getWaitNanoTime() {
    return this.waitNanoTime;
  }

  public String toString() {
    return "latency enabled: "
        + this.isEnable()
        + " TPS: "
        + this.getTps()
        + " timeSliceMs: "
        + this.timeSliceMs
        + " tpsPerTimeSlice: "
        + this.tpsPerTimeSlice
        + " waitNanoTime: "
        + this.waitNanoTime;
  }
}
