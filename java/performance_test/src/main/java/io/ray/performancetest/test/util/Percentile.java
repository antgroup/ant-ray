package io.ray.performancetest.test.util;

import java.util.PriorityQueue;

public class Percentile {
  private int capacity;
  private PriorityQueue<Long> minHeap;

  public Percentile(int capacity) {
    this.capacity = capacity;
    this.minHeap = new PriorityQueue(capacity);
  }

  public void add(long value) {
    if (this.minHeap.size() < this.capacity) {
      this.minHeap.offer(value);
    } else if (value > (Long) this.minHeap.peek()) {
      this.minHeap.poll();
      this.minHeap.offer(value);
    }
  }

  public long getResult() {
    return (Long) this.minHeap.peek();
  }
}
