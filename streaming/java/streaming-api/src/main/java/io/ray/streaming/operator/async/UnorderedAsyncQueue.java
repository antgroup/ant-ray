package io.ray.streaming.operator.async;

import com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unordered async queue used in async operator.
 *
 * @param <T> Type of the input data of {@link AsyncElement}.
 * @param <R> Type of the output data of {@link AsyncElement}.
 */
public class UnorderedAsyncQueue<T, R> implements AsyncQueue<T, R> {

  private static final Logger LOG = LoggerFactory.getLogger(UnorderedAsyncQueue.class);

  private final int capacity;
  private final ExecutorService executorService;
  private final ArrayDeque<AsyncElement<T, R>> uncompletedQueue;
  private final ArrayDeque<AsyncElement<T, R>> completedQueue;
  private final ReentrantLock lock;
  private final Condition queueNotFull;
  private final Condition anyElementCompleted;

  public UnorderedAsyncQueue(int capacity, ExecutorService executorService) {
    Preconditions.checkArgument(capacity > 0, "capacity must be greater than 0");
    Preconditions.checkNotNull(executorService);
    this.capacity = capacity;
    this.executorService = executorService;
    this.uncompletedQueue = new ArrayDeque<>(capacity);
    this.completedQueue = new ArrayDeque<>(capacity);
    this.lock = new ReentrantLock();
    this.queueNotFull = lock.newCondition();
    this.anyElementCompleted = lock.newCondition();
  }

  @Override
  public int size() {
    return uncompletedQueue.size();
  }

  @Override
  public boolean isEmpty() {
    return uncompletedQueue.isEmpty();
  }

  @Override
  public boolean isFull() {
    return uncompletedQueue.size() >= capacity;
  }

  @Override
  public void add(AsyncElement<T, R> asyncElement) throws InterruptedException {
    lock.lockInterruptibly();
    try {
      while (isFull()) {
        queueNotFull.await();
      }
      uncompletedQueue.addLast(asyncElement);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Add a new element to the uncompleted queue, uncompleted queue's size is [{}/{}],"
                + " completed queue's size is [{}/{}].",
            uncompletedQueue.size(),
            capacity,
            completedQueue.size(),
            capacity);
      }
      asyncElement.onCompleted(
          ae -> {
            try {
              completedHandler(ae);
            } catch (InterruptedException e) {
              LOG.warn(e.getMessage(), e);
            }
          },
          executorService);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public AsyncElement<T, R> peek() throws InterruptedException {
    lock.lockInterruptibly();
    try {
      while (completedQueue.isEmpty()) {
        anyElementCompleted.await();
      }
      return completedQueue.peek();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public AsyncElement<T, R> poll() throws InterruptedException {
    lock.lockInterruptibly();
    try {
      while (completedQueue.isEmpty()) {
        anyElementCompleted.await();
      }
      AsyncElement<T, R> element = completedQueue.poll();
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Poll an element from the completed queue, queue's size is [{}/{}].",
            completedQueue.size(),
            capacity);
      }
      queueNotFull.signalAll();
      return element;
    } finally {
      lock.unlock();
    }
  }

  private void completedHandler(AsyncElement<T, R> ae) throws InterruptedException {
    lock.lockInterruptibly();
    try {
      uncompletedQueue.poll();
      completedQueue.offer(ae);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Finished an element, uncompleted queue's size is [{}/{}], "
                + "completed queue's size is [{}/{}].",
            uncompletedQueue.size(),
            capacity,
            completedQueue.size(),
            capacity);
      }
      anyElementCompleted.signalAll();
    } finally {
      lock.unlock();
    }
  }
}
