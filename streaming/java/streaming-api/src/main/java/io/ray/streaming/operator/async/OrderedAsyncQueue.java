package io.ray.streaming.operator.async;

import com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An ordered async queue used in async operator.
 *
 * @param <T> Type of the input data of {@link AsyncElement}.
 * @param <R> Type of the output data of {@link AsyncElement}.
 */
public class OrderedAsyncQueue<T, R> implements AsyncQueue<T, R> {

  private static final Logger LOG = LoggerFactory.getLogger(OrderedAsyncQueue.class);

  private final int capacity;
  private final ExecutorService executorService;
  private final ArrayDeque<AsyncElement<T, R>> queue;
  private final ReentrantLock lock;
  private final Condition queueNotFull;
  private final Condition headCompleted;

  public OrderedAsyncQueue(int capacity, ExecutorService executorService) {
    Preconditions.checkArgument(capacity > 0, "capacity must be greater than 0");
    Preconditions.checkNotNull(executorService);
    this.capacity = capacity;
    this.executorService = executorService;
    this.queue = new ArrayDeque<>(capacity);
    this.lock = new ReentrantLock();
    this.queueNotFull = lock.newCondition();
    this.headCompleted = lock.newCondition();
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public boolean isFull() {
    return queue.size() >= capacity;
  }

  @Override
  public void add(AsyncElement<T, R> asyncElement) throws InterruptedException {
    lock.lockInterruptibly();
    try {
      while (isFull()) {
        queueNotFull.await();
      }
      queue.addLast(asyncElement);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Add a new element to the queue, now queue'size is [{}/{}].", queue.size(), capacity);
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
      while (!isHeadElementDone()) {
        headCompleted.await();
      }
      return queue.peek();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public AsyncElement<T, R> poll() throws InterruptedException {
    lock.lockInterruptibly();
    try {
      while (!isHeadElementDone()) {
        headCompleted.await();
      }
      AsyncElement<T, R> element = queue.poll();
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Poll an element from the queue, now queue's size is [{}/{}].", queue.size(), capacity);
      }
      queueNotFull.signalAll();
      return element;
    } finally {
      lock.unlock();
    }
  }

  private boolean isHeadElementDone() {
    return !queue.isEmpty() && queue.peek().isDone();
  }

  private void completedHandler(AsyncElement<T, R> ae) throws InterruptedException {
    if (LOG.isInfoEnabled()) {
      LOG.info("ae" + ae);
    }
    lock.lockInterruptibly();
    try {
      if (isHeadElementDone()) {
        headCompleted.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }
}
