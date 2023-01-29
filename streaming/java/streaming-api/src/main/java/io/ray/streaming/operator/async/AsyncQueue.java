package io.ray.streaming.operator.async;

/**
 * AsyncQueue is a queue used in {@link AsyncOperator} to ensure async process.
 *
 * @param <T> Type of the input data of {@link AsyncElement}.
 * @param <R> Type of the output data of {@link AsyncElement}.
 */
public interface AsyncQueue<T, R> {

  enum OrderType {
    ORDERED,
    UNORDERED
  }

  /** Gets the queue size. */
  int size();

  /** Queue is empty or not. */
  boolean isEmpty();

  /** Queue is full or not. */
  boolean isFull();

  /**
   * Inserts the given element into this queue until succeed. This is a blocking method.
   *
   * @param asyncElement the element to add
   */
  void add(AsyncElement<T, R> asyncElement) throws InterruptedException;

  /**
   * Retrieves but does not remove the head of this queue. This is a blocking method.
   *
   * @return the head of this queue
   */
  AsyncElement<T, R> peek() throws InterruptedException;

  /**
   * Retrieves and removes the head of this queue. This is a blocking method.
   *
   * @return the head of this queue
   */
  AsyncElement<T, R> poll() throws InterruptedException;
}
