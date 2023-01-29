package io.ray.streaming.common.utils;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Extend {@link PriorityBlockingQueue} with no repeated element.
 *
 * <p>Notice: Only {@link NoRepeatedPriorityBlockingQueue#add(Object)} support no repeated element
 * adding.
 *
 * @param <E> element type
 */
public class NoRepeatedPriorityBlockingQueue<E> extends PriorityBlockingQueue<E> {

  private final Set<E> elementSet = new HashSet<>();

  @Override
  public boolean add(E e) {
    if (!elementSet.contains(e)) {
      boolean result = super.add(e);
      if (result) {
        elementSet.add(e);
      }
      return result;
    }
    return true;
  }

  @Override
  public E poll() {
    E e = super.poll();
    if (e != null) {
      elementSet.remove(e);
    }
    return e;
  }

  @Override
  public E take() throws InterruptedException {
    E e = super.take();
    elementSet.remove(e);
    return e;
  }

  @Override
  public boolean remove(Object o) {
    elementSet.remove(o);
    return super.remove(o);
  }
}
