package io.ray.streaming.operator.async;

import io.ray.streaming.api.collector.Collector;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emitter is a thread which fetches a completed element from {@link AsyncOperator} and sends it to
 * the downstream.
 *
 * @param <T> Type of the output data.
 */
public class Emitter<T> implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Emitter.class);

  private final AsyncQueue queue;
  private final List<Collector> collectors;
  private boolean isRunning;

  public Emitter(AsyncQueue queue, List<Collector> collectors) {
    this.queue = queue;
    this.collectors = collectors;
    this.isRunning = true;
  }

  @Override
  public void run() {
    try {
      while (isRunning) {
        AsyncElement element = queue.peek();
        Collection<T> result = element.get();
        if (result != null) {
          for (Collector collector : collectors) {
            for (T t : result) {
              element.getRecord().setValue(t);
              collector.collect(element.getRecord());
            }
          }
        }
        queue.poll();
      }
    } catch (InterruptedException e) {
      // TODO(buhe): if occurs exception, should consider snapshot and restore.
      LOG.error(e.getMessage(), e);
    } catch (ExecutionException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void stop() {
    this.isRunning = false;
  }
}
