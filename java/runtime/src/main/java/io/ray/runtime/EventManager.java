package io.ray.runtime;

import com.google.common.base.Preconditions;
import io.ray.api.Ray;
import io.ray.api.id.JobId;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EventManager<E> {

  private static final Logger LOG = LoggerFactory.getLogger(EventManager.class);

  private ConcurrentHashMap<JobId, Consumer<E>> listeningTable = new ConcurrentHashMap<>();

  private ConcurrentHashMap<JobId, ExecutorService> executorServices = new ConcurrentHashMap<>();

  private boolean subscribedFromNative = false;

  boolean subscribedFromNative() {
    return subscribedFromNative;
  }

  void setSubscribedFromNative(boolean subscribed) {
    subscribedFromNative = subscribed;
  }

  boolean subscribe(JobId jobId, Consumer<E> consumer) {
    LOG.info("Subscribing for job: {}", jobId);
    if (listeningTable.containsKey(jobId)) {
      return false;
    }
    Preconditions.checkState(!executorServices.containsKey(jobId));
    Object asyncContext = Ray.getAsyncContext();
    // Note, the reason for using single thread executor is we should keep the order of event
    // callbacks for one job.
    executorServices.put(jobId, Executors.newSingleThreadExecutor());
    listeningTable.put(
        jobId,
        (E event) -> {
          Ray.setAsyncContext(asyncContext);
          consumer.accept(event);
        });
    return true;
  }

  public void pushEvent(JobId jobId, E event) {
    LOG.debug("Push event, jobId={}, event={}", jobId, event);
    if (!executorServices.containsKey(jobId) || !listeningTable.containsKey(jobId)) {
      LOG.info("We didn't subscribe for this job yet, job id is {}", jobId);
      return;
    }

    final Runnable r =
        () -> {
          Consumer<E> consumer = listeningTable.get(jobId);
          if (consumer != null) {
            consumer.accept(event);
          } else {
            LOG.debug("The event that we didn't subscribed.");
          }
        };
    ExecutorService executorService = executorServices.get(jobId);
    executorService.submit(r);
  }
}
