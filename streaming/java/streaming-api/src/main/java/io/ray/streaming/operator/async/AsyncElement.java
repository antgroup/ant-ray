package io.ray.streaming.operator.async;

import com.google.common.base.Preconditions;
import io.ray.streaming.api.function.async.ResultFuture;
import io.ray.streaming.message.Record;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * AsyncElementQueue's element.
 *
 * @param <T> Type of the input data.
 * @param <R> Type of the output data.
 */
public class AsyncElement<T, R> implements ResultFuture<R> {

  private final Record<T> record;
  private final CompletableFuture<Collection<R>> future;

  public AsyncElement(Record<T> record) {
    this.record = Preconditions.checkNotNull(record);
    this.future = new CompletableFuture<>();
  }

  @Override
  public void complete(Collection<R> results) {
    future.complete(results);
  }

  @Override
  public void completeExceptionally(Throwable ex) {
    future.completeExceptionally(ex);
  }

  public boolean isDone() {
    return future.isDone();
  }

  public void onCompleted(Consumer<AsyncElement<T, R>> consumer, Executor executor) {
    future.whenCompleteAsync(
        (t, u) -> {
          consumer.accept(this);
        },
        executor);
  }

  public Collection<R> get() throws ExecutionException, InterruptedException {
    return future.get();
  }

  public Record<T> getRecord() {
    return record;
  }

  @Override
  public String toString() {
    return "AsyncElement{" + "record=" + record + '}';
  }
}
