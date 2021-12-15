package io.ray.runtime.task;

import com.google.common.base.Preconditions;
import io.ray.api.id.UniqueId;
import io.ray.runtime.RayRuntimeInternal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Task executor for cluster mode.
 */
public class NativeTaskExecutor extends TaskExecutor<NativeTaskExecutor.NativeActorContext> {

  static class NativeActorContext extends TaskExecutor.ActorContext {

  }

  public NativeTaskExecutor(RayRuntimeInternal runtime) {
    super(runtime);
  }

  @Override
  protected NativeActorContext createActorContext() {
    return new NativeActorContext();
  }

  @Override
  Object handleAsyncActorTaskResult(CompletableFuture<?> result) {
    // TODO fiber
    Preconditions.checkState(result.isDone());
    try {
      return result.get(0, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void onWorkerShutdown(byte[] workerIdBytes) {
    // This is to make sure no memory leak when `Ray.exitActor()` is called.
    removeActorContext(new UniqueId(workerIdBytes));
  }
}
