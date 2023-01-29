package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.runtime.exception.RayTaskException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"singleProcess"})
public class SingleProcessModeTest extends BaseTest {

  private static final int NUM_ACTOR_INSTANCE = 10;

  private static final int TIMES_TO_CALL_PER_ACTOR = 10;

  static class MyActor {
    public MyActor() {}

    public long getThreadId() {
      return Thread.currentThread().getId();
    }
  }

  public void testActorTasksInOneThread() {
    List<ActorHandle<MyActor>> actors = new ArrayList<>();
    Map<ActorId, Long> actorThreadIds = new HashMap<>();
    for (int i = 0; i < NUM_ACTOR_INSTANCE; ++i) {
      ActorHandle<MyActor> actor = Ray.actor(MyActor::new).remote();
      actors.add(actor);
      actorThreadIds.put(actor.getId(), actor.task(MyActor::getThreadId).remote().get());
    }

    Map<ActorId, List<ObjectRef<Long>>> allResults = new HashMap<>();
    for (int i = 0; i < NUM_ACTOR_INSTANCE; ++i) {
      final ActorHandle<MyActor> actor = actors.get(i);
      List<ObjectRef<Long>> thisActorResult = new ArrayList<>();
      for (int j = 0; j < TIMES_TO_CALL_PER_ACTOR; ++j) {
        thisActorResult.add(actor.task(MyActor::getThreadId).remote());
      }
      allResults.put(actor.getId(), thisActorResult);
    }

    // check result.
    for (int i = 0; i < NUM_ACTOR_INSTANCE; ++i) {
      final ActorHandle<MyActor> actor = actors.get(i);
      final List<ObjectRef<Long>> thisActorResult = allResults.get(actor.getId());
      // assert
      for (ObjectRef<Long> threadId : thisActorResult) {
        Assert.assertEquals(threadId.get(), actorThreadIds.get(actor.getId()));
      }
    }
  }

  private static String recursionEcho(int recursionDepth) throws InterruptedException {
    TimeUnit.SECONDS.sleep(1);
    if (recursionDepth > 3) {
      return String.valueOf(recursionDepth);
    } else {
      return String.valueOf(recursionDepth)
          + "_"
          + Ray.task(SingleProcessModeTest::recursionEcho, recursionDepth + 1).remote().get();
    }
  }

  public void testShutdownExecutorService() {
    Ray.task(SingleProcessModeTest::recursionEcho, 1).remote();
    TestUtils.executeWithinTime(
        () -> {
          Ray.shutdown();
        },
        500);
  }

  private static void sleepAndIgnoreInterrupt(long milliseconds) {
    long finishMs = System.currentTimeMillis() + milliseconds;
    while (System.currentTimeMillis() < finishMs) {
      try {
        TimeUnit.MILLISECONDS.sleep(Math.max(0, finishMs - System.currentTimeMillis()));
      } catch (InterruptedException e) {
        // In this test case, we ignore InterruptedException to imitate a compute
        // task. It makes sure that this method won't be cancelled early.
      }
    }
  }

  static class SpawnActor {

    public static AtomicLong counter = new AtomicLong(0);

    public void spawn() {
      // The method should run for about 1s.
      long finishMs = System.currentTimeMillis() + 1000;
      while (System.currentTimeMillis() < finishMs) {
        // Spawn a new actor
        spawnImpl();
        // Create a normal task to spawn a new actor
        Ray.task(SpawnActor::spawnImpl).remote();

        sleepAndIgnoreInterrupt(Math.min(finishMs - System.currentTimeMillis(), 100));
      }
    }

    public static void spawnImpl() {
      counter.incrementAndGet();
      // Spawn a new actor
      ActorHandle<SpawnActor> actor = Ray.actor(SpawnActor::new).remote();
      actor.task(SpawnActor::spawn).remote();
    }
  }

  public void testShutdownAndWait() throws InterruptedException {
    SpawnActor.spawnImpl();
    // Sleep for a while to allow some actors to be created.
    // NOTE: don't sleep for too long here. Creating too many actors may slow down the
    // test and result in test failure.
    TimeUnit.MILLISECONDS.sleep(200);
    Assert.assertTrue(SpawnActor.counter.get() > 0);
    // There should be actors just started, and they will continue to execute for at least 900ms.
    // And Ray.shutdown() should cancel any further tasks, so the shutdown process should finish
    // within 1s.
    // We loose the time range condition to [500ms, 1500ms].
    TestUtils.executeWithinTimeRange(() -> Ray.shutdown(), 500, 1500);
    // Make sure the counter value doesn't change after shutdown.
    long counterValue = SpawnActor.counter.get();
    TimeUnit.SECONDS.sleep(1);
    Assert.assertEquals(SpawnActor.counter.get(), counterValue);
  }

  private static int add1(int value) {
    sleepAndIgnoreInterrupt(100);
    return Ray.task(SingleProcessModeTest::add1, value).remote().get() + 1;
  }

  @Test(expectedExceptions = RayTaskException.class)
  public void testShutdownAndGetFailure() throws Throwable {
    ObjectRef<Integer> obj = Ray.task(SingleProcessModeTest::add1, 0).remote();
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future<?> future = executorService.submit(Ray.wrapRunnable(obj::get));

    TimeUnit.MILLISECONDS.sleep(500);
    Ray.shutdown();
    // Tasks waiting on ObjectRef::get should get a  RayTaskException.
    try {
      future.get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }
}
