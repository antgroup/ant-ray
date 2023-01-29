package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class BackPressureTest extends BaseTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BackPressureTest.class);

  @BeforeClass
  public void setupJobConfig() {
    System.setProperty("ray.job.actor-task-back-pressure-enabled", "true");
    System.setProperty("ray.job.max-pending-calls", "10");
  }

  private static final ObjectId objectId = ObjectId.fromRandom();

  public static String unblockSignalActor(ActorHandle<SignalActor> signal) {
    signal.task(SignalActor::sendSignal).remote().get();
    return null;
  }

  @Test(enabled = false)
  public void testBackPressure() {
    ActorHandle<SignalActor> signalActor =
        Ray.actor(SignalActor::new).setMaxConcurrency(2).remote();
    /// Ping the actor to insure the actor is alive already.
    signalActor.task(SignalActor::ping).remote().get();

    final boolean[] backPressure = {false};
    Thread t =
        new Thread(
            Ray.wrapRunnable(
                new Runnable() {
                  @Override
                  public void run() {
                    /// The first task can be send out successfully.
                    // ClientQueue::max_pending_calls is 10, we should send 10+1 taks
                    for (int i = 0; i < 11; i++) {
                      signalActor.task(SignalActor::waitSignal).remote();
                    }
                    /// Will blocked
                    backPressure[0] = true;
                    signalActor.task(SignalActor::waitSignal).remote();
                    backPressure[0] = false;
                  }
                }));
    t.start();

    Assert.assertTrue(
        TestUtils.waitForCondition(
            () -> {
              return backPressure[0];
            },
            10 * 1000));

    Assert.assertFalse(
        TestUtils.waitForCondition(
            () -> {
              return !backPressure[0];
            },
            10 * 1000));

    Ray.task(BackPressureTest::unblockSignalActor, signalActor).remote().get();
    Assert.assertTrue(
        TestUtils.waitForCondition(
            () -> {
              return !backPressure[0];
            },
            10 * 1000));

    try {
      t.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void testBackPressure2() {
    /// set max concurrency to 11, 10 of them for executing waitSignal, and 1
    /// of them for executing sendSignal.
    ActorHandle<SignalActor> signalActor =
        Ray.actor(SignalActor::new).setMaxConcurrency(11).remote();
    /// Ping the actor to insure the actor is alive already.
    signalActor.task(SignalActor::ping).remote().get();

    for (int i = 0; i < 10; i++) {
      Assert.assertNotNull(signalActor.task(SignalActor::waitSignal).remote());
    }

    // Check backpressure occur.
    Assert.assertNull(signalActor.task(SignalActor::waitSignal).remote());

    // Unblock signal actor, to make all backpressured raycall executed.
    for (int i = 0; i < 10; i++) {
      Ray.task(BackPressureTest::unblockSignalActor, signalActor).remote().get();
    }

    // Check the raycall is normal
    signalActor.task(SignalActor::ping).remote().get();
  }
}
