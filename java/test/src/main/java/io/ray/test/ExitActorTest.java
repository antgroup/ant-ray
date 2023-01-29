package io.ray.test;

import static io.ray.runtime.util.SystemUtil.pid;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.exception.RayActorException;
import io.ray.runtime.task.TaskExecutor;
import io.ray.runtime.util.SystemUtil;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class ExitActorTest extends BaseTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExitActorTest.class);

  private static class ExitingActor {

    int counter = 0;

    public Integer incr() {
      return ++counter;
    }

    public int getPid() {
      return pid();
    }

    public int getSizeOfActorContextMap() {
      TaskExecutor taskExecutor = TestUtils.getRuntime().getTaskExecutor();
      try {
        Field field = TaskExecutor.class.getDeclaredField("actorContextMap");
        field.setAccessible(true);
        return ((Map<?, ?>) field.get(taskExecutor)).size();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public boolean exit() {
      Ray.exitActor();
      return false;
    }
  }

  private static class SourceActor {
    private int taskNum;
    private ActorHandle<ReceiverActor> receiverActor;

    SourceActor(int taskNum, ActorHandle<ReceiverActor> receiverActor) {
      this.taskNum = taskNum;
      this.receiverActor = receiverActor;
    }

    public void start() {
      for (int i = 0; i < taskNum; i++) {
        receiverActor.task(ReceiverActor::receive, i).remote();
      }
      LOGGER.info("SourceActor end send tasks");
      Ray.exitActor();
    }
  }

  private static class ReceiverActor {
    private int taskNum = 0;

    ReceiverActor() {}

    public void receive(int num) {
      taskNum++;
    }

    public Integer getTaskNum() {
      return taskNum;
    }
  }

  public void testTaskLose() {
    int taskNum = 10000;
    ActorHandle<ReceiverActor> receiverActor = Ray.actor(ReceiverActor::new).remote();
    ActorHandle<SourceActor> sourceActor =
        Ray.actor(SourceActor::new, taskNum, receiverActor).remote();
    sourceActor.task(SourceActor::start).remote();
    TestUtils.waitForCondition(
        () -> {
          int receivedTaskNum = (int) receiverActor.task(ReceiverActor::getTaskNum).remote().get();
          LOGGER.info("receiver receive: {} tasks", receivedTaskNum);
          return receivedTaskNum == taskNum;
        },
        30 * 1000);
  }

  public void testExitActor() throws IOException, InterruptedException {
    ActorHandle<ExitingActor> actor = Ray.actor(ExitingActor::new).setMaxRestarts(10000).remote();
    Assert.assertEquals(1, (int) (actor.task(ExitingActor::incr).remote().get()));
    int pid = actor.task(ExitingActor::getPid).remote().get();
    Runtime.getRuntime().exec("kill -9 " + pid);
    TimeUnit.SECONDS.sleep(1);
    // Make sure this actor can be reconstructed.
    Assert.assertEquals(1, (int) actor.task(ExitingActor::incr).remote().get());

    // `exitActor` will exit the actor without reconstructing.
    ObjectRef<Boolean> obj = actor.task(ExitingActor::exit).remote();
    Assert.assertThrows(RayActorException.class, obj::get);
  }

  public void testExitActorInMultiWorker() {
    Assert.assertTrue(TestUtils.getNumWorkersPerProcess() > 1);
    ActorHandle<ExitingActor> actor1 = Ray.actor(ExitingActor::new).setMaxRestarts(10000).remote();
    int pid = actor1.task(ExitingActor::getPid).remote().get();
    Assert.assertEquals(
        1, (int) actor1.task(ExitingActor::getSizeOfActorContextMap).remote().get());
    ActorHandle<ExitingActor> actor2;
    while (true) {
      // Create another actor which share the same process of actor 1.
      actor2 = Ray.actor(ExitingActor::new).setMaxRestarts(0).remote();
      int actor2Pid = actor2.task(ExitingActor::getPid).remote().get();
      if (actor2Pid == pid) {
        break;
      }
    }
    Assert.assertEquals(
        2, (int) actor1.task(ExitingActor::getSizeOfActorContextMap).remote().get());
    Assert.assertEquals(
        2, (int) actor2.task(ExitingActor::getSizeOfActorContextMap).remote().get());
    ObjectRef<Boolean> obj1 = actor1.task(ExitingActor::exit).remote();
    Assert.assertThrows(RayActorException.class, obj1::get);
    Assert.assertTrue(SystemUtil.isProcessAlive(pid));
    // Actor 2 shouldn't exit or be reconstructed.
    Assert.assertEquals(1, (int) actor2.task(ExitingActor::incr).remote().get());
    Assert.assertEquals(
        1, (int) actor2.task(ExitingActor::getSizeOfActorContextMap).remote().get());
    Assert.assertEquals(pid, (int) actor2.task(ExitingActor::getPid).remote().get());
    Assert.assertTrue(SystemUtil.isProcessAlive(pid));
  }

  public void testExitActorWithDynamicOptions() {
    RayConfig config = TestUtils.getRuntime().getRayConfig();
    if (config.gcsTaskSchedulingEnabled) {
      throw new SkipException("This case does not work under new gcs task scheduling mode.");
    }

    ActorHandle<ExitingActor> actor =
        Ray.actor(ExitingActor::new)
            .setMaxRestarts(10000)
            // Set dummy JVM options to start a worker process with only one worker.
            .setJvmOptions(ImmutableList.of("-Ddummy=value"))
            .remote();
    int pid = actor.task(ExitingActor::getPid).remote().get();
    Assert.assertTrue(SystemUtil.isProcessAlive(pid));
    ObjectRef<Boolean> obj1 = actor.task(ExitingActor::exit).remote();
    Assert.assertThrows(RayActorException.class, obj1::get);
    // Now the actor shouldn't be reconstructed anymore.
    Assert.assertThrows(
        RayActorException.class, () -> actor.task(ExitingActor::getPid).remote().get());
    // Now the worker process should be dead.
    Assert.assertTrue(TestUtils.waitForCondition(() -> !SystemUtil.isProcessAlive(pid), 5000));
  }
}
