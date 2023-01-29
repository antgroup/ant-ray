package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class ActorPerfTest extends BaseTest {

  public static class Actor {

    public boolean ping() {
      return true;
    }

    public byte[] mergeInputArgsAsOutput(List<byte[]> args) {
      int length = 0;
      for (byte[] arg : args) {
        length += arg.length;
      }

      byte[] result = new byte[length];
      int index = 0;
      for (byte[] arg : args) {
        System.arraycopy(arg, 0, result, index, arg.length);
        index += arg.length;
      }
      return result;
    }

    public void mergeInputArgsAsOutputNoReturn(List<byte[]> args) {
      mergeInputArgsAsOutput(args);
    }
  }

  public void testActorTaskLocalNoReturnPerformance() {
    testActorTaskPerformance(true);
  }

  private void testActorTaskPerformance(boolean useNoReturns) {
    byte[] bytes = new byte[] {1, 2, 3};
    ActorHandle<Actor> actor = Ray.actor(Actor::new).remote();
    // wait for actor creation finish.
    actor.task(Actor::ping).remote().get();
    // Test submitting some tasks with by-value args for that actor.
    List<ObjectRef<byte[]>> results = new ArrayList<>();
    Instant start = Instant.now();
    // Update this number before running tests.
    int numTasks = 1000;
    System.out.println("start submitting " + numTasks + " tasks");
    for (int i = 0; i < numTasks; i++) {
      // If we are testing tasks with no return objects, then set `num_returns`
      // to 0 except for the last task, as we need to get the return object
      // of last task to know all the tasks are finished.
      boolean ignoreReturn = useNoReturns && i < numTasks - 1;

      // Create arguments with PassByValue.
      if (ignoreReturn) {
        actor.task(Actor::mergeInputArgsAsOutputNoReturn, ImmutableList.of(bytes)).remote();
      } else {
        results.add(actor.task(Actor::mergeInputArgsAsOutput, ImmutableList.of(bytes)).remote());
      }
    }

    System.out.println(
        "finish submitting "
            + numTasks
            + " tasks, which takes "
            + Duration.between(start, Instant.now()).getSeconds()
            + "s.");

    Ray.get(results);

    System.out.println(
        "finish executing "
            + numTasks
            + " tasks, which takes "
            + Duration.between(start, Instant.now()).getSeconds()
            + "s.");
  }
}
