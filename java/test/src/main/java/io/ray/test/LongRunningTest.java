package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class LongRunningTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.head-args.0", "--num-cpus=1");
  }

  public static class Actor {

    public int ping() {
      return 0;
    }
  }

  public static void main(String[] args) {
    boolean longRunning = Boolean.parseBoolean(args[0]);
    boolean driverError = Boolean.parseBoolean(args[1]);
    boolean forgetShutdown = Boolean.parseBoolean(args[2]);
    if (longRunning) {
      System.setProperty("ray.job.long-running", "true");
      System.setProperty("ray.job.default-actor-lifetime", "DETACHED");
    }
    Ray.init();
    // Create an actor which occupies 1 CPU.
    // Note that in ant internal version, we will set detached flag for the actors created in long
    // running driver,
    // so this is the same as:
    // ActorHandle<Actor> actor =
    // Ray.actor(Actor::new).setLifetime(ActorLifetime.DETACHED).setResource("CPU", 1.0).remote();
    ActorHandle<Actor> actor = Ray.actor(Actor::new).setResource("CPU", 1.0).remote();
    // Make sure the actor is now alive
    actor.task(Actor::ping).remote().get();
    if (driverError) {
      throw new RuntimeException("Test exception");
    }
    if (!forgetShutdown) {
      Ray.shutdown();
    }
  }

  @DataProvider
  public static Object[][] parameters() {
    return new Object[][] {
      {false, false, false, false},
      {false, false, false, true},
      {false, false, true, false},
      {false, false, true, true},
      {false, true, false, false},
      {false, true, false, true},
      {false, true, true, false},
      {false, true, true, true},
      {true, false, false, false},
      {true, false, false, true},
      {true, false, true, false},
      {true, false, true, true},
      {true, true, false, false},
      {true, true, false, true},
      {true, true, true, false},
      {true, true, true, true},
    };
  }

  @Test(
      groups = {"cluster"},
      dataProvider = "parameters")
  public void testDriverResourceRelease(
      boolean longRunning, boolean driverError, boolean useDefaultDriver, boolean forgetShutdown)
      throws InterruptedException {
    Supplier<Process> createDriver =
        () -> {
          String[] args =
              new String[] {
                String.valueOf(longRunning),
                String.valueOf(driverError),
                String.valueOf(forgetShutdown),
              };
          try {
            ProcessBuilder builder = TestUtils.buildDriver(getClass(), args, useDefaultDriver);
            builder.redirectOutput(Redirect.INHERIT);
            builder.redirectError(Redirect.INHERIT);
            return builder.start();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        };
    Consumer<Process> assertExitCode =
        (Process p) ->
            Assert.assertEquals(
                p.exitValue(), driverError ? 1 : 0, "The exit code of driver is unexpected.");

    Process driver1 = createDriver.get();
    driver1.waitFor();
    assertExitCode.accept(driver1);
    TimeUnit.SECONDS.sleep(1);
    Process driver2 = createDriver.get();

    boolean slow = longRunning && !driverError && (useDefaultDriver || !forgetShutdown);
    if (slow) {
      Assert.assertFalse(driver2.waitFor(20, TimeUnit.SECONDS));
      driver2.destroyForcibly();
      driver2.waitFor();
    } else {
      Assert.assertTrue(driver2.waitFor(20, TimeUnit.SECONDS));
      assertExitCode.accept(driver2);
    }
  }
}
