package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.events.ActorState;
import io.ray.api.events.ActorStateEvent;
import io.ray.api.id.ActorId;
import io.ray.runtime.RayRuntimeInternal;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class ActorStateChangeEventTest extends BaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(ActorStateChangeEventTest.class);

  /** The event that indicates the state change of an actor. */
  public static class ActorStateChangeEvent implements Serializable {
    private ActorId actorId;
    private ActorState currentState;

    public ActorStateChangeEvent(ActorId actorId, ActorState currentState) {
      this.actorId = actorId;
      this.currentState = currentState;
    }

    public ActorId getActorId() {
      return actorId;
    }

    public ActorState getCurrentState() {
      return currentState;
    }
  }

  @BeforeClass
  public void setup() {
    System.setProperty("ray.job.num-java-workers-per-process", "1");
  }

  private static class Echo {

    String echo(String str) {
      return str;
    }
  }

  public static Boolean test(boolean inActor) {
    List<ActorStateChangeEvent> eventHistory = new ArrayList<>();

    Consumer<ActorStateEvent> consumer =
        (ActorStateEvent event) -> {
          ActorId actorId = event.actorId;
          ActorState currentState = event.currentState;
          LOG.info("A new event:" + actorId + ": " + currentState.name());
          synchronized (ActorStateChangeEventTest.class) {
            // We here filter the event of this actor self.
            if (inActor && actorId.equals(Ray.getRuntimeContext().getCurrentActorId())) {
              LOG.info("Skip the event of this actor self: " + actorId);
              return;
            }
            eventHistory.add(new ActorStateChangeEvent(actorId, currentState));
          }
        };

    boolean ok =
        ((RayRuntimeInternal) Ray.internal())
            .subscribeActorStateChangeEvents(Ray.getRuntimeContext().getCurrentJobId(), consumer);
    Assert.assertTrue(ok);

    ActorHandle<Echo> echo = Ray.actor(Echo::new).remote();
    ObjectRef<String> echoed = echo.task(Echo::echo, "hello").remote();
    Assert.assertEquals(echoed.get(), "hello");
    TestUtils.waitForCondition(
        () -> {
          synchronized (ActorStateChangeEventTest.class) {
            return eventHistory.size() >= 3;
          }
        },
        3000);

    synchronized (ActorStateChangeEventTest.class) {
      // Note, there are 3 stages of actor creation, so it should have 3 state items.
      Assert.assertEquals(eventHistory.size(), 3);
      Assert.assertEquals(eventHistory.get(0).getActorId(), echo.getId());
      Assert.assertEquals(eventHistory.get(0).getCurrentState(), ActorState.DEPENDENCIES_UNREADY);

      Assert.assertEquals(eventHistory.get(1).getActorId(), echo.getId());
      Assert.assertEquals(eventHistory.get(1).getCurrentState(), ActorState.PENDING_CREATION);

      Assert.assertEquals(eventHistory.get(2).getActorId(), echo.getId());
      Assert.assertEquals(eventHistory.get(2).getCurrentState(), ActorState.ALIVE);
    }

    echo.kill(true);
    TestUtils.waitForCondition(
        () -> {
          synchronized (ActorStateChangeEventTest.class) {
            return eventHistory.size() >= 4;
          }
        },
        2000);

    synchronized (ActorStateChangeEventTest.class) {
      Assert.assertEquals(eventHistory.size(), 4);
      Assert.assertEquals(eventHistory.get(3).getActorId(), echo.getId());
      Assert.assertEquals(eventHistory.get(3).getCurrentState(), ActorState.DEAD);
    }

    return true;
  }

  public void testActorStateChangedInDriver() {
    Assert.assertTrue(test(false));
  }

  static class ListenerActor {
    public Boolean testInActor() {
      return test(true);
    }
  }

  @Test
  public void testActorStateChangedInActor() {
    ActorHandle<ListenerActor> actor = Ray.actor(ListenerActor::new).remote();
    ObjectRef<Boolean> ok = actor.task(ListenerActor::testInActor).remote();
    Assert.assertTrue(ok.get());
  }
}
