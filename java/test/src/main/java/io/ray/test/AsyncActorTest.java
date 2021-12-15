package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class AsyncActorTest extends BaseTest {

  public static class AsyncActor {

    Future<String> async() {
      CompletableFuture<String> future = new CompletableFuture<>();
      new Thread(() -> {
        try {
          TimeUnit.MILLISECONDS.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        future.complete("ok");
      }).start();
      return future;
    }

    String sync() {
      return "ok";
    }
  }

  public void testAsyncActor() {
    ActorHandle<AsyncActor> asyncActor = Ray.actor(AsyncActor::new).setAsync().remote();
    ObjectRef<String> res2 = asyncActor.task(AsyncActor::sync).remote();
    Assert.assertEquals(res2.get(), "ok");
  }
}
