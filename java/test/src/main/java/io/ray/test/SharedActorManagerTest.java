package io.ray.test;

import static io.ray.runtime.util.SystemUtil.pid;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.util.SystemUtil;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class SharedActorManagerTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.job.num-java-workers-per-process", "2");
  }

  private static class A {

    static ActorHandle<B> b = null;

    public A() {

    }



    String f() {
      if (b == null) {
        b = (ActorHandle<B>) (Ray.getActor("B").get());
      }

      return b.task(B::hello).remote().get();
    }

  }

  private static class B {
    String hello() {
      return "hello";
    }

  }

  public void testF() throws InterruptedException {
    /// 创建1个actor B在一个进程中
    ActorHandle<B> b = Ray.actor(B::new).setJvmOptions(ImmutableList.of("-DX=1")).setName("B").remote();

    /// 创建2个actor A在一个进程中
    /// 2个A中分别拿到B的handle
    ActorHandle<A> a1 = Ray.actor(A::new).remote();
    a1.task(A::f).remote().get();
    ActorHandle<A> a2 = Ray.actor(A::new).remote();
    a2.task(A::f).remote().get();

    TimeUnit.SECONDS.sleep(10);
  }

}
