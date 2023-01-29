package io.ray.streaming.operator.independent;

import com.google.common.collect.ImmutableMap;
import io.ray.streaming.api.context.IndependentOperatorDescriptor;
import io.ray.streaming.common.enums.ResourceKey;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BaseIndependentOperatorDescriptorTest {

  @Test
  public void testBasic() {
    String className = "testClass";
    String moduleName = "testModule";
    IndependentOperatorDescriptor independentOperatorDescriptor =
        new IndependentOperatorDescriptor(className);
    Assert.assertTrue(independentOperatorDescriptor.isJavaType());
    Assert.assertEquals(independentOperatorDescriptor.getClassName(), className);
    Assert.assertTrue(independentOperatorDescriptor.getModuleName().isEmpty());
    Assert.assertEquals(independentOperatorDescriptor.getParallelism(), 1);
    Assert.assertNotNull(independentOperatorDescriptor.getResource());
    Assert.assertNotNull(independentOperatorDescriptor.getConfig());
    Assert.assertTrue(independentOperatorDescriptor.getResource().isEmpty());
    Assert.assertTrue(independentOperatorDescriptor.getConfig().isEmpty());
    Assert.assertFalse(independentOperatorDescriptor.isLazyScheduling());
    Assert.assertFalse(independentOperatorDescriptor.asJson().isEmpty());
    Assert.assertTrue(independentOperatorDescriptor.asJson().contains(className));

    independentOperatorDescriptor = new IndependentOperatorDescriptor(className, moduleName);
    Assert.assertTrue(independentOperatorDescriptor.isPythonType());
    Assert.assertEquals(independentOperatorDescriptor.getClassName(), className);
    Assert.assertFalse(independentOperatorDescriptor.getModuleName().isEmpty());
    Assert.assertEquals(independentOperatorDescriptor.getParallelism(), 1);
    Assert.assertNotNull(independentOperatorDescriptor.getResource());
    Assert.assertNotNull(independentOperatorDescriptor.getConfig());
    Assert.assertTrue(independentOperatorDescriptor.getResource().isEmpty());
    Assert.assertTrue(independentOperatorDescriptor.getConfig().isEmpty());

    independentOperatorDescriptor.setParallelism(3);
    independentOperatorDescriptor.setConfig(ImmutableMap.of("k1", "v1"));
    independentOperatorDescriptor.setResource(ImmutableMap.of(ResourceKey.CPU.name(), 0.1D));
    independentOperatorDescriptor.setLazyScheduling();
    Assert.assertEquals(independentOperatorDescriptor.getParallelism(), 3);
    Assert.assertFalse(independentOperatorDescriptor.getResource().isEmpty());
    Assert.assertFalse(independentOperatorDescriptor.getConfig().isEmpty());
    Assert.assertEquals(independentOperatorDescriptor.getConfig().get("k1"), "v1");
    Assert.assertEquals(
        (double) independentOperatorDescriptor.getResource().get(ResourceKey.CPU.name()), 0.1D);
    Assert.assertTrue(independentOperatorDescriptor.isLazyScheduling());
  }
}
