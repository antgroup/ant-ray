package io.ray.streaming.common.utils;

import com.google.common.base.MoreObjects;
import io.ray.streaming.common.config.CommonConfig;
import io.ray.streaming.common.config.Config;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CommonUtilTest {

  @Test
  public void testStringMapToObjectMap() {
    Map<String, String> sourceMap = new HashMap<>();
    sourceMap.put("1", "111");
    sourceMap.put("2", "222");

    Map<String, Object> resultMap = CommonUtil.stringMapToObjectMap(sourceMap);
    Assert.assertEquals(resultMap.get("1").toString(), "111");
    Assert.assertEquals(resultMap.get("2").toString(), "222");
  }

  @Test
  public void testObjectMapToStringMap() {
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("1", new TestObject());

    Map<String, String> resultMap = CommonUtil.objectMapToStringMap(sourceMap);
    Assert.assertTrue(resultMap.get("1").contains("test1=1"));
    Assert.assertTrue(resultMap.get("1").contains("test2=2"));
  }

  class TestObject {
    String test1 = "1";
    int test2 = 2;

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("test1", test1).add("test2", test2).toString();
    }
  }

  @Test
  public void testGetConfigKeys() {
    List<String> commonConfigKeys = CommonUtil.getConfigKeys(CommonConfig.class);
    Assert.assertNotNull(commonConfigKeys);
    Assert.assertTrue(commonConfigKeys.contains(CommonConfig.ACTOR_ID));
    Assert.assertTrue(commonConfigKeys.contains(CommonConfig.ENV_TYPE));
    Assert.assertTrue(commonConfigKeys.contains(CommonConfig.CHANNEL_TYPE));
    Assert.assertTrue(commonConfigKeys.contains(CommonConfig.HOSTNAME));
    Assert.assertTrue(commonConfigKeys.contains(CommonConfig.JOB_NAME));
  }

  @Test
  public void testIsTimeExpired() {
    long time = System.currentTimeMillis() - 10000;
    Assert.assertTrue(CommonUtil.isTimeExpired(time));

    time = System.currentTimeMillis() + 10000;
    Assert.assertFalse(CommonUtil.isTimeExpired(time));

    time = System.currentTimeMillis() + 10000 / 1000;
    Assert.assertFalse(CommonUtil.isTimeExpired(time));

    time = System.currentTimeMillis() - 10000 / 1000;
    Assert.assertTrue(CommonUtil.isTimeExpired(time));

    time = 1630376367;
    Assert.assertTrue(CommonUtil.isTimeExpired(time));
  }

  @Test
  public void testFormatDoubleWithSpecifiedDecimals() {
    double number = 1.12345678D;
    Assert.assertEquals(CommonUtil.formatDoubleWithSpecifiedDecimals(number, 0), "1");
    Assert.assertEquals(CommonUtil.formatDoubleWithSpecifiedDecimals(number, 3), "1.123");
    Assert.assertEquals(CommonUtil.formatDoubleWithSpecifiedDecimals(number, 5), "1.12346");

    number = 16000D;
    Assert.assertEquals(CommonUtil.formatDoubleWithSpecifiedDecimals(number, 0), "16000");
  }

  @Test
  public void testIsSubClazz() {
    Assert.assertTrue(Config.class.isAssignableFrom(CommonConfig.class));
    Assert.assertTrue(
        CommonUtil.isSubClazz(
            "io.ray.streaming.common.config.CommonConfig",
            "io.ray.streaming.common.config.Config"));
    Assert.assertFalse(CommonConfig.class.isAssignableFrom(Config.class));
    Assert.assertFalse(
        CommonUtil.isSubClazz(
            "io.ray.streaming.common.config.Config",
            "io.ray.streaming.common.config.CommonConfig"));
  }
}
