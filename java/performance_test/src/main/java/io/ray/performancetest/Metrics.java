package io.ray.performancetest;

import com.taobao.kmonitor.KMonitor;
import com.taobao.kmonitor.KMonitorFactory;
import com.taobao.kmonitor.MetricType;
import com.taobao.kmonitor.core.MetricsTags;

public class Metrics {

  private static final KMonitor K_MONITOR;

  static {
    if (!TestUtils.isDevMode()) {
      KMonitorFactory.start();
      K_MONITOR = KMonitorFactory.getKMonitor("performance_test");
    } else {
      K_MONITOR = null;
    }
  }

  public static boolean register(String var1, MetricType var2) {
    if (!TestUtils.isDevMode()) {
      return K_MONITOR.register(var1, var2);
    } else {
      return false;
    }
  }

  public static void report(String var1, MetricsTags var2, double var3) {
    if (!TestUtils.isDevMode()) {
      K_MONITOR.report(var1, var2, var3);
    }
  }
}
