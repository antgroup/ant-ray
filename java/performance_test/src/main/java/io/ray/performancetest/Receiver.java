package io.ray.performancetest;

import com.taobao.kmonitor.ImmutableMetricTags;
import com.taobao.kmonitor.MetricType;
import com.taobao.kmonitor.core.MetricsTags;
import com.taobao.kmonitor.tool.NetWorkUtil;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;

public class Receiver {

  private static final String RECEIVER_RECEIVE_QPS = "receiver.receive.qps";

  private int value = 0;
  private final MetricsTags tags;

  static {
    Metrics.register(RECEIVER_RECEIVE_QPS, MetricType.QPS);
  }

  public Receiver(String jobName) {
    tags = new ImmutableMetricTags("jobname", jobName);
    tags.addTag("host_name", NetWorkUtil.getLoaclHostname());
    String name = ManagementFactory.getRuntimeMXBean().getName();
    System.out.println(name);
    String pid = name.split("@")[0];
    System.out.println("Pid is:" + pid); // 48040
    tags.addTag("my_pid", pid);
  }

  public boolean ping() {
    return true;
  }

  public void noArgsNoReturn() {
    Metrics.report(RECEIVER_RECEIVE_QPS, tags, 1);
    value += 1;
  }

  public int noArgsHasReturn() {
    Metrics.report(RECEIVER_RECEIVE_QPS, tags, 1);
    value += 1;
    return value;
  }

  public void bytesNoReturn(byte[] data) {
    Metrics.report(RECEIVER_RECEIVE_QPS, tags, 1);
    value += 1;
  }

  public int bytesHasReturn(byte[] data) {
    Metrics.report(RECEIVER_RECEIVE_QPS, tags, 1);
    value += 1;
    return value;
  }

  public int bytesHasReturnSleep5ms(byte[] data) {
    try {
      Thread.sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    synchronized (this) {
      Metrics.report(RECEIVER_RECEIVE_QPS, tags, 1);
    }
    value += 1;
    return value;
  }

  public void byteBufferNoReturn(ByteBuffer data) {
    Metrics.report(RECEIVER_RECEIVE_QPS, tags, 1);
    value += 1;
  }

  public int byteBufferHasReturn(ByteBuffer data) {
    Metrics.report(RECEIVER_RECEIVE_QPS, tags, 1);
    value += 1;
    return value;
  }
}
