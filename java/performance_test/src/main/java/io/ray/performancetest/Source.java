package io.ray.performancetest;

import com.google.common.base.Preconditions;
import com.taobao.kmonitor.ImmutableMetricTags;
import com.taobao.kmonitor.MetricType;
import com.taobao.kmonitor.core.MetricsTags;
import com.taobao.kmonitor.tool.NetWorkUtil;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.WaitResult;
import io.ray.performancetest.test.latency.LatencyParameters;
import io.ray.performancetest.test.util.Percentile;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Source {

  private static final String SOURCE_SEND_QPS = "source.send.qps";
  private static final String SOURCE_GET_RT = "source.get.rt";
  private static final String SOURCE_LATENCY_P95 = "source.latency.p95";
  private static final String SOURCE_LATENCY_P99 = "source.latency.p99";
  private static final String SOURCE_LATENCY_P999 = "source.latency.p999";
  private static final String SOURCE_LATENCY_P9995 = "source.latency.p9995";
  private static final String SOURCE_LATENCY_P9999 = "source.latency.p9999";
  private static final int BATCH_SIZE;

  private static final Logger LOGGER = LoggerFactory.getLogger(Source.class);

  private final List<ActorHandle<Receiver>> receivers;
  private final MetricsTags tags;
  private List<Long> latencyList = new LinkedList();
  private ExecutorService executorService = Executors.newFixedThreadPool(5);
  static final long latencyWindow = 1000000;

  static {
    Metrics.register(SOURCE_SEND_QPS, MetricType.QPS);
    Metrics.register(SOURCE_GET_RT, MetricType.RAW);
    Metrics.register(SOURCE_LATENCY_P95, MetricType.GAUGE);
    Metrics.register(SOURCE_LATENCY_P99, MetricType.GAUGE);
    Metrics.register(SOURCE_LATENCY_P999, MetricType.GAUGE);
    Metrics.register(SOURCE_LATENCY_P9995, MetricType.GAUGE);
    Metrics.register(SOURCE_LATENCY_P9999, MetricType.GAUGE);
    String batchSizeString = System.getenv().get("PERF_TEST_BATCH_SIZE");
    if (batchSizeString != null) {
      BATCH_SIZE = Integer.valueOf(batchSizeString);
    } else {
      BATCH_SIZE = 1000;
    }
  }

  public Source(List<ActorHandle<Receiver>> receivers, String jobName) {
    this.receivers = receivers;
    tags = new ImmutableMetricTags("jobname", jobName);
    tags.addTag("host_name", NetWorkUtil.getLoaclHostname());
    String name = ManagementFactory.getRuntimeMXBean().getName();
    String pid = name.split("@")[0];
    tags.addTag("my_pid", pid);
  }

  public boolean startTest(Parameters parameters, LatencyParameters latencyParameters) {
    byte[] bytes = null;
    ByteBuffer buffer = null;
    if (parameters.getArgSize() > 0) {
      bytes = new byte[parameters.getArgSize()];
      new Random().nextBytes(bytes);
      buffer = ByteBuffer.wrap(bytes);
    } else {
      Preconditions.checkState(!parameters.isUseDirectByteBuffer());
    }

    if (latencyParameters.isEnable()) {
      Preconditions.checkState(BATCH_SIZE == 1, "BATCH_SIZE should be 1 when test latency.");
    }
    // Wait for actors to be created.
    for (ActorHandle<Receiver> receiver : receivers) {
      receiver.task(Receiver::ping).remote().get();
    }

    LOGGER.info(
        "Started executing tasks, parameters: {}," + "latency: {}, BATCH_SIZE: {}",
        parameters,
        latencyParameters,
        BATCH_SIZE);

    List<List<ObjectRef<Integer>>> returnObjects = new ArrayList<>();
    returnObjects.add(new ArrayList<>());
    returnObjects.add(new ArrayList<>());

    long startTime = System.currentTimeMillis();
    int numTasks = 0;
    long lastReport = 0;
    long totalTime = 0;
    long batchCount = 0;
    while (true) {
      long startTimeStamp = System.currentTimeMillis();
      long startTimeNano = System.nanoTime();
      numTasks++;
      boolean batchEnd = numTasks % BATCH_SIZE == 0;
      for (ActorHandle<Receiver> receiver : receivers) {
        if (parameters.isHasReturn() || batchEnd) {
          ObjectRef<Integer> returnObject;
          if (parameters.isUseDirectByteBuffer()) {
            returnObject = receiver.task(Receiver::byteBufferHasReturn, buffer).remote();
          } else if (parameters.getArgSize() > 0) {
            returnObject = receiver.task(Receiver::bytesHasReturn, bytes).remote();
          } else {
            returnObject = receiver.task(Receiver::noArgsHasReturn).remote();
          }
          returnObjects.get(1).add(returnObject);
        } else {
          if (parameters.isUseDirectByteBuffer()) {
            receiver
                .task(Receiver::byteBufferNoReturn, buffer)
                .setIgnoreReturn(parameters.isIgnoreReturn())
                .remote();
          } else if (parameters.getArgSize() > 0) {
            receiver
                .task(Receiver::bytesNoReturn, bytes)
                .setIgnoreReturn(parameters.isIgnoreReturn())
                .remote();
          } else {
            receiver
                .task(Receiver::noArgsNoReturn)
                .setIgnoreReturn(parameters.isIgnoreReturn())
                .remote();
          }
        }
        if (!latencyParameters.isEnable()) {
          Metrics.report(SOURCE_SEND_QPS, tags, 1);
        }
      }

      if (batchEnd && !latencyParameters.isEnable()) {
        batchCount++;
        long getBeginTs = System.currentTimeMillis();
        Ray.get(returnObjects.get(0));
        long rt = System.currentTimeMillis() - getBeginTs;
        totalTime += rt;
        returnObjects.set(0, returnObjects.get(1));
        returnObjects.set(1, new ArrayList<>());

        Metrics.report(SOURCE_GET_RT, tags, rt);
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (elapsedTime / 60000 > lastReport) {
          lastReport = elapsedTime / 60000;
          LOGGER.info(
              "Finished executing {} tasks in {} ms, avg get rt: {}",
              numTasks,
              elapsedTime,
              totalTime / (float) batchCount);
        }
      }

      if (latencyParameters.isEnable()) {
        Ray.get(returnObjects.get(1));
        if (numTasks % (latencyWindow + 1) != 0) {
          latencyList.add((System.nanoTime() - startTimeNano) / 1000);
        } else {
          executorService.execute(new MetricsRunnable(latencyList));
          latencyList = new LinkedList<>();
        }
        returnObjects.set(1, new ArrayList<>());
        long waitNanoTime = latencyParameters.getWaitNanoTime();
        while (System.nanoTime() - startTimeNano < waitNanoTime) {}
      }
    }
  }

  public boolean startTestMultiThread(Parameters parameters, LatencyParameters latencyParameters) {
    ExecutorService executorService = Executors.newFixedThreadPool(parameters.getThreadNum());
    for (int i = 0; i < parameters.getThreadNum(); i++) {
      executorService.execute(
          new Runnable() {
            @Override
            public void run() {
              // Wait for actors to be created.
              for (ActorHandle<Receiver> receiver : receivers) {
                receiver.task(Receiver::ping).remote().get();
              }

              byte[] bytes = null;
              if (parameters.getArgSize() > 0) {
                bytes = new byte[parameters.getArgSize()];
                new Random().nextBytes(bytes);
              } else {
                Preconditions.checkState(!parameters.isUseDirectByteBuffer());
              }

              while (true) {
                for (ActorHandle<Receiver> receiver : receivers) {

                  ObjectRef<Integer> returnObject =
                      receiver.task(Receiver::bytesHasReturnSleep5ms, bytes).remote();
                  List<ObjectRef<Integer>> returnObjects = new ArrayList<>(1);
                  returnObjects.add(returnObject);
                  WaitResult<Integer> result = Ray.wait(returnObjects, 1, 1000);
                  List<ObjectRef<Integer>> ready = result.getReady();
                  if (ready != null && ready.isEmpty()) {
                    LOGGER.error("Wait timeout.");
                  }

                  Metrics.report(SOURCE_SEND_QPS, tags, 1);
                }
              }
            }
          });
    }
    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private class MetricsRunnable implements Runnable {
    private List<Long> list;

    public MetricsRunnable(List<Long> list) {
      this.list = list;
    }

    public void run() {
      Source.LOGGER.info("Compute latency size: {}", this.list.size());
      Percentile percentileP95 = new Percentile((int) ((double) this.list.size() * 0.05D));
      Percentile percentileP99 = new Percentile((int) ((double) this.list.size() * 0.01D));
      Percentile percentileP999 = new Percentile((int) ((double) this.list.size() * 0.001D));
      Percentile percentileP9995 = new Percentile((int) ((double) this.list.size() * 5.0E-4D));
      Percentile percentileP9999 = new Percentile((int) ((double) this.list.size() * 1.0E-4D));
      this.list.forEach(
          (value) -> {
            percentileP95.add(value);
            percentileP99.add(value);
            percentileP999.add(value);
            percentileP9995.add(value);
            percentileP9999.add(value);
          });
      Source.LOGGER.info(
          "Compute latency result: P95 {} P99 {} P999 {} P9995 {} P9999 {}",
          new Object[] {
            percentileP95.getResult(),
            percentileP99.getResult(),
            percentileP999.getResult(),
            percentileP999.getResult(),
            percentileP9995.getResult(),
            percentileP9999.getResult()
          });
      Metrics.report("source.latency.p95", Source.this.tags, (double) percentileP95.getResult());
      Metrics.report("source.latency.p99", Source.this.tags, (double) percentileP99.getResult());
      Metrics.report("source.latency.p999", Source.this.tags, (double) percentileP999.getResult());
      Metrics.report(
          "source.latency.p9995", Source.this.tags, (double) percentileP9995.getResult());
      Metrics.report(
          "source.latency.p9999", Source.this.tags, (double) percentileP9999.getResult());
    }
  }
}
