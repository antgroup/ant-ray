package io.ray.performancetest.test;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.performancetest.JobInfo;
import io.ray.performancetest.Parameters;
import io.ray.performancetest.Receiver;
import io.ray.performancetest.Source;
import io.ray.performancetest.test.latency.LatencyParameters;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// See https://yuque.antfin-inc.com/ray-project/core/agg4us for the description of test cases.
public class ActorPerformanceTestBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActorPerformanceTestBase.class);

  public static void run(
      String[] args,
      int[] layers,
      int[] actorsPerLayer,
      Parameters parameters,
      LatencyParameters latencyParameters) {
    System.setProperty(
        "ray.job.num-java-workers-per-process",
        String.valueOf(parameters.getNumJavaWorkerPerProcess()));
    Ray.init();
    try {
      JobInfo jobInfo = JobInfo.parseJobInfo(args);

      // TODO: Support more layers.
      Preconditions.checkState(layers.length == 2);
      Preconditions.checkState(actorsPerLayer.length == layers.length);
      for (int i = 0; i < layers.length; i++) {
        Preconditions.checkState(layers[i] > 0);
        Preconditions.checkState(actorsPerLayer[i] > 0);
      }

      int receiverActorConcurrency = 1;
      if (parameters.getThreadNum() > 1) {
        receiverActorConcurrency = 20;
      }
      List<ActorHandle<Receiver>> receivers = new ArrayList<>();
      for (int i = 0; i < layers[1]; i++) {
        int nodeIndex = layers[0] - 1 + i;
        for (int j = 0; j < actorsPerLayer[1]; j++) {
          LOGGER.info("Create Receiver nodeIndex: {}", nodeIndex);
          receivers.add(
              jobInfo
                  .assignActorToNode(
                      Ray.actor(Receiver::new, jobInfo.jobName)
                          .setMaxConcurrency(receiverActorConcurrency),
                      nodeIndex)
                  .remote());
        }
      }

      List<ActorHandle<Source>> sources = new ArrayList<>();
      for (int i = 0; i < layers[0]; i++) {
        int nodeIndex = i;
        for (int j = 0; j < actorsPerLayer[0]; j++) {
          LOGGER.info("Create Source nodeIndex: {}", nodeIndex);
          sources.add(
              jobInfo
                  .assignActorToNode(Ray.actor(Source::new, receivers, jobInfo.jobName), nodeIndex)
                  .remote());
        }
      }

      if (parameters.getThreadNum() > 1) {
        List<ObjectRef<Boolean>> results =
            sources.stream()
                .map(
                    source ->
                        source
                            .task(Source::startTestMultiThread, parameters, latencyParameters)
                            .remote())
                .collect(Collectors.toList());

        Ray.get(results);
      } else {
        List<ObjectRef<Boolean>> results =
            sources.stream()
                .map(
                    source ->
                        source.task(Source::startTest, parameters, latencyParameters).remote())
                .collect(Collectors.toList());

        Ray.get(results);
      }
    } catch (Throwable e) {
      LOGGER.error("Run test failed.", e);
      throw e;
    }
  }
}
