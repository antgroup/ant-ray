package io.ray.performancetest.test;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.performancetest.DummyActor;
import io.ray.performancetest.JobInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActorCreatePerfTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(ActorCreatePerfTestBase.class);

  /**
   * Run test.
   *
   * @param actorsPerNode #actors to be created on each nodes.
   * @param flatten actors will be distributed flatten if true, otherwise they random pick node to
   *     run.
   */
  public static void run(String[] args, int actorsPerNode, boolean flatten) {
    try {
      JobInfo jobInfo = JobInfo.parseJobInfo(args);

      Preconditions.checkState(actorsPerNode > 0);

      List<ActorHandle<DummyActor>> actors = new ArrayList<>();
      for (int j = 0; j < jobInfo.nodes.size(); j++) {
        for (int i = 0; i < actorsPerNode; i++) {
          if (flatten) {
            actors.add(jobInfo.assignActorToNode(Ray.actor(DummyActor::new), j).remote());
          } else {
            actors.add(Ray.actor(DummyActor::new).remote());
          }
        }
      }

      long start = System.currentTimeMillis();
      List<ObjectRef<Boolean>> results =
          actors.stream()
              .map(actor -> actor.task(DummyActor::ping).remote())
              .collect(Collectors.toList());

      Ray.get(results);
      long timeElapsed = System.currentTimeMillis() - start;
      LOGGER.info(
          "Created {} actors(on each) on {} nodes, {} ms elapsed.",
          actorsPerNode,
          jobInfo.nodes.size(),
          timeElapsed);
      // For metric collecting.
      LOGGER.info("{}:{}", jobInfo.jobName, timeElapsed);
    } catch (Throwable e) {
      LOGGER.error("Run test failed.", e);
      throw e;
    }
  }
}
