package io.ray.performancetest.test;

import com.google.common.base.Preconditions;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.performancetest.JobInfo;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectCreatePerfTestBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectCreatePerfTestBase.class);

  public static void run(String[] args, int numObjects, int objectSize) {
    try {
      JobInfo jobInfo = JobInfo.parseJobInfo(args);

      Preconditions.checkState(numObjects > 0);
      Preconditions.checkState(objectSize > 0);

      byte[] bytes = null;
      ByteBuffer buffer = null;
      bytes = new byte[objectSize];

      long start = System.currentTimeMillis();
      List<ObjectRef<ByteBuffer>> objects = new ArrayList<>();
      for (int i = 0; i < numObjects; i++) {
        new Random().nextBytes(bytes);
        buffer = ByteBuffer.wrap(bytes);
        objects.add(Ray.put(buffer));
      }

      Ray.get(objects);

      long timeElapsed = System.currentTimeMillis() - start;
      LOGGER.info(
          "Created {} objects with size {} on {} nodes, {} ms elapsed.",
          numObjects,
          objectSize,
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
