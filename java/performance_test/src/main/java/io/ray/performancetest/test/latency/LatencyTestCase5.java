package io.ray.performancetest.test.latency;

import io.ray.performancetest.Parameters;
import io.ray.performancetest.test.ActorPerformanceTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencyTestCase5 {
  public LatencyTestCase5() {}

  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(LatencyTestCase5.class);
    int[] layers = new int[] {1, 1};
    int[] actorsPerLayer = new int[] {1, 1};
    boolean hasReturn = true;
    int argSize = 1024;
    boolean useDirectByteBuffer = true;
    boolean ignoreReturn = false;
    final int numJavaWorkerPerProcess = 1;
    logger.info("arg[0] arg[1]: {}  {}", args[0], args[1]);
    logger.info("Parse TPS from arg[0]: {}", Integer.valueOf(args[1]));
    ActorPerformanceTestBase.run(
        args,
        layers,
        actorsPerLayer,
        new Parameters(
            hasReturn, argSize, useDirectByteBuffer, ignoreReturn, numJavaWorkerPerProcess, 1),
        new LatencyParameters(true, Integer.valueOf(args[1]), 5, 5L));
  }
}
