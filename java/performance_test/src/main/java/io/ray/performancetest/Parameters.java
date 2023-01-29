package io.ray.performancetest;

import java.io.Serializable;

public class Parameters implements Serializable {
  private boolean hasReturn;
  private int argSize;
  private boolean useDirectByteBuffer;
  private boolean ignoreReturn;
  private int numJavaWorkerPerProcess;
  private int threadNum;

  public Parameters(
      boolean hasReturn,
      int argSize,
      boolean useDirectByteBuffer,
      boolean ignoreReturn,
      int numJavaWorkerPerProcess,
      int threadNum) {
    this.hasReturn = hasReturn;
    this.argSize = argSize;
    this.useDirectByteBuffer = useDirectByteBuffer;
    this.ignoreReturn = ignoreReturn;
    this.numJavaWorkerPerProcess = numJavaWorkerPerProcess;
    this.threadNum = threadNum;
  }

  public boolean isHasReturn() {
    return hasReturn;
  }

  public int getArgSize() {
    return argSize;
  }

  public boolean isUseDirectByteBuffer() {
    return useDirectByteBuffer;
  }

  public boolean isIgnoreReturn() {
    return ignoreReturn;
  }

  public int getNumJavaWorkerPerProcess() {
    return numJavaWorkerPerProcess;
  }

  public int getThreadNum() {
    return threadNum;
  }

  public String toString() {
    return "has return: "
        + hasReturn
        + " arg size: "
        + argSize
        + " use direct byte buffer: "
        + useDirectByteBuffer
        + " ignore return: "
        + ignoreReturn
        + " num java worker per process: "
        + numJavaWorkerPerProcess
        + " thread num: "
        + threadNum;
  }
}
