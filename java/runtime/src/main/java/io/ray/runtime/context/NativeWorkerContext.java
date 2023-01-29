package io.ray.runtime.context;

import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.TaskId;
import io.ray.api.id.UniqueId;
import io.ray.runtime.generated.Common.Address;
import io.ray.runtime.generated.Common.TaskType;
import java.nio.ByteBuffer;
import sun.nio.ch.DirectBuffer;

/** Worker context for cluster mode. This is a wrapper class for worker context of core worker. */
public class NativeWorkerContext implements WorkerContext {

  private final ThreadLocal<ClassLoader> currentClassLoader = new ThreadLocal<>();
  private final ThreadLocal<ByteBuffer> workerIdThreadLocal = new ThreadLocal<>();
  private final ThreadLocal<ByteBuffer> taskIdThreadLocal = new ThreadLocal<>();
  private final ThreadLocal<ByteBuffer> jobIdThreadLocal = new ThreadLocal<>();
  private final ThreadLocal<ByteBuffer> actorIdThreadLocal = new ThreadLocal<>();
  private final ThreadLocal<String> nodeNameThreadLocal = new ThreadLocal<>();

  @Override
  public UniqueId getCurrentWorkerId() {
    if (workerIdThreadLocal.get() == null) {
      workerIdThreadLocal.set(ByteBuffer.allocateDirect(UniqueId.LENGTH));
    }
    ByteBuffer buffer = workerIdThreadLocal.get();
    buffer.rewind();
    nativeGetCurrentWorkerId(((DirectBuffer) buffer).address());
    return UniqueId.fromByteBuffer(buffer);
  }

  @Override
  public JobId getCurrentJobId() {
    if (jobIdThreadLocal.get() == null) {
      jobIdThreadLocal.set(ByteBuffer.allocateDirect(JobId.LENGTH));
    }
    ByteBuffer buffer = jobIdThreadLocal.get();
    buffer.rewind();
    nativeGetCurrentJobId(((DirectBuffer) buffer).address());
    return JobId.fromByteBuffer(buffer);
  }

  @Override
  public ActorId getCurrentActorId() {
    if (actorIdThreadLocal.get() == null) {
      actorIdThreadLocal.set(ByteBuffer.allocateDirect(ActorId.LENGTH));
    }
    ByteBuffer buffer = actorIdThreadLocal.get();
    buffer.rewind();
    nativeGetCurrentActorId(((DirectBuffer) buffer).address());
    return ActorId.fromByteBuffer(buffer);
  }

  @Override
  public String getCurrentNodeName() {
    return nativeGetCurrentNodeName();
  }

  @Override
  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader.get();
  }

  @Override
  public void setCurrentClassLoader(ClassLoader currentClassLoader) {
    if (this.currentClassLoader.get() != currentClassLoader) {
      this.currentClassLoader.set(currentClassLoader);
    }
  }

  @Override
  public TaskType getCurrentTaskType() {
    return TaskType.forNumber(nativeGetCurrentTaskType());
  }

  @Override
  public TaskId getCurrentTaskId() {
    if (taskIdThreadLocal.get() == null) {
      taskIdThreadLocal.set(ByteBuffer.allocateDirect(TaskId.LENGTH));
    }
    ByteBuffer buffer = taskIdThreadLocal.get();
    buffer.rewind();
    nativeGetCurrentTaskId(((DirectBuffer) buffer).address());
    return TaskId.fromByteBuffer(buffer);
  }

  @Override
  public Address getRpcAddress() {
    try {
      return Address.parseFrom(nativeGetRpcAddress());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static native int nativeGetCurrentTaskType();

  private static native void nativeGetCurrentTaskId(long ptr);

  private static native void nativeGetCurrentJobId(long ptr);

  private static native void nativeGetCurrentWorkerId(long ptr);

  private static native void nativeGetCurrentActorId(long ptr);

  private static native byte[] nativeGetRpcAddress();

  private static native String nativeGetCurrentNodeName();
}
