package io.ray.runtime.actor;

import io.ray.api.ActorHandle;
import io.ray.api.id.ActorId;
import io.ray.api.id.ObjectId;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicReference;

/** Implementation of actor handle for local mode. */
public class LocalModeActorHandle implements ActorHandle, Externalizable {

  private ActorId actorId;
  private JavaFunctionDescriptor actorCreationDescriptor;

  private AtomicReference<ObjectId> previousActorTaskDummyObjectId = new AtomicReference<>();

  public LocalModeActorHandle(
      ActorId actorId,
      ObjectId previousActorTaskDummyObjectId,
      JavaFunctionDescriptor actorCreationDescriptor) {
    this.actorId = actorId;
    this.previousActorTaskDummyObjectId.set(previousActorTaskDummyObjectId);
    this.actorCreationDescriptor = actorCreationDescriptor;
  }

  /** Required by FST. */
  public LocalModeActorHandle() {}

  @Override
  public ActorId getId() {
    return actorId;
  }

  public ObjectId exchangePreviousActorTaskDummyObjectId(ObjectId previousActorTaskDummyObjectId) {
    return this.previousActorTaskDummyObjectId.getAndSet(previousActorTaskDummyObjectId);
  }

  public LocalModeActorHandle copy() {
    return new LocalModeActorHandle(
        this.actorId, this.previousActorTaskDummyObjectId.get(), actorCreationDescriptor);
  }

  public JavaFunctionDescriptor getActorCreationTaskFunctionDescriptor() {
    return actorCreationDescriptor;
  }

  @Override
  public synchronized void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(actorId);
    out.writeObject(previousActorTaskDummyObjectId.get());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    actorId = (ActorId) in.readObject();
    previousActorTaskDummyObjectId.set((ObjectId) in.readObject());
  }

  @Override
  public String getClassName() {
    return actorCreationDescriptor.className;
  }
}
