package io.ray.api.events;

import io.ray.api.id.ActorId;
import java.io.Serializable;

public class ActorStateEvent implements Serializable {
  public ActorId actorId;
  public ActorState currentState;

  public ActorStateEvent(ActorId actorId, ActorState currentState) {
    this.actorId = actorId;
    this.currentState = currentState;
  }

  @Override
  public String toString() {
    return String.format("{actorId=%s, state=%s}", actorId, currentState);
  }
}
