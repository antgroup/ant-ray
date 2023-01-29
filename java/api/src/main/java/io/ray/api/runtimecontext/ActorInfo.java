package io.ray.api.runtimecontext;

import io.ray.api.events.ActorState;
import io.ray.api.id.ActorId;
import java.util.Map;

public class ActorInfo {
  public final ActorId actorId;

  public final ActorState state;

  public final long numRestarts;

  public final Address address;

  public final String name;

  public final Map<String, String> extendedProperties;

  public ActorInfo(
      ActorId actorId,
      ActorState state,
      long numRestarts,
      Address address,
      String name,
      Map<String, String> extendedProperties) {
    this.actorId = actorId;
    this.state = state;
    this.numRestarts = numRestarts;
    this.address = address;
    this.name = name;
    this.extendedProperties = extendedProperties;
  }
}
