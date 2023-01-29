package io.ray.api.events;

import io.ray.api.id.ActorId;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public class ActorMigrationEvent implements Serializable {

  public long migrationId;

  /**
   * Key: node name, Value: actor ID set
   *
   * <p>Actor ID set includes all created actors in the node at the moment when a migration is
   * triggered. Note that actors with placement group in that node can still be scheduled to that
   * node after migration is triggered. You should reschedule placement group to handle such actors.
   *
   * <p>Node name indicates the node of the actor at the moment when this migration is triggered. If
   * you want to exit this actor, you should check whether this node name is the same as the current
   * node name. If they aren't, you don't need to exit this actor because it's already restarted to
   * another node.
   */
  public Map<String, Set<ActorId>> nodeNameGroupedActorId;

  @Override
  public String toString() {
    return String.format(
        "{migrationId=%s, nodeNameGroupedActorId=%s}", migrationId, nodeNameGroupedActorId);
  }
}
