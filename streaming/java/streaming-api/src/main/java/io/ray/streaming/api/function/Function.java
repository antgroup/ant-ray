package io.ray.streaming.api.function;

import io.ray.streaming.api.udc.UnitedDistributedControllerActorAdapter;
import io.ray.streaming.common.generated.Common;
import java.io.Serializable;

/** Interface of streaming functions. */
public interface Function extends Serializable, UnitedDistributedControllerActorAdapter {

  /**
   * Mark this checkpoint has been finished.
   *
   * @param checkpointId
   * @throws Exception
   */
  default void finish(long checkpointId) throws Exception {}

  /**
   * Save current checkpoint info using checkpoint id.
   *
   * @param checkpointId the checkpoint id
   * @throws Exception error
   */
  default void saveCheckpoint(long checkpointId) throws Exception {}

  /**
   * Load last checkpoint info using checkpoint id.
   *
   * @param checkpointId the checkpoint id
   * @throws Exception error
   */
  default void loadCheckpoint(long checkpointId) throws Exception {}

  /**
   * Delete user-defined checkpoint by checkpoint id.
   *
   * @param checkpointId
   * @throws Exception
   */
  default void deleteCheckpoint(long checkpointId) throws Exception {}

  /**
   * Tear-down method for the user function which called after the last call to the user function.
   */
  default void close() {}

  /* UDC related start. */
  @Override
  default boolean onPrepare(Common.UnitedDistributedControlMessage controlMessage) {
    return true;
  }

  @Override
  default boolean onDisposed() {
    return true;
  }

  @Override
  default boolean onCancel() {
    return true;
  }

  @Override
  default Common.UnitedDistributedControlMessage onBlockingCommit() {
    return null;
  }

  @Override
  default void onNonBlockingCommit() {}
  /* UDC related end. */

  /**
   * User defined command that need to be executed by the function.
   *
   * @param commandMessage the command message
   */
  default void forwardCommand(String commandMessage) {};
}
