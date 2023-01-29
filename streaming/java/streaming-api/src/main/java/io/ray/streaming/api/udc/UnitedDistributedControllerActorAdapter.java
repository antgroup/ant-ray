package io.ray.streaming.api.udc;

import io.ray.streaming.common.generated.Common;

/**
 * UnitedDistributedControllerActorAdapter is the operator interface of UDC, which is responsible
 * for executing specific actions for the different phases.
 */
public interface UnitedDistributedControllerActorAdapter {

  /**
   * In Non-blocking mode, Will be invoked once the specific UDC controller generated a control
   * message. In Blocking mode, Will be invoked when a worker received a control msg from upstream.
   *
   * @return A flag that indicate whether the process was successfully done or not.
   */
  boolean onPrepare(Common.UnitedDistributedControlMessage controlMessage);

  /**
   * In blocking mode, will be invoked once a worker received Control Messages from all the upstream
   * channels.
   *
   * @return A new Control Message that will be pushed to downstream.
   */
  Common.UnitedDistributedControlMessage onBlockingCommit();

  /**
   * In Non-Blocking mode, will be invoked by the UDC lifetime manager after all the preparation
   * phases are successfully done.
   */
  void onNonBlockingCommit();

  /**
   * In Non-blocking mode, will be invoked when all workers executed commit phase successfully. In
   * Blocking mode, will be invoked when UDC lifetime manager received all result from sink workers.
   *
   * @return A flag that indicate whether the process was successfully done or not.
   */
  boolean onDisposed();

  /**
   * Will be invoked when any of the workers occurred exception, whether it was in blocking mode or
   * non-blocking mode.
   *
   * @return A flag that indicate whether the process was successfully done or not.
   */
  boolean onCancel();
}
