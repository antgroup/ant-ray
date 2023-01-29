package io.ray.streaming.api.window.trigger;

import io.ray.streaming.operator.AbstractWindowOperator;

/**
 * Result for {@link Trigger}. {@link AbstractWindowOperator} will process the data according to the
 * result.
 */
public enum TriggerResult {

  /** No action is taken on the window. */
  CONTINUE(false, false),

  /**
   * {@code FIRE_AND_PURGE} evaluates the window function and emits the window result.
   *
   * <p>Discard mode in beam.
   */
  FIRE_AND_PURGE(true, true),

  /**
   * On {@code FIRE}, the window is evaluated and results are emitted. The window is not purged,
   * though, all elements are retained.
   */
  FIRE(true, false),

  /**
   * All elements in the window are cleared and the window is discarded, without evaluating the
   * window function or emitting any elements.
   */
  PURGE(false, true);

  private final boolean fire;
  private final boolean purge;

  TriggerResult(boolean fire, boolean purge) {
    this.purge = purge;
    this.fire = fire;
  }

  public static TriggerResult of(boolean isFire, boolean isPurge) {
    for (TriggerResult triggerResult : TriggerResult.class.getEnumConstants()) {
      if (triggerResult.isFire() == isFire && triggerResult.isPurge() == isPurge) {
        return triggerResult;
      }
    }
    return TriggerResult.CONTINUE;
  }

  public boolean isFire() {
    return fire;
  }

  public boolean isPurge() {
    return purge;
  }

  public TriggerResult merge(TriggerResult result) {
    boolean isFire = fire && result.isFire();
    boolean isPurge = purge && result.isPurge();
    return of(isFire, isPurge);
  }
}
