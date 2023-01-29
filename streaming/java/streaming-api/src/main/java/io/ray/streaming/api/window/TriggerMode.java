package io.ray.streaming.api.window;

/** Whether keep or discard window after trigger. */
public enum TriggerMode {

  /** The trigger emit the results, and discard it. */
  DISCARD,

  /** The trigger emit the results, and keep it. */
  KEEP
}
