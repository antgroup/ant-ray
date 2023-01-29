package io.ray.api.runtimecontext;

import io.ray.api.id.JobId;

public class JobInfo {

  /// The enum values should be aligned to gcs.proto
  public enum State {
    NONE("NONE", -1),
    INIT("INIT", 0),
    SUBMITTED("SUBMITTED", 1),
    RUNNING("RUNNING", 2),
    FINISHED("FINISHED", 3),
    FAILED("FAILED", 4),
    CANCEL("CANCEL", 5);

    private String name;
    private int value;

    State(String name, int value) {
      this.name = name;
      this.value = value;
    }

    public static State fromValue(int value) {
      switch (value) {
        case 0:
          return INIT;
        case 1:
          return SUBMITTED;
        case 2:
          return RUNNING;
        case 3:
          return FINISHED;
        case 4:
          return FAILED;
        case 5:
          return CANCEL;
        default:
          return NONE;
      }
    }
  }

  /// This id of this job.
  public final JobId jobId;

  /// Whether this job is dead.
  public final boolean isDead;

  /// The int value of job state.
  public final State state;

  public JobInfo(JobId jobId, boolean isDead) {
    this.jobId = jobId;
    this.isDead = isDead;
    this.state = State.NONE;
  }

  public JobInfo(JobId jobId, boolean isDead, int stateValue) {
    this.jobId = jobId;
    this.isDead = isDead;
    this.state = State.fromValue(stateValue);
  }
}
