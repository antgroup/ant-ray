package io.ray.streaming.client;

import io.ray.api.ActorHandle;
import io.ray.streaming.jobgraph.JobGraph;
import java.util.Map;

/** Interface of the job client. */
public interface JobClient {

  /**
   * Submit job with logical plan and configuration.
   *
   * @param jobGraph The logical plan.
   * @param conf The configuration of job.
   */
  void submitJob(JobGraph jobGraph, Map<String, String> conf);

  /**
   * Check whether job has finished.
   *
   * @return finished
   */
  boolean isJobFinished();

  /**
   * Destroy all workers in current job.
   *
   * @return void
   */
  boolean shutdownAllWorkers();

  /**
   * Get last success checkpoint id
   *
   * @return checkpoint id
   */
  Long getLastValidCheckpointId();

  ActorHandle getJobMaster();
}
