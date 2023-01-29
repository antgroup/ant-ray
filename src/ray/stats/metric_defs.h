// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/common/ray_config.h"

#pragma once

/// The definitions of metrics that you can use everywhere.
///
/// There are 4 types of metric (default window size 1min):
///   Histogram: Histogram distribution of metric points (.max, .min, .mean).
///   Gauge: Keeps the last recorded value, drops everything before (latest value).
///   Count: The count of the number of metric points (accumulation of current window).
///   Sum: A sum up of the metric points (accumulation of all history window).
///
/// You can follow these examples to define your metrics.

/// NOTE: When adding a new metric, add the metric name to the _METRICS list in
/// python/ray/tests/test_metrics_agent.py to ensure that its existence is tested.

///
/// Common
///
static Histogram GcsLatency("gcs_latency",
                            "The latency of a GCS (by default Redis) operation.", "us",
                            {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000});

///
/// Raylet Metrics
///
static Gauge ObjectStoreAvailableMemory(
    "object_store_available_memory",
    "Amount of memory currently available in the object store.", "bytes");

static Gauge ObjectStoreUsedMemory(
    "object_store_used_memory",
    "Amount of memory currently occupied in the object store.", "bytes");

static Gauge ObjectStoreLocalObjects("object_store_num_local_objects",
                                     "Number of objects currently in the object store.",
                                     "objects");

static Gauge ObjectManagerPullRequests("object_manager_num_pull_requests",
                                       "Number of active pull requests for objects.",
                                       "requests");

static Gauge ObjectDirectoryLocationSubscriptions(
    "object_directory_subscriptions",
    "Number of object location subscriptions. If this is high, the raylet is attempting "
    "to pull a lot of objects.",
    "subscriptions");

static Gauge ObjectDirectoryLocationUpdates(
    "object_directory_updates",
    "Number of object location updates per second., If this is high, the raylet is "
    "attempting to pull a lot of objects and/or the locations for objects are frequently "
    "changing (e.g. due to many object copies or evictions).",
    "updates");

static Gauge ObjectDirectoryLocationLookups(
    "object_directory_lookups",
    "Number of object location lookups per second. If this is high, the raylet is "
    "waiting on a lot of objects.",
    "lookups");

static Gauge ObjectDirectoryAddedLocations(
    "object_directory_added_locations",
    "Number of object locations added per second., If this is high, a lot of objects "
    "have been added on this node.",
    "additions");

static Gauge ObjectDirectoryRemovedLocations(
    "object_directory_removed_locations",
    "Number of object locations removed per second. If this is high, a lot of objects "
    "have been removed from this node.",
    "removals");

static Histogram HeartbeatReportMs(
    "heartbeat_report_ms",
    "Heartbeat report time in raylet. If this value is high, that means there's a high "
    "system load. It is possible that this node will be killed because of missing "
    "heartbeats.",
    "ms", {100, 200, 400, 800, 1600, 3200, 6400, 15000, 30000});

static Histogram ProcessStartupTimeMs("process_startup_time_ms",
                                      "Time to start up a worker process.", "ms",
                                      {1, 10, 100, 1000, 10000});

static Sum NumWorkersStarted(
    "internal_num_processes_started",
    "The total number of worker processes the worker pool has created.", "processes");

static Sum NumReceivedTasks(
    "internal_num_received_tasks",
    "The cumulative number of lease requeusts that this raylet has received.", "tasks");

static Sum NumDispatchedTasks(
    "internal_num_dispatched_tasks",
    "The cumulative number of lease requeusts that this raylet has granted.", "tasks");

static Sum NumSpilledTasks("internal_num_spilled_tasks",
                           "The cumulative number of lease requeusts that this raylet "
                           "has spilled to other raylets.",
                           "tasks");

static Gauge NumInfeasibleTasks(
    "internal_num_infeasible_tasks",
    "The number of tasks in the scheduler that are in the 'infeasible' state.", "tasks");

static Gauge NumInfeasibleSchedulingClasses(
    "internal_num_infeasible_scheduling_classes",
    "The number of unique scheduling classes that are infeasible.", "tasks");

static Gauge SpillingBandwidthMB("object_spilling_bandwidth_mb",
                                 "Bandwidth of object spilling.", "MB");

static Gauge RestoringBandwidthMB("object_restoration_bandwidth_mb",
                                  "Bandwidth of object restoration.", "MB");

static Gauge CurrentWorker("ray.raylet.current_worker",
                           "This metric is used for reporting states of workers."
                           "Through this, we can see the worker's state on dashboard.",
                           "1", {LanguageKey, WorkerStateKey});

static Gauge CurrentDriver("ray.raylet.current_driver",
                           "This metric is used for reporting states of drivers.", "1",
                           {LanguageKey});

static Gauge DependencyLocalObjectCount("ray.raylet.dependency_local_object_count",
                                        "Number of local objects.", "1");

static Count RedisAsyncConnectionCount("ray.raylet.redis_async_connection_count",
                                       "Number of redis async connections.", "1");

static Count RedisSyncConnectionCount("ray.raylet.redis_sync_connection_count",
                                      "Number of redis sync connections.", "1");

static Sum WorkerEvictionCount(
    "ray.raylet.worker_eviction_count",
    "The cumulative number of workers evicted (oom killed) in this node.", "workers",
    {WorkerNodeAddressKey});

///
/// GCS Server Metrics
///
static Count UnintentionalWorkerFailures(
    "unintentional_worker_failures_total",
    "Number of worker failures that are not intentional. For example, worker failures "
    "due to system related errors.",
    "");

static Count NodeFailureTotal(
    "node_failure_total", "Number of node failures that have happened in the cluster.",
    "");

static Gauge PendingActors("pending_actors", "Number of pending actors in GCS server.",
                           "actors");

static Gauge PendingPlacementGroups(
    "pending_placement_groups", "Number of pending placement groups in the GCS server.",
    "placement_groups");

static Histogram OutboundHeartbeatSizeKB("outbound_heartbeat_size_kb",
                                         "Outbound heartbeat payload size", "kb",
                                         {10, 50, 100, 1000, 10000, 100000});

static Histogram GcsUpdateResourceUsageTime(
    "gcs_update_resource_usage_time", "The average RTT of a UpdateResourceUsage RPC.",
    "ms", {1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000}, {CustomKey});

static Gauge NodeRequiredCPU("node_required_cpu",
                             "CPU required by actors and bundles in each node", "1",
                             {WorkerNodeAddressKey});

static Gauge NodeRequiredMemMB("node_required_mem_mb",
                               "Memory required by actors and bundles in each node", "MB",
                               {WorkerNodeAddressKey});

static Gauge NodeAcquiredCPU("node_acquired_cpu",
                             "CPU acquired by actors and bundles in each node", "1",
                             {WorkerNodeAddressKey});

static Gauge NodeAcquiredMemMB("node_acquired_mem_mb",
                               "Memory acquired by actors and bundles in each node", "MB",
                               {WorkerNodeAddressKey});

static Gauge NodeRuntimeCPU("node_runtime_cpu", "Runtime CPU used by actors in each node",
                            "1", {WorkerNodeAddressKey});

static Gauge NodeRuntimeMemMB("node_runtime_mem_mb",
                              "Runtime memory used by actors in each node", "MB",
                              {WorkerNodeAddressKey});

static Gauge JobRequiredCPU("job_required_cpu",
                            "CPU required by job's actors and bundles", "1",
                            {JobNameKey});

static Gauge JobRequiredMemMB("job_required_mem_mb",
                              "Memory required by job's actors and bundles", "MB",
                              {JobNameKey});

static Gauge JobAcquiredCPU("job_acquired_cpu",
                            "CPU acquired by job's actors and bundles", "1",
                            {JobNameKey});

static Gauge JobAcquiredMemMB("job_acquired_mem_mb",
                              "Memory acquired by job's actors and bundles", "MB",
                              {JobNameKey});

static Gauge JobRuntimeCPU("job_runtime_cpu", "Job's runtime cpu used by actors", "1",
                           {JobNameKey});

static Gauge JobRuntimeMemMB("job_runtime_mem_mb", "Job's runtime memory used by actors",
                             "MB", {JobNameKey});

static Gauge JobTotalCPU("job_total_cpu", "Total cpu declared by the job", "1",
                         {JobNameKey});

static Gauge JobTotalMemMB("job_total_mem_mb", "Total memory declared by the job", "MB",
                           {JobNameKey});

///
/// Core Worker Metrics
///
static Gauge WorkerStatus("ray.core_worker.worker_status",
                          "Stat the status of a core worker.", "1");

static Gauge SubmittedTasksCount("ray.core_worker.submitted_tasks_count",
                                 "Number of tasks submitted by the core worker.", "1",
                                 {WorkerIdKey});

static Gauge ReceivedTasksCount("ray.core_worker.received_tasks_count",
                                "Number of tasks received by the core worker.", "1",
                                {WorkerIdKey});

static Gauge SubmittedTasksPerFuncCount(
    "ray.core_worker.submitted_tasks_per_func_count",
    "Number of tasks submitted by a function of the core worker.", "1",
    {WorkerIdKey, FunctionKey});

static Gauge ReceivedTasksPerFuncCount(
    "ray.core_worker.received_tasks_per_func_count",
    "Number of tasks received by a function of the core worker.", "1",
    {WorkerIdKey, FunctionKey});

static Gauge TaskQueueLength(
    "ray.core_worker.task_queue_length",
    "Number of tasks received by the core worker but haven't been executed.", "1",
    {WorkerIdKey});

static Gauge ExecutedTasksCount("ray.core_worker.num_executed_tasks",
                                "Number of executed tasks.", "1", {WorkerIdKey});

static Gauge SubmittedDirectActorCallsPostingCount(
    "ray.core_worker.submitted_direct_actor_calls_posting_count",
    "Number of submitted direct actor calls waiting to be posted.", "1", {WorkerIdKey});

static Gauge ReceivedDirectActorCallsPostingCount(
    "ray.core_worker.received_direct_actor_calls_posting_count",
    "Number of received direct actor calls waiting to be posted.", "1", {WorkerIdKey});

static Gauge ObjectInStoreCount("ray.core_worker.objects_in_store_count",
                                "Number of objects in store.", "1");

static Gauge ObjectInStoreBytes("ray.core_worker.objects_in_store_bytes",
                                "Bytes of objects in store.", "bytes");

static Gauge LogicWorkerCount("ray.core_worker.logic_worker_count",
                              "Number of logic worker.", "1");

static Gauge BackPressureCount("ray.core_worker.backpressure_count",
                               "Number of backpressure occurs", "1", {WorkerIdKey});

static Gauge MemoryStoreHitRate("ray.core_worker.memory_store_hit_rate",
                                "The hit rate of memory store", "0");

/// The rate of objects which can be got from local node, do not fetch from remote nodes.
static Gauge LocalObjectHitRate("ray.core_worker.local_object_hit_rate",
                                "The hit rate of local store", "0");

///
/// Plasma Store Metrics
///
static Count PlasmaCreateRequestCount("ray.plasma.create_request_count",
                                      "Number of plasma creating reqeusts.", "1");

static Count PlasmaCreateRequestSize("ray.plasma.total_created_object_size",
                                     "Size in bytes of all objects created in plasma, "
                                     "including removed or evicted objects.",
                                     "1");

static Count PlasmaGetRequestCount("ray.plasma.get_request_count",
                                   "Number of plasma getting requests.", "1");

static Count PlasmaReleaseRequestCount("ray.plasma.release_request_count",
                                       "Number of plasma releasing requests.", "1");

static Count PlasmaDeleteRequestCount("ray.plasma.delete_request_count",
                                      "Number of plasma deleting requests.", "1");

static Count PlasmaContainsRequestCount("ray.plasma.contains_request_count",
                                        "Number of plasma containing requests.", "1");

static Count PlasmaSealRequestCount("ray.plasma.seal_request_count",
                                    "Number of plasma sealed requests.", "1");

static Count PlasmaEvictRequestCount("ray.plasma.evict_request_count",
                                     "Number of plasma evicted requests.", "1");

static Count PlasmaSubscribeRequestCount("ray.plasma.subscribe_request_count",
                                         "Number of plasma subscribed requests.", "1");

static Count PlasmaConnectRequestCount("ray.plasma.connect_request_count",
                                       "Number of plasma connected requests.", "1");

static Count PlasmaDisconnectClientCount("ray.plasma.disconnect_client_count",
                                         "Number of plasma disconnected clients.", "1");

static Count PlasmaSetOptionsRequestCount("ray.plasma.set_options_request_count",
                                          "Number of plasma setting options requests.",
                                          "1");

static Count PlasmaGetDebugStringRequestCount(
    "ray.plasma.get_debug_string_request_count",
    "Number of plasma getting debug string requests.", "1");

static Gauge PlasmaObjectCount("ray.plasma.object_count", "Number of plasma objects.",
                               "1");

static Gauge PlasmaAllocatedMemory("ray.plasma.allocated_memory_mb",
                                   "Allocated memory of plasma in MB.", "Mi");

static Gauge PlasmaTotalMemory("ray.plasma.total_memory_mb",
                               "Total memory of plasma in MB.", "Mi");

static Count PlasmaEvictedObjectCount("ray.plasma.evicted_object_count",
                                      "Number of plasma evicted objects.", "1");

static Count PlasmaEvictObjectSize("ray.plasma.evict_object_size",
                                   "Data size in bytes of plasma evict objects.", "1");

static Count PlasmaSpilledObjectCount("ray.plasma.spilled_object_count",
                                      "Number of plasma spill objects.", "1");

static Count PlasmaPinnedObjectRequestCount("ray.plasma.pinned_object_request_count",
                                            "Number of plasma pin object reqeusts.", "1");

static Gauge PlasmaPinnedObjectCount("ray.plasma.pinned_object_count",
                                     "Number of plasma pin objects count.", "1");

static Gauge PlasmaPinObjectSize("ray.plasma.pin_object_size",
                                 "Data size in bytes of plasma pin objects.", "1");

static Gauge PlasmaSpillObjectSize("ray.plasma.spill_object_size",
                                   "The data size of spilled objects", "1");

static Gauge PlasmaSpillObjectCount("ray.plasma.spill_object_count",
                                    "The spilled objects count", "1");

static Gauge PlasmaRestoreObjectSize("ray.plasma.restore_object_size",
                                     "The data size of restored objects", "1");

static Gauge PlasmaRestoreObjectCount("ray.plasma.restore_object_count",
                                      "The restored objects count", "1");

// The time cost when fetch object from store, including local store and fetch
// from remote store.
static Gauge PlasmaGetTimeCostP99("ray.plasma.plasma_get_time_cost_p99",
                                  "The p99 time cost of getting from plasma store (ms).",
                                  "1");

static Gauge PlasmaGetTimeCostP95("ray.plasma.plasma_get_time_cost_p95",
                                  "The p95 time cost of getting from plasma store (ms).",
                                  "1");

static Gauge PlasmaGetTimeCostP90("ray.plasma.plasma_get_time_cost_p90",
                                  "The p90 time cost of getting from plasma store (ms).",
                                  "1");

// The time cost when fetch object from memory store.
static Gauge MemoryStoreGetTimeCostP99(
    "ray.core_worker.memory_store_get_time_cost_p99",
    "The p99 time cost of getting from memory store (ms).", "1");

static Gauge MemoryStoreGetTimeCostP95(
    "ray.core_worker.memory_store_get_time_cost_p95",
    "The p95 time cost of getting from memory store (ms).", "1");

static Gauge MemoryStoreGetTimeCostP90(
    "ray.core_worker.memory_store_get_time_cost_p90",
    "The p90 time cost of getting from memory store (ms).", "1");

///
/// GCS Metrics
///
static Count ActorCreationCount("ray.gcs_server.actor_creation_count",
                                "Number of actor creation.", "1", {JobNameKey});

static Histogram ActorCreationElapsedTime(
    "ray.gcs_server.actor_creation_elapsed_time", "The elapsed time of actor creation.",
    "us", {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}, {JobNameKey});

static Count PGCreationCount("ray.gcs_server.pg_creation_count",
                             "Number of placement group creation.", "1");

static Histogram PGCreationElapsedTime(
    "ray.gcs_server.pg_creation_elapsed_time",
    "The elapsed time of placement group creation.", "us",
    {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}, {PlacementGroupIdKey});

static Count RayletCrashCount("ray.gcs_server.raylet_crash_count",
                              "Number of raylet crash.", "1");

static Count WorkerCrashCount("ray.gcs_server.worker_crash_count",
                              "Number of worker crash.", "1");

static Count RequestReceivedCount("ray.gcs_server.request_received_count",
                                  "Number of request received.", "1");

static Histogram GcsServerFailoverElapsedTime(
    "ray.gcs_server.gcs_server_failover_elapsed_time",
    "The elapsed time of gcs failover.", "us",
    {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000});

static Count ActorFailoverCount("ray.gcs_server.actor_failover_count",
                                "Number of actor failover.", "1", {JobNameKey});

static Count ActorFailoverSuccessCount("ray.gcs_server.actor_failover_success_count",
                                       "Number of actor successful failover.", "1",
                                       {JobNameKey});

static Histogram ActorFailoverElapsedTime(
    "ray.gcs_server.actor_failover_elapsed_time", "The elapsed time of actor failover.",
    "us", {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}, {JobNameKey});

static Gauge ActorStateGauge("ray.gcs_server.actor_state_sum", "Number of actor state.",
                             "1", {ActorStateKey, JobNameKey});

static Sum ActorFailoverSum("ray.gcs_server.actor_failover_sum",
                            "Total number of actor failover.", "1", {JobNameKey});

static Sum ActorFailoverSuccessSum("ray.gcs_server.actor_failover_success_sum",
                                   "Total number of actor failover successfully.", "1",
                                   {JobNameKey});

static Count AllocateResourceFailedCount("ray.gcs_server.allocate_resource_failed_count",
                                         "Times of resource allocate failed.", "1",
                                         {JobNameKey});

static Count ResourceSchedulingCount("ray.gcs_server.resource_scheduling_count",
                                     "Times of resource scheduling.", "1");

static Count ResourceSchedulingSuccessCount(
    "ray.gcs_server.resource_scheduling_success_count",
    "Number of successful resource scheduling.", "1");

static Count ResourceSchedulingFailedCount(
    "ray.gcs_server.resource_scheduling_failed_count",
    "Number of resource scheduling failures.", "1");

static Histogram ResourceSchedulingTimeConsumption(
    "ray.gcs_server.resource_scheduling_time_consumption",
    "Time consumption of resource scheduling.", "us",
    {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000});

static Gauge GcsPendingActorCount("ray.gcs_server.pending_actor_count",
                                  "Number of actors pending at gcs side.", "1");

static Gauge CurrentLeasedWorkers(
    "ray.raylet.leased_workers",
    "This metric is used for reporting the number of leased workers at raylet side.", "1",
    {LeasedWorkerTypeKey});

static Gauge NumTasksToSchedule(
    "internal_num_tasks_to_schedule",
    "The number of tasks in the scheduler that are waiting for scheduling", "tasks");

static Gauge NumTasksToDispatch(
    "internal_num_tasks_to_dispatch",
    "The number of tasks in the scheduler that are waiting for diapatching.", "tasks");

static Gauge NumWaitingTasksQueueLength(
    "internal_waiting_tasks_queue_length",
    "The length of waiting task queue in the scheduler.", "tasks");

static Gauge NumExecutingTasks("internal_num_executing_tasks",
                               "The number of tasks in the scheduler that are executing.",
                               "tasks");

static Gauge NumPinnedTasks("internal_num_pinned_tasks",
                            "The number of pinned tasks in the scheduler.", "tasks");

static Count L1FOCount("ray.gcs_server.l1fo_count", "Number of L1FO.", "1", {JobNameKey});

///
/// Event Metrics
///
static Count EventCount("ray.event.event_count", "Number of event generation.", "1",
                        {JobNameKey, EventLabelKey, EventSourceTypeKey,
                         EventSeverityTypeKey});
