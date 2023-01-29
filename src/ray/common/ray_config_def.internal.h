RAY_CONFIG(int64_t, max_redis_buffer_size, 1024 * 1024 * 32)

RAY_CONFIG(int64_t, max_command_redis_sending, 10000)

RAY_CONFIG(int64_t, redis_db_reconnect_retries, 3000)

/// Get cluster name
RAY_CONFIG(string_type, cluster_name,
           getenv("cluster_name") != nullptr ? getenv("cluster_name") : "")

/// Whether to use log reporter in event framework
RAY_CONFIG(bool, event_log_reporter_enabled, true);

/// Whether to use ceresdb exporter
RAY_CONFIG(bool, enable_ceresdb_exporter, false)

RAY_CONFIG(string_type, ceresdb_conf_file,
           getenv("RAY_CERESDB_CONFIG_FILE") != nullptr
               ? getenv("RAY_CERESDB_CONFIG_FILE")
               : "")

/// Whether to use kmonitor exporter
RAY_CONFIG(bool, enable_kmonitor_exporter, false)

RAY_CONFIG(string_type, kmonitor_conf_file,
           getenv("RAY_KMONITOR_CONFIG_FILE") != nullptr
               ? getenv("RAY_KMONITOR_CONFIG_FILE")
               : "")

/// Duration to wait between retries for failed initialization of job env.
RAY_CONFIG(uint32_t, agent_retry_interval_ms, 1000);

/// Duration to wait between retries for failed initialization with fatal error of job
/// env.
RAY_CONFIG(uint32_t, agent_retry_fatal_interval_ms, 5 * 60 * 1000);

/// Interval to monitor driver alive before registered.
RAY_CONFIG(uint32_t, agent_monitor_driver_starting_interval_ms, 1000);

/// Interval to warn the job environment has not been initialized.
RAY_CONFIG(uint32_t, agent_warn_job_initialized_interval_ms, 5 * 60 * 1000);

/// Spill over intervals.
RAY_CONFIG(uint32_t, pending_tasks_reschedule_interval_ms, 1000)

/// Dead worker processes GC intervals.
RAY_CONFIG(uint32_t, actor_worker_assignment_gc_interval_s, 60)

/// Whether to disable profiler.
RAY_CONFIG(bool, profiler_disabled,
           getenv("RAY_PROFILER_DISABLED") == nullptr ||
               getenv("RAY_PROFILER_DISABLED") == std::string("true"))

RAY_CONFIG(bool, gcs_task_scheduling_enabled,
           getenv("RAY_GCS_TASK_SCHEDULING_ENABLED") == nullptr ||
               getenv("RAY_GCS_TASK_SCHEDULING_ENABLED") != std::string("false"))

RAY_CONFIG(uint64_t, pg_bundle_default_memory_mb_gcs_scheduling,
           getenv("PG_BUNDLE_DEFAULT_MEMORY_MB_GCS_SCHEDULING") != nullptr
               ? std::stoull(getenv("PG_BUNDLE_DEFAULT_MEMORY_MB_GCS_SCHEDULING"))
               : 250)

/// This is used to check if we have enough memory when a job is submitted or when
/// a job is trying to update its required resources.
/// If (job's require memory resource) / (current available memory resource in the job's
/// nodegroup) > job_memory_check_ratio, the submission (or resource update) request will
/// be rejected.
RAY_CONFIG(double, job_memory_check_ratio, 0.95)

/// Time that gcs task scheduling would take one time at most, in millseconds. Assume cpu
/// core is 2.7GHz.
RAY_CONFIG(int64_t, gcs_task_scheduling_max_time_per_round_ms, 500)

RAY_CONFIG(int64_t, rpc_execution_alarm_time_ms,
           getenv("RPC_EXECUTION_ALARM_TIME_MS") != nullptr
               ? std::stoull(getenv("RPC_EXECUTION_ALARM_TIME_MS"))
               : 1000)

RAY_CONFIG(int64_t, gcs_main_thread_busy_detect_interval_ms,
           getenv("GCS_MAIN_THREAD_BUSY_DETECT_INTERVAL_MS") != nullptr
               ? std::stoull(getenv("GCS_MAIN_THREAD_BUSY_DETECT_INTERVAL_MS"))
               : 1000);

RAY_CONFIG(int64_t, gcs_main_thread_maximum_miss_detect_count,
           getenv("GCS_MAIN_THREAD_MAXIMUM_MISS_DETECT_COUNT") != nullptr
               ? std::stoull(getenv("GCS_MAIN_THREAD_MAXIMUM_MISS_DETECT_COUNT"))
               : 3);

RAY_CONFIG(uint64_t, job_resources_exhausted_report_interval_ms, 60000)

// ANT-INTERNAL.
RAY_CONFIG(uint64_t, placement_group_scheduling_failed_report_interval_ms, 120000)

// Max size of per job result.
RAY_CONFIG(uint32_t, max_job_result_size, 32767);

/// The interval to inspect the invalid nodegroups.
RAY_CONFIG(uint64_t, nodegroups_inspect_interval_ms, 5000)

/// This is used to config brcp max_body_size, default max size is 500MB.
RAY_CONFIG(uint64_t, brpc_max_body_size, 512 * 1024 * 1024)

// Whether or not workerpool enable watchdog for watching worker processes.
RAY_CONFIG(bool, worker_process_watchdog_enabled, true)

/// When scheduling an actor or a placement group bundle in GCS, we will first find this
/// many nodes with the largest scores and randomly choose one from them for scheduling.
RAY_CONFIG(uint64_t, num_candidate_nodes_for_scheduling,
           getenv("NUM_CANDIDATE_NODES_FOR_SCHEDULING") != nullptr
               ? std::stoull(getenv("NUM_CANDIDATE_NODES_FOR_SCHEDULING"))
               : 3)

/// The max times to retry leasing worker from one node. If this number runs out, we will
/// retry scheduling the actor again.
RAY_CONFIG(uint64_t, max_times_to_retry_leasing_worker_from_node, 3)

RAY_CONFIG(double, gcs_schedule_pack_step,
           getenv("RAY_GCS_SCHEDULE_PACK_STEP") != nullptr
               ? std::stod(getenv("RAY_GCS_SCHEDULE_PACK_STEP"))
               : 0.0)

/// The pack step for actors requiring rare resources.
RAY_CONFIG(double, rare_resources_schedule_pack_step, 1.0)

/// Enable taks reorder at receiver side. This config item is used only for test.
RAY_CONFIG(bool, task_reorder, true)

RAY_CONFIG(int64_t, task_reorder_wait_seconds, 30)

RAY_CONFIG(uint32_t, rpc_reconnect_min_interval_sec, 1)

RAY_CONFIG(uint32_t, rpc_reconnect_max_interval_sec, 60)

RAY_CONFIG(uint32_t, rpc_reconnect_max_try_connect_times, 3)

RAY_CONFIG(int64_t, hanging_tasks_threashold_ms, 600 * 1000)

RAY_CONFIG(int64_t, hanging_tasks_warning_internal_ms, 600 * 1000)

RAY_CONFIG(uint32_t, max_error_msg_size_bytes, 512 * 1024)

RAY_CONFIG(bool, enable_concurrent_resource_scheduler,
           getenv("CONCURRENT_RESOURCE_SCHEDULING_ENABLED") == nullptr ||
               getenv("CONCURRENT_RESOURCE_SCHEDULING_ENABLED") == std::string("true"))

RAY_CONFIG(size_t, minimum_nodes_for_concurrent_resoruce_scheduler, 200)

RAY_CONFIG(size_t, num_threads_for_concurrent_resoruce_scheduler, 4)

RAY_CONFIG(bool, rare_resource_scheduling_enabled,
           getenv("RARE_RESOURCE_SCHEDULING_ENABLED") != nullptr &&
               getenv("RARE_RESOURCE_SCHEDULING_ENABLED") == std::string("true"))

RAY_CONFIG(uint32_t, slow_task_print_log_interval_s, 60)

RAY_CONFIG(uint32_t, slow_actor_creation_task_send_event_interval_s, 60)

RAY_CONFIG(uint32_t, slow_non_actor_creation_task_send_event_interval_s, 60 * 5)

/// How long will raylet pend task for job env preparation
RAY_CONFIG(uint64_t, raylet_check_job_env_initialization_timeout_ms, 10000)

/// Interval(ms) to consume commands pending in the redis client.
RAY_CONFIG(int64_t, redis_waiting_commands_consume_interval_ms, 5000)

/// Minimum interval(ms) to warn the full of redis buffer.
RAY_CONFIG(int64_t, minimum_interval_to_warn_redis_buffer_full_ms, 5000)

/// Whether to enable the scheduler based on runtime resources.
RAY_CONFIG(bool, runtime_resource_scheduling_enabled, true)

RAY_CONFIG(uint32_t, runtime_resources_calculation_interval_s, 600)

RAY_CONFIG(uint32_t, runtime_resources_history_window_len_s, 24 * 60 * 60)

RAY_CONFIG(float, runtime_memory_history_window_tail, 0.99)

RAY_CONFIG(float, runtime_cpu_history_window_tail, 0.95)

RAY_CONFIG(float, runtime_memory_tail_percentile, 1.0)

RAY_CONFIG(float, runtime_cpu_tail_percentile, 0.95)

RAY_CONFIG(float, overcommit_ratio, 1.5)

RAY_CONFIG(float, node_overcommit_ratio, 1.5)

RAY_CONFIG(int64_t, job_runtime_resources_calc_interval_s, 60)

/// ServerCall instance number of each RPC service handler
RAY_CONFIG(int64_t, gcs_max_active_rpcs_per_handler, 300)

// The interval to check the gcs dead data.
RAY_CONFIG(int64_t, gcs_dead_data_check_interval_ms, 600 * 1000)

// Maximum duration in milliseconds to keep dead actor data.
RAY_CONFIG(int64_t, gcs_dead_actor_data_keep_duration_ms, 5 * 86400 * 1000)

// Maximum duration in milliseconds to keep dead node data.
RAY_CONFIG(int64_t, gcs_dead_node_data_keep_duration_ms, 7 * 86400 * 1000)

// Maximum duration in milliseconds to keep dead job data.
RAY_CONFIG(int64_t, gcs_dead_job_data_keep_duration_ms, 30LL * 86400 * 1000)

/// If enabled, raylet will report resources only when resources are changed.
RAY_CONFIG(bool, enable_light_weight_resource_report, true)

// The number of seconds to wait for the Raylet to start. This is normally
// fast, but when RAY_preallocate_plasma_memory=1 is set, it may take some time
// (a few GB/s) to populate all the pages on Raylet startup.
RAY_CONFIG(uint32_t, raylet_start_wait_time_s,
           getenv("RAY_preallocate_plasma_memory") != nullptr &&
                   getenv("RAY_preallocate_plasma_memory") == std::string("1")
               ? 120
               : 10)

// Maximum duration in milliseconds to keep dead worker data.
RAY_CONFIG(int64_t, gcs_dead_worker_data_keep_duration_ms, 600 * 1000)

// Maximum batch size when delete dead data in gcs.
RAY_CONFIG(int64_t, gcs_dead_data_max_batch_delete_size, 500)

RAY_CONFIG(int, gcs_max_number_of_consecutive_error, 30)

// The proportion by which the `mem` resource has to reserve more than the required value.
// This is because a node's total `memory` is generally less than the `mem` resource
// (specified by node shape).
RAY_CONFIG(float, mem_resource_safety_margin, 0.2)

// The weight for rare resources (used in node selector).
RAY_CONFIG(int64_t, rare_resource_weight, 10000)

RAY_CONFIG(string_type, shape_group,
           getenv("RAY_SHAPE_GROUP") != nullptr ? getenv("RAY_SHAPE_GROUP") : "")

RAY_CONFIG(string_type, pod_name, getenv("POD_NAME") != nullptr ? getenv("POD_NAME") : "")

// The ID of the physical machine on which the node is deployed.
RAY_CONFIG(string_type, machine_id,
           getenv("RAY_MACHINE_ID") != nullptr ? getenv("RAY_MACHINE_ID") : "")

/// The max minutes between rounds of l1fo interval. It means if the interval
/// is longer than `max_l1fo_interval_minutes`, the l1fo round number will be
/// assigned to 1.
RAY_CONFIG(int64_t, max_l1fo_interval_minutes, 10)

/// Whether disable the l1fo backoff mechanism.
RAY_CONFIG(bool, disable_l1fo_backoff, false)

RAY_CONFIG(int64_t, gcs_actor_backoff_max_failure_count, 16)

RAY_CONFIG(int64_t, gcs_actor_backoff_trigger_min_duration_ms, 60000)

RAY_CONFIG(int64_t, gcs_actor_backoff_min_interval_ms, 0)

RAY_CONFIG(bool, gcs_actor_restart_backoff_enabled, true)

RAY_CONFIG(int64_t, gcs_runtime_resource_flush_interval_ms, 1000)

RAY_CONFIG(int64_t, gcs_runtime_resource_flush_batch_size_each_period, 50)

RAY_CONFIG(int64_t, raylet_commit_bundle_resources_timeout_ms, 3000)

RAY_CONFIG(int64_t, gcs_actor_migration_check_in_flight_period_ms, 3000)

RAY_CONFIG(int64_t, gcs_actor_migration_reschedule_idle_bundle_period_ms, 3000)

/// The reporting requests queue size of the opentsdb exporter clent.
/// `metrics_report_batch_size` is the number of metrics reported at a time
/// while `opentsdb_report_queue_size` is the size of `OpentsdbExporterClient`
/// requesting queue which contronls the asynchronous thread requests number.
RAY_CONFIG(uint32_t, opentsdb_report_queue_size, 100)

RAY_CONFIG(int, maximum_startup_concurrency, -1)

/// The blacklist of event labels
RAY_CONFIG(string_type, event_label_blacklist, "")

/// The limit ratio of job's max_resource_requirements / min_resource_requirements.
RAY_CONFIG(float, job_resource_requirements_max_min_ratio_limit, 10.0f)

/// The default total memory of a job in MB.
RAY_CONFIG(uint64_t, default_job_total_memory_mb, 4000)

/// Maximum number of dead job in GCS server memory cache.
RAY_CONFIG(uint32_t, maximum_gcs_dead_job_cached_count, 300)

/// Interval to check slow actor scheduling.
RAY_CONFIG(uint32_t, slow_actor_scheduling_check_interval_ms, 60000)

/// Interval to send event of slow actor scheduling.
RAY_CONFIG(uint32_t, slow_actor_scheduling_send_event_interval_s, 60)

/// Time threshold considered to be slow task.
RAY_CONFIG(uint32_t, slow_actor_scheduling_duration_s, 600)

RAY_CONFIG(bool, enable_job_quota, true)

/// Whether rescheduling tasks when detecting worker registration
/// timeouts (more than `worker_registration_timeouts_limit` within
/// the last `worker_registration_timeouts_window_s` seconds).
RAY_CONFIG(bool, rescheduling_by_worker_registration_timeouts, false)

RAY_CONFIG(uint32_t, worker_registration_timeouts_limit, 5)

RAY_CONFIG(uint32_t, worker_registration_timeouts_window_s, 120)

/// The number of IO threads in core worker to process RPC replies.
RAY_CONFIG(uint32_t, worker_num_reply_io_threads, 1)

// The number of threads which push object from local spilled files.
RAY_CONFIG(int32_t, push_rpc_thread_numbers, -1)

// The max allowed size in bytes of a return object from direct actor calls.
// Objects larger than this size will be spilled/promoted to plasma.
RAY_CONFIG(int64_t, max_direct_call_returned_object_size, -1)