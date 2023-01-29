from ray.ray_constants import env_bool, env_integer

MIME_TYPES = {
    "text/plain": [".err", ".out", ".log"] + [f".{i}" for i in range(1, 10)],
}

# Log cleaning
RAY_LOG_CLEANUP_ENABLED = env_bool("RAY_LOG_CLEANUP_ENABLED", True)
RAY_LOG_CLEANUP_INTERVAL_IN_SECONDS = \
    env_integer("RAY_LOG_CLEANUP_INTERVAL_IN_SECONDS", 3600)
RAY_MAXIMUM_NUMBER_OF_LOGS_TO_KEEP = env_integer(
    "RAY_MAXIMUM_NUMBER_OF_LOGS_TO_KEEP", 1000)
RAY_LOGS_TTL_SECONDS = env_integer("RAY_LOGS_TTL_SECONDS", 6 * 24 * 3600)
RAY_PER_JOB_DEAD_PROCESS_LOG_NUMBER_LIMIT = \
    env_integer("RAY_PER_JOB_DEAD_PROCESS_LOG_NUMBER_LIMIT", 500)
