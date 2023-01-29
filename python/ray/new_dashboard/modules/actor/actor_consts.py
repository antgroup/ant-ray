import ray
from ray.ray_constants import env_integer

# ActorUpdater
RETRY_GET_ALL_ACTOR_INFO_INTERVAL_SECONDS = 2
ACTOR_CHANNEL = "ACTOR"
# Actor hard limit.
TOTAL_DEAD_ACTOR_COUNT_LIMIT = env_integer(
    "RAY_DASHBOARD_TOTAL_DEAD_ACTOR_COUNT_LIMIT", 10 * 10000)
# Dead Actor's TTL(seconds). If it is less than 0, means forever.
DEAD_ACTOR_TTL_SECONDS = env_integer("RAY_DASHBOARD_DEAD_ACTOR_TTL_SECONDS",
                                     7 * 24 * 3600)
# Minimum time interval for filter actor
ACTOR_FILTER_MINIMUM_INTERVAL_SECONDS = env_integer(
    "RAY_DASHBOARD_ACTOR_FILTER_MINIMUM_INTERVAL_SECONDS", 10)
NIL_NODE_ID = ray.NodeID.nil().hex()
PROCESS_BATCH_COUNT = 10000
