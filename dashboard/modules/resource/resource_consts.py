import ray.ray_constants as ray_constants

# Default resource update interval value is 30 seconds.
RESOURCE_UPDATE_INTERVAL_SECONDS = ray_constants.env_integer(
    "RESOURCE_UPDATE_INTERVAL_SECONDS", 30)
