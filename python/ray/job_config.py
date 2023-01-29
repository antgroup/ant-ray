import ray
from ray import ray_constants
from typing import Any, Dict, Optional
import uuid
import json

from ray.core.generated.common_pb2 import RuntimeEnv as RuntimeEnvPB
from ray.ray_constants import default_job_total_memory_mb


class JobConfig:
    """A class used to store the configurations of a job.

    Attributes:
        worker_env (dict): Environment variables to be set on worker
            processes.
        num_java_workers_per_process (int): The number of java workers per
            worker process.
        java_worker_process_default_memory_mb (int): The default memory of
            java worker process in mb.
        num_initial_java_worker_processes (int): The initial java worker
            process of the job.
        total_memory_mb (int): The total memory that the job claims.
        max_total_memory_mb (int): The upper-bound of total memory that the
            job can use.
        java_heap_fraction (float): The fraction of each java worker's heap
            size over its total memory.
        jvm_options (str[]): The jvm options for java workers of the job.
        long_running (bool): Whether to clean up actors, objects and tasks
            associated with this job during the shutdown process of the driver.
        enable_l1_fault_tolerance (bool): Whether to enable L1 fault tolerance.
        logging_level (str): Level of logging for user's code.
        code_search_path (list): A list of directories or jar files that
            specify the search path for user code. This will be used as
            `CLASSPATH` in Java and `PYTHONPATH` in Python.
        runtime_env (dict): A runtime environment dictionary (see
            ``runtime_env.py`` for detailed documentation).
        client_job (bool): A boolean represent the source of the job.
        actor_task_back_pressure_enabled (bool): Whether to enable actor task
            back pressure.
        max_pending_calls (int): The maximum actor in client queue
        """

    def __init__(
            self,
            worker_env=None,
            num_java_workers_per_process=1,
            java_worker_process_default_memory_mb=500,
            num_initial_java_worker_processes=0,
            total_memory_mb=default_job_total_memory_mb(),
            max_total_memory_mb=0,
            java_heap_fraction=0.8,
            jvm_options=None,
            long_running=False,
            enable_l1_fault_tolerance=False,
            code_search_path=None,
            runtime_env=None,
            client_job=False,
            metadata=None,
            ray_namespace=None,
            default_actor_lifetime=None,
            logging_level="INFO",
            actor_task_back_pressure_enabled=False,
            max_pending_calls=1024,
            global_owner_number=0,
    ):
        if worker_env is None:
            self.worker_env = dict()
        else:
            self.worker_env = worker_env
        self.num_java_workers_per_process = num_java_workers_per_process
        self.java_worker_process_default_memory_mb = \
            java_worker_process_default_memory_mb
        self.num_initial_java_worker_processes = \
            num_initial_java_worker_processes
        self.total_memory_mb = total_memory_mb
        self.max_total_memory_mb = max_total_memory_mb
        if max_total_memory_mb == 0:
            self.max_total_memory_mb = total_memory_mb
        self.java_heap_fraction = java_heap_fraction
        self.jvm_options = jvm_options or []
        self.long_running = long_running
        self.enable_l1_fault_tolerance = enable_l1_fault_tolerance
        self.code_search_path = code_search_path or []
        # It's difficult to find the error that caused by the
        # code_search_path is a string. So we assert here.
        assert isinstance(self.code_search_path, (list, tuple)), \
            f"The type of code search path is incorrect: " \
            f"{type(code_search_path)}"
        self.client_job = client_job
        self.metadata = metadata or {}
        self.ray_namespace = ray_namespace
        self.set_runtime_env(runtime_env)
        self.set_default_actor_lifetime(default_actor_lifetime)
        self.logging_level = logging_level
        self.actor_task_back_pressure_enabled = \
            actor_task_back_pressure_enabled
        self.max_pending_calls = max_pending_calls
        self.global_owner_number = global_owner_number

    def set_metadata(self, key: str, value: str) -> None:
        self.metadata[key] = value

    def serialize(self):
        """Serialize the struct into protobuf string"""
        job_config = self.get_proto_job_config()
        return job_config.SerializeToString()

    def set_runtime_env(self, runtime_env: Optional[Dict[str, Any]]) -> None:
        # Lazily import this to avoid circular dependencies.
        import ray._private.runtime_env as runtime_support
        if runtime_env:
            self._parsed_runtime_env = runtime_support.RuntimeEnvDict(
                runtime_env)
            self.worker_env.update(
                self._parsed_runtime_env.get_parsed_dict().get("env_vars")
                or {})
        else:
            self._parsed_runtime_env = runtime_support.RuntimeEnvDict({})
        self.runtime_env = runtime_env or dict()
        self._cached_pb = None

    def set_ray_namespace(self, ray_namespace: str) -> None:
        if ray_namespace != self.ray_namespace:
            self.ray_namespace = ray_namespace
            self._cached_pb = None

    def set_default_actor_lifetime(self, default_actor_lifetime: str) -> None:
        if default_actor_lifetime == "detached":
            self._default_actor_lifetime = \
                ray.gcs_utils.JobConfig.ActorLifetime.DETACHED
        elif (default_actor_lifetime is None
              or default_actor_lifetime == "non_detached"):
            self._default_actor_lifetime = \
                ray.gcs_utils.JobConfig.ActorLifetime.NON_DETACHED
        else:
            raise ValueError(
                "Lifetime must be one of `detached`, `non_detached` or None.")

    def get_proto_job_config(self):
        """Return the prototype structure of JobConfig"""
        if self._cached_pb is None:
            self._cached_pb = ray.gcs_utils.JobConfig()
            if self.ray_namespace is None:
                self._cached_pb.ray_namespace = str(uuid.uuid4())
            else:
                self._cached_pb.ray_namespace = self.ray_namespace

            for key in self.worker_env:
                self._cached_pb.worker_env[key] = self.worker_env[key]
            self._cached_pb.num_java_workers_per_process = (
                self.num_java_workers_per_process)
            self._cached_pb.java_worker_process_default_memory_units = (
                ray_constants.to_memory_units(
                    self.java_worker_process_default_memory_mb * 1024 * 1024,
                    round_up=False))
            self._cached_pb.num_initial_java_worker_processes = (
                self.num_initial_java_worker_processes)
            self._cached_pb.total_memory_units = (
                ray_constants.to_memory_units(
                    self.total_memory_mb * 1024 * 1024, round_up=True))
            self._cached_pb.max_total_memory_units = (
                ray_constants.to_memory_units(
                    self.max_total_memory_mb * 1024 * 1024, round_up=True))
            self._cached_pb.java_heap_fraction = self.java_heap_fraction
            self._cached_pb.jvm_options.extend(self.jvm_options)
            self._cached_pb.long_running = self.long_running
            self._cached_pb.enable_l1_fault_tolerance = (
                self.enable_l1_fault_tolerance)
            self._cached_pb.code_search_path.extend(self.code_search_path)
            self._cached_pb.runtime_env.CopyFrom(self._get_proto_runtime())
            self._cached_pb.serialized_runtime_env = \
                self.get_serialized_runtime_env()
            for k, v in self.metadata.items():
                self._cached_pb.metadata[k] = v
            self._cached_pb.logging_level = self.logging_level
            self._cached_pb.actor_task_back_pressure_enabled = \
                self.actor_task_back_pressure_enabled
            if self._default_actor_lifetime is not None:
                self._cached_pb.default_actor_lifetime = \
                    self._default_actor_lifetime
            self._cached_pb.global_owner_number = self.global_owner_number
        return self._cached_pb

    def get_runtime_env_uris(self):
        """Get the uris of runtime environment"""
        if self.runtime_env.get("uris"):
            return self.runtime_env.get("uris")
        return []

    def get_serialized_runtime_env(self) -> str:
        """Return the JSON-serialized parsed runtime env dict"""
        return self._parsed_runtime_env.serialize()

    def _get_proto_runtime(self) -> RuntimeEnvPB:
        runtime_env = RuntimeEnvPB()
        runtime_env.uris[:] = self.get_runtime_env_uris()
        runtime_env.raw_json = json.dumps(self.runtime_env)
        return runtime_env
