import typing
import json
from abc import ABC, abstractmethod

from raystreaming.datastream import StreamSource, JavaStreamSource
from raystreaming.function import LocalFileSourceFunction, Language
from raystreaming.function import CollectionSourceFunction
from raystreaming.function import SourceFunction
from raystreaming.runtime.gateway_client import StreamingGatewayClient
from raystreaming.session import JobStatus


class StreamingContext:
    """
    Main entry point for ray streaming functionality.
    A StreamingContext is also a wrapper of java
    `io.ray.streaming.api.context.StreamingContext`
    """

    class Builder:
        def __init__(self):
            self._options = {}

        def option(self, key=None, value=None, conf=None):
            """
            Sets a config option. Options set using this method are
            automatically propagated to :class:`StreamingContext`'s own
            configuration.

            Args:
                key: a key name string for configuration property
                value: a value string for configuration property
                conf: multi key-value pairs as a dict

            Returns:
                self
            """
            if key is not None:
                assert value is not None
                self._options[key] = str(value)
            if conf is not None:
                for k, v in conf.items():
                    self._options[k] = v
            return self

        def build(self):
            """
            Creates a StreamingContext based on the options set in this
            builder.
            """
            ctx = StreamingContext()
            ctx._gateway_client.with_config(self._options)
            return ctx

    def __init__(self):
        self.__gateway_client = StreamingGatewayClient()
        self._j_ctx = self._gateway_client.create_streaming_context()

    def source(self, source_func: SourceFunction):
        """Create an input data stream with a SourceFunction

        Args:
            source_func: the SourceFunction used to create the data stream

        Returns:
            The data stream constructed from the source_func
        """
        return StreamSource.build_source(self, source_func)

    def from_values(self, *values):
        """Creates a data stream from values

        Args:
            values: The elements to create the data stream from.

        Returns:
            The data stream representing the given values
        """
        return self.from_collection(values)

    def from_collection(self, values):
        """Creates a data stream from the given non-empty collection.

        Args:
            values: The collection of elements to create the data stream from.

        Returns:
            The data stream representing the given collection.
        """
        assert values, "values shouldn't be None or empty"
        func = CollectionSourceFunction(values)
        return self.source(func)

    def java_source(self, java_source_func_class: str):
        """Create a java data stream with a qualified class name.

        Args:
            java_source_func_class: qualified class name of java SourceFunction

        Returns:
            A java StreamSource.
        """
        assert self.__gateway_client, "Gateway Client can't be null, " \
                                      "you should always invoke " \
                                      "`StreamingContext.Builder().build()`" \
                                      " before `java_source`. "
        return JavaStreamSource.build_source(self, java_source_func_class)

    def read_text_file(self, filename: str):
        """Reads the given file line-by-line and creates a data stream that
         contains a string with the contents of each such line."""
        func = LocalFileSourceFunction(filename)
        return self.source(func)

    def with_independent_operator(self,
                                  class_name,
                                  module_name,
                                  language,
                                  parallelism=1,
                                  resource={},
                                  config={},
                                  is_lazy_scheduling=False):
        self._gateway_client.with_independent_operator(
            IndependentOperatorDescriptor(class_name, module_name, language,
                                          parallelism, resource, config,
                                          is_lazy_scheduling))

    def get_independent_operators(self):
        operator_descriptors = []
        for str_result in self._gateway_client.get_independent_operators():
            operator_descriptors.append(
                IndependentOperatorDescriptor.parse_json_from_java(str_result))

        return operator_descriptors

    def submit(self, job_name: str):
        """Submit job for execution.

        Args:
            job_name: name of the job

        Returns:
            An JobSubmissionResult future
        """
        self._gateway_client.execute(job_name)
        # TODO return a JobSubmissionResult future

    def execute(self, job_name: str):
        """Execute the job. This method will block until job finished.

        Args:
            job_name: name of the job
        """
        # TODO support block to job finish
        # job_submit_result = self.submit(job_name)
        # job_submit_result.wait_finish()
        raise Exception("Unsupported")

    @property
    def _gateway_client(self):
        return self.__gateway_client

    def is_job_finished(self):
        return self._gateway_client.is_job_finished()

    def get_failover_info(self, item_cnt=1) -> \
            typing.List[typing.Tuple[int, str]]:
        """get failover info from job master."""
        return self._gateway_client.get_failover_info(item_cnt)

    def get_job_status(self) -> JobStatus:
        return JobStatus[self._gateway_client.get_job_status()]


class IndependentOperatorDescriptor:
    def __init__(self,
                 class_name,
                 module_name,
                 language,
                 parallelism=1,
                 resource={},
                 config={},
                 is_lazy_scheduling=False):
        self._class_name = class_name
        self._module_name = module_name
        self._language = language
        self._parallelism = parallelism
        self._resource = resource
        self._config = config
        self._is_lazy_scheduling = is_lazy_scheduling

    @property
    def class_name(self):
        return self._class_name

    @property
    def module_name(self):
        return self._module_name

    @property
    def language(self):
        return self._language

    @property
    def parallelism(self):
        return self._parallelism

    def set_parallelism(self, parallelism):
        self._parallelism = parallelism

    @property
    def resource(self):
        return self._resource

    def with_resource(self, resource):
        self._resource = resource

    @property
    def config(self):
        return self._config

    def with_config(self, config):
        self._config = config

    @property
    def is_lazy_scheduling(self):
        return self._is_lazy_scheduling

    def set_lazy_scheduling(self):
        self._is_lazy_scheduling = True

    @staticmethod
    def parse_json_from_java(json_str):
        json_obj = json.loads(json_str)
        return IndependentOperatorDescriptor(
            json_obj["className"], json_obj["moduleName"],
            Language[json_obj["language"]], json_obj["parallelism"],
            json_obj["resource"], json_obj["config"],
            json_obj["isLazyScheduling"])


class RuntimeContext(ABC):
    @abstractmethod
    def get_task_id(self):
        """
        Returns:
            Task id of the parallel task.
        """
        pass

    @abstractmethod
    def get_task_index(self):
        """
        Gets the index of this parallel subtask. The index starts from 0
        and goes up to  parallelism-1 (parallelism as returned by
        `get_parallelism()`).

        Returns:
            The index of the parallel subtask.
        """
        pass

    @abstractmethod
    def get_parallelism(self):
        """
        Returns:
            The parallelism with which the parallel task runs.
        """
        pass

    @abstractmethod
    def get_config(self):
        """
        Returns:
            The job config.
        """
        pass


class RuntimeContextImpl(RuntimeContext):
    def __init__(self,
                 task_id,
                 task_index,
                 parallelism,
                 metric,
                 controller_actor_handler,
                 state_backend=None,
                 **kargs):
        """Create a runtime context for function/richfunction.
        User-defined functions can fetch current information of this job and
        other utils like metric/state backend

        """
        self.task_id = task_id
        self.task_index = task_index
        self.parallelism = parallelism
        self.metric = metric
        self.controller_actor_handler = controller_actor_handler
        self.state_backend = state_backend
        self.config = kargs.get("config", {})
        self.job_config = kargs.get("job_config", {})
        self.state_manager = None

    def get_task_id(self):
        return self.task_id

    def get_task_index(self):
        return self.task_index

    def get_parallelism(self):
        return self.parallelism

    def get_metric(self):
        return self.metric

    def get_config(self):
        return self.config

    def get_job_config(self):
        return self.job_config

    def get_controller_actor_handler(self):
        return self.controller_actor_handler

    def get_key_value_state(self):
        """NOTE(lingxuan.zlx): we only support local/dfs files.
        Returns:
            The key-value state instance for storing user data from function.
        """
        return self.state_backend

    def get_state_manager(self):
        return self.state_manager

    def _set_state_manager(self, state_manager):
        self.state_manager = state_manager
