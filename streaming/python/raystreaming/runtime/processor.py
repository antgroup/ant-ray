import logging
from abc import ABC, abstractmethod

import raystreaming.context as context
from raystreaming import message
from raystreaming.operator import OperatorType
from raystreaming.state.state_manager import StateManager
from raystreaming.state.state_config import AntKVStateConfig
import os
logger = logging.getLogger(__name__)


class Processor(ABC):
    """The base interface for all processors."""

    @abstractmethod
    def open(self, collectors, runtime_context):
        pass

    @abstractmethod
    def process(self, record: message.Record):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def save_checkpoint(self, checkpoint_id):
        pass

    @abstractmethod
    def load_checkpoint(self, checkpoint_id):
        pass

    @abstractmethod
    def delete_checkpoint(self, checkpoint_id):
        pass

    @abstractmethod
    def forward_command(self, message):
        pass


class StreamingProcessor(Processor, ABC):
    RAY_JOB_DIR_ENV_KEY = "RAY_JOB_DIR"
    PROCESSOR_OP_NAME_KEY = "StreamingOpName"
    """StreamingProcessor is a process unit for a operator."""

    def __init__(self, operator):
        self.operator = operator
        self.collectors = None
        self.runtime_context = None
        self.state_manager = None

    def open(self, collectors, runtime_context: context.RuntimeContext):
        self.collectors = collectors
        self.runtime_context = runtime_context
        self.__init_state_manager(runtime_context)
        if self.operator is not None:
            runtime_context._set_state_manager(self.state_manager)
            self.operator.open(collectors, runtime_context)
        logger.info(f"Opened Processor {self} with collectors {collectors}")

    def __init_state_manager(self, runtime_context):
        config_mixed = {}
        config_mixed.update(runtime_context.get_config())
        config_mixed.update(runtime_context.get_job_config())

        # NOTE(lingxuan.zlx): it follows java pattern that finds ray job dir
        # first otherwise mark job dir as store and snapshot path.
        # But there is difference bettwen RAY_JOB_DIR and DATA_DIR, which means
        # DATA_DIR shares same lifecyle with process and could be cleaned after
        # process exit.
        state_base_dir = None
        try:
            import ray
            state_base_dir = ray.get_runtime_context().get_job_data_dir()
        except Exception as e:
            logger.info("Get job data dir exception {}.".format(e))

        if StreamingProcessor.RAY_JOB_DIR_ENV_KEY in os.environ:
            state_base_dir = os.environ[StreamingProcessor.RAY_JOB_DIR_ENV_KEY]

        if state_base_dir is not None:
            config_mixed[AntKVStateConfig.STATE_BACKEND_ANTKV_STORE_DIR] = (
                os.path.join(state_base_dir, "state"))

        self.state_manager = StateManager(
            jobname=config_mixed.get("streaming.job.name", "default"),
            key_group=runtime_context.get_task_index(),
            state_name=config_mixed.get(
                StreamingProcessor.PROCESSOR_OP_NAME_KEY, "default_op"),
            state_config=config_mixed,
            metric_group=runtime_context.get_metric())
        logger.info("Init state manager.")

    def close(self):
        self.operator.close()

    def forward_command(self, message):
        if self.operator is None:
            raise RuntimeError(
                "Operator isn't initialized for forward-command: {}.".format(
                    message))
        return self.operator.forward_command(message)

    def save_checkpoint(self, checkpoint_id):
        # TODO: Save checkpoint/snapshot in async mode.
        self.state_manager.snapshot(checkpoint_id)
        self.operator.save_checkpoint(checkpoint_id)

    def load_checkpoint(self, checkpoint_id):
        self.state_manager.rollback_snapshot(checkpoint_id)
        self.operator.load_checkpoint(checkpoint_id)

    def delete_checkpoint(self, checkpoint_id):
        # TODO: Delete checkpoint/snapshot in async mode.
        self.state_manager.delete_snapshot(checkpoint_id)
        self.operator.delete_checkpoint(checkpoint_id)

    def on_finish(self):
        self.operator.on_finish()

    def get_operator(self):
        return self.operator


class SourceProcessor(StreamingProcessor):
    """Processor for :class:`ray.streaming.operator.SourceOperator` """

    def __init__(self, operator):
        super().__init__(operator)

    def process(self, record):
        raise Exception("SourceProcessor should not process record")

    def fetch(self, checkpoint_id):
        self.operator.fetch(checkpoint_id)


class OneInputProcessor(StreamingProcessor):
    """Processor for stream operator with one input"""

    def __init__(self, operator):
        super().__init__(operator)

    def process(self, record):
        self.operator.process_element(record)


class MultiInputProcessor(StreamingProcessor):
    """
    Processor for stream operator with multiple input.
    Note: It's just a special one input processor currently.
    """

    def __init__(self, operator):
        super().__init__(operator)

    def process(self, record):
        self.operator.process_element(record)

    def set_upstream_runtime_context(self, right_input_vertex_ids):
        self.operator.set_upstream_runtime_context(right_input_vertex_ids)


class TwoInputProcessor(StreamingProcessor):
    """Processor for stream operator with two inputs"""

    def __init__(self, operator):
        super().__init__(operator)
        self.left_stream = None
        self.right_stream = None

    def process(self, record: message.Record):
        if record.stream == self.left_stream:
            self.operator.process_element(record, None)
        else:
            self.operator.process_element(None, record)


def build_processor(operator_instance):
    """Create a processor for the given operator."""
    operator_type = operator_instance.operator_type()
    logger.info(
        "Building StreamProcessor, operator type = {}, operator = {}.".format(
            operator_type, operator_instance))
    if operator_type == OperatorType.SOURCE:
        return SourceProcessor(operator_instance)
    elif operator_type == OperatorType.ONE_INPUT:
        return OneInputProcessor(operator_instance)
    elif operator_type == OperatorType.TWO_INPUT:
        return TwoInputProcessor(operator_instance)
    elif operator_type == OperatorType.MULTI_INPUT:
        return MultiInputProcessor(operator_instance)
    else:
        raise Exception("Current operator type is not supported")
