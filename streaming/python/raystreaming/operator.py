import enum
import importlib
import logging
import sys
import threading
import time
from abc import ABC, abstractmethod

from raystreaming import function
from raystreaming import message
from raystreaming.constants import StreamingConstants as constants
from raystreaming.collector import CollectionCollector
from raystreaming.collector import Collector
from raystreaming.function import SourceFunction
from raystreaming.runtime import gateway_client
from raystreaming.runtime.control_message import (
    ControlMessageType,
    ControlMessage,
)
logger = logging.getLogger(__name__)


class OperatorType(enum.Enum):
    SOURCE = 0  # Sources are where your program reads its input from
    ONE_INPUT = 1  # This operator has one data stream as it's input stream.
    TWO_INPUT = 2  # This operator has two data stream as it's input stream.
    MULTI_INPUT = 3  # Operator has multiple data stream as it's input stream.


class Operator(ABC):
    """
    Abstract base class for all operators.
    An operator is used to run a :class:`function.Function`.
    """

    def __init__(self):
        self.op_config = None
        self.stream = None

    @abstractmethod
    def open(self, collectors, runtime_context):
        pass

    @abstractmethod
    def finish(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def operator_type(self) -> OperatorType:
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

    def forward_command(self, message):
        pass

    # united distributed controller function start.
    def on_prepare(self, msg):
        pass

    def on_commit(self):
        pass

    def on_disposed(self):
        pass

    def on_cancel(self):
        pass

    # united distributed controller function end.

    def is_ready_rescaling(self):
        message = {}
        message[constants.OPERATOR_COMMAND_FUNC_NAME] = \
            sys._getframe().f_code.co_name
        control_message = \
            ControlMessage(ControlMessageType.OPERATOR_COMMAND, message)
        try:
            return self.forward_command(control_message)
        except Exception as ex:
            logger.warn(f"An error occured when call forward_command: {ex}.")
            return True

    def set_op_config(self, op_config):
        self.op_config = op_config

    def set_output_schema(self, schema):
        logger.info(f"Create encoder for operator {self} with schema {schema}")
        self.output_schema = schema
        import pyfury as fury
        encoder = fury.create_row_encoder(schema)
        prev_open = self.open

        class ProxyCollector(Collector):
            def __init__(self, collector):
                self.collector = collector

            def collect(self, value):
                value.value = encoder.to_row(value.value).to_bytes()
                self.collector.collect(value)

        def open(collectors, runtime_context):
            collectors = [ProxyCollector(c) for c in collectors]
            prev_open(collectors, runtime_context)

        self.open = open

    # TODO(loushang.ls): Should it be here?
    def set_stream(self, stream):
        self.stream = stream


class OneInputOperator(Operator, ABC):
    """Interface for stream operators with one input."""

    @abstractmethod
    def process_element(self, record):
        pass

    def operator_type(self):
        return OperatorType.ONE_INPUT

    def set_input_schema(self, schema):
        logger.info(f"Create encoder for operator {self} with schema {schema}")
        self.input_schema = schema
        import pyfury as fury
        encoder = fury.create_row_encoder(schema)
        prev_process_element = self.process_element

        def process_element(record):
            buf = fury.Buffer(record.value, 0, len(record.value))
            row = fury.RowData(schema, buf)
            record.value = encoder.from_row(row)
            prev_process_element(record)

        self.process_element = process_element


class TwoInputOperator(Operator, ABC):
    """Interface for stream operators with two input"""

    @abstractmethod
    def process_element(self, left_record, right_record):
        pass

    def operator_type(self):
        return OperatorType.TWO_INPUT


class MultiInputOperator(Operator, ABC):
    """Interface for stream operators with multiple input"""

    @abstractmethod
    def process_element(self, record: message.KeyRecord):
        pass

    def operator_type(self):
        return OperatorType.MULTI_INPUT


class StreamOperator(Operator, ABC):
    """
    Basic interface for stream operators. Implementers would implement one of
    :class:`OneInputOperator` or :class:`TwoInputOperator` to to create
    operators that process elements.
    """

    def __init__(self, func):
        super().__init__()
        self.func = func
        self.collectors = None
        self.runtime_context = None
        self.next_operators_id = []
        self.operator_id = None

    def check_func(func):
        def wrapper(*args, **kwargs):
            instance = args[0]
            if instance.func is None:
                raise RuntimeError("Operator's function isn't initialized.")
            return func(*args, **kwargs)

        return wrapper

    @check_func
    def open(self, collectors, runtime_context):
        self.collectors = collectors
        self.runtime_context = self.__create_runtime_context(runtime_context)
        self.func.open(self.runtime_context)
        self.func.set_master_actor(
            runtime_context.get_controller_actor_handler())

    def __create_runtime_context(self, runtime_context):
        def get_config():
            return self.op_config

        runtime_context.get_config = get_config
        return runtime_context

    @check_func
    def forward_command(self, message):
        return self.func.forward_command(message)

    # united distributed controller function start.
    @check_func
    def on_prepare(self, msg):
        return self.func.on_prepare(msg)

    @check_func
    def on_commit(self):
        return self.func.on_commit()

    @check_func
    def on_disposed(self):
        return self.func.on_disposed()

    @check_func
    def on_cancel(self):
        return self.func.on_cancel()

    # united distributed controller function end.

    def finish(self):
        pass

    def close(self):
        self.func.close()

    def collect(self, record):
        for collector in self.collectors:
            collector.collect(record)

    def set_id(self, op_id):
        self.operator_id = op_id

    def get_id(self):
        return self.operator_id

    def set_next_operators_id_list(self, next_operators_id):
        self.next_operators_id = next_operators_id

    def get_next_operators_id_list(self):
        return self.next_operators_id

    def save_checkpoint(self, checkpoint_id):
        self.func.save_checkpoint(checkpoint_id)

    def load_checkpoint(self, checkpoint_id):
        self.func.load_checkpoint(checkpoint_id)

    def delete_checkpoint(self, checkpoint_id):
        self.func.delete_checkpoint(checkpoint_id)

    def on_finish(self):
        self.func.on_finish()


class SourceOperator(Operator, ABC):
    @abstractmethod
    def fetch(self, checkpoint_id):
        pass


class SourceOperatorImpl(SourceOperator, StreamOperator):
    """
    Operator to run a :class:`function.SourceFunction`
    """

    class SourceContextImpl(function.SourceContext):
        def __init__(self, collectors):
            self.collectors = collectors

        def collect(self, value):
            for collector in self.collectors:
                collector.collect(message.Record(value))

    def __init__(self, func: SourceFunction):
        assert isinstance(func, function.SourceFunction)
        super().__init__(func)
        self.source_context = None

    def open(self, collectors, runtime_context):
        super().open(collectors, runtime_context)
        self.source_context = SourceOperatorImpl.SourceContextImpl(collectors)
        self.func.init(runtime_context.get_parallelism(),
                       runtime_context.get_task_index())

    def fetch(self, checkpoint_id):
        self.func.fetch(self.source_context, checkpoint_id)

    def save_checkpoint(self, checkpoint_id):
        self.func.save_checkpoint(checkpoint_id)

    def load_checkpoint(self, checkpoint_id):
        self.func.load_checkpoint(checkpoint_id)

    def operator_type(self):
        return OperatorType.SOURCE


class MapOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.MapFunction`
    """

    def __init__(self, map_func: function.MapFunction):
        assert isinstance(map_func, function.MapFunction)
        super().__init__(map_func)

    def process_element(self, record):
        self.collect(message.Record(self.func.map(record.value)))


class FlatMapOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.FlatMapFunction`
    """

    def __init__(self, flat_map_func: function.FlatMapFunction):
        assert isinstance(flat_map_func, function.FlatMapFunction)
        super().__init__(flat_map_func)
        self.collection_collector = None

    def open(self, collectors, runtime_context):
        super().open(collectors, runtime_context)
        self.collection_collector = CollectionCollector(collectors)

    def process_element(self, record):
        self.func.flat_map(record.value, self.collection_collector)


class ProcessOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.ProcessFunction`
    """

    def __init__(self, process_func: function.ProcessFunction):
        assert isinstance(process_func, function.ProcessFunction)
        super().__init__(process_func)
        self.collection_collector = None

    def open(self, collectors, runtime_context):
        super().open(collectors, runtime_context)
        self.collection_collector = CollectionCollector(collectors)

    def process_element(self, record):
        self.func.process(record.value, self.collection_collector)


class FilterOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.FilterFunction`
    """

    def __init__(self, filter_func: function.FilterFunction):
        assert isinstance(filter_func, function.FilterFunction)
        super().__init__(filter_func)

    def process_element(self, record):
        if self.func.filter(record.value):
            self.collect(record)


class KeyByOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.KeyFunction`
    """

    def __init__(self, key_func: function.KeyFunction):
        assert isinstance(key_func, function.KeyFunction)
        super().__init__(key_func)

    def process_element(self, record):
        key = self.func.key_by(record.value)
        self.collect(message.KeyRecord(key, record.value, self.stream))


class ReduceOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.ReduceFunction`
    """

    def __init__(self, reduce_func: function.ReduceFunction):
        assert isinstance(reduce_func, function.ReduceFunction)
        super().__init__(reduce_func)
        self.reduce_state = None

    def open(self, collectors, runtime_context):
        super().open(collectors, runtime_context)
        self.reduce_state = runtime_context.get_state_manager(
        ).get_key_value_state()

    def process_element(self, record: message.KeyRecord):
        key = record.key
        value = record.value
        old_value = self.reduce_state.get(key)
        if old_value is not None:
            new_value = self.func.reduce(old_value, value)
            self.reduce_state.put(key, new_value)
            self.collect(message.Record(new_value))
        else:
            self.reduce_state.put(key, value)
            self.collect(record)


class SinkOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.SinkFunction`
    """

    def __init__(self, sink_func: function.SinkFunction):
        assert isinstance(sink_func, function.SinkFunction)
        super().__init__(sink_func)

    def process_element(self, record):
        self.func.sink(record.value)


class JoinOperator(StreamOperator, TwoInputOperator):
    """
    Operator to run a :class:`function.JoinFunction`
    NOTE(loushang.ls) We only consider the memory state currently.
    """

    def __init__(self, join_func: function.JoinFunction):
        super().__init__(join_func)
        self.left_record_list_map = {}
        self.right_record_list_map = {}

    def process_element(self, left_record: message.KeyRecord,
                        right_record: message.KeyRecord):
        if left_record is not None:
            key = left_record.key
            logger.debug(f"Receive a left stream, key is: {key},"
                         f" value is: {left_record.value}")
        else:
            key = right_record.key
            logger.debug(f"Receive a right stream, key is: {key},"
                         f" value is: {right_record.value}")

        if key not in self.left_record_list_map:
            self.left_record_list_map[key] = []
        if key not in self.right_record_list_map:
            self.right_record_list_map[key] = []

        left_list = self.left_record_list_map[key]
        right_list = self.right_record_list_map[key]

        # NOTE(loushang.ls): We only consider the inner join
        # by primary key currently.
        if left_record is not None:
            left_value = left_record.value
            left_list.append(left_value)
            for right_value in right_list:
                res = self.func.join(left_value, right_value)
                self.collect(message.KeyRecord(key, res, self.stream))
                del self.left_record_list_map[key]
                del self.right_record_list_map[key]
        else:
            right_value = right_record.value
            right_list.append(right_value)
            for left_value in left_list:
                res = self.func.join(left_value, right_value)
                self.collect(message.KeyRecord(key, res, self.stream))
                del self.left_record_list_map[key]
                del self.right_record_list_map[key]


class MergeOperator(StreamOperator, MultiInputOperator):
    """
    Operator to run a :class:`function.MergeFunction`
    NOTE(loushang.ls) We only consider the memory state currently.
    """

    def __init__(self, func: function.MergeFunction):
        super().__init__(func)
        self.right_input_vertex_ids = None
        self.stream_size = 0
        # A nested container that outer is a map whose key
        # is the merge key and value is another map,
        # and inner is a map whose key is the stream flag
        # and value is specific data coming from upstreams.
        self.container = {}

    # Note: The `right_input_vertex_ids` is ordered!
    def set_upstream_runtime_context(self, right_input_vertex_ids):
        logger.info(f"Construct `MergeOperator` with right"
                    f" input vertex ids: {right_input_vertex_ids}")
        self.right_input_vertex_ids = right_input_vertex_ids
        self.stream_size = len(right_input_vertex_ids) + 1
        assert self.right_input_vertex_ids is not\
            None and self.stream_size > 1, \
            "For merge operator, the right input vertexes can't be null."

    def process_element(self, record: message.KeyRecord):
        logger.debug(f"Merge operator received"
                     f" a message from: {record.stream}")
        assert record.stream is not None, \
            "Stream flag of the data can't be null " \
            "when processing data in `Merge Operator`."
        current_stream = record.stream
        merge_key = record.key
        merge_value = record.value

        if merge_key not in self.container.keys():
            # Received the first data.
            self.container[merge_key] = {}

        self.container[merge_key][current_stream] = merge_value

        if len(self.container[merge_key].keys()) == self.stream_size:
            logger.debug(f"Merge Operator has received all"
                         f" message from upstream for key: {merge_key}")
            right_ordered_inputs = []
            for index in range(self.stream_size - 1):
                right_ordered_inputs.append(self.container[merge_key][
                    self.right_input_vertex_ids[index]])
            left_inputs = [
                self.container[merge_key][stream]
                for stream in self.container[merge_key].keys()
                if stream not in self.right_input_vertex_ids
            ]
            assert len(
                left_inputs
            ) == 1, "The number of the left streams" \
                    " must be 1 when using `Merge Operator`."
            res = self.func.merge(left_inputs + right_ordered_inputs)
            del self.container[merge_key]
            self.collect(message.Record(res, self.stream))


class UnionOperator(StreamOperator, OneInputOperator):
    """Operator for union operation"""

    def __init__(self):
        super().__init__(function.EmptyFunction())

    def process_element(self, record):
        self.collect(record)


class ArrowOperator(StreamOperator, OneInputOperator):
    """Operator for arrow record batch"""

    def __init__(self):
        super().__init__(function.EmptyFunction())
        self.batch_size = None
        self.timeout_milliseconds = None
        self.thread = None
        self.lock = threading.Lock()
        self.encoder = None
        self.arrow_writer = None
        self.last_write_time_second = 0
        self.row_count = 0
        self.row_buffer = []

    def open(self, collectors, runtime_context):
        assert hasattr(self, "input_schema"), \
            "Arrow operator should have input schema, if the schema can't " \
            "be inferred, please specify it using " \
            "`with_schema`/`withSchema` api."
        super().open(collectors, runtime_context)
        import pyfury as fury
        input_schema = self.input_schema
        self.encoder = fury.create_row_encoder(input_schema)
        self.arrow_writer = fury.ArrowWriter(input_schema)
        prefix = f"{ArrowOperator.__module__}.{ArrowOperator.__name__}"
        self.batch_size = int(
            runtime_context.get_config().get(f"{prefix}.batchSize"))
        self.timeout_milliseconds = int(
            runtime_context.get_config().get(f"{prefix}.timeoutMilliseconds"))
        logger.info(f"Open ArrowOperator with batchSize {self.batch_size} and "
                    f"timeout_milliseconds {self.timeout_milliseconds}")
        # Loaded rows from checkpoint
        if self.row_buffer:
            self.row_count = len(self.row_buffer)
            for row_bytes in self.row_buffer:
                buf = fury.Buffer(row_bytes, 0, len(row_bytes))
                row = fury.RowData(self.input_schema, buf)
                self.arrow_writer.write(row)
        self.last_write_time_second = time.time()
        self.thread = threading.Thread(target=self.flush, daemon=True)
        self.thread.start()

    def process_element(self, record):
        self.lock.acquire()
        try:
            self.row_count += 1
            row = self.encoder.to_row(record.value)
            self.row_buffer.append(row.to_bytes())
            self.arrow_writer.write(row)
            self.last_write_time_second = time.time()
            if self.row_count == self.batch_size:
                self.collect(message.Record(self.arrow_writer.finish()))
                self.arrow_writer.reset()
                self.row_count = 0
                self.row_buffer = []
        finally:
            self.lock.release()

    def flush(self):
        while True:
            self.lock.acquire()
            try:
                duration_milliseconds =\
                    (time.time() - self.last_write_time_second) * 1000
                if self.row_count > 0 and duration_milliseconds >= \
                        self.timeout_milliseconds:
                    logger.info(f"Write {self.row_count} pieces of data as "
                                f"arrow record batch after "
                                f"{duration_milliseconds} "
                                f"milliseconds timeouts.")
                    self.collect(message.Record(self.arrow_writer.finish()))
                    self.arrow_writer.reset()
                    self.row_count = 0
                    self.row_buffer = []
                    self.last_write_time_second = time.time()
            finally:
                self.lock.release()
            need_to_sleep = (
                self.timeout_milliseconds - duration_milliseconds) / 1000
            if need_to_sleep <= 0:
                need_to_sleep = self.timeout_milliseconds / 10
            time.sleep(need_to_sleep)

    def save_checkpoint(self, checkpoint_id):
        self.lock.acquire()
        try:
            return self.row_buffer
        finally:
            self.lock.release()

    def load_checkpoint(self, checkpoint_id, row_buffer):
        self.lock.acquire()
        try:
            self.row_buffer = row_buffer
        finally:
            self.lock.release()


class ChainedOperator(StreamOperator, ABC):
    class ForwardCollector(Collector):
        def __init__(self, succeeding_operator):
            self.succeeding_operator = succeeding_operator

        def collect(self, record):
            self.succeeding_operator.process_element(record)

    def __init__(self, operators, op_configs):
        super().__init__(operators[0].func)
        self.operators = operators
        self.op_configs = op_configs

    def open(self, collectors, runtime_context):
        # Dont' call super.open() as we `open` every operator separately.
        num_operators = len(self.operators)
        operators_map = {
            operator.get_id(): operator
            for operator in self.operators
        }
        logger.info("ChainedOperator open.")

        # Note the order here!
        succeeding_collectors = []
        for operator in self.operators:
            op_succeeding_collectors = []
            next_operators_id_list = operator.get_next_operators_id_list()
            for next_operator_id in next_operators_id_list:
                if next_operator_id in operators_map.keys():
                    op_succeeding_collectors.append(
                        ChainedOperator.ForwardCollector(
                            operators_map[next_operator_id]))
                else:
                    # Traverse the outer collectors and find the
                    # specific collector corresponding to this collector.
                    for collector in collectors:
                        if collector.get_id() == operator.get_id() \
                            and collector.get_downstream_operator_id() \
                                == next_operator_id:
                            op_succeeding_collectors.append(collector)
                            break

            assert len(op_succeeding_collectors) == len(
                next_operators_id_list
            ), f"Generate collector for operator: {operator.get_id()} failed!"
            succeeding_collectors.append(op_succeeding_collectors)

        for i in range(0, num_operators):
            forward_collectors = succeeding_collectors[i]
            self.operators[i].open(
                forward_collectors,
                self.__create_runtime_context(runtime_context, i))

    def operator_type(self) -> OperatorType:
        return self.operators[0].operator_type()

    def __create_runtime_context(self, runtime_context, index):
        def get_config():
            return self.op_configs[index]

        runtime_context.get_config = get_config
        return runtime_context

    @staticmethod
    def new_chained_operator(operators, op_configs):
        operator_type = operators[0].operator_type()
        logger.info(
            "Building ChainedOperator from operators {} and op_configs {}.".
            format(operators, op_configs))
        if operator_type == OperatorType.SOURCE:
            return ChainedSourceOperator(operators, op_configs)
        elif operator_type == OperatorType.ONE_INPUT:
            return ChainedOneInputOperator(operators, op_configs)
        elif operator_type == OperatorType.TWO_INPUT:
            return ChainedTwoInputOperator(operators, op_configs)
        else:
            raise Exception("Current operator type is not supported")

    def on_finish(self):
        for operator in self.operators:
            operator.on_finish()

    def set_stream(self, stream):
        super().set_stream(stream)
        # Each stream should has the `stream` label,
        # not only the `KeyBy Operator`.
        for operator in self.operators:
            operator.set_stream(stream)


class ChainedSourceOperator(SourceOperator, ChainedOperator):
    def __init__(self, operators, op_configs):
        super().__init__(operators, op_configs)

    def fetch(self, checkpoint_id):
        self.operators[0].fetch(checkpoint_id)


class ChainedOneInputOperator(ChainedOperator):
    def __init__(self, operators, op_configs):
        super().__init__(operators, op_configs)

    def process_element(self, record):
        self.operators[0].process_element(record)


class ChainedTwoInputOperator(ChainedOperator):
    def __init__(self, operators, op_configs):
        super().__init__(operators, op_configs)

    def process_element(self, record1, record2):
        self.operators[0].process_element(record1, record2)


class IndependentOperator:
    def __init__(self, config={}):
        self._config = config

    @property
    def config(self):
        return self._config


def load_chained_operator(chained_operator_bytes: bytes):
    """Load chained operator from serialized operators and op_configs"""
    serialized_operators, op_configs = gateway_client.deserialize(
        chained_operator_bytes)
    operators = [
        load_operator(desc_bytes) for desc_bytes in serialized_operators
    ]
    return ChainedOperator.new_chained_operator(operators, op_configs)


def load_operator(descriptor_operator_bytes: bytes):
    """
    Deserialize `descriptor_operator_bytes` to get operator info, then
    create streaming operator.
    Note that this function must be kept in sync with
     `io.ray.streaming.runtime.python.GraphPbBuilder.serializeOperator`

    Args:
        descriptor_operator_bytes: serialized operator info

    Returns:
        a streaming operator
    """
    assert len(descriptor_operator_bytes) > 0
    function_desc_bytes, module_name, class_name, input_schema_bytes, \
        output_schema_bytes, op_config, op_id, next_operators_id_list\
        = gateway_client.deserialize(descriptor_operator_bytes)
    if function_desc_bytes:
        op = create_operator_with_func(
            function.load_function(function_desc_bytes))
    else:
        assert module_name
        assert class_name
        mod = importlib.import_module(module_name)
        cls = getattr(mod, class_name)
        from raystreaming.operator import Operator
        assert issubclass(cls, Operator)
        op = cls()
    import pyarrow as pa
    if input_schema_bytes:
        op.set_input_schema(
            pa.ipc.read_schema(pa.py_buffer(input_schema_bytes)))
    if output_schema_bytes:
        op.set_output_schema(
            pa.ipc.read_schema(pa.py_buffer(output_schema_bytes)))
    if op_config:
        op.set_op_config(op_config)
    if op_id:
        op.set_id(op_id)
    if next_operators_id_list:
        op.set_next_operators_id_list(next_operators_id_list)
    return op


_function_to_operator = {
    function.SourceFunction: SourceOperatorImpl,
    function.MapFunction: MapOperator,
    function.ArrowToPandasFunction: MapOperator,
    function.FlatMapFunction: FlatMapOperator,
    function.FilterFunction: FilterOperator,
    function.KeyFunction: KeyByOperator,
    function.ReduceFunction: ReduceOperator,
    function.SinkFunction: SinkOperator,
    function.JoinFunction: JoinOperator,
    function.MergeFunction: MergeOperator,
    function.ProcessFunction: ProcessOperator,
}


def create_operator_with_func(func: function.Function):
    """Create an operator according to a :class:`function.Function`

    Args:
        func: a subclass of function.Function

    Returns:
        an operator
    """
    operator_class = None
    super_classes = func.__class__.mro()
    for super_class in super_classes:
        operator_class = _function_to_operator.get(super_class, None)
        if operator_class is not None:
            break
    assert operator_class is not None
    return operator_class(func)
