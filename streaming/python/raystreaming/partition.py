import importlib
import inspect
from abc import ABC, abstractmethod

from ray import cloudpickle
from raystreaming.runtime import gateway_client
from raystreaming.runtime.transfer import channel_id_bytes_to_str


class DynamicRebalanceParam:
    def set_data_writer(self, data_writer):
        self.data_writer = data_writer

    def set_output_queue_names(self, output_queue_names):
        self.output_queue_names = output_queue_names

    def set_buffer_capacity(self, buffer_capacity):
        self.buffer_capacity = buffer_capacity

    def set_batch_size(self, batch_size):
        self.batch_size = batch_size

    def get_data_writer(self):
        return self.data_writer

    def get_output_queue_names(self):
        return self.output_queue_names

    def get_buffer_capacity(self):
        return self.buffer_capacity

    def get_batch_size(self):
        return self.batch_size


class Partition(ABC):
    """Interface of the partitioning strategy."""

    @abstractmethod
    def partition(self, record, num_partition: int):
        """Given a record and downstream partitions, determine which partition(s)
         should receive the record.

        Args:
            record: The record.
            num_partition: num of partitions
        Returns:
            IDs of the downstream partitions that should receive the record.
         """
        pass


class BroadcastPartition(Partition):
    """Broadcast the record to all downstream partitions."""

    def __init__(self):
        self.__partitions = []

    def partition(self, record, num_partition: int):
        if len(self.__partitions) != num_partition:
            self.__partitions = list(range(num_partition))
        return self.__partitions


class KeyPartition(Partition):
    """Partition the record by the key."""

    def __init__(self):
        self.__partitions = [-1]

    def partition(self, key_record, num_partition: int):
        # TODO support key group
        self.__partitions[0] = abs(hash(key_record.key)) % num_partition
        return self.__partitions


class RoundRobinPartition(Partition):
    """Partition record to downstream tasks in a round-robin matter."""

    def __init__(self):
        self.__partitions = [-1]
        self.seq = 0

    def partition(self, key_record, num_partition: int):
        self.seq = (self.seq + 1) % num_partition
        self.__partitions[0] = self.seq
        return self.__partitions


class DynamicRebalancePartition(Partition):
    def __init__(self, dynamic_rebalance_param: DynamicRebalanceParam) -> None:
        self.last_channel_index = -1
        self.last_channel_capacity = 0
        self.MINI_NUMBER = 1e-4

        self.writer = dynamic_rebalance_param.get_data_writer()
        self.output_queue_names =\
            dynamic_rebalance_param.get_output_queue_names()
        self.buffer_capacity = dynamic_rebalance_param.get_buffer_capacity()
        self.batch_size = dynamic_rebalance_param.get_batch_size()

    def get_buffer_capacity(self):
        return self.buffer_capacity

    def check_last_channel(self) -> bool:
        return self.last_channel_index != -1 and \
            self.last_channel_capacity >= 1

    def update_last_channel_index(self, new_channel_id, ratio):
        self.last_channel_index = new_channel_id
        self.last_channel_capacity = min(
            int(self.get_buffer_capacity() * (1.0 - ratio)) - 1,
            self.batch_size - 1)

    def get_output_backpressure_ratio(self):
        return self.writer.get_backpressure_ratio_by_list(
            self.output_queue_names)

    def partition(self, record, num_partition: int):
        if self.check_last_channel():
            self.last_channel_capacity -= 1
            return [self.last_channel_index]
        output_queue_ratio = {}
        weight_ratio = {}
        sum_ratio = 0
        sum_weight = 0
        max_norm = 0
        res = 0
        binary_output_queue_ratio = self.get_output_backpressure_ratio()
        for key, value in binary_output_queue_ratio.items():
            output_queue_ratio[channel_id_bytes_to_str(key)] = value
            sum_ratio += value

        for key, value in output_queue_ratio.items():
            current_weight = (sum_ratio + self.MINI_NUMBER) / (
                value + self.MINI_NUMBER)
            weight_ratio[key] = current_weight
            sum_weight += current_weight

        if sum_weight == 0:
            sum_weight += self.MINI_NUMBER
        inv_sum_weight = 1.0 / sum_weight

        index = self.last_channel_index
        output_queue_size = len(self.output_queue_names)
        step = output_queue_size

        while (step > 0):
            step -= 1
            index = (index + 1) % output_queue_size
            output_name = self.output_queue_names[index]
            current_Norm = weight_ratio[output_name] * inv_sum_weight
            if current_Norm > max_norm:
                max_norm = current_Norm
                res = index

        self.update_last_channel_index(
            res, output_queue_ratio[self.output_queue_names[res]])
        return [res]


class ForwardPartition(Partition):
    """Default partition for operator if the operator can be chained with
     succeeding operators."""

    def __init__(self):
        self.__partitions = [0]

    def partition(self, key_record, num_partition: int):
        return self.__partitions


class SimplePartition(Partition):
    """Wrap a python function as subclass of :class:`Partition`"""

    def __init__(self, func):
        self.func = func

    def partition(self, record, num_partition: int):
        return self.func(record, num_partition)


def serialize(partition_func):
    """
    Serialize the partition function so that it can be deserialized by
    :func:`deserialize`
    """
    return cloudpickle.dumps(partition_func)


def deserialize(partition_bytes):
    """Deserialize the binary partition function serialized by
    :func:`serialize`"""
    return cloudpickle.loads(partition_bytes)


def load_partition(descriptor_partition_bytes: bytes):
    """
    Deserialize `descriptor_partition_bytes` to get partition info, then
    get or load partition function.
    Note that this function must be kept in sync with
     `io.ray.streaming.runtime.python.GraphPbBuilder.serializePartition`

    Args:
        descriptor_partition_bytes: serialized partition info

    Returns:
        partition function
    """
    assert len(descriptor_partition_bytes) > 0
    partition_bytes, module_name, function_name =\
        gateway_client.deserialize(descriptor_partition_bytes)
    if partition_bytes:
        return deserialize(partition_bytes)
    else:
        assert module_name
        mod = importlib.import_module(module_name)
        assert function_name
        func = getattr(mod, function_name)
        # If func is a python function, user partition is a simple python
        # function, which will be wrapped as a SimplePartition.
        # If func is a python class, user partition is a sub class
        # of Partition.
        if inspect.isfunction(func):
            return SimplePartition(func)
        else:
            assert issubclass(func, Partition)
            return func()
