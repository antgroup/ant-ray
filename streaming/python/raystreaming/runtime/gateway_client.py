# -*- coding: UTF-8 -*-
"""Module to interact between java and python
"""

import msgpack
import ray
import typing


class GatewayClient:
    """GatewayClient is used to interact with `PythonGateway` java actor
    """

    def __init__(self, gateway_class_name):
        self._python_gateway_actor = ray.java_actor_class(
            gateway_class_name).remote()

    def call_function(self, java_class, java_function, *args):
        java_params = serialize([java_class, java_function] + list(args))
        call = self._python_gateway_actor.callFunction.remote(java_params)
        return deserialize(ray.get(call))

    def call_method(self, java_object, java_method, *args):
        java_params = serialize([java_object, java_method] + list(args))
        call = self._python_gateway_actor.callMethod.remote(java_params)
        return deserialize(ray.get(call))

    def new_instance(self, java_class_name):
        call = self._python_gateway_actor.newInstance.remote(
            serialize(java_class_name))
        return deserialize(ray.get(call))

    def create_py_partition(self, serialized_partition):
        assert isinstance(serialized_partition, bytes)
        call = self._python_gateway_actor.createPyPartition \
            .remote(serialized_partition)
        return deserialize(ray.get(call))

    def is_job_finished(self):
        call = self._python_gateway_actor.isJobFinished.remote()
        return ray.get(call)

    def shutdown_all_workers(self):
        call = self._python_gateway_actor.shutdownAllWorkers.remote()
        return ray.get(call)

    def get_execution_job_vertex_size(self):
        call = self._python_gateway_actor.getExecutionJobVertexSize.remote()
        return deserialize(ray.get(call))

    def get_job_status(self) -> str:
        return ray.get(self._python_gateway_actor.getJobStatus.remote())

    def get_failover_info(self, item_cnt=1) -> \
            typing.List[typing.Tuple[int, str]]:
        """get failover info from job master."""
        call = self._python_gateway_actor.getFailoverInfo.remote(item_cnt)
        return [tuple(item) for item in deserialize(ray.get(call))]

    def get_last_valid_checkpoint_id(self):
        call = self._python_gateway_actor.getLastValidCheckpointId.remote()
        return ray.get(call)

    def do_rescale(self, rescale_param):
        call = self._python_gateway_actor.doRescale.remote(rescale_param)
        return ray.get(call)

    def get_event_info(self, item_cnt=1) -> \
            typing.List[str]:
        """get event info from job master."""
        call = self._python_gateway_actor.getEventInfo.remote(item_cnt)
        return list(deserialize(ray.get(call)))


class StreamingGatewayClient(GatewayClient):
    _PYTHON_GATEWAY_CLASSNAME = \
        "com.alipay.streaming.runtime.python.PyStreamingGateway"

    def __init__(self):
        super().__init__(StreamingGatewayClient._PYTHON_GATEWAY_CLASSNAME)

    def create_streaming_context(self):
        call = self._python_gateway_actor.createStreamingContext.remote()
        return deserialize(ray.get(call))

    def with_config(self, conf):
        call = self._python_gateway_actor.withConfig.remote(serialize(conf))
        ray.get(call)

    def with_independent_operator(self, independent_operator_descriptor):
        call = self._python_gateway_actor.withIndependentOperator\
            .remote(serialize(independent_operator_descriptor.class_name),
                    serialize(independent_operator_descriptor.module_name),
                    serialize(independent_operator_descriptor.language.name),
                    independent_operator_descriptor.parallelism,
                    serialize(independent_operator_descriptor.resource),
                    serialize(independent_operator_descriptor.config),
                    serialize(independent_operator_descriptor
                              .is_lazy_scheduling))
        ray.get(call)

    def get_independent_operators(self):
        call = self._python_gateway_actor.getIndependentOperators.remote()
        return deserialize(ray.get(call))

    def execute(self, job_name):
        call = self._python_gateway_actor.execute.remote(serialize(job_name))
        ray.get(call)

    def create_py_stream_source(self, serialized_func):
        assert isinstance(serialized_func, bytes)
        call = self._python_gateway_actor.createPythonStreamSource \
            .remote(serialized_func)
        return deserialize(ray.get(call))

    def create_py_func(self, serialized_func):
        assert isinstance(serialized_func, bytes)
        call = self._python_gateway_actor.createPyFunc.remote(serialized_func)
        return deserialize(ray.get(call))

    def union(self, *streams):
        serialized_streams = serialize(streams)
        call = self._python_gateway_actor.union \
            .remote(serialized_streams)
        return deserialize(ray.get(call))

    def merge(self, *params):
        serialized_params = serialize(params)
        call = self._python_gateway_actor.merge \
            .remote(serialized_params)
        return deserialize(ray.get(call))

    def with_schema(self, j_stream, schema):
        schema_bytes = schema.serialize().to_pybytes()
        java_params = serialize([j_stream, schema_bytes])
        call = self._python_gateway_actor.withSchema.remote(java_params)
        return deserialize(ray.get(call))


def serialize(obj) -> bytes:
    """Serialize a python object which can be deserialized by `PythonGateway`
    """
    return msgpack.packb(obj, use_bin_type=True)


def deserialize(data: bytes):
    """Deserialize the binary data serialized by `PythonGateway`"""
    return msgpack.unpackb(data, raw=False)


def serialize_object_ref(ref):
    worker = ray.worker.global_worker
    worker.check_connected()
    ref, owner_address = (
        worker.core_worker.serialize_and_promote_object_ref(ref))
    return ref.binary(), owner_address
