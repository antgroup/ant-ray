import ray
import time
import logging.config
from ray.streaming.runtime.worker import (
    JobWorker,
    JobWorkerInstance,
    MockJobMaster,
)
from ray.streaming.tests import test_utils
from ray.streaming.handler import generate_template
from ray.streaming.generated.remote_call_pb2 import (
    CallResult,
    WorkerRuntimeInfo,
    PythonJobWorkerContext,
    ExecutionVertexContext,
    CheckpointId,
    Language,
)
from ray.actor import ActorHandle
from ray.streaming.runtime import gateway_client
from ray.streaming.tests import test_operator
from ray.streaming.runtime.graph import NodeType

logger = logging.getLogger(__name__)


def test_worker_health_check():
    test_utils.start_ray_python_only()
    worker = JobWorker.remote()

    def get_worker_check_state():
        check = getattr(worker, generate_template.format("health_check"))\
            .remote()
        check_pb = WorkerRuntimeInfo()
        check_pb.ParseFromString(ray.get(check))
        return check_pb.healthy

    assert not get_worker_check_state()
    # Invoke worker init in empty config json.
    getattr(worker, generate_template.format("init")).remote("{}")
    assert get_worker_check_state()

    # Worker process exit for mock failover.
    getattr(worker, generate_template.format("shutdown")).remote()

    # Sleep 1 second for waiting actor reconstructing.
    time.sleep(1.0)
    for i in range(0, 3):
        try:
            get_worker_check_state()
            break
        except ray.exceptions.RayActorError:
            time.sleep(1.0)
    assert not get_worker_check_state()

    ray.shutdown()


def test_worker_was_reconstructed():
    @ray.remote(max_restarts=-1)
    class MockJobWorker(JobWorkerInstance):
        def get_was_reconstructed(self):
            return ray.get_runtime_context().was_current_actor_reconstructed

    test_utils.start_ray_python_only(False)
    worker = MockJobWorker.remote()

    worker.init.remote("{}")

    assert not ray.get(worker.get_was_reconstructed.remote())

    # Worker process exit for mock failover.
    worker.shutdown.remote()

    # Sleep 1 second for waiting actor reconstructing.
    time.sleep(1.0)
    final_state = False
    for i in range(0, 3):
        try:
            final_state = ray.get(worker.get_was_reconstructed.remote())
            break
        except ray.exceptions.RayActorError:
            time.sleep(1.0)
    assert final_state

    ray.shutdown()


def get_actor_handle_bytes(actor_handle):
    return ActorHandle._serialization_helper(actor_handle)[0]


def build_python_worker_context(worker, master):
    worker_actor_id = worker._actor_id
    master_actor_bytes = get_actor_handle_bytes(master)
    context = PythonJobWorkerContext()
    context.master_actor = master_actor_bytes
    context.worker_id = worker_actor_id.hex()
    context.is_changed = False
    worker_actor_bytes = get_actor_handle_bytes(worker)
    e_vertex = context.execution_vertex_context.current_execution_vertex
    e_vertex.worker_actor = worker_actor_bytes
    e_vertex.language = Language.PYTHON
    e_vertex.role_in_changed_sub_dag = \
        ray.streaming.generated.remote_call_pb2.OperatorType.TRANSFORM
    e_vertex.chained = False

    descriptor_op_bytes = gateway_client.serialize([
        None, test_operator.__name__, test_operator.EmptyOperator.__name__,
        None, None, {}, None, []
    ])
    e_vertex.operator = descriptor_op_bytes
    return context


def test_sink_vertex():
    test_utils.start_ray_python_only()
    worker = JobWorker.remote()
    master = MockJobMaster.remote()
    context = build_python_worker_context(worker, master)
    input_execution_edges = context.execution_vertex_context \
        .input_execution_edges
    in_edge_1 = ExecutionVertexContext.ExecutionEdge()
    in_edge_1.source_execution_vertex_id = 0
    in_edge_1.target_execution_vertex_id = 1
    input_execution_edges.append(in_edge_1)
    logger.info("adebug")

    # No output edges, so this vertex is sink
    assert ray.streaming.runtime.graph.ExecutionVertexContext(
        context.execution_vertex_context).vertex_type == NodeType.SINK

    out_edge_1 = ExecutionVertexContext.ExecutionEdge()
    out_edge_1.cyclic = False
    out_edge_2 = ExecutionVertexContext.ExecutionEdge()
    out_edge_2.cyclic = False
    context.execution_vertex_context.output_execution_edges.append(out_edge_1)
    context.execution_vertex_context.output_execution_edges.append(out_edge_2)
    # 2 non-cyclic output edges, so this vertex is not sink
    assert ray.streaming.runtime.graph.ExecutionVertexContext(
        context.execution_vertex_context).vertex_type == NodeType.TRANSFORM

    context.execution_vertex_context.output_execution_edges[0].cyclic = False
    context.execution_vertex_context.output_execution_edges[1].cyclic = True
    # 1 non-cyclic output edge and 1 cyclic output edge, so this vertex is not
    # sink
    assert ray.streaming.runtime.graph.ExecutionVertexContext(
        context.execution_vertex_context).vertex_type == NodeType.TRANSFORM

    context.execution_vertex_context.output_execution_edges[0].cyclic = True
    context.execution_vertex_context.output_execution_edges[1].cyclic = True
    # 2 cyclic output edges, so this vertex is sink
    assert ray.streaming.runtime.graph.ExecutionVertexContext(
        context.execution_vertex_context).vertex_type == NodeType.SINK


def test_worker_register_context():
    test_utils.start_ray_python_only()
    worker = JobWorker.remote()
    master = MockJobMaster.remote()
    worker_context = build_python_worker_context(worker,
                                                 master).SerializeToString()
    getattr(worker, generate_template.format("init")).remote("{}")
    ray.get(
        getattr(worker, generate_template.format("register_context")).remote(
            worker_context))

    checkpoint_id_pb = CheckpointId()
    checkpoint_id_pb.checkpoint_id = 0
    result = ray.get(
        getattr(worker, generate_template.format("rollback")).remote(
            checkpoint_id_pb.SerializeToString(),
            checkpoint_id_pb.SerializeToString()))

    call_result_pb = CallResult()
    call_result_pb.ParseFromString(result)
    assert not call_result_pb.success
    print("test worker finished.")
    ray.shutdown()


if __name__ == "__main__":
    test_worker_health_check()
    test_worker_register_context()
