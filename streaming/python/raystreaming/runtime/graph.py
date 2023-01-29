import enum
import logging

import ray
import raystreaming.generated.remote_call_pb2 as remote_call_pb
import raystreaming.operator as operator
import raystreaming.partition as partition
from ray._raylet import ActorID
from ray.actor import ActorHandle
from raystreaming.config import Config
from raystreaming.generated.remote_call_pb2 import Language

logger = logging.getLogger(__name__)


class NodeType(enum.Enum):
    """
    SOURCE: Sources are where your program reads its input from

    TRANSFORM: Operators transform one or more DataStreams into a new
     DataStream. Programs can combine multiple transformations into
     sophisticated dataflow topologies.

    SINK: Sinks consume DataStreams and forward them to files, sockets,
     external systems, or print them.
    """
    SOURCE = 0
    TRANSFORM = 1
    SINK = 2
    SOURCE_AND_SINK = 3


class ExecutionEdge:
    def __init__(self, execution_edge_pb, language):
        self.source_execution_vertex_id = execution_edge_pb \
            .source_execution_vertex_id
        self.target_execution_vertex_id = execution_edge_pb \
            .target_execution_vertex_id
        self.cyclic = execution_edge_pb.cyclic
        self.original_src_operator_id = execution_edge_pb \
            .original_src_operator_id
        partition_bytes = execution_edge_pb.partition
        # Sink node doesn't have partition function,
        # so we only deserialize partition_bytes when it's not None or empty
        if language == Language.PYTHON and partition_bytes:
            self.partition = partition.load_partition(partition_bytes)


class ExecutionVertex:
    worker_actor: ActorHandle

    def __init__(self, execution_vertex_pb):
        # Exceution vertex id is global index of whole job taskes.
        self.execution_vertex_id = execution_vertex_pb.execution_vertex_id
        self.execution_job_vertex_id = execution_vertex_pb \
            .execution_job_vertex_id
        self.execution_job_vertex_name = execution_vertex_pb \
            .execution_job_vertex_name
        # Execution vertex index is subtask index in this job vertex.
        self.execution_vertex_index = execution_vertex_pb \
            .execution_vertex_index
        self.parallelism = execution_vertex_pb.parallelism

        if execution_vertex_pb.language == Language.PYTHON:
            # python operator descriptor
            operator_bytes = execution_vertex_pb.operator
            if operator_bytes == b"":
                logger.debug("Empty bytes process")
                self.stream_operator = None
            elif execution_vertex_pb.chained:
                logger.debug("Load chained operator")
                self.stream_operator = operator.load_chained_operator(
                    operator_bytes)
                self.stream_operator.set_stream(self.execution_job_vertex_id)
            else:
                logger.debug("Load operator")
                self.stream_operator = operator.load_operator(operator_bytes)
                self.stream_operator.set_stream(self.execution_job_vertex_id)
        self.worker_actor = ray.actor.ActorHandle. \
            _deserialization_helper(execution_vertex_pb.worker_actor)
        self.container_id = execution_vertex_pb.container_id
        self.build_time = execution_vertex_pb.build_time
        self.language = execution_vertex_pb.language
        self.config = execution_vertex_pb.config
        self.resource = execution_vertex_pb.resource
        self.worker_state = execution_vertex_pb.worker_state
        self.role_in_changed_sub_dag = \
            execution_vertex_pb.role_in_changed_sub_dag

    @property
    def execution_vertex_name(self):
        return "{}_{}_{}".format(self.execution_job_vertex_id,
                                 self.execution_job_vertex_name,
                                 self.execution_vertex_id)


class ExecutionVertexContext:
    actor_id: ActorID
    execution_vertex: ExecutionVertex

    def __init__(
            self,
            execution_vertex_context_pb: remote_call_pb.ExecutionVertexContext
    ):
        self.execution_vertex = ExecutionVertex(
            execution_vertex_context_pb.current_execution_vertex)
        self.job_name = self.execution_vertex.config[Config.STREAMING_JOB_NAME]
        self.exe_vertex_name = self.execution_vertex.execution_vertex_name
        self.actor_id = self.execution_vertex.worker_actor._ray_actor_id
        self.upstream_execution_vertices = [
            ExecutionVertex(vertex) for vertex in
            execution_vertex_context_pb.upstream_execution_vertices
        ]
        self.downstream_execution_vertices = [
            ExecutionVertex(vertex) for vertex in
            execution_vertex_context_pb.downstream_execution_vertices
        ]
        self.input_execution_edges = [
            ExecutionEdge(edge, self.execution_vertex.language)
            for edge in execution_vertex_context_pb.input_execution_edges
        ]
        self.output_execution_edges = [
            ExecutionEdge(edge, self.execution_vertex.language)
            for edge in execution_vertex_context_pb.output_execution_edges
        ]
        self._left_input_vertex_id = execution_vertex_context_pb \
            .left_input_vertex_id
        self._right_input_vertex_ids = execution_vertex_context_pb \
            .right_input_vertex_ids

    def get_parallelism(self):
        return self.execution_vertex.parallelism

    def get_upstream_parallelism(self):
        return len(self.input_execution_edges)

    def get_non_cyclic_upstream_parallelism(self):
        return len(
            list(
                filter(lambda edge: not edge.cyclic,
                       self.input_execution_edges)))

    def get_downstream_parallelism(self):
        return len(self.output_execution_edges)

    def get_non_cyclic_downstream_parallelism(self):
        return len(
            list(
                filter(lambda edge: not edge.cyclic,
                       self.output_execution_edges)))

    @property
    def left_input_vertex_id(self):
        return self._left_input_vertex_id

    @property
    def right_input_vertex_ids(self):
        return self._right_input_vertex_ids

    @property
    def vertex_type(self):
        if self.get_non_cyclic_upstream_parallelism() == 0:
            return NodeType.SOURCE
        elif self.get_non_cyclic_downstream_parallelism() == 0:
            return NodeType.SINK
        elif self.get_non_cyclic_upstream_parallelism() == 0 \
                and self.get_non_cyclic_downstream_parallelism() == 0:
            return NodeType.SOURCE_AND_SINK
        else:
            return NodeType.TRANSFORM

    @property
    def build_time(self):
        return self.execution_vertex.build_time

    @property
    def stream_operator(self):
        return self.execution_vertex.stream_operator

    @property
    def config(self):
        return self.execution_vertex.config

    @property
    def get_id(self):
        return self.execution_vertex.execution_vertex_id

    @property
    def execution_job_vertex_id(self):
        return self.execution_vertex.execution_job_vertex_id

    @property
    def execution_job_vertex_name(self):
        return self.execution_vertex.execution_job_vertex_name

    def get_vertex_id(self):
        return self.execution_vertex.execution_vertex_id

    def get_source_actor_by_execution_vertex_id(self, execution_vertex_id):
        return self.get_vertex_by_execution_vertex_id(execution_vertex_id) \
            .worker_actor

    def get_target_actor_by_execution_vertex_id(self, execution_vertex_id):
        return self.get_vertex_by_execution_vertex_id(execution_vertex_id)\
            .worker_actor

    def get_vertex_by_execution_vertex_id(self, execution_vertex_id):
        for execution_vertex in self.upstream_execution_vertices:
            if execution_vertex.execution_vertex_id == execution_vertex_id:
                return execution_vertex
        for execution_vertex in self.downstream_execution_vertices:
            if execution_vertex.execution_vertex_id == execution_vertex_id:
                return execution_vertex
        raise Exception(
            "Vertex {} does not exist!".format(execution_vertex_id))

    @property
    def worker_state(self):
        return self.execution_vertex.worker_state

    @property
    def role_in_changed_sub_dag(self):
        return self.execution_vertex.role_in_changed_sub_dag
