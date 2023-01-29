from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from google.protobuf import any_pb2

from ray.streaming.generated import remote_call_pb2


def test_protobuf_WorkerRollbackRequest():
    cmdWkRollbackReq = remote_call_pb2.BaseWorkerCmd()
    cmdWkRollbackReq.actor_id = b"1234"
    cmdWkRollbackReq.detail.Pack(remote_call_pb2.WorkerRollbackRequest())
    encoded = cmdWkRollbackReq.SerializeToString()
    decoded = remote_call_pb2.BaseWorkerCmd()
    decoded.ParseFromString(encoded)
    print("decoded: {}".format(decoded))
    assert decoded.actor_id == b"1234"


def test_protobuf_any():
    msg = any_pb2.Any()
    wkRollbackReq = remote_call_pb2.WorkerRollbackRequest()
    msg.Pack(wkRollbackReq)
    assert msg.Is(remote_call_pb2.WorkerRollbackRequest().DESCRIPTOR)


def test_protobuf_WorkerCommitReport():
    report_pb = remote_call_pb2.BaseWorkerCmd()
    report_pb.actor_id = b"1"
    worker_commit_report = remote_call_pb2.WorkerCommitReport()
    worker_commit_report.commit_checkpoint_id = 1
    report_pb.detail.Pack(worker_commit_report)
    print(report_pb)
    assert report_pb is not None
    encoded = report_pb.SerializeToString()
    decoded = remote_call_pb2.BaseWorkerCmd()
    decoded.ParseFromString(encoded)
    assert decoded.actor_id == b"1"


def test_protobuf_MetricDump():
    dump_pb = remote_call_pb2.MetricDump()
    dump_pb.category = remote_call_pb2.MetricDump.MetricCategory.Gauge
    dump_pb.name = "test_metric"
    dump_pb.value = "3.5"

    dump_pb.query_scope.host = "localhost"
    dump_pb.query_scope.job_name = "test_job"
    dump_pb.query_scope.op_name = "1-SourceOperator"
    dump_pb.query_scope.pid = "10341"
    dump_pb.query_scope.worker_id.id = b"ffff001"
    dump_pb.query_scope.worker_name = "0"

    print("MetricDump: ")
    print(dump_pb)

    encoded = dump_pb.SerializeToString()
    decoded = remote_call_pb2.MetricDump()
    decoded.ParseFromString(encoded)
    print("Decoded MetricDump: ")
    print(decoded)

    assert dump_pb.name == "test_metric"


def test_protobuf_MetricResult():
    result_pb = remote_call_pb2.MetricResult()
    dump1 = result_pb.dumps.add()
    dump1.category = remote_call_pb2.MetricDump.MetricCategory.Counter
    dump1.name = "test_metric1"
    dump1.value = str(3)
    dump1.query_scope.host = "localhost"
    dump1.query_scope.job_name = "test_job"
    dump1.query_scope.op_name = "1-SourceOperator"
    dump1.query_scope.pid = "10341"
    dump1.query_scope.worker_id.id = b"ffff001"
    dump1.query_scope.worker_name = "0"

    dump2 = result_pb.dumps.add()
    dump2.category = remote_call_pb2.MetricDump.MetricCategory.Gauge
    dump2.name = "test_metric2"
    dump2.value = "1.60"
    dump2.query_scope.host = "localhost"
    dump2.query_scope.job_name = "test_job"
    dump2.query_scope.op_name = "4-ReduceOperator"
    dump2.query_scope.pid = "20341"
    dump2.query_scope.worker_id.id = b"ffff002"
    dump2.query_scope.worker_name = "1"

    print("MetricResult: ")
    print(result_pb.dumps)

    assert len(result_pb.dumps) == 2

    encoded = result_pb.SerializeToString()
    print("type: ", type(encoded))
    print("len: ", len(encoded))
