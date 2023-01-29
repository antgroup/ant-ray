import logging
import pickle
import threading
import time

import ray
from ray import streaming
from ray.streaming.constants import StreamingConstants
from ray.streaming.runtime.metrics.worker_metrics import WorkerMetrics
from ray.streaming.runtime.transfer import ChannelID, DataReader, DataWriter
from ray.streaming.runtime.transfer import DataMessage, CheckpointBarrier
from ray.streaming.runtime.transfer import NativeConfigKeys
from ray.streaming.tests.base_test_actor import (
    BaseReaderActor,
    BaseWriterActor,
)

logger = logging.getLogger(__name__)

LOG_LEVEL = 0
max_message_seq_id = 1000


@ray.remote
class Writer(BaseWriterActor):
    def __init__(self):
        print("Writer init called")
        BaseWriterActor.__init__(self, __name__ + ".Writer")

    def remote_writer(self, peer_actor, q_id_list, id_list, queue_size):
        writer_thread = threading.Thread(
            target=self._remote_writer,
            args=(
                peer_actor,
                q_id_list,
                id_list,
                queue_size,
            ))
        writer_thread.start()

    def _remote_writer(self, peer_actor, q_id_list, outpoint_points,
                       checkpoint_id):
        peer_actor_handle = pickle.loads(peer_actor)
        output_queue_ids_dict = {q_id: peer_actor_handle for q_id in q_id_list}
        output_cyclic_dict = {q_id: False for q_id in q_id_list}
        configuration = {
            NativeConfigKeys.QUEUE_SIZE: 10**7,
            StreamingConstants.BUFFER_POOL_MIN_BUFFER_SIZE: 10**6,
            NativeConfigKeys.PLASMA_STORE_PATH: ray.worker.global_worker.node.
            plasma_store_socket_name
        }
        streaming.set_log_config(LOG_LEVEL)
        writer = DataWriter()
        worker_metric = WorkerMetrics("Test", "WriterTransfer", "Producer",
                                      "Id", {})
        worker_metric.open()
        writer.init(configuration, output_queue_ids_dict, output_cyclic_dict,
                    outpoint_points, checkpoint_id, worker_metric, 2000)
        self._writer = writer
        for i in range(0, max_message_seq_id):
            for q_id in q_id_list:
                self._writer.write(ChannelID(q_id), pickle.dumps(i))
            if i % 100 == 0 and i > 0:
                self._writer.broadcast_barrier(i, b"data")
                self._writer.clear_checkpoint(0, i)
        self._writer.broadcast_barrier(max_message_seq_id - 1,
                                       b"reader_finished")
        bp_ratio = self._writer.get_backpressure_ratio()
        logger.info("send message finished, {}".format(bp_ratio))
        time.sleep(5)  # for last message to be sent
        self._writer = None


@ray.remote
class Reader(BaseReaderActor):
    def __init__(self):
        print("Reader init called")
        BaseReaderActor.__init__(self, __name__ + ".Reader")
        self._done = False
        pass

    def remote_reader(self, peer_actor, q_id_list, points, checkpoint_id):
        reader_thread = threading.Thread(
            target=self._remote_reader,
            args=(
                peer_actor,
                q_id_list,
                points,
                checkpoint_id,
            ))
        reader_thread.start()

    def _remote_reader(self, peer_actor, q_id_list, points, checkpoint_id):
        peer_actor_handle = pickle.loads(peer_actor)
        input_queue_ids_dict = {q_id: peer_actor_handle for q_id in q_id_list}
        input_cyclic_dict = {q_id: False for q_id in q_id_list}
        configuration = {
            NativeConfigKeys.QUEUE_SIZE: 10**7,
            StreamingConstants.BUFFER_POOL_MIN_BUFFER_SIZE: 10**6,
            NativeConfigKeys.PLASMA_STORE_PATH: ray.worker.global_worker.node.
            plasma_store_socket_name
        }
        reader = DataReader()
        reader.init(configuration, input_queue_ids_dict, input_cyclic_dict,
                    points, checkpoint_id)
        self._reader = reader
        streaming.set_log_config(LOG_LEVEL)
        logger.info("reader init done.")

        data_map = {}
        import time
        slept_time = 0
        while True:
            message = self._reader.read(1000)
            if message:
                if isinstance(message, CheckpointBarrier):
                    read_checkpoint_id = message.checkpoint_id
                    logger.info("read_checkpoint_id %s", read_checkpoint_id)
                    if read_checkpoint_id == max_message_seq_id - 1:
                        break
                if isinstance(message, DataMessage):
                    last_data = data_map.get(message.channel_id, -1)
                    new_data = pickle.loads(message.body)
                    data_map[message.channel_id] = new_data
                    assert last_data + 1 == new_data, (last_data, new_data)
            else:
                time.sleep(1)
                slept_time += 1
            if slept_time > 60:
                raise Exception("Execution not finished")
        assert read_checkpoint_id == max_message_seq_id - 1
        print("Execution succeed")
        self._reader = None
        self._done = True

    def test_done(self):
        return self._done


def test_streaming_actor_local_cluster():
    print(f"=======dir(streaming) {dir(streaming)}")
    streaming.set_log_config(LOG_LEVEL)
    if not ray.is_initialized():
        ray.init(num_cpus=2)

    streaming.set_log_config(LOG_LEVEL)
    q_id_list = list(
        map(lambda x: ChannelID.gen_random_bytes_id(), range(0, 2)))

    points = {}
    checkpoint_id = 0
    writer = Writer._remote()
    reader = Reader._remote()

    time.sleep(2)
    reader.remote_reader.remote(
        pickle.dumps(writer), q_id_list, points, checkpoint_id)
    time.sleep(1)
    writer.remote_writer.remote(
        pickle.dumps(reader), q_id_list, points, checkpoint_id)
    slept_time = 0
    while not ray.get(reader.test_done.remote()):
        print("Wait finished...")
        time.sleep(1)
        slept_time += 1
        if slept_time > 60:
            raise Exception("Execution not finished")
    assert ray.get(reader.test_done.remote())
    print("Execution succeed")
    ray.shutdown()


if __name__ == "__main__":
    test_streaming_actor_local_cluster()
