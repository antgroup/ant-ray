import datetime
import logging
import pickle
import threading
import time

import pytest
import ray
from ray import streaming
from ray.streaming.runtime.transfer import ChannelID, DataMessage
from ray.streaming.runtime.transfer import DataReader, DataWriter
from ray.streaming.runtime.transfer import NativeConfigKeys
from ray.streaming.tests.base_test_actor import BaseReaderActor
from ray.streaming.tests.base_test_actor import BaseWriterActor

logger = logging.getLogger(__name__)

LOG_LEVEL = -1
MSG_MAX_ID = 10
TIME_OUT = 60


@pytest.fixture(scope="session")
def streaming_setup():
    streaming.set_log_config(LOG_LEVEL)
    if not ray.is_initialized():
        ray.init()

    queue_size = 10**8
    q_id_list = [ChannelID.gen_random_bytes_id() for _ in range(0, 1)]
    msg_id_list = [0]
    yield q_id_list, msg_id_list, queue_size

    if ray.is_initialized():
        ray.shutdown()


@ray.remote
class Writer(BaseWriterActor):
    def __init__(self):
        print("Writer init called")
        BaseWriterActor.__init__(self, __name__ + ".Writer")

    def write_data(self,
                   peer_actor,
                   q_id_list,
                   msg_id_list,
                   queue_size,
                   data,
                   msg_size=MSG_MAX_ID):
        print("write_data called")
        writer_thread = threading.Thread(
            target=self._write_data,
            args=(
                peer_actor,
                q_id_list,
                msg_id_list,
                queue_size,
                data,
                msg_size,
            ))
        writer_thread.start()

    def _write_data(self, peer_actor, q_id_list, msg_id_list, queue_size, data,
                    msg_size):
        print("_write_data q_id_list size: {}, msg_size {}".format(
            len(q_id_list), msg_size))
        peer_actor_handle = pickle.loads(peer_actor)
        output_queue_ids_dict = {q_id: peer_actor_handle for q_id in q_id_list}
        points = dict(zip(q_id_list, msg_id_list))
        configuration = {
            NativeConfigKeys.QUEUE_SIZE: queue_size,
            NativeConfigKeys.PLASMA_STORE_PATH: ray.worker.global_worker.node.
            plasma_store_socket_name
        }
        writer = DataWriter()
        print("init start")
        writer.init(configuration, output_queue_ids_dict, points, 0)
        print("init succeed")
        self._writer = writer

        print("write message loop, data: {}".format(data))
        for i in range(0, msg_size):
            print("write message loop, data: {}".format(data))
            for q_id in q_id_list:
                self._writer.write(q_id, data)
            time.sleep(0.1)
        time.sleep(5.0)  # for last message to be sent
        self._writer = None


@ray.remote
class Reader(BaseReaderActor):
    def __init__(self):
        print("Reader init called")
        BaseReaderActor.__init__(self, __name__ + ".Reader")
        self._done = False
        self.msg_size = 0
        self._ret = []

    def read_data(self,
                  peer_actor,
                  q_id_list,
                  msg_id_list,
                  queue_size,
                  msg_size=MSG_MAX_ID):
        self.msg_size = msg_size
        reader_thread = threading.Thread(
            target=self._read_data,
            args=(peer_actor, q_id_list, msg_id_list, queue_size, msg_size))
        reader_thread.start()

    def _read_data(self, peer_actor, q_id_list, msg_id_list, queue_size,
                   msg_size):
        peer_actor_handle = pickle.loads(peer_actor)
        input_queue_ids_dict = {q_id: peer_actor_handle for q_id in q_id_list}
        configuration = {
            NativeConfigKeys.QUEUE_SIZE: queue_size,
            NativeConfigKeys.PLASMA_STORE_PATH: ray.worker.global_worker.node.
            plasma_store_socket_name
        }
        points = dict(zip(q_id_list, msg_id_list))
        reader = DataReader()
        reader.init(configuration, input_queue_ids_dict, points, 0)
        self._reader = reader

        self._ret = []
        start_ts = datetime.datetime.now().timestamp()
        while datetime.datetime.now().timestamp() - start_ts <= TIME_OUT:
            data = self._reader.read(5000)
            if data is None:
                continue
            assert isinstance(data, DataMessage)
            self._ret.append(pickle.loads(data.body))
            if len(self._ret) >= msg_size:
                break
        self._reader = None
        print("read finished")

    def get_result(self):
        while len(self._ret) != self.msg_size:
            print("Reading data...", self._ret)
            time.sleep(1)
        return self._ret


@pytest.mark.skip(reason="skip temporarily")
def test_streaming_create_reader_first(streaming_setup):
    q_id_list, msg_id_list, queue_size = streaming_setup
    writer = Writer._remote()
    reader = Reader._remote()
    msg = 2
    writer.write_data.remote(
        pickle.dumps(reader), q_id_list, msg_id_list, queue_size, msg)
    reader.read_data.remote(
        pickle.dumps(writer), q_id_list, msg_id_list, queue_size)

    msgs = ray.get(reader.get_result.remote())
    assert sum(msgs) == MSG_MAX_ID * msg


@pytest.mark.skip(reason="skip temporarily")
def test_streaming_recreate(streaming_setup):
    q_id_list, msg_id_list, queue_size = streaming_setup
    logger.info("queue id list => {}".format(q_id_list))

    writer = Writer._remote()
    reader = Reader._remote()
    msg = 2
    writer.write_data.remote(
        pickle.dumps(reader), q_id_list, msg_id_list, queue_size, msg)
    reader.read_data.remote(
        pickle.dumps(writer), q_id_list, msg_id_list, queue_size)

    msgs = ray.get(reader.get_result.remote())
    assert sum(msgs) == MSG_MAX_ID * msg

    time.sleep(5)
    # append 10 items in queue
    writer.write_data.remote(
        pickle.dumps(reader), q_id_list, msg_id_list, queue_size, msg)
    reader.read_data.remote(
        pickle.dumps(writer), q_id_list, msg_id_list, queue_size,
        MSG_MAX_ID * 2)

    msgs2 = ray.get(reader.get_result.remote())
    assert sum(msgs2) == MSG_MAX_ID * msg * 2
