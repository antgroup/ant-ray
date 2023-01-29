from raystreaming.state.backend.hdfs import HDFSBackend
from raystreaming.state.state_config import HDFSStateConfig
from raystreaming.state.state_manager import StateManager
from raystreaming.state.utils import remove_files
import random
import logging
import sys
import os


def test_pangu_state_backend():
    hdfs_config = HDFSStateConfig({"state.backend.hdfs.store.type": "PANGU"})
    hdfs_backend = HDFSBackend(hdfs_config)
    hdfs_db = hdfs_backend.get_db()
    test_file_name = "aaa" + str(random.random())
    hdfs_db.put(test_file_name, b"bbb")
    assert hdfs_db.get(test_file_name) == b"bbb"
    hdfs_db.delete(test_file_name)
    assert hdfs_db.get(test_file_name) is None


def test_pangu_state_backend_compatible_config():
    hdfs_config = HDFSStateConfig({
        "cp_state_backend_type": "cp_state_backend_pangu",
        "cp_pangu_cluster_name": "pangu://alipay-pangu-for-hdfs-test"
    })
    hdfs_backend = HDFSBackend(hdfs_config)
    hdfs_db = hdfs_backend.get_db()
    test_file_name = "aaa" + str(random.random())
    hdfs_db.put(test_file_name, b"bbb")
    assert hdfs_db.get(test_file_name) == b"bbb"
    hdfs_db.delete(test_file_name)
    assert hdfs_db.get(test_file_name) is None


def test_dfs_state_backend():
    hdfs_config = HDFSStateConfig({"state.backend.hdfs.store.type": "DFS"})
    hdfs_backend = HDFSBackend(hdfs_config)
    hdfs_db = hdfs_backend.get_db()
    test_file_name = "aaa" + str(random.random())
    hdfs_db.put(test_file_name, b"bbb")
    assert hdfs_db.get(test_file_name) == b"bbb"
    hdfs_db.delete(test_file_name)
    ret_data = hdfs_db.get(test_file_name)
    assert ret_data in [None, b""]


def test_dfs_state_backend_compatible_config():
    hdfs_config = HDFSStateConfig({
        "cp_state_backend_type": "cp_state_backend_dfs"
    })
    hdfs_backend = HDFSBackend(hdfs_config)
    hdfs_db = hdfs_backend.get_db()
    test_file_name = "aaa" + str(random.random())
    hdfs_db.put(test_file_name, b"bbb")
    assert hdfs_db.get(test_file_name) == b"bbb"
    hdfs_db.delete(test_file_name)
    ret_data = hdfs_db.get(test_file_name)
    assert ret_data in [None, b""]


def test_hdfs_state_store_manager():
    hdfs_config = {
        "state.backend.hdfs.store.type": "DFS",
        "state.backend.type": "HDFS"
    }
    state_manager = StateManager(
        jobname="default",
        state_name="default",
        state_config=hdfs_config,
        metric_group=None)
    test_file_name = "aaa" + str(random.random())
    kv_store = state_manager.get_key_value_state()
    kv_store.put(test_file_name, b"bbb")
    assert kv_store.get(test_file_name) == b"bbb"
    kv_store.put(test_file_name, b"replaced")
    assert kv_store.get(test_file_name) == b"replaced"
    kv_store.delete(test_file_name)
    assert kv_store.get(test_file_name) in [None, b""]


def randbytes(n):
    for _ in range(n):
        yield random.getrandbits(8)


def test_pangu_read_write_local_remote_test():
    hdfs_config = HDFSStateConfig({"state.backend.hdfs.store.type": "PANGU"})
    hdfs_backend = HDFSBackend(hdfs_config)
    hdfs_db = hdfs_backend.get_db()
    test_file_name = "/tmp/local" + str(random.random())
    hdfs_file_name = "remote" + str(random.random())
    N = 1000000
    random_bytes = bytes(bytearray(randbytes(N)))
    with open(test_file_name, "wb") as local_file:
        local_file.write(random_bytes)
    hdfs_db.local_write_to_remote(test_file_name, hdfs_file_name)

    test_file_name_copy = "/tmp/local" + str(random.random())
    hdfs_db.local_read_from_remote(test_file_name_copy, hdfs_file_name)
    read_bytes = b""
    with open(test_file_name_copy, "rb") as local_file:
        read_bytes = local_file.read()
    assert len(read_bytes) == N
    assert random_bytes == read_bytes

    hdfs_db.delete(hdfs_file_name)
    remove_files(test_file_name)
    remove_files(test_file_name_copy)


def test_dfs_read_write_local_remote_test():
    hdfs_config = HDFSStateConfig({"state.backend.hdfs.store.type": "DFS"})
    hdfs_backend = HDFSBackend(hdfs_config)
    hdfs_db = hdfs_backend.get_db()
    test_file_name = "/tmp/local" + str(random.random())
    hdfs_file_name = "remote" + str(random.random())
    N = 1000000
    random_bytes = bytes(bytearray(randbytes(N)))
    with open(test_file_name, "wb") as local_file:
        local_file.write(random_bytes)
    hdfs_db.local_write_to_remote(test_file_name, hdfs_file_name)

    test_file_name_copy = "/tmp/local" + str(random.random())
    hdfs_db.local_read_from_remote(test_file_name_copy, hdfs_file_name)
    read_bytes = b""
    with open(test_file_name_copy, "rb") as local_file:
        read_bytes = local_file.read()
    assert len(read_bytes) == N
    assert random_bytes == read_bytes

    hdfs_db.delete(hdfs_file_name)
    remove_files(test_file_name)
    remove_files(test_file_name_copy)


def test_oss_state_backend():
    hdfs_config = HDFSStateConfig({
        "state.backend.hdfs.store.type": "OSS",
        "state.backend.oss.endpoint": "oss-cn-hangzhou-zmf.aliyuncs.com",
        "state.backend.oss.id": "LTAI4GKK6Chbk8gNULPpYV4j",
        "state.backend.oss.key": os.environ["ACI_VAR_RAY_PACK_OSS_KEY"],
        "state.backend.oss.bucket": "rayoltest",
        "state.backend.oss.store.dir": "ci/raystreaming/tmp"
    })
    hdfs_backend = HDFSBackend(hdfs_config)
    hdfs_db = hdfs_backend.get_db()
    test_file_name = "aaa" + str(random.random())
    hdfs_db.put(test_file_name, b"bbb")
    assert hdfs_db.get(test_file_name) == b"bbb"
    hdfs_db.delete(test_file_name)
    assert hdfs_db.get(test_file_name) is None


def test_oss_read_write_local_remote_test():
    hdfs_config = HDFSStateConfig({
        "state.backend.hdfs.store.type": "OSS",
        "state.backend.oss.endpoint": "oss-cn-hangzhou-zmf.aliyuncs.com",
        "state.backend.oss.id": "LTAI4GKK6Chbk8gNULPpYV4j",
        "state.backend.oss.key": os.environ["ACI_VAR_RAY_PACK_OSS_KEY"],
        "state.backend.oss.bucket": "rayoltest",
        "state.backend.oss.store.dir": "ci/raystreaming/tmp"
    })
    hdfs_backend = HDFSBackend(hdfs_config)
    hdfs_db = hdfs_backend.get_db()
    test_file_name = "/tmp/local" + str(random.random())
    hdfs_file_name = "remote" + str(random.random())
    N = 1000000
    random_bytes = bytes(bytearray(randbytes(N)))
    with open(test_file_name, "wb") as local_file:
        local_file.write(random_bytes)
    hdfs_db.local_write_to_remote(test_file_name, hdfs_file_name)

    test_file_name_copy = "/tmp/local" + str(random.random())
    hdfs_db.local_read_from_remote(test_file_name_copy, hdfs_file_name)
    read_bytes = b""
    with open(test_file_name_copy, "rb") as local_file:
        read_bytes = local_file.read()
    assert len(read_bytes) == N
    assert random_bytes == read_bytes

    assert os.path.join(hdfs_config.state_backend_oss_store_dir,
                        hdfs_file_name) in hdfs_db.list_files("")
    hdfs_db.delete(hdfs_file_name)
    remove_files(test_file_name)
    remove_files(test_file_name_copy)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger = logging.getLogger()
    test_pangu_state_backend()
    test_pangu_state_backend_compatible_config()
    test_dfs_state_backend()
    test_dfs_state_backend_compatible_config()
    test_hdfs_state_store_manager()
    test_pangu_read_write_local_remote_test()
    test_dfs_read_write_local_remote_test()
    test_oss_state_backend()
    test_oss_read_write_local_remote_test()
