from raystreaming.state.state_config import AntKVStateConfig
from raystreaming.state.state_manager import StateManager
from raystreaming.state.store.store_manager import StoreManager
from raystreaming.state.utils import remove_files
import logging
import random
import sys

Log_Format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(stream=sys.stdout, format=Log_Format, level=logging.INFO)

logger = logging.getLogger()


def test_store_manager():
    test_antkv_path_1 = "/tmp/test_antkv_1"
    state_config = {
        "state.backend.type": "ANTKV",
        AntKVStateConfig.STATE_BACKEND_ANTKV_STORE_DIR: test_antkv_path_1
    }
    remove_files(test_antkv_path_1)

    jobname = "test"
    state_name = "op1" + str(random.random())
    metric_group = None
    key_group = 1
    store_manager = StoreManager(jobname, state_name, state_config, key_group,
                                 metric_group).build_store_manager()
    kv_store = store_manager.build_key_value_store()
    kv_store.put("aaa", "bbb")
    assert kv_store.get("aaa") == "bbb"
    store_manager.snapshot(1)


def test_state_manager():
    test_antkv_path_2 = "/tmp/test_antkv_2"
    state_config = {
        "state.backend.type": "ANTKV",
        AntKVStateConfig.STATE_BACKEND_ANTKV_STORE_DIR: test_antkv_path_2
    }
    remove_files(test_antkv_path_2)

    jobname = "test"
    state_name = "op1" + str(random.random())
    state_manager = StateManager(
        jobname=jobname, state_name=state_name, state_config=state_config)
    kv_store = state_manager.get_key_value_state()
    kv_store.put("aaa", "bbb")
    assert kv_store.get("aaa") == "bbb"
    kv_store.put("aaa", "replaced")
    state_manager.snapshot(1)
    assert kv_store.get("aaa") == "replaced"
    kv_store.put("aaa", "replaced_2")
    state_manager.snapshot(2)
    state_manager.rollback_snapshot(1)
    kv_state_rollbacked = state_manager.get_key_value_state()
    logger.info(f"Value {kv_state_rollbacked.get('aaa')}")
    assert kv_state_rollbacked.get("aaa") == "replaced"
    state_manager.delete_snapshot(1)

    state_manager.rollback_snapshot(2)
    kv_state_rollbacked_2 = state_manager.get_key_value_state()
    assert kv_state_rollbacked_2.get("aaa") == "replaced_2"

    state_manager.delete_snapshot(2)


if __name__ == "__main__":
    test_store_manager()
    test_state_manager()
