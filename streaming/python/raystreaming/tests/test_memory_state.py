from raystreaming.state.state_config import StateBackendType
from raystreaming.state.state_manager import StateManager
import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()


def test_state_manager():
    state_config = {
        "state.backend.type": StateBackendType.MEMORY.name,
    }
    jobname = "test"
    state_name = "op1"
    metric_group = None
    state_manager = StateManager(
        jobname=jobname,
        state_name=state_name,
        state_config=state_config,
        metric_group=metric_group)
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
    logger.info(kv_state_rollbacked.get("aaa"))
    assert kv_state_rollbacked.get("aaa") == "replaced"

    state_manager.rollback_snapshot(2)
    kv_state_rollbacked_2 = state_manager.get_key_value_state()
    assert kv_state_rollbacked_2.get("aaa") == "replaced_2"


if __name__ == "__main__":
    test_state_manager()
