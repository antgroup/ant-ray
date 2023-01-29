from ray.streaming.constants import StreamingConstants
from ray.streaming.runtime.state_backend import StateBackendFactory
import random


def test_dfs_backend_put_get():
    dfs = StateBackendFactory.get_state_backend(
        StreamingConstants.CP_STATE_BACKEND_DFS)
    dfs.init({"cp_state_backend_type": "cp_state_backend_dfs"})
    data = b"xyz"
    file_name = str(random.random())
    dfs.put(file_name, data)
    assert dfs.get(file_name) == data
    dfs.remove(file_name)
    assert dfs.get(file_name) is None
