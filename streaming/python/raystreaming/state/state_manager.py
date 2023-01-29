from raystreaming.state.store.store_manager import StoreManager
from raystreaming.state.key_value_state import KeyValueState


class StateManager:
    def __init__(self,
                 jobname,
                 key_group=0,
                 state_name="default",
                 state_config={},
                 metric_group=None):
        self.jobname = jobname
        self.state_name = state_name
        self.state_config = state_config
        # Current key group is 0 by default since it's unsupport to rescale.
        self.key_group = key_group
        self.metric_group = metric_group
        self.store_manager = StoreManager(
            self.jobname, self.state_name, self.state_config, self.key_group,
            self.metric_group).build_store_manager()

    def get_key_value_state(self, key_value_descriptor=None) -> KeyValueState:
        return self.store_manager.build_key_value_store()

    def snapshot(self, checkpoint_id: int):
        self.store_manager.snapshot(checkpoint_id)

    def rollback_snapshot(self, checkpoint_id: int):
        self.store_manager.rollback_snapshot(checkpoint_id)

    def delete_snapshot(self, checkpoint_id: int):
        self.store_manager.delete_snapshot(checkpoint_id)

    def close(self):
        self.store_manager.close()

    def __state_map(self):
        pass
