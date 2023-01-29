import copy
from raystreaming.state.key_value_state import KeyValueState
import logging
logger = logging.getLogger(__name__)


class MemoryKeyValueStore(KeyValueState):
    def __init__(self, state_manager, jobname, state_name, config,
                 metric_group):
        self.__data_map = {}
        self.__data_snapshot = {}

    def get(self, key):
        if key not in self.__data_map:
            return None
        return self.__data_map[key]

    def put(self, key, value):
        self.__data_map[key] = value

    def delete(self, key):
        del self.__data_map[key]

    def snapshot(self, checkpoint_id: int):
        logger.info("Snapshot in memory store with checkpoint id {}.".format(
            checkpoint_id))
        self.__data_snapshot[checkpoint_id] = copy.deepcopy(self.__data_map)

    def rollback_snapshot(self, checkpoint_id: int):
        if checkpoint_id > 0:
            logger.info(
                "Rollback snapshot in memory store with checkpoint id {}.".
                format(checkpoint_id))
            self.__data_map = copy.deepcopy(
                self.__data_snapshot[checkpoint_id])

    def delete_snapshot(self, checkpoint_id: int):
        logger.info(
            "Delete snapshot in memory store with checkpoint id {}.".format(
                checkpoint_id))
        if checkpoint_id > 0 and checkpoint_id in self.__data_snapshot:
            del self.__data_snapshot[checkpoint_id]
        else:
            logger.warning("No such checkpoint found, skip it.")
