import os
import logging
import shutil
logger = logging.getLogger(__name__)


def make_directory_created(path):
    if not os.path.exists(path):
        logger.info(f"No such path : {path}, now creating.")
        os.makedirs(path)


def remove_files(path):
    """Remove specific file by full name.
    """
    if os.path.isfile(path):
        logger.info(f"Remove files in {path}.")
        os.remove(path)
    elif os.path.isdir(path):
        logger.info(f"Remove directory in {path}.")
        shutil.rmtree(path)


OPERATR_STATE_PREFIX = "streaming.operator."


def convert_operator_state_config(config):
    state_config = {}
    for item in config.items():
        if item[0].startswith(OPERATR_STATE_PREFIX):
            state_config[item[0].replace(OPERATR_STATE_PREFIX, "")] = item[1]
    return state_config
