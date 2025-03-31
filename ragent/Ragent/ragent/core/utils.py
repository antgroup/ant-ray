import os
import re
import json
import yaml
import logging
from pathlib import Path
from typing import Tuple, List

from ragent.core import constants
from ragent.core.schemas import AgentTask, ToolSpec
from ragent.core.config import ragent_config


def parse_thought_action(llm_output: str) -> Tuple[str, str]:
    """Parse the LLM output into the thought and action.

    Args:
        llm_output: The LLM output.

    Returns:
        The thought and action.
    """
    thoughts = re.findall(r"Thought (\d+):(.*)", llm_output)
    # re.DOTALL to ignore '\n' and continue matching
    actions = re.findall(r"Action (\d+):(.*)", llm_output, re.DOTALL)

    _, thought = max(thoughts, key=lambda x: int(x[0])) if thoughts else (None, None)
    _, action = max(actions, key=lambda x: int(x[0])) if actions else (None, None)

    return thought, action


def parse_function_call(llm_output: str) -> Tuple[str, str]:
    """Parse a function call from LLM output into the function name and arguments.

    Args:
        func_call: The function call.

    Returns:
        The function name and arguments.
    """
    llm_output = llm_output.strip()
    if llm_output.startswith("Using Tool: "):
        llm_output = llm_output.replace("Using Tool: ", "", 1)
    try:
        output = json.loads(llm_output)
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON output from LLM: {llm_output}")
    # NOTE: `ToolSpec` is used to semantically describe the function call, not
    # for direct function calling
    # tool_spec = ToolSpec(**output)
    func_name = output.get("name")
    input_variables = output.get("input")
    return func_name, input_variables


def load_yaml_config(config_file_path):
    try:
        with open(config_file_path, "r", encoding="utf-8") as f:
            return yaml.load(f, Loader=yaml.FullLoader)
    except Exception:
        raise Exception(f"Cannot load config file {config_file_path}")


def setup_logger(log_level: str = "INFO", log_file: str = None, agent_name: str = None):
    class AgentNameRecordFilter(logging.Filter):
        def __init__(self, agent_name=None) -> None:
            self._agent_name = agent_name
            super().__init__("AgentNameRecordFilter")

        def filter(self, record) -> bool:
            if not hasattr(record, "agentname"):
                record.agentname = self._agent_name
            return True

    logger = logging.getLogger()
    # Remove default handlers otherwise a msg will be printed twice.
    for hdlr in logger.handlers:
        logger.removeHandler(hdlr)

    if type(log_level) is str:
        log_level = logging.getLevelName(log_level.upper())

    logger.setLevel(log_level)
    _formatter = logging.Formatter(
        fmt=constants.LOGGING_FMT, datefmt=constants.LOGGING_DATE_FMT
    )
    name_filter = AgentNameRecordFilter(agent_name=agent_name)

    _customed_handler = logging.StreamHandler()
    _customed_handler.setFormatter(_formatter)
    _customed_handler.addFilter(name_filter)
    _customed_handler.setLevel(log_level)

    logger.addHandler(_customed_handler)

    # If a log file is specified, add a FileHandler to output to the log file
    if log_file:
        os.makedirs(Path(log_file).parent, exist_ok=True)
        _file_handler = logging.FileHandler(log_file)
        _file_handler.setFormatter(_formatter)
        _file_handler.addFilter(name_filter)
        _file_handler.setLevel(log_level)
        logger.addHandler(_file_handler)
