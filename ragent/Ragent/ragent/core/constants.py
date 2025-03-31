from enum import Enum


class RuntimeEngineType(Enum):
    LOCAL = "LOCAL"
    RAY = "RAY"


LLM_FINAL_ANSWER = "Final Answer: "
LLM_GIVE_UP = "I give up."
ENV_CONFIG_KEY = "RAGENT_ENV_CONFIG_FILE"

LOGGING_DATE_FMT = "%Y-%m-%d %H:%M:%S"
LOGGING_FMT = "%(asctime)s %(levelname)s %(filename)s:%(lineno)s [%(agentname)s]: %(message)s"  # noqa

RAGENT_RUNTIME_ENV_VAR = "RAGENT_RUNTIME"
