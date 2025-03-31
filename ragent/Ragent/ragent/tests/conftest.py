import re
import secrets
import pytest
from ragent.core.agent import BaseAgent
from ragent.core.environment import Environment
from ragent.core.schemas import AgentTask


DEFAULT_ENV_CONFIG = {
    "name": "MockEnv",
    "runtime": "LOCAL",
    "max_agent_amount": 5,
    "max_inflight_task": 100,
    "log_level": "INFO",
    "log_file": "",
}


class MockAgent(BaseAgent):
    def profile(self, profile):
        pass

    def reason(self, goal, strategy="Chain-of-Thoughts"):
        pass

    def action(self, task):
        pass

    def construct_memory(self, session_id: str):
        pass

    def update_memory(self, session_id, memory):
        pass

    def invoke(self, task):
        match = re.search(r"Execute (\d+) times", task.goal)
        times = 0 if match is None else int(match.group(1))
        times -= 1
        need_reply = times > 0
        ret = AgentTask(
            id=str(secrets.token_hex(4)),
            goal=f"Execute {times} times",
            sender=self.name,
            receiver=["<all>"],
            need_reply=need_reply,
            caused_by=task.id,
        )

        return ret


@pytest.fixture
def mock_agent(name="MockAgent"):
    return MockAgent(name=name)


@pytest.fixture
def env_config():
    return DEFAULT_ENV_CONFIG


@pytest.fixture
def env():
    e = Environment(DEFAULT_ENV_CONFIG)
    yield e
    e.stop()
