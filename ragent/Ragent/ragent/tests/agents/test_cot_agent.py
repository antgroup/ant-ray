import logging
import sys

import pytest

from ragent.agents.cot_agent import CoTAgent
from ragent.core.schemas import AgentTask
from ragent.core.graph.graph import RagentGraph
from ragent.llms.mock_llm import MockLLM
from ragent.tools.ant_sort import ant_sort
from ragent.tests.conftest import mock_agent


logger = logging.getLogger(__name__)

EXAMPLES = [
    """
Task: Sort this integer list in ascending order: [1,3,7,9,5]
Answer: {
    "name": "ant_sort",
    "input": {
        "input": [1,3,7,9,5]
    }
}
"""
]

MOCK_REPLY_1 = """
Thought 1: I need to use the ant_sort tool to sort the list in 'Ant' order.
Action 1: Using Tool: {
    "name": "ant_sort",
    "input": {
        "input": [1, 100, -1, 5, 9, 7, 4]
    }
}
"""

MOCK_REPLY_2 = """
Action 1 result: ['ant_-1', 'ant_1', 'ant_4', 'ant_5', 'ant_7', 'ant_9', 'ant_100']
Thought 2: Final Answer: ['ant_-1', 'ant_1', 'ant_4', 'ant_5', 'ant_7', 'ant_9', 'ant_100']
"""

# NOTE(paer): The order of the keys in the dictionary is important since the LLM will
# use the key to match the prompt, and the "'Ant' order" is always in the prompt, so
# it should be the last key in the dictionary.
MOCK_LLM_PROMPT = {
    "Action result: ": MOCK_REPLY_2,
    "'Ant' order": MOCK_REPLY_1,
}

# FIXME: This UT is currently broken because a single agent without
# an environment is not able to get the task result. Fix this by
# handling the task reply procedure.
def test_user_ask(mock_agent):
    question = "Mock question"
    expectation = "Mock task result Mock question"
    res = mock_agent.query(question, config=None)
    assert res == expectation

    remote_agent = RagentGraph(mock_agent)
    res = remote_agent.query(question)
    assert res == expectation


def test_receive_task(mock_agent):
    task: AgentTask = AgentTask(
        id=1,
        goal="Mock question",
        sender="User",
        receiver=mock_agent.name,
        need_reply=True,
    )
    expectation = "Mock task result Mock question"
    res = mock_agent._receive_task(task, sender="User")
    assert res == expectation

    remote_agent = RagentGraph(mock_agent)
    res = remote_agent._receive_task(task)
    assert res == expectation


def test_cot_agent():
    agent = CoTAgent().profile(
        {
            "llm": MockLLM(MOCK_LLM_PROMPT),
            "examples": EXAMPLES,
        }
    )

    agent.register_tools(ant_sort)

    data = [1, 100, -1, 5, 9, 7, 4]
    result = agent.query(f"Sort this integer list in 'Ant' order: {data}")
    logger.info(f"Agent result: {type(result)} {result}")
    expectation = ant_sort.invoke(data)
    assert (
        str(expectation) in result
    ), f"Expectation: {expectation}, LLM result: {result}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
