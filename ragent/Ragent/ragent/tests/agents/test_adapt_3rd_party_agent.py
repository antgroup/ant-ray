import os
import sys
import pytest

from ragent.core.schemas import AgentTask
from ragent.core.graph.graph import RagentGraph


def test_adapt_metagpt_agent():
    from ragent.agents.metagpt_agent import MetaGptAgent
    from metagpt.context import Context
    from metagpt.roles.product_manager import ProductManager

    msg = "Write a PRD for a snake game"

    context = Context()  # For the assertion
    assert context.git_repo is None
    assert context.repo is None
    role = ProductManager(context=context)
    agent = MetaGptAgent(role)
    result = agent.ask(msg)

    # Assert on MetaGPT's result
    assert context.git_repo
    assert context.repo
    assert "PrepareDocuments" in result.cause_by

    msg = "Write a PRD for a super-mario game"
    os.environ["RAGENT_RUNTIME"] = "RAY"

    remote_agent = RagentGraph(agent)
    result = remote_agent.ask(msg)
    assert context.git_repo
    assert context.repo
    assert "PrepareDocuments" in result.cause_by


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
