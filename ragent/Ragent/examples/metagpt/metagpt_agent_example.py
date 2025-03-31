import time
import os
from metagpt.context import Context
from metagpt.roles.product_manager import ProductManager

from ragent.agents.metagpt_agent import MetaGptAgent


os.environ["RAGENT_RUNTIME"] = "RAY"
context = Context()


def create_role():
    return ProductManager(context=context)


msg = "Write a PRD for a snake game"

context = Context()  # For the assertion
assert context.git_repo is None
assert context.repo is None
role = ProductManager(context=context)
agent = MetaGptAgent(role)
result = agent.ask(msg)
print(f"Result: {result}")
agent.run()
time.sleep(1)
agent.stop()
