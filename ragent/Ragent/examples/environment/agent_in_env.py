import argparse
import secrets
import time
import ray
import os
from metagpt.context import Context
from metagpt.roles import ProductManager, Architect, ProjectManager
from metagpt.schema import Message

from ragent.agents.metagpt_agent import MetaGptAgent
from ragent.core.constants import ENV_CONFIG_KEY
from ragent.core.environment import Environment
from ragent.core.graph.graph import RagentGraph
from ragent.core.schemas import AgentTask


def create_pm(context: Context):
    # `Context` must be given, otherwise, states produced
    # have nowhere to be stored in, and the procedure
    # that dependes on the states will get stuck.
    return ProductManager(context=context)


def create_architect(context: Context):
    return Architect(context=context)


def create_pj(context: Context):
    return ProjectManager(context=context)


def run_env():
    env = Environment()

    ragent_pm = MetaGptAgent(create_pm, name="PM")
    ragent_architect = MetaGptAgent(
        create_architect, name="Architect", log_level="DEBUG"
    )
    ragent_pj = MetaGptAgent(create_pj, name="PJ")

    env.join(ragent_pm)
    env.join(ragent_architect)
    env.join(ragent_pj)

    env.run()
    pm = env.get_agent(name="PM")
    assert pm

    # while True:
    msg = input("Enter a task that a ProductManager should do: ")
    # The receiver should be the agent name, not the role name
    # FIXME: Can't detect when the MetaGPT task is completed, so the
    # `timeout_s` must be provided to avoid inifinite waiting.
    resp = env.query(msg, receiver="PM", timeout_s=60, poll_interval_s=10)
    print(f"Response: {resp[-1]}")
    env.stop()


def run_architect():
    env = Environment()

    ragent_pm = MetaGptAgent(create_pm, name="PM")
    ragent_architect = MetaGptAgent(
        create_architect, name="Architect", log_level="DEBUG"
    )
    ragent_pj = MetaGptAgent(create_pj, name="PJ")

    env.join(ragent_pm)
    env.join(ragent_architect)
    env.join(ragent_pj)

    env.run()

    task = AgentTask(
        id=secrets.token_hex(4),
        goal='{"docs":{"20240408130433.json":{"root_path":"docs/prd","filename":"20240408130433.json","content":"{\\"Language\\":\\"en_us\\",\\"Programming Language\\":\\"Python\\",\\"Original Requirements\\":\\"Write PRD for snake game\\",\\"Project Name\\":\\"snake_game\\",\\"Product Goals\\":[\\"Create an addictive and intuitive gameplay experience\\",\\"Ensure cross-platform compatibility for various devices\\",\\"Incorporate social features for sharing high scores and challenges\\"],\\"User Stories\\":[\\"As a casual gamer, I want to easily understand and play the game so that I can enjoy it during my free time\\",\\"As a competitive player, I want to be able to challenge my friends and see where I rank on the leaderboard\\",\\"As a nostalgic player, I want a classic snake game with modern enhancements to keep me engaged\\"],\\"Competitive Analysis\\":[\\"Snake Game Classic: Simple and traditional gameplay, lacks modern features\\",\\"Slither.io: Modern take on the snake game with online multiplayer, but can be overwhelming\\",\\"Nibbler: Unique art style and gameplay, but limited in terms of social interaction\\"],\\"Competitive Quadrant Chart\\":\\"quadrantChart\\\\n    title \\\\\\"Engagement and innovation of snake games\\\\\\"\\\\n    x-axis \\\\\\"Low Engagement\\\\\\" --> \\\\\\"High Engagement\\\\\\"\\\\n    y-axis \\\\\\"Low Innovation\\\\\\" --> \\\\\\"High Innovation\\\\\\"\\\\n    quadrant-1 \\\\\\"Innovate more\\\\\\"\\\\n    quadrant-2 \\\\\\"Focus on user experience\\\\\\"\\\\n    quadrant-3 \\\\\\"Stabilize\\\\\\"\\\\n    quadrant-4 \\\\\\"Innovate and grow\\\\\\"\\\\n    \\\\\\"Snake Game Classic\\\\\\": [0.4, 0.2]\\\\n    \\\\\\"Slither.io\\\\\\": [0.8, 0.6]\\\\n    \\\\\\"Nibbler\\\\\\": [0.3, 0.7]\\\\n    \\\\\\"Our Target Product\\\\\\": [0.6, 0.5]\\",\\"Requirement Analysis\\":\\"The snake game should be designed with a focus on intuitive gameplay, cross-platform compatibility, and social features. The game must be easy to understand for casual gamers while offering a challenge and social interaction for competitive players and modern enhancements for nostalgic players.\\",\\"Requirement Pool\\":[[\\"P0\\",\\"Develop a game engine with intuitive controls and responsive gameplay\\"],[\\"P1\\",\\"Design a user-friendly interface with accessible options and clear instructions\\"],[\\"P1\\",\\"Implement a leaderboard system for players to compete and share their scores\\"],[\\"P2\\",\\"Create a variety of levels and challenges to keep players engaged\\"]],\\"UI Design draft\\":\\"The UI should be clean and simple, with a focus on readability and ease of use. Key elements include a clear game screen, control buttons, level selection, and leaderboard integration.\\",\\"Anything UNCLEAR\\":\\"The specific features for cross-platform compatibility need to be defined and clarified.\\"}"}}}',
        sender="metagpt.roles.product_manager.ProductManager",
        receiver={"<all>"},
        need_reply=True,
        metadata={"cause_by": "metagpt.actions.write_prd.WritePRD"},
    )
    task_id = env._receive_task(task)
    print(f"Task id: {task_id}")
    time.sleep(10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env-config-path", help="File path to the configuration YAML file."
    )
    mode_group = parser.add_mutually_exclusive_group(required=False)
    mode_group.add_argument(
        "-local", action="store_true", default=True, help="Run in local mode"
    )
    mode_group.add_argument(
        "-remote", action="store_true", default=False, help="Run in ray cluster"
    )
    args = parser.parse_args()

    if args.env_config_path:
        os.environ["RAGENT_ENV_CONFIG_FILE"] = args.env_config_path
    os.environ["RAGENT_RUNTIME"] = "LOCAL" if args.local else "REMOTE"
    os.environ[
        ENV_CONFIG_KEY
    ] = "/Users/paer/Documents/work/codes/X/ray_common_libs/Ragent/examples/environment/default_env_config.yaml"

    run_env()
