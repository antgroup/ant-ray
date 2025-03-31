import copy
import pytest
import sys
import time

from ragent.core.environment import Environment
from ragent.core.schemas import AgentTask
from ragent.tests.conftest import env_config, env, mock_agent, MockAgent


def test_env_basic_op(env_config):
    env = Environment(env_config)
    assert len(env._agents) == 0
    assert env._name == env_config["name"]
    assert env._is_local_mode
    assert env._env_config == env_config
    assert env._task_queue.qsize() == 0
    assert env._reply_queue.qsize() == 0
    assert not env._task_publisher.is_started()
    assert not env._task_reply_poller.is_started()

    env.run()
    assert env._task_publisher.is_started()
    assert env._task_reply_poller.is_started()

    env.stop()
    assert not env._task_publisher.is_started()
    assert not env._task_reply_poller.is_started()

    query = AgentTask(id="0", goal="Test", sender="User", receiver=["<all>"])
    ret = env.submit_query(query)
    assert ret == query.id
    assert env._task_queue.qsize() == 1
    assert env._reply_queue.qsize() == 0
    msg = "Test message"
    ret = env.query(query=msg, receiver=["<all>"], timeout_s=0)
    assert ret

    ret = env._select_task_receiver(query)
    # Should be None since no agent has joined, even if `receiver` is "<all>"
    assert not ret
    ret = env._select_task_receiver(None)
    assert len(ret) == 0

    ret = env.get_agent("FakeAgentName")
    assert not ret
    ret = env.get_agent(None)
    assert not ret

    trace, is_complete = env.get_query_result(query.id)
    # The one that user sent
    assert len(trace) == 1
    # Should be False since the task is not even executed
    assert not is_complete
    trace, is_complete = env.get_query_result(None)
    # Should be None since the task is not completed
    assert not len(trace)
    assert not is_complete
    env.stop()

def test_env_join_agent(env, mock_agent):
    # Test join & repeat join
    assert len(env._agents) == 0
    env.join(mock_agent)
    assert len(env._agents) == 1
    assert env.get_agent(mock_agent.name) == mock_agent
    env.join(mock_agent)
    assert len(env._agents) == 1
    assert env.get_agent(mock_agent.name) == mock_agent

    # Test join multiple agents
    agents = []
    for idx in range(1, 5):
        agents.append(MockAgent(mock_agent.name + "-" + str(idx)))
    for agent in agents:
        env.join(agent)
    assert len(env._agents) == 5

    # Test agents number exceed the limit
    out_number_agent = MockAgent("OutNumberAgent")
    with pytest.raises(ValueError):
        env.join(out_number_agent)
    assert len(env._agents) == 5

    # Test select agent based on task
    task = AgentTask(id="0", goal="Test", sender="User", receiver=["<all>"])
    receivers = env._select_task_receiver(task)
    assert len(receivers) == 5
    assert mock_agent.name in receivers
    task = AgentTask(id="0", goal="Test", sender="User", receiver=[mock_agent.name])
    receivers = env._select_task_receiver(task)
    assert len(receivers) == 1
    assert mock_agent.name in receivers
    for agent in agents:
        assert agent.name not in receivers

    env.stop()


def test_env_task_submit(env, mock_agent):
    env.join(mock_agent)

    task = AgentTask(id="0", goal="Test", sender="User", receiver=[mock_agent.name])
    env._receive_task(task)
    assert env._task_queue.qsize() == 1
    env.run()

    # wait for task polling
    trace, is_complete = env.get_query_result(task.id, timeout_s=5)
    # There should be no tasks pending to be published
    assert env._task_queue.qsize() == 0
    assert trace
    assert isinstance(trace, list)
    assert len(trace) == 2
    # The first element must be the originial task
    assert trace[0].id == task.id
    assert trace[0].goal == "Test"
    assert is_complete

    # Test submit job while env is running
    second_task = AgentTask(
        id="1", goal="Test", sender="User", receiver=[mock_agent.name]
    )
    env._receive_task(second_task)
    time.sleep(1)
    trace, is_complete = env.get_query_result(second_task.id)
    assert trace
    assert isinstance(trace, list)
    # The first element must be the originial task
    assert trace[0].id == second_task.id
    assert trace[0].goal == "Test"
    assert is_complete


@pytest.mark.parametrize("times", [1, 2])
def test_env_series_task_execute(env, mock_agent, times):
    env.join(mock_agent)
    task = AgentTask(
        id="0", goal=f"Execute {times} times", sender="User", receiver=[mock_agent.name]
    )
    env._receive_task(task)
    env.run()

    time.sleep(2)
    trace, is_complete = env.get_query_result(task.id)
    assert trace
    assert isinstance(trace, list)
    # The user initial task, and tasks executed by the agent
    assert len(trace) == times + 1
    assert trace[0].id == task.id
    for idx in range(1, times):
        assert f"Execute {times - idx} times" in trace[idx].goal
    assert is_complete


@pytest.mark.parametrize("times", [1, 2])
def test_env_multiple_series_task_execute(env, mock_agent, times):
    env.join(mock_agent)
    task = AgentTask(
        id="0", goal=f"Execute {times} times", sender="User", receiver=[mock_agent.name]
    )
    env._receive_task(task)
    env.run()

    second_task = AgentTask(
        id="1",
        goal=f"Execute {times + 1} times",
        sender="User",
        receiver=[mock_agent.name],
    )
    env._receive_task(second_task)
    time.sleep(5)

    trace, is_complete = env.get_query_result(task.id)
    assert trace
    assert isinstance(trace, list)
    assert len(trace) == times + 1
    assert trace[0].id == task.id
    for idx in range(1, times):
        assert f"Execute {times - idx} times" in trace[idx].goal
    assert is_complete

    trace, is_complete = env.get_query_result(second_task.id)
    assert trace
    assert isinstance(trace, list)
    assert len(trace) == (times + 1) + 1
    assert trace[0].id == second_task.id
    for idx in range(1, (times + 1)):
        assert f"Execute {(times + 1) - idx} times" in trace[idx].goal
    assert is_complete


def test_env_user_ask(env, mock_agent):
    env.join(mock_agent)

    msg = "Execute 1 times"
    trace = env.query(msg, timeout_s=-1)

    assert trace
    assert isinstance(trace, list)
    assert len(trace) == 2

    # Specify receiver by its name
    trace = env.query(msg, receiver=mock_agent.name, timeout_s=-1)
    assert trace
    assert isinstance(trace, list)
    assert len(trace) == 2

    # Assign to an non-exist agent, timeout must be specified
    # since the task will never complete.
    with pytest.raises(TimeoutError):
        trace = env.query(msg, receiver="NonExist", timeout_s=1, err_when_timeout=True)

    # Same result but instant return
    trace = env.query(msg, receiver="NonExist", timeout_s=0, err_when_timeout=True)
    assert trace
    assert isinstance(trace, list)
    # No agent will execute the task, so should
    # only have the user initial task
    assert len(trace) == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
