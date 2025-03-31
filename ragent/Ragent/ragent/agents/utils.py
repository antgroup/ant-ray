from ragent.agents.cot_agent import CoTAgent


def select_agent_wrapper(agent_instance):
    from metagpt.roles import Role
    from ragent.agents.metagpt_agent import MetaGptAgent
    from langchain.agents import AgentExecutor

    if isinstance(agent_instance, MetaGptAgent):
        return MetaGptAgent
    elif isinstance(agent_instance, CoTAgent):
        return CoTAgent
    elif isinstance(agent_instance, AgentExecutor):
        from ragent.agents.langchain_agent import LangchainAgent

        return LangchainAgent
    else:
        return type(agent_instance)
