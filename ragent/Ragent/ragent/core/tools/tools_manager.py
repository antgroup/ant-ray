import logging

from ragent.core.graph.graph import RagentGraph
from ragent.core.runnables import ToolRunnable

logger = logging.getLogger(__name__)


class ToolEntity:
    def __init__(self, tool: ToolRunnable):
        self.tool = RagentGraph(tool)
        self.tool_spec = tool.tool_spec
        self.tool_name = self.tool_spec.name


class ToolsManager:
    def __init__(self):
        self.tool_map = {}

    def register_tools(self, *tools: ToolRunnable):
        for tool in tools:
            tool_entity = ToolEntity(tool)
            self.tool_map[tool_entity.tool_name] = tool_entity
            logger.info(f"Registered a tool:{tool_entity.tool_name}")

    def use_tool(self, tool_name: str, **kwargs):
        tool_entity = self.tool_map.get(tool_name, None)
        if tool_entity is None:
            logger.error(f"Failed to find tool {tool_name}.")
            return None

        return tool_entity.tool.invoke(**kwargs)

    def elaborate_tools(self):
        tools_info = [tool.tool_spec.__dict__ for tool in self.tool_map.values()]
        return tools_info
