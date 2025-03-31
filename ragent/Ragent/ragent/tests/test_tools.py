import pytest
import sys
from typing import List

from ragent.core.BaseTool import Tool
from typing_extensions import Annotated


@Tool(
    desc="Take a list of numbers and return the ascending ordered list",
    type="PythonCode",
    input={
        "data": "list of integers",
    },
    output="The ascending ordered list",
    metadata={
        "tag": [
            "big data",
        ]
    },
)
def ant_sort(input: List[int], config=None):
    sorted_numbers = sorted(input)
    return ["ant_" + str(i) for i in sorted_numbers]


def test_tool_calling():
    res = ant_sort.invoke([1, 5, 9, 7, 4])
    assert res == ["ant_1", "ant_4", "ant_5", "ant_7", "ant_9"]
    inputs = {"input": [1, 5, 9, 7, 4]}
    res = ant_sort.invoke(**inputs)
    assert res == ["ant_1", "ant_4", "ant_5", "ant_7", "ant_9"]


@Tool
def udf_tool_simple(input: Annotated[List[int], "int elements"], config: dict = None):
    """
    User defined function for testing the generated tool spec.
    Args:
        input: list of integers to be sorted
        config: Configurations of sorting, default is None
    """
    return "hello world"


@Tool(
    output="A fixed-content string",
    metadata={
        "tag": [
            "big data",
        ]
    },
)
def udf_tool_meta(input: Annotated[List[int], "int elements"], config: dict = None):
    """
    User defined function with little information for testing the generated tool spec.

    Args:
        input: list of integers to be sorted
        config: Configurations of sorting, default is None
    """
    return "hello world"


@Tool(
    output="A fixed-content string",
    desc="Description of the tool.",
    metadata={
        "tag": [
            "big data",
        ]
    },
)
def udf_tool_both_desc_docstring(
    input: Annotated[List[int], "int elements"], config: dict = None
):
    """
    User defined function with little information for testing the generated tool spec.

    Args:
        input: list of integers to be sorted
        config: Configurations of sorting, default is None
    """
    return "hello world"


def test_generated_tool_spec():
    tool_spec = udf_tool_simple.tool_spec
    assert tool_spec.name == "udf_tool_simple"
    assert (
        "User defined function for testing the generated tool spec." in tool_spec.desc
    )
    assert tool_spec.type == "PythonCode"
    assert tool_spec.input
    assert "input" in tool_spec.input
    assert "config" in tool_spec.input
    assert "int elements" in tool_spec.input["input"]
    assert "dict" in tool_spec.input["config"]
    assert tool_spec.output == "Any"
    assert tool_spec.metadata == {}

    tool_spec = udf_tool_meta.tool_spec
    assert tool_spec.name == "udf_tool_meta"
    assert "with little" in tool_spec.desc
    assert tool_spec.type == "PythonCode"
    assert tool_spec.input
    assert "input" in tool_spec.input
    assert "config" in tool_spec.input
    assert "int elements" in tool_spec.input["input"]
    assert "dict" in tool_spec.input["config"]
    assert tool_spec.output == "A fixed-content string"
    assert tool_spec.metadata == {
        "tag": [
            "big data",
        ]
    }

    tool_spec = udf_tool_both_desc_docstring.tool_spec
    assert tool_spec.name == "udf_tool_both_desc_docstring"
    assert "Description of the tool." in tool_spec.desc
    assert tool_spec.type == "PythonCode"
    assert tool_spec.input
    assert "input" in tool_spec.input
    assert "config" in tool_spec.input
    assert "int elements" in tool_spec.input["input"]
    assert "dict" in tool_spec.input["config"]
    assert tool_spec.output == "A fixed-content string"
    assert tool_spec.metadata == {
        "tag": [
            "big data",
        ]
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
