from typing import List
from ragent.core.BaseTool import Tool
from typing_extensions import Annotated


@Tool(
    desc="Take a list of numbers and return the ordered list in an special 'Ant' way.",
    type="PythonCode",
    input={
        "input": "list of integers",
    },
    output="The ascending ordered list",
    metadata={
        "tag": [
            "big data",
        ]
    },
)
def ant_sort(
    input: List[int], config: Annotated[dict, "Config dict for sorting"] = None
):
    """
    Sorting the function in a special way.
    Args:
        input: list of integers
        config: None
    """
    sorted_numbers = sorted(input)
    return str(["ant_" + str(i) for i in sorted_numbers])


if __name__ == "__main__":
    numbers = [3, 5, 2, 1, 4]
    sorted_numbers = ant_sort.invoke(numbers)
    print(f"Sorted numbers: {sorted_numbers}")
    expected = ["ant_1", "ant_2", "ant_3", "ant_4", "ant_5"]
    assert sorted_numbers == expected, f"Expected {expected}, got {sorted_numbers}"
