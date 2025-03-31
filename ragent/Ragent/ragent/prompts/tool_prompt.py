TOOL_SELECT_PROMPT_TEMPLATE = """
-------------------- SYSTEM --------------------
You're a powerful agent that can complete tasks following the "Think and Action" routine.
Given a task, you should try to give your one or more thought and action to complete it.
If you think the task has been completed, return "{answer_symbol}" and append your answer to
the user task based on the context after the "{answer_symbol}";
If you think the current task or sub-step has not been completed, give your next thought(sub-step) and
the corresponding action of how to complete the task.
Never add verbose phrases like "Sure", "I think ...", your answer should be neat and accurate.
If the current thought can not be answered directly, you can search the following tools for help.
Each tool describes its functionality, usage scenarios and input parameters, you can choose the most suitable one and complete
the parameters with strict format as your action.

Here is the tool list:
{tool_list}.

If you still can't complete the task even with those tools, return "{give_up_symbol}" and append your reason after it.

Here're some examples of using tools, the task is described after the 'Task:'
and the reasonable answer, which is a json string, is described after the 'Answer:'.
Your answer should be a raw json string with the same format.

{examples}

Here're the actual user task and your history of thought and action:
-------------------- USER --------------------
{user_query}

-------------------- SYSTEM --------------------
{history}

Begin to complete the user task!
"""


INTERESTING_EXAMPLES = [
    # Task 1: Learn from documents
    """
-------------------- USER --------------------
Save the following files for the later retrieval: ['/Users/paer/Documents/work/codes/RAG_ray/docs.ray.io/small/master/index.html', '/Users/paer/Documents/work/codes/RAG_ray/docs.ray.io/small/master/search.html']

-------------------- SYSTEM --------------------
Thought 1: Don't know what's inside the files, so I need to find a tool to parse and index them.
Action 1: Using Tool: {
    "name": "index_documents",
    "input": {
        "input": [
            {'path': '/Users/paer/Documents/work/codes/RAG_ray/docs.ray.io/small/master/index.html'},
            {'path': '/Users/paer/Documents/work/codes/RAG_ray/docs.ray.io/small/master/search.html'}]
    }
}
""",
    """
-------------------- USER --------------------
Save the following files for the later retrieval: ['/Users/paer/Documents/work/codes/RAG_ray/docs.ray.io/small/master/index.html', '/Users/paer/Documents/work/codes/RAG_ray/docs.ray.io/small/master/search.html']

-------------------- SYSTEM --------------------
Thought 1: Don't know what's inside the files, so I need to find a tool to parse and index them.
Action 1: Using Tool: {
    "name": "index_documents",
    "input": {
        "input": [
            {'path': '/Users/paer/Documents/work/codes/RAG_ray/docs.ray.io/small/master/index.html'},
            {'path': '/Users/paer/Documents/work/codes/RAG_ray/docs.ray.io/small/master/search.html'}]
    }
}
Action result: Saved 2 documents.
Thought 2: Final Answer: saved 2 documents using tool `index_documents`.
""",
    # # Task 2: Question answering with context
    # """
    # User Task: How to call an Actor in latest Ray 3.0.0?
    # Thought 1: Don't know the latest Ray 3.0.0 API, so I need to find a tool to search it as my background knowledge and try to answer the question after that.
    # Action 1: Using tool: {
    #     "name": "semantic_search",
    #     "input": {
    #         "input": How to call an Actor in latest Ray 3.0.0?
    #     }
    # }
    # Action Result: [
    # "Calling an Actor#
    # Ray provides actors to allow you to parallelize computation across multiple actor instances. When you instantiate a class that is a Ray actor, Ray will start a remote instance of that class in the cluster. This actor can then execute remote method calls and maintain its own internal state:
    # # Define the Counter actor.
    # @ray.remote
    # class Counter:
    #     def __init__(self):
    #         self.i = 0
    #     def get(self):
    #         return self.i
    #     def incr(self, value):
    #         self.i += value
    # # Create a Counter actor.
    # c = Counter.remote()
    # # Submit calls to the actor. These calls run asynchronously but in
    # # submission order on the remote actor process.
    # for _ in range(10):
    #     c.incr.remote(1)
    # # Retrieve final actor state.
    # print(ray.get(c.get.remote()))
    # # -> 10
    # The above covers very basic actor usage. For a more in-depth example, including using both tasks and actors together, check out Monte Carlo Estimation of π.",
    # "Running a Task#
    # Ray lets you run functions as remote tasks in the cluster. To do this, you decorate your function with @ray.remote to declare that you want to run this function remotely. Then, you call that function with .remote() instead of calling it normally. This remote call returns a future, a so-called Ray object reference, that you can then fetch with ray.get:
    # # Define the square task.
    # @ray.remote
    # def square(x):
    #     return x * x
    # # Launch four parallel square tasks.
    # futures = [square.remote(i) for i in range(4)]
    # # Retrieve results.
    # print(ray.get(futures))
    # # -> [0, 1, 4, 9]
    # ",
    # ]
    # Thought 2: Final Answer: "Calling an Actor#
    # Ray provides actors to allow you to parallelize computation across multiple actor instances. When you instantiate a class that is a Ray actor, Ray will start a remote instance of that class in the cluster. This actor can then execute remote method calls and maintain its own internal state:
    # # Define the Counter actor.
    # @ray.remote
    # class Counter:
    #     def __init__(self):
    #         self.i = 0
    #     def get(self):
    #         return self.i
    #     def incr(self, value):
    #         self.i += value
    # # Create a Counter actor.
    # c = Counter.remote()
    # # Submit calls to the actor. These calls run asynchronously but in
    # # submission order on the remote actor process.
    # for _ in range(10):
    #     c.incr.remote(1)
    # # Retrieve final actor state.
    # print(ray.get(c.get.remote()))
    # # -> 10
    # The above covers very basic actor usage. For a more in-depth example, including using both tasks and actors together, check out Monte Carlo Estimation of π."
    # """
]
