# This file is used by CrossLanguageInvocationTest.java to test cross-language
# invocation.

import asyncio
import pytest
import ray
from ray.util.scheduling_strategies import (ActorAffinityMatchExpression,
                                            ActorAffinityOperator,
                                            ActorAffinitySchedulingStrategy)


@ray.remote
def py_return_input(v):
    return v


@ray.remote
def py_func_call_java_function():
    try:
        # None
        r = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest",
            "returnInput").remote(None)
        assert ray.get(r) is None
        # bool
        r = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest",
            "returnInputBoolean").remote(True)
        assert ray.get(r) is True
        # int
        r = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest",
            "returnInputInt").remote(100)
        assert ray.get(r) == 100
        # double
        r = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest",
            "returnInputDouble").remote(1.23)
        assert ray.get(r) == 1.23
        # string
        r = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest",
            "returnInputString").remote("Hello World!")
        assert ray.get(r) == "Hello World!"
        # list (tuple will be packed by pickle,
        # so only list can be transferred across language)
        r = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest",
            "returnInputIntArray").remote([1, 2, 3])
        assert ray.get(r) == [1, 2, 3]
        # pack
        f = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "pack")
        input = [100, "hello", 1.23, [1, "2", 3.0]]
        r = f.remote(*input)
        assert ray.get(r) == input
        return "success"
    except Exception as ex:
        return str(ex)


@ray.remote
def py_func_call_java_actor(value):
    assert isinstance(value, bytes)
    c = ray.cross_language.java_actor_class(
        "io.ray.test.CrossLanguageInvocationTest$TestActor")
    java_actor = c.remote(b"Counter")
    r = java_actor.concat.remote(value)
    return ray.get(r)


@ray.remote
def py_func_call_java_actor_from_handle(actor_handle):
    r = actor_handle.concat.remote(b"2")
    return ray.get(r)


@ray.remote
def py_func_call_python_actor_from_handle(actor_handle):
    r = actor_handle.increase.remote(2)
    return ray.get(r)


@ray.remote
def py_func_pass_python_actor_handle():
    counter = Counter.remote(2)
    f = ray.cross_language.java_function(
        "io.ray.test.CrossLanguageInvocationTest", "callPythonActorHandle")
    r = f.remote(counter)
    return ray.get(r)


@ray.remote
def py_func_python_raise_exception():
    1 / 0


@ray.remote
def py_func_java_throw_exception():
    f = ray.cross_language.java_function(
        "io.ray.test.CrossLanguageInvocationTest", "throwException")
    r = f.remote()
    return ray.get(r)


@ray.remote
def py_func_nest_python_raise_exception():
    f = ray.cross_language.java_function(
        "io.ray.test.CrossLanguageInvocationTest", "raisePythonException")
    r = f.remote()
    return ray.get(r)


@ray.remote
def py_func_nest_java_throw_exception():
    f = ray.cross_language.java_function(
        "io.ray.test.CrossLanguageInvocationTest", "throwJavaException")
    r = f.remote()
    return ray.get(r)


@ray.remote
class Counter(object):
    def __init__(self, value):
        self.value = int(value)

    def increase(self, delta):
        self.value += int(delta)
        return str(self.value).encode("utf-8")

    def value(self):
        return str(self.value).encode("utf-8")


@ray.remote
class AsyncCounter(object):
    def __init__(self, value):
        self.value = int(value)
        self.event = asyncio.Event()

    async def block_task(self):
        self.event.wait()

    async def increase(self, delta):
        self.value += int(delta)
        self.event.set()
        return str(self.value).encode("utf-8")


@ray.remote
class NonAsyncCounter(object):
    def __init__(self, value):
        self.value = int(value)
        self.event = asyncio.Event()

    def block_task(self):
        self.event.wait()

    def increase(self, delta):
        self.value += int(delta)
        return str(self.value).encode("utf-8")


@ray.remote
class RuntimeEnvActor(object):
    def __init__(self):
        pass

    def get_env_var(self, key):
        import os
        return os.environ[key.decode()].encode()


@ray.remote
def py_func_get_and_invoke_named_actor():
    java_named_actor = ray.get_actor("java_named_actor")
    assert ray.get(java_named_actor.concat.remote(b"world")) == b"helloworld"
    return b"true"


@ray.remote
def py_func_call_java_overrided_method_with_default_keyword():
    cls = ray.cross_language.java_actor_class("io.ray.test.ExampleImpl")
    handle = cls.remote()
    return ray.get(handle.echo.remote("hi"))


@ray.remote
def py_func_call_java_overloaded_method():
    cls = ray.cross_language.java_actor_class("io.ray.test.ExampleImpl")
    handle = cls.remote()
    ref1 = handle.overloadedFunc.remote("first")
    ref2 = handle.overloadedFunc.remote("first", "second")
    result = ray.get([ref1, ref2])
    assert result == ["first", "firstsecond"]
    return True


def py_func_call_java_actor_test_actor_affinity():
    actor_0 = Counter.options(
        scheduling_strategy=ActorAffinitySchedulingStrategy([
            ActorAffinityMatchExpression("language", ActorAffinityOperator.IN,
                                         ["java"], False)
        ])).remote(0)
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(actor_0.value.remote(), timeout=3)

    java_actor_1 = ray.cross_language.java_actor_class(
        "io.ray.test.CrossLanguageInvocationTest$TestActor").options(
            scheduling_strategy=ActorAffinitySchedulingStrategy([
                ActorAffinityMatchExpression(
                    "language", ActorAffinityOperator.IN, ["java"], False)
            ])).remote(b"Counter")
    with pytest.raises(ray.exceptions.GetTimeoutError):
        assert ray.get(java_actor_1.value.remote(), timeout=3) == b"Counter"

    java_actor_2 = ray.cross_language.java_actor_class(
        "io.ray.test.CrossLanguageInvocationTest$TestActor").options(labels={
            "language": "java"
        }).remote(b"Counter")
    assert ray.get(java_actor_2.value.remote(), timeout=3) == b"Counter"

    assert ray.get(actor_0.value.remote(), timeout=3) == b"0"
    assert ray.get(java_actor_1.value.remote(), timeout=3) == b"Counter"
    return b"OK"


@ray.remote
def py_get_current_node_id_hex():
    return ray.get_runtime_context().node_id.hex()
