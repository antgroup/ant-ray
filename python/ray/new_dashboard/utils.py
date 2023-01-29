import abc
import asyncio
import collections
import concurrent.futures
import datetime
import functools
import importlib
import inspect
import json
import logging
import os
import queue
import sys
import pkgutil
import socket
import threading
import traceback
import subprocess
from abc import ABCMeta, abstractmethod
from base64 import b64decode
from collections import namedtuple
from collections.abc import MutableMapping, Mapping, Sequence
from typing import Any, List
from stat import S_IREAD, S_IRGRP, S_IROTH

import aiohttp.signals
import aiohttp.web
import redis
import aioredis
import time
import grpc.aio as aiogrpc
from aiohttp import hdrs
from aiohttp.frozenlist import FrozenList
from aiohttp.typedefs import PathLike
from aiohttp.web import RouteDef
from google.protobuf.json_format import MessageToDict

import ray.new_dashboard.consts as dashboard_consts
from ray.ray_constants import env_bool
from ray._private.utils import binary_to_hex
from ray.util import metrics
from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
import base64
import codecs

try:
    # This function has been added in Python 3.7. Prior to Python 3.7,
    # the low-level asyncio.ensure_future() function can be used instead.
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future

logger = logging.getLogger(__name__)


class DashboardAgentModule(abc.ABC):
    def __init__(self, dashboard_agent):
        """
        Initialize current module when DashboardAgent loading modules.

        :param dashboard_agent: The DashboardAgent instance.
        """
        self._dashboard_agent = dashboard_agent

    @abc.abstractmethod
    async def run(self, server):
        """
        Run the module in an asyncio loop. An agent module can provide
        servicers to the server.

        :param server: Asyncio GRPC server.
        """


class DashboardHeadModule(abc.ABC):
    def __init__(self, dashboard_head):
        """
        Initialize current module when DashboardHead loading modules.

        :param dashboard_head: The DashboardHead instance.
        """
        self._dashboard_head = dashboard_head

    @abc.abstractmethod
    async def run(self, server):
        """
        Run the module in an asyncio loop. A head module can provide
        servicers to the server.

        :param server: Asyncio GRPC server.
        """


class ClassMethodRouteTable:
    """A helper class to bind http route to class method."""

    _bind_map = collections.defaultdict(dict)
    _routes = aiohttp.web.RouteTableDef()

    class _BindInfo:
        def __init__(self, filename, lineno, instance):
            self.filename = filename
            self.lineno = lineno
            self.instance = instance

    @classmethod
    def routes(cls):
        return cls._routes

    @classmethod
    def bound_routes(cls):
        bound_items = []
        for r in cls._routes._items:
            if isinstance(r, RouteDef):
                route_method = getattr(r.handler, "__route_method__")
                route_path = getattr(r.handler, "__route_path__")
                instance = cls._bind_map[route_method][route_path].instance
                if instance is not None:
                    bound_items.append(r)
            else:
                bound_items.append(r)
        routes = aiohttp.web.RouteTableDef()
        routes._items = bound_items
        return routes

    @classmethod
    def _register_route(cls, method, path, **kwargs):
        def _wrapper(handler):
            if path in cls._bind_map[method]:
                bind_info = cls._bind_map[method][path]
                raise Exception(f"Duplicated route path: {path}, "
                                f"previous one registered at "
                                f"{bind_info.filename}:{bind_info.lineno}")

            bind_info = cls._BindInfo(handler.__code__.co_filename,
                                      handler.__code__.co_firstlineno, None)
            handler = ClusterTokenManager.check(handler)

            @functools.wraps(handler)
            async def _handler_route(*args) -> aiohttp.web.Response:
                try:
                    # Make the route handler as a bound method.
                    # The args may be:
                    #   * (Request, )
                    #   * (self, Request)
                    req = args[-1]
                    return await handler(bind_info.instance, req)
                except Exception:
                    logger.exception("Handle %s %s failed.", method, path)
                    return rest_response(
                        success=False, message=traceback.format_exc())

            cls._bind_map[method][path] = bind_info
            _handler_route.__route_method__ = method
            _handler_route.__route_path__ = path
            return cls._routes.route(method, path, **kwargs)(_handler_route)

        return _wrapper

    @classmethod
    def head(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_HEAD, path, **kwargs)

    @classmethod
    def get(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_GET, path, **kwargs)

    @classmethod
    def post(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_POST, path, **kwargs)

    @classmethod
    def put(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_PUT, path, **kwargs)

    @classmethod
    def patch(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_PATCH, path, **kwargs)

    @classmethod
    def delete(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_DELETE, path, **kwargs)

    @classmethod
    def view(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_ANY, path, **kwargs)

    @classmethod
    def static(cls, prefix: str, path: PathLike, **kwargs: Any) -> None:
        cls._routes.static(prefix, path, **kwargs)

    @classmethod
    def bind(cls, instance):
        def predicate(o):
            if inspect.ismethod(o):
                return hasattr(o, "__route_method__") and hasattr(
                    o, "__route_path__")
            return False

        handler_routes = inspect.getmembers(instance, predicate)
        for _, h in handler_routes:
            cls._bind_map[h.__func__.__route_method__][
                h.__func__.__route_path__].instance = instance


def dashboard_module(enable):
    """A decorator for dashboard module."""

    def _cls_wrapper(cls):
        cls.__ray_dashboard_module_enable__ = enable
        return cls

    return _cls_wrapper


def get_all_modules(module_type):
    logger.info(f"Get all modules by type: {module_type.__name__}")
    import ray.new_dashboard.modules

    for module_loader, name, ispkg in pkgutil.walk_packages(
            ray.new_dashboard.modules.__path__,
            ray.new_dashboard.modules.__name__ + "."):
        importlib.import_module(name)
    return [
        m for m in module_type.__subclasses__()
        if getattr(m, "__ray_dashboard_module_enable__", True)
    ]


def to_posix_time(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()


def address_tuple(address):
    if isinstance(address, tuple):
        return address
    ip, port = address.split(":")
    return ip, int(port)


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return binary_to_hex(obj)
        if isinstance(obj, Immutable):
            return obj.mutable()
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def rest_response(success, message, code=None,
                  **kwargs) -> aiohttp.web.Response:
    # In the dev context we allow a dev server running on a
    # different port to consume the API, meaning we need to allow
    # cross-origin access
    if os.environ.get("RAY_DASHBOARD_DEV") == "1":
        headers = {"Access-Control-Allow-Origin": "*"}
    else:
        headers = {}
    return aiohttp.web.json_response(
        {
            "result": success,
            "code": code,
            "msg": message,
            "data": to_google_style(kwargs),
            "timestamp": time.time(),
        },
        dumps=functools.partial(json.dumps, cls=CustomEncoder),
        headers=headers)


def rest_raw_response(success, message, **kwargs) -> aiohttp.web.Response:
    return aiohttp.web.json_response({
        "result": success,
        "msg": message,
        "data": kwargs,
        "timestamp": time.time(),
    })


def response_to_dict(resp) -> dict:
    return json.loads(resp.body)


def to_camel_case(snake_str):
    """Convert a snake str to camel case."""
    if not isinstance(snake_str, str):
        return snake_str
    components = snake_str.split("_")
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + "".join(x.title() for x in components[1:])


def to_google_style(d):
    """Recursive convert all keys in dict to google style."""
    new_dict = {}

    for k, v in d.items():
        if isinstance(v, dict):
            new_dict[to_camel_case(k)] = to_google_style(v)
        elif isinstance(v, list):
            new_list = []
            for i in v:
                if isinstance(i, dict):
                    new_list.append(to_google_style(i))
                else:
                    new_list.append(i)
            new_dict[to_camel_case(k)] = new_list
        else:
            new_dict[to_camel_case(k)] = v
    return new_dict


def message_to_dict(message, decode_keys=None, **kwargs):
    """Convert protobuf message to Python dict."""

    def _decode_keys(d):
        for k, v in d.items():
            if isinstance(v, dict):
                d[k] = _decode_keys(v)
            if isinstance(v, list):
                new_list = []
                for i in v:
                    if isinstance(i, dict):
                        new_list.append(_decode_keys(i))
                    else:
                        new_list.append(i)
                d[k] = new_list
            else:
                if k in decode_keys:
                    d[k] = binary_to_hex(b64decode(v))
                else:
                    d[k] = v
        return d

    if decode_keys:
        return _decode_keys(
            MessageToDict(message, use_integers_for_enums=False, **kwargs))
    else:
        return MessageToDict(message, use_integers_for_enums=False, **kwargs)


# The cache value type used by aiohttp_cache.
_AiohttpCacheValue = namedtuple("AiohttpCacheValue",
                                ["data", "expiration", "task"])
# The methods with no request body used by aiohttp_cache.
_AIOHTTP_CACHE_NOBODY_METHODS = {hdrs.METH_GET, hdrs.METH_DELETE}


def aiohttp_cache(
        ttl_seconds=dashboard_consts.DEFAULT_AIOHTTP_CACHE_TTL_SECONDS,
        maxsize=dashboard_consts.DEFAULT_AIOHTTP_CACHE_MAX_SIZE,
        enable=not env_bool(
            dashboard_consts.AIOHTTP_CACHE_DISABLE_ENVIRONMENT_KEY, False),
        include_filter=None,
        exclude_filter=None,
        cache_exception=True):
    assert maxsize > 0
    cache = collections.OrderedDict()

    def _wrapper(handler):
        if enable:

            @functools.wraps(handler)
            async def _cache_handler(*args) -> aiohttp.web.Response:
                # Make the route handler as a bound method.
                # The args may be:
                #   * (Request, )
                #   * (self, Request)
                req = args[-1]
                # Request filter.
                include = include_filter(req) if include_filter else True
                exclude = exclude_filter(req) if exclude_filter else False

                if not include or exclude:
                    return await handler(*args)

                # Make key.
                if req.method in _AIOHTTP_CACHE_NOBODY_METHODS:
                    key = req.path_qs
                else:
                    key = (req.path_qs, await req.read())
                # Query cache.
                value = cache.get(key)
                if value is not None:
                    if value.expiration >= time.time():
                        cache.move_to_end(key)
                        # Return cached value only when data is not expired.
                        return aiohttp.web.Response(**value.data)
                    if value.task is not None:
                        # Wait for other task's result if it's still running.
                        return await value.task

                def _update_cache(_task):
                    try:
                        response = _task.result()
                    except Exception as ex:
                        if not cache_exception:
                            logger.info(
                                "Skip cache the result of %s due to %s",
                                _cache_handler, ex)
                            return
                        response = rest_response(
                            success=False, message=traceback.format_exc())

                    data = {
                        "status": response.status,
                        "headers": dict(response.headers),
                        "body": response.body,
                    }
                    cache[key] = _AiohttpCacheValue(data,
                                                    time.time() + ttl_seconds,
                                                    None)
                    cache.move_to_end(key)
                    if len(cache) > maxsize:
                        cache.popitem(last=False)

                task = create_task(handler(*args))
                task.add_done_callback(_update_cache)
                cache[key] = _AiohttpCacheValue(None, 0, task)
                return await task

            suffix = f"[cache ttl={ttl_seconds}, max_size={maxsize}]"
            _cache_handler.__name__ += suffix
            _cache_handler.__qualname__ += suffix
            _cache_handler.cache = cache
            return _cache_handler
        else:
            return handler

    if inspect.iscoroutinefunction(ttl_seconds):
        target_func = ttl_seconds
        ttl_seconds = dashboard_consts.DEFAULT_AIOHTTP_CACHE_TTL_SECONDS
        return _wrapper(target_func)
    else:
        return _wrapper


class SignalManager:
    _signals = FrozenList()

    @classmethod
    def register(cls, sig):
        cls._signals.append(sig)

    @classmethod
    def freeze(cls):
        cls._signals.freeze()
        for sig in cls._signals:
            sig.freeze()


class Signal(aiohttp.signals.Signal):
    __slots__ = ()

    def __init__(self, owner):
        super().__init__(owner)
        SignalManager.register(self)

    def freeze(self) -> None:
        for receiver in self:
            # Currently, we only support coroutine function
            # as receiver.
            #
            # Be aware that functools.partial may returns
            # incorrect function type in Python < 3.8:
            # https://docs.python.org/3/library/inspect.html#inspect.isgeneratorfunction
            #
            # We do not support callable returns awaitabe
            # because it is hard to check type in freeze().
            assert inspect.iscoroutinefunction(receiver)
        super().freeze()


class Bunch(dict):
    """A dict with attribute-access."""

    def __getattr__(self, key):
        try:
            return self.__getitem__(key)
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)


class Change:
    """Notify change object."""

    def __init__(self, owner=None, old=None, new=None):
        self.owner = owner
        self.old = old
        self.new = new

    def __str__(self):
        return f"Change(owner: {type(self.owner)}), " \
               f"old: {self.old}, new: {self.new}"


class NotifyQueue:
    """Asyncio notify queue for Dict signal."""

    _queue = asyncio.Queue()

    @classmethod
    def put(cls, co):
        cls._queue.put_nowait(co)

    @classmethod
    async def get(cls):
        return await cls._queue.get()


"""
https://docs.python.org/3/library/json.html?highlight=json#json.JSONEncoder
    +-------------------+---------------+
    | Python            | JSON          |
    +===================+===============+
    | dict              | object        |
    +-------------------+---------------+
    | list, tuple       | array         |
    +-------------------+---------------+
    | str               | string        |
    +-------------------+---------------+
    | int, float        | number        |
    +-------------------+---------------+
    | True              | true          |
    +-------------------+---------------+
    | False             | false         |
    +-------------------+---------------+
    | None              | null          |
    +-------------------+---------------+
"""
_json_compatible_types = {
    dict, list, tuple, str, int, float, bool,
    type(None), bytes
}


def is_immutable(self):
    raise TypeError("%r objects are immutable" % self.__class__.__name__)


def make_immutable(value, strict=True):
    value_type = type(value)
    if value_type is dict:
        return ImmutableDict(value)
    if value_type is list:
        return ImmutableList(value)
    if strict:
        if value_type not in _json_compatible_types:
            raise TypeError("Type {} can't be immutable.".format(value_type))
    return value


class Immutable(metaclass=ABCMeta):
    @abstractmethod
    def mutable(self):
        pass


class ImmutableList(Immutable, Sequence):
    """Makes a :class:`list` immutable.
    """

    __slots__ = ("_list", "_proxy")

    def __init__(self, list_value):
        if type(list_value) not in (list, ImmutableList):
            raise TypeError(f"{type(list_value)} object is not a list.")
        if isinstance(list_value, ImmutableList):
            list_value = list_value.mutable()
        self._list = list_value
        self._proxy = [None] * len(list_value)

    def __reduce_ex__(self, protocol):
        return type(self), (self._list, )

    def mutable(self):
        self._proxy = [None] * len(self._list)
        return self._list

    def __eq__(self, other):
        if isinstance(other, ImmutableList):
            other = other.mutable()
        return list.__eq__(self._list, other)

    def __ne__(self, other):
        if isinstance(other, ImmutableList):
            other = other.mutable()
        return list.__ne__(self._list, other)

    def __contains__(self, item):
        if isinstance(item, Immutable):
            item = item.mutable()
        return list.__contains__(self._list, item)

    def __getitem__(self, item):
        proxy = self._proxy[item]
        if proxy is None:
            proxy = self._proxy[item] = make_immutable(self._list[item])
        return proxy

    def __len__(self):
        return len(self._list)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, list.__repr__(self._list))


class ImmutableDict(Immutable, Mapping):
    """Makes a :class:`dict` immutable.
    """

    __slots__ = ("_dict", "_proxy")

    def __init__(self, dict_value):
        if type(dict_value) not in (dict, ImmutableDict):
            raise TypeError(f"{type(dict_value)} object is not a dict.")
        if isinstance(dict_value, ImmutableDict):
            dict_value = dict_value.mutable()
        self._dict = dict_value
        self._proxy = {}

    def __reduce_ex__(self, protocol):
        return type(self), (self._dict, )

    def mutable(self):
        self._proxy = {}
        return self._dict

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return make_immutable(default)

    def __eq__(self, other):
        if isinstance(other, ImmutableDict):
            other = other.mutable()
        return dict.__eq__(self._dict, other)

    def __ne__(self, other):
        if isinstance(other, ImmutableDict):
            other = other.mutable()
        return dict.__ne__(self._dict, other)

    def __contains__(self, item):
        if isinstance(item, Immutable):
            item = item.mutable()
        return dict.__contains__(self._dict, item)

    def __getitem__(self, item):
        proxy = self._proxy.get(item, None)
        if proxy is None:
            proxy = self._proxy[item] = make_immutable(self._dict[item])
        return proxy

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self):
        if len(self._proxy) != len(self._dict):
            for key in self._dict.keys() - self._proxy.keys():
                self._proxy[key] = make_immutable(self._dict[key])
        return iter(self._proxy)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict.__repr__(self._dict))


class Dict(ImmutableDict, MutableMapping):
    """A simple descriptor for dict type to notify data changes.

    :note: Only the first level data report change.
    """

    ChangeItem = namedtuple("DictChangeItem", ["key", "value"])

    def __init__(self, *args, **kwargs):
        super().__init__(dict(*args, **kwargs))
        self.signal = Signal(self)

    def __setitem__(self, key, value):
        old = self._dict.pop(key, None)
        self._proxy.pop(key, None)
        self._dict[key] = value
        if len(self.signal) and old != value:
            if old is None:
                co = self.signal.send(
                    Change(owner=self, new=Dict.ChangeItem(key, value)))
            else:
                co = self.signal.send(
                    Change(
                        owner=self,
                        old=Dict.ChangeItem(key, old),
                        new=Dict.ChangeItem(key, value)))
            NotifyQueue.put(co)

    def __delitem__(self, key):
        old = self._dict.pop(key, None)
        self._proxy.pop(key, None)
        if len(self.signal) and old is not None:
            co = self.signal.send(
                Change(owner=self, old=Dict.ChangeItem(key, old)))
            NotifyQueue.put(co)

    def reset(self, d):
        assert isinstance(d, Mapping)
        for key in self._dict.keys() - d.keys():
            del self[key]
        for key, value in d.items():
            self[key] = value


class LimitedCapacityDict(Dict):
    """A dict with limited capacity.

    :note: FIFO item pop order is guaranteed.
    """

    __slots__ = ("_write_proxy", "_capacity")

    def __init__(self, capacity: int, *args, **kwargs):
        assert capacity > 0
        self._capacity = capacity
        self._write_proxy = collections.OrderedDict()
        super().__init__(*args, **kwargs)

    def __delitem__(self, key):
        del self._write_proxy[key]
        super().__delitem__(key)

    def __setitem__(self, key, value):
        if len(self) == self._capacity and key not in self._dict:
            first_inserted_key = next(iter(self._write_proxy))
            del self[first_inserted_key]
        # Use the OrderedDict as an OrderedSet to record key insertion order
        self._write_proxy[key] = None
        super().__setitem__(key, value)

    def reset(self, kvs):
        if isinstance(kvs, Mapping):
            kvs = list(kvs.items())
        assert isinstance(kvs, List)
        if len(kvs) > self._capacity:
            kvs = kvs[-self._capacity:]
        key_set = {kv[0] for kv in kvs}
        # Delete keys not in kvs in insertion order
        keys_to_delete = list(
            filter(lambda key: key not in key_set, self._write_proxy))
        for key in keys_to_delete:
            del self[key]
        for key, value in kvs:
            self[key] = value


# Register immutable types.
for immutable_type in Immutable.__subclasses__():
    _json_compatible_types.add(immutable_type)


async def get_aioredis_client(redis_address, redis_password,
                              retry_interval_seconds, retry_times):
    for x in range(retry_times):
        try:
            return await aioredis.create_redis_pool(
                address=redis_address, password=redis_password)
        except (socket.gaierror, ConnectionError) as ex:
            logger.error("Connect to Redis failed: %s, retry...", ex)
            await asyncio.sleep(retry_interval_seconds)
    # Raise exception from create_redis_pool
    return await aioredis.create_redis_pool(
        address=redis_address, password=redis_password)


RedisCommand = namedtuple("RedisCommand",
                          ["method", "args", "kwargs", "future"])


class _RedisClientThreadBase(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cmd_queue = queue.Queue()
        self.stop_event = threading.Event()

    def __getattr__(self, item):
        meth = getattr(self.redis_client, item)

        @functools.wraps(meth)
        def _async_wrapper(*args, **kwargs):
            future = concurrent.futures.Future()
            cmd = RedisCommand(meth, args, kwargs, future)
            self._cmd_queue.put(cmd)
            return asyncio.wrap_future(future)

        return _async_wrapper

    def run(self) -> None:
        while not self.stop_event.is_set():
            cmd = self._cmd_queue.get()
            try:
                r = cmd.method(*cmd.args, **cmd.kwargs)
                cmd.future.set_result(r)
            except Exception as e:
                cmd.future.set_exception(e)

    def stop(self):
        self.stop_event.set()


class RedisClientPubsubThread(_RedisClientThreadBase):
    def __init__(self, redis_client_thread, **kwargs):
        super().__init__(name="RedisClientPubsubThread", daemon=True)
        self.redis_client_thread = redis_client_thread
        self.pubsub = redis_client_thread.redis_client.pubsub(**kwargs)
        self.running = threading.Event()

    def __getattr__(self, item):
        if item not in [
                "psubscribe", "punsubscribe", "subscribe", "unsubscribe"
        ]:
            raise AttributeError(
                f"'{type(self)}' type has no attribute '{item}'")

        meth = getattr(self.pubsub, item)

        @functools.wraps(meth)
        def _sync_wrapper(*args, **kwargs):
            future = concurrent.futures.Future()
            cmd = RedisCommand(meth, args, kwargs, future)
            self._cmd_queue.put(cmd)
            return future.result()

        return _sync_wrapper

    @staticmethod
    def _run_iter(pubsub, loop, q, sleep_time, stop_event):
        while not stop_event.is_set():
            message = pubsub.get_message(
                ignore_subscribe_messages=True, timeout=sleep_time)
            if message is not None and (not stop_event.is_set()):
                loop.call_soon_threadsafe(q.put_nowait, message)

    async def iter(self):
        if self.running.is_set():
            raise Exception("Already running pubsub iter.")
        self.running.set()

        finish = object()
        subscribe_queue = asyncio.Queue()
        future = concurrent.futures.Future()
        future.add_done_callback(lambda _: subscribe_queue.put_nowait(finish))
        args = (self.pubsub, asyncio.get_event_loop(), subscribe_queue, 1,
                self.stop_event)
        cmd = RedisCommand(self._run_iter, args, {}, future)
        self._cmd_queue.put(cmd)

        while True:
            data = await subscribe_queue.get()
            if data is finish:
                yield await asyncio.wrap_future(future)
            else:
                yield data

    async def stop(self):
        super().stop()

        # Use a dummy command to avoid thread hanging
        # (Thread created but iter() never called)
        def dummy_method():
            return 0

        cmd = RedisCommand(dummy_method, (), {}, concurrent.futures.Future())
        self._cmd_queue.put(cmd)
        self.pubsub.close()


class RedisClientThread(_RedisClientThreadBase):
    def __init__(self, host, port, password):
        super().__init__(name="RedisClientThread", daemon=True)
        self.loop = asyncio.get_event_loop()
        self.host = host
        self.port = port
        self.password = password
        self.redis_client = redis.Redis(
            self.host,
            self.port,
            password=self.password,
            health_check_interval=5)

    def pubsub(self, **kwargs):
        t = RedisClientPubsubThread(self, **kwargs)
        t.start()
        return t


def async_loop_forever(interval_seconds, cancellable=False):
    def _wrapper(coro):
        @functools.wraps(coro)
        async def _looper(*args, **kwargs):
            while True:
                try:
                    await coro(*args, **kwargs)
                except asyncio.CancelledError as ex:
                    if cancellable:
                        logger.info(f"An async loop forever coroutine "
                                    f"is cancelled {coro}.")
                        raise ex
                    else:
                        logger.exception(f"Can not cancel the async loop "
                                         f"forever coroutine {coro}.")
                except Exception:
                    logger.exception(f"Error looping coroutine {coro}.")
                await asyncio.sleep(interval_seconds)

        return _looper

    return _wrapper


class ClusterTokenManager:
    _cluster_unique_token = None

    @classmethod
    def cluster_unique_token(cls):
        return cls._cluster_unique_token

    @classmethod
    def check(cls, handler):
        @functools.wraps(handler)
        async def _check_token_handler(self, req) -> aiohttp.web.Response:
            token = req.query.get("clusterToken")
            if token is not None and token != cls._cluster_unique_token:
                return rest_response(
                    success=False,
                    message=f"Cluster token mismatch: "
                    f"{token} != {cls._cluster_unique_token}")
            return await handler(self, req)

        return _check_token_handler

    @classmethod
    async def create_or_get(cls, aioredis_client):
        if cls._cluster_unique_token:
            return cls._cluster_unique_token
        token = await aioredis_client.get(
            dashboard_consts.REDIS_KEY_CLUSTER_UNIQUE_TOKEN)
        if not token:
            await aioredis_client.setnx(
                dashboard_consts.REDIS_KEY_CLUSTER_UNIQUE_TOKEN,
                str(int(time.time())))
            token = await aioredis_client.get(
                dashboard_consts.REDIS_KEY_CLUSTER_UNIQUE_TOKEN)
        assert token, "Cluster unique token is empty."
        cls._cluster_unique_token = token.decode("utf-8")
        return cls._cluster_unique_token


class _AioGRPCChannelProxy:
    """
    A proxy class for asyncio grpc channel.
    """

    def __init__(self, aio_channel):
        assert not isinstance(aio_channel, _AioGRPCChannelProxy)
        self._aio_channel = aio_channel

    def __getattr__(self, item):
        return getattr(self._aio_channel, item)

    def reset(self, aio_channel):
        self._aio_channel = aio_channel


class _AioGRPCReconnectChannel(aiogrpc._channel.Channel):
    """
    A channel class with reconnect() for asyncio grpc.
    """
    _sig_aio_channel = inspect.signature(aiogrpc._channel.Channel)

    def __init__(self, target, options, credentials, compression,
                 interceptors):
        """Constructor.

        Args:
          target: The target to which to connect.
          options: Configuration options for the channel.
          credentials: A cygrpc.ChannelCredentials or None.
          compression: An optional value indicating the compression method to
            be used over the lifetime of the channel.
          interceptors: An optional list of interceptors that would be used for
            intercepting any RPC executed with that channel.
        """
        super().__init__(target, options, credentials, compression,
                         interceptors)
        init_locals = locals()
        reserved_keys = set(self._sig_aio_channel.parameters.keys())
        assert reserved_keys.issubset(init_locals.keys())
        self._init_kwargs = {k: init_locals[k] for k in reserved_keys}
        self._channel = _AioGRPCChannelProxy(self._channel)
        logger.info("Connect channel with: %s", self._init_kwargs)

    async def reconnect(self,
                        target,
                        options=None,
                        credentials=None,
                        compression=None,
                        interceptors=None):
        """Reconnect an asynchronous Channel to a server.

        Args:
          target: The server address
          options: An optional list of key-value pairs (channel args
            in gRPC Core runtime) to configure the channel.
          credentials: A ChannelCredentials instance.
          compression: An optional value indicating the compression method to
            be used over the lifetime of the channel. This is an EXPERIMENTAL
            option.
          interceptors: An optional sequence of interceptors that will be
            executed for any call executed with this channel.

        Returns:
          A Channel.
        """
        current_locals = locals()
        # Update the init kwargs by input kwargs.
        kwargs = {
            k: (current_locals[k] or self._init_kwargs[k])
            for k in self._init_kwargs
        }
        # Close old channel.
        await self.close()
        logger.info("Reconnect channel with: %s", kwargs)
        self._channel_storage = aiogrpc._channel.Channel(**kwargs)
        # Reset channel proxy.
        self._channel.reset(self._channel_storage._channel)
        # Update init kwargs.
        self._init_kwargs = kwargs


def insecure_channel(target, options=None, compression=None,
                     interceptors=None):
    """Creates an insecure asynchronous Channel to a server.

    Args:
      target: The server address
      options: An optional list of key-value pairs (channel args
        in gRPC Core runtime) to configure the channel.
      compression: An optional value indicating the compression method to be
        used over the lifetime of the channel. This is an EXPERIMENTAL option.
      interceptors: An optional sequence of interceptors that will be executed
        for any call executed with this channel.

    Returns:
      A Channel.
    """
    return _AioGRPCReconnectChannel(target, () if options is None else options,
                                    None, compression, interceptors)


def secure_channel(target,
                   credentials,
                   options=None,
                   compression=None,
                   interceptors=None):
    """Creates a secure asynchronous Channel to a server.

    Args:
      target: The server address.
      credentials: A ChannelCredentials instance.
      options: An optional list of key-value pairs (channel args
        in gRPC Core runtime) to configure the channel.
      compression: An optional value indicating the compression method to be
        used over the lifetime of the channel. This is an EXPERIMENTAL option.
      interceptors: An optional sequence of interceptors that will be executed
        for any call executed with this channel.

    Returns:
      An aio.Channel.
    """
    return _AioGRPCReconnectChannel(target, () if options is None else options,
                                    credentials._credentials, compression,
                                    interceptors)


class RestartableTask:
    def __init__(self, gen_coro, auto_restart=False):
        self._gen_coro = gen_coro
        self._auto_restart = auto_restart
        self._task = create_task(self._gen_coro())
        self._task.add_done_callback(self._callback)
        self._lock = asyncio.Lock()

    def __getattr__(self, item):
        return getattr(self._task, item)

    def __await__(self):
        return self._task.__await__()

    def __iter__(self):
        return self._task.__iter__()

    def __repr__(self):
        return f"<{type(self).__name__} {repr(self._task)}"

    def _callback(self, fut):
        if self._auto_restart and not fut.cancelled():
            logger.error(f"Unexpected task done. The task is {self._gen_coro}."
                         + " It will be restarted automatically.")
            # Do not restart the task when it's cancelled by us.
            create_task(self.restart())

    async def restart(self):
        await asyncio.sleep(
            dashboard_consts.RESTARTABLE_TASK_AUTO_RESTART_INTERVAL_SECONDS)
        async with self._lock:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = create_task(self._gen_coro())
            self._task.add_done_callback(self._callback)
        return self


class PSubscribeUpdater(abc.ABC):
    @classmethod
    @abc.abstractmethod
    async def _update_all(cls, gcs_channel):
        pass

    @classmethod
    @abc.abstractmethod
    async def _update_from_psubscribe_channel(cls, receiver):
        pass

    @classmethod
    def create(cls,
               psubscribe_key,
               aioredis_client,
               gcs_channel,
               auto_restart=True):
        async def _run():
            try:
                pubsub = None
                await cls._update_all(gcs_channel)
                # Only subscribe to redis channel after full update finished
                pubsub = aioredis_client.pubsub(ignore_subscribe_messages=True)
                pubsub.psubscribe(psubscribe_key)
                logger.info("Subscribed to %s", psubscribe_key)
                await cls._update_from_psubscribe_channel(pubsub)
            except asyncio.CancelledError:
                # Raise this error so the task can be cancelled.
                raise
            except Exception:
                logger.exception(
                    "Failed to run psubscribe updater for channel %s.",
                    psubscribe_key)
            finally:
                if pubsub:
                    # Release pubsub connection and thread
                    await pubsub.stop()
                    pubsub.join()

        return RestartableTask(_run, auto_restart)


# Ant Internal API
def grpc_force_ipv4():
    import ipaddress
    import ray.services

    force_ipv4 = os.environ.get("GRPC_FORCE_IPV4")
    if force_ipv4 is None:
        force_ipv4 = sys.platform == "linux"

    def _force_ipv4(bind_ip, host):
        logger.warning("_force_ipv4: %s replaced with %s", host, bind_ip)
        return ipaddress.IPv4Address(bind_ip)

    if force_ipv4:
        logger.warning("GRPC_FORCE_IPV4: true")
        ip = ray.services.get_node_ip_address()
        ipaddress.ip_address_original = ipaddress.ip_address
        ipaddress.ip_address = functools.partial(_force_ipv4, ip)
        return "{}:0".format(ip)
    else:
        ipaddress.ip_address_original = ipaddress.ip_address
        logger.warning("GRPC_FORCE_IPV4: false")
        return "[::]:0"


def logger_prefix_str(logger_prefix, template="[{}]", suffix=" "):
    if logger_prefix is None:
        return ""
    # (list, tuple) is simpler than collections.abc.Sequence.
    if isinstance(logger_prefix, (list, tuple)):
        return "".join(template.format(p) for p in logger_prefix) + suffix
    return template.format(logger_prefix) + suffix


def force_remove(filename, ignore_error=False):
    try:
        os.remove(filename)
    except Exception:
        if not ignore_error:
            raise


def force_hardlink(src, dst, logger_prefix=None):
    logger.info("%sLink %s to %s", logger_prefix_str(logger_prefix), src, dst)
    force_remove(dst, ignore_error=True)
    os.link(src, dst)


def file_readonly(file_path, ignore_error=False, logger_prefix=None):
    try:
        os.chmod(file_path, S_IREAD | S_IRGRP | S_IROTH)
        logger.info("%sSet file %s as read-only",
                    logger_prefix_str(logger_prefix), file_path)
    except Exception as error:
        if not ignore_error:
            logger.info("%sSet file %s as read-only and got error %s",
                        logger_prefix_str(logger_prefix), file_path, error)
            raise


def _generate_stats_configs():
    """Generates the configuration list for initializing stats."""
    metrics_config = {}
    enable_ceresdb_exporter = os.environ.get("ENABLE_CERESDB_EXPORTER", "")
    enable_kmonitor_exporter = os.environ.get("ENABLE_KMONITOR_EXPORTER", "")
    if enable_ceresdb_exporter == "true":
        metrics_config["enable_ceresdb_exporter"] = True
    if enable_kmonitor_exporter == "true":
        metrics_config["enable_kmonitor_exporter"] = True
    return metrics_config


def init_stats(global_tags, configs=None, app_name=""):
    """Initialize the stats for recording metrics."""
    if configs is None:
        configs = _generate_stats_configs()
    metrics.start(
        global_tags=global_tags,
        config_list=json.dumps(configs),
        app_name=app_name)
    logger.info("Finished initialize stats with global tags: %s, configs: %s",
                global_tags, configs)


# Initialize RSA key.
# https://www.dlitz.net/software/pycrypto/api/current/Crypto.Signature.PKCS1_v1_5-module.html
verifier = None
try:
    with open(dashboard_consts.DASHBOARD_PUBLIC_KEY, "rb") as f:
        key_text = f.read()
        public_key = RSA.importKey(key_text)
        verifier = PKCS1_v1_5.new(public_key)
except Exception:
    logger.exception("Initialize RSA key failed.")


class _SignatureVerifier:
    _verifier = None

    @classmethod
    def _get_verifier(cls):
        if cls._verifier is None:
            try:
                with open(dashboard_consts.DASHBOARD_PUBLIC_KEY, "rb") as f:
                    key_text = f.read()
                    public_key = RSA.importKey(key_text)
                    cls._verifier = PKCS1_v1_5.new(public_key)
            except Exception as ex:
                logger.exception("Initialize RSA key failed.")
                raise ex
        return cls._verifier

    @classmethod
    def verify(cls, body, signature):
        return cls._get_verifier().verify(SHA256.new(body), signature)


def check_body_signature(
        enable=env_bool(dashboard_consts.AIOHTTP_CHECK_SIGNATURE_KEY, False),
        verifier=_SignatureVerifier):
    """Check signature in http headers by RSA with SHA256.
       The signature is generated from http body which enocoded by 'utf-8'.
       And the signature bytes should be wrapped by base64 encoding string.
       If no body found, the signature check will be skipped.
    """

    def _wrapper(handler):
        if enable:

            @functools.wraps(handler)
            async def _signature_handler(*args) -> aiohttp.web.Response:
                # Make the route handler as a bound method.
                # The args may be:
                #   * (Request, )
                #   * (self, Request)
                req = args[-1]
                logger.info("Get request in signature headler: %s", req)
                if req.has_body:
                    try:
                        signature = req.headers.get("Body-Signature")
                        if not signature:
                            return rest_response(
                                success=False,
                                message="No signaure found in headers.")
                        signature_byte = base64.decodebytes(
                            codecs.encode(signature, encoding="utf-8"))
                        body = await req.text()
                        body_bytes = codecs.encode(body, encoding="utf-8")
                        if not verifier.verify(body_bytes, signature_byte):
                            return rest_response(
                                success=False,
                                message="Signature verification failed.")
                    except Exception:
                        logger.exception(
                            "Signaure verification raised exception.")
                        return rest_response(
                            success=False,
                            message="Signature verification raised exception.")

                return await handler(*args)

            suffix = "[signature_handler]"
            _signature_handler.__name__ += suffix
            _signature_handler.__qualname__ += suffix
            return _signature_handler
        else:
            return handler

    return _wrapper


class LazySemaphore:
    """ Initialize the semaphore in the first __aenter__, make sure the binding
    loop is the current running loop.
    """

    def __init__(self, value):
        self._semaphore = None
        self._value = value

    @property
    def value(self):
        return self._value

    async def __aenter__(self):
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._value)
        return await self._semaphore.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._semaphore.__aexit__(exc_type, exc_val, exc_tb)


class StrWithStdoutStderrMixin:
    @staticmethod
    def get_last_n_line(str_data, last_n_lines):
        if last_n_lines < 0:
            return str_data
        lines = str_data.split("\n")
        return "\n".join(lines[-last_n_lines:])

    def __str__(self):
        s = [super().__str__().strip(",.")]
        stdout = getattr(self, "stdout", None)
        last_n_lines_of_stdout = getattr(self, "last_n_lines_of_stdout", -1)
        if stdout:
            s.append("stdout={!r}".format(
                self.get_last_n_line(stdout, last_n_lines_of_stdout)))
        stderr = getattr(self, "stderr", None)
        last_n_lines_of_stderr = getattr(self, "last_n_lines_of_stderr", -1)
        if stderr:
            s.append("stderr={!r}".format(
                self.get_last_n_line(stderr, last_n_lines_of_stderr)))
        return ", ".join(s)


class SubprocessTimeoutExpired(StrWithStdoutStderrMixin,
                               subprocess.TimeoutExpired):
    pass


class SubprocessCalledProcessError(StrWithStdoutStderrMixin,
                                   subprocess.CalledProcessError):
    pass
