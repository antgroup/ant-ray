import ray.worker
from typing import Union


def put(key: Union[str, bytes],
        value: Union[str, bytes],
        overwrite: bool = True,
        isGlobal: bool = False):
    _check_arg_len(key)
    _check_arg_len(value)
    return ray.worker.global_worker.core_worker.put_kv(key, value, overwrite,
                                                       isGlobal)


def get(key: Union[str, bytes], isGlobal: bool = False):
    _check_arg_len(key)
    return ray.worker.global_worker.core_worker.get_kv(key, isGlobal)


def exists(key: Union[str, bytes], isGlobal: bool = False):
    _check_arg_len(key)
    return ray.worker.global_worker.core_worker.exists_kv(key, isGlobal)


def delete(key: Union[str, bytes], isGlobal: bool = False):
    _check_arg_len(key)
    return ray.worker.global_worker.core_worker.delete_kv(key, isGlobal)


def _check_arg_len(arg: Union[str, bytes]):
    if len(arg) > 5 * 1024 * 1024:
        raise ValueError("key or value too long")
