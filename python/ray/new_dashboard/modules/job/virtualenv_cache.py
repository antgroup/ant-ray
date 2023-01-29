import os
import pathlib
import asyncio
import aiohttp.web
import uuid
import pickle
import hashlib
import logging
import itertools
import collections
from typing import List, Dict

from ray.new_dashboard.modules.job import job_consts
from ray.new_dashboard.modules.job import job_utils

logger = logging.getLogger(__name__)

VenvInfo = collections.namedtuple("VenvInfo", ["jobs", "path"])


class VenvHash(collections.Hashable):
    __slots__ = ("_requirement_list", "_url_content_hash", "_vcs_salt",
                 "_pip_cmd_salt", "_hash")

    def __init__(self, requirement_list: List[str],
                 url_content_hash: Dict[str, str]):
        self._requirement_list = tuple(sorted(requirement_list))
        self._url_content_hash = url_content_hash
        # Found vcs in requirement list, treat it as unique.
        self._vcs_salt = uuid.uuid4() if any(
            job_utils.is_vcs(r) for r in self._requirement_list) else ""
        # Found pip cmd in requirement list, treat it as unique.
        self._pip_cmd_salt = uuid.uuid4() if any(
            job_utils.is_pip_cmd(r) for r in self._requirement_list) else ""
        self._hash = None

    @classmethod
    async def create(cls, job_id, requirement_list, http_session=None):
        async def _get_url_content_hash(_http_session):
            url_list = [r for r in requirement_list if job_utils.is_url(r)]
            content_hash_list = await asyncio.gather(*[
                job_utils.get_url_content_hash(_http_session, job_id, r)
                for r in url_list
            ])
            # Give a unique hash for the URL that the http server cannot
            # provide a hash.
            content_hash_list = [
                h or uuid.uuid4().hex for h in content_hash_list
            ]
            return dict(zip(url_list, content_hash_list))

        if http_session is None:
            async with aiohttp.ClientSession() as http_session:
                url_content_hash = await _get_url_content_hash(http_session)
        else:
            url_content_hash = await _get_url_content_hash(http_session)

        return cls(requirement_list, url_content_hash)

    def __eq__(self, other):
        if type(other) is type(self):
            return (self._vcs_salt == other._vcs_salt
                    and self._pip_cmd_salt == other._pip_cmd_salt
                    and self._requirement_list == other._requirement_list
                    and self._url_content_hash == other._url_content_hash)
        return False

    def __hash__(self):
        if self._hash is None:
            m = hashlib.md5()
            for i in itertools.chain(
                [self._vcs_salt, self._pip_cmd_salt], self._requirement_list,
                    *(self._url_content_hash[k]
                      for k in sorted(self._url_content_hash))):
                m.update(
                    i.bytes if type(i) is uuid.UUID else i.encode("utf-8"))
            self._hash = int.from_bytes(m.digest(), "little")
        return self._hash


class VenvCacheSerializer:
    @staticmethod
    def init(venv_path, venv_hash):
        pass

    @staticmethod
    def ref(venv_path, job_id):
        pass

    @staticmethod
    def deref(venv_path, job_id):
        pass

    @staticmethod
    def load(venv_path):
        pass


class VenvCacheFileSerializer(VenvCacheSerializer):
    @staticmethod
    def init(venv_path, venv_hash):
        assert os.path.exists(venv_path)
        hash_file = job_consts.PYTHON_VIRTUALENV_HASH.format(
            virtualenv_dir=venv_path)
        os.makedirs(os.path.dirname(hash_file), exist_ok=True)
        with open(hash_file, "wb") as f:
            pickle.dump(venv_hash, f)

    @staticmethod
    def ref(venv_path, job_id):
        assert os.path.exists(venv_path)
        venv_jobs_dir = job_consts.PYTHON_VIRTUALENV_JOBS_DIR.format(
            virtualenv_dir=venv_path)
        os.makedirs(venv_jobs_dir, exist_ok=True)
        venv_job = os.path.join(venv_jobs_dir, job_id)
        pathlib.Path(venv_job).touch()

    @staticmethod
    def deref(venv_path, job_id):
        assert os.path.exists(venv_path)
        venv_jobs_dir = job_consts.PYTHON_VIRTUALENV_JOBS_DIR.format(
            virtualenv_dir=venv_path)
        os.makedirs(venv_jobs_dir, exist_ok=True)
        venv_job = os.path.join(venv_jobs_dir, job_id)
        try:
            os.remove(venv_job)
        except Exception:
            pass

    @staticmethod
    def load(venv_path):
        assert os.path.exists(venv_path)
        hash_file = job_consts.PYTHON_VIRTUALENV_HASH.format(
            virtualenv_dir=venv_path)
        with open(hash_file, "rb") as f:
            venv_hash = pickle.load(f)
        venv_jobs_dir = job_consts.PYTHON_VIRTUALENV_JOBS_DIR.format(
            virtualenv_dir=venv_path)
        try:
            jobs = set(os.listdir(venv_jobs_dir))
        except FileNotFoundError:
            jobs = set()
        return venv_hash, jobs


class VenvCache:
    _serializer = VenvCacheFileSerializer
    _venv_map = {}
    _free_venv_count = job_consts.PYTHON_VIRTUALENV_CACHE_FREE_COUNT
    _free_venv_map = collections.OrderedDict()
    _job_id_to_venv_hash = {}

    @classmethod
    async def init(cls, cache_dir, co_delete_virtual_env):
        def _load_one(path):
            venv_hash, jobs = cls._serializer.load(path)
            assert isinstance(jobs, set)
            venv = VenvInfo(jobs, path)
            cls._venv_map[venv_hash] = venv
            if len(jobs) == 0:
                cls._free_venv_map[venv_hash] = venv
            else:
                for job_id in jobs:
                    cls._job_id_to_venv_hash[job_id] = venv_hash

        try:
            virtualenv_names = os.listdir(cache_dir)
        except FileNotFoundError:
            logger.info("Loaded 0 virtualenv.")
            return

        for name in virtualenv_names:
            virtualenv_path = os.path.join(cache_dir, name)
            try:
                _load_one(virtualenv_path)
            except Exception:
                logger.exception("Invalid virtualenv %s.", virtualenv_path)
                logger.info("Delete virtualenv: %s.", virtualenv_path)
                await co_delete_virtual_env(virtualenv_path)
        await cls._ensure_free_list(co_delete_virtual_env)
        logger.info("Loaded %s virtualenv.", len(cls._venv_map))

    @classmethod
    def clear(cls):
        cls._venv_map.clear()
        cls._free_venv_map.clear()
        cls._job_id_to_venv_hash.clear()

    @classmethod
    async def get_virtualenv(cls,
                             cache_dir,
                             job_id,
                             requirement_list,
                             co_new_virtual_env,
                             http_session=None):
        venv_hash = await VenvHash.create(job_id, requirement_list,
                                          http_session)
        cls._job_id_to_venv_hash[job_id] = venv_hash
        venv = cls._venv_map.get(venv_hash)
        if venv is not None:
            logger.info("[%s] Cache hit virtualenv %s.", job_id, venv.path)
            if len(venv.jobs) == 0:
                logger.info("[%s] Move virtualenv %s out of free list.",
                            job_id, venv.path)
                cls._free_venv_map.pop(venv_hash)
            venv.jobs.add(job_id)
            cls._serializer.ref(venv.path, job_id)
            return venv.path
        else:
            unique_virtualenv_path = os.path.join(cache_dir, str(uuid.uuid4()))
            venv = VenvInfo({job_id}, unique_virtualenv_path)
            logger.info("[%s] New virtualenv: %s.", job_id,
                        unique_virtualenv_path)
            await co_new_virtual_env(unique_virtualenv_path)
            cls._serializer.init(unique_virtualenv_path, venv_hash)
            cls._serializer.ref(unique_virtualenv_path, job_id)
            cls._venv_map[venv_hash] = venv
            return unique_virtualenv_path

    @classmethod
    async def deref_virtualenv(cls, job_id, co_delete_virtual_env):
        venv_hash = cls._job_id_to_venv_hash.pop(job_id, None)
        if venv_hash is None:
            logger.info(
                "[%s] Dereference virtualenv failed: "
                "No virtualenv key.", job_id)
        else:
            venv = cls._venv_map.get(venv_hash)
            if venv is None:
                logger.info(
                    "[%s] Dereference virtualenv failed: "
                    "Invalid virtualenv key %s.", job_id, venv_hash)
            else:
                venv.jobs.discard(job_id)
                cls._serializer.deref(venv.path, job_id)
                if len(venv.jobs) == 0:
                    cls._free_venv_map[venv_hash] = venv
                    logger.info("[%s] Move virtualenv %s to free list.",
                                job_id, venv.path)
                    await cls._ensure_free_list(co_delete_virtual_env)

    @classmethod
    async def _ensure_free_list(cls, co_delete_virtual_env):
        while len(cls._free_venv_map) > cls._free_venv_count:
            delete_venv_hash, delete_venv = \
                cls._free_venv_map.popitem(last=False)
            try:
                logger.info("Delete virtualenv: %s.", delete_venv.path)
                cls._venv_map.pop(delete_venv_hash)
                await co_delete_virtual_env(delete_venv.path)
            except Exception:
                logger.exception("Delete virtualenv failed: %s.",
                                 delete_venv.path)

    @classmethod
    def free_list(cls):
        return [venv.path for venv in cls._free_venv_map.values()]

    @classmethod
    def job_count(cls):
        return len(cls._job_id_to_venv_hash)

    @classmethod
    def size(cls):
        return len(cls._venv_map)
