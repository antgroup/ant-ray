import platform
import sys
import mock
import io

import numpy as np
import pytest
import ray
from ray.external_storage import (PanguDFSStorage, PanguErrorCode,
                                  parse_url_with_offset)

# -- File system param --
spill_local_path = "/tmp/spill"

# -- an invalid DFS cluster addr for tests --
dfs_cluster_addr = "dfs://dfs_cluster_addr.com:10290/"

# -- Pangu Storage param --
pangu_dfs_config = {
    "cluster_addr": dfs_cluster_addr,
    "directory_path": spill_local_path,
}

global_owner_address = b"global_owner_address"


class MockPanguFile:
    def __init__(self):
        print("__init__")
        self.f = io.BytesIO()

    def Append(self, bytes, options):
        print(f"Append, lenth: {len(bytes)}")
        self.f.write(bytes)
        return PanguErrorCode.PANGU_EOK, None

    def Close(self, options):
        print("Close")
        # Can't really close the stream because it will discard the
        # content. Since the stream may still have other fused objects
        # in it, close it in advance will lose them.
        return PanguErrorCode.PANGU_EOK

    def PReadV(self, offset, options, lengths: list):
        print(f"PReadV, offset: {offset}, lengths: {lengths}")
        self.f.seek(offset)
        result = []
        for lenth in lengths:
            result.append(self.f.read(lenth))
        return PanguErrorCode.PANGU_EOK, result


class MockPanguFileSystem:
    def __init__(self):
        self.paths = {"/": None}

    def Stat(self, path, options):
        if path not in self.paths:
            return PanguErrorCode.PANGU_ENOENT, None
        return PanguErrorCode.PANGU_EOK, None

    def CreateDirectory(self, path, options):
        print(f"CreateDirectory: {path}")
        if path in self.paths:
            return PanguErrorCode.PANGU_EEXIST
        self.paths[path] = None
        return PanguErrorCode.PANGU_EOK

    def CreateFile(self, path, options):
        print(f"CreateFile: {path}")
        if path in self.paths:
            return PanguErrorCode.PANGU_EEXIST
        self.paths[path] = MockPanguFile()
        return PanguErrorCode.PANGU_EOK

    def OpenFile(self, path, mode, options):
        if path not in self.paths:
            return PanguErrorCode.PANGU_ENOENT, None
        return PanguErrorCode.PANGU_EOK, self.paths[path]

    def Delete(self, path, options):
        if path not in self.paths:
            return PanguErrorCode.PANGU_ENOENT
        self.paths[path].Close()
        del self.paths[path]
        return PanguErrorCode.PANGU_EOK


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
@mock.patch("zdfs.PanguFileSystem.Create", return_value=MockPanguFileSystem())
def test_dfs_conn(shutdown_only):
    with pytest.raises(AssertionError):
        empty_addr_config = {
            "cluster_addr": None,
            "directory_path": spill_local_path,
        }
        _storage = PanguDFSStorage(**empty_addr_config)

    _storage = PanguDFSStorage(**pangu_dfs_config)

    assert _storage.try_connect_to_cluster(dfs_cluster_addr) is not None


@mock.patch("zdfs.PanguFileSystem.Create", return_value=MockPanguFileSystem())
def test_dfs_spill(shutdown_only):
    _storage = PanguDFSStorage(**pangu_dfs_config)

    ray.init()
    arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
    obj_ref = ray.put(arr)  # noqa

    spilled_urls = _storage.spill_objects([obj_ref], [global_owner_address])
    assert len(spilled_urls) == 1
    base_url, offset, size = parse_url_with_offset(spilled_urls[0].decode())
    assert offset == 0

    # spill the same obj
    duplicate_spilled_urls = _storage.spill_objects([obj_ref],
                                                    [global_owner_address])
    # spill will fail since two obj cannot write into one file
    assert len(duplicate_spilled_urls) == 0

    obj_ref_1 = ray.put(arr)
    obj_ref_2 = ray.put(arr)
    spilled_urls = _storage.spill_objects(
        [obj_ref_1, obj_ref_2], [global_owner_address, global_owner_address])
    assert len(spilled_urls) == 2
    base_url, offset, size = parse_url_with_offset(spilled_urls[0].decode())
    assert offset == 0
    base_url, offset, _ = parse_url_with_offset(spilled_urls[1].decode())
    assert offset == size
    ray.shutdown()


@mock.patch("zdfs.PanguFileSystem.Create", return_value=MockPanguFileSystem())
def test_dfs_restore(shutdown_only):
    _storage = PanguDFSStorage(**pangu_dfs_config)

    ray.init()
    arr1 = np.random.rand(1 * 1024 * 1024)  # 8 MB
    exist_obj_ref = ray.put(arr1)  # noqa

    arr2 = np.random.rand(2 * 1024 * 1024)  # 16 MB
    deleted_obj_ref = ray.put(arr2)

    spilled_urls = _storage.spill_objects(
        [exist_obj_ref, deleted_obj_ref],
        [global_owner_address, global_owner_address])

    assert len(spilled_urls) == 2
    _, offset, first_size = parse_url_with_offset(spilled_urls[0].decode())
    assert offset == 0

    _, offset, _ = parse_url_with_offset(spilled_urls[1].decode())
    assert offset == first_size

    ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
