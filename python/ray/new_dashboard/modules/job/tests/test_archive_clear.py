import asyncio
import pytest
import tempfile
import os
from pathlib import Path
from unittest import mock
from uuid import uuid4
import shutil
import zipfile
import sys

from ray.new_dashboard.utils import create_task


def touch_zipfile(filename):
    with zipfile.ZipFile(filename, mode="w") as zip_f:
        with zip_f.open("test.txt", "w") as driver:
            driver.write("test".encode())


def unzip_file(zipfile_name, target_dir):
    shutil.unpack_archive(zipfile_name, target_dir, "zip")


@pytest.fixture
def init_archive_shared_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        shared_archive_dir = Path(temp_dir) / "shared_archives"
        os.makedirs(shared_archive_dir)
        yield Path(temp_dir), shared_archive_dir


@pytest.mark.asyncio
async def test_clear_signal_archive_file(init_archive_shared_dir):
    temp_dir, shared_archive_dir = init_archive_shared_dir
    with mock.patch(
            "ray.new_dashboard.modules.job.job_consts.ARCHIVE_MIN_RETRNTION_SECONDS",  # noqa: E501
            2
    ), mock.patch(
            "ray.new_dashboard.modules.job.job_consts.ARCHIVE_CLEAR_INTERVAL_SECONDS",  # noqa: E501
            1):

        from ray.new_dashboard.modules.job.job_agent import ArchiveCacheClear

        clear = ArchiveCacheClear(temp_dir)
        _ = create_task(clear.clean_archive_cache())

        zip_name = "test.zip"
        fake_hash = "fakehash"
        uuid_zip_file = shared_archive_dir / f"{zip_name}.{fake_hash}.{uuid4()}"  # noqa: E501
        zip_file = shared_archive_dir / f"{zip_name}.{fake_hash}"
        unzip_dir = shared_archive_dir / f"unpack_{zip_name}_{fake_hash}"  # noqa: E501

        touch_zipfile(uuid_zip_file)
        os.link(uuid_zip_file, zip_file)
        unzip_file(zip_file, unzip_dir)

        assert uuid_zip_file.exists()
        assert zip_file.exists()
        assert unzip_dir.exists()

        await asyncio.sleep(1)
        assert uuid_zip_file.exists()
        assert zip_file.exists()
        assert unzip_dir.exists()

        await asyncio.sleep(3)
        assert not uuid_zip_file.exists()
        assert not zip_file.exists()
        assert not unzip_dir.exists()

        touch_zipfile(uuid_zip_file)
        os.link(uuid_zip_file, zip_file)
        unzip_file(zip_file, unzip_dir)
        os.link(zip_file, temp_dir / "test.zip")

        await asyncio.sleep(4)
        assert not uuid_zip_file.exists()
        assert zip_file.exists()
        assert unzip_dir.exists()
        os.remove(temp_dir / "test.zip")

        await asyncio.sleep(2)
        assert not uuid_zip_file.exists()
        assert not zip_file.exists()
        assert not unzip_dir.exists()


@pytest.mark.asyncio
async def test_clear_mutiple_archive_files(init_archive_shared_dir):
    temp_dir, shared_archive_dir = init_archive_shared_dir
    with mock.patch(
            "ray.new_dashboard.modules.job.job_consts.ARCHIVE_MIN_RETRNTION_SECONDS",  # noqa: E501
            2
    ), mock.patch(
            "ray.new_dashboard.modules.job.job_consts.ARCHIVE_CLEAR_INTERVAL_SECONDS",  # noqa: E501
            1):

        from ray.new_dashboard.modules.job.job_agent import ArchiveCacheClear

        clear = ArchiveCacheClear(temp_dir)
        _ = create_task(clear.clean_archive_cache())

        ZIPFILE_NAME = 10
        zipfile_infos = []
        for index in range(ZIPFILE_NAME):
            zip_name = f"test_{index}.zip"
            fake_hash = f"fakehash_{index}"
            uuid_zip_file = shared_archive_dir / f"{zip_name}.{fake_hash}.{uuid4()}"  # noqa: E501
            zip_file = shared_archive_dir / f"{zip_name}.{fake_hash}"
            unzip_dir = shared_archive_dir / f"unpack_{zip_name}_{fake_hash}"  # noqa: E501

            touch_zipfile(uuid_zip_file)
            os.link(uuid_zip_file, zip_file)
            unzip_file(zip_file, unzip_dir)
            zipfile_infos.append((uuid_zip_file, zip_file, unzip_dir))

        for uuid_zip_file, zip_file, unzip_dir in zipfile_infos:
            assert uuid_zip_file.exists()
            assert zip_file.exists()
            assert unzip_dir.exists()

        await asyncio.sleep(1)
        for uuid_zip_file, zip_file, unzip_dir in zipfile_infos:
            assert uuid_zip_file.exists()
            assert zip_file.exists()
            assert unzip_dir.exists()

        await asyncio.sleep(3)
        for uuid_zip_file, zip_file, unzip_dir in zipfile_infos:
            assert not uuid_zip_file.exists()
            assert not zip_file.exists()
            assert not unzip_dir.exists()

        zipfile_infos = []
        for index in range(ZIPFILE_NAME):
            zip_name = f"test_{index}.zip"
            fake_hash = f"fakehash_{index}"
            uuid_zip_file = shared_archive_dir / f"{zip_name}.{fake_hash}.{uuid4()}"  # noqa: E501
            zip_file = shared_archive_dir / f"{zip_name}.{fake_hash}"
            unzip_dir = shared_archive_dir / f"unpack_{zip_name}_{fake_hash}"
            zip_ref = temp_dir / f"test_{index}.zip"

            touch_zipfile(uuid_zip_file)
            os.link(uuid_zip_file, zip_file)
            unzip_file(zip_file, unzip_dir)
            os.link(zip_file, zip_ref)
            zipfile_infos.append((uuid_zip_file, zip_file, unzip_dir, zip_ref))

        await asyncio.sleep(4)

        for uuid_zip_file, zip_file, unzip_dir, zip_ref in zipfile_infos:
            assert not uuid_zip_file.exists()
            assert zip_file.exists()
            assert unzip_dir.exists()
            os.remove(zip_ref)

        await asyncio.sleep(2)
        for uuid_zip_file, zip_file, unzip_dir, _ in zipfile_infos:
            assert not uuid_zip_file.exists()
            assert not zip_file.exists()
            assert not unzip_dir.exists()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
