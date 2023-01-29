import os
import tempfile
import sys
import pytest
from zipfile import ZipInfo

from ray.new_dashboard.modules.job import job_utils


def touch_file(filename, txt):
    with open(filename, "w") as f:
        f.write(txt)


def test_hardlink_all_zip_files():
    with tempfile.TemporaryDirectory() as temp_dir:
        src_path = os.path.join(temp_dir, "src")
        os.makedirs(src_path)
        tar_path = os.path.join(temp_dir, "tar")
        os.makedirs(tar_path)
        """
        - src
            - a.txt
            - a_dir
                - a_dir_internal (symlink: link to parent dir: src)
                - a_internal.txt
        """
        touch_file(os.path.join(src_path, "a.txt"), "test a.txt")
        os.makedirs(os.path.join(src_path, "a_dir"))
        os.symlink(src_path, os.path.join(src_path, "a_dir", "a_dir_internal"))
        touch_file(
            os.path.join(src_path, "a_dir", "a_internal.txt"),
            "test a_dir_internal.txt")

        file_list = [
            ZipInfo("a.txt"),
            ZipInfo("a_dir/"),
            ZipInfo("a_dir/a_dir_internal"),
            ZipInfo("a_dir/a_internal.txt"),
        ]
        job_utils.hardlink_all_zip_files(src_path, tar_path, file_list)

        tar_a_txt = os.path.join(tar_path, "a.txt")
        tar_a_dir = os.path.join(tar_path, "a_dir")
        tar_a_dir_internal_a_txt = os.path.join(tar_a_dir, "a_internal.txt")
        tar_a_dir_internal_a_dir = os.path.join(tar_a_dir, "a_dir_internal")

        assert os.path.exists(tar_a_txt)
        # hard link
        assert os.stat(tar_a_txt).st_nlink == 2

        assert os.path.exists(tar_a_dir_internal_a_txt)
        # hard link
        assert os.stat(tar_a_dir_internal_a_txt).st_nlink == 2

        assert os.path.exists(tar_a_dir)

        assert os.path.exists(tar_a_dir_internal_a_dir)
        # soft link
        assert os.path.islink(tar_a_dir_internal_a_dir)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
