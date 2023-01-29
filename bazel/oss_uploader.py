"""
This script is used to upload extenal dependencies to internal OSS.
"""

import os
import subprocess
import sys

SUPPORTED_EXTENSIONS = [".zip", ".tar.gz", ".tar.bz2"]
OSSCMD_EXECUTABLE = "osscmd"

TEMP_DIR = "/tmp/ray_dep_downloads/"
os.makedirs(TEMP_DIR, exist_ok=True)

oss_id = sys.argv[1]
oss_key = sys.argv[2]


def upload(*args, **kwargs):
    name = kwargs["name"]
    sha256 = kwargs["sha256"]
    url = kwargs.get("url", None)
    if not url:
        urls = kwargs["urls"]
        assert len(urls) == 1
        url = urls[0]

    extension = None
    for ex in SUPPORTED_EXTENSIONS:
        if url.endswith(ex):
            extension = ex
            break
    assert extension is not None

    file_name = "%s-%s%s" % (name, sha256, extension)
    local_file = TEMP_DIR + file_name

    print("Downloading %s from %s." % (name, url))
    wget_cmd = [
        "wget",
        "-O",
        local_file,
        url,
    ]
    process = subprocess.Popen(wget_cmd)
    process.wait()
    print("%s downloaded to %s." % (name, local_file))

    oss_url = "oss://raylet/ci/" + file_name

    upload_cmd = [
        OSSCMD_EXECUTABLE,
        "put",
        local_file,
        oss_url,
        "config",
        "--id=" + oss_id,
        "--key=" + oss_key,
        "--host=cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com",
    ]

    print("Uploading %s to OSS: %s" % (name, " ".join(upload_cmd)))
    process = subprocess.Popen(upload_cmd)
    process.wait()
    print("%s uploaded." % name)

    os.remove(local_file)
    print("Local file %s removed." % local_file)


"""
To upload a dependency library, copy the `auto_http_archive` rule from
`ray_deps_setup.bzl` and change `auto_http_archive` to `upload`, then run this
script. For example:

upload(
    name = "com_github_antirez_redis",
    build_file = "//bazel:BUILD.redis",
    url = "https://github.com/antirez/redis/archive/5.0.3.tar.gz",
    sha256 = "7084e8bd9e5dedf2dbb2a1e1d862d0c46e66cc0872654bdc677f4470d28d84c5",
    patches = [
        "//thirdparty/patches:hiredis-connect-rename.patch",
        "//thirdparty/patches:hiredis-windows-sigpipe.patch",
        "//thirdparty/patches:hiredis-windows-sockets.patch",
        "//thirdparty/patches:hiredis-windows-strerror.patch",
    ],
)
"""  # noqa: E501
