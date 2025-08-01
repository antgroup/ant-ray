import argparse
import errno
import io
import logging
import os
import pathlib
import re
import shlex
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import warnings
from datetime import datetime
from enum import Enum
from itertools import chain

# Workaround for setuptools_scm (used on macos) adding junk files
# https://stackoverflow.com/a/61274968/8162137
try:
    import setuptools_scm.integration

    setuptools_scm.integration.find_files = lambda _: []
except ImportError:
    pass

logger = logging.getLogger(__name__)

SUPPORTED_PYTHONS = [(3, 9), (3, 10), (3, 11), (3, 12), (3, 13)]
# When the bazel version is updated, make sure to update it
# in WORKSPACE file as well.

ROOT_DIR = os.path.dirname(__file__)
BUILD_JAVA = os.getenv("RAY_INSTALL_JAVA") == "1"
BUILD_CPP = os.getenv("RAY_INSTALL_CPP") == "1"
SKIP_BAZEL_BUILD = os.getenv("SKIP_BAZEL_BUILD") == "1"
BAZEL_ARGS = os.getenv("BAZEL_ARGS")
BAZEL_LIMIT_CPUS = os.getenv("BAZEL_LIMIT_CPUS")

THIRDPARTY_SUBDIR = os.path.join("ray", "thirdparty_files")
RUNTIME_ENV_AGENT_THIRDPARTY_SUBDIR = os.path.join(
    "ray", "_private", "runtime_env", "agent", "thirdparty_files"
)

CLEANABLE_SUBDIRS = [
    THIRDPARTY_SUBDIR,
    RUNTIME_ENV_AGENT_THIRDPARTY_SUBDIR,
]

# In automated builds, we do a few adjustments before building. For instance,
# the bazel environment is set up slightly differently, and symlinks are
# replaced with junctions in Windows. This variable is set in our conda-forge
# feedstock.
is_conda_forge_build = bool(int(os.environ.get("IS_AUTOMATED_BUILD", "0")))

exe_suffix = ".exe" if sys.platform == "win32" else ""

# .pyd is the extension Python requires on Windows for shared libraries.
# https://docs.python.org/3/faq/windows.html#is-a-pyd-file-the-same-as-a-dll
pyd_suffix = ".pyd" if sys.platform == "win32" else ".so"


def find_version(*filepath):
    # Extract version information from filepath
    with open(os.path.join(ROOT_DIR, *filepath)) as fp:
        version_match = re.search(r"^version = ['\"]([^'\"]*)['\"]", fp.read(), re.M)
        if version_match:
            return version_match.group(1)
        raise RuntimeError("Unable to find version string.")


class SetupType(Enum):
    RAY = 1
    RAY_CPP = 2


class BuildType(Enum):
    DEFAULT = 1
    DEBUG = 2
    ASAN = 3
    TSAN = 4
    NIGHTLY = 5


class SetupSpec:
    def __init__(
        self, type: SetupType, name: str, description: str, build_type: BuildType
    ):
        self.type: SetupType = type
        self.name: str = name
        version = find_version("ray", "_version.py")
        # add .dbg suffix if debug mode is on.
        if build_type == BuildType.DEBUG:
            self.version: str = f"{version}+dbg"
        elif build_type == BuildType.ASAN:
            self.version: str = f"{version}+asan"
        elif build_type == BuildType.TSAN:
            self.version: str = f"{version}+tsan"
        elif build_type == BuildType.NIGHTLY:
            version_postfix = datetime.today().strftime("%Y%m%d")
            version = re.sub(r"dev\d*", f"dev{version_postfix}", version)
            self.version: str = version
            self.name = f"{self.name}-nightly"
        else:
            self.version = version
        self.description: str = description
        self.build_type: BuildType = build_type
        self.files_to_include: list = []
        self.install_requires: list = []
        self.extras: dict = {}

    def get_packages(self):
        if self.type == SetupType.RAY:
            return setuptools.find_packages(exclude=("tests", "*.tests", "*.tests.*"))
        else:
            return []


build_type = os.getenv("RAY_DEBUG_BUILD")
if build_type == "debug":
    BUILD_TYPE = BuildType.DEBUG
elif build_type == "asan":
    BUILD_TYPE = BuildType.ASAN
elif build_type == "tsan":
    BUILD_TYPE = BuildType.TSAN
elif build_type == "nightly":
    BUILD_TYPE = BuildType.NIGHTLY
else:
    BUILD_TYPE = BuildType.DEFAULT

if os.getenv("RAY_INSTALL_CPP") == "1":
    # "ray-cpp" wheel package.
    setup_spec = SetupSpec(
        SetupType.RAY_CPP,
        "ant-ray-cpp",
        "A subpackage of Ray which provides the Ray C++ API.",
        BUILD_TYPE,
    )
else:
    # "ray" primary wheel package.
    setup_spec = SetupSpec(
        SetupType.RAY,
        "ant-ray",
        "Ray provides a simple, "
        "universal API for building distributed applications.",
        BUILD_TYPE,
    )
    RAY_EXTRA_CPP = True
    # Disable extra cpp for the development versions.
    if "dev" in setup_spec.version or os.getenv("RAY_DISABLE_EXTRA_CPP") == "1":
        RAY_EXTRA_CPP = False

# Ideally, we could include these files by putting them in a
# MANIFEST.in or using the package_data argument to setup, but the
# MANIFEST.in gets applied at the very beginning when setup.py runs
# before these files have been created, so we have to move the files
# manually.

# NOTE: The lists below must be kept in sync with ray/BUILD.bazel.
ray_files = [
    "ray/_raylet" + pyd_suffix,
    "ray/core/src/ray/gcs/gcs_server" + exe_suffix,
    "ray/core/src/ray/raylet/raylet" + exe_suffix,
]

if sys.platform == "linux":
    ray_files.append("ray/core/libjemalloc.so")

if BUILD_JAVA or os.path.exists(os.path.join(ROOT_DIR, "ray/jars/ray_dist.jar")):
    ray_files.append("ray/jars/ray_dist.jar")

if setup_spec.type == SetupType.RAY_CPP:
    setup_spec.files_to_include += ["ray/cpp/default_worker" + exe_suffix]
    # C++ API library and project template files.
    setup_spec.files_to_include += [
        os.path.join(dirpath, filename)
        for dirpath, dirnames, filenames in os.walk("ray/cpp")
        for filename in filenames
    ]

# These are the directories where automatically generated Python protobuf
# bindings are created.
generated_python_directories = [
    "ray/core/generated",
    "ray/serve/generated",
]

ray_files.append("ray/nightly-wheels.yaml")

# Autoscaler files.
ray_files += [
    "ray/autoscaler/aws/defaults.yaml",
    "ray/autoscaler/aws/cloudwatch/prometheus.yml",
    "ray/autoscaler/aws/cloudwatch/ray_prometheus_waiter.sh",
    "ray/autoscaler/azure/defaults.yaml",
    "ray/autoscaler/spark/defaults.yaml",
    "ray/autoscaler/_private/_azure/azure-vm-template.json",
    "ray/autoscaler/_private/_azure/azure-config-template.json",
    "ray/autoscaler/gcp/defaults.yaml",
    "ray/autoscaler/local/defaults.yaml",
    "ray/autoscaler/vsphere/defaults.yaml",
    "ray/autoscaler/ray-schema.json",
]

# Dashboard files.
ray_files += [
    os.path.join(dirpath, filename)
    for dirpath, dirnames, filenames in os.walk("ray/dashboard/client/build")
    for filename in filenames
]

# Dashboard metrics files.
ray_files += [
    os.path.join(dirpath, filename)
    for dirpath, dirnames, filenames in os.walk("ray/dashboard/modules/metrics/export")
    for filename in filenames
]
ray_files += [
    os.path.join(dirpath, filename)
    for dirpath, dirnames, filenames in os.walk(
        "ray/dashboard/modules/metrics/dashboards"
    )
    for filename in filenames
    if filename.endswith(".json")
]

# html templates for notebook integration
ray_files += [
    p.as_posix() for p in pathlib.Path("ray/widgets/templates/").glob("*.html.j2")
]

# If you're adding dependencies for ray extras, please
# also update the matching section of requirements/requirements.txt
# in this directory
if setup_spec.type == SetupType.RAY:
    pandas_dep = "pandas >= 1.3"
    numpy_dep = "numpy >= 1.20"
    pyarrow_deps = [
        "pyarrow >= 9.0.0",
        "pyarrow <18; sys_platform == 'darwin' and platform_machine == 'x86_64'",
    ]
    pydantic_dep = "pydantic!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.*,!=2.4.*,<3"
    setup_spec.extras = {
        "cgraph": [
            "cupy-cuda12x; sys_platform != 'darwin'",
        ],
        "client": [
            # The Ray client needs a specific range of gRPC to work:
            # Tracking issues: https://github.com/grpc/grpc/issues/33714
            "grpcio != 1.56.0; sys_platform == 'darwin'",
            "grpcio",
        ],
        "data": [
            numpy_dep,
            pandas_dep,
            *pyarrow_deps,
            "fsspec",
        ],
        "default": [
            # If adding dependencies necessary to launch the dashboard api server,
            # please add it to python/ray/dashboard/optional_deps.py as well.
            "aiohttp >= 3.7",
            "aiohttp_cors",
            "colorful",
            "py-spy >= 0.2.0; python_version < '3.12'",  # noqa:E501
            "py-spy >= 0.4.0; python_version >= '3.12'",  # noqa:E501
            "requests",
            "grpcio >= 1.32.0; python_version < '3.10'",  # noqa:E501
            "grpcio >= 1.42.0; python_version >= '3.10'",  # noqa:E501
            "opencensus",
            pydantic_dep,
            "prometheus_client >= 0.7.1",
            "smart_open",
            "virtualenv >=20.0.24, !=20.21.1",  # For pip runtime env.
            "flow-insight>=1.0.0",
        ],
        "observability": [
            "opentelemetry-api",
            "opentelemetry-sdk",
            "opentelemetry-exporter-otlp",
            "memray; sys_platform != 'win32'",
        ],
        "serve": [
            "uvicorn[standard]",
            "requests",
            "starlette",
            "fastapi",
            "watchfiles",
        ],
        "tune": [
            "pandas",
            "tensorboardX>=1.9",
            "requests",
            *pyarrow_deps,
            "fsspec",
        ],
    }

    # Both "adag" and "cgraph" are for Compiled Graphs.
    # "adag" is deprecated and will be removed in the future.
    setup_spec.extras["adag"] = list(setup_spec.extras["cgraph"])

    # Ray Serve depends on the Ray dashboard components.
    setup_spec.extras["serve"] = list(
        set(setup_spec.extras["serve"] + setup_spec.extras["default"])
    )

    # Ensure gRPC library exists for Ray Serve gRPC support.
    setup_spec.extras["serve-grpc"] = list(
        set(
            setup_spec.extras["serve"]
            + [
                "grpcio >= 1.32.0; python_version < '3.10'",  # noqa:E501
                "grpcio >= 1.42.0; python_version >= '3.10'",  # noqa:E501
                "pyOpenSSL",
            ]
        )
    )

    if RAY_EXTRA_CPP:
        setup_spec.extras["cpp"] = ["ray-cpp==" + setup_spec.version]

    setup_spec.extras["rllib"] = setup_spec.extras["tune"] + [
        "dm_tree",
        "gymnasium==1.0.0",
        "lz4",
        "ormsgpack==1.7.0",
        "pyyaml",
        "scipy",
    ]

    setup_spec.extras["train"] = setup_spec.extras["tune"] + [pydantic_dep]

    # Ray AI Runtime should encompass Data, Tune, and Serve.
    setup_spec.extras["air"] = list(
        set(
            setup_spec.extras["tune"]
            + setup_spec.extras["data"]
            + setup_spec.extras["train"]
            + setup_spec.extras["serve"]
        )
    )

    # NOTE: While we keep ray[all] for compatibility, you probably
    # shouldn't use it because it contains too many dependencies
    # and no deployment needs all of them. Instead you should list
    # the extras you actually need, see
    # https://docs.ray.io/en/latest/ray-overview/installation.html#from-wheels
    #
    # "all" will not include "cpp" anymore. It is a big depedendency
    # that most people do not need.
    #
    # Instead, when cpp is supported, we add a "all-cpp".
    setup_spec.extras["all"] = list(
        set(
            chain.from_iterable([v for k, v in setup_spec.extras.items() if k != "cpp"])
        )
    )
    if RAY_EXTRA_CPP:
        setup_spec.extras["all-cpp"] = list(
            set(setup_spec.extras["all"] + setup_spec.extras["cpp"])
        )

    # "llm" is not included in all, by design. vllm's dependency set is very
    # large and specific, will likely run into dependency conflicts with other
    # ML libraries. As a result, it is an "extra-extra" that is not part
    # ray[all].
    #
    # ray[llm] depends on ray[data].
    #
    # Keep this in sync with python/requirements/llm/llm-requirements.txt
    #
    setup_spec.extras["llm"] = list(
        set(
            [
                "vllm>=0.7.2",
                "jsonref>=1.1.0",
                "jsonschema",
                "ninja",
                # async-timeout is a backport of asyncio.timeout for python < 3.11
                "async-timeout; python_version < '3.11'",
                "typer",
            ]
            + setup_spec.extras["data"]
            + setup_spec.extras["serve"]
        )
    )

# These are the main dependencies for users of ray. This list
# should be carefully curated. If you change it, please reflect
# the change in the matching section of requirements/requirements.txt
#
# NOTE: if you add any unbounded dependency, please also update
# install-core-prerelease-dependencies.sh so we can test
# new releases candidates.
if setup_spec.type == SetupType.RAY:
    setup_spec.install_requires = [
        "click >= 7.0",
        "filelock",
        "jsonschema",
        "msgpack >= 1.0.0, < 2.0.0",
        "packaging",
        "protobuf >= 3.15.3, != 3.19.5",
        "pyyaml",
        "aiosignal",
        "frozenlist",
        "requests",
        "redis >= 3.5.0, <= 4.5.5",
    ]


def is_native_windows_or_msys():
    """Check to see if we are running on native Windows,
    but NOT WSL (which is seen as Linux)."""
    return sys.platform == "msys" or sys.platform == "win32"


def is_invalid_windows_platform():
    # 'GCC' check is how you detect MinGW:
    # https://github.com/msys2/MINGW-packages/blob/abd06ca92d876b9db05dd65f27d71c4ebe2673a9/mingw-w64-python2/0410-MINGW-build-extensions-with-GCC.patch#L53
    platform = sys.platform
    ver = sys.version
    return platform == "msys" or (platform == "win32" and ver and "GCC" in ver)


# Calls Bazel in PATH, falling back to the standard user installation path
# (~/bin/bazel) if it isn't found.
def bazel_invoke(invoker, cmdline, *args, **kwargs):
    home = os.path.expanduser("~")
    first_candidate = os.getenv("BAZEL_PATH", "bazel")
    candidates = [first_candidate]
    if sys.platform == "win32":
        mingw_dir = os.getenv("MINGW_DIR")
        if mingw_dir:
            candidates.append(mingw_dir + "/bin/bazel.exe")
    else:
        candidates.append(os.path.join(home, "bin", "bazel"))
    result = None
    for i, cmd in enumerate(candidates):
        try:
            result = invoker([cmd] + cmdline, *args, **kwargs)
            break
        except IOError:
            if i >= len(candidates) - 1:
                raise
    return result


def download(url):
    try:
        result = urllib.request.urlopen(url).read()
    except urllib.error.URLError:
        # This fallback is necessary on Python 3.5 on macOS due to TLS 1.2.
        curl_args = ["curl", "-s", "-L", "-f", "-o", "-", url]
        result = subprocess.check_output(curl_args)
    return result


def patch_isdir():
    """
    Python on Windows is having hard times at telling if a symlink is
    a directory - it can "guess" wrong at times, which bites when
    finding packages. Replace with a fixed version which unwraps links first.
    """
    orig_isdir = os.path.isdir

    def fixed_isdir(path):
        while os.path.islink(path):
            try:
                link = os.readlink(path)
            except OSError:
                break
            path = os.path.abspath(os.path.join(os.path.dirname(path), link))
        return orig_isdir(path)

    os.path.isdir = fixed_isdir


def replace_symlinks_with_junctions():
    """
    Per default Windows requires admin access to create symlinks, while
    junctions (which behave similarly) can be created by users.

    This function replaces symlinks (which might be broken when checked
    out without admin rights) with junctions so Ray can be built both
    with and without admin access.
    """
    assert is_native_windows_or_msys()

    # Update this list if new symlinks are introduced to the source tree
    _LINKS = {
        r"ray\rllib": "../../rllib",
    }
    root_dir = os.path.dirname(__file__)
    for link, default in _LINKS.items():
        path = os.path.join(root_dir, link)
        try:
            out = subprocess.check_output(
                "DIR /A:LD /B", shell=True, cwd=os.path.dirname(path)
            )
        except subprocess.CalledProcessError:
            out = b""
        if os.path.basename(path) in out.decode("utf8").splitlines():
            logger.info(f"'{link}' is already converted to junction point")
        else:
            logger.info(f"Converting '{link}' to junction point...")
            if os.path.isfile(path):
                with open(path) as inp:
                    target = inp.read()
                os.unlink(path)
            elif os.path.isdir(path):
                target = default
                try:
                    # unlink() works on links as well as on regular files,
                    # and links to directories are considered directories now
                    os.unlink(path)
                except OSError as err:
                    # On Windows attempt to unlink a regular directory results
                    # in a PermissionError with errno set to errno.EACCES.
                    if err.errno != errno.EACCES:
                        raise
                    # For regular directories deletion is done with rmdir call.
                    os.rmdir(path)
            else:
                raise ValueError(f"Unexpected type of entry: '{path}'")
            target = os.path.abspath(os.path.join(os.path.dirname(path), target))
            logger.info("Setting {} -> {}".format(link, target))
            subprocess.check_call(
                f'MKLINK /J "{os.path.basename(link)}" "{target}"',
                shell=True,
                cwd=os.path.dirname(path),
            )


if is_conda_forge_build and is_native_windows_or_msys():
    # Automated replacements should only happen in automatic build
    # contexts for now
    patch_isdir()
    replace_symlinks_with_junctions()


def build(build_python, build_java, build_cpp):
    if tuple(sys.version_info[:2]) not in SUPPORTED_PYTHONS:
        msg = (
            "Detected Python version {}, which is not supported. "
            "Only Python {} are supported."
        ).format(
            ".".join(map(str, sys.version_info[:2])),
            ", ".join(".".join(map(str, v)) for v in SUPPORTED_PYTHONS),
        )
        raise RuntimeError(msg)

    if is_invalid_windows_platform():
        msg = (
            "Please use official native CPython on Windows,"
            " not Cygwin/MSYS/MSYS2/MinGW/etc.\n"
            + "Detected: {}\n  at: {!r}".format(sys.version, sys.executable)
        )
        raise OSError(msg)

    bazel_env = dict(os.environ, PYTHON3_BIN_PATH=sys.executable)

    if is_native_windows_or_msys():
        SHELL = bazel_env.get("SHELL")
        if SHELL:
            bazel_env.setdefault("BAZEL_SH", os.path.normpath(SHELL))
        BAZEL_SH = bazel_env.get("BAZEL_SH", "")
        SYSTEMROOT = os.getenv("SystemRoot")
        wsl_bash = os.path.join(SYSTEMROOT, "System32", "bash.exe")
        if (not BAZEL_SH) and SYSTEMROOT and os.path.isfile(wsl_bash):
            msg = (
                "You appear to have Bash from WSL,"
                " which Bazel may invoke unexpectedly. "
                "To avoid potential problems,"
                " please explicitly set the {name!r}"
                " environment variable for Bazel."
            ).format(name="BAZEL_SH")
            raise RuntimeError(msg)

    # Note: We are passing in sys.executable so that we use the same
    # version of Python to build packages inside the build.sh script. Note
    # that certain flags will not be passed along such as --user or sudo.
    # TODO(rkn): Fix this.
    if not os.getenv("SKIP_THIRDPARTY_INSTALL_CONDA_FORGE"):
        pip_packages = ["psutil", "setproctitle==1.2.2", "colorama"]
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "-q",
                "--target=" + os.path.join(ROOT_DIR, THIRDPARTY_SUBDIR),
            ]
            + pip_packages,
            env=dict(os.environ, CC="gcc"),
        )

        # runtime env agent dependenceis
        runtime_env_agent_pip_packages = ["aiohttp"]
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "-q",
                "--target="
                + os.path.join(ROOT_DIR, RUNTIME_ENV_AGENT_THIRDPARTY_SUBDIR),
            ]
            + runtime_env_agent_pip_packages
        )

    bazel_flags = ["--verbose_failures"]
    if BAZEL_ARGS:
        bazel_flags.extend(shlex.split(BAZEL_ARGS))

    if BAZEL_LIMIT_CPUS:
        n = int(BAZEL_LIMIT_CPUS)  # the value must be an int
        bazel_flags.append(f"--local_cpu_resources={n}")
        warnings.warn(
            "Setting BAZEL_LIMIT_CPUS is deprecated and will be removed in a future"
            " version. Please use BAZEL_ARGS instead.",
            FutureWarning,
        )

    if is_conda_forge_build:
        src_dir = os.environ.get("SRC_DIR", False) or os.getcwd()
        src_dir = os.path.abspath(src_dir)
        if is_native_windows_or_msys():
            drive = os.path.splitdrive(src_dir)[0] + "\\"
            root_dir = os.path.join(drive, "bazel-root")
            out_dir = os.path.join(drive, "b-o")
            bazel_flags.append("--enable_runfiles=false")
        else:
            root_dir = os.path.join(src_dir, "..", "bazel-root")
            out_dir = os.path.join(src_dir, "..", "b-o")

        for d in (root_dir, out_dir):
            if not os.path.exists(d):
                os.makedirs(d)

        bazel_precmd_flags = [
            "--output_user_root=" + root_dir,
            "--output_base=" + out_dir,
        ]
    else:
        bazel_precmd_flags = []
        # Using --incompatible_strict_action_env so that the build is more
        # cache-able We cannot turn this on for Python tests yet, as Ray's
        # Python bazel tests are not hermetic.
        #
        # And we put it here so that does not change behavior of
        # conda-forge build.
        if sys.platform != "darwin":  # TODO(aslonnie): does not work on macOS..
            bazel_flags.append("--incompatible_strict_action_env")

    bazel_targets = []
    bazel_targets += ["//:ray_pkg"] if build_python else []
    bazel_targets += ["//cpp:ray_cpp_pkg"] if build_cpp else []
    bazel_targets += ["//java:ray_java_pkg"] if build_java else []

    if setup_spec.build_type == BuildType.DEBUG:
        bazel_flags.append("--config=debug")
    if setup_spec.build_type == BuildType.ASAN:
        bazel_flags.append("--config=asan-build")
    if setup_spec.build_type == BuildType.TSAN:
        bazel_flags.append("--config=tsan")

    return bazel_invoke(
        subprocess.check_call,
        bazel_precmd_flags + ["build"] + bazel_flags + ["--"] + bazel_targets,
        env=bazel_env,
    )


def _walk_thirdparty_dir(directory):
    file_list = []
    for root, dirs, filenames in os.walk(directory):
        # Exclude generated bytecode cache directories and tests directories
        # from vendored packages.
        for exclude_dir in ["__pycache__", "tests"]:
            if exclude_dir in dirs:
                dirs.remove(exclude_dir)
        for name in filenames:
            file_list.append(os.path.join(root, name))
    return file_list


def copy_file(target_dir, filename, rootdir):
    # TODO(rkn): This feels very brittle. It may not handle all cases. See
    # https://github.com/apache/arrow/blob/master/python/setup.py for an
    # example.
    # File names can be absolute paths, e.g. from _walk_thirdparty_dir().
    source = os.path.relpath(filename, rootdir)
    destination = os.path.join(target_dir, source)
    # Create the target directory if it doesn't already exist.
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    if not os.path.exists(destination):
        if sys.platform == "win32":
            # Does not preserve file mode (needed to avoid read-only bit)
            shutil.copyfile(source, destination, follow_symlinks=True)
        else:
            # Preserves file mode (needed to copy executable bit)
            shutil.copy(source, destination, follow_symlinks=True)
        return 1
    return 0


def pip_run(build_ext):
    if SKIP_BAZEL_BUILD:
        build(False, False, False)
    else:
        build(True, BUILD_JAVA, BUILD_CPP or BUILD_JAVA)

    if setup_spec.type == SetupType.RAY:
        setup_spec.files_to_include += ray_files

        thirdparty_dir = os.path.join(ROOT_DIR, THIRDPARTY_SUBDIR)
        setup_spec.files_to_include += _walk_thirdparty_dir(thirdparty_dir)

        runtime_env_agent_thirdparty_dir = os.path.join(
            ROOT_DIR, RUNTIME_ENV_AGENT_THIRDPARTY_SUBDIR
        )
        setup_spec.files_to_include += _walk_thirdparty_dir(
            runtime_env_agent_thirdparty_dir
        )

        # Copy over the autogenerated protobuf Python bindings.
        for directory in generated_python_directories:
            for filename in os.listdir(directory):
                if filename[-3:] == ".py":
                    setup_spec.files_to_include.append(
                        os.path.join(directory, filename)
                    )

    copied_files = 0
    for filename in setup_spec.files_to_include:
        copied_files += copy_file(build_ext.build_lib, filename, ROOT_DIR)
    print("# of files copied to {}: {}".format(build_ext.build_lib, copied_files))


def api_main(program, *args):
    parser = argparse.ArgumentParser()
    choices = ["build", "python_versions", "clean", "help"]
    parser.add_argument("command", type=str, choices=choices)
    parser.add_argument(
        "-l",
        "--language",
        default="python",
        type=str,
        help="A list of languages to build native libraries. "
        'Supported languages include "python", "cpp", and "java". '
        "If not specified, only the Python library will be built.",
    )
    parsed_args = parser.parse_args(args)

    result = None

    if parsed_args.command == "build":
        kwargs = dict(build_python=False, build_java=False, build_cpp=False)
        for lang in parsed_args.language.split(","):
            if "python" in lang:
                kwargs.update(build_python=True)
            elif "java" in lang:
                kwargs.update(build_java=True)
            elif "cpp" in lang:
                kwargs.update(build_cpp=True)
            else:
                raise ValueError("invalid language: {!r}".format(lang))
        result = build(**kwargs)
    elif parsed_args.command == "python_versions":
        for version in SUPPORTED_PYTHONS:
            # NOTE: On Windows this will print "\r\n" on the command line.
            # Strip it out by piping to tr -d "\r".
            print(".".join(map(str, version)))
    elif parsed_args.command == "clean":

        def onerror(function, path, excinfo):
            nonlocal result
            if excinfo[1].errno != errno.ENOENT:
                msg = excinfo[1].strerror
                logger.error("cannot remove {}: {}".format(path, msg))
                result = 1

        for subdir in CLEANABLE_SUBDIRS:
            shutil.rmtree(os.path.join(ROOT_DIR, subdir), onerror=onerror)
    elif parsed_args.command == "help":
        parser.print_help()
    else:
        raise ValueError("Invalid command: {!r}".format(parsed_args.command))

    return result


if __name__ == "__api__":
    api_main(*sys.argv)

if __name__ == "__main__":
    import setuptools
    import setuptools.command.build_ext

    class build_ext(setuptools.command.build_ext.build_ext):
        def run(self):
            return pip_run(self)

    class BinaryDistribution(setuptools.Distribution):
        def has_ext_modules(self):
            return True


# Ensure no remaining lib files.
build_dir = os.path.join(ROOT_DIR, "build")
if os.path.isdir(build_dir):
    shutil.rmtree(build_dir)

setuptools.setup(
    name=setup_spec.name,
    version=setup_spec.version,
    author="Ray Team",
    author_email="ray-dev@googlegroups.com",
    description=(setup_spec.description),
    long_description=io.open(
        os.path.join(ROOT_DIR, os.path.pardir, "README.rst"), "r", encoding="utf-8"
    ).read(),
    url="https://github.com/ray-project/ray",
    keywords=(
        "ray distributed parallel machine-learning hyperparameter-tuning"
        "reinforcement-learning deep-learning serving python"
    ),
    python_requires=">=3.9",
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    packages=setup_spec.get_packages(),
    cmdclass={"build_ext": build_ext},
    # The BinaryDistribution argument triggers build_ext.
    distclass=BinaryDistribution,
    install_requires=setup_spec.install_requires,
    setup_requires=["cython >= 3.0.12", "pip", "wheel"],
    extras_require=setup_spec.extras,
    entry_points={
        "console_scripts": [
            "ray=ray.scripts.scripts:main",
            "tune=ray.tune.cli.scripts:cli",
            "serve=ray.serve.scripts:cli",
        ]
    },
    package_data={
        "ray": [
            "includes/*.pxd",
            "*.pxd",
            "llm/_internal/serve/config_generator/base_configs/templates/*.yaml",
        ],
    },
    include_package_data=True,
    exclude_package_data={
        # Empty string means "any package".
        # Therefore, exclude BUILD from every package:
        "": ["BUILD"],
    },
    zip_safe=False,
    license="Apache 2.0",
) if __name__ == "__main__" else None
