import sys
import subprocess
import argparse
import os
import json
import base64
import logging

logging.basicConfig(
    stream=sys.stdout, format="%(asctime)s %(message)s", level=logging.DEBUG
)
logger = logging.getLogger(__name__)



def install_ray_package(extra_packages, ray_version):

    pip_install_command = [sys.executable, "-m", "pip", "install", "-U"]

    # if extra package is "core", we just install basic ray package.
    if extra_packages == "core":
        ray_package_name = f"ray=={ray_version}"
    else:
        ray_package_name = f"ray[{extra_packages}]=={ray_version}"

    pip_install_command.append(ray_package_name)

    logger.info("Staring install ray: {}".format(pip_install_command))

    # install whl
    result = subprocess.run(" ".join(pip_install_command), shell=True)

    if result.returncode != 0:
        raise RuntimeError(f"Failed to install ray whl, got ex: {result.stderr}")


def install_pip_package_with_specify_path(
    packages,
    exclude_python_path,
):
    pip_packages = json.loads(
        base64.b64decode(packages.encode("utf-8")).decode("utf-8")
    )
    install_pip_package(pip_packages, exclude_python_path)


def install_pip_package(pip_packages, exclude_python_path):
    formatted_pip_packages = []
    for package in pip_packages:
        package = package.strip("'")
        formatted_pip_packages.append(f"{package}")
    pip_install_command = [sys.executable, "-m", "pip", "install", "-U"]

    pip_install_command.extend(formatted_pip_packages)
    logger.info("Starting install pip package: {}".format(pip_install_command))
    env = None
    if exclude_python_path:
        logger.info("Installing pip packages without `PYTHONPATH`.")
        env = os.environ.copy()
        env["PYTHONPATH"] = ""
    result = subprocess.run(pip_install_command, env=env)

    if result.returncode != 0:
        raise Exception(f"Failed to install pip packages, got ex {result.stderr}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Install Ray whl package and pip packages."
    )
    parser.add_argument(
        "--extra-packages", required=False, help="Containing extra packages for Ray. Ex: data"
    )

    parser.add_argument(
        "--packages", required=False, help="Containing pip packages from runtime env."
    )

    parser.add_argument(
        "--ray-version", required=False, help="Install Which version for Ray."
    )

    parser.add_argument(
        "--without-python-path",
        required=False,
        help="Enable to install pip install without python path",
    )

    args = parser.parse_args()
    pip_install_without_python_path = False
    if args.without_python_path:
        pip_install_without_python_path = args.without_python_path.lower() == "true"
    if args.extra_packages:
        install_ray_package(args.extra_packages, args.ray_version)
    if args.packages:
        install_pip_package_with_specify_path(args.packages, pip_install_without_python_path)
