import os
from datetime import datetime
from setuptools import setup, find_packages

# Package and version.
BASE_VERSION = "0.1.3"
build_mode = os.getenv("RAGENT_BUILD_MODE", "")
package_name = os.getenv("RAGENT_PACKAGE_NAME", "ragent")

if build_mode == "nightly":
    VERSION = BASE_VERSION + datetime.today().strftime("b%Y%m%d.dev0")
    package_name = "ragent-nightly"
else:
    VERSION = BASE_VERSION + ".dev1"

requirements = [
    "langchain",
    "pydantic"
]

setup(
    name=package_name,
    version=VERSION,
    packages=find_packages(exclude=("examples", "tests", "tests.*")),
    install_requires=requirements,
    author="Ragent Team",
    author_email="chenqixiang.cqx@antgroup.com",
    description="A distributed agent framework based on Ray.",
    url="https://github.com/your_username/your_project",
    options={"bdist_wheel": {"plat_name": "any"}},
)
