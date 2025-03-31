import importlib
import json
import os
import sys
from urllib.parse import urlparse

import requests


def load_flow_metadata(metadata_url: str):
    if not metadata_url:
        raise ValueError("Failed to load flow metadata for metadata url is None.")
    url = urlparse(metadata_url)
    if url.scheme in ["http", "https"]:
        response = requests.get(metadata_url)
        if response.status_code == 200:
            return response.json()
        else:
            raise ConnectionError(
                f"Failed to load metadata from {metadata_url} for {response.status_code}."
            )
    if url.scheme in ["file"]:
        with open(url.path, "r") as file:
            data = file.read()
        return json.loads(data)
    raise ValueError(
        f"Flow metadata url don't support this protocol, only support http/https/file, url:{metadata_url}."
    )


def get_env_info():
    env_info = {
        "python version": sys.version,
    }
    modules = ["ragent", "antflow"]
    for module in modules:
        try:
            version = importlib.metadata.version(module)
        except importlib.metadata.PackageNotFoundError:
            # 如果模块未安装，打印未找到消息
            version = "Not install"
        env_info[module] = version

    return env_info
