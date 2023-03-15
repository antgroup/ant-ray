#!/usr/bin/env bash

set -e

# export OCCLUM_LOG_LEVEL=trace

occlum run /bin/python3.8 /root/demo.py

# occlum run /bin/ray start --head