#!/bin/bash
# shellcheck disable=SC2009

ps -ef | grep redis-server | awk '{print $2}' | xargs kill -9
ps -ef | grep plasma_store_server | awk '{print $2}' | xargs kill -9
ps -ef | grep gcs_server | awk '{print $2}' | xargs kill -9
ps -ef | grep raylet | awk '{print $2}' | xargs kill -9
rm -f /tmp/plasma_store_socket