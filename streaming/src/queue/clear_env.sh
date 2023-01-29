#!/bin/bash

# shellcheck disable=SC2009
ps -ax | grep -E "streaming_test_worker|DefaultDriver|DefaultWorker|AppStarter|redis|plasma" | grep -v grep | awk '{print $1}' | xargs kill -9 &> /dev/null
rm -rf /tmp/streaminglogs/*
rm -rf /tmp/ray/logs/*
rm -rf /tmp/ray_streaming_logs/
rm -rf /tmp/ray/sockets/*
rm -rf /cores/*
