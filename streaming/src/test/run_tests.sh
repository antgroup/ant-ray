#!/usr/bin/env bash

# This needs to be run in the build tree, which is normally ray/build

# Cause the script to exit if a single command fails.
# set -e
export STREAMING_METRICS_MODE=DEV

# Get the directory in which this script is executing.
SCRIPT_DIR="$(dirname "${BASH_SOURCE:-$0}")"
# switch to ray dir
cd "${SCRIPT_DIR}/../../.." || exit 1

IS_RUN_FAST_CASE=$1

if [[ -z $IS_RUN_FAST_CASE || $IS_RUN_FAST_CASE != "on" ]]; then
  bazel coverage //streaming/src:all --test_tag_filters=-cluster_mode_test
  # bazel coverage //streaming/src:streaming_queue_tests
  # bazel coverage //streaming/src:streaming_writer_tests_with_streamingqueue
else
  bazel test //streaming/src:all --test_tag_filters=-cluster_mode_test
  # bazel test //streaming/src:streaming_queue_tests
  # bazel test //streaming/src:streaming_writer_tests_with_streamingqueue
fi
