#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
java -version

pushd "$ROOT_DIR"
  echo "Check java code format."
  # check google java style
  mvn -T16 spotless:check
  # check naming and others
  mvn -T16 checkstyle:check
popd

run_testng() {
    local exit_code
    if "$@"; then
        exit_code=0
    else
        exit_code=$?
    fi
    # exit_code == 2 means there are skipped tests.
    if [ $exit_code -ne 2 ] && [ $exit_code -ne 0 ] ; then
        if [ $exit_code -gt 128 ] ; then
            # Test crashed. Print the driver log for diagnosis.
            cat /tmp/ray/session_latest/logs/java-core-driver-*
        fi
        find . -name "hs_err_*log" -exec cat {} +
        exit $exit_code
    fi
}

pushd "$ROOT_DIR"/..
echo "Build java maven deps."
bazel build //java:gen_maven_deps

echo "Build test jar."
bazel build //java:all_tests_deploy.jar




RAY_BACKEND_LOG_LEVEL=debug ray start --head --port=6379 --redis-password=123456
sleep 10
RAY_BACKEND_LOG_LEVEL=debug java -cp bazel-bin/java/all_tests_deploy.jar -Dray.address="$ip:6379"\
 -Dray.redis.password='123456' -Dray.job.code-search-path="$PWD/bazel-bin/java/all_tests_deploy.jar" io.ray.test.MultiDriverTest
ray stop

popd
