#!/bin/bash

#########################################################################
# File Name: ray/run_aci_ut.sh
# Author: sean.xd
# mail: sean.xd@antfin.com
# Created Time: Tue 05 Mar 2019 02:22:00 PM CST
#########################################################################

set -e
grep -c process /proc/cpuinfo
grep MemTotal /proc/meminfo
cp -nrf /home/admin/.ssh/* /root/.ssh/

# !!!! delete existing maven repository
rm -rf /root/.m2/repository  /home/admin/.m2/repository/ /root/maven-repositories/

mv   /home/admin/.m2/   /root/


# clear tmp file before ut
rm -f /tmp/i_am_a_temp_socket

cd "$WORKSPACE"

# link bazel cache
if [ -f link_cache.sh ]; then
  sh -x link_cache.sh
fi

# Too many logs in bash_profile
set +x && source /home/admin/.bash_profile && set -x
# uniq sort PATH
PATH=$(echo "$PATH" | sed 's/:/\n/g' | sort | uniq | tr -s '\n' ':' | sed 's/:$//g')
export PATH
echo "after sort uniq, PATH: $PATH"
source /home/admin/py3/bin/activate
chown root -R "$WORKSPACE"

export SUPPRESS_OUTPUT_DIR="$WORKSPACE/ray/testresult"

cd "$WORKSPACE/ray/"
#export RAY_BACKEND_LOG_LEVEL=debug
export BOOST_ROOT=/home/admin/ray_depend/boost-install

set +e
sh -x buildtest.sh  "$@"
exit_code=$?
# Zip bazel test outputs.
zip -q -r "$WORKSPACE/ray/testresult/bazel_test_logs.zip" ./bazel-out/k8-*/testlogs
rm -f ray_log.zip
zip -q -r ray_log.zip /tmp/ray --include "*/logs/*"
set -e

if [[ $exit_code -ne 0 ]]; then
  exit $exit_code
fi

unset SUPPRESS_OUTPUT_DIR
