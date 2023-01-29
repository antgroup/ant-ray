#!/usr/bin/env bash

set -x
set -e

if [[ $# -gt 0 ]]&&[[ $1 = "--config=asan" ]]; then
  ASAN=$1
fi

function build_bazel_module()
{
    bazel build "$1" -c opt --verbose_failures "${ASAN[@]}"
}

script_dir=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [[ "$(uname)" == "Linux" ]]; then
    . "${script_dir}/../ci/prepare_env.sh"
fi

pushd "${script_dir}/python"
    wget https://gw.alipayobjects.com/os/bmw-prod/6bd2753a-00bc-40ec-81c6-be01dead61dd.gz -O ray/new_dashboard/client/ray-dashboard-fe-1.8.1.tar.gz
    tar -xvf ray/new_dashboard/client/ray-dashboard-fe-1.8.1.tar.gz -C ray/new_dashboard/client/
    rm -rf build || echo "clean build directory"
    python setup.py sdist bdist_wheel -d dist
popd

pushd "${script_dir}/java"
    mvn -B -e -T 1C clean install -Dmaven.test.skip -Dcheckstyle.skip=true
popd

# tcmalloc
if [[ "$(uname)" == "Linux" ]]; then
    mkdir -p "${script_dir}/libtcmalloc"
    pushd "${script_dir}/libtcmalloc"
    curl http://arcos.oss-cn-hangzhou-zmf.aliyuncs.com/tcmalloc/libtcmalloc.zip -o libtcmalloc.zip
    unzip -o libtcmalloc.zip
    popd
fi

pushd "${script_dir}"
if [[ -z "${RAY_USE_CMAKE}" ]] ; then
    # prepare debug tool
    build_bazel_module //streaming/src:bundle_meta_info
    if [[ "$(uname)" == "Linux" ]]; then
        build_bazel_module //streaming/src:pangu
    fi
    build_bazel_module @com_github_brpc_brpc//:rpc_view
fi
popd
