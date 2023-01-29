#!/bin/bash
# shellcheck disable=SC2086

set -e
##### 命令定义和include lib###################################################
CITOOLS="./citools"
rm -fr $CITOOLS && mkdir -p $CITOOLS
wget http://aivolvo-dev.cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com/citools/build_lib.sh -O $CITOOLS/build_lib.sh
source  $CITOOLS/build_lib.sh
# 一定要在source后重置DINGAPI变量
# shellcheck disable=SC2034
DINGAPI="https://oapi.dingtalk.com/robot/send?access_token=4889c4a88a3123cf5c2aa9e16dd7d7248cb7a56e323f90a109b089278e349723"
# 本地模式
[[ -z $JOB_NAME ]] && export IS_RUN_FAST_CASE="on"
script_dir=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
unamestr="$(uname)"
##### end #####################################################################

function showip()
{
  if [[ "$unamestr" == "Linux" ]]; then
    echo "==================== show ip addr"
    ip addr | grep inet
  fi
}


#build.sh相关参数初使化
function init()
{
    set -e
    build_arr_init "$@"
    date

    set +x
    if [[ "$unamestr" == "Linux" ]]; then
      mem=$(grep MemTotal /proc/meminfo | awk '{print $2}')
      if [ $mem -lt 16777216 ]; then
        echo "=================== mem is lower than 16G, exit"
        exit 1
      fi
      echo "==================== show ip addr"
      ip addr | grep inet
      echo "==================== show disk"
      df -h
      echo "==================== show cpuinfo"
      head -n 40 /proc/cpuinfo
      echo "==================== show cpu count"
      nproc
      grep -c processor /proc/cpuinfo

      # set current cpu info, this will be used in bazel build
      # see .bazelrc file in root dir
      CPU_INFO=$(grep flags /proc/cpuinfo | head -n 1)
      export CPU_INFO
      echo "$CPU_INFO"
    fi
    set -x


    # Download jacoco jar and modify authority
    curl http://raylet.cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com/ci/jacoco/org.jacoco.cli-0.8.0-nodeps.jar -o $WORKSPACE/ray/java/jacococli.jar
    curl http://raylet.cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com/ci/jacoco/org.jacoco.agent-0.8.0-runtime.jar -o $WORKSPACE/ray/java/jacocoagent.jar
    chmod 777 $WORKSPACE/ray/java/jacoco*.jar

    # Setting the environment variables of the jacoagent JVM parameters
    export RAY_TEST_JVM_CLUSTER_ARGS="-javaagent:$WORKSPACE/ray/java/jacocoagent.jar=dumponexit=true,destfile=/tmp/jacoco_cluster.exec,includes=*"

    # start mem monitor
    URL1="http://aivolvo-dev.cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com/citools/proc_top_monitor.py"
    curl -sL $URL1 -o $WORKSPACE/ray/proc_top_monitor.py
    nohup python $WORKSPACE/ray/proc_top_monitor.py 5 &

    set +e
    # install dependencies
    echo "$2"
    $script_dir/../ci/suppress_output sh -x $script_dir/../ci/prepare_env.sh
    prepare_code=$?
    if [[ $prepare_code != 0 ]];then
        echo "put lsof_end.log to oss--------------------"
        #文件系统最大可打开文件数
        cat /proc/sys/fs/file-max
        #程序限制打开文件数
        ulimit -n
        #查看打开句柄总数
        lsof|awk '{print $2}'|wc -l
        lsof  > lsof_end.log
        osscmd put lsof_end.log oss://rayoltest/volvo_ci/streaming/lsof_end.log config --id=LTAI4GKK6Chbk8gNULPpYV4j --key=$2 --host=oss-cn-hangzhou-zmf.aliyuncs.com
        #查看linux单进程最大文件连接数
        tail -2 /etc/security/limits.conf
        exit $prepare_code
     fi

     set -e

    # 卸载旧的版本
    pip uninstall cython-examples -y
    rm -fr /home/admin/py3/lib/python3.6/site-packages/cython-examples.egg-link

    # clean up for conflicts first
    sudo rm -fr /dev/shm/plasma*
    sudo find /tmp/|grep -v "^\/tmp\/$"| xargs sudo rm -rf
}

function python_coverage_summary()
{
    # Coverage summary should not block the ci.
    set +e
    if [ ! -d $script_dir/testresult/pythoncov ]; then
        mkdir -p $script_dir/testresult/pythoncov
    fi
    rm -rf $script_dir/testresult/pythoncov/*
    # generate combined pytest coverage report
    find $script_dir/../ -name "\.coverage*" | grep -v coveragerc | xargs -I {} cp {} $script_dir/testresult/pythoncov/
    cd $script_dir/testresult/pythoncov/
    coverage combine
    coverage xml -o $script_dir/testresult/pythoncov/cobertura.xml

    ls -al $script_dir/testresult/pythoncov/
    set -e
}

function coverage_bazel_summary()
{
    # Coverage summary should not block the ci.
    set +ex
    testlogsPath=$(bazel info bazel-testlogs)
    coverages=$(find ${testlogsPath} -follow -name "coverage.dat")
    if [ ! "$coverages" ];then
        echo "no coverage file"
        # TODO: enable coverage results
        exit 0
    fi
    lcov_command="lcov"
    for coverage in $coverages; do
      if [ -s ${coverage} ];then
        lcov_command="$lcov_command -a $coverage"
      else
        echo $coverage " is empty file !!!"
      fi
    done
    $lcov_command -o coverage.info
    wget http://aivolvo-dev.cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com/citools/lcov_cobertura.py
    python lcov_cobertura.py coverage.info --output $script_dir/testresult/coverage.xml --demangle
    set -ex
}


# Retry in case of OOM
function bazel_build_with_retry()
{
  # build ray with retry
  i=0
  job_num=6
  set +e
  while [ "$i" -lt "3" ]; do
    i=$((i+1))
    bazel build $1 --jobs=${job_num} -k
    job_num=$((job_num-2))
  done
  set -e
  # Execute again to check return value.
  bazel build $1 --jobs=1 -k
}

# Retry in case of OOM
function bazel_coverage_with_retry()
{
  # build ray with retry
  i=0
  job_num=6
  set +e
  while [ "$i" -lt "3" ]; do
    i=$((i+1))
    $script_dir/../ci/suppress_output bazel coverage $1 --jobs=${job_num} -k
    job_num=$((job_num-2))
  done
  set -e
  # Execute again to check return value.
  $script_dir/../ci/suppress_output bazel coverage $1 --jobs=1 -k
}

function compile()
{
    set -e
    pushd "${script_dir}/python"
      wget -q -t 3 https://gw.alipayobjects.com/os/bmw-prod/6bd2753a-00bc-40ec-81c6-be01dead61dd.gz -O ray/new_dashboard/client/ray-dashboard-fe-1.8.1.tar.gz
      tar -xvf ray/new_dashboard/client/ray-dashboard-fe-1.8.1.tar.gz -C  ray/new_dashboard/client/
    popd
    pushd $script_dir
      bazel_build_with_retry "//:ray_pkg"
      bazel_build_with_retry "//java:ray_java_pkg"
      # for ray cpp pkg
      bazel_build_with_retry "//:ray_api_user_lib"
      bazel_build_with_retry "//cpp:ray_cpp_pkg"
      export SKIP_BAZEL_BUILD=1
      $script_dir/../ci/suppress_output pip install -e python --verbose
      pushd streaming
        # build streaming
        $script_dir/../ci/suppress_output pip install -e python --verbose
      popd
    popd
}


# run a bunch of ut cases
# param 1 could be like examples below：
# raylet, java, python_core, python_non_core, streaming
# or combination of several param, splited by white space or comma, such as:
# raylet  java  python_core
# empty param means run all cases
function ut_all()
{
    run_case  "$@"
}



function print_stage()
{
  echo ">>>>> $1 >>>>>"
}

function collect_test_results()
{
  mkdir -p $WORKSPACE/testresult/
  set +x
  find ./bazel-out/k8-*/testlogs/ -follow -name "*.xml" | grep -v "test_attempts" | awk -F/ '{ print $0" "$(NF-2)"-"$(NF-1) }' |\
  while read -r fn tn; do
    echo ${fn}
    cp ${fn} $WORKSPACE/testresult/TEST-${tn}.xml
  done
  set -x
}

function run_ray_python_core_test()
{
  test_type=$1
  test_tag_filters=$2
  print_stage "Running $test_type tests."
  export PYTHONPATH="$PYTHONPATH:$script_dir/test:$script_dir/doc/examples/cython"
  # Install cython-examples.
  pushd $script_dir/doc/examples/cython
    $script_dir/../ci/suppress_output pip install -e .
  popd

  coveragepy="/tmp/coveragepy"
  wget http://raylet.cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com/ci/coveragepy.tar.gz -O coveragepy.tar.gz
  tar -xzf coveragepy.tar.gz
  mv coveragepy /tmp

  if [[ "$test_type" == "dashboard" ]]; then
    # Install dependencies for unit test `RAY_DASHBOARD`
    pip install -r python/ray/new_dashboard/requirements-dev.txt
  else
    pushd $script_dir/python
    $script_dir/../ci/suppress_output pip install -r requirements.txt
    popd
  fi

  if [[ -n "$RAY_CI_PARTIAL_TEST" ]]; then
    part_index=${RAY_CI_PARTIAL_TEST%/*}
    part_count=${RAY_CI_PARTIAL_TEST##*/}
    if [[ $part_index -lt 1 || $part_count -lt 1 || $part_index -gt $part_count ]]; then
      echo Invalid RAY_CI_PARTIAL_TEST value: $RAY_CI_PARTIAL_TEST
      exit 1
    fi
    part_index=$((part_index - 1))
    all_tests=( $(bazel query $3) )
    filtered_tests=()
    for test in "${all_tests[@]}"; do
      hash=$((0x$(sha1sum <<<"$test" | cut -c1-8) % part_count))
      if [[ $part_index -eq $hash ]]; then
        filtered_tests+=("$test")
      fi
    done
    test_arg=${filtered_tests[*]}

    echo Run partial tests: $RAY_CI_PARTIAL_TEST
    echo Filtered tests to run: $test_arg
  else
    test_arg=$3
  fi

  set +e
  $script_dir/../ci/suppress_output bazel coverage --test_env=PYTHON_COVERAGE=${coveragepy}/coverage/__main__.py --build_tests_only --test_tag_filters=$test_tag_filters --spawn_strategy=local --flaky_test_attempts=3 --nocache_test_results --test_verbose_timeout_warnings --progress_report_interval=10 --show_timestamps --test_output=streamed $test_arg
  testcode=$?
  collect_test_results
  coverage_bazel_summary
  rm -rf ${coveragepy}
  print_stage "Finished $test_type tests."
  exit $testcode
}

function pytest_ignore_files()
{
  set +x
  local ignore=""
  for item in "$@"; do
    ignore="$ignore --ignore=$item"
  done
  set -x
  echo "$ignore"
}

function collect_bazel_test_result_xml()
{
  find ./bazel-testlogs/ -name "test.xml" | awk -F/ '{ print $0" "$(NF-1)" "$(NF-2)"_"$(NF-1)"_"NR }' |\
  while read -r fn tn un; do
    if [[ -f "${WORKSPACE}/ray/testresult/gtest/TEST-${tn}.xml" ]]; then
      cp ${fn} ${WORKSPACE}/ray/testresult/gtest/TEST-${un}.xml
    else
      cp ${fn} ${WORKSPACE}/ray/testresult/gtest/TEST-${tn}.xml
    fi
  done
}

function collect_coverage_binary() {
    # collect .gcno and .gcda to coverage.dat
    set +x
    objsPath=$(bazel info bazel-bin)/_objs/
    gcnoFiles=$(find $objsPath -name '*.gcno')
    # shellcheck disable=SC2044
    for file in $(find $objsPath -name '*.gcda')
    do
      fileStr=$(echo $file | awk -F ".gcda" '{print $1}')
      if echo "${gcnoFiles[@]}" | grep -q -w "${fileStr}.gcno"; then
        rm -f $file
      fi
    done
    set -x
    testlogsPath=$(bazel info bazel-testlogs)
    mkdir $testlogsPath/binary_test/
    $script_dir/../ci/suppress_output lcov --rc lcov_branch_coverage=1 -c -d $objsPath -o $testlogsPath/binary_test/coverage.dat
}

function run_case()
{
    local test_categories="$1"
    if [[ "$test_categories" == "" ]]; then
      test_categories="raylet java python_core streaming"
    fi

    cd $script_dir

    if [[ "$test_categories" == *raylet* ]]; then
      mkdir -p ${WORKSPACE}/ray/testresult/gtest/
      export GTEST_OUTPUT="xml:${WORKSPACE}/ray/testresult/gtest/"
      print_stage "Running raylet tests."
      if [[ -z $IS_RUN_FAST_CASE || $IS_RUN_FAST_CASE != "on" ]]; then
          # Temporarily add build options for coverage.
          # !!! bad for remote-cache. If .bazelrc is modified, no cache will be used
          echo "build --copt=-coverage --linkopt=-lgcov --spawn_strategy=standalone" >> .bazelrc
      fi
      set +e
      test_ray_ret=0
      if [[ -z $IS_RUN_FAST_CASE || $IS_RUN_FAST_CASE != "on" ]]; then
        # delete the build options
        sed '$d' .bazelrc
      fi
      pushd $script_dir
        bazel_coverage_with_retry "//:dummy_test_for_build_only"
      popd
      set -e
      if [[ -z $IS_RUN_FAST_CASE || $IS_RUN_FAST_CASE != "on" ]]; then
          $script_dir/../ci/suppress_output bazel coverage ${SANITIZER_PARAM} --test_lang_filters=cc -- //:all -//:core_worker_test || test_ray_ret=$?
      else
          # Execute unit tests without coverage.
          # Skip core_worker_test: https://github.com/ray-project/ray/pull/13514#issuecomment-764120791
          $script_dir/../ci/suppress_output bazel test --test_lang_filters=cc -- //:all -//:core_worker_test || test_ray_ret=$?
      fi

      # collect .gcno and .gcda to $testlogsPath/binary_test/coverage.dat
      set +e
      collect_coverage_binary
      set -e

      # generate xml from bazel coverage .dat files
      coverage_bazel_summary

      print_stage "Finished raylet tests."

      # rename xml, only files starts with "TEST" will be resolved by ACI platform
      pushd ${WORKSPACE}/ray/testresult/gtest/
        for file in *.xml; do
          [[ -e "$file" ]] || break
          echo $file
          mv $file  TEST-$file
        done
      popd

      collect_bazel_test_result_xml

      # return
      gen_result_md
      if [[ $test_ray_ret != 0 ]]; then
          return 255
      else
          return 0
      fi
    fi

    if [[ "$test_categories" == *java* ]]; then
      print_stage "Running Java tests."
      mkdir -p ${WORKSPACE}/ray/testresult/
      set +e
      $script_dir/../ci/suppress_output $script_dir/java/test.sh
      CODE=$?
      # Collect test results.
      for mode in "cluster_mode" "local_mode"; do
        mkdir -p ${WORKSPACE}/ray/testresult/$mode
        for file in /tmp/ray/java_test_reports/"$mode"/junitreports/*; do
          [[ -e "$file" ]] || break
          if grep SkipException "$file" >/dev/null; then
            # TestNG treats skipped tests as errors, skip these reports.
            :
          else
            cp "$file" ${WORKSPACE}/ray/testresult/$mode/"$(basename $file)"
          fi
        done
      done
      set -e
      print_stage "Finished Java tests."

      sh ${WORKSPACE}/ray/java/build-jar-multiplatform.sh build_jars_linux

      #generated java ut coverage report
      $script_dir/../ci/suppress_output bash ${WORKSPACE}/ray/java/java_coverage_report.sh ${WORKSPACE}/ray/testresult/javaCoverage

      if [[ $CODE != 0 ]]; then
        exit $CODE
      fi
    fi

    if [[ "$test_categories" == *SMALL_AND_LARGE_TESTS* ]]; then
      # split tags: ray/tests/BUILD
      # skip client_tests because it's slow
      run_ray_python_core_test "core Python" \
        -kubernetes,-jenkins_only,-client_tests,-medium_size_python_tests_a_to_j,-medium_size_python_tests_k_to_z \
        python/ray/tests/...
    fi

    if [[ "$test_categories" == *MEDIUM_TESTS_A_TO_J* ]]; then
      # split tags: ray/tests/BUILD
      run_ray_python_core_test "core Python" \
        -kubernetes,-jenkins_only,medium_size_python_tests_a_to_j \
        python/ray/tests/...
    fi

    if [[ "$test_categories" == *MEDIUM_TESTS_K_TO_Z* ]]; then
      # split tags: ray/tests/BUILD
      run_ray_python_core_test "core Python" \
        -kubernetes,-jenkins_only,medium_size_python_tests_k_to_z \
        python/ray/tests/...
    fi

    if [[ "$test_categories" == *RAY_DASHBOARD* ]]; then
      # split tags: ray/tests/BUILD
      run_ray_python_core_test dashboard \
        "" \
        python/ray/new_dashboard/...
    fi

    if [[ "$test_categories" == *streaming_py* ]]; then
      print_stage "Running streaming python tests."
      pip install pytest-cov pandas==0.24.0
      echo "Build streaming jar"
      set -e
      cd $script_dir/java
      echo "Install ray java"
      bazel build gen_maven_deps && mvn -B -T16 install -DskipTests -Dcheckstyle.skip
      pip install -i https://pypi.antfin-inc.com/simple pyfury==0.0.6.4
      pip install -i https://pypi.antfin-inc.com/simple/ zdfs-dfs-gcc492
      streaming_runtime_dir=$(realpath $script_dir/../streaming/runtime)
      pushd $streaming_runtime_dir/../
      sh build.sh compile
      cd runtime
      bazel_build_with_retry "gen_maven_deps"
      mvn -B -e -T16 clean package -Pstreaming_jar_shade_all -DskipTests  -Dcheckstyle.skip
      cd -
      streaming_jar="$streaming_runtime_dir/target/test-classes:$streaming_runtime_dir/target/classes:$streaming_runtime_dir/target/lib/*"
      echo "Streaming jar path $streaming_jar"
      export STREAMING_JAR=$streaming_jar
      # set ray streaming's log together with ray core's log
      export STREAMING_LOG_DIR=/tmp/ray/session_latest/logs
      export RAY_BACKEND_LOG_LEVEL=debug
      popd
      cd $script_dir
      run_pytest_streaming="pytest -v -s --cov-report=xml --cov=$script_dir --cov-config=streaming/.coveragerc  --durations=50  --junit-xml=${WORKSPACE}/ray/testresult/TEST-report.xml"
      set +e
      $script_dir/../ci/suppress_output $run_pytest_streaming streaming/python/raystreaming/tests
      exit_code=$?
      cov_file=$(find ./ -name coverage.xml)
      mv $cov_file $script_dir/testresult
      set -e
      # compatible with double-linked error
      if [[ $exit_code -ne 139 ]]&&[[ $exit_code -ne 134 ]]&&[[ $exit_code -ne 0 ]]; then
        exit $exit_code
      fi
      print_stage "Finished streaming python tests."
    fi

    if [[ "$test_categories" == *streaming_cpp* ]]; then
      print_stage "Running streaming c++ tests."
      export GTEST_OUTPUT="xml:$script_dir/testresult/gtest/"
      export STREAMING_METRICS_MODE=DEV
      bazel_coverage_with_retry "//:dummy_test_for_build_only"
      bazel_coverage_with_retry "//streaming/src:dummy_test_for_build_only"
      $script_dir/../ci/suppress_output bash -x $script_dir/streaming/src/test/run_tests.sh $IS_RUN_FAST_CASE
      EXIT_CODE=$?

      # collect .gcno and .gcda to coverage.dat
      set +ex
      testlogsPath=$(bazel info bazel-testlogs)
      coverages=$(find ${testlogsPath} -follow -name "coverage.dat")
      if [ ! $coverages ];then
          echo "no coverage file"
      else
        lcov_command="lcov"
        for coverage in $coverages; do
          if [ -s ${coverage} ];then
            lcov_command="$lcov_command -a $coverage"
          else
            echo $coverage " is empty file !!!"
          fi
        done
        $lcov_command -o coverage.info
        lcov --extract coverage.info '*/streaming/src/*' --rc lcov_branch_coverage=1 -o coverage_tmp.info
        lcov --remove coverage_tmp.info '*/streaming/src/test/*' '*/streaming/src/thirdparty/*' --rc lcov_branch_coverage=1 -o coverage.info
        curl http://aivolvo-dev.cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com/citools/lcov_cobertura.py -o lcov_cobertura.py
        python lcov_cobertura.py  coverage.info  --output $script_dir/testresult/coverage.xml --demangle
      fi

      find ${testlogsPath} -name "test.xml" | awk -F/ '{ print $0" "$(NF-2)"-"$(NF-1) }' |\
      while read -r fn tn; do
        cp ${fn} ${WORKSPACE}/ray/testresult/TEST-${tn}.xml
      done

      if [ $EXIT_CODE -ne 0 ]; then
        echo "run_tests.sh run fail !!!"
        echo "EXIT_CODE: $EXIT_CODE"
      fi
      set -ex
      exit $EXIT_CODE
      print_stage "Finished streaming c++ tests."
    fi

    if [[ "$test_categories" == *lint* ]]; then
      print_stage "Running Lint tests."
      FORMAT_SH_PRINT_DIFF=1 $script_dir/ci/travis/format.sh --all
      sh -x $script_dir/ci/travis/bazel-format.sh

      print_stage "Finished All Lint tests."
    fi

    if [[ "$test_categories" == *ray_perf_app* ]]; then
      print_stage "Running ray perf app package."
      pushd $script_dir
      bazel build //java:io_ray_ray_performance_test
      popd
      print_stage "Finished ray perf app package."
    fi

    if [[ "$test_categories" == *cpp_worker* ]]; then
      set -ex
      print_stage "Preparing cpp worker tests."
      cd java
      mvn package -DskipTests=true
      cp test/target/ray-test-1.0.0-SNAPSHOT.jar ../cpp/src/ray/test/cluster/
      cd ..
      print_stage "Running cpp worker tests."
      $script_dir/../ci/suppress_output bazel test --test_strategy=exclusive //cpp:all
      cd cpp/example
      $script_dir/../ci/suppress_output bash run.sh
      set +ex
      testlogsPath=$(bazel info bazel-testlogs)
      find ${testlogsPath} -name "test.xml" | awk -F/ '{ print $0" "$(NF-2)"-"$(NF-1) }' |\
      while read -r fn tn; do
        cp ${fn} ${WORKSPACE}/ray/testresult/TEST-${tn}.xml
      done
      print_stage "Finished All Lint tests."
    fi

    gen_result_md

    return 0
}


function gen_result_md()
{
    if [ ! -d $script_dir/testresult ]; then
        mkdir -p $script_dir/testresult
    fi
    # copy bazel profile
    find $script_dir/../ -name "bazel*profile.json" -print0 | xargs -0 -I {} cp {} $script_dir/testresult/
}


#退出信号捕捉处理
#TODO:这里要改下procname
#trap_exit "procname"



# private time calc function
function show_time_init()
{
    show_time_init_time1=$(date +%s)
    incr_base=$(date +%s)
}


# private time calc function
# param 1, description
function show_time_use()
{
    show_time_use_time2=$(date +%s)
    echo "desc: [$1], time elapsed:"  $((show_time_use_time2 - show_time_init_time1))  " sec,  incr time use: " $((show_time_use_time2 - incr_base))  " sec"
    incr_base=$(date +%s)
}

show_time_init


function usage(){
  echo "use like this:"
  echo 'sh buildtest.sh  --test_categories="raylet,java,python_core" --'
  echo 'sh buildtest.sh --"'
  echo '--test_categories: specify which type of tests you want to run'
  echo '                   you can specify one or several types'
  echo '                   splited by white space or comma'
}


TEST_CATEGORIES=""

optspec=":hv-:"
while getopts "$optspec" optchar; do
    case "${optchar}" in
        -)
            case "${OPTARG}" in
                test_categories)
                    val="${!OPTIND}"; OPTIND=$(( OPTIND + 1 ))
                    echo "Parsing option: '--${OPTARG}', value: '${val}'" >&2;
                    ;;
                test_categories=*)
                    val=${OPTARG#*=}
                    opt=${OPTARG%=$val}
                    echo "Parsing option: '--${opt}', value: '${val}'" >&2
                    TEST_CATEGORIES=${val}
                    ;;
                *)
                    if [ "$OPTERR" = 1 ] && [ "${optspec:0:1}" != ":" ]; then
                        echo "Unknown option --${OPTARG}" >&2
                    fi
                    ;;
            esac;;
        h)
            usage
            exit 2
            ;;
        *)
            if [ "$OPTERR" != 1 ] || [ "${optspec:0:1}" = ":" ]; then
                echo "Non-option argument: '-${OPTARG}'" >&2
            fi
            ;;
    esac
done

if [[ $TEST_CATEGORIES == "raylet" ]]; then
    shift $((OPTIND - 1))
    sanitizer=$1

    case ${sanitizer} in
        "asan")
            SANITIZER_PARAM="--config=asan";;
        "tsan")
            SANITIZER_PARAM="--config=tsan"
            cat /proc/sys/kernel/randomize_va_space
            ;;
        *)
            echo "no "${sanitizer}
    esac
fi

showip
init "$@"
if [[ "$TEST_CATEGORIES" != *lint* ]]; then
  compile
fi
ut_all $TEST_CATEGORIES
