#!/bin/bash

#############################
# build deploy file and deploy cluster

function run_test() {
    use_raylet="false"
    if [ "$1" == "raylet" ]; then
        sed -i 's/^use_raylet.*$/use_raylet = true/g' ray.config.ini
    else
        sed -i 's/^use_raylet.*$/use_raylet = false/g' ray.config.ini
    fi

    sh cleanup.sh
    rm -rf local_deploy
    ./prepare.sh -t local_deploy
    pushd local_deploy
    local_ip=`ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
    echo "use local_ip" $local_ip

    OVERWRITE="ray.java.start.redis_port=34222;ray.java.start.node_ip_address=$local_ip;ray.java.start.deploy=true;ray.java.start.run_mode=CLUSTER;ray.java.start.raylet_port=35567;"

    echo OVERWRITE is $OVERWRITE
    ./run.sh start --head --overwrite=$OVERWRITE > cli.log 2>&1 &
    popd
    sleep 10

    # auto-pack zip for app example
    if [ ! -d "example/" ]; then
        mkdir example
    fi

    pushd example
    if [ ! -d "app1/" ]; then
        mkdir app1
    fi
    popd

    cp -rf tutorial/target/ray-tutorial-1.0.jar example/app1/
    pushd example
    zip -r app1.zip app1
    popd

    # run with cluster mode
    pushd local_deploy
    export RAY_CONFIG=ray/ray.config.ini
    ARGS=" --package ../example/app1.zip --class org.ray.exercise.Exercise02 --args=test1,test2  --redis-address=$local_ip:34222"
    ../local_deploy/run.sh submit $ARGS
    popd

    # clean up
    rm -rf example

    sleep 3
    # Remove raylet socket file.
    if [[ -a /tmp/raylet35567 ]]; then
        rm /tmp/raylet35567
    fi

    # Check the result
    start_process_log=$(cat "./local_deploy/cli.log")
    [[ ${start_process_log} =~ "Started Ray head node" ]] || exit 1
    echo "Check: Ray all process started."

    execution_log=$(cat "./local_deploy/ray/run/org.ray.exercise.Exercise02/0.out.txt")
    [[ ${execution_log} =~ "hello,world!" ]] || exit 1
    echo "Check: The test ran successfully."
}

run_test non-raylet
run_test raylet
