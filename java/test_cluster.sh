#!/bin/bash

#############################
# build deploy file and deploy cluster

function run_test() {
    sh cleanup.sh
    rm -rf /tmp/ray_test

    ./prepare.sh -t /tmp/ray_test/local_deploy

    pushd /tmp/ray_test/local_deploy
    local_ips=`ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
    local_ip=$(echo $local_ips | awk -F " " '{print $NF}')
    echo "use local_ip" $local_ip

    # Rewrite the ray.config.ini
    if [ "$1" == "raylet" ]; then
        sed -i 's/^use_raylet.*$/use_raylet = true/g' ./ray/ray.config.ini
    else
        sed -i 's/^use_raylet.*$/use_raylet = false/g' ./ray/ray.config.ini
    fi

    OVERWRITE="ray.java.start.redis_port=34222;ray.java.start.node_ip_address=$local_ip;ray.java.start.deploy=true;ray.java.start.run_mode=CLUSTER;ray.java.start.raylet_port=35567;"

    echo OVERWRITE is $OVERWRITE
    ./run.sh start --head --overwrite=$OVERWRITE > cli.log 2>&1 &
    popd
    sleep 10

    pushd /tmp/ray_test
    # auto-pack zip for app example
    if [ ! -d "example/" ]; then
        mkdir example
    fi

    pushd example
    if [ ! -d "app1/" ]; then
        mkdir app1
    fi
    popd

    popd # popd from /tmp/ray_test

    cp -rf test/target/ray-test-1.0.jar /tmp/ray_test/example/app1/

    pushd /tmp/ray_test
    pushd example
    zip -r app1.zip app1
    popd

    # run with cluster mode
    pushd local_deploy
    export RAY_CONFIG=ray/ray.config.ini
    ARGS=" --package ../example/app1.zip --class org.ray.api.example.HelloExample --args=test1,test2  --redis-address=$local_ip:34222"
    ../local_deploy/run.sh submit $ARGS
    popd

    sleep 3
    # Remove raylet socket file.
    if [[ -a /tmp/raylet35567 ]]; then
        rm /tmp/raylet35567
    fi

    # Check the result
    start_process_log=$(cat "./local_deploy/cli.log")
    [[ ${start_process_log} =~ "Started Ray head node" ]] || exit 1
    echo "Check[$1]: Ray all processes started."

    execution_log=$(cat "./local_deploy/ray/run/org.ray.api.example.HelloExample/0.out.txt")
    [[ ${execution_log} =~ "hello,world!" ]] || exit 1
    echo "Check[$1]: The tests ran successfully."

    popd # popd from /tmp/ray_test

    sudo rm -rf /tmp/ray_test
}

run_test non-raylet
run_test raylet
