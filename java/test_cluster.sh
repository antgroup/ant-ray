#!/bin/bash

#############################
# build deploy file and deploy cluster
sh cleanup.sh
rm -rf local_deploy
./prepare.sh -t local_deploy
pushd local_deploy
local_ip=`ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
echo "use local_ip" $local_ip
OVERWRITE="ray.java.start.redis_port=34222;ray.java.start.node_ip_address=$local_ip;ray.java.start.deploy=true;ray.java.start.run_mode=CLUSTER"
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
