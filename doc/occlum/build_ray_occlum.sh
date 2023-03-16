#!/usr/bin/env bash

set -x

docker build -t occlum_ray .

# '--rm': Delete the container when exit
# '-v `pwd`src_path:dest_path': redirect the src path in the container to dest path outside, i.e. the running host 
docker run --rm  -it --device /dev/sgx/enclave --device /dev/sgx/provision --shm-size=2g \
                    --memory=28g \
                    -v `pwd`:/root/codes/occlum_ray/occlum-ray-docker \
                    -v `pwd`/tmp/ray:/root/occlum_instance/tmp/ray \
                    occlum_ray:latest /root/codes/occlum_ray/occlum-ray-docker/run_occlum_job.sh \
                        > output.log 2>&1
