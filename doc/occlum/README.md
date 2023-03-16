# Build Ray in Occlum
This folder contains the neccessary scripts and config files for building Ray into the Occlum image.

## 1. Building & Running Ray-In-Occlum Image
To build the image, simply run the following command in the :
```bash
cd /workdir/ray/doc/occlum # Same path of this turorial
docker build -t occlum_ray .
```

The Occlum runtime requires certain devices mounted, i.e. *enclave*  and *provision*; Therefore, set `--device /dev/sgx/enclave` and `--device /dev/sgx/provision` flags when starting the container.

Besides, the runtime memory usage of a certain program in Occlum will be obviously larger than in a normal OS, because of the missing piece of virtual memory; A small memory container will incur bizarr errors when running Ray programs.
Therefore, give a larger memory to the container like `--memory=28g` (by experience).

Putting it together:
```
docker run --rm  -it \
                    --device /dev/sgx/enclave \
                    --device /dev/sgx/provision \
                    --shm-size=2g \
                    --memory=28g \
                    occlum_ray:latest
```

## 2. Running a basic Ray program

*demo.py* is a basic Ray program, which is already putted into the Occlum instance under the `/root` path. To run:
```
occlum run /bin/python3.8 /root/demo.py
```

This demo shows:
1. Starting Ray cluster;
2. Putting/getting value to/from object store;
3. Submitting normal task;
4. Creating Actor;
5. Submitting Actor task with/without arguments

## 3. Running a torch-on-ray program
*pytorch_ray.py* shows a pytorch hand-writing recognition job running in Ray. To run:
```
occlum run /bin/python3.8 /root/pytorch_ray.py
```

## 4. Running with ray-cli
Ray-cli is putted under `/bin`. To run:
```
occlum run /bin/ray [options] [cmd]
```
For example, to start a Ray cluster:
```
occlum run /bin/ray start --head
```

## 5. Running a customzied Ray program
Running a Ray program inside the occlum just needs to put the relevant scripts and data into Occlum.
Take *pytorch_ray.py* as an example:

Copy the *pytorch_ray.py* file from docker into Occlum instance via a delaration in *python-ray.yaml*:

```yaml
targets:
  - target: /root # The destination path in Occlum
    copy:
     - files:
        - /tmp/pytorch_ray.py # The source file path in docker
```

Copy other relevant dependencies files as the same, in this case, "MINIT" hand-writing dataset.
```yaml
targets:
  - target: /root
    copy:
     - dirs:
        - /tmp/data # The source directory path in docker
```

Run `occlum build` and then `occlum run /bin/python3.8 /root/pytorch_ray.py` !

## 6. Test & Debug
For security reason, logs are written in Occlum OS by default, which is a black box.

For testing and debugging, you can explicitly set the log path under `/host` when starting the cluster. Occlum will redirect any files under `/host` to its host fs, i.e. docker container. For example, *demo.py* starts the cluster by `ray.init(_temp_dir="/host/tmp/ray")`, or by ray-cli `ray start --head --temp-dir /host/tmp/ray`.

