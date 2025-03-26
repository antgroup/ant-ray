.. _fault-tolerance-nodes:

Head High-Availability Feature
====================

Head High-Availability Feature (Head HA), which reduces the impact of head-failover in ray clusters by deploying multi heads.


Why we need it
----------------

In Ray Clusters, the head node is the most important node. It is responsible for managing all the other nodes and tasks. So it is neccessary to make the head node highly available.

Otherwise, detecting a head node failure and starting a new one usually involves extra time and additional steps. 

Head HA is a simple and effective way to make the head node highly available.


How it works
----------------
Head High-Availability Feature is implemented by the following steps:

1. Start two or more head nodes at the same time.
2. The startup process is before initializing the node and starting the head node process. It connects to redis and compete for the leadership through redis's distributed lock.
3. Only the node that successfully competes for the leadership will execute the subsequent gcs_server/dashboard process startup normally.
4. The standby node will be stuck in the competition process until the original leader node fails.
5. After normal startup, the startup process of the leader node will periodically renew the distributed lock of redis to maintain the leader status. Then the startup process will run as a daemon process to check the leadership of this head node.
6. If the entire pod of the leader node fails or the lease renewal fails, it considers itself as a standby node and kills all processes and itself and then exit the startup process. Exit of the startup process will cause the pod to restart, which is done by kuberay.
7. The standby node will terminate the competition process when it finds itself as the leader, starting the gcs and dashboard processes, etc.
8. Then the newly started process in step 6 will be stuck in the competition process as a standby node until the current leader node in step 7 fails.


How to use it
----------------
To enable it, set the OS environment variable ``RAY_ENABLE_HEAD_NODE_HA`` to ``True``.


Dependency
----------------
- Redis is required for the head node to compete for the leadership.
- Kuberay is required for the head node to restart the pod when it fails. And worker nodes must access the head node through the domain name provided by Kuberay to avoid the problem that the head node's ip address is changed.


