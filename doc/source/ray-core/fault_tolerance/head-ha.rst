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
The Head High-Availability Feature is implemented through the following steps:

1. **Simultaneous Initialization:** Two or more head nodes are launched simultaneously.

2. **Leadership Election:** During the startup process—before initializing the node or starting the head node process—each node connects to Redis and competes for leadership using Redis’s distributed lock mechanism.

3. **Leader Node Activation:** Only the node that successfully acquires leadership proceeds with starting critical processes such as the `gcs_server` and `dashboard`. 

4. **Standby Node Behavior:** Nodes that fail to gain leadership remain in the competition loop as standby nodes until the leader node encounters a failure.

5. **Periodic Lease Renewal:** After a successful startup, the leader node periodically renews the distributed lock in Redis to maintain its leadership status. Meanwhile, a daemon process monitors the node's leadership status.

6. **Leader Node Failure Handling:** If the leader node's pod fails entirely or lease renewal fails, the node considers itself as a standby node. It terminates all processes, exits the startup process, and triggers a pod restart via KubeRay.

7. **Leadership Transition:** When a standby node detects that it has become the new leader, it terminates the competition process and initiates the necessary services (e.g., `gcs_server`, `dashboard`).

8. **Standby Node Re-entry:** Upon restarting, the previously failed leader node enters the competition loop as a standby node, waiting for the current leader node to fail before taking over.



How to use it
----------------
To enable it, set the OS environment variable ``RAY_ENABLE_HEAD_HA`` to ``True``.


Dependency
----------------
- Redis is required for the head node to compete for the leadership.
- Kuberay is required for the head node to restart the pod when it fails. And worker nodes must access the head node through the domain name provided by Kuberay to avoid the problem that the head node's ip address is changed.


