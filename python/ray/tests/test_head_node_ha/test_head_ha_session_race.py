import pytest
import time
import os
import threading
from ray._private.parameter import RayParams
from ray._private.node import Node


def test_head_ha_session_race(external_redis):
    """
    Manually test the race condition by:
    1. Starting node1 normally
    2. Before node1 writes session to GCS, start node2
    3. Node2 should generate its own session name
    4. When node2 tries to write to GCS, it should fail

    This simulates the exact bug scenario.
    """
    redis_addr = os.environ.get("RAY_REDIS_ADDRESS")

    # Enable HEAD HA mode
    os.environ["RAY_ENABLE_HEAD_HA"] = "TRUE"
    # Set HEAD HA configuration to speed up the test
    os.environ["RAY_HA_CHECK_INTERVAL_MS"] = "1000"
    os.environ["RAY_HA_KEY_EXPIRE_TIME_MS"] = "5000"
    os.environ["RAY_HA_WAIT_TIME_AFTER_BE_ACTIVE_MS"] = "1000"
    os.environ["RAY_HA_WAIT_PRE_GCS_STOP_MAX_TIME_MS"] = "10000"

    # Monkey-patch the critical methods
    original_write_cluster_info = Node._write_cluster_info_to_kv

    def patched_write_cluster_info(self):
        """Patched to add synchronization."""
        time.sleep(0.5)  # sleep to ensure both nodes create own session names
        result = original_write_cluster_info(self)
        print("Node 1: Successfully wrote to GCS")
        return result

    # Apply patches
    Node._write_cluster_info_to_kv = patched_write_cluster_info

    node1_start_first_tag: bool = True
    leader_head, standby_head = None, None
    thread1, thread2 = None, None
    node1: Node = None
    node2: Node = None
    node1_exception = None
    node2_exception = None

    def start_node_1():
        nonlocal node1, node1_exception
        threading.current_thread().name = "node1_thread"

        try:
            print("Node 1: Starting...")
            ray_params = RayParams(
                external_addresses=[redis_addr],
            )
            node1 = Node(
                ray_params,
                head=True,
                shutdown_at_exit=False,
                connect_only=False,
            )

            print("Node 1: Initialized successfully")
        except Exception as e:
            node1_exception = e
            print(f"Node 1: Failed with exception: {e}")

    def start_node_2():
        nonlocal node2, node2_exception
        threading.current_thread().name = "node2_thread"

        try:
            print("Node 2: Starting...")
            ray_params = RayParams(
                external_addresses=[redis_addr],
            )

            node2 = Node(
                ray_params,
                head=True,
                shutdown_at_exit=False,
                connect_only=False,
            )

            print("Node 2: Initialized successfully")
        except Exception as e:
            node2_exception = e
            print(f"Node 2: Failed with exception: {e}")

    try:
        print("\n=== Starting test ===")

        # Start node1
        thread1 = threading.Thread(target=start_node_1, name="node1_thread")
        thread1.start()
        time.sleep(0.2)

        # Start node2, it should be standby head
        thread2 = threading.Thread(target=start_node_2, name="node2_thread")
        thread2.start()

        time.sleep(10)

        # set leader and standby head
        if node1 is not None:
            leader_head = node1
            node1_start_first_tag = True
            print("Node 1 is leader head")
        else:
            leader_head = node2
            node1_start_first_tag = False
            print("Node 2 is leader head")

        # try to kill leader head, allow standby head to become leader head
        try:
            print("Killing leader head...")
            leader_head.kill_all_processes(check_alive=False, allow_graceful=True)
        except Exception as e:
            print(f"Error killing leader head: {e}")

        time.sleep(5)

        print("\n=== Test Results ===")

        assert (
            node1_exception is None
        ), f"Node 1 failed with exception: {node1_exception}"
        assert (
            node2_exception is None
        ), f"Node 2 failed with exception: {node2_exception}"

    finally:
        # wait for threads to finish
        thread1.join(timeout=5)
        thread2.join(timeout=5)
        # Restore original methods
        Node._write_cluster_info_to_kv = original_write_cluster_info
        if node1_start_first_tag:
            standby_head = node2
        else:
            standby_head = node1
        try:
            print("Cleaning up standby head...")
            # clear the leader selector to avoid issues during cleanup
            if standby_head is not None and standby_head.leader_selector is not None:
                standby_head.leader_selector.stop()
                standby_head.leader_selector = None
                standby_head.kill_all_processes(check_alive=False, allow_graceful=True)
        except Exception as e:
            print("Error cleaning up standby head :", e)
        # Wait a bit to ensure all processes are cleaned up
        time.sleep(1)


if __name__ == "__main__":
    import sys

    # Run with pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
