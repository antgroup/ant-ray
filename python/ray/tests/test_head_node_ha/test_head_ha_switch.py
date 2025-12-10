import pytest
import time
import redis
import os
import threading
from ray._private.parameter import RayParams
from ray._private.node import Node


def check_redis_alive(redis_client, description="Redis"):
    """Check if Redis is alive and accessible."""
    try:
        redis_client.ping()
        print(f"✓ {description} is alive and responding")
        return True
    except Exception as e:
        print(f"✗ {description} check failed: {e}")
        return False


def test_head_ha_switch(external_redis):
    """
    Test head HA failover: when the leader node fails, a standby node should
    take over and successfully initialize using the persisted session name.

    This test:
    1. Starts node_1 as the initial leader
    2. Starts node_2 as a standby in a separate thread
    3. Kills node_1 to simulate failure
    4. Verifies node_2 takes over as the new leader with the same session name
    """
    redis_addr = os.environ.get("RAY_REDIS_ADDRESS")
    ip, port = redis_addr.split(":")
    redis_cli = redis.Redis(ip, int(port))
    print(f"Using Redis at {ip}:{port}")

    # Initial Redis health check
    assert check_redis_alive(
        redis_cli, "External Redis"
    ), "Redis must be alive before starting test"

    # Enable HA mode
    os.environ["RAY_ENABLE_HEAD_HA"] = "TRUE"

    # Set HA configuration to speed up the test
    # Reduce check interval from 3s to 500ms
    os.environ["RAY_HA_CHECK_INTERVAL_MS"] = "500"
    # Reduce key expire time from 60s to 5s
    os.environ["RAY_HA_KEY_EXPIRE_TIME_MS"] = "5000"
    # Reduce wait time after becoming active from 2s to 500ms
    os.environ["RAY_HA_WAIT_TIME_AFTER_BE_ACTIVE_MS"] = "500"
    # Reduce max wait time for pre-GCS stop from 3 days to 10s
    os.environ["RAY_HA_WAIT_PRE_GCS_STOP_MAX_TIME_MS"] = "10000"

    # Track nodes and exceptions from threads
    node_1 = None
    node_2 = None
    node_2_exception = None
    node_2_session_name = None

    def start_node_2():
        """Start node_2 as a standby head node."""
        nonlocal node_2, node_2_exception, node_2_session_name
        try:
            print("Starting node_2 as standby...")
            ray_params_2 = RayParams(
                external_addresses=[redis_addr],
            )

            node_2 = Node(
                ray_params_2,
                head=True,
                shutdown_at_exit=False,
                connect_only=False,
            )

            # If we get here, node_2 successfully initialized
            node_2_session_name = node_2.session_name
            print(
                f"Node 2 successfully initialized with session: {node_2_session_name}"
            )

        except Exception as e:
            node_2_exception = e
            print(f"Node 2 failed with exception: {e}")

    try:
        # Step 1: Start node_1 as the initial leader
        print("Starting node_1 as initial leader...")
        ray_params_1 = RayParams(
            external_addresses=[redis_addr],
        )

        node_1 = Node(
            ray_params_1,
            head=True,
            shutdown_at_exit=False,
            connect_only=False,
        )

        # Wait for node_1 to fully initialize and write session name to Redis
        time.sleep(2)

        node_1_session_name = node_1.session_name
        print(f"Node 1 session name: {node_1_session_name}")

        # Verify session name was persisted to Redis
        persisted_name = node_1.check_persisted_session_name()
        assert persisted_name is not None, "Session name should be persisted in Redis"
        persisted_name_str = persisted_name.decode("utf-8")
        print(f"Persisted session name in Redis: {persisted_name_str}")
        assert (
            persisted_name_str == node_1_session_name
        ), f"Persisted session name should match node_1: expected {node_1_session_name}, got {persisted_name_str}"

        # Step 2: Start node_2 in a separate thread as a standby
        node_2_thread = threading.Thread(target=start_node_2)
        node_2_thread.start()

        # Give node_2 a moment to start up and begin waiting for leadership
        # Reduced from 5s to 2s
        time.sleep(2)

        # Verify Redis is still alive before killing node_1
        assert check_redis_alive(
            redis_cli, "Redis before killing node_1"
        ), "Redis should be alive before killing node_1"

        # Step 3: Kill node_1's processes to simulate failure
        try:
            print("Killing node_1 to simulate failure...")
            node_1.kill_all_processes(check_alive=False, allow_graceful=True)
        except Exception as e:
            print(f"Error killing node_1 processes: {e}")
        node_1 = None  # Mark as cleaned up

        time.sleep(
            2
        )  # Wait a moment for the kill to take effect (reduced from 5s to 2s)
        # Verify Redis is still alive after killing node_1
        assert check_redis_alive(
            redis_cli, "Redis after killing node_1"
        ), "Redis should still be alive after killing node_1"

        # Step 4: Wait for node_2 to take over
        # Give it enough time to detect the leader failure and become the new leader
        print("Waiting for node_2 to take over as leader...")
        node_2_thread.join(timeout=30)

        # Check if node_2 successfully took over
        if node_2_exception:
            raise AssertionError(f"Node 2 failed to take over: {node_2_exception}")

        if node_2_thread.is_alive():
            raise AssertionError(
                "Node 2 did not complete initialization within 30 seconds"
            )

        assert (
            node_2_session_name is not None
        ), "Node 2 should have initialized with a session name"
        assert (
            node_2_session_name == node_1_session_name
        ), f"Node 2 session name should match node_1: expected {node_1_session_name}, got {node_2_session_name}"

        print(
            f"✓ HA failover successful! Node 2 took over with session: {node_2_session_name}"
        )

    finally:
        # Cleanup
        if node_1:
            try:
                print("Cleaning up node_1...")
                node_1.kill_all_processes(check_alive=False, allow_graceful=True)
            except Exception as e:
                print(f"Error cleaning up node_1: {e}")

        if node_2:
            try:
                print("Cleaning up node_2...")
                node_2.kill_all_processes(check_alive=False, allow_graceful=True)
            except Exception as e:
                print(f"Error cleaning up node_2: {e}")


if __name__ == "__main__":
    import sys

    # Run with pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
