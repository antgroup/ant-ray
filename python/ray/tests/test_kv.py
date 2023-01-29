import ray
from ray import ray_constants
from ray._raylet import connect_to_gcs

KEY_1 = b"TEST_KEY"
KEY_2 = b"TEST_KEY_2"
KEY_3 = b"TEST_KEY_3"
KEY_4 = b"TEST_KEY_4"

VALUE_1 = b"TEST_VAL"
VALUE_2 = b"TEST_VAL"
VALUE_3 = b"TEST_VAL"
VALUE_4 = b"TEST_VAL"

KEY_STR_1 = "TEST_KEY"
VALUE_STR_1 = "TEST_VAL"


def run_kv_test(gcs_client):
    assert gcs_client.kv_put(KEY_1, VALUE_1, True)
    assert VALUE_1 == gcs_client.kv_get(KEY_1)
    assert not gcs_client.kv_exists(KEY_2)
    assert gcs_client.kv_exists(KEY_1)
    assert gcs_client.kv_del(KEY_1)
    assert not gcs_client.kv_exists(KEY_1)
    assert gcs_client.kv_put(KEY_1, VALUE_1, False)
    assert not gcs_client.kv_put(KEY_1, VALUE_2, False)
    assert gcs_client.kv_get(KEY_1) == VALUE_1
    assert gcs_client.kv_put(KEY_1, VALUE_2, True)
    assert gcs_client.kv_get(KEY_1) == VALUE_2
    assert gcs_client.kv_del(KEY_1)
    assert not gcs_client.kv_del(KEY_1)

    assert gcs_client.kv_get(KEY_1) is None
    assert gcs_client.kv_put(KEY_1, VALUE_1, True)
    assert gcs_client.kv_put(KEY_2, VALUE_2, True)
    assert gcs_client.kv_put(KEY_3, VALUE_3, True)
    assert gcs_client.kv_put(KEY_4, VALUE_4, True)
    keys = set(gcs_client.kv_keys(b"TEST_KEY"))
    assert keys == {KEY_1, KEY_2, KEY_3, KEY_4}
    for key in keys:
        assert gcs_client.kv_del(key)

    # test string type kv
    assert gcs_client.kv_put(KEY_STR_1, VALUE_STR_1, True)
    assert VALUE_1 == gcs_client.kv_get(KEY_1)
    assert VALUE_1 == gcs_client.kv_get(KEY_STR_1)
    assert gcs_client.kv_exists(KEY_1)
    assert gcs_client.kv_exists(KEY_STR_1)
    assert gcs_client.kv_del(KEY_STR_1)
    assert not gcs_client.kv_exists(KEY_STR_1)


def test_gcs_client_core_worker(shutdown_only):
    ray.init()
    gcs_client = ray.worker.global_worker.core_worker.get_gcs_client()
    run_kv_test(gcs_client)


def test_gcs_client_address(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    ip, port = cluster.address.split(":")
    password = ray_constants.REDIS_DEFAULT_PASSWORD
    gcs_client = connect_to_gcs(ip, int(port), password)
    run_kv_test(gcs_client)


def test_internal_kv(ray_start_cluster_head):
    # 1. test put/get/exists/delete
    assert ray.internal.kv.put(KEY_1, VALUE_1, True)
    assert ray.internal.kv.put(KEY_1, VALUE_1, True)
    assert not ray.internal.kv.exists(KEY_1, True)
    assert ray.internal.kv.put(KEY_1, VALUE_2, True, True)
    assert ray.internal.kv.put(KEY_1, VALUE_2, True, True)
    assert VALUE_1 == ray.internal.kv.get(KEY_1)
    assert VALUE_2 == ray.internal.kv.get(KEY_1, True)
    assert not ray.internal.kv.exists(KEY_2)
    assert not ray.internal.kv.exists(KEY_2, True)
    assert ray.internal.kv.exists(KEY_1)
    assert ray.internal.kv.exists(KEY_1, True)
    assert ray.internal.kv.delete(KEY_1)
    assert not ray.internal.kv.exists(KEY_1)
    assert ray.internal.kv.delete(KEY_1, True)
    assert not ray.internal.kv.exists(KEY_1, True)

    # 2. test put overwrite
    assert ray.internal.kv.put(KEY_1, VALUE_1, False)
    assert ray.internal.kv.put(KEY_1, VALUE_1, False, True)
    assert not ray.internal.kv.put(KEY_1, VALUE_2, False)
    assert not ray.internal.kv.put(KEY_1, VALUE_2, False, True)
    assert ray.internal.kv.get(KEY_1) == VALUE_1
    assert ray.internal.kv.get(KEY_1, True) == VALUE_1
    assert ray.internal.kv.put(KEY_1, VALUE_2, True)
    assert ray.internal.kv.put(KEY_1, VALUE_3, True, True)
    assert ray.internal.kv.get(KEY_1) == VALUE_2
    assert ray.internal.kv.get(KEY_1, True) == VALUE_3
    assert ray.internal.kv.delete(KEY_1)
    assert ray.internal.kv.delete(KEY_1, True)
    assert not ray.internal.kv.delete(KEY_1)
    assert not ray.internal.kv.delete(KEY_1, True)
    assert not ray.internal.kv.exists(KEY_1)
    assert not ray.internal.kv.exists(KEY_1, True)

    assert ray.internal.kv.get(KEY_1) is None
    assert ray.internal.kv.put(KEY_1, VALUE_1, True)
    assert ray.internal.kv.put(KEY_2, VALUE_2, True)
    assert ray.internal.kv.put(KEY_3, VALUE_3, True)
    assert ray.internal.kv.put(KEY_4, VALUE_4, True)
    for key in {KEY_1, KEY_2, KEY_3, KEY_4}:
        assert ray.internal.kv.delete(key)

    # test string type key/value
    assert ray.internal.kv.put(KEY_STR_1, VALUE_STR_1, True)
    assert VALUE_1 == ray.internal.kv.get(KEY_1)
    assert VALUE_1 == ray.internal.kv.get(KEY_STR_1)
    assert ray.internal.kv.exists(KEY_STR_1)
    assert ray.internal.kv.exists(KEY_1)
    assert ray.internal.kv.delete(KEY_STR_1)


def test_actor_call_kv(ray_start_cluster_head):
    @ray.remote
    class Actor:
        def get_values(self):
            assert VALUE_1 == ray.internal.kv.get(KEY_1)
            assert VALUE_2 == ray.internal.kv.get(KEY_1, True)
            assert ray.internal.kv.exists(KEY_1)
            assert ray.internal.kv.exists(KEY_1, True)
            assert ray.internal.kv.delete(KEY_1)
            assert ray.internal.kv.delete(KEY_1, True)
            return True

    assert ray.internal.kv.put(KEY_1, VALUE_1, True)
    assert ray.internal.kv.put(KEY_1, VALUE_2, True, True)
    actor = Actor.remote()
    assert ray.get(actor.get_values.remote()) is True
    assert not ray.internal.kv.exists(KEY_1)
    assert not ray.internal.kv.exists(KEY_1, True)


def test_task_call_kv(ray_start_cluster_head):
    @ray.remote(num_cpus=1)
    def f():
        assert ray.internal.kv.put(KEY_1, VALUE_1, True)
        assert ray.internal.kv.put(KEY_1, VALUE_2, True, True)
        return True

    assert ray.get(f.remote()) is True
    assert VALUE_1 == ray.internal.kv.get(KEY_1)
    assert VALUE_2 == ray.internal.kv.get(KEY_1, True)
    assert ray.internal.kv.exists(KEY_1)
    assert ray.internal.kv.exists(KEY_1, True)
    assert ray.internal.kv.delete(KEY_1)
    assert ray.internal.kv.delete(KEY_1, True)


def test_kv_too_long(ray_start_cluster_head):
    b = bytes(5 * 1024 * 1024 + 1)
    import pytest
    with pytest.raises(ValueError):
        ray.internal.kv.put(KEY_1, b, True)
    with pytest.raises(ValueError):
        ray.internal.kv.put(b, VALUE_1, True)
    with pytest.raises(ValueError):
        ray.internal.kv.get(b)
    with pytest.raises(ValueError):
        ray.internal.kv.delete(b)
    with pytest.raises(ValueError):
        ray.internal.kv.exists(b)

    with pytest.raises(ValueError):
        ray.internal.kv.put(KEY_1, b, True, True)
    with pytest.raises(ValueError):
        ray.internal.kv.put(b, VALUE_1, True, True)
    with pytest.raises(ValueError):
        ray.internal.kv.get(b, True)
    with pytest.raises(ValueError):
        ray.internal.kv.delete(b, True)
    with pytest.raises(ValueError):
        ray.internal.kv.exists(b, True)


def test_kv_auto_deleted_when_job_finish(ray_start_cluster):
    # start ray head
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    cluster.wait_for_nodes()

    # job 1
    ray.init(address=cluster.address)
    gcs_client = ray.worker.global_worker.core_worker.get_gcs_client()
    assert ray.internal.kv.put(KEY_1, VALUE_1)
    assert gcs_client.kv_get(b"kv-01000000-" + KEY_1) == VALUE_1
    assert gcs_client.kv_get(KEY_1) is None
    assert ray.internal.kv.put(KEY_1, VALUE_2, isGlobal=True)
    assert gcs_client.kv_get(KEY_1) == VALUE_2
    assert ray.internal.kv.exists(KEY_1)
    assert ray.internal.kv.exists(KEY_1, True)
    ray.shutdown()

    # job 2
    ray.init(address=cluster.address)
    gcs_client = ray.worker.global_worker.core_worker.get_gcs_client()
    assert not ray.internal.kv.exists(KEY_1)
    assert ray.internal.kv.exists(KEY_1, True)
    assert gcs_client.kv_get(b"kv-01000000-" + KEY_1) is None
    assert gcs_client.kv_get(KEY_1) == VALUE_1
    assert ray.internal.kv.put(KEY_1, VALUE_1)
    assert gcs_client.kv_get(KEY_1) == VALUE_1
    assert gcs_client.kv_get(b"kv-02000000-" + KEY_1) == VALUE_1
    ray.shutdown()


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
