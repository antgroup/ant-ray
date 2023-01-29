import os
import pytest
from ray.tests.conftest import *  # noqa


@pytest.fixture
def enable_test_module():
    os.environ["RAY_DASHBOARD_MODULE_TEST"] = "true"
    yield
    os.environ.pop("RAY_DASHBOARD_MODULE_TEST", None)


@pytest.fixture
def disable_aiohttp_cache():
    os.environ["RAY_DASHBOARD_NO_CACHE"] = "true"
    yield
    os.environ.pop("RAY_DASHBOARD_NO_CACHE", None)


@pytest.fixture
def set_http_proxy():
    http_proxy = os.environ.get("http_proxy", None)
    https_proxy = os.environ.get("https_proxy", None)

    # set http proxy
    os.environ["http_proxy"] = "www.example.com:990"
    os.environ["https_proxy"] = "www.example.com:990"

    yield

    # reset http proxy
    if http_proxy:
        os.environ["http_proxy"] = http_proxy
    else:
        del os.environ["http_proxy"]

    if https_proxy:
        os.environ["https_proxy"] = https_proxy
    else:
        del os.environ["https_proxy"]


@pytest.fixture
def fast_gcs_failure_detection():
    os.environ["GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR"] = "2"
    os.environ["GCS_CHECK_ALIVE_INTERVAL_SECONDS"] = "1"
    os.environ["GCS_RETRY_CONNECT_INTERVAL_SECONDS"] = "1"
    yield
    os.environ.pop("GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR", None)
    os.environ.pop("GCS_CHECK_ALIVE_INTERVAL_SECONDS", None)
    os.environ.pop("GCS_RETRY_CONNECT_INTERVAL_SECONDS", None)


@pytest.fixture
def fast_redis_failure_detection():
    os.environ["REDIS_CHECK_ALIVE_MAX_NUM_OF_UNRECEIVED_MESSAGES"] = "2"
    os.environ["REDIS_CHECK_ALIVE_INTERVAL_SECONDS"] = "1"
    yield
    os.environ.pop("REDIS_CHECK_ALIVE_MAX_NUM_OF_UNRECEIVED_MESSAGES", None)
    os.environ.pop("REDIS_CHECK_ALIVE_INTERVAL_SECONDS", None)


@pytest.fixture
def fast_organize_data():
    os.environ["ORGANIZE_DATA_INTERVAL_SECONDS"] = "1"
    yield
    os.environ.pop("ORGANIZE_DATA_INTERVAL_SECONDS", None)


@pytest.fixture
def fast_update_nodes():
    os.environ["NODE_STATS_UPDATE_INTERVAL_SECONDS"] = "1"
    yield
    os.environ.pop("NODE_STATS_UPDATE_INTERVAL_SECONDS", None)


@pytest.fixture
def fast_update_agents():
    os.environ["UPDATE_AGENTS_INTERVAL_SECONDS"] = "1"
    yield
    os.environ.pop("UPDATE_AGENTS_INTERVAL_SECONDS", None)


@pytest.fixture
def fast_reporter_update():
    os.environ["REPORTER_UPDATE_INTERVAL_MS"] = "1000"
    yield
    os.environ.pop("REPORTER_UPDATE_INTERVAL_MS", None)


@pytest.fixture
def minimal_event_persistence_logger_count():
    os.environ["PERSISTENCE_LOGGER_COUNT_LIMIT"] = "1"
    yield
    os.environ.pop("PERSISTENCE_LOGGER_COUNT_LIMIT", None)


@pytest.fixture
def minimal_per_job_event_count():
    os.environ["PER_JOB_EVENTS_COUNT_LIMIT"] = "5"
    yield
    os.environ.pop("PER_JOB_EVENTS_COUNT_LIMIT", None)


@pytest.fixture
def maximize_per_job_event_count():
    os.environ["PER_JOB_EVENTS_COUNT_LIMIT"] = "9999999"
    yield
    os.environ.pop("PER_JOB_EVENTS_COUNT_LIMIT", None)


@pytest.fixture
def minimal_head_cache_size():
    os.environ["EVENT_HEAD_CACHE_SIZE"] = "1"
    yield
    os.environ.pop("EVENT_HEAD_CACHE_SIZE", None)


@pytest.fixture
def maximize_head_update_interval_seconds():
    os.environ["EVENT_HEAD_UPDATE_INTERVAL_SECONDS"] = "9999999"
    yield
    os.environ.pop("EVENT_HEAD_UPDATE_INTERVAL_SECONDS", None)


@pytest.fixture
def minmize_head_update_internal_seconds():
    os.environ["EVENT_HEAD_UPDATE_INTERVAL_SECONDS"] = "1"
    yield
    os.environ.pop("EVENT_HEAD_UPDATE_INTERVAL_SECONDS", None)


@pytest.fixture
def test_event_report():
    os.environ["DISABLE_EVENT_HEAD_MONITOR"] = "1"
    yield
    os.environ.pop("DISABLE_EVENT_HEAD_MONITOR", None)


@pytest.fixture
def fast_monitor_events():
    os.environ["SCAN_EVENT_DIR_INTERVAL_SECONDS"] = "1"
    yield
    os.environ.pop("SCAN_EVENT_DIR_INTERVAL_SECONDS", None)


@pytest.fixture
def fast_get_etag_timeout():
    os.environ["GET_ETAG_TIMEOUT_SECONDS"] = "1"
    yield
    os.environ.pop("GET_ETAG_TIMEOUT_SECONDS", None)


@pytest.fixture
def enable_check_signature():
    os.environ["RAY_DASHBOARD_CHECK_SIGNATURE"] = "true"
    yield
    os.environ.pop("RAY_DASHBOARD_CHECK_SIGNATURE", None)


@pytest.fixture
def fast_pop_dead_actor():
    os.environ["RAY_DASHBOARD_ACTOR_FILTER_MINIMUM_INTERVAL_SECONDS"] = "0"
    os.environ["RAY_DASHBOARD_DEAD_ACTOR_TTL_SECONDS"] = "0"
    yield
    os.environ.pop("RAY_DASHBOARD_ACTOR_FILTER_MINIMUM_INTERVAL_SECONDS", None)
    os.environ.pop("RAY_DASHBOARD_DEAD_ACTOR_TTL_SECONDS", None)


@pytest.fixture
def fast_pop_dead_actor_with_five_second_interval():
    os.environ["RAY_DASHBOARD_ACTOR_FILTER_MINIMUM_INTERVAL_SECONDS"] = "5"
    os.environ["RAY_DASHBOARD_DEAD_ACTOR_TTL_SECONDS"] = "0"
    yield
    os.environ.pop("RAY_DASHBOARD_ACTOR_FILTER_MINIMUM_INTERVAL_SECONDS", None)
    os.environ.pop("RAY_DASHBOARD_DEAD_ACTOR_TTL_SECONDS", None)


@pytest.fixture
def zero_dead_actors():
    os.environ["RAY_DASHBOARD_ACTOR_FILTER_MINIMUM_INTERVAL_SECONDS"] = "0"
    os.environ["RAY_DASHBOARD_TOTAL_DEAD_ACTOR_COUNT_LIMIT"] = "0"
    yield
    os.environ.pop("RAY_DASHBOARD_ACTOR_FILTER_MINIMUM_INTERVAL_SECONDS", None)
    os.environ.pop("RAY_DASHBOARD_TOTAL_DEAD_ACTOR_COUNT_LIMIT", None)


@pytest.fixture
def enable_system_jobs():
    os.environ[
        "ray_config_file"] = "http://alipay-kepler-resource.cn-hangzhou.alipay.aliyun-inc.com/ray-cluster-config-dev/systemjob-test.yaml"  # noqa: E501
    yield
    os.environ.pop("ray_config_file", None)


@pytest.fixture
def set_event_display_level_info():
    os.environ["RAY_EVENT_DISPLAY_LEVEL"] = "info"
    yield
    os.environ.pop("RAY_EVENT_DISPLAY_LEVEL", None)


@pytest.fixture
def set_dashboard_inactive_jobs_capacity():
    os.environ["DASHBOARD_INACTIVE_JOBS_CAPACITY"] = "1"
    yield
    os.environ.pop("DASHBOARD_INACTIVE_JOBS_CAPACITY", None)


@pytest.fixture
def set_dashboard_agent_log_clean_configs():
    os.environ["RAY_LOG_CLEANUP_ENABLED"] = "true"
    os.environ["RAY_LOG_CLEANUP_INTERVAL_IN_SECONDS"] = "5"
    os.environ["RAY_MAXIMUM_NUMBER_OF_LOGS_TO_KEEP"] = "15"
    os.environ["RAY_LOGS_TTL_SECONDS"] = "10"
    os.environ["RAY_PER_JOB_DEAD_PROCESS_LOG_NUMBER_LIMIT"] = "11"
    yield
    os.environ.pop("RAY_LOG_CLEANUP_ENABLED", None)
    os.environ.pop("RAY_LOG_CLEANUP_INTERVAL_IN_SECONDS", None)
    os.environ.pop("RAY_MAXIMUM_NUMBER_OF_LOGS_TO_KEEP", None)
    os.environ.pop("RAY_LOGS_TTL_SECONDS", None)
    os.environ.pop("RAY_PER_JOB_DEAD_PROCESS_LOG_NUMBER_LIMIT", None)


@pytest.fixture
def set_running_job_for_agent_log_clean():
    jobs_dir = "/tmp/ray/job"
    try:
        os.mkdir(jobs_dir)
    except FileExistsError:
        pass
    mocked_job_id = "6"
    mocked_job_dir = os.path.join(jobs_dir, mocked_job_id)
    try:
        os.mkdir(mocked_job_dir)
    except FileExistsError:
        pass
    yield {"mocked_job_id": mocked_job_id}
    os.removedirs(mocked_job_dir)


@pytest.fixture
def fastern_worker_register_failures():
    os.environ["RAY_worker_register_timeout_seconds"] = "3"
    os.environ["RAY_worker_register_timeout_max_retries"] = "0"
    yield
    os.environ.pop("RAY_worker_register_timeout_seconds", None)
    os.environ.pop("RAY_worker_register_timeout_max_retries", None)


@pytest.fixture
def disable_gcs_task_scheduling():
    os.environ["RAY_GCS_TASK_SCHEDULING_ENABLED"] = "false"
    yield
    os.environ.pop("RAY_GCS_TASK_SCHEDULING_ENABLED", None)
