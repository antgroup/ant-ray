import asyncio
import pytest
import mock
import os
import async_timeout
import psutil

from ray.new_dashboard.modules.job.job_agent import DownloadManager
from ray.new_dashboard.modules.job import job_consts
from ray.new_dashboard.utils import create_task


class MockDownloadManager(DownloadManager):

    terminated_proc = []

    class MockProcess:
        def __init__(self, url):
            self.key = url

        def kill(self):
            pass

        async def wait(self):
            await asyncio.sleep(0.1)

    def __init__(self, use_meaningful_url=False):
        super().__init__("/tmp/ray")
        self.call_check_cmd_record = []
        self.download_by_http_request = []
        self.download_time = 0
        self._use_meaningful_url = use_meaningful_url

    def _raise_exception_or_not(self, url):
        if url == "exception":
            raise Exception(url)

    async def _check_output_cmd(self, cmd, *, logger_prefix=None, **kwargs):
        url = cmd[-1]
        if not self._use_meaningful_url:
            await asyncio.sleep(0.1)
            self.call_check_cmd_record.append((logger_prefix, cmd))
            new_cmd = ["sleep", "0.1"]
        else:
            url, suffix_url = url.split("-", 1)
            assert suffix_url.startswith("sleep-")
            float(suffix_url.split("-", 1)[-1])
            new_cmd = ["sleep", suffix_url.split("-", 1)[-1]]
        await super()._check_output_cmd(
            new_cmd, logger_prefix=logger_prefix, **kwargs)
        self._raise_exception_or_not(url)
        return url

    async def _download_by_http_request(self, http_session, url,
                                        temp_filename):
        await asyncio.sleep(0.1)
        self.download_by_http_request.append((http_session, url,
                                              temp_filename))
        self._raise_exception_or_not(url)


def unpack_download_task(download_task):
    return download_task.task, download_task.temp_filename


async def fake_recycle_func(process):
    await asyncio.sleep(0.1)


def fake_kill_func(process):
    MockDownloadManager.terminated_proc.append(process)


@mock.patch.object(MockDownloadManager, "_kill_process")
@mock.patch.object(MockDownloadManager, "_wait_process")
@pytest.mark.asyncio
async def test_create_one_wget_download_task(fake_wait, fake_kill):
    dm = MockDownloadManager()
    MockDownloadManager.terminated_proc = []
    fake_wait.side_effect = fake_recycle_func
    fake_kill.side_effect = fake_kill_func

    task, temp_filename = unpack_download_task(
        dm._create_download_task(job_consts.WGET_DOWNLOADER, None, "url",
                                 "filename"))

    assert len(dm._tasks_store) == 1
    assert dm._tasks_store["url"].targets == {"filename"}

    await task

    assert len(dm._tasks_store) == 0
    assert "download_manager" in temp_filename
    assert len(dm.call_check_cmd_record) == 1
    assert len(dm.download_by_http_request) == 0

    assert len(MockDownloadManager.terminated_proc) == 1

    # Triggering `_wait_process`
    await asyncio.sleep(0.1)

    assert fake_wait.call_count == 1


@mock.patch.object(MockDownloadManager, "_kill_process")
@mock.patch.object(MockDownloadManager, "_wait_process")
@pytest.mark.asyncio
async def test_create_one_aiohttp_download_task(fake_wait, fake_kill):
    dm = MockDownloadManager()
    MockDownloadManager.terminated_proc = []
    fake_wait.side_effect = fake_recycle_func
    fake_kill.side_effect = fake_kill_func

    task, temp_filename = unpack_download_task(
        dm._create_download_task(job_consts.AIOHTTP_DOWNLOADER, "http_session",
                                 "url", "filename"))

    assert len(dm._tasks_store) == 1
    assert dm._tasks_store["url"].targets == {"filename"}

    await task

    assert len(dm._tasks_store) == 0
    assert "download_manager" in temp_filename
    assert len(dm.call_check_cmd_record) == 0
    assert len(dm.download_by_http_request) == 1

    assert len(MockDownloadManager.terminated_proc) == 0

    # Triggering `_wait_process`
    await asyncio.sleep(0.1)

    assert fake_wait.call_count == 0


@mock.patch.object(MockDownloadManager, "_kill_process")
@mock.patch.object(MockDownloadManager, "_wait_process")
@mock.patch("ray.new_dashboard.utils.force_hardlink", return_value=None)
@pytest.mark.asyncio
async def test_create_multi_download_task(fake_link, fake_wait, fake_kill):
    dm = MockDownloadManager()
    MockDownloadManager.terminated_proc = []
    fake_wait.side_effect = fake_recycle_func
    fake_kill.side_effect = fake_kill_func

    task1_0, temp_filename1_0 = unpack_download_task(
        dm._create_download_task(job_consts.WGET_DOWNLOADER, None, "url1",
                                 "111"))

    task1_1, temp_filename1_1 = unpack_download_task(
        dm._create_download_task(job_consts.WGET_DOWNLOADER, None, "url1",
                                 "222"))

    task1_2, temp_filename1_2 = unpack_download_task(
        dm._create_download_task(job_consts.WGET_DOWNLOADER, None, "url1",
                                 "333"))

    task2_0, temp_filename2_0 = unpack_download_task(
        dm._create_download_task(job_consts.AIOHTTP_DOWNLOADER, "http_session",
                                 "url2", "444"))

    task3_0, temp_filename3_0 = unpack_download_task(
        dm._create_download_task(job_consts.WGET_DOWNLOADER, None, "url3",
                                 "555"))

    task3_1, temp_filename3_1 = unpack_download_task(
        dm._create_download_task(job_consts.WGET_DOWNLOADER, None, "url3",
                                 "666"))

    task4_0, temp_filename4_0 = unpack_download_task(
        dm._create_download_task(job_consts.AIOHTTP_DOWNLOADER, "http_session",
                                 "url4", "777"))

    task4_1, temp_filename4_1 = unpack_download_task(
        dm._create_download_task(job_consts.AIOHTTP_DOWNLOADER, "http_session",
                                 "url4", "888"))

    # 1.6 second is enough, this 8 test case just takes 0.8 second
    # and it can triggering terminate_process
    await asyncio.sleep(1.6)

    assert fake_link.call_count == 8
    all_args = list(map(lambda arg: arg[1], fake_link.mock_calls))
    all_args.sort(key=lambda arg: arg[-1])
    assert all_args[0] == (temp_filename1_0, "111")
    assert all_args[1] == (temp_filename1_1, "222")
    assert all_args[2] == (temp_filename1_2, "333")
    assert all_args[3] == (temp_filename2_0, "444")
    assert all_args[4] == (temp_filename3_0, "555")
    assert all_args[5] == (temp_filename3_1, "666")
    assert all_args[6] == (temp_filename4_0, "777")
    assert all_args[7] == (temp_filename4_1, "888")

    assert len(dm.call_check_cmd_record) == 2
    assert len(dm.download_by_http_request) == 2

    assert temp_filename1_0 == temp_filename1_1
    assert temp_filename1_1 == temp_filename1_2
    assert task1_0 is task1_1
    assert task1_1 is task1_2

    assert temp_filename3_0 == temp_filename3_1
    assert task3_0 is task3_1

    assert temp_filename4_0 == temp_filename4_1
    assert task4_0 is task4_1

    assert len(dm._tasks_store) == 0

    assert len(MockDownloadManager.terminated_proc) == 2


@mock.patch("ray.new_dashboard.utils.force_hardlink", return_value=None)
@pytest.mark.asyncio
async def test_download(fake_link):
    dm = MockDownloadManager()
    url = "url"
    temp_filename = os.path.join(dm._temp_dir, dm._get_url_hash(url))
    filename = "filename"

    await dm.download(job_consts.WGET_DOWNLOADER, None, url, filename)

    class Any:
        def __eq__(self, other):
            return True

    fake_link.assert_has_calls(
        [mock.call(temp_filename, filename, logger_prefix=Any())])


@mock.patch("os.path.exists", return_value=True)
@mock.patch("os.remove", return_value=None)
@pytest.mark.asyncio
async def test_download_with_exception(fake_remove, fake_exists):
    dm = MockDownloadManager()
    url = "exception"
    temp_filename = os.path.join(dm._temp_dir, dm._get_url_hash(url))
    filename = "filename"

    with pytest.raises(RuntimeError) as error:
        await dm.download(job_consts.WGET_DOWNLOADER, None, url, filename)

    assert error.value.args[0] == \
        f"Download failed, downloader: wget, \
url: exception, filename: {temp_filename}"

    fake_remove.assert_has_calls([mock.call(temp_filename)])
    fake_exists.assert_has_calls([mock.call(temp_filename)])

    fake_exists.reset()
    fake_remove.reset()

    fake_exists.return_value = False

    with pytest.raises(RuntimeError) as error:
        await dm.download(job_consts.WGET_DOWNLOADER, None, url, filename)

    assert error.value.args[0] == \
        f"Download failed, downloader: wget, \
url: exception, filename: {temp_filename}"

    fake_exists.assert_has_calls([mock.call(temp_filename)])


@mock.patch("os.path.exists", return_value=True)
@mock.patch("os.remove", return_value=None)
@mock.patch.object(MockDownloadManager, "_download_post_process")
@pytest.mark.asyncio
async def test_download_with_cancel(fake_download_post_process, fake_remove,
                                    fake_exists):
    dm = MockDownloadManager()
    url = "url"
    filename = "filename"
    fake_download_post_process.return_value = None

    t1 = dm.download(job_consts.WGET_DOWNLOADER, None, url, filename)

    async def cancel_task(task):
        await asyncio.sleep(0.01)

    create_task(cancel_task(t1))
    await t1

    assert 1 == len(fake_download_post_process.mock_calls)

    name, args, kwargs = fake_download_post_process.mock_calls[0]
    url_called, raw_task_called = args

    assert url_called == url
    assert raw_task_called.cancelled


@mock.patch("os.link", return_value=None)
@pytest.mark.asyncio
async def test_process_self_killed_when_download_task_cancelled(fake_os_link):

    dm = MockDownloadManager(use_meaningful_url=True)
    url = "url-sleep-3600"
    filename = "filename"

    t1 = dm.download(job_consts.WGET_DOWNLOADER, None, url, filename)
    try:
        with async_timeout.timeout(1):
            await t1
    except RuntimeError:
        pass

    pro = psutil.Process(os.getpid())
    chil_pro = pro.children(recursive=True)
    assert len(chil_pro) == 1
    assert chil_pro[0].status() == "zombie"

    # wait for zombie-process been collected
    await asyncio.sleep(1)
    chil_pro = pro.children(recursive=True)
    assert len(chil_pro) == 0


@mock.patch("os.link", return_value=None)
@pytest.mark.asyncio
async def test_multi_download_task_with_timeout(fake_os_link):

    dm = MockDownloadManager(use_meaningful_url=True)

    url1 = "url_1-sleep-3600"
    filename1 = "filename1"
    t1 = dm.download(job_consts.WGET_DOWNLOADER, None, url1, filename1)

    url2 = "url_2-sleep-0.1"
    filename2 = "filename2"
    t2 = dm.download(job_consts.WGET_DOWNLOADER, None, url2, filename2)

    url3 = "url_3-sleep-0.1"
    filename3 = "filename3"
    t3 = dm.download(job_consts.WGET_DOWNLOADER, None, url3, filename3)

    url4 = "url_4-sleep-3600"
    filename4 = "filename4"
    t4 = dm.download(job_consts.WGET_DOWNLOADER, None, url4, filename4)

    url5 = "url_5-sleep-0.5"
    filename5 = "filename5"
    t5 = dm.download(job_consts.WGET_DOWNLOADER, None, url5, filename5)

    try:
        with async_timeout.timeout(1):
            await asyncio.gather(t1, t2, t3, t4, t5)
    except RuntimeError:
        pass

    pro = psutil.Process(os.getpid())
    chil_pro = pro.children(recursive=True)
    assert len(chil_pro) == 2
    assert chil_pro[0].status() == "zombie"
    assert chil_pro[1].status() == "zombie"

    # wait for zombie-process been collected
    await asyncio.sleep(1)
    chil_pro = pro.children(recursive=True)
    assert len(chil_pro) == 0
