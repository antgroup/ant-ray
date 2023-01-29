import os
import time
import json
import copy
import logging
import urllib.parse
import hashlib

import aiohttp
import async_timeout
import ray.new_dashboard.consts as dashboard_consts

logger = logging.getLogger(__name__)


class OpenTSDBExporter:
    API = "/api/put"
    HEADERS = {"Content-Type": "application/json; charset=utf-8"}

    def __init__(self, http_session=None):
        host, user, key = self._get_config()
        self._enable = host is not None
        self._http_session = http_session
        self._header = copy.deepcopy(self.HEADERS)
        if user:
            self._header["X-CeresDB-AccessUser"] = user
        if key:
            self._key = key
        self._url = urllib.parse.urljoin(host, self.API)
        self._data = []

    def _get_config(self):
        config_file = os.environ.get("RAY_CERESDB_CONFIG_FILE")
        if config_file:
            with open(config_file, "r") as f:
                config_str = f.read()
            lines = config_str.splitlines()
            config_dict = dict(i.split("=") for i in lines)
            host = "http://" + \
                config_dict["ray.ceresdb.server.host"] + ":" + \
                config_dict["ray.ceresdb.server.port"]
            user = config_dict["ray.ceresdb.server.user"]
            key = config_dict["ray.ceresdb.server.key"]
            return host, user, key
        return None, None, None

    def put(self, metric, value, tags=None, timestamp=None):
        logger.debug("%s metric: %s, value: %s, tags: %s",
                     type(self).__name__, metric, value, tags)
        assert isinstance(metric, str)
        assert tags is None or isinstance(tags, dict)
        assert timestamp is None or isinstance(timestamp, int)
        assert all(
            isinstance(k, str) and isinstance(v, str)
            for k, v in (tags or {}).items())
        if self._enable and value is not None:
            self._data.append({
                "metric": metric,
                "timestamp": timestamp or int(time.time()),
                "value": value,
                "tags": tags,
            })
        return self

    def data_size(self):
        return len(self._data)

    async def commit(self,
                     timeout=dashboard_consts.REPORT_METRICS_TIMEOUT_SECONDS):
        logger.debug("%s commit", type(self).__name__)
        if not self._enable:
            return
        if len(self._data) == 0:
            return
        elif len(self._data) == 1:
            data = self._data[0]
        else:
            data = self._data
            self._data = []

        ts = str(time.time())
        token = hashlib.md5((self._key + ts).encode("utf-8")).hexdigest()
        self._header["X-CeresDB-Timestamp"] = ts
        self._header["X-CeresDB-AccessToken"] = token

        async def _post(http_session):
            async with http_session.post(
                    self._url, json=data, headers=self._header) as resp:
                if resp.status == 200:
                    # Server reply with
                    # 'Content-Type': 'text/plain; charset=utf-8',
                    # So, resp.json() will raise exception.
                    json_str = await resp.text()
                    return json.loads(json_str)
                else:
                    raise Exception("Failed to post to {}, status {}".format(
                        self._url, resp.status))

        with async_timeout.timeout(timeout):
            logger.info("POST to %s, data size is %d.", self._url, len(data))
            if self._http_session is None:
                async with aiohttp.ClientSession() as session:
                    r = await _post(session)
            else:
                r = await _post(self._http_session)
            logger.info("Response of post request is %s.", r)

        return r
