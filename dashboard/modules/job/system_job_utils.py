import logging
import yaml
import requests

logger = logging.getLogger(__name__)


def get_field_from_json(j, field_name, default_value=None):
    if field_name not in j:
        return default_value
    return j[field_name]


async def load_cluster_config_from_url(ray_config_file_url):
    r = requests.get(ray_config_file_url, stream=True, timeout=(5, 5))
    if r.status_code != requests.codes.ok:
        logger.error(("Failed to load ray cluster config. URL is %s ."),
                     ray_config_file_url)
        r.raise_for_status()
    r.encoding = "utf-8"
    cluster_conf = yaml.load(r.text, Loader=yaml.FullLoader)
    r.close()
    return cluster_conf


# Statically definition of SystemJobConf.
class SystemJobConf:
    def __init__(self):
        self.job_id = None
        self.job_url = None
        self.job_name = None
        self.driver_entry = None
        self.total_memory_mb = None

    @staticmethod
    def create(job_id, job_url, job_name, driver_entry, total_memory_mb):
        conf = SystemJobConf()
        conf.job_id = job_id
        conf.job_url = job_url
        conf.job_name = job_name
        conf.driver_entry = driver_entry
        conf.total_memory_mb = total_memory_mb
        return conf

    def __repr__(self):
        d = {
            "job_id": self.job_id,
            "job_url": self.job_url,
            "job_name": self.job_name,
            "driver_entry": self.driver_entry,
            "total_memory_mb": self.total_memory_mb,
        }
        return str(d)
