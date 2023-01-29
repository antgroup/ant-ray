import logging
from raystreaming.utils import read_control_message
from raystreaming.runtime.udc_adapter import \
    UnitedDistributedControllerActorAdapter as UDCAdapter
from raystreaming.config import Config
import ray
import json

logger = logging.getLogger(__name__)


class UnitedDistributedControllerAdapterCallee(UDCAdapter):
    def __init__(self, config):
        if config is None:
            logger.warning("UDC callee can't get config from sub class,"
                           "so UDC will not be enabled.")
        else:
            self._conf_dict = json.loads(config)
            self.__job_name = self._conf_dict[Config.STREAMING_JOB_NAME_ALIAS]
            # There is a default agreement that the actor
            # name of job master is named by job name.
            self.job_master = ray.get_actor(self.__job_name)

    def set_master_actor(self, master_actor):
        self.job_master = master_actor

    def get_master_actor(self):
        return self.job_master

    @read_control_message
    def udc_prepare(self, udc_msg):
        return self.on_prepare(udc_msg)

    def udc_commit(self):
        self.on_commit()

    def udc_disposed(self):
        return self.on_disposed()

    def udc_cancel(self):
        return self.on_cancel()

    # united distributed controller function start.

    def on_prepare(self, msg):
        logger.warning("This UDC function `on_prepare` is undefined.")
        return True

    def on_commit(self):
        logger.warning("This UDC function `on_commit` is undefined.")

    def on_disposed(self):
        logger.warning("This UDC function `on_disposed` is undefined.")
        return True

    def on_cancel(self):
        logger.warning("This UDC function `on_cancel` is undefined.")
        return True

    # united distributed controller function end.
