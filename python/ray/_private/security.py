import logging
import yaml
import re
from ray.ray_constants import SECURITY_CONFIG_PATH

logger = logging.getLogger(__name__)


class RemoteFunctionWhitelist:
    module_whitelist = None

    @classmethod
    def whitelist_init(cls):
        if SECURITY_CONFIG_PATH:
            cls.module_whitelist = yaml.safe_load(open(SECURITY_CONFIG_PATH, "rt")).get(
                "remote_function_whitelist", None
            )
            if cls.module_whitelist:
                logger.info(
                    "The remote function module whitelist have been set. It means that"
                    " if your remote functions or actor classes are not included in "
                    "the whitelist, your remote tasks or actors will fail. You can "
                    "modify the environment variable %s to change the whitelist or "
                    'disable this functionality by "unset %s" directly. The whitelist'
                    " is %s.",
                    SECURITY_CONFIG_PATH,
                    SECURITY_CONFIG_PATH,
                    cls.module_whitelist,
                )

    @classmethod
    def whitelist_check(cls, function_descriptor):
        if not cls.module_whitelist:
            return
        else:
            for module in cls.module_whitelist:
                if re.match(module, function_descriptor.module_name):
                    return
            raise KeyError(
                "Remote function module whitelist check failed "
                f"for {function_descriptor}"
            )


RemoteFunctionWhitelist.whitelist_init()
