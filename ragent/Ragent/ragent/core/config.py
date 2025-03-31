import logging


class _RagentConfig:
    """Global configuration for ragent."""

    def __init__(self):
        self.log_level = logging.INFO

    def update_config(self, **kwargs):
        self.__dict__.update(kwargs)


ragent_config = _RagentConfig()


def set_log(level=logging.INFO, **kwargs):
    ragent_config.log_level = level
    logging.basicConfig(level=level, **kwargs)
