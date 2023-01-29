from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import redis
import time
from redis.exceptions import (
    BusyLoadingError,
    ReadOnlyError,
    ResponseError,
    AuthenticationError,
    DataError,
)
from redis._compat import iteritems


class DefaultRetryPolicy(object):
    """Defines the policy of retry mechanism."""

    class RetryContext(object):
        current_retry_count = 0

    def __init__(self, max_retry_attempts=3000, delta_delay=0.1):
        self.max_retry_attempts = max_retry_attempts
        self.delta_delay = delta_delay

    def start_context(self):
        return self.RetryContext()

    def should_retry(self, retry_context):
        return retry_context.current_retry_count < self.max_retry_attempts

    def wait_before_retry(self, retry_context):
        time.sleep(self.delta_delay)
        retry_context.current_retry_count += 1


def execute_with_retry(operation, retry_policy, retry_on_timeout):
    """A helper function which implements retry logic"""
    retry_context = retry_policy.start_context()
    while True:
        try:
            return operation()
        except (ConnectionError, TimeoutError, BusyLoadingError, ReadOnlyError,
                ResponseError) as e:
            if isinstance(e, AuthenticationError):
                raise
            if not retry_on_timeout and isinstance(e, TimeoutError):
                raise
            if isinstance(e, ResponseError) and str(e) != "REPLICATION LAG":
                raise
            if retry_policy.should_retry(retry_context):
                retry_policy.wait_before_retry(retry_context)
            else:
                raise


class PubSubWithRetry(redis.client.PubSub):
    def __init__(self, retry_policy, connection_pool, *args, **kwargs):
        super(PubSubWithRetry, self).__init__(connection_pool, *args, **kwargs)
        self.retry_policy = retry_policy

    def _execute_internal(self, connection, command, *args):
        # PubSub.parse_response(...) doesn't check connection before reading
        # data from socket.
        # To support retry, we must add connection.connect() before
        # super()._execute(...) to make sure it's connected before reading
        # data from socket.
        connection.connect()
        return super(PubSubWithRetry, self)._execute(connection, command,
                                                     *args)

    def _execute(self, connection, command, *args, check_health=True):
        return execute_with_retry(
            lambda: self._execute_internal(connection, command, *args),
            self.retry_policy, connection.retry_on_timeout)


REDIS_MAX_COMMAND_SIZE = 1024 * 1024 * 10


class CustomConnection(redis.Connection):
    def send_packed_command(self, command, check_health=True):
        if isinstance(command, str):
            size = len(command)
        else:
            size = sum(map(len, command))
        if size > REDIS_MAX_COMMAND_SIZE:
            raise DataError(
                "Command to send is too large. Maximum allowed size: {}."
                " Actual size: {}.".format(REDIS_MAX_COMMAND_SIZE, size))
        super(CustomConnection, self).send_packed_command(
            command, check_health)


class RedisClientWithRetry(redis.StrictRedis):
    def __init__(self, retry_policy=DefaultRetryPolicy(), *args, **kwargs):
        super(RedisClientWithRetry, self).__init__(*args, **kwargs)
        self.retry_policy = retry_policy

        self.retry_on_timeout = kwargs.get("retry_on_timeout", False)
        self.connection_pool.connection_class = CustomConnection

    def execute_command(self, *args, **options):
        return execute_with_retry(
            lambda: super(RedisClientWithRetry, self)
            .execute_command(*args, **options), self.retry_policy,
            self.retry_on_timeout)

    def get(self, key):
        return self.execute_command("GET", key)

    def pubsub(self, **kwargs):
        return PubSubWithRetry(self.retry_policy, self.connection_pool,
                               **kwargs)

    def keys(self, pattern="*"):
        """Get all the keys according to one pattern from Redis.
        Args:
            key_pattern: The key matching pattern.
        Returns:
            The keys matching the pattern in Redis.
        """
        scan_iter_count = 5000
        return list(self.scan_iter(pattern, scan_iter_count))

    def hset(self, name, key=None, value=None, mapping=None):
        """ANT-INTERNAL: Convert `hset` calls to `hmset` calls.

        Due to the fact that TBase doesn't support HSET with multiple
        field/value pairs, we need to convert `hset` to `hmset` when the
        `mapping` argument is not None.
        """
        if mapping is None:
            return super(RedisClientWithRetry, self) \
                .hset(name=name, key=key, value=value, mapping=mapping)
        else:
            return self.hmset(name, mapping)

    def hmset(self, name, mapping):
        """ANT-INTERNAL: This is a copy of hmset from redis-py but with the
                warning message removed.
        """
        if not mapping:
            raise DataError("'hmset' with 'mapping' of length 0")
        items = []
        for pair in iteritems(mapping):
            items.extend(pair)
        return self.execute_command("HMSET", name, *items)


def patch_redis_client():
    """Replace the default redis client with a new redis client with retry
    mechanism implemented.
    Doesn't support retry for pipeline commands yet."""
    redis.StrictRedis = RedisClientWithRetry
