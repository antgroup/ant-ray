import logging
import math
import ray
import sys
import time
from queue import Queue
from threading import Thread, Lock, Condition

logger = logging.getLogger(__name__)

# Actor default memory is 500 Mb
_ACTOR_DEFAULT_MEMORY_BYTES = 500 * 1024 * 1024


class BaseExecutor(object):
    def __check_health__(self):
        """Used to check Executor health."""
        return True


class DynamicActorPool:
    """Utility class to operate on a dynamic pool of actors which can construct
    actors on demand.

    Arguments:
        actor_class: a actor class which extends BaseExecutor.
        actor_args: actor class construction args which is a set.
        actor_kwargs: actor class constructions kwargs which is a dict.
        init_total_memory_mb (int): initial total memory in mega byte which
            should be a multiple of 50 and guarantee enough for core actor
            number. If set None, actor pool will create actors asynchronously.
        actor_num (int): maximum actor number of actor pool.
        actor_creation_batch (int): If init_total_memory_mb is set to None,
            this argument will control actor creation number each time.
        retry_update_num (int): the number of attempts to update job total
            memory. It will not try to update after retry retry_update_num
            times.
        options: actor creation options.
    """

    def __init__(self,
                 actor_class,
                 actor_args=(),
                 actor_kwargs={},
                 init_total_memory_mb: int = None,
                 actor_num: int = 1,
                 actor_creation_batch=1,
                 retry_update_num=sys.maxsize,
                 **options):
        self._check_params(actor_class, actor_num, actor_creation_batch,
                           retry_update_num)
        self._actor_class = actor_class
        self._actor_args = actor_args
        self._actor_kwargs = actor_kwargs
        self._total_memory_mb = init_total_memory_mb
        self._actor_num = actor_num
        self._actor_creation_batch = actor_creation_batch
        self._retry_update_num = retry_update_num
        self._options = options
        self._actor_memory_mb = math.ceil(
            self._options.get("memory", _ACTOR_DEFAULT_MEMORY_BYTES) / 1024 /
            1024)
        self._actor_creation_order = 0
        self._created_actor_count = 0
        self._idle_actors = Queue()
        self._pending_submits = Queue()
        self._lock = Lock()
        self._condition = Condition()
        self._running_futures = []
        self._running_future_to_actor = {}
        self._future_to_index = {}
        self._index_to_future = {}
        self._next_task_index = 0
        self._next_return_index = 0
        self.__customized_name = options.get("name", None)
        self.__add_more_available_actors()
        self.__schedule_pending_submits()
        self.__check_submits_completed()
        logger.info(
            "Finished initialize actor pool with actor_class: %s, "
            "actor_args: %s, actor_kwargs: %s, init_total_memory_mb: %sMB, "
            "actor_num: %d, actor_memory_mb: %dMB, options: %s",
            self._actor_class.__class__.__name__, self._actor_args,
            self._actor_kwargs, init_total_memory_mb, self._actor_num,
            self._actor_memory_mb, options)

    def _check_params(self, actor_class, actor_num, actor_creation_batch,
                      retry_update_num):
        assert hasattr(
            actor_class,
            "__ray_actor_class__"), "Should add @ray.remote to {}".format(
                actor_class.__name__)
        assert issubclass(
            actor_class.__ray_actor_class__,
            BaseExecutor), "{} is not subclass of BaseExecutor".format(
                actor_class.__ray_actor_class__.__name__)
        assert actor_num > 0, "actor_num must be great than 0"
        assert actor_creation_batch > 0, \
            "actor_creation_batch must be great than 0"
        assert retry_update_num >= 0, \
            "retry_update_num should not a be negative number"

    def __try_keep_update_total_memory(self,
                                       total_memory_mb=None,
                                       retry_num=100,
                                       retry_interval_secs=1):
        i = 0
        while (i < retry_num):
            try:
                ray.update_job_total_resources(total_memory_mb=total_memory_mb)
                logger.info("Updated total memory to %dMB.", total_memory_mb)
                return True
            except Exception:
                i += 1
                if (i % 100) == 0:
                    logger.warn(
                        "Failed to update job total resources as the "
                        "namespace lacks of enough resources after %d "
                        "times retry.", i)
                time.sleep(i * retry_interval_secs)
        return False

    def __add_more_available_actors(self):
        self._initialized_t = Thread(
            target=self.__try_add_available_actor,
            name="init-actors-thread",
            daemon=True)
        self._initialized_t.start()

    def __try_add_available_actor(self):
        if self._total_memory_mb is None:
            self.__create_actor_in_batch(self._actor_num)
        else:
            self.__create_actor_step_on_step()

    def __create_actor_in_batch(self, batch_size):
        initial_actors = []
        for i in range(0, batch_size):
            if self.__customized_name:
                self._options["name"] = self.__gen_actor_name(
                    self.__customized_name, self._actor_creation_order)
                self._actor_creation_order += 1
            initial_actors.append(
                self._actor_class.options(**self._options).remote(
                    *self._actor_args, **self._actor_kwargs))
        futures = []
        future_to_actor = {}
        for actor in initial_actors:
            f = actor.__check_health__.remote()
            futures.append(f)
            future_to_actor[f] = actor
        while len(futures) > 0:
            res, _ = ray.wait(futures, num_returns=len(futures), timeout=0.5)
            if res:
                self._created_actor_count += len(res)
                logger.info("Created %d actors", self._created_actor_count)
            for f in res:
                a = future_to_actor[f]
                self._idle_actors.put(a)
                del future_to_actor[f]
                futures.remove(f)
                initial_actors.remove(a)

    def __create_actor_step_on_step(self):
        count, extra = divmod(self._actor_num, self._actor_creation_batch)
        for i in range(0, count):
            self._total_memory_mb += \
                self._actor_memory_mb * self._actor_creation_batch
            if self.__try_keep_update_total_memory(self._total_memory_mb,
                                                   self._retry_update_num):
                self.__create_actor_in_batch(self._actor_creation_batch)
            else:
                logger.info(
                    "Failed to update job total resources as the "
                    "namespace lacks of enough resources after %d "
                    "times retry and do not try to update again.",
                    self._retry_update_num)
                return
        if extra > 0:
            self._total_memory_mb += self._actor_memory_mb * extra
            if self.__try_keep_update_total_memory(self._total_memory_mb,
                                                   self._retry_update_num):
                self.__create_actor_in_batch(extra)

    def __schedule_pending_submits(self):
        self._scheduler_t = Thread(
            target=self.__run_func, name="schedule-thread", daemon=True)
        self._scheduler_t.start()

    def __run_func(self):
        while True:
            actor = self._idle_actors.get()
            fn, value = self._pending_submits.get()
            future = fn(actor, value)
            with self._lock:
                self._running_futures.append(future)
                self._running_future_to_actor[future] = actor
                self._future_to_index[future] = self._next_task_index
                self._index_to_future[self._next_task_index] = future
                self._next_task_index += 1
            with self._condition:
                self._condition.notify_all()

    def __check_submits_completed(self):
        self._check_completed_t = Thread(
            target=self.__check_completed,
            name="check-completed-thread",
            daemon=True)
        self._check_completed_t.start()

    def __check_completed(self):
        while True:
            if self._running_futures:
                futures = list(self._running_futures)
                res, _ = ray.wait(
                    futures, num_returns=len(futures), timeout=0.1)
                for f in res:
                    a = self._running_future_to_actor[f]
                    with self._lock:
                        self._idle_actors.put(a)
                        self._running_futures.remove(f)
                        self._running_future_to_actor.pop(f)
            else:
                with self._condition:
                    self._condition.wait()

    def __check_actor_health(self, actor):
        future = actor.__check_health__.remote()
        try:
            ray.get(future)
        except Exception:
            logger.exception("Actor %s is not ready.", actor._actor_id)
            return False
        return True

    def __gen_actor_name(self, name, i):
        """ Generate an actor name.

        TODO:
            Adding a timestamp as actor name suffix because name uniqueness
            is not guaranteed in python ray. We will remove the suffix if ray
            provides a default prefix like job id in the future.
        """
        return f"{name}_{i}_{int(time.time())}"

    @property
    def total_memory_mb(self):
        return self._total_memory_mb

    def is_initial_finished(self):
        return not self._initialized_t.is_alive()

    def map(self, fn, values):
        for v in values:
            self.submit(fn, v)
        while self.has_next():
            yield self.get_next()

    def map_unordered(self, fn, values):
        for v in values:
            self.submit(fn, v)
        while self.has_next():
            yield self.get_next_unordered()

    def submit(self, fn, value):
        self._pending_submits.put((fn, value))

    def has_next(self):
        return bool(self._future_to_index) or not self._pending_submits.empty()

    def get_next(self, timeout=None):
        if not self.has_next():
            raise StopIteration("No more results to get")

        if not bool(
                self._future_to_index) and not self._pending_submits.empty():
            with self._condition:
                self._condition.wait()

        if self._next_return_index >= self._next_task_index:
            raise ValueError("It is not allowed to call get_next() after "
                             "get_next_unordered().")
        future = self._index_to_future[self._next_return_index]
        if timeout is not None:
            res, _ = ray.wait([future], timeout=timeout)
            if not res:
                raise TimeoutError("Timed out waiting for result")
        del self._index_to_future[self._next_return_index]
        self._next_return_index += 1
        self._future_to_index.pop(future)
        return ray.get(future)

    def get_next_unordered(self, timeout=None):
        if not self.has_next():
            raise StopIteration("No more results to get")

        if not bool(
                self._future_to_index) and not self._pending_submits.empty():
            with self._condition:
                self._condition.wait()

        res, _ = ray.wait(
            list(self._future_to_index), num_returns=1, timeout=timeout)
        if res:
            [future] = res
        else:
            raise TimeoutError("Timed out waiting for result")
        i = self._future_to_index.pop(future)
        del self._index_to_future[i]
        self._next_return_index = max(self._next_return_index, i + 1)
        return ray.get(future)
