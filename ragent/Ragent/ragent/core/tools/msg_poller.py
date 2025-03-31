import importlib
import queue
import threading
import time
import logging

logger = logging.getLogger(__name__)

# NOTE(NKcqx): The symbol to let the polling thread inside message queue to stop.
# Because in python, the recommended way to stop a sub-thread is to set a flag
# that checked by the sub-thread itself.(see https://stackoverflow.com/a/325528).
STOP_SYMBOL = False


class MessagePoller:
    def __init__(
        self,
        msg_handler,
        msg_queue=None,
        failure_handler=None,
        thread_name="",
        poll_interval_s=0.01,
    ):
        assert callable(msg_handler), "msg_handler must be a callable function"
        # `deque()` is thread safe on `popleft` and `append` operations.
        # See https://docs.python.org/3/library/collections.html#deque-objects
        # NOTE(paer): different Queue has different interface, e.g. queue.Queue has
        # not `append`
        self._msg_queue = msg_queue if msg_queue is not None else queue.Queue()
        queue_module_name = type(msg_queue).__module__
        queue_module = importlib.import_module(queue_module_name)
        self._empty_exception_type = getattr(queue_module, "Empty", queue.Empty)
        self._msg_handler = msg_handler
        self._failure_handler = failure_handler
        self._thread = None
        # Assign a name to the thread to better distinguish
        self._thread_name = thread_name
        self._poll_interval_s = poll_interval_s

    def start(self):
        def _loop():
            while True:
                try:
                    message = self._msg_queue.get(
                        block=True, timeout=self._poll_interval_s
                    )
                    logger.debug(f"[{self._thread_name}] Polled message: {message}")
                except self._empty_exception_type:
                    continue
                except Exception as e:
                    logger.error(
                        f"Try poll message after {self._poll_interval_s}, got {e}."
                    )
                    continue

                if message == STOP_SYMBOL:
                    break

                # Execute just in the current polling thread
                res = self._msg_handler(message)
                if not res:
                    break

        if self._thread is None or not self._thread.is_alive():
            logger.debug(
                f"Starting new thread[{self._thread_name}] for message polling."
            )
            self._thread = threading.Thread(target=_loop, name=self._thread_name)
            self._thread.start()

    def append(self, message):
        self._msg_queue.put(message, block=True, timeout=self._poll_interval_s)

    def _notify_to_exit(self):
        logger.info(f"Notify message polling thread[{self._thread_name}] to exit.")
        self.append(STOP_SYMBOL)

    def stop(self):
        """
        Stop the message queue.

        Args:
            graceful (bool): A flag indicating whether to stop the queue
                    gracefully or not. Default is True.
                If True: insert the STOP_SYMBOL at the end of the queue
                    and wait for it to be processed, which will break the for-loop;
                If False: forcelly kill the for-loop sub-thread.
        """
        if threading.current_thread() == self._thread:
            logger.error(
                f"Can't stop the message queue in the message "
                f"polling thread[{self._thread_name}]. Ignore it as this"
                f"could bring unknown time sequence problems."
            )
            raise RuntimeError("Thread can't kill itself")

        # TODO(NKcqx): Force kill sub-thread by calling `._stop()` will
        # encounter AssertionError because sub-thread's lock is not released.
        # Therefore, currently, not support forcelly kill thread
        if self.is_started():
            logger.debug(f"Gracefully killing thread[{self._thread_name}].")
            self._notify_to_exit()
            self._thread.join()

        logger.info(f"The message polling thread[{self._thread_name}] was exited.")

    def is_started(self):
        return self._thread is not None and self._thread.is_alive()
