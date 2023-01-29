import logging

logger = logging.getLogger(__name__)


class BaseReaderActor(object):
    def __init__(self, class_name):
        self._class_name = class_name
        self._reader = None

    def on_reader_message(self, *buffers):
        if self._reader is None:
            return b" " * 4  # special flag to indicate this actor not ready
        self._reader.on_reader_message(*buffers)

    def on_reader_message_sync(self, buffer: bytes):
        print("========={} on_reader_message_sync========== size: {}".format(
            self._class_name, len(buffer)))
        if self._reader is None:
            print("BaseReaderActor reader is None!!")
            return b" " * 4  # special flag to indicate this actor not ready
        return self._reader.on_reader_message_sync(buffer)


class BaseWriterActor(object):
    def __init__(self, class_name):
        self._class_name = class_name
        self._writer = None

    def on_writer_message(self, buffer: bytes):
        if self._writer is None:
            return b" " * 4  # special flag to indicate this actor not ready
        assert self._writer is not None
        self._writer.on_writer_message(buffer)

    def on_writer_message_sync(self, buffer: bytes):
        print("========={} on_writer_message_sync========== size: {}".format(
            self._class_name, len(buffer)))
        if self._writer is None:
            print("BaseWriterActor writer is None!!")
            return b" " * 4  # special flag to indicate this actor not ready
        return self._writer.on_writer_message_sync(buffer)
