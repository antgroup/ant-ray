import logging
import os
from abc import ABC, abstractmethod
from os import path

from raystreaming.config import ConfigHelper
from raystreaming.constants import StreamingConstants
try:
    import zdfs
except Exception:
    print("import zdfs failed, please check module env.")

logger = logging.getLogger(__name__)


class StateBackend(ABC):
    @abstractmethod
    def init(self, conf):
        pass

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def put(self, key, value):
        pass

    @abstractmethod
    def remove(self, key):
        pass


class MemoryStateBackend(StateBackend):
    def __init__(self):
        self.__dic = dict()

    def init(self, conf):
        pass

    def get(self, key):
        return self.__dic.get(key)

    def put(self, key, value):
        self.__dic[key] = value

    def remove(self, key):
        if key in self.__dic:
            del self.__dic[key]


class FsStateBackend(StateBackend):
    """
    file system state backend
    """

    def __init__(self):
        self.__cluster_name = None
        self.__dir = None
        self.__fs = None
        self.__is_local = False

    def init(self, conf):
        logger.info("Checkpoint state init start.")
        if StreamingConstants.CP_STATE_BACKEND_PANGU == \
                ConfigHelper.get_cp_state_backend_type(conf):
            self.__cluster_name = ConfigHelper.get_cp_pangu_cluster_name(conf)
            self.__dir = ConfigHelper.get_cp_pangu_root_dir(conf)
            logger.info("cluster_name is {}, root dir is {}.".format(
                self.__cluster_name, self.__dir))
            if self.__dir.startswith("file://"):
                logger.info("use local disk.")
                self.__is_local = True
                self.__dir = self.__dir[7:]
                try:
                    os.mkdir(self.__dir)
                except FileExistsError:
                    logger.info("dir already exists, skipped.")
            else:
                logger.info("use pangu.")
                self.__cluster_name = self.__cluster_name[8:]
                from raystreaming.runtime.pangu_fs_lib import PanguFileSystem
                self.__fs = PanguFileSystem(self.__cluster_name)
                try:
                    logger.info("creating dir {}.".format(self.__dir))
                    self.__fs.mkdir(self.__dir)
                except FileExistsError:
                    logger.info("dir already exists, skipped.")

        logger.info("Checkpoint state init success.")

    def list_files(self):
        res = []
        if self.__is_local:
            res = [
                f for f in os.listdir(self.__dir)
                if path.isfile(self.__gen_file_path(f))
            ]
        elif self.__fs is not None:
            for stat in self.__fs.listdir(self.__dir):
                if not stat.is_dir():
                    res.append(stat.get_path())
        return res

    def rename(self, src, dst):
        logger.info("rename {} to {}".format(src, dst))
        if self.__is_local:
            os.rename(self.__gen_file_path(src), self.__gen_file_path(dst))
        elif self.__fs is not None:
            self.__fs.rename(
                self.__gen_file_path(src), self.__gen_file_path(dst))

    def get(self, key):
        logger.info("Get value of key {} start.".format(key))
        if self.__is_local:
            full_path = self.__gen_file_path(key)
            if not os.path.isfile(full_path):
                return None
            with open(full_path, "rb") as f:
                return f.read()
        elif self.__fs is not None:
            content = b""
            file_path = self.__gen_file_path(key)
            try:
                with self.__fs.open(file_path, self.__fs.FLAG_GENERIC_READ,
                                    0) as file:
                    for line in file:
                        content += line
            except IOError:
                logger.info(
                    "Get value of key {} failed, return null.".format(key))
                return None
            logger.info("Get value of key {} success.".format(key))
            return content

    def put(self, key, value):
        logger.info("Put value of key {} start.".format(key))
        if self.__is_local:
            with open(self.__gen_file_path(key), "wb") as f:
                f.write(value)
        elif self.__fs is not None:
            file_path = self.__gen_file_path(key)
            try:
                logger.info("Try to remove value of key: {}.".format(key))
                self._remove(key)
            except FileNotFoundError:
                pass
            try:
                self.__fs.create(file_path, 0o755)
            except FileExistsError:
                logger.warn("File {} existed".format(file_path))
            file = self.__fs.open(file_path, self.__fs.FLAG_SEQUENTIAL_WRITE,
                                  0)
            file.append(value)
            file.close()
            logger.info("Put value of key {} success.".format(key))

    def _remove(self, key):
        logger.info("Remove value of key {} start.".format(key))
        if self.__is_local:
            os.remove(self.__gen_file_path(key))
        elif self.__fs is not None:
            file_path = self.__gen_file_path(key)
            self.__fs.remove(file_path)
            logger.info("Remove value of key {} success.".format(key))

    def remove(self, key):
        self._remove(key)

    def _exists(self, key):
        if self.__is_local:
            return path.isfile(self.__gen_file_path(key))
        elif self.__fs is not None:
            file_path = self.__gen_file_path(key)
            # Note(lingxuan.zlx): query file exists or not by using file system
            # stat function since there is no such function in pangu fs lib.
            try:
                self.__fs.stat(file_path)
            except FileNotFoundError:
                return False
            return True
        return False

    def exists(self, key):
        return self._exists(key)

    def __gen_file_path(self, key):
        return path.join(self.__dir, key)


TMP_FLAG = "_tmp"
VERSION_FLAG = "_"


class SafeFsBackend(StateBackend):
    def __init__(self, instance):
        self._instance = instance

    def init(self, conf):
        self._instance.init(conf)

    def get(self, key):
        tmp_key = key + TMP_FLAG
        if self._instance.exists(tmp_key) and not self._instance.exists(key):
            return self._instance.get(tmp_key)
        if self._instance.exists(key):
            return self._instance.get(key)
        return None

    def put(self, key, value):
        tmp_key = key + TMP_FLAG
        # To workadound pangu rename error exception or go ahead without
        # failed checkpoint
        try:
            if (self._instance.exists(tmp_key)
                    and not self._instance.exists(key)):
                self._instance.rename(tmp_key, key)
        except BaseException:
            logger.warn("Rename tmp file exception.")
        self._instance.put(tmp_key, value)
        try:
            self._instance.remove(key)
        except FileNotFoundError:
            pass
        self._instance.rename(tmp_key, key)

    def remove(self, key):
        logger.info("SafeFsBackend remove value of key {}.".format(key))
        self._instance.remove(key)


class DfsStateBackend(StateBackend):
    """
    Distributed file system state backend
    """

    def __init__(self):
        self.__cluster_name = None
        self.__dir = None
        self.__fs = None
        self.__options = None

    def init(self, conf):
        logger.info("Checkpoint dfs state init start.")
        if StreamingConstants.CP_STATE_BACKEND_DFS == \
                ConfigHelper.get_cp_state_backend_type(conf):
            self.__cluster_name = ConfigHelper.get_cp_dfs_cluster_name(conf)
            self.__dir = ConfigHelper.get_cp_dfs_root_dir(conf)
            logger.info("cluster_name is {}, root dir is {}.".format(
                self.__cluster_name, self.__dir))
            logger.info("use dfs.")
            self.__options = zdfs.FileSystemOptions()
            pangu_options = zdfs.PanguOptions()
            # Thread num in hard code.
            pangu_options.io_thread_num = 2
            pangu_options.callback_thread_num = 2
            pangu_options.callback_in_iothread = True
            pangu_options.log_level = zdfs.LogLevel_ERROR
            zdfs.PanguFileSystem.SetOptions(pangu_options)

            self.__fs = zdfs.PanguFileSystem.Create(self.__cluster_name,
                                                    self.__options)
            try:
                logger.info("creating dir {}.".format(self.__dir))
                ec = self.__fs.CreateDirectory(self.__dir,
                                               zdfs.CreateDirectoryOptions())
                if ec != 0:
                    logger.info("create directory {}.".format(ec))
            except FileExistsError:
                logger.info("dir already exists, skipped.")

        logger.info("Checkpoint state init success.")

    def list_files(self):
        res = []
        if self.__fs is not None:
            options_list_dir = zdfs.ListDirectoryOptions()
            options_list_dir.full_stat = False
            ec, entries, entry_stats, has_more = self.__fs.ListDirectory(
                self.__dir, options_list_dir)
            res = list(
                filter(lambda entry: len(entry) > 1 and entry[-1] != "/",
                       entries))
        return res

    def rename(self, src, dst):
        logger.info("rename {} to {}".format(src, dst))
        if self.__fs is not None:
            ec = self.__fs.Rename(
                self.__gen_file_path(src), self.__gen_file_path(dst),
                zdfs.RenameOptions())
            if ec != 0:
                logger.info("Rename file error code : {}.".format(ec))

    def get(self, key):
        logger.info("Get value of key {} start.".format(key))
        if self.__fs is not None:
            content = b""
            file_path = self.__gen_file_path(key)
            ec, file = self.__fs.OpenFile(file_path, zdfs.OpenMode_READ_ONLY,
                                          zdfs.OpenFileOptions())
            if ec != 0:
                logger.info(f"Get file {file_path} error {ec}")
                return content
            ec, stat = self.__fs.Stat(file_path, zdfs.StatOptions())
            read_count = 0
            while read_count < stat.file.length:
                ec, data, read_len = file.Read(stat.file.length - read_count,
                                               zdfs.FileReadOptions())
                content += data
                read_count += read_len
                if ec != 0:
                    logger.warning(f"Read failed, error code {ec}")
                    break
            ec = file.Close(zdfs.FileCloseOptions())
            logger.info("Get value of key {}, length {} success.".format(
                key, stat.file.length))
        return content

    def put(self, key, value):
        logger.info("Put value of key {} start.".format(key))
        if self.__fs is not None:
            file_path = self.__gen_file_path(key)
            try:
                logger.info("Try to remove value of key: {}.".format(key))
                self.remove(key)
            except FileNotFoundError:
                pass
            ec = self.__fs.CreateFile(file_path, zdfs.CreateFileOptions())
            ec, file = self.__fs.OpenFile(file_path, zdfs.OpenMode_WRITE_ONLY,
                                          zdfs.OpenFileOptions())
            if ec != 0:
                logger.info(f"Get file {file_path} error {ec}")
            ec, offset = file.Append(value, zdfs.FileAppendOptions())
            ec, length = file.Flush(zdfs.FileFlushOptions())
            ec = file.Close(zdfs.FileCloseOptions())
            logger.info(
                "Put value of key {}, offset {}, length {} success.".format(
                    key, offset, length))

    def _remove(self, key):
        logger.info("Remove value of key {} start.".format(key))
        if self.__fs is not None:
            file_path = self.__gen_file_path(key)
            ec = self.__fs.Delete(file_path, zdfs.DeleteOptions())
            if ec == 0:
                logger.info("Remove value of key {} success.".format(key))
            else:
                logger.warn(
                    "Remove value of key {} failed, error code {}.".format(
                        key, ec))

    def remove(self, key):
        if self.exists(key):
            self._remove(key)

    def _exists(self, key):
        if self.__fs is not None:
            file_path = self.__gen_file_path(key)
            ec, stat = self.__fs.Stat(file_path, zdfs.StatOptions())
            return stat.path != ""
        return False

    def exists(self, key):
        return self._exists(key)

    def __gen_file_path(self, key):
        return path.join(self.__dir, key)


class StateBackendFactory:
    @staticmethod
    def get_state_backend(backend_type) -> StateBackend:
        state_backend = None
        if backend_type == StreamingConstants.CP_STATE_BACKEND_PANGU:
            state_backend = SafeFsBackend(FsStateBackend())
        elif backend_type == StreamingConstants.CP_STATE_BACKEND_DFS:
            state_backend = SafeFsBackend(DfsStateBackend())
        elif backend_type == StreamingConstants.CP_STATE_BACKEND_MEMORY:
            state_backend = MemoryStateBackend()
        return state_backend
