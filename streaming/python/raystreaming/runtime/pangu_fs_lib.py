# flake8: noqa
"""
This library is to operate on the pangu filesystem based on the pangu-client driver
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import atexit
import errno
import os
import sys
import threading
import time
from ctypes import Structure
from ctypes import byref
from ctypes import c_byte
from ctypes import c_char_p
from ctypes import c_int, c_uint
from ctypes import c_long, c_ulong
from ctypes import c_ushort
from ctypes import c_void_p
from ctypes import cdll

try:
    pangu_api = cdll.LoadLibrary('libpangu_api.so')
except Exception as e:
    print("please install RPM pangu-client")
    # sys.exit(1)

# track the init times for pangu filesystem
pangu_fs_init_times = 0
pangu_fs_init_lock = threading.Lock()


def pangu_cleanup():
    with pangu_fs_init_lock:
        global pangu_fs_init_times
        if pangu_fs_init_times > 0:
            for n in range(0, pangu_fs_init_times):
                pangu_api.pangu_uninit()
            pangu_fs_init_times = 0
            print('auto close pangu filesystem')


# add sys exit clean shutdown
atexit.register(pangu_cleanup)

if sys.version_info < (3, 0):

    class PanguException(IOError):
        def __init__(self, msg):
            super(IOError, self).__init__()
            self._msg = msg

        def __str__(self):
            return self._msg

    class PermissionError(PanguException):
        def __init__(self, msg):
            super(PanguException, self).__init__(msg)

    class FileNotFoundError(PanguException):
        def __init__(self, msg):
            super(PanguException, self).__init__(msg)

    class FileExistsError(PanguException):
        def __init__(self, msg):
            super(PanguException, self).__init__(msg)


### end of exception definition

PANGU_SCHEME = "pangu://"


class PanguFileMeta(Structure):
    _fields_ = [("file_length", c_long), ("is_dir", c_int), ("copys", c_int),
                ("create_time", c_long), ("modified_time", c_long), ("file_id",
                                                                     c_long),
                ("hardlinks", c_int), ("file_flag", c_int), ("file_attr",
                                                             c_byte),
                ("access", c_ushort), ("owner", c_uint), ("group", c_uint)]


class PanguDirMeta(Structure):
    _fields_ = [("dir_count", c_ulong), ("file_count", c_ulong),
                ("space_size", c_ulong), ("space_quota",
                                          c_long), ("files_quota", c_long)]


class FileStatus(object):
    """
    Pangu File Status
    """
    UNIX_PERM_SYMBOLS = [
        '---', '--x', '-w-', '-wx', 'r--', 'r-x', 'rw-', 'rwx'
    ]

    def __init__(self, path, length, is_dir, copys, block_size, \
                 mtime, atime, access, owner, group):
        self.path = path
        self.length = length
        self.isdir = is_dir
        self.block_replication = copys
        self.blocksize = block_size
        self.modification_time = mtime
        self.access_time = atime
        self.permission = oct(access)
        self.owner = owner
        self.group = group
        self.symlink = None

    def __str__(self):
        is_dir = "false"
        if self.isdir:
            is_dir = "true"
        mtime_str = time.strftime("%Y-%m-%d %H:%M:%S",
                                  time.localtime(self.modification_time))
        atime_str = time.strftime("%Y-%m-%d %H:%M:%S",
                                  time.localtime(self.access_time))
        if self.owner:
            out = "path=%s,length=%s,dir=%s,mtime=%s,atime=%s,owner=%s,group=%s,perm=%s" % \
                  (self.path, self.length, is_dir, mtime_str, atime_str, self.owner, self.group, self.permission)
        else:
            out = "path=%s,length=%s,dir=%s,mtime=%s,atime=%s" % \
                  (self.path, self.length, is_dir, mtime_str, atime_str)
        return out

    @classmethod
    def permToUnixSymbol(cls, perm):
        perm = str(perm)
        allSymbol = ""
        if len(perm) == 5:
            perm = perm[2:]
        elif perm == "0":
            perm = "000"
        elif perm == "0o0":
            perm = "000"
        for i in range(0, len(perm)):
            allSymbol = allSymbol + cls.UNIX_PERM_SYMBOLS[int(perm[i])]
        return allSymbol

    def pretty(self):
        perm_symbol = "%s%s" % (
            ('d'
             if self.isdir else '-'), self.permToUnixSymbol(self.permission))
        mtime_str = time.strftime("%Y-%m-%d %H:%M:%S",
                                  time.localtime(self.modification_time))
        out = "%s  %d %-12s %-12s %-10d %s  %s" % (perm_symbol, 1, self.owner,
                                                   self.group, self.length,
                                                   mtime_str, self.path)
        return out

    def is_dir(self):
        return self.isdir

    def get_path(self):
        return self.path

    def mtime(self):
        return self.mtime

    def atime(self):
        return self.atime


class ContentSummary(object):
    """
    Pangu Dir Content Summary
    """

    def __init__(self, length, file_count, dir_count, quota, space_consumed,
                 space_quota):
        self.length = length
        self.file_count = file_count
        self.dir_count = dir_count
        self.quota = quota
        self.space_consumed = space_consumed
        self.space_quota = space_quota

    def __str__(self):
        out = "length=%d,files=%d,dirs=%d,used=%d,quota=%d,space_quota=%d" % \
              (self.length, self.file_count, self.dir_count, self.space_consumed, self.quota, self.space_quota)
        return out

    def length(self):
        return self.length

    def file_count(self):
        return self.file_count

    def dir_count(self):
        return self.dir_count

    def quota(self):
        return self.quota

    def used(self):
        return self.space_consumed

    def space_quota(self):
        return self.space_quota


class PanguFile(object):
    """
    Pangu file class
    """
    # Seek from beginning of file
    SEEK_SET = 0
    # Seek from current position.
    SEEK_CUR = 1

    def __init__(self, handle, flag, path, buffer_size=8388608):
        self._handle = handle
        self._flag = flag
        self._path = path
        self._offset = 0
        self._closed = False
        self._buffer = None
        self._buf_head = 0
        self._data_len = 0
        self._buf_size = buffer_size

    def __enter__(self):
        return self

    def __exit__(self, type=None, value=None, trace=None):
        self.close()

    def __del__(self):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        line = self.readline()
        if line:
            return line
        else:
            raise StopIteration

    def close(self):
        if not self._closed:
            self._closed = True
            pangu_api.pangu_close(self._handle)

    def append(self, data):
        if self._closed:
            raise Exception("%s already closed" % self._path)
        r = pangu_api.pangu_write(self._handle, c_char_p(data), c_int(
            len(data)))
        if r != len(data):
            PanguFileSystem._to_exception(r, self._path)
        return r

    def fsync(self):
        if self._closed:
            raise Exception("%s already closed" % self._path)
        r = pangu_api.pangu_fsync(self._handle)
        if r != 0:
            PanguFileSystem._to_exception(r, self._path)
        return r

    def read(self, size):
        data, size = self.pread(self._offset, size)
        self._offset += size
        return data, size

    def pread(self, position, size):
        if self._closed:
            raise Exception("%s already closed" % self._path)
        pangu_api.pangu_lseek.restype = c_long
        r = pangu_api.pangu_lseek(self._handle, c_long(position),
                                  c_int(self.SEEK_SET))
        if r != position:
            PanguFileSystem._to_exception(r, self._path)
        cdata = (c_byte * (size + 1))()
        e = pangu_api.pangu_read(self._handle, cdata, c_int(size))
        if e < 0:
            PanguFileSystem._to_exception(e, self._path)
        return bytearray(cdata)[:e], e

    def length(self):
        meta = PanguFileMeta()
        r = pangu_api.pangu_get_status(c_char_p(self._path), byref(meta))
        if r != 0:
            self._to_exception(r, self._path)
        return meta.file_length

    def readline(self):
        line = b""
        nbytes = 1
        while nbytes > 0:
            if self._offset >= self._buf_head + self._data_len or \
                    self._offset < self._buf_head:
                nbytes = self._fillbuffer()
            else:
                nbytes = self._buf_head + self._data_len - self._offset
            if nbytes > 0:
                buf_off = self._offset - self._buf_head
                pos = str(self._buffer).find('\n', buf_off)
                if pos >= 0:
                    line = self._buffer[buf_off:pos + 1]
                    self._offset = self._buf_head + pos + 1
                    return line
                else:
                    line += self._buffer[buf_off:]
                    self._offset += nbytes
        return line

    def _fillbuffer(self):
        self._buffer, size = self.pread(self._offset, self._buf_size)
        if size > 0:
            self._buf_head = self._offset
            self._data_len = size
            return size
        return -1

    def readlines(self, size=0):
        if size > 0:
            lines = []
            total = 0
            while total < size:
                line = self.readline()
                if len(line) == 0:
                    break
                total += len(line)
                lines.append(line.rstrip('\n'))
            return lines
        else:
            all_data, _ = self.read(self.length())
            return all_data.splitlines()


class PanguFileSystem(object):
    """
    Class for accessing Pangu via pangu API
    """
    PANGU_BLOCK_SIZE = 1024 * 1024 * 64
    # file types
    FILE_TYPE_NORMAL = 0
    FILE_TYPE_LOGFILE = 2
    FILE_TYPE_RAIDFILE = 3
    # open flags
    FLAG_GENERIC_READ = 0x1
    FLAG_SEQUENTIAL_READ = 0x4
    FLAG_SEQUENTIAL_WRITE = 0x8

    def __init__(self, cluster, username=None, conf={}):
        self._fsname = "pangu://%s" % cluster
        self._capability = None
        self._verbose = False
        for key in conf.keys():
            if key.startswith("fs.pangu.flag."):
                flag = key[len("fs.pangu.flag."):]
                self.set_flag(flag, conf[key])
            elif key == "fs.pangu.capability":
                self._capability = conf[key].strip()
            elif key == "fs.pangu.verbose" and conf[key].lower() in [
                    'on', 'true'
            ]:
                self._verbose = True
        if self._capability:
            pangu_api.pangu_set_capability(
                c_char_p(self._capability), c_int(len(self._capability)),
                c_int(0))
        e = pangu_api.pangu_init(c_char_p(self._fsname.encode('utf-8')), 0)
        if e != 0:
            raise Exception("init pangu env failure, errno=%d" % e)
        with pangu_fs_init_lock:
            global pangu_fs_init_times
            pangu_fs_init_times += 1
        uid, gid = self._get_default_uid_gid()
        pangu_api.pangu_set_user_group(uid, gid)

    def __enter__(self):
        return self

    def __exit__(self, type=None, value=None, trace=None):
        with pangu_fs_init_lock:
            global pangu_fs_init_times
            if pangu_fs_init_times <= 0:
                return
            pangu_fs_init_times -= 1
        pangu_api.pangu_uninit()

    def __del__(self):
        self.__exit__()

    def _get_default_uid_gid(self):
        """
        get os uid/gid with enviroment
        """
        uid = os.getuid()
        gid = os.getgid()
        return uid, gid

    def _make_path(self, path, fix_dir=False):
        if fix_dir and not path.endswith("/"):
            return "%s%s/" % (self._fsname, path)
        else:
            return "%s%s" % (self._fsname, path)

    @classmethod
    def _to_exception(cls, err, path):
        if err < 0:
            err = -err
        if err == errno.EPERM or err == errno.EACCES:
            raise PermissionError("%s no permission" % path)
        elif err == errno.ENOENT:
            raise FileNotFoundError("%s not found" % path)
        elif err == errno.EEXIST:
            raise FileExistsError("%s existed" % path)
        elif err == errno.EINVAL:
            raise OSError("%s, Invalid Arguement" % path)
        elif err == errno.ENOSPC:
            raise IOError("%s, No Space" % path)
        elif err == errno.EDQUOT:
            raise IOError("%s, Quota Exceed" % path)
        elif err == errno.EBUSY:
            raise IOError("%s, Busy" % path)
        elif err == errno.ENOTEMPTY:
            raise OSError("%s, Dir not Empty" % path)
        elif err == errno.EBADF:
            raise IOError("%s, Bad Descriptor" % path)
        elif err == errno.EIO:
            raise IOError("%s, IO Error" % path)
        else:
            raise Exception("%s, Unknown Error %d" % (path, err))

    def _print_verbose(self, msg):
        if self._verbose:
            print(msg)

    def create(self, path, mode, overwrite=False, copys=3, ftt=1, options={}):
        """
        create a file, ftt means failure copys to tolerant
        """
        app_name = "BIGFILE_APPNAME"
        if 'appname' in options:
            app_name = options['appname']
        part_name = "BIGFILE_PARTNAME"
        if 'partname' in options:
            part_name = options['partname']
        file_type = self.FILE_TYPE_NORMAL
        if 'filetype' in options:
            file_type = int(options['filetype'])
        trunz = 0
        if overwrite:
            trunz = 1
        uri = self._make_path(path)
        rc = pangu_api.pangu_create1(c_char_p(uri.encode('utf-8')), c_int(copys - ftt), c_int(copys),
                                     c_char_p(app_name.encode('utf-8')), \
                                     c_char_p(part_name.encode('utf-8')), c_int(trunz), c_int(mode), c_int(file_type))
        if rc != 0:
            self._to_exception(rc, uri)
        return rc

    def open(self, path, flag, mode):
        """
        Open a file for read or write
          for read, set the flag to FLAG_GENERIC_READ
          for write, set the flag to FLAG_SEQUENTIAL_WRITE
        """
        handle = c_void_p(0)
        uri = self._make_path(path)
        file_type = c_int(self.FILE_TYPE_NORMAL)
        rc = pangu_api.pangu_open(
            c_char_p(uri.encode('utf-8')), c_int(flag), c_int(mode), file_type,
            byref(handle))
        if rc != 0:
            self._to_exception(rc, uri)
        return PanguFile(handle, flag, uri)

    def mkdir(self, path, mode=0o775):
        uri = self._make_path(path, True).encode()
        rc = pangu_api.pangu_mkdir(c_char_p(uri), c_int(mode))
        if rc != 0:
            if rc == errno.EEXIST and self.isdir(path):
                pass
            else:
                self._to_exception(rc, uri)
        return rc

    def rmdir(self, path):
        uri = self._make_path(path, True).encode()
        rc = pangu_api.pangu_rmdir(c_char_p(uri), c_int(0))
        if rc != 0 and rc != errno.ENOENT:
            self._to_exception(rc, uri)
        return rc

    def remove(self, path):
        uri = self._make_path(path)
        rc = pangu_api.pangu_remove(c_char_p(uri.encode('utf-8')), c_int(0))
        if rc != 0 and rc != errno.ENOENT:
            self._to_exception(rc, uri)
        return rc

    def unlink(self, path):
        return self.remove(path)

    def link(self, src_path, dst_path):
        src_abs_path = self._make_path(src_path)
        dst_abs_path = self._make_path(dst_path)
        rc = pangu_api.pangu_link(
            c_char_p(src_abs_path), c_char_p(dst_abs_path))
        if rc != 0:
            self._to_exception(rc, src_abs_path)
        return rc

    def chmod(self, path, mode, recursive=False):
        meta = self.stat(path)
        e = []
        if meta.isdir and recursive:
            files = self.listdir(path)
            for f in files:
                if f.isdir:
                    r = self.chmod("%s/%s" % (path, f.path), mode, recursive)
                else:
                    uri = self._make_path("%s/%s" % (path, f.path))
                    r = pangu_api.pangu_chmod(c_char_p(uri), c_int(mode))
                if r != 0:
                    e.append(r)
        uri = self._make_path(path)
        r = pangu_api.pangu_chmod(c_char_p(uri), c_int(mode))
        if len(e) != 0:
            return e[0]
        return r

    def chown(self, path, uid, gid, recursive=False):
        meta = self.stat(path)
        e = []
        if meta.isdir and recursive:
            files = self.listdir(path)
            for f in files:
                if f.isdir:
                    r = self.chown("%s/%s" % (path, f.path), uid, gid,
                                   recursive)
                else:
                    uri = self._make_path("%s/%s" % (path, f.path))
                    r = pangu_api.pangu_chown(
                        c_char_p(uri), c_int(uid), c_int(gid))
                if r != 0:
                    e.append(r)
        uri = self._make_path(path)
        r = pangu_api.pangu_chown(c_char_p(uri), c_int(uid), c_int(gid))
        if len(e) != 0:
            return e[0]
        return r

    def seal(self, path):
        return -errno.ENOSYS

    def rename(self, src_path, dst_path):
        filemeta = self.stat(src_path)
        src_uri = self._make_path(src_path).encode()
        dst_uri = self._make_path(dst_path).encode()
        rc = 0
        if filemeta.isdir:
            rc = pangu_api.pangu_rename_dir(
                c_char_p(src_uri + "/"), c_char_p(dst_uri + "/"))
        else:
            rc = pangu_api.pangu_rename_file(
                c_char_p(src_uri), c_char_p(dst_uri))
        if rc != 0:
            self._to_exception(rc, src_uri)
        return rc

    def stat(self, path):
        """
        Get the file or dir status
        """
        uri = self._make_path(path)
        meta = PanguFileMeta()
        r = pangu_api.pangu_get_status(
            c_char_p(uri.encode('utf-8')), byref(meta))
        if r != 0:
            self._to_exception(r, uri)

        is_dir = False
        if meta.is_dir > 0:
            is_dir = True
            uri += "/"
        return FileStatus(uri, meta.file_length, is_dir, meta.copys, \
                          self.PANGU_BLOCK_SIZE, meta.modified_time, meta.create_time, \
                          meta.access, meta.owner, meta.group)

    def listdir(self, path):
        """
        List directory
        """
        MAX_NAME_LEN = 1024
        LIST_BATCH_SIZE = 1024
        uri = self._make_path(path, True)
        dir_handle = c_void_p(0)
        r = pangu_api.pangu_open_dir(
            c_char_p(uri.encode('utf-8')), byref(dir_handle),
            c_int(LIST_BATCH_SIZE))
        if r != 0:
            self._to_exception(r, uri)
        meta = PanguFileMeta()
        cname = (c_byte * (MAX_NAME_LEN + 1))()
        is_dir = False
        file_stats = []
        while r == 0:
            name_size = c_int(MAX_NAME_LEN)
            meta.file_length = 0
            meta.create_time = 0
            meta.modified_time = 0
            r = pangu_api.pangu_read_dir(dir_handle, cname, byref(name_size),
                                         byref(meta))
            if r != 0:
                break
            if meta.is_dir > 0:
                is_dir = True
            if name_size.value >= MAX_NAME_LEN:
                raise Exception("name length too long")
            uri = str(bytearray(cname)[:name_size.value].decode())
            file_stats.append(FileStatus(uri, meta.file_length, is_dir, meta.copys, \
                                         self.PANGU_BLOCK_SIZE, meta.modified_time, meta.create_time, \
                                         meta.access, meta.owner, meta.group))

        pangu_api.pangu_close_dir(dir_handle)
        if r < 0:
            self._to_exception(r, uri)
        return file_stats

    def list_recursive_dir(self, path, include_parent=True):
        """
        Recursively list all the files (not include dir) under the path
        """
        full_paths = []
        file_metas = self.listdir(path)
        for meta in file_metas:
            if meta.isdir:
                sub_dir = make_path(path, meta.path)
                out_paths = self.list_recursive_dir(sub_dir, include_parent)
                if len(out_paths) == 0:
                    print("WARNING, empty dir %s" % sub_dir)
                else:
                    full_paths.extend(out_paths)
            else:
                if include_parent:
                    full_paths.append(make_path(path, meta.path))
                else:
                    full_paths.append(meta._path)
        return full_paths

    def statfs(self, path):
        """
        Get the dir content summary
        """
        uri = self._make_path(path, True)
        dir_meta = PanguDirMeta()
        r = pangu_api.pangu_dir_status(c_char_p(uri), byref(dir_meta))
        if r < 0:
            self._to_exception(r, path)
        space_consumed = 0
        if dir_meta.space_quota > 0:
            space_consumed = dir_meta.space_size
        return ContentSummary(dir_meta.space_size, dir_meta.file_count, dir_meta.dir_count, \
                              dir_meta.files_quota, space_consumed, dir_meta.space_quota)

    def read_into(self, path, out_file, buffer_size=1024 * 256):
        """
        read the data from pangu path into a local python file object
        """
        pangu_file = self.open(path, self.FLAG_GENERIC_READ, 0)
        try:
            while True:
                readed_data, size = pangu_file.read(buffer_size)
                out_file.write(readed_data[:size])
                if size != buffer_size:
                    break
        finally:
            pangu_file.close()

    def isdir(self, path):
        try:
            filemeta = self.stat(path)
            return filemeta.isdir > 0
        except FileNotFoundError as e:
            return False

    def download_dir(self, path, local_dir, buffer_size=1024 * 256):
        """
        download the files under pangu dir into the local dir. In case the
        local dir does not exist, the api will create it automatically
        """
        file_metas = self.listdir(path)
        # create in case local dir not exists
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        elif os.path.isfile(local_dir):
            raise Exception("%s not a local dir" % local_dir)

        for meta in file_metas:
            pangu_path = make_path(path, meta.path)
            local_path = make_path(local_dir, meta.path)
            if not meta.isdir:
                with open(local_path, "wb+") as out_file:
                    self.read_into(pangu_path, out_file, buffer_size)
                    self._print_verbose(
                        "%s -> %s downloaded" % (pangu_path, local_path))
            else:
                self.download_dir(pangu_path, local_path, buffer_size)

    def upload(self, local_path, path, overwrite=False,
               buffer_size=1024 * 256):
        """
        read the data from local_path file object and write the data
        into the pangu file identified by path

        in case target file already exists and overwrite not set,
        FileExistsError will be thrown
        """
        self.create(path, 0o775, overwrite)
        pangu_file = self.open(path, self.FLAG_SEQUENTIAL_WRITE, 0)
        local_file = open(local_path, "r")
        try:
            while True:
                data = local_file.read(buffer_size)
                if len(data) > 0:
                    pangu_file.append(data)
                if len(data) != buffer_size:
                    break
        finally:
            local_file.close()
            pangu_file.close()

    def upload_dir(self,
                   local_dir,
                   path,
                   overwrite=False,
                   buffer_size=1024 * 256):
        """
        upload a files under the local_dir to the destination pangu path
        """
        local_dir = os.path.abspath(local_dir)
        if not os.path.isdir(local_dir):
            raise Exception("%s not a local dir" % local_dir)
        prefix_len = len(local_dir) + 1  # plus /
        for maindir, subdir, filenames in os.walk(local_dir):
            for name in subdir:
                dirname = os.path.join(maindir, name)
                assert dirname.startswith(local_dir)
                inner_name = dirname[prefix_len:]
                final_name = make_path(path, inner_name)
                self.mkdir(final_name)
                self._print_verbose("mkdir %s" % final_name)

            for name in filenames:
                filename = os.path.join(maindir, name)
                assert filename.startswith(local_dir)
                inner_name = filename[prefix_len:]
                final_name = make_path(path, inner_name)
                self.upload(filename, final_name, overwrite, buffer_size)
                self._print_verbose(
                    "%s -> %s uploaded" % (filename, final_name))

    def set_flag(self, name, value):
        """
        Set the pangu filesystem flags
        """
        r = pangu_api.pangu_set_flag(
            c_char_p(name), c_char_p(value), c_int(len(value)))
        if r < 0:
            self._to_exception(r, name)
        return r


def make_path(parent_path, path):
    """
    Make a full abs path
    """
    if parent_path.endswith("/"):
        return "%s%s" % (parent_path, path)
    else:
        return "%s/%s" % (parent_path, path)


def parse_uri(uri):
    """
    Return host and path according to the URI
    """
    if not uri.startswith(PANGU_SCHEME):
        raise Exception("invalid uri " + uri)

    begin = len(PANGU_SCHEME)
    pos = uri.find('/', begin)

    if pos > 0:
        cluster = uri[begin:pos]
    else:
        raise Exception("invalid uri " + uri)
    path = os.path.normpath(uri[pos:])
    return cluster, path


def exit_with_error(msg=""):
    if msg:
        print(msg)
    sys.exit(1)


def ensure_enough_param(action, param_num):
    if len(sys.argv) - 2 < param_num:
        usage()
        exit_with_error(action + " does not have enough param")
    return True


def usage():
    """
    Print the help message
    """
    usage_msg = '''USAGE
    %s - Tool to operate the pangu filesystem through pangu-client.rpm
    %s ls <path>
    %s mkdir <path>
    %s rm <path>
    %s rmdir <path>
    %s cat <path>
    %s get <path> <local path>
    %s put <local path> <path>
    %s mv <src path> <dst path>
    %s stat <path>
    %s chmod <mode> <path> [ -R ]
    %s chown <user:group> <path> [ -R ]
    '''
    print(usage_msg % ((os.path.basename(sys.argv[0]), ) * 13))


def main():
    """
    main function
    """
    if len(sys.argv) < 3:
        usage()
        sys.exit(1)
    action = sys.argv[1]
    pangu_uri = sys.argv[2]
    if action in ["get", "put", "mv", "chmod", "chown"]:
        if len(sys.argv) < 4:
            usage()
            sys.exit(1)
    if action in ["put", "chmod", "chown"]:
        pangu_uri = sys.argv[3]
    cluster, path = parse_uri(pangu_uri)
    fs = PanguFileSystem(cluster)
    if action == "ls":
        if not fs.isdir(path):
            exit_with_error("%s is not a dir" % path)
        file_stats = fs.listdir(path)
        need_sort = True
        if need_sort:
            file_stats = sorted(file_stats, key=lambda stat: stat.path)
        for stat in file_stats:
            print(stat.pretty())
    elif action == "cat":
        fs.read_into(path, sys.stdout)
        sys.stdout.flush()
    elif action == "put":
        local_path = sys.argv[2]
        if not os.path.exists(local_path):
            exit_with_error("path %s not existed" % local_path)

        if os.path.isdir(local_path):
            fs.upload_dir(local_path, path)
        else:
            try:
                meta = fs.stat(path)
                if meta.isdir:
                    path = "%s/%s" % (path, os.path.basename(local_path))
                    meta = fs.stat(path)
                    # in case not exception, the path should be existed
                    exit_with_error("file %s already exists" % path)
            except FileNotFoundError as e:
                pass
            fs.upload(local_path, path)
            print("%s uploaded" % path)
    elif action == "get":
        meta = fs.stat(path)
        if not meta.isdir:
            filename = "%s/%s" % (os.path.abspath(sys.argv[3]),
                                  os.path.basename(path))
            with open(filename, "w+") as out_file:
                fs.read_into(path, out_file)
        else:
            fs.download_dir(path, sys.argv[3])
    elif action == "mv":
        if len(sys.argv) < 4:
            usage()
            sys.exit(1)
        _, dst_path = parse_uri(sys.argv[3])
        meta = fs.stat(path)
        if not meta.isdir and fs.isdir(dst_path):
            dst_path = "%s/%s" % (dst_path, os.path.basename(path))
        fs.rename(path, dst_path)
    elif action == "stat":
        props = str(fs.stat(path)).split(',')
        for prop in props:
            print(prop)
    elif action == 'mkdir':
        mode = 0o775
        fs.mkdir(path, mode)
    elif action == 'rm':
        if fs.unlink(path) == 0:
            print("Deleted %s" % path)
    elif action == 'rmdir':
        if fs.isdir(path):
            confirm = input("Deleting %s, confirm with [Y/N]: " % path)
            if confirm not in ['y', 'Y', 'yes', 'YES']:
                exit_with_error("abort deletion")
            fs.rmdir(path)
            print("Deleted %s" % path)
        else:
            print("%s not a dir" % path)
    elif action == 'chmod':
        if len(sys.argv[2]) != 3:
            exit_with_error("Invalid file mode %s" % sys.argv[2])
        recursive = False
        if len(sys.argv) > 4 and sys.argv[4] == "-R":
            recursive = True
        mode = int(sys.argv[2], 8)
        fs.chmod(path, mode, recursive)
    elif action == 'chown':
        recursive = False
        if len(sys.argv) > 4 and sys.argv[4] == "-R":
            recursive = True
        user_group = sys.argv[2].split(':')
        fs.chown(path, int(user_group[0]), int(user_group[1]), recursive)


if __name__ == '__main__':
    main()
