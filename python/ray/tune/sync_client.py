import distutils
import distutils.spawn
import logging
import os
import subprocess
import tempfile
import types
from shlex import quote

from ray.tune.error import TuneError
from ray.tune.utils.filelock import FileLock

logger = logging.getLogger(__name__)

S3_PREFIX = "s3://"
GS_PREFIX = "gs://"
HDFS_PREFIX = "hdfs://"
OSS_PREFIX = "oss://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GS_PREFIX, HDFS_PREFIX, OSS_PREFIX)

noop_template = ": {target}"  # noop in bash


def noop(*args):
    return


def get_sync_client(sync_function, delete_function=None):
    """Returns a sync client.

    Args:
        sync_function (Optional[str|function]): Sync function.
        delete_function (Optional[str|function]): Delete function. Must be
            the same type as sync_function if it is provided.

    Raises:
        ValueError if sync_function or delete_function are malformed.
    """
    if sync_function is None:
        return None
    if delete_function and type(sync_function) != type(delete_function):
        raise ValueError("Sync and delete functions must be of same type.")
    if isinstance(sync_function, types.FunctionType):
        delete_function = delete_function or noop
        client_cls = FunctionBasedClient
    elif isinstance(sync_function, str):
        delete_function = delete_function or noop_template
        client_cls = CommandBasedClient
    else:
        raise ValueError("Sync function {} must be string or function".format(
            sync_function))
    return client_cls(sync_function, sync_function, delete_function)


def set_aws_configure(max_retry_times=3):
    """Setting aws configure.

     Args:
         max_retry_times: The maximum time of retries is more than 3.
    """
    p = os.popen("cd ~ && pwd")
    path = p.readlines()[0].strip()
    aws_configure_path = f"{path}/.aws/credentials"

    # Supports compatibility with OSS.
    storage_type = os.environ.get("STORAGE_TYPE")
    if storage_type is not None and storage_type.upper() == "OSS":
        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_access_key_log = f"{aws_access_key_id[0:4]}****" \
                             f"{aws_access_key_id[-4:]}"
    else:
        return

    def _aws_configure(retry_number):
        """Retry Setting aws configure.

        Args:
            retry_number: The number of retries.

        Raises:
            ValueError if Sync aws config is failure.
        """

        def _judge_aws_profile_configure():
            judge_flag = False
            if os.path.exists(aws_configure_path):
                with open(aws_configure_path) as f:
                    f.seek(0)
                    lines = f.readlines()
                    for line in lines:
                        if aws_access_key_id in line:
                            judge_flag = True
                            break
            return judge_flag

        retry_number = retry_number - 1
        retry_times = max_retry_times - retry_number
        try:
            file_lock = FileLock(aws_configure_path)
            file_lock.acquire()
            if not _judge_aws_profile_configure():
                aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
                p_cmd = f"--p {aws_access_key_id}"
                aws_configure = f"aws configure set s3.addressing_style " \
                                f"virtual {p_cmd} && "
                aws_configure_set_key = f"aws configure set " \
                                        f"aws_access_key_id" \
                                        f" {aws_access_key_id} {p_cmd} && "
                aws_configure_set_secret = f"aws configure " \
                                           f"set aws_secret_access_key" \
                                           f" {aws_secret_access_key} {p_cmd}"
                aws_configure = aws_configure + aws_configure_set_key + \
                    aws_configure_set_secret

                result = os.system(aws_configure)
                file_lock.release()

                if result == 0:
                    logger.info(f"Setting aws configure is done, "
                                f"the profile is {aws_access_key_log}")
                else:
                    if retry_number < 0:
                        raise ValueError("Sync aws configuration parameters"
                                         "is failure about running "
                                         "the aws configure command.")
                    else:
                        logger.error(f"Setting aws configure is failure, "
                                     f"the profile is {aws_access_key_log}, "
                                     f"retry {retry_times} times.")
                        _aws_configure(retry_number)
            else:
                file_lock.release()
                logger.info(f"The aws configure profile "
                            f"{aws_access_key_log} is exist.")
        except Exception as e:
            file_lock.release()
            if retry_number < 0:
                raise ValueError(f"Sync aws config is failure, {e}")
            else:
                logger.error(f"FileLock failed to be executed, "
                             f"release the lock, "
                             f"retry {retry_times} times, {e}.")
                _aws_configure(retry_number)

    _aws_configure(max_retry_times)


def get_cloud_sync_client(remote_path):
    """Returns a CommandBasedClient that can sync to/from remote storage.

    Args:
        remote_path (str): Path to remote storage (S3(OSS), GS or HDFS).

    Raises:
        ValueError if malformed remote_dir.
    """
    if remote_path.startswith(S3_PREFIX):
        if not distutils.spawn.find_executable("aws"):
            raise ValueError(
                "Upload uri starting with '{}' requires awscli tool"
                " to be installed".format(S3_PREFIX))
        # Supports compatibility with OSS.
        storage_type = os.environ.get("STORAGE_TYPE")
        endpoint = os.environ.get("S3_ENDPOINT")
        if storage_type is not None and storage_type.upper() == "OSS":
            aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
            p_cmd = f"--p {aws_access_key_id}"
            endpoint = "--endpoint-url " + endpoint + " " + p_cmd + " "
        else:
            endpoint = ""
        sync_up_template = ("aws s3 sync {source} {target} " + endpoint +
                            "--exact-timestamps --only-show-errors")
        sync_down_template = sync_up_template
        delete_template = ("aws s3 rm {target} " + endpoint +
                           "--recursive --only-show-errors")
    elif remote_path.startswith(GS_PREFIX):
        if not distutils.spawn.find_executable("gsutil"):
            raise ValueError(
                "Upload uri starting with '{}' requires gsutil tool"
                " to be installed".format(GS_PREFIX))
        sync_up_template = "gsutil rsync -r {source} {target}"
        sync_down_template = sync_up_template
        delete_template = "gsutil rm -r {target}"
    elif remote_path.startswith(HDFS_PREFIX):
        if not distutils.spawn.find_executable("hdfs"):
            raise ValueError("Upload uri starting with '{}' requires hdfs tool"
                             " to be installed".format(HDFS_PREFIX))
        sync_up_template = "hdfs dfs -put -f {source} {target}"
        sync_down_template = "hdfs dfs -get -f {target} {source}"
        delete_template = "hdfs dfs -rm -r {target}"
    else:
        raise ValueError("Upload uri must start with one of: {}"
                         "".format(ALLOWED_REMOTE_PREFIXES))
    return CommandBasedClient(sync_up_template, sync_down_template,
                              delete_template)


class SyncClient:
    """Client interface for interacting with remote storage options."""

    def sync_up(self, source, target):
        """Syncs up from source to target.

        Args:
            source (str): Source path.
            target (str): Target path.

        Returns:
            True if sync initiation successful, False otherwise.
        """
        raise NotImplementedError

    def sync_down(self, source, target):
        """Syncs down from source to target.

        Args:
            source (str): Source path.
            target (str): Target path.

        Returns:
            True if sync initiation successful, False otherwise.
        """
        raise NotImplementedError

    def delete(self, target):
        """Deletes target.

        Args:
            target (str): Target path.

        Returns:
            True if delete initiation successful, False otherwise.
        """
        raise NotImplementedError

    def wait(self):
        """Waits for current sync to complete, if asynchronously started."""
        pass

    def reset(self):
        """Resets state."""
        pass

    def close(self):
        """Clean up hook."""
        pass


class FunctionBasedClient(SyncClient):
    def __init__(self, sync_up_func, sync_down_func, delete_func=None):
        self.sync_up_func = sync_up_func
        self.sync_down_func = sync_down_func
        self.delete_func = delete_func or noop

    def sync_up(self, source, target):
        self.sync_up_func(source, target)
        return True

    def sync_down(self, source, target):
        self.sync_down_func(source, target)
        return True

    def delete(self, target):
        self.delete_func(target)
        return True


NOOP = FunctionBasedClient(noop, noop)


class CommandBasedClient(SyncClient):
    def __init__(self,
                 sync_up_template,
                 sync_down_template,
                 delete_template=noop_template):
        """Syncs between two directories with the given command.

        Arguments:
            sync_up_template (str): A runnable string template; needs to
                include replacement fields '{source}' and '{target}'.
            sync_down_template (str): A runnable string template; needs to
                include replacement fields '{source}' and '{target}'.
            delete_template (Optional[str]): A runnable string template; needs
                to include replacement field '{target}'. Noop by default.
        """
        self._validate_sync_string(sync_up_template)
        self._validate_sync_string(sync_down_template)
        self.sync_up_template = sync_up_template
        self.sync_down_template = sync_down_template
        self.delete_template = delete_template
        self.logfile = None
        self._closed = False
        self.cmd_process = None

    def set_logdir(self, logdir):
        """Sets the directory to log sync execution output in.

        Args:
            logdir (str): Log directory.
        """
        self.logfile = tempfile.NamedTemporaryFile(
            prefix="log_sync_out", dir=logdir, suffix=".log", delete=False)
        self._closed = False

    def _get_logfile(self):
        if self._closed:
            raise RuntimeError(
                "[internalerror] The client has been closed. "
                "Please report this stacktrace + your cluster configuration "
                "on Github!")
        else:
            return self.logfile

    def sync_up(self, source, target):
        return self._execute(self.sync_up_template, source, target)

    def sync_down(self, source, target):
        return self._execute(self.sync_down_template, source, target)

    def delete(self, target):
        if self.is_running:
            logger.warning("Last sync client cmd still in progress, skipping.")
            return False
        final_cmd = self.delete_template.format(target=quote(target))
        logger.debug("Running delete: {}".format(final_cmd))
        self.cmd_process = subprocess.Popen(
            final_cmd,
            shell=True,
            stderr=subprocess.PIPE,
            stdout=self._get_logfile())
        return True

    def wait(self):
        if self.cmd_process:
            _, error_msg = self.cmd_process.communicate()
            error_msg = error_msg.decode("ascii")
            code = self.cmd_process.returncode
            args = self.cmd_process.args
            self.cmd_process = None
            if code != 0:
                raise TuneError("Sync error. Ran command: {}\n"
                                "Error message ({}): {}".format(
                                    args, code, error_msg))

    def reset(self):
        if self.is_running:
            logger.warning("Sync process still running but resetting anyways.")
        self.cmd_process = None

    def close(self):
        if self.logfile:
            logger.debug(f"Closing the logfile: {str(self.logfile)}")
            self.logfile.close()
            self.logfile = None
            self._closed = True

    @property
    def is_running(self):
        """Returns whether a sync or delete process is running."""
        if self.cmd_process:
            self.cmd_process.poll()
            return self.cmd_process.returncode is None
        return False

    def _execute(self, sync_template, source, target):
        """Executes sync_template on source and target."""
        if self.is_running:
            logger.warning("Last sync client cmd still in progress, skipping.")
            return False
        final_cmd = sync_template.format(
            source=quote(source), target=quote(target))
        logger.debug("Running sync: {}".format(final_cmd))
        self.cmd_process = subprocess.Popen(
            final_cmd,
            shell=True,
            stderr=subprocess.PIPE,
            stdout=self._get_logfile())
        return True

    @staticmethod
    def _validate_sync_string(sync_string):
        if not isinstance(sync_string, str):
            raise ValueError("{} is not a string.".format(sync_string))
        if "{source}" not in sync_string:
            raise ValueError("Sync template missing '{source}'.")
        if "{target}" not in sync_string:
            raise ValueError("Sync template missing '{target}'.")
