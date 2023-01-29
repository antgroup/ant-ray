#include "streaming_asio.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "logging.h"
#include "utility.h"

namespace ray {
namespace streaming {

StreamingLocalFileSystem::StreamingLocalFileSystem(const std::string &file_path,
                                                   bool is_reader)
    : is_reader_(is_reader), file_path_(file_path) {}

StreamingLocalFileSystem::~StreamingLocalFileSystem() {
  if (file_.is_open()) {
    Close();
  }
}

void StreamingLocalFileSystem::Open() {
  if (is_reader_) {
    file_.open(file_path_, std::ios::in | std::ios::binary);
  } else {
    file_.open(file_path_, std::ios::out | std::ios::binary | std::ios::app);
  }
}

void StreamingLocalFileSystem::Write(uint8_t *data, uint32_t data_size) {
  STREAMING_CHECK(!is_reader_);
  file_.write(reinterpret_cast<char *>(data), data_size);
  file_.flush();
  STREAMING_LOG(DEBUG) << "write " << StreamingUtility::Byte2hex(data, data_size);
}

void StreamingLocalFileSystem::Close() { file_.close(); }

void StreamingLocalFileSystem::Read(uint8_t *data, uint32_t data_size) {
  STREAMING_CHECK(is_reader_);
  STREAMING_CHECK(file_.is_open()) << "file not found or can't open";

  file_.read(reinterpret_cast<char *>(data), data_size);
}

void StreamingLocalFileSystem::ReadAll(std::shared_ptr<uint8_t> *data,
                                       uint32_t &data_size) {
  STREAMING_CHECK(is_reader_);
  STREAMING_CHECK(file_.is_open()) << "file not found or can't open";

  file_.seekg(0, std::ios::end);
  data_size = file_.tellg();
  file_.seekg(0);
  std::shared_ptr<uint8_t> ptr(new uint8_t[data_size], std::default_delete<uint8_t[]>());
  file_.read(reinterpret_cast<char *>(ptr.get()), data_size);
  STREAMING_LOG(DEBUG) << "file size => " << data_size;
  data->swap(ptr);
}

bool StreamingLocalFileSystem::Delete() { return Delete(file_path_); }

bool StreamingLocalFileSystem::Delete(const std::string &filename) {
  return std::remove(filename.c_str()) == 0;
}

bool StreamingLocalFileSystem::Rename(const std::string &target_name) {
  return std::rename(file_path_.c_str(), target_name.c_str());
}

bool StreamingLocalFileSystem::Rename(const std::string &source_name,
                                      const std::string &target_name) {
  return std::rename(source_name.c_str(), target_name.c_str());
}

bool StreamingLocalFileSystem::Exists() { return access(file_path_.c_str(), 0) == 0; }

bool StreamingLocalFileSystem::Exists(const std::string &filename) {
  return access(filename.c_str(), 0) == 0;
}

bool StreamingLocalFileSystem::IsDirectory(const std::string &dirname) {
  // STREAMING_LOG(INFO) << "[IsDirectory] dirname is " << dirname;
  if (dirname.empty()) {
    return false;
  }
  struct stat info;
  int32_t statRC = stat(dirname.c_str(), &info);
  if (statRC != 0) {
    return false;
  }
  return (info.st_mode & S_IFDIR) ? true : false;
}

bool StreamingLocalFileSystem::IsDirectory() { return IsDirectory(file_path_); }

bool StreamingLocalFileSystem::CreateDirectory(const std::string &dirname,
                                               bool ChangeDirectory) {
  if (dirname.empty()) {
    return false;
  }
  std::string dir = dirname;
  TransToDir(dir);
  if (access(dir.c_str(), 0) == -1) {
    mkdir(dir.c_str(), S_IRWXU | S_IRGRP | S_IROTH);
  }
  if (ChangeDirectory) {
    file_path_ = dir;
  }
  return true;
}

bool StreamingLocalFileSystem::CreateDirectory() { return CreateDirectory(file_path_); }

bool StreamingLocalFileSystem::ExistsSubDirectory(const std::string &dirname) {
  TransToDir(file_path_);
  return IsDirectory(file_path_ + dirname);
}

bool StreamingLocalFileSystem::CreateSubDirectory(const std::string &dirname) {
  if (!IsDirectory() || ExistsSubDirectory(dirname)) {
    return false;
  }
  TransToDir(file_path_);
  return CreateDirectory(file_path_ + dirname, false);
}

bool StreamingLocalFileSystem::RenameSubDirectory(const std::string &dirname,
                                                  const std::string &newname) {
  if (!ExistsSubDirectory(dirname) || ExistsSubDirectory(newname)) {
    return false;
  }
  return 0 == Rename(file_path_ + dirname, file_path_ + newname);
}

bool StreamingLocalFileSystem::DeleteDirectory(const std::string &dirname) {
  DIR *dirp = opendir(dirname.c_str());
  if (!dirp) {
    return false;
  }
  dirent *dir;
  struct stat st;
  while ((dir = readdir(dirp)) != NULL) {
    if (strcmp(dir->d_name, ".") == 0 || strcmp(dir->d_name, "..") == 0) {
      continue;
    }
    std::string sub_path = dirname + '/' + dir->d_name;
    if (lstat(sub_path.c_str(), &st) == -1) {
      STREAMING_LOG(WARNING) << "DeleteDirectory:lstat " << sub_path << " error";
      continue;
    }
    if (S_ISDIR(st.st_mode)) {
      if (DeleteDirectory(sub_path) == false) {
        closedir(dirp);
        return false;
      }
      rmdir(sub_path.c_str());
    } else if (S_ISREG(st.st_mode)) {
      Delete(sub_path);
    } else {
      STREAMING_LOG(WARNING) << "DeleteDirectory:st_mode " << sub_path << " error";
      continue;
    }
  }
  if (rmdir(dirname.c_str()) == -1) {
    closedir(dirp);
    return false;
  }
  closedir(dirp);
  return true;
}

bool StreamingLocalFileSystem::DeleteDirectory() { return DeleteDirectory(file_path_); }

bool StreamingLocalFileSystem::DeleteSubDirectory(const std::string &dirname) {
  TransToDir(file_path_);
  return DeleteDirectory(file_path_ + dirname);
}

#ifdef USE_PANGU
std::unique_ptr<zdfs::Env> StreamingPanguFileSystem::pangu_env_;
std::atomic<uint32_t> StreamingPanguFileSystem::pangu_env_ref_count_(0);

StreamingPanguFileSystem::StreamingPanguFileSystem(const std::string &file_path,
                                                   bool is_reader)
    : file_path_(file_path), is_reader_(is_reader){};

StreamingPanguFileSystem::~StreamingPanguFileSystem() {}

void StreamingPanguFileSystem::Init(const std::string &cluster_name,
                                    const std::string &pangu_log_conf_path) {
  static std::mutex env_mutex;
  {
    std::unique_lock<std::mutex> lock_guard(env_mutex);
    if (!pangu_env_ || !pangu_env_.get()) {
      StreamingLocalFileSystem local_file(pangu_log_conf_path);
      if (local_file.Exists()) {
        pangu_set_flag("apsara_log_conf_path", pangu_log_conf_path.c_str(),
                       pangu_log_conf_path.length());
        STREAMING_LOG(WARNING) << "pangu log conf path : " << pangu_log_conf_path;
      } else {
        STREAMING_LOG(WARNING)
            << "pang log conf file does not exist or no read permission : "
            << pangu_log_conf_path;
        auto pangu_log_conf_env = std::getenv("PANGU_LOG_CONF_PATH");
        if (pangu_log_conf_env) {
          std::string pangu_log_conf_env_str(pangu_log_conf_env);
          StreamingLocalFileSystem env_file(pangu_log_conf_env_str);
          if (env_file.Exists()) {
            pangu_set_flag("apsara_log_conf_path", pangu_log_conf_env_str.c_str(),
                           pangu_log_conf_env_str.length());
            STREAMING_LOG(INFO) << "use pangu env path config " << pangu_log_conf_env_str;
          } else {
            STREAMING_LOG(WARNING)
                << "pangu env config " << pangu_log_conf_env_str << " does not exist";
          }
        }
      }
      zdfs::Status status = NewPanguEnv(&pangu_env_, cluster_name);
      STREAMING_CHECK(status.ok()) << "new pangu env error, status " << status.ToString();
      STREAMING_LOG(INFO) << "new pangu env with cluster name : " << cluster_name;

    } else {
      STREAMING_LOG(WARNING) << "pangu cluster name " << cluster_name
                             << " env may not be same with last one, "
                             << " its ref count => " << pangu_env_ref_count_;
    }
    pangu_env_ref_count_++;
  }
}

void StreamingPanguFileSystem::Destory() {
  pangu_env_ref_count_--;
  STREAMING_LOG(DEBUG) << "pangu env ref count => " << pangu_env_ref_count_;
  if (!pangu_env_ref_count_ && pangu_env_.get()) {
    pangu_env_.reset();
    STREAMING_LOG(INFO) << "pangu env destory";
  }
}

void StreamingPanguFileSystem::Open() {
  STREAMING_CHECK(pangu_env_.get());
  zdfs::Status status;
  if (is_reader_) {
    status = pangu_env_->NewRandomAccessFile(file_path_, &read_file_, zdfs::EnvOptions());
    STREAMING_CHECK(status.ok())
        << "init pangu reader error, status " << status.ToString();

  } else {
    status = pangu_env_->NewWritableFile(file_path_, &write_file_, zdfs::EnvOptions());
    // Maybe one session of this file is in processing, so a tricky way to fix
    // this : firstly read all contents from this file and delete it, finally
    // recreate this file in same name.
    if (status.IsIOError()) {
      STREAMING_LOG(WARNING)
          << "recreate pangu file when io error(maybe session in processing)";
      status =
          pangu_env_->NewRandomAccessFile(file_path_, &read_file_, zdfs::EnvOptions());
      STREAMING_CHECK(status.ok())
          << "init read file handler failed, status " << status.ToString();
      std::shared_ptr<uint8_t> data;
      uint32_t data_size = 0;
      is_reader_ = true;
      ReadAll(&data, data_size);
      is_reader_ = false;
      // reuse read_file in writer mode and then keep it empty(reset it).
      read_file_.reset();
      Delete(file_path_);
      // recreate file
      status = pangu_env_->NewWritableFile(file_path_, &write_file_, zdfs::EnvOptions());
      STREAMING_CHECK(status.ok())
          << "recreate pangu writer error, status " << status.ToString();
      if (data_size > 0) {
        // write original data to file
        STREAMING_LOG(INFO) << "original file size => " << data_size;
        Write(data.get(), data_size);
      }
    } else {
      STREAMING_CHECK(status.ok())
          << "init pangu writer error, status " << status.ToString();
    }
  }
}

void StreamingPanguFileSystem::Write(uint8_t *data, uint32_t data_size) {
  STREAMING_CHECK(!is_reader_ && pangu_env_.get());
  zdfs::Slice in(reinterpret_cast<const char *>(data), data_size);
  write_file_->Append(in);
  write_file_->Sync();
}

void StreamingPanguFileSystem::Read(uint8_t *data, uint32_t data_size) {
  STREAMING_CHECK(is_reader_ && pangu_env_.get());

  zdfs::Status status = pangu_env_->FileExists(file_path_);
  STREAMING_CHECK(status.ok()) << "no such file : " << file_path_ << " in pangu";

  zdfs::Slice out;
  read_file_->Read(0, data_size, &out, reinterpret_cast<char *>(data));
}

void StreamingPanguFileSystem::ReadAll(std::shared_ptr<uint8_t> *data,
                                       uint32_t &data_size) {
  STREAMING_CHECK(is_reader_ && pangu_env_.get());

  zdfs::Status status = pangu_env_->FileExists(file_path_);
  STREAMING_CHECK(status.ok()) << "no such file : " << file_path_ << " in pangu";

  zdfs::Slice out;
  uint64_t data_len = 0;
  RAY_IGNORE_EXPR(pangu_env_->GetFileSize(file_path_, &data_len));
  data_size = data_len;
  STREAMING_LOG(DEBUG) << " file size " << data_size;
  std::shared_ptr<uint8_t> ptr(new uint8_t[data_size], std::default_delete<uint8_t[]>());
  data->swap(ptr);
  read_file_->Read(0, data_size, &out, reinterpret_cast<char *>(data->get()));
}

void StreamingPanguFileSystem::Close() {
  if (is_reader_) {
    // no close operator in read file handler
    // read_file_->Close();
  } else {
    write_file_->Close();
  }
}

bool StreamingPanguFileSystem::Delete() { return Delete(file_path_); }

bool StreamingPanguFileSystem::Delete(const std::string &filename) {
  STREAMING_CHECK(pangu_env_.get());
  zdfs::Status status = pangu_env_->FileExists(filename);
  if (status.ok()) {
    status = pangu_env_->DeleteFile(filename);
    STREAMING_CHECK(status.ok()) << "pangu delete file error : " << filename;
  } else {
    return false;
  }
  return true;
}

bool StreamingPanguFileSystem::Rename(const std::string &target_name) {
  return Rename(file_path_, target_name);
}

bool StreamingPanguFileSystem::Rename(const std::string &source_name,
                                      const std::string &target_name) {
  STREAMING_CHECK(pangu_env_.get());
  zdfs::Status status = pangu_env_->FileExists(source_name);
  if (status.ok()) {
    status = pangu_env_->RenameFile(source_name, target_name);
    STREAMING_CHECK(status.ok())
        << "pangu rename file error : " << source_name << ", target name " << target_name;
  } else {
    return false;
  }
  return true;
}

bool StreamingPanguFileSystem::Exists() { return Exists(file_path_); }

bool StreamingPanguFileSystem::Exists(const std::string &filename) {
  STREAMING_CHECK(pangu_env_.get());
  zdfs::Status status = pangu_env_->FileExists(filename);
  return status.ok();
}

bool StreamingPanguFileSystem::IsDirectory(const std::string &dirname) {
  STREAMING_CHECK(pangu_env_.get());
  // TODO:Not Support
  return false;
}

bool StreamingPanguFileSystem::IsDirectory() { return IsDirectory(file_path_); }

bool StreamingPanguFileSystem::CreateDirectory(const std::string &dirname,
                                               bool ChangeDirectory) {
  STREAMING_CHECK(pangu_env_.get());
  std::string dir = dirname;
  TransToDir(dir);
  zdfs::Status status = pangu_env_->CreateDirIfMissing(dir);
  if (status.ok() && ChangeDirectory) {
    file_path_ = dir;
  }
  return status.ok();
}
bool StreamingPanguFileSystem::CreateDirectory() { return CreateDirectory(file_path_); }
bool StreamingPanguFileSystem::DeleteDirectory(const std::string &dirname) {
  STREAMING_CHECK(pangu_env_.get());
  zdfs::Status status = pangu_env_->DeleteDir(dirname);
  return status.ok();
}
bool StreamingPanguFileSystem::DeleteDirectory() { return DeleteDirectory(file_path_); }
bool StreamingPanguFileSystem::CreateSubDirectory(const std::string &dirname) {
  STREAMING_CHECK(pangu_env_.get());
  TransToDir(file_path_);
  return CreateDirectory(file_path_ + dirname, false);
}
bool StreamingPanguFileSystem::RenameSubDirectory(const std::string &dirname,
                                                  const std::string &newname) {
  STREAMING_CHECK(pangu_env_.get());
  // TODO:Not Support
  return false;
}
bool StreamingPanguFileSystem::ExistsSubDirectory(const std::string &dirname) {
  STREAMING_CHECK(pangu_env_.get());
  // TODO:Not Support
  return false;
}
bool StreamingPanguFileSystem::DeleteSubDirectory(const std::string &dirname) {
  STREAMING_CHECK(pangu_env_.get());
  TransToDir(file_path_);
  return DeleteDirectory(file_path_ + dirname);
}
#endif
}  // namespace streaming
}  // namespace ray
