//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "env.h"
#include "env_pangu.h"

#include <algorithm>
#include <stdio.h>
#include <sys/time.h>
#include <fcntl.h>
#include <time.h>
#include <iostream>
#include <sstream>
#include <atomic>
#include "util/status.h"

#define PANGU_EXISTS 0
#define PANGU_DOESNT_EXIST -1
#define PANGU_SUCCESS 0

#ifdef _DEBUG
#define VLOG_ERROR	printf
#define VLOG_WARN	printf
#define VLOG_DEBUG	printf
#define VLOG_FATAL	printf
#else
#define VLOG_ERROR(fmt, ...)
#define VLOG_WARN(fmt, ...)
#define VLOG_DEBUG(fmt, ...)
#define VLOG_FATAL(fmt, ...)
#endif

//
// This file defines an Pangu environment for zdfs.
//

namespace zdfs {

namespace {

// Log error message
static Status IOError(const std::string& context, int err_number) {
  Status s;
  switch (err_number) {
  case ENOSPC:
    s = Status::NoSpace(context, strerror(err_number));
    break;
  case EINVAL:
    s = Status::InvalidArgument(context, strerror(err_number));
    break;
  case ENOENT:
    s = Status::NotFound(context, strerror(err_number));
    break;
  case EAGAIN:
    s = Status::TryAgain(context, strerror(err_number));
    break;
  case EBUSY:
    s = Status::Busy(context, strerror(err_number));
    break;
  default:
    s = Status::IOError(context, strerror(err_number));
    break;
  }
  errno = err_number;
  return s;
}

template <typename T>
inline std::string NumToString(T value) {
  std::ostringstream os;
  os << value;
  return os.str();
}

// Used for reading a file from Pangu. It implements both sequential-read
// access methods as well as random read access methods.
class PanguReadableFile : virtual public SequentialFile,
                         virtual public RandomAccessFile {
 private:
  std::string filename_;
  file_handle_t hfile_;
  std::atomic<uint64_t> offset_;

 public:
  PanguReadableFile(const std::string& fname)
      : filename_(fname), hfile_(nullptr), offset_(0) {
    VLOG_DEBUG("[pangu] PanguReadableFile opening file %s\n",
                    filename_.c_str());
#ifdef USE_PANGU2
    int rc = pangu2_open(filename_.c_str(), O_RDONLY, 0, &hfile_);
#else
    int rc = pangu_open(filename_.c_str(), FLAG_GENERIC_READ, 0, FILE_TYPE_NORMAL, &hfile_);
#endif
    VLOG_DEBUG("[pangu] PanguReadableFile opened file %s hfile_=0x%p\n",
                    filename_.c_str(), hfile_);
    if (rc < 0) {
      errno = -rc;
    }
  }

  virtual ~PanguReadableFile() {
    VLOG_DEBUG("[pangu] PanguReadableFile closing file %s\n",
                    filename_.c_str());
    if (hfile_ != nullptr) {
#ifdef USE_PANGU2
      pangu2_close(hfile_);
#else
      pangu_close(hfile_);
#endif
    }
    VLOG_DEBUG("[pangu] PanguReadableFile closed file %s\n",
                    filename_.c_str());
    hfile_ = nullptr;
  }

  bool isValid() {
    return hfile_ != nullptr;
  }

  // sequential access, read data at current offset in file
  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    VLOG_DEBUG("[pangu] PanguReadableFile reading %s %ld\n",
                    filename_.c_str(), n);

    char* buffer = scratch;
    size_t total_bytes_read = 0;
    int bytes_read = 0;
    size_t remaining_bytes = n;

#ifndef USE_PANGU2
    int64_t fp = pangu_lseek(hfile_, offset_.load(), SEEK_SET);
    if (fp < 0) {
      s = IOError(filename_, (int)(-fp));
      return s;
    }
#endif
    const int BUFF_SIZE = 1024 * 1024;  // 1M
    // Read a total of n bytes repeatedly until we hit error or eof
    while (remaining_bytes > 0) {
      int read_size = remaining_bytes > (size_t)BUFF_SIZE ? BUFF_SIZE : (int)remaining_bytes;
#ifdef USE_PANGU2
      bytes_read = pangu2_pread(hfile_, buffer, read_size, offset_.load());
#else
      bytes_read = pangu_read1(hfile_, buffer, read_size, 0);
#endif
      if (bytes_read <= 0) {
        break;
      }
      assert(bytes_read <= read_size);

      offset_.fetch_add(bytes_read);
      total_bytes_read += bytes_read;
      remaining_bytes -= bytes_read;
      buffer += bytes_read;
    }
    assert(total_bytes_read <= n);

    VLOG_DEBUG("[pangu] PanguReadableFile read %s\n",
                    filename_.c_str());

    if (bytes_read < 0) {
      // return rc bytes_read means the -errno
      s = IOError(filename_, -bytes_read);
    } else {
      *result = Slice(scratch, total_bytes_read);
    }

    return s;
  }

  // random access, read data from specified offset in file
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    VLOG_DEBUG("[pangu] PanguReadableFile preading %s\n",
                    filename_.c_str());
    char* buffer = scratch;
    size_t total_bytes_read = 0;
    int bytes_read = 0;
    size_t remaining_bytes = n;

#ifndef USE_PANGU2
    int64_t fp = pangu_lseek(hfile_, offset, SEEK_SET);
    if (fp < 0) {
      s = IOError(filename_, (int)(-fp));
      return s;
    }
#endif

    const int BUFF_SIZE = 1024 * 1024;  // 1M
    while (remaining_bytes > 0) {
      int read_size = remaining_bytes > (size_t)BUFF_SIZE ? BUFF_SIZE : (int)remaining_bytes;
#ifdef USE_PANGU2
      bytes_read = pangu2_pread(hfile_, buffer, read_size, offset);
#else
      bytes_read = pangu_read1(hfile_, buffer, read_size, 0);
#endif
      if (bytes_read <= 0) {
        break;
      }
      assert(bytes_read <= read_size);

      offset += bytes_read;
      total_bytes_read += bytes_read;
      remaining_bytes -= bytes_read;
      buffer += bytes_read;
    }
    VLOG_DEBUG("[pangu] PanguReadableFile pread %s\n",
                    filename_.c_str());
    if (bytes_read < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, -bytes_read);
    }
    else {
      *result = Slice(scratch, total_bytes_read);
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    VLOG_DEBUG("[pangu] PanguReadableFile skip %s\n",
                    filename_.c_str());
    offset_.fetch_add(n);
    return Status::OK();
  }
};

// Appends to an existing file in Pangu.
class PanguWritableFile: public WritableFile {
 private:
  std::string filename_;
  file_handle_t hfile_;
  bool syncwrite_;

 public:
  PanguWritableFile(const std::string& fname, const EnvOptions& options)
      : filename_(fname), hfile_(nullptr), syncwrite_(false) {
    int overwrite = 0;
    if (options.use_direct_writes) {
      syncwrite_ = true;
    }
    if (options.truncate_if_exists) {
      overwrite = 1;
    }
    VLOG_DEBUG("[pangu] PanguWritableFile opening %s\n",
                    filename_.c_str());
    int copys = 3, ftt = 1;
#ifndef USE_PANGU2
    int file_type = FILE_TYPE_NORMAL;
#endif
    const PanguEnvOptions* pu_options = dynamic_cast<const PanguEnvOptions*>(&options);
    if (pu_options) {
      copys = pu_options->copys;
      ftt = pu_options->ftt;
#ifndef USE_PANGU2
      file_type = pu_options->file_type;
#endif
    }
#ifdef USE_PANGU2
    int rc = pangu2_create(filename_.c_str(), copys, ftt, "BIGFILE_APPNAME", overwrite);
#else
    // change all files mode 0x1ff (777)
    int rc = pangu_create1(filename_.c_str(), copys-ftt, copys, "BIGFILE_APPNAME", "BIGFILE_PARTNAME", overwrite, 0x1ff, file_type);
#endif
    if (rc != 0 && rc != -EEXIST) {
      VLOG_ERROR("[pangu] PanguWritableFile create %s, %d\n",
                      filename_.c_str(), rc);
      errno = -rc;
      return;
    }
#ifdef USE_PANGU2
    int flag = O_WRONLY;
    if (syncwrite_)
      flag |= O_SYNC;

    if (pu_options && pu_options->write_mode == 2)
      rc = pangu2_open(filename_.c_str(), flag, OPEN_MODE_Y_WRITE, &hfile_);
    else
      rc = pangu2_open(filename_.c_str(), flag, OPEN_MODE_STAR_WRITE, &hfile_);
#else
    int flag = FLAG_SEQUENTIAL_WRITE;
    rc = pangu_open(filename_.c_str(), flag, 0, FILE_TYPE_NORMAL, &hfile_);
#endif

    if (rc < 0) {
      errno = -rc;
    }
    VLOG_DEBUG("[pangu] PanguWritableFile opened %s\n",
                    filename_.c_str());
    //open may fail in case the file is already opened (or crash recovery)
    //assert(hfile_ != nullptr);
  }
  virtual ~PanguWritableFile() {
    if (hfile_ != nullptr) {
      VLOG_DEBUG("[pangu] PanguWritableFile closing %s\n",
                      filename_.c_str());
#ifdef USE_PANGU2
      pangu2_close(hfile_);
#else
      pangu_close(hfile_);
#endif
      VLOG_DEBUG("[pangu] PanguWritableFile closed %s\n",
                      filename_.c_str());
      hfile_ = nullptr;
    }
  }

  // If the file was successfully created, then this returns true.
  // Otherwise returns false.
  bool isValid() {
    return hfile_ != nullptr;
  }

  // The name of the file, mostly needed for debug logging.
  const std::string& getName() {
    return filename_;
  }

  virtual Status Append(const Slice& data) {
    VLOG_DEBUG("[pangu] PanguWritableFile Append %s\n",
                    filename_.c_str());
    const char* src = data.data();
    size_t left = data.size();
#ifdef USE_PANGU2
    size_t ret = pangu2_append(hfile_, src, left);
#else
    size_t ret = pangu_write1(hfile_, src, left, 0);
#endif
    VLOG_DEBUG("[pangu] PanguWritableFile Appended %s\n",
                    filename_.c_str());
    if (ret != left) {
      return IOError(filename_, -ret);
    }
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    VLOG_DEBUG("[pangu] PanguWritableFile Sync %s\n",
                    filename_.c_str());
    if (!syncwrite_) {
#ifdef USE_PANGU2
      int rc = pangu2_fsync(hfile_);
#else
      int rc = pangu_fsync(hfile_);
#endif
      if (rc < 0) {
        return IOError(filename_, -rc);
      }
    }
    VLOG_DEBUG("[pangu] PanguWritableFile Synced %s\n",
                    filename_.c_str());
    return Status::OK();
  }

  virtual Status Close() {
    VLOG_DEBUG("[pangu] PanguWritableFile closing %s\n",
                    filename_.c_str());
    // pangu sdk will auto sync the data when closed
#ifdef USE_PANGU2
    int rc = pangu2_close(hfile_);
#else
    int rc = pangu_close(hfile_);
#endif
    if (rc != 0) {
      return IOError(filename_, -rc);
    }
    VLOG_DEBUG("[pangu] PanguWritableFile closed %s\n",
                    filename_.c_str());
    hfile_ = nullptr;
    return Status::OK();
  }
};

}  // namespace

// Finally, the pangu environment

std::string PanguEnv::MakeAbsPath(const std::string& fname) {
  std::stringstream ss;
  ss << fsname_ << "/" << fname;
  return ss.str();
}

void PanguEnv::FixDirectoryName(std::string& name) {
  int len = name.length();
  if (len && name[len-1] != '/') {
    name.push_back('/');
  }
}

std::string PanguEnv::GenPanguDockerHost() {
  // HOSTNAME=pangurestgwsh-40-5009
  // ALIPAY_ZONE=GZ00C
  // ALIPAY_APP_ZONE=GZ00C
  // ALIPAY_APP_IDC=EM14
  // ALIPAY_LOCATION=em14
  // Rack=D03
  // Sigma_Room=A1-1.EM14
  char* location = getenv("ALIPAY_LOCATION");
  char* hostname = getenv("HOSTNAME");

  if (!location || !hostname) {
    VLOG_WARN("location=%s host=%s incomplete information, may run on "
              "older docker or hardware\n", location ? location : "null", hostname ? hostname : "null");
    return "";
  }

  std::string str_host = hostname;
  std::string::size_type pos = str_host.find('.');
  if (pos != std::string::npos) {
    str_host = str_host.substr(0, pos);
  }
  std::string virtualHostName = str_host + ".alipay." + location;
  VLOG_WARN("Use virtual hostname %s as client name\n", virtualHostName.c_str());
  return virtualHostName;
}

// open a file for sequential reading
Status PanguEnv::NewSequentialFile(const std::string& fname,
                                  unique_ptr<SequentialFile>* result,
                                  const EnvOptions& options) {
  result->reset();
  PanguReadableFile* f = new PanguReadableFile(MakeAbsPath(fname));
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  result->reset(dynamic_cast<SequentialFile*>(f));
  return Status::OK();
}

// open a file for random reading
Status PanguEnv::NewRandomAccessFile(const std::string& fname,
                                    unique_ptr<RandomAccessFile>* result,
                                    const EnvOptions& options) {
  result->reset();
  PanguReadableFile* f = new PanguReadableFile(MakeAbsPath(fname));
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  result->reset(dynamic_cast<RandomAccessFile*>(f));
  return Status::OK();
}

// create a new file for writing
Status PanguEnv::NewWritableFile(const std::string& fname,
                                unique_ptr<WritableFile>* result,
                                const EnvOptions& options) {
  result->reset();
  Status s;
  int flags = 0;
  if (options.use_direct_writes) {
    flags |= O_SYNC;
  }
  if (options.truncate_if_exists) {
    flags |= O_TRUNC;
  }
  PanguWritableFile* f = new PanguWritableFile(MakeAbsPath(fname), options);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  result->reset(dynamic_cast<WritableFile*>(f));
  return Status::OK();
}

class PanguDirectory : public Directory {
 public:
  explicit PanguDirectory(int fd) {}
  ~PanguDirectory() {}

  virtual Status Fsync() { return Status::OK(); }
};

Status PanguEnv::NewDirectory(const std::string& name,
                             unique_ptr<Directory>* result) {
  std::string absName = MakeAbsPath(name);
  FixDirectoryName(absName);
  file_status_t stat;
#ifdef USE_PANGU2
  int rc = pangu2_get_status(absName.c_str(), &stat);
#else
  int rc = pangu_get_status(absName.c_str(), &stat);
#endif
  if (rc == 0 && stat.is_dir) {
      result->reset(new PanguDirectory(0));
      return Status::OK();
  }
  else if (rc == -ENOENT) {
    return Status::NotFound();
  }
  else {
      VLOG_FATAL("NewDirectory panguExists call failed");
      throw PanguFatalException("panguExists call failed with error " +
                               NumToString(rc) + " on path " + name +
                               ".\n");
  }
}

Status PanguEnv::FileExists(const std::string& fname) {
  std::string absName = MakeAbsPath(fname);
  file_status_t stat;
#ifdef USE_PANGU2
  int rc = pangu2_get_status(absName.c_str(), &stat);
#else
  int rc = pangu_get_status(absName.c_str(), &stat);
#endif
  if (rc == -ENOENT) {
    return Status::NotFound();
  }
  else if (rc == 0) {
    return Status::OK();
  }
  else {
      VLOG_FATAL("FileExists panguExists call failed");
      return Status::IOError("panguExists call failed with error " +
                             NumToString(rc) + " on path " + fname + ".\n");
  }
}

Status PanguEnv::GetChildren(const std::string& path,
                            std::vector<std::string>* result) {
  std::string absPath = MakeAbsPath(path);
  FixDirectoryName(absPath);
  pangu_dir_t hdir;
#ifdef USE_PANGU2
  int rc = pangu2_open_dir(absPath.c_str(), &hdir, 4096);
#else
  int rc = pangu_open_dir(absPath.c_str(), &hdir, 4096);
#endif
  if (rc == -ENOENT) {
    return Status::NotFound();
  }
  else if (rc != 0) {
    VLOG_FATAL("GetChildren panguExists call failed");
    throw PanguFatalException("panguExists call failed with error " +
                             NumToString(rc) + ".\n");
  }

  file_status_t stat;
  char name[1024];
  while (rc == 0) {
    int nameLen = sizeof(name) - 1;
#ifdef USE_PANGU2
    rc = pangu2_read_dir(hdir, name, &nameLen, &stat);
#else
    rc = pangu_read_dir(hdir, name, &nameLen, &stat);
#endif
    if (rc == 0) {
      name[nameLen] = '\0';
      // workaround pangu2_api issue as the read_dir return abs path
#ifdef USE_PANGU2
      if (stat.is_dir) {
        name[nameLen - 1] = '\0';
        result->push_back(std::string(::basename(name)) + "/");
      }
      else {
        result->push_back(::basename(name));
      }
#else
      result->push_back(name);
#endif
      //result->push_back(std::string(name, nameLen));
    }
    else if (rc < 0) {
      break;
    }
  }
#ifdef USE_PANGU2
  pangu2_close_dir(hdir);
#else
  pangu_close_dir(hdir);
#endif
  if (rc < 0) {
    VLOG_FATAL("GetChildren panguExists call failed");
    throw PanguFatalException("panguExists call failed with error " +
                             NumToString(rc) + ".\n");
  }
  return Status::OK();
}

Status PanguEnv::DeleteFile(const std::string& fname) {
#ifdef USE_PANGU2
  int rc = pangu2_remove(MakeAbsPath(fname).c_str(), 0);
#else
  int rc = pangu_remove(MakeAbsPath(fname).c_str(), 0);
#endif
  if (rc == 0) {
    return Status::OK();
  }
  return IOError(fname, -rc);
};

Status PanguEnv::CreateDir(const std::string& name) {
  std::string absName = MakeAbsPath(name);
  FixDirectoryName(absName);
#ifdef USE_PANGU2
  int rc = pangu2_mkdir(absName.c_str(), 0);
#else
  int rc = pangu_mkdir(absName.c_str(), 0);
#endif
  if (rc == 0) {
    return Status::OK();
  }
  return IOError(name, -rc);
};

Status PanguEnv::CreateDirIfMissing(const std::string& name) {
  std::string absName = MakeAbsPath(name);
  FixDirectoryName(absName);
#ifdef USE_PANGU2
  int rc = pangu2_mkdir(absName.c_str(), 0);
#else
  int rc = pangu_mkdir(absName.c_str(), 0);
#endif
  //  Not atomic. state might change b/w panguExists and CreateDir.
  if (rc != 0 && rc != -EEXIST) {
      VLOG_FATAL("CreateDirIfMissing panguExists call failed");
      throw PanguFatalException("CreateDir call failed with error " +
                               NumToString(rc) + ".\n");
  }
  return Status::OK();
};

Status PanguEnv::DeleteDir(const std::string& name) {
  std::string absName = MakeAbsPath(name);
  FixDirectoryName(absName);
#ifdef USE_PANGU2
  int rc = pangu2_rmdir(absName.c_str());
#else
  int rc = pangu_rmdir(absName.c_str(), 0);
#endif
  if (rc != 0 && rc != -ENOENT) {
      VLOG_FATAL("DeleteDir call failed");
      throw PanguFatalException("DeleteDir call failed with error " +
                               NumToString(rc) + ".\n");
  }
  return Status::OK();
};

Status PanguEnv::GetFileSize(const std::string& fname, uint64_t* size) {
  *size = 0L;
  file_status_t stat;
#ifdef USE_PANGU2
  int rc = pangu2_get_status(MakeAbsPath(fname).c_str(), &stat);
#else
  int rc = pangu_get_status(MakeAbsPath(fname).c_str(), &stat);
#endif
  if (rc == 0) {
    *size = stat.file_length;
    return Status::OK();
  }
  return IOError(fname, -rc);
}

Status PanguEnv::GetFileModificationTime(const std::string& fname,
                                        uint64_t* time) {
  file_status_t stat;
#ifdef USE_PANGU2
  int rc = pangu2_get_status(MakeAbsPath(fname).c_str(), &stat);
#else
  int rc = pangu_get_status(MakeAbsPath(fname).c_str(), &stat);
#endif
  if (rc == 0) {
    *time = stat.modified_time;
    return Status::OK();
  }
  return IOError(fname, -rc);

}

// The rename is not atomic. Pangu does not allow a renaming if the
// target already exists. So, we delete the target before attempting the
// rename.
Status PanguEnv::RenameFile(const std::string& src, const std::string& target) {
  std::string srcPath = MakeAbsPath(src);
  std::string dstPath = MakeAbsPath(target);

#ifdef USE_PANGU2
  pangu2_remove(dstPath.c_str(), 0);
  int rc = pangu2_rename(srcPath.c_str(), dstPath.c_str());
#else
  pangu_remove(dstPath.c_str(), 0);
  int rc = pangu_rename_file(srcPath.c_str(), dstPath.c_str());
#endif
  if (rc == 0) {
    return Status::OK();
  }
  return IOError(src, -rc);
}

Status PanguEnv::SealFile(const std::string& fname) {
  std::string absPath = MakeAbsPath(fname);

#ifdef USE_PANGU2
  int rc = pangu2_seal_file(absPath.c_str());
#else
  int rc = 0; // TODO
#endif
  if (rc == 0) {
    return Status::OK();
  }
  return IOError(fname, -rc);
}

Status PanguEnv::LinkFile(const std::string& src, const std::string& target) {
  std::string srcPath = MakeAbsPath(src);
  std::string dstPath = MakeAbsPath(target);

#ifdef USE_PANGU2
  int rc = pangu2_link(srcPath.c_str(), dstPath.c_str());
#else
  int rc = pangu_link(srcPath.c_str(), dstPath.c_str());
#endif

  if (rc == 0) {
    return Status::OK();
  }
  return IOError(src, -rc);
}

Status PanguEnv::LockFile(const std::string& fname, FileLock** lock) {
  // there isn's a very good way to atomically check and create
  // a file via libpangu
  *lock = nullptr;
  return Status::OK();
}

Status PanguEnv::UnlockFile(FileLock* lock) {
  return Status::OK();
}

// The factory method for creating an Pangu Env
Status NewPanguEnv(unique_ptr<Env>* pangu_env, const std::string& fsname) {
  pangu_env->reset(new PanguEnv(fsname));
  return Status::OK();
}

}  // namespace zdfs
