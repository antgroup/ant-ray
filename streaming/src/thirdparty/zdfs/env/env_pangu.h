//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once
#include <algorithm>
#include <stdio.h>
#include <time.h>
#include <iostream>
#include "env.h"
#include "util/status.h"

#ifdef USE_PANGU2
#include "pangu2_api.h"
#else
#include "pangu1_api.h"
#endif

namespace zdfs {

// Thrown during execution when there is an issue with the supplied
// arguments.
class PanguUsageException : public std::exception { };

// A simple exception that indicates something went wrong that is not
// recoverable.  The intention is for the message to be printed (with
// nothing else) and the process terminate.
class PanguFatalException : public std::exception {
public:
  explicit PanguFatalException(const std::string& s) : what_(s) { }
  virtual ~PanguFatalException() throw() { }
  virtual const char* what() const throw() {
    return what_.c_str();
  }
private:
  const std::string what_;
};


// Options while opening a pangu file to read/write
struct PanguEnvOptions : public EnvOptions {

  // Construct with default Options
  PanguEnvOptions() : EnvOptions() {}
  virtual ~PanguEnvOptions() {}

  // 1 is STAR write, 2 is Y write
  uint32_t write_mode = 1;

  // file copys or EC columns
  uint32_t copys = 3;

  // copys or columns to tolerant
  uint32_t ftt = 1;

  // file type, should be normal file, log file,
  // flat log file and EC file
  uint32_t file_type = 0;

  // placement for the replica
  uint32_t placement = 0;
};

//
// The Pangu environment. This class overrides all the
// file/dir access methods
//
class PanguEnv : public Env {

 public:
  explicit PanguEnv(const std::string& fsname) : fsname_(fsname) {
#ifdef USE_PANGU2
  #ifdef PANGU_SDK_DYNAMIC_LOAD
    pangu2_init_lib();
  #endif
    pangu2_init(fsname.c_str(), 0);
#else
  #ifdef PANGU_SDK_DYNAMIC_LOAD
    pangu_init_lib();
  #endif
    pangu_init(fsname.c_str(), 0);
    std::string vhost = GenPanguDockerHost();
    pangu_set_flag("pangu_client_HostName", vhost.c_str(), vhost.length());
#endif
  }

  virtual ~PanguEnv() {
    fprintf(stderr, "Destroying PanguEnv::Default()\n");
#ifdef USE_PANGU2
    pangu2_uninit();
  #ifdef PANGU_SDK_DYNAMIC_LOAD
    pangu2_free_lib();
  #endif
#else
    pangu_uninit();
  #ifdef PANGU_SDK_DYNAMIC_LOAD
    pangu_free_lib();
  #endif
#endif
  }

  std::string GenPanguDockerHost();

  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options);

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options);

  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& options);

  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result);

  virtual Status FileExists(const std::string& fname);

  virtual Status GetChildren(const std::string& path,
                             std::vector<std::string>* result);

  virtual Status DeleteFile(const std::string& fname);

  virtual Status CreateDir(const std::string& name);

  virtual Status CreateDirIfMissing(const std::string& name);

  virtual Status DeleteDir(const std::string& name);

  virtual Status GetFileSize(const std::string& fname, uint64_t* size);

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime);

  virtual Status RenameFile(const std::string& src, const std::string& target);

  virtual Status SealFile(const std::string& fname);

  virtual Status LinkFile(const std::string& src, const std::string& target);

  virtual Status LockFile(const std::string& fname, FileLock** lock);

  virtual Status UnlockFile(FileLock* lock);

 private:
  std::string MakeAbsPath(const std::string& fname);
  void FixDirectoryName(std::string& name);
 private:
  std::string fsname_;  // string of the form "pangu://clustername
};

}  // namespace zdfs
