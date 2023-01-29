#ifndef RAY_STREAMING_ASIO_H
#define RAY_STREAMING_ASIO_H
#include <atomic>
#include <condition_variable>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "logging.h"

#ifdef USE_PANGU
#include "env/env.h"
#include "env/env_pangu.h"
#endif

namespace ray {
namespace streaming {

class StreamingFileIO {
 public:
  virtual void Open() = 0;
  virtual void Write(uint8_t *data, uint32_t data_size) = 0;
  virtual void Read(uint8_t *data, uint32_t data_size) = 0;
  virtual void ReadAll(std::shared_ptr<uint8_t> *data, uint32_t &data_size) = 0;
  virtual void Close() = 0;
  virtual bool Delete() = 0;
  virtual bool Delete(const std::string &filename) = 0;
  virtual bool Rename(const std::string &target_name) = 0;
  virtual bool Rename(const std::string &source_name, const std::string &target_name) = 0;
  virtual bool Exists() = 0;
  virtual bool Exists(const std::string &filename) = 0;
  virtual bool IsDirectory(const std::string &dirname) = 0;
  virtual bool IsDirectory() = 0;
  virtual bool CreateDirectory(const std::string &dirname,
                               bool ChangeDirectory = true) = 0;
  virtual bool CreateDirectory() = 0;
  virtual bool DeleteDirectory(const std::string &dirname) = 0;
  virtual bool DeleteDirectory() = 0;
  virtual bool CreateSubDirectory(const std::string &dirname) = 0;
  virtual bool RenameSubDirectory(const std::string &dirname,
                                  const std::string &newname) = 0;
  virtual bool ExistsSubDirectory(const std::string &dirname) = 0;
  virtual bool DeleteSubDirectory(const std::string &dirname) = 0;
  virtual const std::string &GetFileName() = 0;
  inline void TransToDir(std::string &dirname) {
    if (dirname.empty() || dirname[dirname.length() - 1] != '/') {
      dirname += "/";
    }
  }
};

class StreamingLocalFileSystem : public StreamingFileIO {
 public:
  explicit StreamingLocalFileSystem(const std::string &file_path, bool is_reader = false);
  virtual ~StreamingLocalFileSystem();
  void Open() override;
  void Write(uint8_t *data, uint32_t data_size) override;
  void Read(uint8_t *data, uint32_t data_size) override;
  void ReadAll(std::shared_ptr<uint8_t> *data, uint32_t &data_size) override;
  void Close() override;
  bool Delete() override;
  bool Delete(const std::string &filename) override;
  bool Rename(const std::string &target_name) override;
  bool Rename(const std::string &source_name, const std::string &target_name) override;
  bool Exists() override;
  bool Exists(const std::string &filename) override;
  bool IsDirectory(const std::string &directoryname) override;
  bool IsDirectory() override;
  bool CreateDirectory(const std::string &dirname, bool ChangeDirectory = true) override;
  bool CreateDirectory() override;
  bool DeleteDirectory(const std::string &dirname) override;
  bool DeleteDirectory() override;
  bool CreateSubDirectory(const std::string &dirname) override;
  bool RenameSubDirectory(const std::string &dirname,
                          const std::string &newname) override;
  bool ExistsSubDirectory(const std::string &dirname) override;
  bool DeleteSubDirectory(const std::string &dirname) override;
  inline const std::string &GetFileName() override { return file_path_; };

 private:
  bool is_reader_;
  std::string file_path_;
  std::fstream file_;
};

#ifdef USE_PANGU
class StreamingPanguFileSystem : public StreamingFileIO {
 public:
  explicit StreamingPanguFileSystem(const std::string &file_path, bool is_reader = false);
  virtual ~StreamingPanguFileSystem();
  static void Init(
      const std::string &cluster_name = "pangu://localcluster",
      const std::string &pangu_log_conf_path = "/apsara/conf/apsara_log_conf.json");
  static void Destory();
  void Open() override;
  void Write(uint8_t *data, uint32_t data_size) override;
  void Read(uint8_t *data, uint32_t data_size) override;
  void ReadAll(std::shared_ptr<uint8_t> *data, uint32_t &data_size) override;
  void Close() override;
  bool Delete() override;
  bool Delete(const std::string &filename) override;
  // Pangu client don't support rename directory (only file works)
  bool Rename(const std::string &target_name) override;
  bool Rename(const std::string &source_name, const std::string &target_name) override;
  bool Exists() override;
  bool Exists(const std::string &filename) override;
  bool IsDirectory(const std::string &dirname) override;
  bool IsDirectory() override;
  bool CreateDirectory(const std::string &dirname, bool ChangeDirectory = true) override;
  bool CreateDirectory() override;
  bool DeleteDirectory(const std::string &dirname) override;
  bool DeleteDirectory() override;
  bool CreateSubDirectory(const std::string &dirname) override;
  bool RenameSubDirectory(const std::string &dirname,
                          const std::string &newname) override;
  bool ExistsSubDirectory(const std::string &dirname) override;
  bool DeleteSubDirectory(const std::string &dirname) override;
  inline const std::string &GetFileName() override { return file_path_; };

 private:
  std::string file_path_;
  bool is_reader_;
  static std::unique_ptr<zdfs::Env> pangu_env_;
  static std::atomic<uint32_t> pangu_env_ref_count_;
  std::unique_ptr<zdfs::WritableFile> write_file_;
  std::unique_ptr<zdfs::RandomAccessFile> read_file_;
};
#endif
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_ASIO_H
