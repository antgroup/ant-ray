
#include "eventer.h"

#include <filesystem>
#include <fstream>
#include <thread>

Eventer::Eventer() {}

Eventer *Eventer::FactoryCreate() { return new Eventer(); }

bool Eventer::WriteEvent() {
  ray::ReportEvent(ray::EventSeverity::INFO, "PIPELINE_2",
                   "running increment function in the Eventer Actor");
  return true;
}

bool Eventer::WriteEventInPrivateThread() {
  std::thread private_thread = std::thread([]() {
    ray::ReportEvent(ray::EventSeverity::INFO, "PIPELINE_2",
                     "running increment function in the Eventer Actor in private thread");
  });
  private_thread.join();
  return true;
}

int Eventer::SearchEvent(const std::string &path, const std::string &label,
                         const std::string &msg) {
  return Search(path, label, msg);
}

RAY_REMOTE(RAY_FUNC(Eventer::FactoryCreate), &Eventer::WriteEvent,
           &Eventer::WriteEventInPrivateThread, &Eventer::SearchEvent);

int Search(const std::string &path, const std::string &label, const std::string &msg) {
  int count = 0;
  for (const auto &entry : std::filesystem::directory_iterator(path)) {
    RAYLOG(INFO) << entry.path().filename().string();
    if (entry.path().filename().string().find("CORE_WORKER") != std::string::npos) {
      std::string line;
      std::ifstream file;
      file.open(entry.path());

      if (file.is_open()) {
        while (!file.eof()) {
          getline(file, line);
          if (line.find(msg) != std::string::npos &&
              line.find(label, 0) != std::string::npos) {
            count++;
          }
        }
        file.close();
      }
    }
  }
  return count;
}