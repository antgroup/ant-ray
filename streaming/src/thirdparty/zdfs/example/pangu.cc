#include "env/env.h"
#include "env/pangu1_api.h"
#include <string>
#include <iostream>
#include <vector>
#include <getopt.h>
#include <cstdio>
#include <ctime>

using namespace zdfs;
using namespace std;

void Usage(char* cmd) {
  cout << "Usage: ";
  cout << cmd << " -l list files\n"
       << " -r remove dir\n"
       << " -d remove file\n"
       << " -c create dir" << endl;
}

int main(int argc, char** argv) {
  std::unique_ptr<Env> panguEnv;
  std::string clusterName = "pangu://localcluster";
  Status status = NewPanguEnv(&panguEnv, clusterName);
  if (!status.ok()) {
      std::cerr << status.ToString() << std::endl; 
      return 1;
  }
  int c;
  while ((c = getopt(argc, argv, "l:r:c:d:")) != -1) {
    switch (c) {
      case 'l': {
          std::string path(optarg);
          std::vector<std::string> path_vec;
          status = panguEnv->GetChildren(path, &path_vec);
          if (!status.ok()) {
              std::cerr << status.ToString() << std::endl; 
              return 1;
          }
          for(auto &item : path_vec) {
            std::string child_file = path + "/" + item;
            uint64_t file_len = 0;
            status = panguEnv->GetFileSize(child_file, &file_len);
            uint64_t file_time = 0;
            status = panguEnv->GetFileModificationTime(child_file, &file_time);
            time_t ts = file_time;
            cout << asctime(gmtime(&ts)) << item << " " << file_len << " " << endl;
          }
        }
        break;
      case 'r': {
          std::string path(optarg);
          status = panguEnv->DeleteDir(path);
          if (status.ok()) {
            cout << path << " remove succ" << endl;
          } else {
            cout << path << " remove failed" << endl;
          }
        }
        break;
      case 'c': {
        std::string path(optarg);
        status = panguEnv->CreateDirIfMissing(path);
        if (status.ok()) {
          cout << path << " create dir succ" << endl;
        } else {
          cout << path << " create dir failed" << endl;
        }
        break;
        }
      case 'd' : {
        std::string path(optarg);
        status = panguEnv->DeleteFile(path);
        if (status.ok()) {
          cout << path << " delete file succ" << endl;
        } else {
          cout << path << " delete file failed" << endl;
        }
        break;
      }
      default:
        Usage(argv[0]);
        exit(-1);
    }
  }
}
