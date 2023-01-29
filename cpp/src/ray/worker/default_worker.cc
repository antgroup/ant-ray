
#include <ray/api.h>

int main(int argc, char **argv) {
  ray::RayConfigCpp config;
  config.is_worker = true;
  ray::Init(config, argc, argv);
  ray::RunTaskExecutionLoop();
  return 0;
}
