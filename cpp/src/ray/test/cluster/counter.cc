
#include "counter.h"

#ifdef _WIN32
#include "windows.h"
#else
#include "signal.h"
#include "unistd.h"
#endif

Counter::Counter(int init) {
  count = init;
  is_restared = ray::WasCurrentActorRestarted();
}

Counter *Counter::FactoryCreate() { return new Counter(0); }

Counter *Counter::FactoryCreate(int init) { return new Counter(init); }

Counter *Counter::FactoryCreate(int init1, int init2) {
  return new Counter(init1 + init2);
}

int Counter::Plus1() {
  count += 1;
  return count;
}

int Counter::Add(int x) {
  count += x;
  return count;
}

int Counter::Exit() {
  ray::ExitActor();
  return 1;
}

std::string Counter::GetJobDataDir() { return ray::GetJobDataDir(); }

std::string Counter::GetEnv(std::string env_name) {
  const char *env_p = std::getenv(env_name.c_str());
  std::string env;
  if (env_p) {
    env = env_p;
  }
  return env;
}

bool Counter::IsProcessAlive(uint64_t pid) {
#ifdef _WIN32
  auto process = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, pid);
  if (process == NULL) {
    return false;
  }

  CloseHandle(process);
  return true;
#else
  if (kill(pid, 0) == -1 && errno == ESRCH) {
    return false;
  }
  return true;
#endif
}

uint64_t Counter::GetPid() {
#ifdef _WIN32
  return GetCurrentProcessId();
#else
  return getpid();
#endif
}

bool Counter::CheckRestartInActorCreationTask() { return is_restared; }

bool Counter::CheckRestartInActorTask() { return ray::WasCurrentActorRestarted(); }

ray::ActorHandleCpp<Counter> Counter::CreateChildActor(std::string actor_name) {
  auto child_actor =
      ray::Actor(RAY_FUNC(Counter::FactoryCreate)).SetName(actor_name).Remote();
  child_actor.Task(&Counter::GetCount).Remote().Get();
  return child_actor;
}

std::string Counter::CreateNestedChildActor(std::string actor_name) {
  child_actor = ray::Actor(RAY_FUNC(Counter::FactoryCreate)).SetName(actor_name).Remote();
  child_actor.Task(&Counter::GetCount).Remote().Get();
  return "OK";
}

int Counter::Plus1ForActor(ray::ActorHandleCpp<Counter> actor) {
  return *actor.Task(&Counter::Plus1).Remote().Get();
}

std::string Counter::GetNamespaceInActor() { return ray::GetNamespace(); }

RAY_REMOTE(RAY_FUNC(Counter::FactoryCreate), RAY_FUNC(Counter::FactoryCreate, int),
           RAY_FUNC(Counter::FactoryCreate, int, int), &Counter::Plus1, &Counter::Add,
           &Counter::Exit, &Counter::GetPid, &Counter::ExceptionFunc,
           &Counter::CheckRestartInActorCreationTask, &Counter::CheckRestartInActorTask,
           &Counter::GetVal, &Counter::GetIntVal, &Counter::GetJobDataDir,
           &Counter::GetEnv, &Counter::GetBytes, &Counter::Initialized,
           &Counter::CreateChildActor, &Counter::Plus1ForActor, &Counter::GetCount,
           &Counter::CreateNestedChildActor, &Counter::echoBytes, &Counter::echoString,
           &Counter::GetNamespaceInActor);

RAY_REMOTE(ActorConcurrentCall::FactoryCreate, &ActorConcurrentCall::CountDown);
