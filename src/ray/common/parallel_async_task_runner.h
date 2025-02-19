#pragma once
#include <functional>
#include <iostream>
#include <memory>
#include <queue>
#include <tuple>
#include <type_traits>
#include <vector>

namespace ray {

template <class Ret, class... Args>
class AsyncParallelRunner;

template <class Ret, class... Args>
class AsyncTask : public std::enable_shared_from_this<AsyncTask<Ret, Args...>> {
 private:
  using CB = std::function<Ret(Args...)>;
  using TaskQueue = std::queue<std::shared_ptr<AsyncTask<Ret, Args...>>>;
  std::function<void(const CB &)> t;
  CB consumer;

 public:
  AsyncTask(std::function<void(const CB &)> &&t_, CB &&consumer_)
      : t(std::move(t_)), consumer(std::move(consumer_)) {}

  AsyncTask(AsyncTask &&) = default;
  AsyncTask &operator=(AsyncTask &&) = default;
  AsyncTask(const AsyncTask &) = delete;
  AsyncTask &operator=(const AsyncTask &) = delete;

  template <typename U = Ret>
  typename std::enable_if<!std::is_void<U>::value, void>::type exec(
      std::function<void()> &&cb) {
    auto self = this->shared_from_this();
    CB wrapper_consumer = [self, cb = std::move(cb)](Args... args) -> Ret {
      Ret ret = self->consumer(std::forward<Args>(args)...);
      cb();
      return ret;
    };
    t(wrapper_consumer);
  }

  template <typename U = Ret>
  typename std::enable_if<std::is_void<U>::value, void>::type exec(
      std::function<void()> &&cb) {
    auto self = this->shared_from_this();
    CB wrapper_consumer = [self, cb = std::move(cb)](Args... args) {
      self->consumer(std::forward<Args>(args)...);
      cb();
    };
    t(wrapper_consumer);
  }
};

template <class Ret, class... Args>
class AsyncTaskRunner
    : public std::enable_shared_from_this<AsyncTaskRunner<Ret, Args...>> {
 private:
  using TaskQueue = std::queue<std::shared_ptr<AsyncTask<Ret, Args...>>>;
  std::shared_ptr<TaskQueue> q;

 public:
  AsyncTaskRunner(std::shared_ptr<TaskQueue> q_) : q(q_) {}
  void consume() {
    if (q->empty()) {
      return;
    }
    auto task_ptr = std::move(q->front());
    q->pop();
    if (task_ptr) {
      auto self = this->shared_from_this();
      task_ptr->exec([self]() { self->consume(); });
    }
  }
};

template <class Ret, class... Args>
class AsyncParallelRunner {
 private:
  using TaskQueue = std::queue<std::shared_ptr<AsyncTask<Ret, Args...>>>;
  std::vector<std::shared_ptr<AsyncTaskRunner<Ret, Args...>>> runners;
  size_t parallel;
  std::shared_ptr<TaskQueue> q;

 public:
  using CB = std::function<Ret(Args...)>;

  AsyncParallelRunner(size_t parallel_) {
    q = std::make_shared<TaskQueue>();
    if (parallel_ > 0) {
      parallel = parallel_;
    } else {
      parallel = 1;
    }
    for (size_t i = 0; i < parallel; ++i) {
      runners.emplace_back(std::make_shared<AsyncTaskRunner<Ret, Args...>>(q));
    }
  }

  void addTask(std::function<void(const CB &)> &&t, CB &&consumer) {
    auto task =
        std::make_shared<AsyncTask<Ret, Args...>>(std::move(t), std::move(consumer));
    q->push(std::move(task));
  }

  void run() {
    for (auto &runner : runners) {
      runner->consume();
    }
  }
};

}  // namespace ray
