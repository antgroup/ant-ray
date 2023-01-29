#ifndef RAY_STREAMING_MESSAGE_MERGER_H
#define RAY_STREAMING_MESSAGE_MERGER_H

#include <algorithm>
#include <functional>
#include <memory>
#include <unordered_set>
#include <vector>

#include "logging.h"

namespace ray {
namespace streaming {

template <class T, class C, class I, int N = 1>

class PriorityQueue {
 private:
  typedef std::function<I(const T &)> IndexFunction;
  std::vector<T> merge_vec_;
  C comparator_;
  IndexFunction index_func_;
  std::unordered_map<I, uint32_t> unique_set_;
  uint32_t max_hold_count_;

 public:
  PriorityQueue(C &comparator, IndexFunction index_fun, uint32_t max_hold_count = N)
      : comparator_(comparator),
        index_func_(index_fun),
        max_hold_count_(max_hold_count){};

  inline void push(const T &item) {
    auto index = index_func_(item);
    auto &hold_count = unique_set_[index];
    STREAMING_CHECK(hold_count < max_hold_count_)
        << "Duplicated item index in merger. " << index;
    merge_vec_.push_back(item);
    std::push_heap(merge_vec_.begin(), merge_vec_.end(), comparator_);
    hold_count++;
  }

  inline void pop() {
    STREAMING_CHECK(!isEmpty());
    unique_set_[index_func_(this->top())]--;
    std::pop_heap(merge_vec_.begin(), merge_vec_.end(), comparator_);
    merge_vec_.pop_back();
  }

  inline void remove(const I &index) {
    merge_vec_.erase(std::remove_if(
        merge_vec_.begin(), merge_vec_.end(),
        [&index, this](const T &item) { return index_func_(item) == index; }));
    unique_set_.erase(index);
    makeHeap();
  }

  inline T &top() { return merge_vec_.front(); }

  inline uint32_t size() { return merge_vec_.size(); }

  inline bool isEmpty() { return merge_vec_.empty(); }
  inline const std::vector<T> &getRawVector() const { return merge_vec_; }

 private:
  inline void makeHeap() {
    std::make_heap(merge_vec_.begin(), merge_vec_.end(), comparator_);
  }
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_MESSAGE_MERGER_H
