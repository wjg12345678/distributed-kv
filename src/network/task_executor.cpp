#include "network/task_executor.h"

#include <algorithm>
#include <utility>

TaskExecutor::TaskExecutor(std::size_t worker_count) {
  const std::size_t actual_worker_count = worker_count == 0
      ? std::max<std::size_t>(2, std::thread::hardware_concurrency())
      : worker_count;
  workers_.reserve(actual_worker_count);
  for (std::size_t i = 0; i < actual_worker_count; ++i) {
    workers_.emplace_back([this]() {
      WorkerLoop();
    });
  }
}

TaskExecutor::~TaskExecutor() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
  }
  condition_.notify_all();
  for (auto& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
}

void TaskExecutor::Submit(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) {
      return;
    }
    tasks_.push(std::move(task));
  }
  condition_.notify_one();
}

void TaskExecutor::WorkerLoop() {
  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait(lock, [this]() {
        return stopping_ || !tasks_.empty();
      });
      if (stopping_ && tasks_.empty()) {
        return;
      }
      task = std::move(tasks_.front());
      tasks_.pop();
    }

    try {
      task();
    } catch (...) {
    }
  }
}
