//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cmath>
#include <limits>
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k), frames_(num_frames), locks_(num_frames) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::shared_mutex> lk(mlatch_);
  if (curr_size_ <= 0) {
    return false;
  }

  int id = -1;
  double duration = std::numeric_limits<double>::infinity();
  for (int i = 0; i < static_cast<int>(replacer_size_); i++) {
    //    std::scoped_lock<std::mutex> lock()
    if (frames_[i].is_used_ && frames_[i].evicted_) {
      if (frames_[i].times_.size() < k_) {
        if (std::isinf(duration)) {
          if (id == -1) {
            id = i;
          } else {
            if (frames_[i].times_.front() < frames_[id].times_.front()) {
              id = i;
            }
          }
        } else {
          id = i;
        }
        duration = std::numeric_limits<double>::infinity();
      } else {
        //        std::chrono::duration<double> elapsed_seconds = std::chrono::steady_clock::now() -
        //        frames[i].times.front(); double t = elapsed_seconds.count();
        double t = current_timestamp_ - frames_[i].times_.front();
        if (id == -1) {
          id = i;
          duration = t;
        } else {
          if (t > duration) {
            duration = t;
            id = i;
          }
        }
      }
    }
  }

  *frame_id = id;

  while (!frames_[id].times_.empty()) {
    frames_[id].times_.pop();
  }
  frames_[id].evicted_ = false;
  frames_[id].is_used_ = false;
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::shared_lock<std::shared_mutex> lk(mlatch_);
  //  std::scoped_lock<std::mutex> lock(locks_.at(frame_id));
  if (frame_id >= static_cast<int>(replacer_size_) || frame_id < 0) {
    throw "invalid id";
  }

  std::scoped_lock<std::mutex> lock(locks_.at(frame_id));
  if (!frames_[frame_id].is_used_) {
    frames_[frame_id].is_used_ = true;
  }

  if (frames_[frame_id].times_.size() >= k_) {
    frames_[frame_id].times_.pop();
  }

  frames_[frame_id].times_.push(current_timestamp_);
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::shared_lock<std::shared_mutex> lk(mlatch_);
  if (frame_id >= static_cast<int>(replacer_size_) || frame_id < 0) {
    throw "invalid id";
  }

  std::scoped_lock<std::mutex> lock(locks_.at(frame_id));

  if (!frames_[frame_id].is_used_) {
    return;
  }

  if (!frames_[frame_id].evicted_ && set_evictable) {
    curr_size_++;
  } else if (frames_[frame_id].evicted_ && !set_evictable) {
    curr_size_--;
  }

  //  {
  //    std::scoped_lock<std::mutex> lk1(latch_);
  //    if (!frames_[frame_id].evicted_ && set_evictable) {
  //      curr_size_++;
  //    } else if (frames_[frame_id].evicted_ && !set_evictable) {
  //      curr_size_--;
  //    }
  //  }

  frames_[frame_id].evicted_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::shared_lock<std::shared_mutex> lk(mlatch_);
  //  std::scoped_lock<std::mutex> lk(locks_.at(frame_id));
  if (frame_id < 0 || frame_id >= static_cast<int>(replacer_size_)) {
    return;
  }

  std::scoped_lock<std::mutex> lock(locks_.at(frame_id));

  if (!frames_[frame_id].is_used_) {
    return;
  }

  if (!frames_[frame_id].evicted_) {
    throw "cannot remove";
  }

  while (!frames_[frame_id].times_.empty()) {
    frames_[frame_id].times_.pop();
  }
  frames_[frame_id].evicted_ = false;
  frames_[frame_id].is_used_ = false;

  curr_size_--;
  //  {
  //    std::scoped_lock<std::mutex> lk1(latch_);
  //    curr_size_--;
  //  }
}

auto LRUKReplacer::Size() -> size_t {
  std::shared_lock<std::shared_mutex> lk(mlatch_);
  return curr_size_;
}

}  // namespace bustub
