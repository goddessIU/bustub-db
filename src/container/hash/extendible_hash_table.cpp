//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1), entries_(1) {
  dir_.push_back(std::make_shared<ExtendibleHashTable<K, V>::Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() -> int {
  rwlatch_.RLock();
  int res = GetGlobalDepthInternal();
  rwlatch_.RUnlock();
  return res;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) -> int {
  //  std::scoped_lock<std::mutex> lock(latch_);
  rwlatch_.RLock();
  int res = GetLocalDepthInternal(dir_index);
  rwlatch_.RUnlock();
  return res;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::AddGlobalDepthInternal() -> void {
  global_depth_++;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::AddLocalDepthInternal(int dir_index) -> void {
  dir_[dir_index]->IncrementDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() -> int {
  //  std::scoped_lock<std::mutex> lock(latch_);
  rwlatch_.RLock();
  int res = GetNumBucketsInternal();
  rwlatch_.RUnlock();
  return res;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::SetBucketNumInternal() -> void {
  int num = entries_ * 2;
  dir_.resize(num);
  for (int i = entries_; i < num; i++) {
    dir_[i] = dir_[i - entries_];
  }
  entries_ = num;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  //  std::scoped_lock<std::mutex> lock(latch_);
  rwlatch_.RLock();
  auto index = IndexOf(key);

  bool res = dir_.at(index)->Find(key, value);
  rwlatch_.RUnlock();
  return res;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  //  std::scoped_lock<std::mutex> lock(latch_);
  rwlatch_.RLock();
  auto index = IndexOf(key);

  bool res = dir_.at(index)->Remove(key);
  rwlatch_.RUnlock();

  return res;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  rwlatch_.RLock();
  auto index = IndexOf(key);

  if (dir_.at(index)->Insert(key, value)) {
    rwlatch_.RUnlock();
    return;
  }
  rwlatch_.RUnlock();

  rwlatch_.WLock();
  index = IndexOf(key);

  if (dir_.at(index)->Insert(key, value)) {
    rwlatch_.WUnlock();
    return;
  }

  if (GetGlobalDepthInternal() == GetLocalDepthInternal(index)) {
    AddGlobalDepthInternal();
    SetBucketNumInternal();
  }

  AddLocalDepthInternal(index);
  std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> ptr = dir_[index];
  std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> nptr =
      std::make_shared<ExtendibleHashTable<K, V>::Bucket>(bucket_size_, dir_[index]->GetDepth());
  num_buckets_++;
  int mask = (1 << GetLocalDepthInternal(index)) - 1;
  int idx = index & mask;
  for (int i = 0; i < entries_; i++) {
    int d = i & mask;
    if (d == idx) {
      dir_[i] = nptr;
    }
  }

  RedistributeBucket(ptr, index);
  rwlatch_.WUnlock();
  Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  std::shared_lock<std::shared_mutex> b_lock(bucket_latch_);
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  std::scoped_lock<std::shared_mutex> b_lock(bucket_latch_);
  auto it = list_.begin();
  while (it != list_.end()) {
    if (it->first == key) {
      it = list_.erase(it);
      return true;
    }
    it++;
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  std::scoped_lock<std::shared_mutex> b_lock(bucket_latch_);
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }

  if (IsFull()) {
    return false;
  }

  list_.emplace_back(key, value);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket, int idx) -> void {
  auto &items = bucket->GetItems();
  for (auto it = items.begin(); it != items.end();) {
    // deal it with grammar
    int index = IndexOf(it->first);
    if (dir_.at(index) != bucket) {
      dir_.at(index)->Insert(it->first, it->second);
      it = items.erase(it);
    } else {
      it++;
    }
  }
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
