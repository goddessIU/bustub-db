//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  /*
   * * You should pick the replacement frame from either the free list or the replacer (always find from the free list
   * first), and then call the AllocatePage() method to get a new page id.
   */
  std::scoped_lock<std::mutex> lk(latch_);
  frame_id_t fid = -1;
  page_id_t pid = -1;
  //  rwlatch_.WLock();
  if (free_list_.empty()) {
    // If the replacement frame has a dirty page,
    //    * you should write it back to the disk first.
    if (!replacer_->Evict(&fid)) {
      //      rwlatch_.WUnlock();
      return nullptr;
    }

    //    pages_[fid].RLatch();
    page_table_->Remove(pages_[fid].GetPageId());
    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
    }
    //    pages_[fid].RUnlatch();

  } else {
    fid = free_list_.front();
    free_list_.pop_front();
  }

  if (fid == -1) {
    //    rwlatch_.WUnlock();
    return nullptr;
  }
  pid = AllocatePage();

  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  page_table_->Insert(pid, fid);

  //  pages_[fid].WLatch();
  //  rwlatch_.WUnlock();

  ZeroPage(fid);
  pages_[fid].pin_count_++;
  pages_[fid].page_id_ = pid;

  //  pages_[fid].WUnlatch();
  *page_id = pid;
  return &pages_[fid];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // First search for page_id in the buffer pool.
  frame_id_t frame_id;
  std::scoped_lock<std::mutex> lk(latch_);
  //  rwlatch_.WLock();
  if (!page_table_->Find(page_id, frame_id)) {
    // If not found, pick a replacement frame from either the free list or
    //    * the replacer (always find from the free list first), read the page from disk by calling
    //    disk_manager_->ReadPage(),
    //    * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it
    //    back
    //   * to disk and update the metadata of the new page
    //    page_table_->Insert(pid, fid);, do sth to hash table
    frame_id_t fid = -1;
    if (free_list_.empty()) {
      // If the replacement frame has a dirty page,
      //    * you should write it back to the disk first.
      if (!replacer_->Evict(&fid)) {
        //        rwlatch_.WUnlock();
        return nullptr;
      }

      //      pages_[fid].RLatch();
      page_table_->Remove(pages_[fid].GetPageId());
      if (pages_[fid].IsDirty()) {
        disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
      }
      //      pages_[fid].RUnlatch();

    } else {
      fid = free_list_.front();
      free_list_.pop_front();
    }

    if (fid == -1) {
      //      rwlatch_.WUnlock();
      return nullptr;
    }

    //    pages_[fid].WLatch();
    ZeroPage(fid);
    disk_manager_->ReadPage(page_id, pages_[fid].GetData());
    //    pages_[fid].WUnlatch();

    // You also need to reset the memory and metadata for the new page.

    frame_id = fid;
    page_table_->Insert(page_id, frame_id);
  }

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  //  pages_[frame_id].WLatch();
  //  rwlatch_.WUnlock();

  pages_[frame_id].pin_count_++;
  pages_[frame_id].page_id_ = page_id;

  //  pages_[frame_id].WUnlatch();

  return pages_ + frame_id;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  std::scoped_lock<std::mutex> lk(latch_);
  //  rwlatch_.RLock();
  if (!page_table_->Find(page_id, frame_id)) {
    //    rwlatch_.RUnlock();
    return false;
  }

  //  pages_[frame_id].WLatch();
  //  rwlatch_.RUnlock();

  if (pages_[frame_id].GetPinCount() <= 0) {
    //    pages_[frame_id].WUnlatch();
    return false;
  }

  pages_[frame_id].pin_count_--;

  if (!pages_[frame_id].is_dirty_) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }

  if (pages_[frame_id].pin_count_ <= 0) {
    //    pages_[frame_id].WUnlatch();
    //    rwlatch_.WLock();
    replacer_->SetEvictable(frame_id, true);
    //    rwlatch_.WUnlock();
  }
  //  } else {
  //    pages_[frame_id].WUnlatch();
  //  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  std::scoped_lock<std::mutex> lk(latch_);
  //  rwlatch_.RLock();
  if (!page_table_->Find(page_id, frame_id)) {
    //    rwlatch_.RUnlock();
    return false;
  }

  //  pages_[frame_id].WLatch();
  //  rwlatch_.RUnlock();

  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());

  pages_[frame_id].is_dirty_ = false;

  //  pages_[frame_id].WUnlatch();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lk(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    //    pages_[i].WLatch();
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
    //    pages_[i].WUnlatch();
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lk(latch_);
  frame_id_t frame_id;
  //  rwlatch_.WLock();
  if (!page_table_->Find(page_id, frame_id)) {
    //    rwlatch_.WUnlock();
    return true;
  }

  //  pages_[frame_id].RLatch();
  if (pages_[frame_id].GetPinCount() > 0) {
    //    pages_[frame_id].RUnlatch();
    //    rwlatch_.WUnlock();
    return false;
  }
  //  pages_[frame_id].RUnlatch();

  if (!page_table_->Remove(page_id)) {
    //    rwlatch_.WUnlock();
    return false;
  }

  //  pages_[frame_id].WLatch();
  ZeroPage(frame_id);
  //  pages_[frame_id].WUnlatch();

  replacer_->Remove(frame_id);

  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  //  rwlatch_.WUnlock();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::ZeroPage(frame_id_t fid) -> void {
  pages_[fid].ResetMemory();
  pages_[fid].page_id_ = INVALID_PAGE_ID;
  pages_[fid].pin_count_ = 0;
  pages_[fid].is_dirty_ = false;
}
}  // namespace bustub
