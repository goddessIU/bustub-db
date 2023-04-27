//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {
auto LockManager::IsCompatible(std::shared_ptr<LockManager::LockRequestQueue> queue, const table_oid_t &oid,
                               LockMode lock_mode) -> bool {
  int s_num = 0;
  int x_num = 0;
  int is_num = 0;
  int ix_num = 0;
  int six_num = 0;
  for (const auto &t : queue->request_queue_) {
    if (!t->granted_) {
      break;
    }

    if (t->lock_mode_ == LockMode::SHARED) {
      s_num++;
    } else if (t->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
      ix_num++;
    } else if (t->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      six_num++;
    } else if (t->lock_mode_ == LockMode::EXCLUSIVE) {
      x_num++;
    } else if (t->lock_mode_ == LockMode::INTENTION_SHARED) {
      is_num++;
    }
  }

  if (lock_mode == LockMode::SHARED) {
    if (x_num > 0 || ix_num > 0 || six_num > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    if (s_num > 0 || six_num > 0 || x_num > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (ix_num > 0 || six_num > 0 || s_num > 0 || x_num > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    if (ix_num > 0 || six_num > 0 || s_num > 0 || x_num > 0 || is_num > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    if (x_num > 0) {
      return false;
    }
  }

  return true;
}

auto LockManager::IsCompatibleForRow(std::shared_ptr<LockManager::LockRequestQueue> queue, const table_oid_t &oid,
                        LockMode lock_mode, const RID& rid) -> bool {
  int s_num = 0;
  int x_num = 0;
  for (const auto &t : queue->request_queue_) {
    if (!t->granted_) {
      break;
    }

    if (t->lock_mode_ == LockMode::SHARED) {
      s_num++;
    } else if (t->lock_mode_ == LockMode::EXCLUSIVE) {
      x_num++;
    }
  }

  if (lock_mode == LockMode::SHARED) {
    if (x_num > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    if (s_num > 0 || x_num > 0) {
      return false;
    }
  }

  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  std::cout << "lock table " << "txn " << txn->GetTransactionId() << "oid " << oid << "lock mode " << (int)(lock_mode) << std::endl;
  if (txn->GetState() == TransactionState::ABORTED) {
    std::cout << "aborted1 " << txn->GetTransactionId() << std::endl;
    return false;
  }

  // check isolation level
  const auto level = txn->GetIsolationLevel();
  std::cout << "txn " << txn->GetTransactionId() <<  "level is " << (int)(txn->GetIsolationLevel()) << std::endl;
  if (level == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      std::cout << "aborted2 " << txn->GetTransactionId() << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }

    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      std::cout << "aborted15 " << txn->GetTransactionId() << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (level == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      std::cout << "aborted3 " << txn->GetTransactionId() << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (level == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING &&
        !(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED)) {
      txn->SetState(TransactionState::ABORTED);
      std::cout << "aborted4 " << txn->GetTransactionId() << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  // lock upgrade
  bool is_upgrade = false;
  if (txn->IsTableSharedLocked(oid)) {
    if (lock_mode == LockMode::SHARED) {
      std::cout << "111 " << txn->GetTransactionId() << std::endl;
      return true;
    }

    if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      std::cout << "aborted5 " << txn->GetTransactionId() << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    is_upgrade = true;
  } else if (txn->IsTableIntentionSharedLocked(oid)) {
    if (lock_mode == LockMode::INTENTION_SHARED) {
      std::cout << "111 " << txn->GetTransactionId() << std::endl;
      return true;
    }

    is_upgrade = true;
  } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      std::cout << "111 " << txn->GetTransactionId() << std::endl;
      return true;
    }

    if (lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      std::cout << "aborted6 " << txn->GetTransactionId() << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    is_upgrade = true;
  } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
    if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      std::cout << "111 " << txn->GetTransactionId() << std::endl;
      return true;
    }

    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED) {
      txn->SetState(TransactionState::ABORTED);
      std::cout << "aborted7 " << txn->GetTransactionId() << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    is_upgrade = true;
  } else if (txn->IsTableExclusiveLocked(oid)) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      std::cout << "111 " << txn->GetTransactionId() << std::endl;
      return true;
    }

    txn->SetState(TransactionState::ABORTED);
    std::cout << "aborted8 " << txn->GetTransactionId() << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }

  std::unique_lock<std::mutex> table_lock_map_lck(table_lock_map_latch_);

  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }

  // deal queue
  auto &l_queue = table_lock_map_[oid];

  std::unique_lock<std::mutex> lck(l_queue->latch_);
  table_lock_map_lck.unlock();

  if (is_upgrade) {
    if (l_queue->upgrading_ == INVALID_TXN_ID) {
      l_queue->upgrading_ = txn->GetTransactionId();
    } else {
      txn->SetState(TransactionState::ABORTED);
      std::cout << "aborted9 " << txn->GetTransactionId() << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
  }

  auto request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);

  bool can_granted = true;
  for (auto t : l_queue->request_queue_) {
    if (!t->granted_) {
      can_granted = false;
      break;
    }
  }

  if (l_queue->request_queue_.empty() || (can_granted && IsCompatible(l_queue, oid, lock_mode))) {
    request->granted_ = true;
  }

//  std::cout << 2 << std::endl;
  if (is_upgrade) {
    // delete the old
    auto f_itr = l_queue->request_queue_.begin();
    while (f_itr != l_queue->request_queue_.end() && (*f_itr)->granted_) {
      if ((*f_itr)->txn_id_ == txn->GetTransactionId()) {
        if ((*f_itr)->lock_mode_ == LockMode::INTENTION_SHARED) {
          txn->GetIntentionSharedTableLockSet()->erase((*f_itr)->oid_);
        } else if ((*f_itr)->lock_mode_ == LockMode::SHARED) {
          txn->GetSharedTableLockSet()->erase((*f_itr)->oid_);
        }else if ((*f_itr)->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
          txn->GetIntentionExclusiveTableLockSet()->erase((*f_itr)->oid_);
        }else if ((*f_itr)->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->GetExclusiveTableLockSet()->erase((*f_itr)->oid_);
        }else if ((*f_itr)->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
          txn->GetSharedIntentionExclusiveTableLockSet()->erase((*f_itr)->oid_);
        }
        auto m = *f_itr;
        delete m;
        (l_queue->request_queue_).erase(f_itr);
        break;
      }
      f_itr++;
    }
    // insert new
    if (l_queue->request_queue_.empty()) {
      request->granted_ = true;
      l_queue->request_queue_.push_back(request);
//      std::cout << 666 << std::endl;
    } else {
      auto itr = l_queue->request_queue_.begin();
      while (itr != l_queue->request_queue_.end()) {
        if ((*itr)->granted_ == false) {
          break;
        }
        itr++;
      }

      request->granted_ = false;
      l_queue->request_queue_.insert(itr, request);

//      std::cout << "size " << l_queue->request_queue_.size() << std::endl;
    }

  } else {
    l_queue->request_queue_.push_back(request);
//    std::cout << "lock size " << l_queue->request_queue_.size() << std::endl;
  }

  // the grant is false
  while (!(request->granted_)) {
    l_queue->cv_.wait(lck);
  }

  if (is_upgrade) {
    l_queue->upgrading_ = INVALID_TXN_ID;
  }


  // bookkeeping
  if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }


  if (txn->GetState() == TransactionState::ABORTED) {
    std::cout << "aborted10 " << txn->GetTransactionId() << std::endl;
    auto f_itr = l_queue->request_queue_.begin();
    while (f_itr != l_queue->request_queue_.end()) {
      if ((*f_itr)->txn_id_ == txn->GetTransactionId()) {
        if ((*f_itr)->granted_) {
          if ((*f_itr)->lock_mode_ == LockMode::INTENTION_SHARED) {
            txn->GetIntentionSharedTableLockSet()->erase((*f_itr)->oid_);
          } else if ((*f_itr)->lock_mode_ == LockMode::SHARED) {
            txn->GetSharedTableLockSet()->erase((*f_itr)->oid_);
          }else if ((*f_itr)->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
            txn->GetIntentionExclusiveTableLockSet()->erase((*f_itr)->oid_);
          }else if ((*f_itr)->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->GetExclusiveTableLockSet()->erase((*f_itr)->oid_);
          }else if ((*f_itr)->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
            txn->GetSharedIntentionExclusiveTableLockSet()->erase((*f_itr)->oid_);
          }
        }
        auto m = *f_itr;
        delete m;
        f_itr = (l_queue->request_queue_).erase(f_itr);
      } else {
        f_itr++;
      }

    }

    if (!l_queue->request_queue_.empty()) {
      auto t = l_queue->request_queue_.begin();
      while (t != l_queue->request_queue_.end() && (*t)->granted_) {
        t++;
      }

      while (t != l_queue->request_queue_.end() && IsCompatible(l_queue, oid, (*t)->lock_mode_)) {
        (*t)->granted_ = true;
        t++;
      }
    }

    l_queue->cv_.notify_all();
    return false;
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::cout << "unlock table " << "txn " << txn->GetTransactionId() << "oid " << oid  << std::endl;
  std::unique_lock<std::mutex> table_lock_map_lck(table_lock_map_latch_);
//  std::cout << 3 << std::endl;

  if (table_lock_map_.count(oid) == 0) {
    txn->SetState(TransactionState::ABORTED);
    std::cout << "aborted11 " << txn->GetTransactionId() << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  bool is_is_lock = txn->IsTableIntentionSharedLocked(oid);
  bool is_six_lock = txn->IsTableSharedIntentionExclusiveLocked(oid);
  bool is_x_lock = txn->IsTableExclusiveLocked(oid);
  bool is_ix_lock = txn->IsTableIntentionExclusiveLocked(oid);
  bool is_s_lock = txn->IsTableSharedLocked(oid);
  bool has_lock = is_is_lock || is_s_lock || is_six_lock || is_x_lock || is_ix_lock;
  if (!has_lock) {
    txn->SetState(TransactionState::ABORTED);
    std::cout << "aborted12 " << txn->GetTransactionId() << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  bool is_s_row_empty = txn->GetSharedRowLockSet()->count(oid) == 0 || (*(txn->GetSharedRowLockSet()))[oid].empty();
  bool is_x_row_empty =
      txn->GetExclusiveRowLockSet()->count(oid) == 0 || (*(txn->GetExclusiveRowLockSet()))[oid].empty();
  if (!is_s_row_empty || !is_x_row_empty) {
    txn->SetState(TransactionState::ABORTED);
    std::cout << "aborted13 " << txn->GetTransactionId() << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  const auto level = txn->GetIsolationLevel();
  if (level == IsolationLevel::READ_COMMITTED) {
    if (is_x_lock && txn->GetState() == TransactionState::GROWING) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (level == IsolationLevel::REPEATABLE_READ) {
    if ((is_s_lock || is_x_lock) && txn->GetState() == TransactionState::GROWING) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (level == IsolationLevel::READ_UNCOMMITTED) {
    if (is_x_lock && txn->GetState() == TransactionState::GROWING) {
      txn->SetState(TransactionState::SHRINKING);
    } else if (is_s_lock) {
      //      txn->SetState(TransactionState::ABORTED);
      //      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::);
      // is right?
      return false;
    }
  }

  // bookkeeping
  if (is_is_lock) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (is_s_lock) {
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (is_ix_lock) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (is_x_lock) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  } else if (is_six_lock) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }

  auto &l_queue = table_lock_map_[oid];

  std::unique_lock<std::mutex> lck(l_queue->latch_);
  table_lock_map_lck.unlock();

//  std::cout << 2 << std::endl;


  // remove the old request
  auto itr = l_queue->request_queue_.begin();
  while (itr != l_queue->request_queue_.end()) {
    if ((*itr)->txn_id_ == txn->GetTransactionId()) {
      auto m = *itr;
      delete m;
      (l_queue->request_queue_).erase(itr);
      break;
    }
    itr++;
  }

//  std::cout << "ddd" << std::endl;


  // find all can granted request, grant = true
  if (!l_queue->request_queue_.empty()) {
    auto t = l_queue->request_queue_.begin();
    while (t != l_queue->request_queue_.end() && (*t)->granted_) {
      t++;
    }

    while (t != l_queue->request_queue_.end() && IsCompatible(l_queue, oid, (*t)->lock_mode_)) {
      (*t)->granted_ = true;
      t++;
    }
  }

//  std::cout << 5 << std::endl;
//  std::cout << "size is " << l_queue->request_queue_.size() << std::endl;

  l_queue->cv_.notify_all();

//  std::cout << 6 << std::endl;

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  std::cout << "lock row " << "txn " << txn->GetTransactionId() << "oid " << oid << "lock mode " << (int)(lock_mode) << std::endl;
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  } else {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  // check isolation level
  const auto level = txn->GetIsolationLevel();
  if (level == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  } else if (level == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (level == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING &&
        !(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  if (lock_mode == LockMode::SHARED) {
    if (!(txn->IsTableSharedLocked(oid) || txn->IsTableIntentionSharedLocked(oid) ||
          txn->IsTableSharedIntentionExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid))) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    if (!(txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
          txn->IsTableSharedIntentionExclusiveLocked(oid))) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }



  // lock upgrade
  bool is_upgrade = false;
  if (txn->IsRowSharedLocked(oid, rid)) {
    if (lock_mode == LockMode::SHARED) {
      return true;
    }

    is_upgrade = true;
  } else if (txn->IsRowExclusiveLocked(oid, rid)) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      return true;
    }

    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }

  std::unique_lock<std::mutex> row_lock_map_lck(row_lock_map_latch_);

  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }

  // deal queue
  auto &l_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> lck(l_queue->latch_);
  row_lock_map_lck.unlock();

  if (is_upgrade) {
    if (l_queue->upgrading_ == INVALID_TXN_ID) {
      l_queue->upgrading_ = txn->GetTransactionId();
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
  }

  auto request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);


  bool can_granted = true;
  for (auto t : l_queue->request_queue_) {
    if (!t->granted_) {
      can_granted = false;
      break;
    }
  }

  if (l_queue->request_queue_.empty() || (can_granted && IsCompatibleForRow(l_queue, oid, lock_mode, rid))) {
    request->granted_ = true;
  }

  if (is_upgrade) {
    // delete the old
    auto f_itr = l_queue->request_queue_.begin();
    while (f_itr != l_queue->request_queue_.end()) {
      if ((*f_itr)->txn_id_ == txn->GetTransactionId() && (*f_itr)->granted_) {
        if ((*f_itr)->lock_mode_ == LockMode::SHARED) {
          (*(txn->GetSharedRowLockSet()))[oid].erase((*f_itr)->rid_);
        } else if ((*f_itr)->lock_mode_ == LockMode::EXCLUSIVE) {
          (*(txn->GetExclusiveRowLockSet()))[oid].erase((*f_itr)->rid_);
        }
        auto m = *f_itr;
        delete m;
        (l_queue->request_queue_).erase(f_itr);
        break;
      }
      f_itr++;
    }
    // insert new
    if (l_queue->request_queue_.empty()) {
      request->granted_ = true;
      l_queue->request_queue_.push_back(request);
    } else {
      auto itr = l_queue->request_queue_.begin();
      while (itr != l_queue->request_queue_.end()) {
        if ((*itr)->granted_ == false) {
          break;
        }
        itr++;
      }

      request->granted_ = false;
      l_queue->request_queue_.insert(itr, request);
    }

  } else {
    l_queue->request_queue_.push_back(request);
  }

  // the grant is false
  while (!(request->granted_)) {
    l_queue->cv_.wait(lck);
  }

  if (is_upgrade) {
    l_queue->upgrading_ = INVALID_TXN_ID;
  }

  // bookkeeping
  if (lock_mode == LockMode::SHARED) {
    if (txn->GetSharedRowLockSet()->count(oid) == 0) {
      (*(txn->GetSharedRowLockSet()))[oid] = {};
    }

    (*(txn->GetSharedRowLockSet()))[oid].insert(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    if (txn->GetExclusiveRowLockSet()->count(oid) == 0) {
      (*(txn->GetExclusiveRowLockSet()))[oid] = {};
    }

    (*(txn->GetExclusiveRowLockSet()))[oid].insert(rid);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  std::cout << "unlock row " << "txn " << txn->GetTransactionId() << "oid " << oid  << std::endl;

  std::unique_lock<std::mutex> row_lock_map_lck(row_lock_map_latch_);

  if (row_lock_map_.count(rid) == 0) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  bool is_x_lock = txn->IsRowExclusiveLocked(oid, rid);
  bool is_s_lock = txn->IsRowSharedLocked(oid, rid);
  bool has_lock = is_s_lock || is_x_lock;
  if (!has_lock) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  const auto level = txn->GetIsolationLevel();
  if (level == IsolationLevel::READ_COMMITTED) {
    if (is_x_lock && txn->GetState() == TransactionState::GROWING) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (level == IsolationLevel::REPEATABLE_READ) {
    if ((is_s_lock || is_x_lock) && txn->GetState() == TransactionState::GROWING) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (level == IsolationLevel::READ_UNCOMMITTED) {
    if (is_x_lock && txn->GetState() == TransactionState::GROWING) {
      txn->SetState(TransactionState::SHRINKING);
    } else if (is_s_lock) {
      //      txn->SetState(TransactionState::ABORTED);
      //      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::);
      // is right?
      return false;
    }
  }

  // bookkeeping
  if (is_s_lock) {
    (*(txn->GetSharedRowLockSet()))[oid].erase(rid);
  } else if (is_x_lock) {
    (*(txn->GetExclusiveRowLockSet()))[oid].erase(rid);
  }

  auto &l_queue = row_lock_map_[rid];

  std::unique_lock<std::mutex> lck(l_queue->latch_);
  row_lock_map_lck.unlock();

  // remove the old request
  auto itr = l_queue->request_queue_.begin();
  while (itr != l_queue->request_queue_.end()) {
    if ((*itr)->txn_id_ == txn->GetTransactionId()) {
      auto m = *itr;
      delete m;
      (l_queue->request_queue_).erase(itr);
      break;
    }
    itr++;
  }

  // find all can granted request, grant = true
  if (!l_queue->request_queue_.empty()) {
    auto t = l_queue->request_queue_.begin();
    while (t != l_queue->request_queue_.end() && (*t)->granted_) {
      t++;
    }

    while (t != l_queue->request_queue_.end() && IsCompatibleForRow(l_queue, oid, (*t)->lock_mode_, rid)) {
      (*t)->granted_ = true;
      t++;
    }
  }

  l_queue->cv_.notify_all();

  return true;
}


void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
