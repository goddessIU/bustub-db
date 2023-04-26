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

auto LockManager::CanGetLock(std::shared_ptr<LockManager::LockRequestQueue> queue, const table_oid_t &oid,
                             LockMode lock_mode) -> bool {
  //  if (table_lock_map_.count(oid) == 0) {
  //    return false;
  //  }
  //
  //  const auto& queue = table_lock_map_[oid];
  const auto &mapper = queue->lck_counter_;

  if (lock_mode == LockMode::SHARED) {
    if (mapper[1] > 0 || mapper[3] > 0 || mapper[4] > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    if (mapper[4] > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    for (auto t : mapper) {
      if (t > 0) {
        return false;
      }
    }
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    if (mapper[2] > 0 || mapper[3] > 0 || mapper[4] > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (mapper[1] > 0 || mapper[2] > 0 || mapper[3] > 0 || mapper[4] > 0) {
      return false;
    }
  }

  return true;
}

auto LockManager::CanGetLockForRow(std::shared_ptr<LockManager::LockRequestQueue> queue, const table_oid_t &oid,
                                   LockMode lock_mode, const RID &rid) -> bool {
  //  if (row_lock_map_.count(rid) == 0) {
  //    return false;
  //  }
  //
  //  const auto &queue = row_lock_map_[rid];
  const auto &mapper = queue->lck_counter_;

  if (lock_mode == LockMode::SHARED) {
    if (mapper[4] > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    if (mapper[2] > 0 || mapper[4] > 0) {
      return false;
    }
  }

  return true;
}

auto LockManager::CanUpgrade(std::shared_ptr<LockManager::LockRequestQueue> queue, const table_oid_t &oid,
                             LockMode lock_mode, txn_id_t tid) -> bool {
  //  const auto& queue = table_lock_map_[oid];
  const auto &mapper = queue->lck_counter_;

  if (tid != queue->upgrading_) {
    return false;
  }

  int is_num = mapper[0];
  int ix_num = mapper[1];
  int s_num = mapper[2];
  int six_num = mapper[3];
  int x_num = mapper[4];

  if (queue->old_mode == LockMode::SHARED) {
    s_num--;
  } else if (queue->old_mode == LockMode::INTENTION_SHARED) {
    is_num--;
  } else if (queue->old_mode == LockMode::EXCLUSIVE) {
    x_num--;
  } else if (queue->old_mode == LockMode::INTENTION_EXCLUSIVE) {
    ix_num--;
  } else if (queue->old_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    six_num--;
  }

  if (lock_mode == LockMode::SHARED) {
    if (ix_num > 0 || six_num > 0 || x_num > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    if (x_num > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    if (ix_num > 0 || six_num > 0 || x_num > 0 || s_num > 0 || is_num > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    if (s_num > 0 || six_num > 0 || x_num > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (s_num > 0 || six_num > 0 || x_num > 0 || ix_num > 0) {
      return false;
    }
  }

  return true;
}

auto LockManager::CanUpgradeForRow(std::shared_ptr<LockManager::LockRequestQueue> queue, const table_oid_t &oid,
                                   LockMode lock_mode, txn_id_t tid, const RID &rid) -> bool {
  const auto &mapper = queue->lck_counter_;

  if (tid != queue->upgrading_) {
    return false;
  }

  int s_num = mapper[2];
  int x_num = mapper[4];

  if (queue->old_mode == LockMode::SHARED) {
    s_num--;
  } else if (queue->old_mode == LockMode::EXCLUSIVE) {
    x_num--;
  }

  if (lock_mode == LockMode::SHARED) {
    if (x_num > 0) {
      return false;
    }
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    if (x_num > 0 || s_num > 0) {
      return false;
    }
  }

  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
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

  // lock upgrade
  bool is_upgrade = false;
  if (txn->IsTableSharedLocked(oid)) {
    if (lock_mode == LockMode::SHARED) {
      return true;
    }

    if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    //    txn->GetSharedTableLockSet()->erase(oid);
    is_upgrade = true;
  } else if (txn->IsTableIntentionSharedLocked(oid)) {
    if (lock_mode == LockMode::INTENTION_SHARED) {
      return true;
    }

    //    txn->GetIntentionSharedTableLockSet()->erase(oid);
    is_upgrade = true;
  } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      return true;
    }

    if (lock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    //    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
    is_upgrade = true;
  } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
    if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      return true;
    }

    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    //    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
    is_upgrade = true;
  } else if (txn->IsTableExclusiveLocked(oid)) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      return true;
    }

    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    is_upgrade = true;
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
      if (txn->IsTableSharedLocked(oid)) {
        l_queue->old_mode = LockMode::SHARED;
      } else if (txn->IsTableIntentionSharedLocked(oid)) {
        l_queue->old_mode = LockMode::INTENTION_SHARED;
      } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        l_queue->old_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
      } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
        l_queue->old_mode = LockMode::INTENTION_EXCLUSIVE;
      } else if (txn->IsTableExclusiveLocked(oid)) {
        l_queue->old_mode = LockMode::EXCLUSIVE;
      }
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
  }

  if (l_queue->request_queue_.size() == 0 && CanGetLock(l_queue, oid, lock_mode)) {
    //    l_queue->upgrading_ = txn->GetTransactionId();
    if (lock_mode == LockMode::INTENTION_SHARED) {
      l_queue->lck_counter_[0]++;
    } else if (lock_mode == LockMode::SHARED) {
      l_queue->lck_counter_[2]++;
    } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      l_queue->lck_counter_[1]++;
    } else if (lock_mode == LockMode::EXCLUSIVE) {
      l_queue->lck_counter_[4]++;
    } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      l_queue->lck_counter_[3]++;
    }
  } else if (CanUpgrade(l_queue, oid, lock_mode, txn->GetTransactionId())) {
    if (lock_mode == LockMode::INTENTION_SHARED) {
      l_queue->lck_counter_[0]++;
    } else if (lock_mode == LockMode::SHARED) {
      l_queue->lck_counter_[2]++;
    } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      l_queue->lck_counter_[1]++;
    } else if (lock_mode == LockMode::EXCLUSIVE) {
      l_queue->lck_counter_[4]++;
    } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      l_queue->lck_counter_[3]++;
    }
  } else {
    auto request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);

    if (is_upgrade) {
      l_queue->request_queue_.push_front(request);
    } else {
      l_queue->request_queue_.push_back(request);
    }

    while (l_queue->fifo_set_.count(txn->GetTransactionId()) == 0) {
      l_queue->cv_.wait(lck);
    }

    l_queue->fifo_set_.erase(txn->GetTransactionId());
  }

  if (is_upgrade) {
    if (txn->GetTransactionId() == l_queue->upgrading_) {
      l_queue->upgrading_ = INVALID_TXN_ID;
    }

//    if (txn->IsTableSharedLocked(oid)) {
//      txn->GetSharedTableLockSet()->erase(oid);
//      l_queue->lck_counter_[2]--;
//    } else if (txn->IsTableIntentionSharedLocked(oid)) {
//      txn->GetIntentionSharedTableLockSet()->erase(oid);
//      l_queue->lck_counter_[0]--;
//    } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
//      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
//      l_queue->lck_counter_[3]--;
//    } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
//      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
//      l_queue->lck_counter_[1]--;
//    } else if (txn->IsTableExclusiveLocked(oid)) {
//      txn->GetExclusiveTableLockSet()->erase(oid);
//      l_queue->lck_counter_[4]--;
//    }
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
    return false;
  }

  std::cout << "lock ok " << oid << "mode " << (int)lock_mode << std::endl;
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::unique_lock<std::mutex> table_lock_map_lck(table_lock_map_latch_);

  if (table_lock_map_.count(oid) == 0) {
    txn->SetState(TransactionState::ABORTED);
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
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  bool is_s_row_empty = txn->GetSharedRowLockSet()->count(oid) == 0 || (*(txn->GetSharedRowLockSet()))[oid].empty();
  bool is_x_row_empty =
      txn->GetExclusiveRowLockSet()->count(oid) == 0 || (*(txn->GetExclusiveRowLockSet()))[oid].empty();
  if (!is_s_row_empty || !is_x_row_empty) {
    txn->SetState(TransactionState::ABORTED);
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

  if (is_is_lock) {
    l_queue->lck_counter_[0]--;
  } else if (is_s_lock) {
    l_queue->lck_counter_[2]--;
  } else if (is_ix_lock) {
    l_queue->lck_counter_[1]--;
  } else if (is_x_lock) {
    l_queue->lck_counter_[4]--;
  } else if (is_six_lock) {
    l_queue->lck_counter_[3]--;
  }

  while (!l_queue->request_queue_.empty()) {
    auto trans = l_queue->request_queue_.front();

    if (CanGetLock(l_queue, oid, trans->lock_mode_) || CanUpgrade(l_queue, oid, trans->lock_mode_, trans->txn_id_)) {
      l_queue->fifo_set_.insert(trans->txn_id_);

      if (trans->lock_mode_ == LockMode::INTENTION_SHARED) {
        l_queue->lck_counter_[0]++;
      } else if (trans->lock_mode_ == LockMode::SHARED) {
        l_queue->lck_counter_[2]++;
      } else if (trans->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
        l_queue->lck_counter_[1]++;
      } else if (trans->lock_mode_ == LockMode::EXCLUSIVE) {
        l_queue->lck_counter_[4]++;
      } else if (trans->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        l_queue->lck_counter_[3]++;
      }

      l_queue->request_queue_.pop_front();
      delete trans;
    } else {
      break;
    }
  }

  l_queue->cv_.notify_all();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
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
    is_upgrade = true;
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
      if (txn->IsRowSharedLocked(oid, rid)) {
        l_queue->old_mode = LockMode::SHARED;
      } else if (txn->IsRowExclusiveLocked(oid, rid)) {
        l_queue->old_mode = LockMode::EXCLUSIVE;
      }
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
  }

  if (l_queue->request_queue_.size() == 0 && CanGetLockForRow(l_queue, oid, lock_mode, rid)) {
    if (lock_mode == LockMode::SHARED) {
      l_queue->lck_counter_[2]++;
    } else if (lock_mode == LockMode::EXCLUSIVE) {
      l_queue->lck_counter_[4]++;
    }
  } else if (CanUpgradeForRow(l_queue, oid, lock_mode, txn->GetTransactionId(), rid)) {
    if (lock_mode == LockMode::SHARED) {
      l_queue->lck_counter_[2]++;
    } else if (lock_mode == LockMode::EXCLUSIVE) {
      l_queue->lck_counter_[4]++;
    }
  } else {
    auto request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);

    if (is_upgrade) {
      l_queue->request_queue_.push_front(request);
    } else {
      l_queue->request_queue_.push_back(request);
    }

    while (l_queue->fifo_set_.count(txn->GetTransactionId()) == 0) {
      l_queue->cv_.wait(lck);
    }

    l_queue->fifo_set_.erase(txn->GetTransactionId());
  }

  if (is_upgrade) {
    if (txn->GetTransactionId() == l_queue->upgrading_) {
      l_queue->upgrading_ = INVALID_TXN_ID;
    }

    if (txn->IsRowSharedLocked(oid, rid)) {
      txn->GetSharedTableLockSet()->erase(oid);
      l_queue->lck_counter_[2]--;
    } else if (txn->IsRowExclusiveLocked(oid, rid)) {
      txn->GetExclusiveTableLockSet()->erase(oid);
      l_queue->lck_counter_[4]--;
    }
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

  if (is_s_lock) {
    l_queue->lck_counter_[2]--;
  } else if (is_x_lock) {
    l_queue->lck_counter_[4]--;
  }

  while (!l_queue->request_queue_.empty()) {
    auto trans = l_queue->request_queue_.front();

    if (CanGetLockForRow(l_queue, oid, trans->lock_mode_, rid) ||
        CanUpgradeForRow(l_queue, oid, trans->lock_mode_, trans->txn_id_, rid)) {
      l_queue->fifo_set_.insert(trans->txn_id_);

      if (trans->lock_mode_ == LockMode::SHARED) {
        l_queue->lck_counter_[2]++;
      } else if (trans->lock_mode_ == LockMode::EXCLUSIVE) {
        l_queue->lck_counter_[4]++;
      }

      l_queue->request_queue_.pop_front();
      delete trans;
    } else {
      break;
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
