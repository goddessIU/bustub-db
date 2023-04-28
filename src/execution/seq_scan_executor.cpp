//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), iter_ptr_(nullptr) {}

void SeqScanExecutor::Init() {
  auto oid = plan_->GetTableOid();
  bool res = false;
  const auto &level = exec_ctx_->GetTransaction()->GetIsolationLevel();
  if (level == IsolationLevel::REPEATABLE_READ || level == IsolationLevel::READ_COMMITTED) {
    if (exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(oid)) {
      //      res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
      //      LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
      res = true;
      //    } else if (exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(oid)) {
      //      res = true;
    } else {
      res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                                   oid);
    }
  } else if (level == IsolationLevel::READ_UNCOMMITTED) {
    res = true;
    //  } else if () {
    //    //    if (exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(oid)) {
    //    //      res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
    //    //      LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
    //    //    } else if (exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(oid)) {
    //    //      res = true;
    //    //    } else {
    //    //      res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
    //    LockManager::LockMode::SHARED,
    //    //      oid);
    //    //    }
    //    if (exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(oid)) {
    //      res = true;
    //    } else {
    //      res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
    //      LockManager::LockMode::INTENTION_SHARED,
    //                                                   oid);
    //    }
  }

  if (!res) {
    throw ExecutionException("cannot lock");
  }

  table_info_ = exec_ctx_->GetCatalog()->GetTable(oid);
  iter_ptr_ = std::make_unique<TableIterator>(table_info_->table_->Begin(exec_ctx_->GetTransaction()));
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*iter_ptr_ == table_info_->table_->End()) {
    //    exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->GetTableOid());
    const auto &level = exec_ctx_->GetTransaction()->GetIsolationLevel();
    if (level == IsolationLevel::READ_COMMITTED) {
      exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->GetTableOid());
    }
    return false;
  }

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ
      || exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                              plan_->GetTableOid(), (*iter_ptr_)->GetRid())) {
      throw ExecutionException("cannot lock");
    }
  }

  *tuple = *(*iter_ptr_);
  *rid = tuple->GetRid();

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), (*iter_ptr_)->GetRid());
  }

  ++(*iter_ptr_);
  return true;
}

}  // namespace bustub
