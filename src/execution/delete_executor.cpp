//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      deleted_nums_(0),
      has_deleted_(false) {}

void DeleteExecutor::Init() {
  auto oid = plan_->TableOid();

  bool res = false;
  //  const auto level = exec_ctx_->GetTransaction()->GetIsolationLevel();
  res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                               oid);
  //  if (level == IsolationLevel::REPEATABLE_READ) {
  //    if (exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(oid)) {
  //      res = true;
  //    } else if (exec_ctx_->GetTransaction()->IsTableSharedLocked(oid)) {
  //      res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
  //      LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
  //    } else {
  //      res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
  //      LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  //    }
  //    res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
  //                                                 LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  //  } else if (level == IsolationLevel::READ_UNCOMMITTED) {
  //    res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
  //                                                 LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  //  } else if (level == IsolationLevel::READ_COMMITTED) {
  //    if (exec_ctx_->GetTransaction()->IsTableSharedLocked(oid)) {
  //      res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
  //      LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
  //    } else if (exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(oid)) {
  //      res = true;
  //    } else {
  //      res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
  //      LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  //    }
  //    res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
  //                                                 LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  //  }
  if (!res) {
    throw ExecutionException("cannot lock");
  }

  table_info_ = exec_ctx_->GetCatalog()->GetTable(oid);
  auto vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  if (child_executor_) {
    child_executor_->Init();
    auto c_tuple = Tuple();
    auto c_rid = RID();

    while (child_executor_->Next(&c_tuple, &c_rid)) {
      if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, oid,
                                                c_rid)) {
        throw ExecutionException("cannot lock");
      }

      table_info_->table_->MarkDelete(c_rid, exec_ctx_->GetTransaction());
      for (auto info : vec) {
        info->index_->DeleteEntry(c_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), info->key_schema_,
                                                       info->index_->GetMetadata()->GetKeyAttrs()),
                                  c_rid, exec_ctx_->GetTransaction());
        exec_ctx_->GetTransaction()->AppendIndexWriteRecord(
            {c_rid, oid, WType::DELETE, c_tuple, info->index_oid_, exec_ctx_->GetCatalog()});
      }

      //      exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, c_rid);

      deleted_nums_++;
    }
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_deleted_) {
    return false;
  }

  has_deleted_ = true;
  std::vector<Value> vec{Value(TypeId::INTEGER, deleted_nums_)};
  *tuple = Tuple(vec, &(plan_->OutputSchema()));
  //  exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->table_oid_);

  return true;
}

}  // namespace bustub
