//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(nullptr),
      child_executor_(std::move(child_executor)),
      inserted_num_(0),
      is_first_(false) {}

void InsertExecutor::Init() {
  auto oid = plan_->TableOid();
  bool res = false;
  res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                               oid);

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

      table_info_->table_->InsertTuple(c_tuple, &c_rid, exec_ctx_->GetTransaction());

      for (auto info : vec) {
        info->index_->InsertEntry(c_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), info->key_schema_,
                                                       info->index_->GetMetadata()->GetKeyAttrs()),
                                  c_rid, exec_ctx_->GetTransaction());
        exec_ctx_->GetTransaction()->AppendIndexWriteRecord(
            {c_rid, oid, WType::INSERT, c_tuple, info->index_oid_, exec_ctx_->GetCatalog()});
      }

      //      exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, c_rid);

      inserted_num_++;
    }
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!is_first_) {
    std::vector<Value> vec{Value(TypeId::INTEGER, inserted_num_)};
    *tuple = Tuple(vec, &(plan_->OutputSchema()));
    is_first_ = true;
    return true;
  }

  return false;
}

}  // namespace bustub
