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
  table_info_ = exec_ctx_->GetCatalog()->GetTable(oid);
  auto vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  if (child_executor_) {
    child_executor_->Init();
    auto c_tuple = Tuple();
    auto c_rid = RID();

    while (child_executor_->Next(&c_tuple, &c_rid)) {
      table_info_->table_->InsertTuple(c_tuple, &c_rid, exec_ctx_->GetTransaction());
      for (auto info : vec) {
        info->index_->InsertEntry(c_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), info->key_schema_,
                                                       info->index_->GetMetadata()->GetKeyAttrs()),
                                  c_rid, exec_ctx_->GetTransaction());
      }

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
