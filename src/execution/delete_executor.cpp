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
  table_info_ = exec_ctx_->GetCatalog()->GetTable(oid);
  auto vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  if (child_executor_) {
    child_executor_->Init();
    auto c_tuple = Tuple();
    auto c_rid = RID();

    while (child_executor_->Next(&c_tuple, &c_rid)) {
      table_info_->table_->MarkDelete(c_rid, exec_ctx_->GetTransaction());
      for (auto info : vec) {
        info->index_->DeleteEntry(c_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), info->key_schema_,
                                                       info->index_->GetMetadata()->GetKeyAttrs()),
                                  c_rid, exec_ctx_->GetTransaction());
      }

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
  return true;
}

}  // namespace bustub
