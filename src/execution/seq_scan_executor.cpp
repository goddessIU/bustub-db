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
  table_info_ = exec_ctx_->GetCatalog()->GetTable(oid);
  iter_ptr_ = std::make_unique<TableIterator>(table_info_->table_->Begin(exec_ctx_->GetTransaction()));
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*iter_ptr_ == table_info_->table_->End()) {
    return false;
  }

  *tuple = *(*iter_ptr_);
  *rid = tuple->GetRid();
  ++(*iter_ptr_);
  return true;
}

}  // namespace bustub
