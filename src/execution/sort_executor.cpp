#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), idx_(0) {
  order_bys_ = plan_->GetOrderBy();
}

void SortExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_executor_->Init();
  idx_ = 0;
  tuples_.clear();
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }

  std::sort(tuples_.begin(), tuples_.end(), [this](Tuple &left, Tuple &right) {
    for (auto &comp : order_bys_) {
      Value left_val = comp.second->Evaluate(&left, plan_->OutputSchema());
      Value right_val = comp.second->Evaluate(&right, plan_->OutputSchema());
      if (comp.first == OrderByType::DESC) {
        if (left_val.CompareLessThan(right_val) == CmpBool::CmpTrue) {
          return false;
        }

        if (left_val.CompareGreaterThan(right_val) == CmpBool::CmpTrue) {
          return true;
        }
      } else {
        if (left_val.CompareGreaterThan(right_val) == CmpBool::CmpTrue) {
          return false;
        }

        if (left_val.CompareLessThan(right_val) == CmpBool::CmpTrue) {
          return true;
        }
      }
    }

    return true;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (idx_ < static_cast<int>(tuples_.size())) {
    *tuple = tuples_[idx_];
    *rid = tuple->GetRid();
    idx_++;
    return true;
  }

  return false;
}

}  // namespace bustub
