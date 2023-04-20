#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  tuples_.clear();
  idx_ = 0;
  num_ = 0;
  while (!heap_.empty()) {
    heap_.pop();
  }

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    if (num_ < plan_->GetN()) {
      num_++;
      heap_.push(tuple);
    } else {
      heap_.push(tuple);
      heap_.pop();
    }
  }

  while (!heap_.empty()) {
    tuples_.push_back(heap_.top());
    heap_.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (idx_ < static_cast<int>(tuples_.size())) {
    *tuple = tuples_[tuples_.size() - idx_ - 1];
    *rid = tuple->GetRid();
    idx_++;
    return true;
  }

  return false;
}

}  // namespace bustub
