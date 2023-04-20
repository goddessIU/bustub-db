//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "execution/expressions/logic_expression.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      has_get_left_(false),
      has_join_(false) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { left_executor_->Init(); }

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const AbstractExpression *judge_expr_ptr = &(plan_->Predicate());

  Tuple tuple_right;
  RID rid_right;

  if (plan_->GetJoinType() == JoinType::INNER) {
    while (true) {
      if (!has_get_left_) {
        has_get_left_ = true;
        if (!left_executor_->Next(&tuple_left_, &rid_left_)) {
          return false;
        }
        right_executor_->Init();
      }

      while (right_executor_->Next(&tuple_right, &rid_right)) {
        auto value = judge_expr_ptr->EvaluateJoin(&tuple_left_, left_executor_->GetOutputSchema(), &tuple_right,
                                                  right_executor_->GetOutputSchema());
        if (!value.IsNull() && value.GetAs<bool>()) {
          std::vector<Value> vec;
          int col_left_num = left_executor_->GetOutputSchema().GetColumnCount();
          int col_right_num = right_executor_->GetOutputSchema().GetColumnCount();

          vec.resize(col_left_num + col_right_num);
          int idx = 0;

          for (int i = 0; i < col_left_num; i++) {
            vec[idx++] = tuple_left_.GetValue(&(left_executor_->GetOutputSchema()), i);
          }

          for (int i = 0; i < col_right_num; i++) {
            vec[idx++] = tuple_right.GetValue(&(right_executor_->GetOutputSchema()), i);
          }
          *tuple = {vec, &(plan_->OutputSchema())};
          *rid = tuple->GetRid();
          return true;
        }
      }

      has_get_left_ = false;
    }
  } else if (plan_->GetJoinType() == JoinType::LEFT) {
    while (true) {
      if (!has_get_left_) {
        has_get_left_ = true;
        if (!left_executor_->Next(&tuple_left_, &rid_left_)) {
          return false;
        }
        right_executor_->Init();
      }
      while (right_executor_->Next(&tuple_right, &rid_right)) {
        auto value = judge_expr_ptr->EvaluateJoin(&tuple_left_, left_executor_->GetOutputSchema(), &tuple_right,
                                                  right_executor_->GetOutputSchema());
        if (!value.IsNull() && value.GetAs<bool>()) {
          has_join_ = true;
          std::vector<Value> vec;
          int col_left_num = left_executor_->GetOutputSchema().GetColumnCount();
          int col_right_num = right_executor_->GetOutputSchema().GetColumnCount();
          vec.resize(col_right_num + col_left_num);
          int idx = 0;
          for (int i = 0; i < col_left_num; i++) {
            vec[idx++] = tuple_left_.GetValue(&(left_executor_->GetOutputSchema()), i);
          }

          for (int i = 0; i < col_right_num; i++) {
            vec[idx++] = tuple_right.GetValue(&(right_executor_->GetOutputSchema()), i);
          }
          *tuple = {vec, &(plan_->OutputSchema())};
          *rid = tuple->GetRid();
          return true;
        }
      }

      if (!has_join_) {
        std::vector<Value> vec;
        int col_left_num = left_executor_->GetOutputSchema().GetColumnCount();
        int col_right_num = right_executor_->GetOutputSchema().GetColumnCount();
        vec.resize(col_left_num + col_right_num);
        int idx = 0;
        for (int i = 0; i < col_left_num; i++) {
          vec[idx++] = tuple_left_.GetValue(&(left_executor_->GetOutputSchema()), i);
        }

        for (int i = 0; i < col_right_num; i++) {
          vec[idx++] = ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType());
        }
        *tuple = {vec, &(plan_->OutputSchema())};
        *rid = tuple->GetRid();

        has_join_ = false;
        has_get_left_ = false;
        return true;
      }

      has_join_ = false;
      has_get_left_ = false;
    }
  }

  return false;
}
}  // namespace bustub
