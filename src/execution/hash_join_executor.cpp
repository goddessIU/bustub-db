//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  hash_table_.clear();
  has_vector_ = false;
  right_vector_idx_ = -1;
  right_vector_ptr_ = nullptr;

  left_child_->Init();
  right_child_->Init();
  Tuple tuple_right;
  RID rid_right;

  //  std::cout << "abc" << std::endl;

  while (right_child_->Next(&tuple_right, &rid_right)) {
    auto right_key = plan_->RightJoinKeyExpression().Evaluate(&tuple_right, right_child_->GetOutputSchema());
    HashJoinKey find_key = {right_key};
    if (hash_table_.count(find_key) == 0) {
      hash_table_[find_key] = {};
    }

    hash_table_[find_key].push_back(tuple_right);
  }
  //  std::cout << "size is " << ht_.size() << std::endl;

  //  std::cout << "efg" << std::endl;
  // Optimizer opt_temp(*(exec_ctx_->GetCatalog()), true);
  // opt_temp

  // build the hash table, so the data structure and the operation?

  // left child next until no more tuple, and insert into table
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // right child next, find a join tuple once a time

  while (true) {
    if (has_vector_) {
      right_vector_idx_++;
      if (right_vector_idx_ == static_cast<int>(right_vector_ptr_->size())) {
        has_vector_ = false;
        right_vector_idx_ = -1;
        right_vector_ptr_ = nullptr;
      } else {
        int idx = 0;
        int left_num = left_child_->GetOutputSchema().GetColumnCount();
        int right_num = right_child_->GetOutputSchema().GetColumnCount();
        std::vector<Value> vec(left_num + right_num);
        for (int i = 0; i < left_num; i++) {
          vec[idx++] = tuple_left_.GetValue(&(left_child_->GetOutputSchema()), i);
          //        vec[idx++] = ht_[find_key].GetValue(&(left_child_->GetOutputSchema()), i);
        }
        for (int i = 0; i < right_num; i++) {
          vec[idx++] = right_vector_ptr_->at(right_vector_idx_).GetValue(&(right_child_->GetOutputSchema()), i);
        }

        *tuple = {vec, &(plan_->OutputSchema())};
        *rid = tuple->GetRid();
        return true;
      }
    } else {
      if (!left_child_->Next(&tuple_left_, &rid_left_)) {
        return false;
      }

      auto left_key = plan_->LeftJoinKeyExpression().Evaluate(&tuple_left_, left_child_->GetOutputSchema());
      HashJoinKey find_key = {left_key};
      if (hash_table_.count(find_key) == 1) {
        has_vector_ = true;
        right_vector_idx_ = -1;
        right_vector_ptr_ = &(hash_table_[find_key]);

        //        int idx = 0;
        //        int left_num = left_child_->GetOutputSchema().GetColumnCount();
        //        int right_num = right_child_->GetOutputSchema().GetColumnCount();
        //        std::vector<Value> vec(left_num + right_num);
        //        for (int i = 0; i < left_num; i++) {
        //          vec[idx++] = tuple_left_.GetValue(&(left_child_->GetOutputSchema()), i);
        //          //        vec[idx++] = ht_[find_key].GetValue(&(left_child_->GetOutputSchema()), i);
        //        }
        //        for (int i = 0; i < right_num; i++) {
        //          vec[idx++] = ht_[find_key].GetValue(&(right_child_->GetOutputSchema()), i);
        //        }
        //
        //        *tuple = {vec, &(plan_->OutputSchema())};
        //        *rid = tuple->GetRid();
        //        has_get_left_ = false;
        //        return true;
      } else if (plan_->GetJoinType() == JoinType::LEFT) {
        int idx = 0;
        int left_num = left_child_->GetOutputSchema().GetColumnCount();
        int right_num = right_child_->GetOutputSchema().GetColumnCount();
        std::vector<Value> vec(left_num + right_num);
        for (int i = 0; i < left_num; i++) {
          vec[idx++] = tuple_left_.GetValue(&(left_child_->GetOutputSchema()), i);
          //        vec[idx++] = ht_[find_key].GetValue(&(left_child_->GetOutputSchema()), i);
        }
        for (int i = 0; i < right_num; i++) {
          vec[idx++] = ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType());
          //        vec[idx++] = ht_[find_key].GetValue(&(right_child_->GetOutputSchema()), i);
        }

        *tuple = {vec, &(plan_->OutputSchema())};
        *rid = tuple->GetRid();
        return true;
      }
    }
  }

  return false;
}

}  // namespace bustub
