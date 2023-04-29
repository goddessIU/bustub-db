//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), index_ptr_(nullptr) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();

  has_optimized_ = false;
  auto &t1 = child_executor_->GetOutputSchema();
  auto &t2 = plan_->OutputSchema();
  int sz = t1.GetColumnCount();
  for (int i = 0; i < sz; i++) {
    if (t1.GetColumn(i).GetName() != t2.GetColumn(i).GetName()) {
      has_optimized_ = true;
      break;
    }
  }

  auto oid = plan_->GetIndexOid();
  index_ptr_ = exec_ctx_->GetCatalog()->GetIndex(oid);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_ptr_->index_.get());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_ptr_->table_name_);
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  AbstractExpressionRef judge_expr_ptr = plan_->KeyPredicate();
  Schema *key_schema = index_ptr_->index_->GetMetadata()->GetKeySchema();
  Tuple tuple_left;
  RID rid_left;

  while (child_executor_->Next(&tuple_left, &rid_left)) {
    auto join_value = judge_expr_ptr->Evaluate(&tuple_left, child_executor_->GetOutputSchema());
    Tuple join_key({join_value}, key_schema);
    std::vector<RID> result;
    index_ptr_->index_->ScanKey(join_key, &result, exec_ctx_->GetTransaction());

    if (result.empty()) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> vec;

        int col_left_num = child_executor_->GetOutputSchema().GetColumnCount();
        int col_right_num = key_schema->GetColumnCount();

        vec.resize(col_left_num + col_right_num);
        int idx = 0;
        for (int i = 0; i < col_left_num; i++) {
          vec[idx++] = tuple_left.GetValue(&(child_executor_->GetOutputSchema()), i);
        }

        for (int i = 0; i < col_right_num; i++) {
          vec[idx++] = ValueFactory::GetNullValueByType(key_schema->GetColumn(i).GetType());
        }
        *tuple = {vec, &(plan_->OutputSchema())};
        *rid = tuple->GetRid();
        return true;
      }
    } else {
      Tuple tuple_right;
      for (auto &res : result) {
        table_info_->table_->GetTuple(res, &tuple_right, exec_ctx_->GetTransaction());
        Tuple join_tuple;

        Value val;
        if (has_optimized_) {
          val = judge_expr_ptr->EvaluateJoin(&tuple_right, table_info_->schema_, &tuple_left,
                                             child_executor_->GetOutputSchema());
        } else {
          val = judge_expr_ptr->EvaluateJoin(&tuple_left, child_executor_->GetOutputSchema(), &tuple_right,
                                             table_info_->schema_);
        }

        std::vector<Value> vec;
        int col_left_num = child_executor_->GetOutputSchema().GetColumnCount();
        int col_right_num = table_info_->schema_.GetColumnCount();
        vec.resize(col_right_num + col_left_num);
        int idx = 0;

        if (has_optimized_) {
          for (int i = 0; i < col_right_num; i++) {
            vec[idx++] = tuple_right.GetValue(&(table_info_->schema_), i);
          }

          for (int i = 0; i < col_left_num; i++) {
            vec[idx++] = tuple_left.GetValue(&(child_executor_->GetOutputSchema()), i);
          }
        } else {
          for (int i = 0; i < col_left_num; i++) {
            vec[idx++] = tuple_left.GetValue(&(child_executor_->GetOutputSchema()), i);
          }

          for (int i = 0; i < col_right_num; i++) {
            vec[idx++] = tuple_right.GetValue(&(table_info_->schema_), i);
          }
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
