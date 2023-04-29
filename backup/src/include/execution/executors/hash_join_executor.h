//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {
/** HashJoinKey represents a key in an hash join operation */
struct HashJoinKey {
  /** The hash join key */
  Value val_;

  HashJoinKey() = default;

  /**
   * Compares two hash join keys for equality.
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join keys have equivalent hash join expressions, `false` otherwise
   */
  auto operator==(const HashJoinKey &other) const -> bool { return val_.CompareEquals(other.val_) == CmpBool::CmpTrue; }

  //  auto operator()(const HashJoinKey &hash_join_key) const -> std::size_t {
  //    size_t curr_hash = 0;
  //    const auto &key = hash_join_key.val_;
  //    curr_hash = HashUtil::CombineHashes(curr_hash, HashUtil::HashValue(&key));
  //    return curr_hash;
  //  }
};
}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &hash_join_key) const -> std::size_t {
    size_t curr_hash = 0;
    const auto &key = hash_join_key.val_;
    curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
    return curr_hash;
  }
};
}  // namespace std
namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  //  HashJoinKey kkk;
  std::unordered_map<HashJoinKey, std::vector<Tuple>> hash_table_;
  Tuple tuple_left_;
  RID rid_left_;
  bool has_vector_;
  std::vector<Tuple> *right_vector_ptr_;
  int right_vector_idx_;
  bool has_optimized_;
};

}  // namespace bustub

// namespace std {
//
///** Implements std::hash on AggregateKey */
// template <>
// struct hash<bustub::HashJoinKey> {
//
//};
//
//}  // namespace std
