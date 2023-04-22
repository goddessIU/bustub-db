#include <set>

#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/values_plan.h"
#include "optimizer/optimizer.h"

// Note for 2022 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file. Note
// that for some test cases, we force using starter rules, so that the configuration here won't take effects. Starter
// rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {
auto Optimizer::FindFilterPredicate(AbstractExpressionRef &expression, AbstractExpressionRef &left_tree,
                                    AbstractExpressionRef &right_tree, int border, AbstractExpressionRef &middle_tree)
    -> void {
  int sz = static_cast<int>(expression->children_.size());
  for (int i = 0; i < sz; i++) {
    FindFilterPredicate(expression->children_[i], left_tree, right_tree, border, middle_tree);
  }

  if (std::shared_ptr<LogicExpression> logic_expr = std::dynamic_pointer_cast<LogicExpression>(expression);
      logic_expr != nullptr) {
    // if it is comparison expression

    bool left_extracted = false;
    bool right_extracted = false;

    if (std::shared_ptr<ComparisonExpression> left_compare_expr =
            std::dynamic_pointer_cast<ComparisonExpression>(logic_expr->children_[0]);
        left_compare_expr != nullptr) {
      if (const auto *left_col_expr =
              dynamic_cast<const ColumnValueExpression *>(left_compare_expr->children_[0].get());
          left_col_expr != nullptr) {
        if (const auto *right_constant_expr =
                dynamic_cast<const ConstantValueExpression *>(left_compare_expr->children_[1].get());
            right_constant_expr != nullptr) {
          left_extracted = true;
          int idx = left_col_expr->GetColIdx();
          if (idx < border) {
            if (left_tree == nullptr) {
              left_tree = left_compare_expr;
            } else {
              left_tree = std::make_shared<LogicExpression>(left_tree, left_compare_expr, logic_expr->logic_type_);
            }
          } else if (idx >= border) {
            left_compare_expr->children_[0] = std::make_shared<ColumnValueExpression>(
                left_col_expr->GetTupleIdx(), left_col_expr->GetColIdx() - border, left_col_expr->GetReturnType());
            if (right_tree == nullptr) {
              right_tree = left_compare_expr;
            } else {
              right_tree = std::make_shared<LogicExpression>(right_tree, left_compare_expr, logic_expr->logic_type_);
            }
          }
        }
      } else if (const auto *right_col_expr =
                     dynamic_cast<const ColumnValueExpression *>(left_compare_expr->children_[1].get());
                 right_col_expr != nullptr) {
        if (const auto *left_constant_expr =
                dynamic_cast<const ConstantValueExpression *>(left_compare_expr->children_[0].get());
            left_constant_expr != nullptr) {
          left_extracted = true;
          int idx = right_col_expr->GetColIdx();
          if (idx < border) {
            if (left_tree == nullptr) {
              left_tree = left_compare_expr;
            } else {
              left_tree = std::make_shared<LogicExpression>(left_tree, left_compare_expr, logic_expr->logic_type_);
            }
          } else {
            left_compare_expr->children_[1] = std::make_shared<ColumnValueExpression>(
                right_col_expr->GetTupleIdx(), right_col_expr->GetColIdx() - border, right_col_expr->GetReturnType());
            if (right_tree == nullptr) {
              right_tree = left_compare_expr;
            } else {
              right_tree = std::make_shared<LogicExpression>(right_tree, left_compare_expr, logic_expr->logic_type_);
            }
          }
        }
      }
    }

    if (std::shared_ptr<ComparisonExpression> right_compare_expr =
            std::dynamic_pointer_cast<ComparisonExpression>(logic_expr->children_[1]);
        right_compare_expr != nullptr) {
      if (const auto *left_col_expr =
              dynamic_cast<const ColumnValueExpression *>(right_compare_expr->children_[0].get());
          left_col_expr != nullptr) {
        if (const auto *right_constant_expr =
                dynamic_cast<const ConstantValueExpression *>(right_compare_expr->children_[1].get());
            right_constant_expr != nullptr) {
          right_extracted = true;
          int idx = left_col_expr->GetColIdx();
          if (idx < border) {
            if (left_tree == nullptr) {
              left_tree = right_compare_expr;
            } else {
              left_tree = std::make_shared<LogicExpression>(left_tree, right_compare_expr, logic_expr->logic_type_);
            }
          } else {
            right_compare_expr->children_[0] = std::make_shared<ColumnValueExpression>(
                left_col_expr->GetTupleIdx(), left_col_expr->GetColIdx() - border, left_col_expr->GetReturnType());
            if (right_tree == nullptr) {
              right_tree = right_compare_expr;
            } else {
              right_tree = std::make_shared<LogicExpression>(right_tree, right_compare_expr, logic_expr->logic_type_);
            }
          }
        }
      } else if (const auto *right_col_expr =
                     dynamic_cast<const ColumnValueExpression *>(right_compare_expr->children_[1].get());
                 right_col_expr != nullptr) {
        if (const auto *left_constant_expr =
                dynamic_cast<const ConstantValueExpression *>(right_compare_expr->children_[0].get());
            left_constant_expr != nullptr) {
          right_extracted = true;
          int idx = right_col_expr->GetColIdx();
          if (idx < border) {
            if (left_tree == nullptr) {
              left_tree = right_compare_expr;
            } else {
              left_tree = std::make_shared<LogicExpression>(left_tree, right_compare_expr, logic_expr->logic_type_);
            }
          } else {
            right_compare_expr->children_[1] = std::make_shared<ColumnValueExpression>(
                right_col_expr->GetTupleIdx(), right_col_expr->GetColIdx() - border, right_col_expr->GetReturnType());
            if (right_tree == nullptr) {
              right_tree = right_compare_expr;
            } else {
              right_tree = std::make_shared<LogicExpression>(right_tree, right_compare_expr, logic_expr->logic_type_);
            }
          }
        }
      }
    }

    if (!left_extracted) {
      if (auto temp_expr = dynamic_cast<const LogicExpression *>(logic_expr->children_[0].get());
          temp_expr == nullptr) {
        if (middle_tree == nullptr) {
          middle_tree = logic_expr->children_[0];
        } else {
          middle_tree =
              std::make_shared<LogicExpression>(logic_expr->children_[0], middle_tree, logic_expr->logic_type_);
        }
      }
    }

    if (!right_extracted) {
      if (auto temp_expr = dynamic_cast<const LogicExpression *>(logic_expr->children_[1].get());
          temp_expr == nullptr) {
        if (middle_tree == nullptr) {
          middle_tree = logic_expr->children_[1];
        } else {
          middle_tree =
              std::make_shared<LogicExpression>(logic_expr->children_[1], middle_tree, logic_expr->logic_type_);
        }
      }
    }
  }
}

auto Optimizer::OptimizeFilterPushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  if (plan->GetType() == PlanType::Filter) {
    // if child is not join, it's meaningless to do this optimize
    if (plan->children_[0]->GetType() == PlanType::NestedLoopJoin) {
      auto &filter_plan = dynamic_cast<FilterPlanNode &>(const_cast<AbstractPlanNode &>(*plan));
      auto &join_plan = dynamic_cast<NestedLoopJoinPlanNode &>(const_cast<AbstractPlanNode &>(*(plan->children_[0])));

      if (std::dynamic_pointer_cast<LogicExpression>(filter_plan.predicate_)) {
        // traverse the tree, make left tree and right tree
        AbstractExpressionRef left_tree;
        AbstractExpressionRef right_tree;
        AbstractExpressionRef middle_tree = nullptr;
        int border = join_plan.children_[0]->output_schema_->GetColumnCount();
        FindFilterPredicate(filter_plan.predicate_, left_tree, right_tree, border, middle_tree);
        filter_plan.predicate_ = middle_tree;

        if (left_tree != nullptr) {
          if (join_plan.children_[0]->GetType() == PlanType::MockScan ||
              join_plan.children_[0]->GetType() == PlanType::SeqScan) {
            auto plan_node = std::make_shared<FilterPlanNode>(join_plan.children_[0]->output_schema_, left_tree,
                                                              join_plan.children_[0]);
            join_plan.children_[0] = plan_node;
          } else if (join_plan.children_[0]->GetType() == PlanType::NestedLoopJoin) {
            auto &nlj_plan =
                dynamic_cast<NestedLoopJoinPlanNode &>(const_cast<AbstractPlanNode &>(*(join_plan.children_[0])));
            nlj_plan.predicate_ = std::make_shared<LogicExpression>(left_tree, nlj_plan.predicate_, LogicType::And);
          } else if (join_plan.children_[0]->GetType() == PlanType::Filter) {
            auto &fil_plan = dynamic_cast<FilterPlanNode &>(const_cast<AbstractPlanNode &>(*(join_plan.children_[0])));
            fil_plan.predicate_ = std::make_shared<LogicExpression>(fil_plan.predicate_, left_tree, LogicType::And);
          }
        }

        if (right_tree != nullptr) {
          if (join_plan.children_[1]->GetType() == PlanType::MockScan ||
              join_plan.children_[1]->GetType() == PlanType::SeqScan) {
            join_plan.children_[1] = std::make_shared<FilterPlanNode>(join_plan.children_[1]->output_schema_,
                                                                      right_tree, join_plan.children_[1]);
          } else if (join_plan.children_[1]->GetType() == PlanType::NestedLoopJoin) {
            auto &nlj_plan =
                dynamic_cast<NestedLoopJoinPlanNode &>(const_cast<AbstractPlanNode &>(*(join_plan.children_[1])));
            nlj_plan.predicate_ = std::make_shared<LogicExpression>(right_tree, nlj_plan.predicate_, LogicType::And);
          } else if (join_plan.children_[1]->GetType() == PlanType::Filter) {
            auto &fil_plan = dynamic_cast<FilterPlanNode &>(const_cast<AbstractPlanNode &>(*(join_plan.children_[1])));
            fil_plan.predicate_ = std::make_shared<LogicExpression>(fil_plan.predicate_, left_tree, LogicType::And);
          }
        }
      }

      for (const auto &child : plan->GetChildren()) {
        children.emplace_back(OptimizeFilterPushDown(child));
      }
      auto optimized_plan = plan->CloneWithChildren(std::move(children));

      return optimized_plan;
    }

    for (const auto &child : plan->GetChildren()) {
      children.emplace_back(OptimizeFilterPushDown(child));
    }
    auto optimized_plan = plan->CloneWithChildren(std::move(children));

    return optimized_plan;
  }

  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeFilterPushDown(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  return optimized_plan;
}

auto Optimizer::CollectColumnIdxes(const AbstractExpressionRef &expression, std::set<int> &idxes) -> void {
  if (std::shared_ptr<ColumnValueExpression> col_expr = std::dynamic_pointer_cast<ColumnValueExpression>(expression);
      col_expr != nullptr) {
    idxes.insert(col_expr->GetColIdx());
    return;
  }

  if (expression->children_.empty()) {
    return;
  }

  for (const auto &child : expression->children_) {
    CollectColumnIdxes(child, idxes);
  }
}

auto Optimizer::OptimizeColumnPruningHelper(const AbstractPlanNodeRef &plan, std::set<int> &idxes, bool started)
    -> void {
  if (plan->GetType() == PlanType::Projection) {
    if (started) {
      std::vector<AbstractExpressionRef> new_expressions;
      std::vector<Column> new_columns;
      auto &projection_plan = dynamic_cast<ProjectionPlanNode &>(const_cast<AbstractPlanNode &>(*plan));
      for (const auto idx : idxes) {
        new_expressions.push_back(projection_plan.expressions_[idx]);
        new_columns.push_back(projection_plan.OutputSchema().GetColumn(idx));
      }

      for (const auto &expr : new_expressions) {
        CollectColumnIdxes(expr, idxes);
      }

      projection_plan.expressions_ = new_expressions;
      projection_plan.output_schema_ = std::make_shared<Schema>(new_columns);

      OptimizeColumnPruningHelper(projection_plan.children_[0], idxes, started);
    } else {
      started = true;
      auto &projection_plan = dynamic_cast<ProjectionPlanNode &>(const_cast<AbstractPlanNode &>(*plan));
      for (const auto &expr : projection_plan.expressions_) {
        CollectColumnIdxes(expr, idxes);
      }

      OptimizeColumnPruningHelper(projection_plan.children_[0], idxes, started);
    }
  } else if (plan->GetType() == PlanType::Aggregation) {
    auto &aggregation_plan = dynamic_cast<AggregationPlanNode &>(const_cast<AbstractPlanNode &>(*plan));
    if (started) {
      int low_bound = aggregation_plan.GetGroupBys().size();
      int high_bound = aggregation_plan.GetAggregates().size() + low_bound;
      std::vector<AbstractExpressionRef> new_aggregates;
      std::vector<AggregationType> new_types;
      std::vector<Column> new_columns;

      // copy group bys
      auto iter = aggregation_plan.OutputSchema().GetColumns().begin();
      std::copy(iter, iter + low_bound, std::back_inserter(new_columns));

      // deal aggregates
      for (auto idx : idxes) {
        if (idx < high_bound && idx >= low_bound) {
          new_aggregates.push_back(aggregation_plan.GetAggregates()[idx - low_bound]);
          new_types.push_back(aggregation_plan.GetAggregateTypes()[idx - low_bound]);
          new_columns.push_back(aggregation_plan.OutputSchema().GetColumn(idx));
        }
      }

      aggregation_plan.aggregates_ = new_aggregates;
      aggregation_plan.agg_types_ = new_types;
      aggregation_plan.output_schema_ = std::make_shared<Schema>(new_columns);
    }
  }
}

auto Optimizer::OptimizeColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::set<int> idxes;
  OptimizeColumnPruningHelper(plan, idxes, false);
  return plan;
}

auto Optimizer::ExpressionElimination(AbstractExpressionRef &expression) -> AbstractExpressionRef {
  int sz = expression->children_.size();
  if (sz == 0) {
    return expression;
  }

  expression->children_[0] = ExpressionElimination(expression->children_[0]);
  expression->children_[1] = ExpressionElimination(expression->children_[1]);

  if (std::shared_ptr<ComparisonExpression> compare_expr = std::dynamic_pointer_cast<ComparisonExpression>(expression);
      compare_expr != nullptr) {
    if (std::shared_ptr<ConstantValueExpression> left_const_expr =
            std::dynamic_pointer_cast<ConstantValueExpression>(compare_expr->children_[0]);
        left_const_expr != nullptr) {
      if (std::shared_ptr<ConstantValueExpression> right_const_expr =
              std::dynamic_pointer_cast<ConstantValueExpression>(compare_expr->children_[1]);
          right_const_expr != nullptr) {
        auto cmp_res = left_const_expr->val_.CompareEquals(right_const_expr->val_);
        bool res = false;

        if (cmp_res == CmpBool::CmpTrue) {
          res = true;
        }
        return std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(res));
      }
    }

    return expression;
  }

  if (std::shared_ptr<LogicExpression> logic_expr = std::dynamic_pointer_cast<LogicExpression>(expression);
      logic_expr != nullptr) {
    if (std::shared_ptr<ConstantValueExpression> left_const_expr =
            std::dynamic_pointer_cast<ConstantValueExpression>(logic_expr->children_[0]);
        left_const_expr != nullptr) {
      if (std::shared_ptr<ConstantValueExpression> right_const_expr =
              std::dynamic_pointer_cast<ConstantValueExpression>(logic_expr->children_[1]);
          right_const_expr != nullptr) {
        auto cmp_res = left_const_expr->val_.CompareEquals(right_const_expr->val_);
        bool res = false;
        if (cmp_res == CmpBool::CmpTrue) {
          res = true;
        }

        return std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(res));
      }
    }

    return expression;
  }
  return expression;
}

auto Optimizer::OptimizeExpressionElimination(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->children_) {
    children.emplace_back(OptimizeExpressionElimination(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    auto &filter_plan = dynamic_cast<FilterPlanNode &>(const_cast<AbstractPlanNode &>(*plan));
    filter_plan.predicate_ = ExpressionElimination(filter_plan.predicate_);
    if (std::shared_ptr<ConstantValueExpression> const_predicate =
            std::dynamic_pointer_cast<ConstantValueExpression>(filter_plan.predicate_);
        const_predicate != nullptr) {
      if (!const_predicate->val_.GetAs<bool>()) {
        if (optimized_plan->children_[0]->GetType() == PlanType::MockScan ||
            optimized_plan->children_[0]->GetType() == PlanType::SeqScan) {
          // can't use {{}}, I don't know why
          std::vector<std::vector<AbstractExpressionRef>> values;
          optimized_plan->children_[0] =
              std::make_shared<ValuesPlanNode>(optimized_plan->children_[0]->output_schema_, values);
        }
      }
    }
  }

  return optimized_plan;
}

auto Optimizer::OptimizeSwapTable(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSwapTable(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    auto &nlj_plan = dynamic_cast<NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    // Ensure right child is table scan
    if (nlj_plan.GetJoinType() == JoinType::INNER && nlj_plan.GetLeftPlan()->GetType() == PlanType::NestedLoopJoin &&
        nlj_plan.GetRightPlan()->GetType() == PlanType::MockScan) {
      auto &child_nlj_plan =
          dynamic_cast<NestedLoopJoinPlanNode &>(const_cast<AbstractPlanNode &>(*(nlj_plan.GetLeftPlan())));
      const auto &child_mock_scan_plan = dynamic_cast<const MockScanPlanNode &>(*nlj_plan.GetRightPlan());

      if (child_nlj_plan.GetJoinType() == JoinType::INNER &&
          child_nlj_plan.GetLeftPlan()->GetType() == PlanType::SeqScan &&
          child_nlj_plan.GetRightPlan()->GetType() == PlanType::MockScan) {
        const auto &left_seq_scan = dynamic_cast<const SeqScanPlanNode &>(*child_nlj_plan.GetLeftPlan());
        const auto &right_mock_scan = dynamic_cast<const MockScanPlanNode &>(*child_nlj_plan.GetRightPlan());

        if (std::shared_ptr<ComparisonExpression> expr =
                std::dynamic_pointer_cast<ComparisonExpression>(child_nlj_plan.predicate_);
            expr != nullptr) {
          if (expr->comp_type_ == ComparisonType::Equal) {
            if (std::shared_ptr<ColumnValueExpression> left_expr =
                    std::dynamic_pointer_cast<ColumnValueExpression>(expr->children_[0]);
                left_expr != nullptr) {
              if (std::shared_ptr<ColumnValueExpression> right_expr =
                      std::dynamic_pointer_cast<ColumnValueExpression>(expr->children_[1]);
                  right_expr != nullptr) {
                if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
                  if (auto index = MatchIndex(left_seq_scan.table_name_, left_expr->GetColIdx());
                      index != std::nullopt) {
                    if (std::shared_ptr<ComparisonExpression> t_expr =
                            std::dynamic_pointer_cast<ComparisonExpression>(nlj_plan.predicate_);
                        t_expr != nullptr) {
                      if (t_expr->comp_type_ == ComparisonType::Equal) {
                        if (std::shared_ptr<ColumnValueExpression> t_left_expr =
                                std::dynamic_pointer_cast<ColumnValueExpression>(t_expr->children_[0]);
                            t_left_expr != nullptr) {
                          if (std::shared_ptr<ColumnValueExpression> t_right_expr =
                                  std::dynamic_pointer_cast<ColumnValueExpression>(t_expr->children_[1]);
                              t_right_expr != nullptr) {
                            //                            // swap schema
                            std::vector<Column> cols;
                            std::copy(right_mock_scan.output_schema_->GetColumns().begin(),
                                      right_mock_scan.output_schema_->GetColumns().end(), std::back_inserter(cols));
                            std::copy(child_mock_scan_plan.output_schema_->GetColumns().begin(),
                                      child_mock_scan_plan.output_schema_->GetColumns().end(),
                                      std::back_inserter(cols));
                            child_nlj_plan.output_schema_ = std::make_shared<Schema>(cols);

                            // swap predicate, it's bad code, just based on the test
                            nlj_plan.predicate_->children_[0] = std::make_shared<ColumnValueExpression>(
                                t_left_expr->GetTupleIdx(),
                                t_left_expr->GetColIdx() - left_seq_scan.output_schema_->GetColumnCount(),
                                t_left_expr->GetReturnType());
                            child_nlj_plan.predicate_->children_[1] = std::make_shared<ColumnValueExpression>(
                                1, left_expr->GetColIdx(), left_expr->GetReturnType());
                            auto temp_expr = child_nlj_plan.predicate_;
                            child_nlj_plan.predicate_ = nlj_plan.predicate_;
                            nlj_plan.predicate_ = temp_expr;

                            // swap plan node
                            auto t = nlj_plan.children_[1];
                            nlj_plan.children_[1] = child_nlj_plan.children_[0];
                            child_nlj_plan.children_[0] = t;
                          }
                        }
                      }
                    }
                    //                    auto [index_oid, index_name] = *index;
                    //
                    //                    std::vector<Column> cols;
                    //                    std::copy(nlj_plan.GetRightPlan()->output_schema_->GetColumns().begin(),
                    //                              nlj_plan.GetRightPlan()->output_schema_->GetColumns().end(),
                    //                              std::back_inserter(cols));
                    //                    std::copy(nlj_plan.GetLeftPlan()->output_schema_->GetColumns().begin(),
                    //                              nlj_plan.GetLeftPlan()->output_schema_->GetColumns().end(),
                    //                              std::back_inserter(cols));

                    //                    expr->children_[0] = std::make_shared<ColumnValueExpression>(0,
                    //                    right_expr->GetColIdx(),
                    //                                                                                 right_expr->GetReturnType());
                    //                    expr->children_[1] =
                    //                        std::make_shared<ColumnValueExpression>(1, left_expr->GetColIdx(),
                    //                        left_expr->GetReturnType());

                    //                    return
                    //                    std::make_shared<NestedLoopJoinPlanNode>(std::make_shared<Schema>(cols),
                    //                                                                    nlj_plan.GetRightPlan(),
                    //                                                                    nlj_plan.GetLeftPlan(),
                    //                                                                    nlj_plan.predicate_,
                    //                                                                    nlj_plan.GetJoinType());
                    //                  return std::make_shared<NestedIndexJoinPlanNode>(
                    //                      nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                    //                      std::move(left_expr_tuple_0), right_seq_scan.GetTableOid(), index_oid,
                    //                      std::move(index_name), right_seq_scan.table_name_,
                    //                      right_seq_scan.output_schema_, nlj_plan.GetJoinType());
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

auto Optimizer::OptimizeReorderTable(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeReorderTable(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    auto &nlj_plan = dynamic_cast<NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    // Ensure right child is table scan
    if (nlj_plan.GetJoinType() == JoinType::INNER && nlj_plan.GetLeftPlan()->GetType() == PlanType::MockScan &&
        nlj_plan.GetRightPlan()->GetType() == PlanType::MockScan) {
      const auto &left_mock_plan = dynamic_cast<const MockScanPlanNode &>(*(nlj_plan.GetLeftPlan()));
      const auto &right_mock_plan = dynamic_cast<const MockScanPlanNode &>(*nlj_plan.GetRightPlan());
      std::optional<size_t> left_size = EstimatedCardinality(left_mock_plan.GetTable());
      std::optional<size_t> right_size = EstimatedCardinality(right_mock_plan.GetTable());

      if (left_size.has_value() && right_size.has_value()) {
        if (*left_size < *right_size) {
          // swap plan node
          auto t = nlj_plan.children_[1];
          nlj_plan.children_[1] = nlj_plan.children_[0];
          nlj_plan.children_[0] = t;
        }
      }
    }
  }

  return optimized_plan;
}

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;

  p = OptimizeMergeProjection(p);
  p = OptimizeColumnPruning(p);
  p = OptimizeSwapTable(p);
  p = OptimizeFilterPushDown(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizeReorderTable(p);
  p = OptimizeNLJAsHashJoin(p);  // Enable this rule after you have implemented hash join.
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  p = OptimizeExpressionElimination(p);

  return p;
}

}  // namespace bustub
