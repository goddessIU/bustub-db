/**
 * lock_manager_test.cpp
 */

#include "concurrency/lock_manager.h"

#include <random>
#include <thread>  // NOLINT

#include "common/config.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace bustub {

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) {
  //  std::cout << "state " << (int)txn->GetState() << std::endl;
  EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED);
}

void CheckTxnRowLockSize(Transaction *txn, table_oid_t oid, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ((*(txn->GetSharedRowLockSet()))[oid].size(), shared_size);
  EXPECT_EQ((*(txn->GetExclusiveRowLockSet()))[oid].size(), exclusive_size);
}

int GetTxnTableLockSize(Transaction *txn, LockManager::LockMode lock_mode) {
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      return txn->GetSharedTableLockSet()->size();
    case LockManager::LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet()->size();
    case LockManager::LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet()->size();
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet()->size();
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet()->size();
  }

  return -1;
}

void CheckTableLockSizes(Transaction *txn, size_t s_size, size_t x_size, size_t is_size, size_t ix_size,
                         size_t six_size) {
  EXPECT_EQ(s_size, txn->GetSharedTableLockSet()->size());
  EXPECT_EQ(x_size, txn->GetExclusiveTableLockSet()->size());
  EXPECT_EQ(is_size, txn->GetIntentionSharedTableLockSet()->size());
  EXPECT_EQ(ix_size, txn->GetIntentionExclusiveTableLockSet()->size());
  EXPECT_EQ(six_size, txn->GetSharedIntentionExclusiveTableLockSet()->size());
}

// TEST(LockManagerTest, CompatibilityTest1) {
//  LockManager lock_mgr{};
//  TransactionManager txn_mgr{&lock_mgr};
//  table_oid_t oid = 0;
//  auto *txn0 = txn_mgr.Begin();
//  auto *txn1 = txn_mgr.Begin();
//  // [S] SIX IS
//  // [SIX IS]
//  //    txn_mgr.Commit(txn0);
//  std::thread t0([&]() {
//    oid = 0;
//    lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_SHARED, oid);
//    lock_mgr.UnlockTable(txn0,oid);
////    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    oid = 0;
//    lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_SHARED, 0);
//    lock_mgr.UnlockTable(txn0, oid);
////    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    oid = 0;
//    lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_SHARED, 0);
//    lock_mgr.UnlockTable(txn0, oid);
////    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    oid = 0;
//    lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_SHARED, 0);
//    lock_mgr.UnlockTable(txn0, oid);
////    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    oid = 0;
//    lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, 0);
//    lock_mgr.UnlockTable(txn0, oid);
////    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, 0);
//    lock_mgr.UnlockTable(txn0, oid);
////    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED, 0);
//    lock_mgr.UnlockTable(txn0, oid);
////    std::this_thread::sleep_for(std::chrono::milliseconds(15));
//
//
//    lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED, 0);
//    lock_mgr.UnlockTable(txn0, oid);
////    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, 0);
//    lock_mgr.UnlockTable(txn0, oid);
////    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_SHARED, 0);
//    lock_mgr.UnlockTable(txn0, oid);
////    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//  });
//
//  std::thread t1([&]() {
//    lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_SHARED, oid);
//    lock_mgr.UnlockTable(txn1,oid);
//    //    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    oid = 0;
//    lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, 0);
//    lock_mgr.UnlockTable(txn1, oid);
//    //    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    oid = 0;
//    lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, 0);
//    lock_mgr.UnlockTable(txn1, oid);
//    //    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    oid = 0;
//    lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, 0);
//    lock_mgr.UnlockTable(txn1, oid);
//    //    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    oid = 0;
//    lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_SHARED, 0);
//    lock_mgr.UnlockTable(txn1, oid);
//    //    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, 0);
//    lock_mgr.UnlockTable(txn1, oid);
//    //    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_SHARED, 0);
//    lock_mgr.UnlockTable(txn1, oid);
//    //    std::this_thread::sleep_for(std::chrono::milliseconds(15));
//
//
//    lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, 0);
//    lock_mgr.UnlockTable(txn1, oid);
//    //    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, 0);
//    lock_mgr.UnlockTable(txn1, oid);
//    //    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//
//    lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, 0);
//    lock_mgr.UnlockTable(txn1, oid);
//    //    std::this_thread::sleep_for(std::chrono::milliseconds(10));
//  });
//
//  t0.join();
//  t1.join();
//  delete txn0;
//  delete txn1;
//}
//
//
//

// TEST(LockManagerTest, AbortTest1) {
//  for (int i = 0; i < 1; i++) {
//    LockManager lock_mgr{};
//    TransactionManager txn_mgr{&lock_mgr};
//    table_oid_t oid = 0;
//
//    auto *txn0 = txn_mgr.Begin();
//    auto *txn1 = txn_mgr.Begin();
//    auto *txn2 = txn_mgr.Begin();
//    auto *txn3 = txn_mgr.Begin();
//    auto *txn4 = txn_mgr.Begin();
//    auto *txn5 = txn_mgr.Begin();
//    auto *txn6 = txn_mgr.Begin();
//    auto *txn7 = txn_mgr.Begin();
//    auto *txn8 = txn_mgr.Begin();
//    auto *txn9 = txn_mgr.Begin();
//
//    std::thread t1([&]() {
//      bool res;
//      res = lock_mgr.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, oid);
//      EXPECT_TRUE(res);
//      std::this_thread::sleep_for(std::chrono::milliseconds(100));
//      lock_mgr.UnlockTable(txn0, oid);
//      txn_mgr.Commit(txn0);
//      EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
//    });
//
//    std::thread t2([&]() {
//      bool res;
//      std::this_thread::sleep_for(std::chrono::milliseconds(30));
//      res = lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid);
//      EXPECT_FALSE(res);
//
//      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
//
//
//    });
//
//    std::thread t3([&]() {
//      bool res;
//      std::this_thread::sleep_for(std::chrono::milliseconds(30));
//      res = lock_mgr.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, oid);
//      EXPECT_TRUE(res);
//      lock_mgr.UnlockTable(txn2, oid);
//      txn_mgr.Commit(txn2);
//      EXPECT_EQ(TransactionState::COMMITTED, txn2->GetState());
//    });
//
//    std::this_thread::sleep_for(std::chrono::milliseconds(70));
//    txn1->SetState(TransactionState::ABORTED);
//
//    t1.join();
//    t2.join();
//    t3.join();
//    txn_mgr.Abort(txn1);
//
//
//
//    std::thread t4([&]() {
//      bool res;
//      res = lock_mgr.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, oid);
//      EXPECT_TRUE(res);
//      std::this_thread::sleep_for(std::chrono::milliseconds(100));
//      lock_mgr.UnlockTable(txn0, oid);
//      txn_mgr.Commit(txn0);
//      EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
//    });
//
//    std::thread t5([&]() {
//      bool res;
//      std::this_thread::sleep_for(std::chrono::milliseconds(30));
//      res = lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid);
//      EXPECT_FALSE(res);
//
//      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
//
//    });
//
//    std::thread t6([&]() {
//      bool res;
//      std::this_thread::sleep_for(std::chrono::milliseconds(30));
//      res = lock_mgr.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, oid);
//      EXPECT_FALSE(res);
//
//      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
//    });
//
//    std::thread t7([&]() {
//      bool res;
//      std::this_thread::sleep_for(std::chrono::milliseconds(30));
//      res = lock_mgr.LockTable(txn3, LockManager::LockMode::EXCLUSIVE, oid);
//      EXPECT_FALSE(res);
//
//      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
//    });
//
//    std::thread t8([&]() {
//      bool res;
//      std::this_thread::sleep_for(std::chrono::milliseconds(30));
//      res = lock_mgr.LockTable(txn4, LockManager::LockMode::EXCLUSIVE, oid);
//      EXPECT_FALSE(res);
//
//      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
//
//    });
//
//    std::thread t9([&]() {
//      bool res;
//      std::this_thread::sleep_for(std::chrono::milliseconds(30));
//      res = lock_mgr.LockTable(txn5, LockManager::LockMode::EXCLUSIVE, oid);
//      EXPECT_FALSE(res);
//
//      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
//    });
//
//    std::thread t10([&]() {
//      bool res;
//      std::this_thread::sleep_for(std::chrono::milliseconds(30));
//      res = lock_mgr.LockTable(txn6, LockManager::LockMode::EXCLUSIVE, oid);
//      EXPECT_FALSE(res);
//
//      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
//    });
//
//    std::thread t11([&]() {
//      bool res;
//      std::this_thread::sleep_for(std::chrono::milliseconds(30));
//      res = lock_mgr.LockTable(txn7, LockManager::LockMode::EXCLUSIVE, oid);
//      EXPECT_FALSE(res);
//
//      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
//
//    });
//
//    std::thread t12([&]() {
//      bool res;
//      std::this_thread::sleep_for(std::chrono::milliseconds(30));
//      res = lock_mgr.LockTable(txn8, LockManager::LockMode::EXCLUSIVE, oid);
//      EXPECT_FALSE(res);
//
//      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
//    });
//
//    std::thread t13([&]() {
//      bool res;
//      std::this_thread::sleep_for(std::chrono::milliseconds(30));
//      res = lock_mgr.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, oid);
//      EXPECT_TRUE(res);
//      lock_mgr.UnlockTable(txn2, oid);
//      txn_mgr.Commit(txn2);
//      EXPECT_EQ(TransactionState::COMMITTED, txn2->GetState());
//    });
//
//
//    std::this_thread::sleep_for(std::chrono::milliseconds(70));
//    txn1->SetState(TransactionState::ABORTED);
//    txn1->SetState(TransactionState::ABORTED);
//    txn1->SetState(TransactionState::ABORTED);
//    txn1->SetState(TransactionState::ABORTED);
//    txn1->SetState(TransactionState::ABORTED);
//    txn1->SetState(TransactionState::ABORTED);
//    txn1->SetState(TransactionState::ABORTED);
//    txn1->SetState(TransactionState::ABORTED);
//    txn1->SetState(TransactionState::ABORTED);
//    txn1->SetState(TransactionState::ABORTED);
//    txn1->SetState(TransactionState::ABORTED);
//    txn1->SetState(TransactionState::ABORTED);
//
//    t4.join();
//    t5.join();
//    t6.join();
//    t7.join();
//    t8.join();
//    t9.join();
//    t10.join();
//    t11.join();
//    t12.join();
//    t13.join();
//    txn_mgr.Abort(txn1);
//
//    delete txn0;
//    delete txn1;
//    delete txn2;
//    delete txn3;
//    delete txn4;
//    delete txn5;
//    delete txn6;
//    delete txn7;
//    delete txn8;
//    delete txn9;
//
//  }
//
//}

void TableLockTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<table_oid_t> oids;
  std::vector<Transaction *> txns;
  /** 10 tables */
  int num_oids = 10;
  for (int i = 0; i < num_oids; i++) {
    table_oid_t oid{static_cast<uint32_t>(i)};
    oids.push_back(oid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }
  /** Each transaction takes an S lock on every table and then unlocks */
  auto task = [&](int txn_id) {
    bool res;
    for (const table_oid_t &oid : oids) {
      //      LOG_INFO()
      //            std::cout << "begin lock " << txn_id << "oid " << oid << std::endl;
      res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, oid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
      //      std::cout << "lock ok " << txn_id << "oid " << oid << std::endl;
    }
    //    std::cout << 7 << std::endl;
    for (const table_oid_t &oid : oids) {
      //            std::cout << "bgin unlock " << txn_id << "oid " << oid << std::endl;
      res = lock_mgr.UnlockTable(txns[txn_id], oid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
      //      std::cout << "unlock ok " << txn_id << "oid " << oid << std::endl;
    }
    //    std::cout << 8 << std::endl;
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
    //    std::cout << 9 << std::endl;
    /** All locks should be dropped */
    CheckTableLockSizes(txns[txn_id], 0, 0, 0, 0, 0);
    //    std::cout << 10 << std::endl;
  };

  std::vector<std::thread> threads;
  threads.reserve(num_oids);

  for (int i = 0; i < num_oids; i++) {
    threads.emplace_back(std::thread{task, i});
  }
  //  std::cout << 3 << std::endl;
  for (int i = 0; i < num_oids; i++) {
    //    std::cout << "thread " << i << std::endl;
    threads[i].join();
  }
  //  std::cout << 6 << std::endl;
  for (int i = 0; i < num_oids; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, TableLockTest1) { TableLockTest1(); }  // NOLINT

/** Upgrading single transaction from S -> X */
void TableLockUpgradeTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  auto txn1 = txn_mgr.Begin();

  //  std::cout << "a" << std::endl;
  /** Take S lock */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, oid));
  CheckTableLockSizes(txn1, 1, 0, 0, 0, 0);
  //  std::cout << "a" << std::endl;
  /** Upgrade S to X */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid));
  CheckTableLockSizes(txn1, 0, 1, 0, 0, 0);
  //  std::cout << "a" << std::endl;
  /** Clean up */
  txn_mgr.Commit(txn1);
  //  std::cout << "t6 " << std::endl;
  CheckCommitted(txn1);
  //  std::cout << "t6 " << std::endl;
  CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);
  //  std::cout << "t6 " << std::endl;
  delete txn1;
}
TEST(LockManagerTest, TableLockUpgradeTest1) { TableLockUpgradeTest1(); }  // NOLINT

void RowLockTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  RID rid{0, 0};

  int num_txns = 3;
  std::vector<Transaction *> txns;
  for (int i = 0; i < num_txns; i++) {
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Each transaction takes an S lock on the same table and row and then unlocks */
  auto task = [&](int txn_id) {
    bool res;

    res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[txn_id]);

    res = lock_mgr.LockRow(txns[txn_id], LockManager::LockMode::SHARED, oid, rid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[txn_id]);
    /** Lock set should be updated */
    ASSERT_EQ(true, txns[txn_id]->IsRowSharedLocked(oid, rid));

    res = lock_mgr.UnlockRow(txns[txn_id], oid, rid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[txn_id]);
    /** Lock set should be updated */
    ASSERT_EQ(false, txns[txn_id]->IsRowSharedLocked(oid, rid));

    res = lock_mgr.UnlockTable(txns[txn_id], oid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[txn_id]);

    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_txns);

  for (int i = 0; i < num_txns; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_txns; i++) {
    threads[i].join();
    delete txns[i];
  }
}
TEST(LockManagerTest, RowLockTest1) { RowLockTest1(); }  // NOLINT

void TwoPLTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  RID rid0{0, 0};
  RID rid1{0, 1};

  auto *txn = txn_mgr.Begin();
  EXPECT_EQ(0, txn->GetTransactionId());

  bool res;
  res = lock_mgr.LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  EXPECT_TRUE(res);

  //  std::cout << 6 << std::endl;

  res = lock_mgr.LockRow(txn, LockManager::LockMode::SHARED, oid, rid0);
  EXPECT_TRUE(res);

  //  std::cout << 7 << std::endl;

  CheckGrowing(txn);
  CheckTxnRowLockSize(txn, oid, 1, 0);

  //  std::cout << 1 << std::endl;

  res = lock_mgr.LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, rid1);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnRowLockSize(txn, oid, 1, 1);

  //  std::cout << 2 << std::endl;

  res = lock_mgr.UnlockRow(txn, oid, rid0);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnRowLockSize(txn, oid, 0, 1);

  //  std::cout << 3 << std::endl;

  try {
    lock_mgr.LockRow(txn, LockManager::LockMode::SHARED, oid, rid0);
  } catch (TransactionAbortException &e) {
    CheckAborted(txn);
    CheckTxnRowLockSize(txn, oid, 0, 1);
  }

  // Need to call txn_mgr's abort
  txn_mgr.Abort(txn);
  CheckAborted(txn);
  CheckTxnRowLockSize(txn, oid, 0, 0);
  CheckTableLockSizes(txn, 0, 0, 0, 0, 0);

  delete txn;
}

TEST(LockManagerTest, TwoPLTest1) { TwoPLTest1(); }  // NOLINT

}  // namespace bustub
