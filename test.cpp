/**
 * deadlock_detection_test.cpp
 */

#include <atomic>
#include <random>
#include <thread>  // NOLINT

#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"
#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace bustub {
TEST(LockManagerDeadlockDetectionTest, BasicDeadlockDetectionTest888) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t toid{0};
  RID rid0{0, 0};
  RID rid1{0, 1};
  RID rid2{0, 2};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  auto *txn3 = txn_mgr.Begin();
  auto *txn4 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());
  EXPECT_EQ(2, txn2->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_SHARED, 0);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::SHARED, 0, rid0);
    EXPECT_EQ(true, res);
//    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, 0);

    // This will block
//    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid1);
//    EXPECT_EQ(true, res);

    lock_mgr.UnlockRow(txn0, toid, rid1);
    lock_mgr.UnlockRow(txn0, toid, rid0);
    lock_mgr.UnlockTable(txn0, toid);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    // Sleep so T0 can take necessary locks
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_SHARED, 0);
    EXPECT_EQ(res, true);

    res = lock_mgr.LockRow(txn1, LockManager::LockMode::SHARED, 0, rid1);
//    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, 0);

//    // This will block
//    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid2);
//    EXPECT_EQ(res, true);

    lock_mgr.UnlockRow(txn1, toid, rid2);
    lock_mgr.UnlockRow(txn1, toid, rid1);
    lock_mgr.UnlockTable(txn1, toid);

    txn_mgr.Commit(txn1);
    EXPECT_EQ(TransactionState::COMMITTED, txn1->GetState());
  });

  std::thread t2([&] {
//    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_SHARED, 0);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockRow(txn2, LockManager::LockMode::SHARED, 0, rid2);
    EXPECT_EQ(TransactionState::GROWING, txn2->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    res = lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_EXCLUSIVE, 0);

    // This will block
//    res = lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, toid, rid0);
//    EXPECT_EQ(res, false);

//    EXPECT_EQ(TransactionState::ABORTED, txn2->GetState());
//    txn_mgr.Abort(txn2);
  });

  std::thread t3([&] {
    //    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn3, LockManager::LockMode::INTENTION_SHARED, 0);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockRow(txn3, LockManager::LockMode::SHARED, 0, {0, 3});
    EXPECT_EQ(TransactionState::GROWING, txn2->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    res = lock_mgr.LockTable(txn3, LockManager::LockMode::INTENTION_EXCLUSIVE, 0);

    // This will block
    //    res = lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    //    EXPECT_EQ(res, false);

    //    EXPECT_EQ(TransactionState::ABORTED, txn2->GetState());
    //    txn_mgr.Abort(txn2);
  });

  std::thread t4([&] {
    //    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn4, LockManager::LockMode::INTENTION_SHARED, 0);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockRow(txn4, LockManager::LockMode::SHARED, 0, {0, 4});
    EXPECT_EQ(TransactionState::GROWING, txn2->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    res = lock_mgr.LockTable(txn4, LockManager::LockMode::INTENTION_EXCLUSIVE, 0);
    // This will block
    //    res = lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    //    EXPECT_EQ(res, false);

    //    EXPECT_EQ(TransactionState::ABORTED, txn2->GetState());
    //    txn_mgr.Abort(txn2);
  });


  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();
  t2.join();
  delete txn0;
  delete txn1;
  delete txn2;
}


}  // namespace bustub
