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
  RID rid3{0, 3};
  RID rid4{0, 4};
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
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_SHARED, toid);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::SHARED, toid, rid0);
    EXPECT_EQ(true, res);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

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
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    lock_mgr.UnlockRow(txn1, 0, rid1);
    lock_mgr.UnlockTable(txn1, 0);

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
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    lock_mgr.UnlockRow(txn2, 0, rid2);
    lock_mgr.UnlockTable(txn2, 0);

    txn_mgr.Commit(txn2);
    EXPECT_EQ(TransactionState::COMMITTED, txn2->GetState());
  });

  std::thread t3([&] {
    //    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn3, LockManager::LockMode::INTENTION_SHARED, 0);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockRow(txn3, LockManager::LockMode::SHARED, 0, rid3);
    //    EXPECT_EQ(TransactionState::GROWING, txn3->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    res = lock_mgr.LockTable(txn3, LockManager::LockMode::INTENTION_EXCLUSIVE, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    lock_mgr.UnlockRow(txn3, 0, rid3);
    lock_mgr.UnlockTable(txn3, 0);

    txn_mgr.Commit(txn3);
    EXPECT_EQ(TransactionState::COMMITTED, txn3->GetState());
  });

  std::thread t4([&] {
    //    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn4, LockManager::LockMode::INTENTION_SHARED, 0);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockRow(txn4, LockManager::LockMode::SHARED, 0, rid4);
    EXPECT_EQ(TransactionState::GROWING, txn4->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    res = lock_mgr.LockTable(txn4, LockManager::LockMode::INTENTION_EXCLUSIVE, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    lock_mgr.UnlockRow(txn4, 0, rid4);
    lock_mgr.UnlockTable(txn4, 0);

    txn_mgr.Commit(txn4);
    EXPECT_EQ(TransactionState::COMMITTED, txn4->GetState());
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

TEST(LockManagerDeadlockDetectionTest, CycleTest11) {
  LockManager lock_mgr{};
  //  lock_mgr.AddEdge(81, 63);
  //  lock_mgr.AddEdge(38, 66);
  //  lock_mgr.AddEdge(95, 33);
  //  lock_mgr.AddEdge(90, 37);
  //  lock_mgr.AddEdge(20, 35);
  //
  //  lock_mgr.AddEdge(14, 22);
  //  lock_mgr.AddEdge(67, 73);
  //  lock_mgr.AddEdge(36, 19);
  //  lock_mgr.AddEdge(30, 64);
  //  lock_mgr.AddEdge(3, 10);
  //  lock_mgr.AddEdge(99, 71);
  //  lock_mgr.AddEdge(59, 70);
  //  lock_mgr.AddEdge(77, 24);
  //  lock_mgr.AddEdge(96, 12);
  //  lock_mgr.AddEdge(78, 42);
  //  lock_mgr.AddEdge(29, 5);
  //  lock_mgr.AddEdge(61, 0);
  //  lock_mgr.AddEdge(26, 54);
  //  lock_mgr.AddEdge(85, 9);
  //  lock_mgr.AddEdge(15, 13);
  //  lock_mgr.AddEdge(18, 58);
  //  lock_mgr.AddEdge(8, 98);
  //  lock_mgr.AddEdge(97, 51);
  //  lock_mgr.AddEdge(83, 87);
  //  lock_mgr.AddEdge(62, 45);
  //  lock_mgr.AddEdge(68, 43);
  //  lock_mgr.AddEdge(41, 17);
  //  lock_mgr.AddEdge(55, 75);
  //  lock_mgr.AddEdge(91, 88);
  //  lock_mgr.AddEdge(92, 32);
  //
  //  lock_mgr.AddEdge(47, 84);
  //  lock_mgr.AddEdge(11, 28);
  //  lock_mgr.AddEdge(40, 86);
  //  lock_mgr.AddEdge(65, 2);
  //  lock_mgr.AddEdge(79, 93);
  //  lock_mgr.AddEdge(56, 7);
  //  lock_mgr.AddEdge(16, 31);
  //  lock_mgr.AddEdge(46, 69);
  //  lock_mgr.AddEdge(76, 39);
  //
  //  lock_mgr.AddEdge(21, 44);
  //  lock_mgr.AddEdge(25, 82);
  //  lock_mgr.AddEdge(34, 72);
  //  lock_mgr.AddEdge(94, 1);
  //  lock_mgr.AddEdge(6, 53);
  //  lock_mgr.AddEdge(89, 48);
  //  lock_mgr.AddEdge(4, 74);
  //  lock_mgr.AddEdge(57, 60);
  //  lock_mgr.AddEdge(23, 52);
  //  lock_mgr.AddEdge(50, 27);
  //  lock_mgr.AddEdge(46, 80);
  lock_mgr.AddEdge(0, 1);
  lock_mgr.AddEdge(1, 2);
  lock_mgr.AddEdge(2, 3);
  lock_mgr.AddEdge(3, 4);
  lock_mgr.AddEdge(4, 5);
  lock_mgr.AddEdge(5, 0);
  EXPECT_EQ(6, lock_mgr.GetEdgeList().size());

  lock_mgr.AddEdge(2, 6);
  EXPECT_EQ(7, lock_mgr.GetEdgeList().size());
  lock_mgr.AddEdge(6, 7);
  EXPECT_EQ(8, lock_mgr.GetEdgeList().size());
  lock_mgr.AddEdge(7, 2);

  EXPECT_EQ(9, lock_mgr.GetEdgeList().size());

  //  txn_id_t txn_id;
  //  lock_mgr.HasCycle(&txn_id);
  //  EXPECT_EQ(1, txn_id);
  //  lock_mgr.RemoveEdge(1, 0);
  //  lock_mgr.HasCycle(&txn_id);
  //  EXPECT_EQ(4, txn_id);
  //  lock_mgr.RemoveEdge(4, 2);
  //  lock_mgr.AddEdge(4, 5);
  //  bool res = lock_mgr.HasCycle(&txn_id);
  //  EXPECT_FALSE(res);
}

TEST(LockManagerDeadlockDetectionTest, EdgeTest) {
  LockManager lock_mgr{};

  const int num_nodes = 100;
  const int num_edges = num_nodes / 2;
  const int seed = 15445;
  std::srand(seed);

  // Create txn ids and shuffle
  std::vector<txn_id_t> txn_ids;
  txn_ids.reserve(num_nodes);
  for (int i = 0; i < num_nodes; i++) {
    txn_ids.push_back(i);
  }
  EXPECT_EQ(num_nodes, txn_ids.size());
  auto rng = std::default_random_engine{};
  std::shuffle(txn_ids.begin(), txn_ids.end(), rng);
  EXPECT_EQ(num_nodes, txn_ids.size());

  // Create edges by pairing adjacent txn_ids
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (int i = 0; i < num_nodes; i += 2) {
    EXPECT_EQ(i / 2, lock_mgr.GetEdgeList().size());
    auto t1 = txn_ids[i];
    auto t2 = txn_ids[i + 1];
    lock_mgr.AddEdge(t1, t2);
    edges.emplace_back(t1, t2);
    EXPECT_EQ((i / 2) + 1, lock_mgr.GetEdgeList().size());
  }

  auto lock_mgr_edges = lock_mgr.GetEdgeList();
  EXPECT_EQ(num_edges, lock_mgr_edges.size());
  EXPECT_EQ(num_edges, edges.size());

  std::sort(lock_mgr_edges.begin(), lock_mgr_edges.end());
  std::sort(edges.begin(), edges.end());

  for (int i = 0; i < num_edges; i++) {
    EXPECT_EQ(edges[i], lock_mgr_edges[i]);
  }
}

TEST(LockManagerDeadlockDetectionTest, BasicDeadlockDetectionTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t toid{0};
  RID rid0{0, 0};
  RID rid1{1, 1};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // This will block
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(true, res);

    lock_mgr.UnlockRow(txn0, toid, rid1);
    lock_mgr.UnlockRow(txn0, toid, rid0);
    lock_mgr.UnlockTable(txn0, toid);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);

    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());

    // This will block
    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(res, false);

    EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
    txn_mgr.Abort(txn1);
  });

  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();

  delete txn0;
  delete txn1;
}

TEST(LockManagerDeadlockDetectionTest, BasicDeadlockDetectionTest1) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t toid{0};
  RID rid0{0, 0};
  RID rid1{1, 1};
  RID rid2{2, 2};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());
  EXPECT_EQ(2, txn2->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // This will block
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(true, res);

    lock_mgr.UnlockRow(txn0, toid, rid1);
    lock_mgr.UnlockRow(txn0, toid, rid0);
    lock_mgr.UnlockTable(txn0, toid);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    // Sleep so T0 can take necessary locks
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);

    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // This will block
    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid2);
    EXPECT_EQ(res, true);

    lock_mgr.UnlockRow(txn1, toid, rid2);
    lock_mgr.UnlockRow(txn1, toid, rid1);
    lock_mgr.UnlockTable(txn1, toid);

    txn_mgr.Commit(txn1);
    EXPECT_EQ(TransactionState::COMMITTED, txn1->GetState());
  });

  std::thread t2([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);
    res = lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, toid, rid2);
    EXPECT_EQ(TransactionState::GROWING, txn2->GetState());

    // This will block
    res = lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(res, false);

    EXPECT_EQ(TransactionState::ABORTED, txn2->GetState());
    txn_mgr.Abort(txn2);
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

TEST(LockManagerDeadlockDetectionTest, CycleTest1) {
  LockManager lock_mgr{};
  lock_mgr.AddEdge(0, 1);
  lock_mgr.AddEdge(1, 0);
  lock_mgr.AddEdge(2, 3);
  lock_mgr.AddEdge(3, 4);
  lock_mgr.AddEdge(4, 2);
  txn_id_t txn_id;
  lock_mgr.HasCycle(&txn_id);
  EXPECT_EQ(1, txn_id);
  lock_mgr.RemoveEdge(1, 0);
  lock_mgr.HasCycle(&txn_id);
  EXPECT_EQ(4, txn_id);
  lock_mgr.RemoveEdge(4, 2);
  lock_mgr.AddEdge(4, 5);
  bool res = lock_mgr.HasCycle(&txn_id);
  EXPECT_FALSE(res);
}

TEST(LockManagerDeadlockDetectionTest, BasicDeadlockDetectionTest222) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t toid{0};
  RID rid0{0, 0};
  RID rid1{1, 1};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // This will block
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(true, res);

    lock_mgr.UnlockRow(txn0, toid, rid1);
    lock_mgr.UnlockRow(txn0, toid, rid0);
    lock_mgr.UnlockTable(txn0, toid);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);

    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());

    // This will block
    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(res, false);

    EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
    txn_mgr.Abort(txn1);
  });

  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();

  delete txn0;
  delete txn1;
}
}  // namespace bustub
