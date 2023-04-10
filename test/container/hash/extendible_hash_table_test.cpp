/**
 * extendible_hash_test.cpp
 */

#include <memory>
#include <thread>  // NOLINT
#include "common/logger.h"

#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(ExtendibleHashTableTest, Test3) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);
  table->Insert(4, "a");
  table->Insert(12, "a");
  table->Insert(16, "a");
  table->Insert(64, "a");
  table->Insert(5, "a");
  table->Insert(10, "a");
  table->Insert(51, "a");
  table->Insert(15, "a");
  table->Insert(18, "a");
  table->Insert(20, "a");
  table->Insert(7, "a");
  table->Insert(21, "a");
  table->Insert(11, "a");
  table->Insert(19, "a");
  EXPECT_EQ(2, table->GetLocalDepth(1));
}

TEST(ExtendibleHashTableTest, Test2) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);
  table->Insert(4, "a");
  //  std::cout << "1\n";
  table->Insert(12, "b");
  table->Insert(10, "d");
  table->Insert(16, "c");
  //  std::cout << "2\n";
  table->Insert(15, "d");

  table->Insert(7, "e");
  //  std::cout << "3\n";
  table->Insert(18, "c");
  table->Insert(23, "e");

  table->Insert(20, "e");
  table->Insert(31, "c");
  //  table->Insert(64, "e");
  table->Insert(39, "d");
  table->Insert(64, "e");

  EXPECT_EQ(7, table->GetNumBuckets());
  EXPECT_EQ(3, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(7));
  EXPECT_EQ(4, table->GetGlobalDepth());
}

TEST(ExtendibleHashTableTest, Test1) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(5);
  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  EXPECT_EQ(1, table->GetNumBuckets());
  EXPECT_EQ(0, table->GetLocalDepth(0));
  EXPECT_EQ(0, table->GetGlobalDepth());
  std::string result;
  table->Find(5, result);
  EXPECT_EQ("e", result);
  EXPECT_FALSE(table->Find(10, result));

  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "f");
  table->Find(5, result);
  EXPECT_EQ("f", result);

  table->Insert(6, "a");
  table->Insert(7, "b");
  table->Insert(8, "c");
  table->Insert(9, "d");
  table->Insert(10, "f");
  EXPECT_EQ(2, table->GetNumBuckets());
  EXPECT_EQ(1, table->GetLocalDepth(0));
  EXPECT_EQ(1, table->GetLocalDepth(1));
  EXPECT_EQ(1, table->GetGlobalDepth());
  table->Find(8, result);
  EXPECT_EQ("c", result);

  table->Insert(11, "a");
  table->Insert(12, "b");
  table->Insert(13, "c");
  table->Insert(14, "d");
  table->Insert(15, "f");
  EXPECT_EQ(4, table->GetNumBuckets());
  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(2, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(3));
  EXPECT_EQ(2, table->GetGlobalDepth());
  table->Find(13, result);
  EXPECT_EQ("c", result);

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(12));
  EXPECT_EQ(3, table->GetNumBuckets());
  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(2, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(3));
  EXPECT_EQ(2, table->GetGlobalDepth());
  EXPECT_FALSE(table->Find(8, result));

  table->Insert(4, "a");
  table->Insert(12, "b");
  table->Insert(8, "c");
  table->Find(8, result);
  EXPECT_EQ("c", result);
  EXPECT_EQ(4, table->GetNumBuckets());
  EXPECT_EQ(2, table->GetLocalDepth(0));

  table->Insert(20, "a");
  table->Insert(16, "b");
  table->Insert(18, "c");
}

TEST(ExtendibleHashTableTest, SampleTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");

  EXPECT_EQ(5, table->GetNumBuckets());

  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  std::string result;
  table->Find(9, result);
  EXPECT_EQ("i", result);
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));
}

TEST(ExtendibleHashTableTest, ConcurrentInsertTest) {
  const int num_runs = 50;
  const int num_threads = 3;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Insert(tid, tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

}  // namespace bustub
