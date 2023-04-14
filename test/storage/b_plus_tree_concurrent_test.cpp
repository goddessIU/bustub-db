/**
 * grading_b_plus_tree_checkpoint_2_concurrent_test.cpp
 */

#include <chrono>  // NOLINT
#include <cstdio>
#include <functional>
#include <future>       // NOLINT
#include <thread>       // NOLINT
#include "test_util.h"  // NOLINT

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

// Macro for time out mechanism
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

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  uint64_t tid, BufferPoolManager *bpm, __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(tid);
  for (int i = 0; i < static_cast<int>(keys.size()); i++) {
    auto key = keys[i];
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
    //        tree->Draw(bpm, std::to_string(key) + ".dot");
    //    tree->Print(bpm);
  }
  delete transaction;
}

void InsertHelper1(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                   uint64_t tid, BufferPoolManager *bpm, __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(tid);
  for (int i = 0; i < static_cast<int>(keys.size()); i++) {
    auto key = keys[i];
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
    //    tree->Draw(bpm, std::to_string(i) + ".dot");
    //    tree->Print(bpm);
  }
  delete transaction;
}
// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, uint64_t tid, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(tid);
  for (int i = 0; i < static_cast<int>(keys.size()); i++) {
    auto key = keys[i];
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  uint64_t tid, BufferPoolManager *bpm, __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(tid);
  for (int i = 0; i < static_cast<int>(remove_keys.size()); i++) {
    auto key = remove_keys[i];
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
    //    tree->Draw(bpm, std::to_string(key) + ".dot");
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads, uint64_t tid, int iter,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(tid);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

void LookupHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  Transaction *transaction = new Transaction(tid);
  GenericKey<8> index_key;
  RID rid;
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    std::vector<RID> result;
    std::cout << "4" << std::endl;
    bool res = tree->GetValue(index_key, &result, transaction);
    std::cout << "5" << std::endl;
    EXPECT_EQ(res, true);
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], rid);
  }
  delete transaction;
}

const size_t NUM_ITERS = 100;
// 100
const size_t NUM_ITERS_DEBUG = 100;

void MixTest1Call1() {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 5);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> for_insert;
  std::vector<int64_t> for_delete;
  size_t sieve = 2;  // divide evenly
  size_t total_keys = 1000;
  for (size_t i = 1; i <= total_keys; i++) {
    if (i % sieve == 0) {
      for_insert.push_back(i);
    } else {
      for_delete.push_back(i);
    }
  }

  GenericKey<8> index_key;
  RID rid;
  int tid = 0;
  // create transaction
  Transaction *transaction = new Transaction(tid);

  // Insert all the keys to delete
  for (int i = 0; i < static_cast<int>(for_delete.size()); i++) {
    auto key = for_delete[i];
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    //    tree->Draw(bpm, std::to_string(i) + ".dot");
  }

  // 24
  std::vector<int64_t> keys{};

  for (auto key : keys) {
    if (key % 2 == 0) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree.Insert(index_key, rid, transaction);
    } else {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree.Remove(index_key, transaction);
    }
  }
  tree.Draw(bpm, "6.dot");

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

void MixTest1Call2() {
  for (size_t iter = 0; iter < 1; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 5);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // first, populate index
    std::vector<int64_t> for_insert;
    std::vector<int64_t> for_delete;
    size_t sieve = 2;  // divide evenly
    size_t total_keys = 500;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        for_insert.push_back(i);
      } else {
        for_delete.push_back(i);
      }
    }

    GenericKey<8> index_key;
    RID rid;
    int tid = 0;
    // create transaction
    Transaction *transaction = new Transaction(tid);

    // Insert all the keys to delete
    for (int i = 0; i < static_cast<int>(for_insert.size()); i++) {
      auto key = for_insert[i];
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree.Insert(index_key, rid, transaction);
      //    tree->Draw(bpm, std::to_string(i) + ".dot");
    }

    std::vector<int64_t> keys{
        2,   4,   1,   6,   3,   8,   5,   10,  7,   12,  9,   14,  11,  16,  18,  13,  20,  15,  22,  17,  24,  19,
        26,  21,  28,  23,  30,  25,  32,  27,  34,  29,  36,  31,  38,  33,  35,  40,  37,  42,  39,  44,  41,  46,
        43,  48,  45,  50,  47,  49,  52,  51,  54,  53,  56,  55,  58,  57,  60,  59,  62,  61,  64,  63,  66,  65,
        68,  67,  70,  69,  72,  71,  74,  73,  76,  75,  78,  77,  80,  79,  82,  81,  84,  83,  86,  85,  88,  87,
        90,  89,  92,  91,  94,  93,  96,  95,  98,  97,  100, 99,  102, 101, 104, 103, 106, 105, 108, 107, 110, 112,
        109, 114, 111, 116, 113, 118, 120, 115, 122, 117, 124, 119, 121, 126, 123, 128, 125, 130, 127, 132, 129, 134,
        131, 136, 133, 138, 135, 140, 137, 142, 139, 144, 141, 146, 148, 143, 145, 150, 147, 152, 149, 154, 151, 156,
        153, 158, 155, 160, 157, 162, 159, 164, 161, 166, 163, 168, 165, 170, 172, 167, 174, 169, 176, 171, 178, 173,
        180, 175, 182, 177, 184, 179, 186, 181, 188, 183, 190, 185, 192, 187, 194, 189, 196, 191, 198, 193, 195, 200,
        197, 202, 199, 204, 201, 206, 203, 208, 205, 210, 207, 212, 209, 214, 211, 216, 213, 218, 215, 220, 217, 222,
        219, 224, 221, 226, 223, 228, 225, 230, 227, 232, 229, 234, 231, 236, 233, 238, 235, 240, 237, 239, 242, 241,
        244, 243, 246, 245, 248, 247, 250, 249, 252, 251, 254, 253, 256, 255, 258, 257, 260, 259, 262, 261, 264, 263,
        266, 265, 268, 267, 270, 269, 272, 271, 274, 273, 276, 275, 278, 277, 280, 279, 282, 281, 284, 283, 286, 285,
        288, 287, 290, 289, 292, 291, 294, 293, 296, 295, 298, 297, 300, 299, 302, 301, 304, 303, 306, 305, 308, 307,
        310, 309, 311, 312, 313, 314, 315, 316, 317, 319, 318, 321, 320, 323, 322, 325, 324, 326, 327, 328, 329, 330,
        331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 349, 348, 351, 350, 353,
        352, 355, 354, 357, 356, 359, 361, 358, 363, 360, 365, 362, 367, 364, 369, 366, 371, 368, 373, 370, 375, 377,
        372, 379, 374, 381, 376, 383};

    for (auto key : keys) {
      if (key % 2 == 0) {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set(static_cast<int32_t>(key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
      } else {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set(static_cast<int32_t>(key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Remove(index_key, transaction);
      }
    }

    //    int64_t key = 2;
    //    int64_t value = key & 0xFFFFFFFF;
    //    rid.Set(static_cast<int32_t>(key >> 32), value);
    //    index_key.SetFromInteger(key);
    //    tree.Insert(index_key, rid, transaction);
    //
    //    key = 4;
    //    value = key & 0xFFFFFFFF;
    //    rid.Set(static_cast<int32_t>(key >> 32), value);
    //    index_key.SetFromInteger(key);
    //    tree.Insert(index_key, rid, transaction);

    //    tree.Print(bpm);
    //    LOG_INFO("hahaha");
    //    DeleteHelper(&tree, for_delete, 0);
    //    int64_t size = 0;

    //    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    //      EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
    //      size++;
    //    }

    //    EXPECT_EQ(size, for_insert.size());

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void MixTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 5);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // first, populate index
    std::vector<int64_t> for_insert;
    std::vector<int64_t> for_delete;
    size_t sieve = 2;  // divide evenly
    size_t total_keys = 1000;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        for_insert.push_back(i);
      } else {
        for_delete.push_back(i);
      }
    }
    // Insert all the keys to delete
    InsertHelper1(&tree, for_delete, 0, bpm);

    tree.Draw(bpm, "my.dot");
    //    tree.Print(bpm);
    auto insert_task = [&](int tid) { InsertHelper(&tree, for_insert, tid, bpm); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, for_delete, tid, bpm); };
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(delete_task);
    tasks.emplace_back(insert_task);

    std::vector<std::thread> threads;
    size_t num_threads = 2;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }

    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }
    //    tree.Print(bpm);
    //    LOG_INFO("hahaha");
    tree.Draw(bpm, "my11.dot");
    //    DeleteHelper(&tree, for_delete, 0);
    int64_t size = 0;

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
      size++;
    }

    EXPECT_EQ(size, for_insert.size());

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
    std::cout << iter << std::endl;
  }
}

const int T2 = 200;

void MixTest2Call() {
  for (size_t iter = 0; iter < T2; iter++) {
    // create KeyComparator and index schema
    LOG_DEBUG("iteration %lu", iter);
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;

    // Add perserved_keys
    std::vector<int64_t> perserved_keys;
    std::vector<int64_t> dynamic_keys;
    size_t total_keys = 10;
    size_t sieve = 5;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        perserved_keys.push_back(i);
      } else {
        dynamic_keys.push_back(i);
      }
    }
    std::cout << "0" << std::endl;
    InsertHelper(&tree, perserved_keys, 1, bpm);
    // Check there are 1000 keys in there
    size_t size;

    auto insert_task = [&](int tid) { InsertHelper(&tree, dynamic_keys, tid, bpm); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, dynamic_keys, tid, bpm); };
    auto lookup_task = [&](int tid) { LookupHelper(&tree, perserved_keys, tid); };

    std::vector<std::thread> threads;
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    tasks.emplace_back(lookup_task);

    size_t num_threads = 6;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    size = 0;
    std::cout << "1" << std::endl;
    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      if (((*iterator).first).ToString() % sieve == 0) {
        size++;
      }
    }
    std::cout << "2" << std::endl;

    EXPECT_EQ(size, perserved_keys.size());

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}
//
// void MixTest3Call() {
//  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
//    // create KeyComparator and index schema
//    auto key_schema = ParseCreateStatement("a bigint");
//    GenericComparator<8> comparator(key_schema.get());
//
//    DiskManager *disk_manager = new DiskManager("test.db");
//    BufferPoolManager *bpm = new BufferPoolManagerInstance(10, disk_manager);
//    // create b+ tree
//    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
//
//    // create and fetch header_page
//    page_id_t page_id;
//    auto header_page = bpm->NewPage(&page_id);
//    (void)header_page;
//    // first, populate index
//    std::vector<int64_t> for_insert;
//    std::vector<int64_t> for_delete;
//    size_t total_keys = 1000;
//    for (size_t i = 1; i <= total_keys; i++) {
//      if (i > 500) {
//        for_insert.push_back(i);
//      } else {
//        for_delete.push_back(i);
//      }
//    }
//    // Insert all the keys to delete
//    InsertHelper(&tree, for_delete, 1);
//
//    auto insert_task = [&](int tid) { InsertHelper(&tree, for_insert, tid); };
//    auto delete_task = [&](int tid) { DeleteHelper(&tree, for_delete, tid); };
//
//    std::vector<std::function<void(int)>> tasks;
//    tasks.emplace_back(insert_task);
//    tasks.emplace_back(delete_task);
//    std::vector<std::thread> threads;
//    size_t num_threads = 10;
//    for (size_t i = 0; i < num_threads; i++) {
//      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
//    }
//    for (size_t i = 0; i < num_threads; i++) {
//      threads[i].join();
//    }
//
//    int64_t size = 0;
//
//    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
//      EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
//      size++;
//    }
//
//    EXPECT_EQ(size, for_insert.size());
//
//    bpm->UnpinPage(HEADER_PAGE_ID, true);
//
//    delete disk_manager;
//    delete bpm;
//    remove("test.db");
//    remove("test.log");
//  }
//}
//
// void MixTest4Call() {
//  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
//    // create KeyComparator and index schema
//    auto key_schema = ParseCreateStatement("a bigint");
//    GenericComparator<8> comparator(key_schema.get());
//
//    DiskManager *disk_manager = new DiskManager("test.db");
//    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
//    // create b+ tree
//    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
//
//    // create and fetch header_page
//    page_id_t page_id;
//    auto header_page = bpm->NewPage(&page_id);
//    (void)header_page;
//    // first, populate index
//    std::vector<int64_t> for_insert;
//    std::vector<int64_t> for_delete;
//    size_t total_keys = 1000;
//    for (size_t i = 1; i <= total_keys; i++) {
//      if (i > total_keys / 2) {
//        for_insert.push_back(i);
//      } else {
//        for_delete.push_back(i);
//      }
//    }
//    // Insert all the keys to delete
//    InsertHelper(&tree, for_delete, 1);
//    int64_t size = 0;
//
//    auto insert_task = [&](int tid) { InsertHelper(&tree, for_insert, tid); };
//    auto delete_task = [&](int tid) { DeleteHelper(&tree, for_delete, tid); };
//
//    std::vector<std::function<void(int)>> tasks;
//    tasks.emplace_back(insert_task);
//    tasks.emplace_back(delete_task);
//    std::vector<std::thread> threads;
//    size_t num_threads = 10;
//    for (size_t i = 0; i < num_threads; i++) {
//      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
//    }
//    for (size_t i = 0; i < num_threads; i++) {
//      threads[i].join();
//    }
//
//    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
//      EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
//      size++;
//    }
//
//    EXPECT_EQ(size, for_insert.size());
//
//    DeleteHelper(&tree, for_insert, 1);
//    size = 0;
//
//    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
//      EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
//      size++;
//    }
//    EXPECT_EQ(size, 0);
//
//    bpm->UnpinPage(HEADER_PAGE_ID, true);
//
//    delete disk_manager;
//    delete bpm;
//    remove("test.db");
//    remove("test.log");
//  }
//}

/*
 * Score: 5
 * Description: Concurrently insert a set of keys.
 */
// TEST(BPlusTreeTestC2Con, DISABLED_InsertTest1) {
//  TEST_TIMEOUT_BEGIN
//  InsertTest1Call();
//  remove("test.db");
//  remove("test.log");
//  TEST_TIMEOUT_FAIL_END(1000 * 600)
//}
//
///*
// * Score: 5
// * Description: Split the concurrent insert test to multiple threads
// * without overlap.
// */
// TEST(BPlusTreeTestC2Con, DISABLED_InsertTest2) {
////  TEST_TIMEOUT_BEGIN
//  InsertTest2Call();
//  remove("test.db");
//  remove("test.log");
////  TEST_TIMEOUT_FAIL_END(1000 * 600)
//}
//
///*
// * Score: 5
// * Description: Concurrently delete a set of keys.
// */
// TEST(BPlusTreeTestC2Con, DISABLED_DeleteTest1) {
//  TEST_TIMEOUT_BEGIN
//  DeleteTest1Call();
//  remove("test.db");
//  remove("test.log");
//  TEST_TIMEOUT_FAIL_END(1000 * 600)
//}
//
///*
// * Score: 5
// * Description: Split the concurrent delete task to multiple threads
// * without overlap.
// */
// TEST(BPlusTreeTestC2Con,DISABLED_DeleteTest2) {
//  TEST_TIMEOUT_BEGIN
//  DeleteTest2Call();
//  remove("test.db");
//  remove("test.log");
//  TEST_TIMEOUT_FAIL_END(1000 * 600)
//}

/*
 * Score: 5
 * Description: First insert a set of keys.
 * Then concurrently delete those already inserted keys and
 * insert different set of keys. Check if all old keys are
 * deleted and new keys are added correctly.
 */
TEST(BPlusTreeTestC2Con, MixTest1) {
  TEST_TIMEOUT_BEGIN
  MixTest1Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: Insert a set of keys. Concurrently insert and delete
 * a differnt set of keys.
 * At the same time, concurrently get the previously inserted keys.
 * Check all the keys get are the same set of keys as previously
 * inserted.
 */
TEST(BPlusTreeTestC2Con, MixTest2) {
  TEST_TIMEOUT_BEGIN
  MixTest2Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

///*
// * Score: 5
// * Description: First insert a set of keys.
// * Then concurrently delete those already inserted keys and
// * insert different set of keys. Check if all old keys are
// * deleted and new keys are added correctly.
// */
// TEST(BPlusTreeTestC2Con,  DISABLED_MixTest3) {
//  TEST_TIMEOUT_BEGIN
//  MixTest3Call();
//  remove("test.db");
//  remove("test.log");
//  TEST_TIMEOUT_FAIL_END(1000 * 600)
//}
//
// TEST(BPlusTreeTestC2Con,  DISABLED_MixTest4) {
//  TEST_TIMEOUT_BEGIN
//  MixTest4Call();
//  remove("test.db");
//  remove("test.log");
//  TEST_TIMEOUT_FAIL_END(1000 * 600)
//}

}  // namespace bustub
