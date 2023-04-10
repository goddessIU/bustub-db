//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_delete_test.cpp
//
// Identification: test/storage/b_plus_tree_delete_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

TEST(BPlusTreeTests, SimpleTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {481,297,555,32,753,851,296,963,906,698,945,248,876,438,947,913,206,251,615,142,179,43,670,726,388,639,812,778,262,973,325,811,71,511,870,174,50,744,689,654,489,937,514,55,3,602,151,953,431,107,414,915,61,668,354,542,930,506,831,383,123,36,197,496,273,900,545,838,175,361,539,727,222,422,957,427,798,756,562,338,931,936,271,481,30,908,266,377,113,77,101,152,772,900,366,374,665,221,504,568,753,963,109,692,455,48,752,1000,829,31,223,471,785,109,803,54,281,696,532,961,356,550,11,259,912,177,511,919,949,562,522,556,905,322,874,480,167,848,63,712,994,996,366,233,452,565,930,451,20,172,255,903,125,819,708,814,398,752,171,701,35,140,110,328,478,98,957,418,771,518,698,988,913,402,510,738,663,951,134,793,239,423,246,496,179,736,269,319,414,681,240,798,253,715,41,644,117,421,346,39,981,62,951,191,662,460,682,907,58,124,813,671,386,166,835,401,9,2,759,817,637,161,565,869,156,585,8,347,863,996,283,623,321,828,136,428,733,890,172,238,872,678,148,330,933,168,729,812,149,688,779,266,582,523,752,409,576,620,329,98,189,39,378,303,567,547,342,425,891,651,789,597,570,61,841,228,242,325,649,628,88,575,910,965,456,459,328,110,862,297,71,647,48,744,755,101,103,391,632,390,512,426,381,661,492,504,23,688,536,248,130,152,678,72,336,232,534,109,251,808,330,41,803,37,596,956,320,888,998,726,704,920,872,375,248,236,721,332,32,424,669,306,166,117,951,363,218,101,567,236,477,509,52,818,521,559,343,162,354,924,224,408,890,337,68,386,961,899,693};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  //  std::vector<RID> rids;
  //  for (auto key : keys) {
  //    rids.clear();
  //    index_key.SetFromInteger(key);
  //    tree.GetValue(index_key, &rids);
  //    EXPECT_EQ(rids.size(), 1);
  //
  //    int64_t value = key & 0xFFFFFFFF;
  //    EXPECT_EQ(rids[0].GetSlotNum(), value);
  //  }

  std::vector<int64_t> remove_keys = {481,297,555,32,753,851,296,963,906,698,945,248,876,438,947,913,206,251,615,142,179,43,670,726,388,639,812,778,262,973,325,811,71,511,870,174,50,744,689,654,489,937,514,55,3,602,151,953,431,107,414,915,61,668,354,542,930,506,831,383,123,36,197,496,273,900,545,838,175,361,539,727,222,422,957,427,798,756,562,338,931,936};

  for (int i = 0; i < static_cast<int>(remove_keys.size()); i++) {
    auto key = remove_keys[i];
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  //    int64_t size = 0;
  //    bool is_present;
  //
  //    std::vector<int64_t> tkeys = {6, 7, 8, 9};
  //    for (auto key : tkeys) {
  //      rids.clear();
  //      index_key.SetFromInteger(key);
  //      tree.GetValue(index_key, &rids);
  //      EXPECT_EQ(rids.size(), 1);
  //     EXPECT_EQ(rids[0].GetPageId(), 0);
  //      EXPECT_EQ(rids[0].GetSlotNum(), key);
  //      size = size + 1;
  //      is_present = tree.GetValue(index_key, &rids);

  //      if (!is_present) {
  //        EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
  //      } else {
  //        EXPECT_EQ(rids.size(), 1);
  //        EXPECT_EQ(rids[0].GetPageId(), 0);
  //        EXPECT_EQ(rids[0].GetSlotNum(), key);
  //        size = size + 1;
  //      }
  //    }

  //    EXPECT_EQ(size, 4);
  //    std::cout << "aaa\n";

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }
  //
  //  int64_t size = 0;
  //  bool is_present;
  //
  //  for (auto key : keys) {
  //    rids.clear();
  //    index_key.SetFromInteger(key);
  //    is_present = tree.GetValue(index_key, &rids);
  //
  //    if (!is_present) {
  //      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
  //    } else {
  //      EXPECT_EQ(rids.size(), 1);
  //      EXPECT_EQ(rids[0].GetPageId(), 0);
  //      EXPECT_EQ(rids[0].GetSlotNum(), key);
  //      size = size + 1;
  //    }
  //  }
  //
  //  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, DISABLED_DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
}  // namespace bustub
