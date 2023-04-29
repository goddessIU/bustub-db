//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  if (index >= GetSize()) {
    return {};
  }

  return array_[index].first;
}

/*
 * you must ensure the leaf is not full
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> void {
  if (GetSize() == 0) {
    array_[0] = std::make_pair(key, value);
    IncreaseSize(1);
    return;
  }

  int sz = GetSize();
  int left = 0;
  int right = sz - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    int res = comparator(key, array_[mid].first);
    if (res < 0) {
      right = mid - 1;
    } else if (res == 0) {
      return;
    } else {
      left = mid + 1;
    }
  }

  int target = left;
  if (target < 0) {
    return;
  }
  for (int i = GetSize(); i > target; i--) {
    array_[i] = array_[i - 1];
  }
  array_[target] = std::make_pair(key, value);

  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::CopyArray(MappingType *new_array, int sz) -> void {
  std::copy(array_, array_ + sz, new_array);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFromArray(MappingType *array, int sz) -> void {
  std::copy(array, array + sz, array_);
  SetSize(sz);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindIndex(const KeyType &key, const KeyComparator &comparator) -> int {
  int sz = GetSize();

  int left = 0;
  int right = sz - 1;
  int mid;
  while (left <= right) {
    mid = (left + right) / 2;
    int res = comparator(key, array_[mid].first);
    if (res < 0) {
      right = mid - 1;
    } else if (res == 0) {
      return mid;
    } else {
      left = mid + 1;
    }
  }

  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Find(const KeyType &key, std::vector<ValueType> *result,
                                      const KeyComparator &comparator) -> bool {
  int idx = FindIndex(key, comparator);

  if (idx == -1) {
    return false;
  }

  result->push_back(array_[idx].second);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Erase() -> void { SetSize(0); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &comparator) -> bool {
  int target = FindIndex(key, comparator);
  if (target == -1) {
    return false;
  }

  int sz = GetSize();
  for (int i = target; i < sz - 1; i++) {
    array_[i] = array_[i + 1];
  }
  DecreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetArray() -> MappingType * { return array_; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteLast() -> void { DecreaseSize(1); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteFirst() -> void {
  int sz = GetSize();
  for (int i = 0; i < sz - 1; i++) {
    array_[i] = array_[i + 1];
  }
  DecreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value) -> void {
  int sz = GetSize();
  for (int i = sz; i >= 1; i--) {
    array_[i] = array_[i - 1];
  }
  array_[0] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertLast(const KeyType &key, const ValueType &value) -> void {
  int sz = GetSize();
  array_[sz] = std::make_pair(key, value);
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
