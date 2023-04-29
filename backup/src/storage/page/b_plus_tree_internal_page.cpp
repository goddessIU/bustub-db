//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  // maybe is not 0?maybe 1?
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  if (index >= GetSize()) {
    return {};
  }

  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteLast() -> void { DecreaseSize(1); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteFirst() -> void {
  int sz = GetSize();
  for (int i = 0; i < sz - 1; i++) {
    array_[i] = array_[i + 1];
  }
  DecreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirst(const KeyType &key, page_id_t page_id) -> void {
  int sz = GetSize();
  for (int i = sz; i > 0; i--) {
    array_[i] = array_[i - 1];
  }
  array_[0].second = page_id;
  array_[1].first = key;
  IncreaseSize(1);
};

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertLast(const KeyType &key, page_id_t page_id) -> void {
  int sz = GetSize();
  array_[sz] = std::make_pair(key, page_id);
  IncreaseSize(1);
};

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index >= GetSize()) {
    return {};
  }

  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int sz = GetSize();
  int left = 1;
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
  return left - 1;
}

/*
 * Helper method to get the pointer for insert
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindNextPid(const KeyType &key, const KeyComparator &comparator) const
    -> ValueType {
  int sz = GetSize();
  if (sz <= 0) {
    std::cout << "big wrong !" << std::endl;
  }

  if (sz == 2) {
    if (comparator(key, array_[1].first) < 0) {
      return array_[0].second;
    }
    return array_[1].second;
  }

  int idx = GetIndex(key, comparator);
  return array_[idx].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyArray(MappingType *new_array, int sz) -> void {
  // maybe is wrong
  std::copy(array_, array_ + sz, new_array);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFromArray(MappingType *array, int sz) -> void {
  std::copy(array, array + sz, array_);
  SetSize(sz);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetChildrenParentId(BufferPoolManager *bpm) -> void {
  int sz = GetSize();
  for (int i = 0; i < sz; i++) {
    Page *p = bpm->FetchPage(array_[i].second);
    auto *bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
    int id = GetPageId();
    bp->SetParentPageId(id);
    bpm->UnpinPage(p->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindSibling(BPlusTreePage *child, std::vector<page_id_t> &vec,
                                                 std::vector<KeyType> &k_vec) -> bool {
  page_id_t cid = child->GetPageId();
  int sz = GetSize();
  if (sz <= 1) {
    // must at least two child
    std::cout << "oh no!" << std::endl;
    return false;
  }

  if (array_[0].second == cid) {
    vec[1] = array_[1].second;
    k_vec[1] = array_[1].first;
    return true;
  }

  if (array_[sz - 1].second == cid) {
    vec[0] = array_[sz - 2].second;
    k_vec[0] = array_[sz - 1].first;
    return true;
  }

  for (int i = 1; i < sz - 1; i++) {
    if (array_[i].second == cid) {
      if (i - 1 >= 0) {
        vec[0] = array_[i - 1].second;
        k_vec[0] = array_[i].first;
      }

      if (i + 1 < sz) {
        vec[1] = array_[i + 1].second;
        k_vec[1] = array_[i + 1].first;
      }

      break;
    }
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetArray() -> MappingType * { return array_; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> void {
  int sz = GetSize();
  if (sz == 0) {
    array_[0] = std::make_pair(key, value);
    IncreaseSize(1);
    return;
  }

  if (sz == 1) {
    array_[1] = std::make_pair(key, value);
    IncreaseSize(1);
    return;
  }

  int idx = GetIndex(key, comparator);
  int target = idx + 1;
  for (int i = GetSize(); i > target; i--) {
    array_[i] = array_[i - 1];
  }
  array_[target] = std::make_pair(key, value);

  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &comparator) -> bool {
  int sz = GetSize();
  int idx = GetIndex(key, comparator);
  if (comparator(key, array_[idx].first) != 0) {
    return false;
  }

  int target = idx;

  for (int i = target; i < sz - 1; i++) {
    array_[i] = array_[i + 1];
  }
  DecreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ReplaceKey(const KeyType &key_old, const KeyType &key_new,
                                                const KeyComparator &comparator) -> void {
  int idx = GetIndex(key_old, comparator);
  if (comparator(array_[idx].first, key_old) == 0) {
    array_[idx].first = key_new;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetChildParent(int index, BufferPoolManager *bpm) -> void {
  Page *p = bpm->FetchPage(array_[index].second);
  auto *bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  bp->SetParentPageId(GetPageId());
  bpm->UnpinPage(p->GetPageId(), true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
