#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  // maybe not right
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }

  Page *c_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(c_page->GetData());
  while (!node->IsLeafPage()) {
    auto *i_node = reinterpret_cast<InternalPage *>(node);
    page_id_t pid = i_node->FindNextPid(key, comparator_);
    buffer_pool_manager_->UnpinPage(c_page->GetPageId(), false);
    c_page = buffer_pool_manager_->FetchPage(pid);
    node = reinterpret_cast<BPlusTreePage *>(c_page->GetData());
  }
  auto *l_node = reinterpret_cast<LeafPage *>(node);
  bool res = l_node->Find(key, result, comparator_);
  buffer_pool_manager_->UnpinPage(c_page->GetPageId(), false);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value in leaf, it is a helper function for Insert
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInLeaf(LeafPage *l_node, const KeyType &key, const ValueType &value) -> void {
  l_node->Insert(key, value, comparator_);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *l_node, const KeyType &key, BPlusTreePage *r_node, page_id_t l_pid,
                                    page_id_t r_pid) -> void {
  // it is root page id, but can I add a new field to enum
  if (l_node->GetPageId() == root_page_id_) {
    page_id_t pid;
    Page *page_r = buffer_pool_manager_->NewPage(&pid);
    if (page_r == nullptr) {
      // cannot nullptr, you can't into this scope
      return;
    }
    auto *node_r = reinterpret_cast<InternalPage *>(page_r->GetData());
    node_r->Init(pid, INVALID_PAGE_ID, internal_max_size_);
    node_r->Insert({}, l_pid, comparator_);
    node_r->Insert(key, r_pid, comparator_);

    UpdateRootPageIdWrapper(pid);

    l_node->SetParentPageId(pid);
    r_node->SetParentPageId(pid);
    buffer_pool_manager_->UnpinPage(pid, true);
    return;
  }

  page_id_t parent_id = l_node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  if (parent_page == nullptr) {
    // cannot nullptr, you can't into this scope
    return;
  }

  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    parent_node->Insert(key, r_pid, comparator_);
    buffer_pool_manager_->UnpinPage(parent_id, true);
  } else {
    // I don't know is that right, how to deal with this?
    std::pair<KeyType, page_id_t> temp_array[internal_max_size_ + 1];
    parent_node->CopyArray(temp_array, parent_node->GetMaxSize());

    InsertINTempInternal(temp_array, key, r_pid);

    parent_node->SetSize(0);

    // create a new node
    page_id_t parent_id_new;
    Page *parent_page_new = buffer_pool_manager_->NewPage(&parent_id_new);
    if (parent_page_new == nullptr) {
      // it is wrong, you can't into there
      return;
    }

    auto *parent_node_new = reinterpret_cast<InternalPage *>(parent_page_new->GetData());
    parent_node_new->Init(parent_id_new, parent_node->GetParentPageId(), internal_max_size_);

    // copy k&v1 -- k&v n/2 to L, copy the else to L1
    int sz = (internal_max_size_ + 1) % 2 + (internal_max_size_ + 1) / 2;
    parent_node->CopyFromArray(temp_array, sz);
    parent_node_new->CopyFromArray(temp_array + sz, internal_max_size_ + 1 - sz);
    parent_node->SetChildrenParentId(buffer_pool_manager_);
    parent_node_new->SetChildrenParentId(buffer_pool_manager_);

    KeyType k2 = temp_array[sz].first;
    InsertInParent(parent_node, k2, parent_node_new, parent_node->GetPageId(), parent_page_new->GetPageId());

    buffer_pool_manager_->UnpinPage(parent_id, true);
    buffer_pool_manager_->UnpinPage(parent_id_new, true);
  }
}

/*
 * Insert key & value in leaf, it is a helper function for Insert
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertINTemp(MappingType *temp_array, const KeyType &key, const ValueType &value) -> void {
  int target = -1;
  for (int i = 0; i < leaf_max_size_; i++) {
    if (comparator_(key, temp_array[i].first) < 0) {
      target = i;
      break;
    }
  }

  if (target == -1) {
    // insert at the end
    temp_array[leaf_max_size_] = std::make_pair(key, value);
  } else {
    // insert at target
    // careful, this is unsigned long
    for (int i = leaf_max_size_; i > target; i--) {
      temp_array[i] = temp_array[i - 1];
    }
    temp_array[target] = std::make_pair(key, value);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertINTempInternal(std::pair<KeyType, page_id_t> *temp_array, const KeyType &key,
                                          const page_id_t &value) -> void {
  int target = 0;
  for (int i = 1; i < internal_max_size_; i++) {
    if (comparator_(key, temp_array[i].first) < 0) {
      target = i;
      break;
    }
  }

  if (target == 0) {
    // insert at the end
    temp_array[internal_max_size_] = std::make_pair(key, value);
  } else {
    // insert at target
    // careful, this is unsigned long
    for (int i = internal_max_size_; i > target; i--) {
      temp_array[i] = temp_array[i - 1];
    }
    temp_array[target] = std::make_pair(key, value);
  }
}

/*
 * Insert key & value in leaf, it is a helper function for Insert
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CopyArray(LeafPage *l_node, MappingType *new_array, int sz) -> void {
  l_node->CopyArray(new_array, sz);
}

/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  Page *node;
  LeafPage *l_node;
  std::vector<ValueType> vec{};

  if (GetValue(key, &vec)) {
    return false;
  }

  if (root_page_id_ == INVALID_PAGE_ID) {
    node = buffer_pool_manager_->NewPage(&root_page_id_);
    UpdateRootPageIdWrapper(root_page_id_);
    if (node == nullptr) {
      return false;
    }
    // cast page->data to leaf page, and init it
    l_node = reinterpret_cast<LeafPage *>(node->GetData());
    l_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  } else {
    // find the leaf node L, do it later
    node = buffer_pool_manager_->FetchPage(root_page_id_);
    if (node == nullptr) {
      return false;
    }

    auto *b_node = reinterpret_cast<BPlusTreePage *>(node->GetData());
    if (b_node->IsLeafPage()) {
      l_node = reinterpret_cast<LeafPage *>(node->GetData());
    } else {
      auto *i_node = reinterpret_cast<InternalPage *>(node->GetData());
      b_node = reinterpret_cast<BPlusTreePage *>(i_node);
      while (!b_node->IsLeafPage()) {
        page_id_t pid = i_node->FindNextPid(key, comparator_);
        // unpin the page
        buffer_pool_manager_->UnpinPage(node->GetPageId(), false);

        node = buffer_pool_manager_->FetchPage(pid);
        i_node = reinterpret_cast<InternalPage *>(node->GetData());
        b_node = reinterpret_cast<BPlusTreePage *>(i_node);
      }
      l_node = reinterpret_cast<LeafPage *>(node->GetData());
    }
  }

  // l_node's max size is the num of key&value, and the extra pointer is next_page_id field
  if (l_node->GetSize() < l_node->GetMaxSize()) {
    // if L has less than n-1 keys values
    InsertInLeaf(l_node, key, value);
    // unpin the apge
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  } else {
    // if the leaf node is full
    // creat a new node L1 first
    page_id_t l1_pid;
    Page *page_1 = buffer_pool_manager_->NewPage(&l1_pid);
    if (page_1 == nullptr) {
      buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
      return false;
    }

    auto *l_page_1 = reinterpret_cast<LeafPage *>(page_1->GetData());
    l_page_1->Init(l1_pid, l_node->GetParentPageId(), leaf_max_size_);

    MappingType temp_array[leaf_max_size_ + 1];
    // copy all key&value from leaf node and new key&val to temp_array
    CopyArray(l_node, temp_array, leaf_max_size_);
    InsertINTemp(temp_array, key, value);

    // L's next pid = L1
    l_page_1->SetNextPageId(l_node->GetNextPageId());
    l_node->SetNextPageId(l1_pid);

    // erase l_node
    l_node->Erase();

    // copy k&v1 -- k&v n/2 to L, copy the else to L1
    int sz = (leaf_max_size_ + 1) % 2 + (leaf_max_size_ + 1) / 2;
    l_node->CopyFromArray(temp_array, sz);
    l_page_1->CopyFromArray(temp_array + sz, leaf_max_size_ + 1 - sz);
    KeyType k1 = temp_array[sz].first;

    // bottom up, spread to the parent
    InsertInParent(l_node, k1, l_page_1, node->GetPageId(), page_1->GetPageId());

    // unpin the page
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(page_1->GetPageId(), true);
  }

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * append n1 to n2
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CoalesceLeaf(BPlusTreePage *n1, BPlusTreePage *n2) -> void {
  // append all pairs in N to N1
  // change the size
  // change the next page pid
  auto *l_n1 = reinterpret_cast<LeafPage *>(n1);
  auto *l_n2 = reinterpret_cast<LeafPage *>(n2);
  std::copy(l_n1->GetArray(), l_n1->GetArray() + l_n1->GetSize(), l_n2->GetArray() + l_n2->GetSize());
  l_n2->SetSize(l_n2->GetSize() + l_n1->GetSize());
  l_n2->SetNextPageId(l_n1->GetNextPageId());
}

/*
 * append n1 to n2
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CoalesceInternal(BPlusTreePage *n1, BPlusTreePage *n2, const KeyType &key) -> void {
  // append K1 and n1 array_[0] ptr to n2
  // append all n1 to n2
  // set all children point to n2
  auto *l_n1 = reinterpret_cast<InternalPage *>(n1);
  auto *l_n2 = reinterpret_cast<InternalPage *>(n2);
  std::pair<KeyType, page_id_t> *arr_1 = l_n1->GetArray();
  std::pair<KeyType, page_id_t> *arr_2 = l_n2->GetArray();
  arr_2[l_n2->GetSize()] = std::make_pair(key, arr_1[0].second);
  l_n2->IncreaseSize(1);
  std::copy(arr_1 + 1, arr_1 + l_n1->GetSize(), arr_2 + l_n2->GetSize());
  l_n2->SetSize(l_n2->GetSize() + l_n1->GetSize() - 1);
  l_n2->SetChildrenParentId(buffer_pool_manager_);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(BPlusTreePage *node, const KeyType &key) {
  if (node->IsLeafPage()) {
    auto l_node = reinterpret_cast<LeafPage *>(node);
    l_node->Delete(key, comparator_);
  } else {
    // internal delete, do it later
    auto i_node = reinterpret_cast<InternalPage *>(node);
    i_node->Delete(key, comparator_);
  }

  // should think size == 0?

  if (node->GetPageId() == root_page_id_ && node->GetSize() == 0) {
    UpdateRootPageIdWrapper(INVALID_PAGE_ID);
    buffer_pool_manager_->DeletePage(node->GetPageId());
  } else if (node->GetPageId() == root_page_id_ && node->GetSize() == 1) {
    // if it's not leaf, then make the child as the new root
    if (!node->IsLeafPage()) {
      auto i_node = reinterpret_cast<InternalPage *>(node);
      page_id_t pp_id = i_node->ValueAt(0);
      Page *pp_page = buffer_pool_manager_->FetchPage(pp_id);
      reinterpret_cast<InternalPage *>(pp_page->GetData())->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(pp_id, true);
      UpdateRootPageIdWrapper(pp_id);
      buffer_pool_manager_->DeletePage(node->GetPageId());
    } else {
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    }
  } else if ((node->GetSize() < node->GetMinSize())) {
    // find the parent_node
    page_id_t parent_id = node->GetParentPageId();
    Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
    auto parent_node = reinterpret_cast<BPlusTreePage *>(parent_page->GetData());

    // find the sibling
    page_id_t sibling_id = INVALID_PAGE_ID;
    int idx = -1;
    std::vector<page_id_t> vec{INVALID_PAGE_ID, INVALID_PAGE_ID};
    std::vector<KeyType> k_vec{{}, {}};
    if (!reinterpret_cast<InternalPage *>(parent_node)->FindSibling(node, vec, k_vec)) {
      // must have a sibling
      return;
    }

    Page *sibling_page;
    BPlusTreePage *sibling_node;
    int csz = node->GetSize();
    int msz = node->GetMaxSize();

    for (int i = 0; i < 2; i++) {
      page_id_t pid = vec[i];
      if (pid == INVALID_PAGE_ID) {
        continue;
      }

      sibling_page = buffer_pool_manager_->FetchPage(pid);
      sibling_node = reinterpret_cast<BPlusTreePage *>(sibling_page->GetData());

      if (sibling_node->GetSize() + csz <= msz) {
        sibling_id = pid;
        idx = i;
        // for passing clang-tidy
        buffer_pool_manager_->UnpinPage(pid, false);
        break;
      }

      buffer_pool_manager_->UnpinPage(pid, false);
    }

    if (sibling_id != INVALID_PAGE_ID) {
      // for passing clang-tidy
      sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
      sibling_node = reinterpret_cast<BPlusTreePage *>(sibling_page->GetData());

      if (idx == 1) {
        BPlusTreePage *tmp = sibling_node;
        sibling_node = node;
        node = tmp;
      }

      if (node->IsLeafPage()) {
        CoalesceLeaf(node, sibling_node);
      } else {
        CoalesceInternal(node, sibling_node, k_vec[idx]);
      }

      // delete bottom up
      DeleteEntry(parent_node, k_vec[idx]);

      // unpin or delete
      buffer_pool_manager_->UnpinPage(parent_id, true);
      buffer_pool_manager_->UnpinPage(sibling_id, true);
      buffer_pool_manager_->DeletePage(node->GetPageId());
    } else {
      if (vec[0] != INVALID_PAGE_ID) {
        idx = 0;
      } else if (vec[1] != INVALID_PAGE_ID) {
        idx = 1;
      }
      // for passing clang-tidy
      sibling_id = vec[idx];
      sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
      sibling_node = reinterpret_cast<BPlusTreePage *>(sibling_page->GetData());

      // change the sibling node
      // if (sibling_node->GetPageId() != vec[idx]) {
      //  buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), false);
      //  sibling_id = vec[idx];
      //  sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
      //  sibling_node = reinterpret_cast<BPlusTreePage *>(sibling_page->GetData());
      //  buffer_pool_manager_->UnpinPage(sibling_id, false);
      // }

      if (idx == 0) {
        if (node->IsLeafPage()) {
          auto sibling_leaf_node = reinterpret_cast<LeafPage *>(sibling_node);
          KeyType k1 = sibling_leaf_node->KeyAt(sibling_leaf_node->GetSize() - 1);
          ValueType v1 = sibling_leaf_node->ValueAt(sibling_leaf_node->GetSize() - 1);
          sibling_leaf_node->DeleteLast();
          reinterpret_cast<LeafPage *>(node)->InsertFirst(k1, v1);
          reinterpret_cast<InternalPage *>(parent_node)->ReplaceKey(k_vec[idx], k1, comparator_);

          buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(parent_id, true);
        } else {
          auto sibling_internal_node = reinterpret_cast<InternalPage *>(sibling_node);
          KeyType k1 = sibling_internal_node->KeyAt(sibling_internal_node->GetSize() - 1);
          page_id_t v1 = sibling_internal_node->ValueAt(sibling_internal_node->GetSize() - 1);
          sibling_internal_node->DeleteLast();
          reinterpret_cast<InternalPage *>(node)->InsertFirst(k_vec[idx], v1);
          reinterpret_cast<InternalPage *>(node)->SetChildParent(0, buffer_pool_manager_);
          reinterpret_cast<InternalPage *>(parent_node)->ReplaceKey(k_vec[idx], k1, comparator_);

          buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(parent_id, true);
        }
      } else {
        // symmetric of above
        if (node->IsLeafPage()) {
          auto sibling_leaf_node = reinterpret_cast<LeafPage *>(sibling_node);
          KeyType k1 = sibling_leaf_node->KeyAt(0);
          ValueType v1 = sibling_leaf_node->ValueAt(0);
          sibling_leaf_node->DeleteFirst();
          reinterpret_cast<LeafPage *>(node)->InsertLast(k1, v1);
          reinterpret_cast<InternalPage *>(parent_node)
              ->ReplaceKey(k_vec[idx], sibling_leaf_node->KeyAt(0), comparator_);

          buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(parent_id, true);
        } else {
          auto sibling_internal_node = reinterpret_cast<InternalPage *>(sibling_node);
          KeyType k1 = sibling_internal_node->KeyAt(1);
          page_id_t v1 = sibling_internal_node->ValueAt(0);
          sibling_internal_node->DeleteFirst();
          reinterpret_cast<InternalPage *>(node)->InsertLast(k_vec[idx], v1);
          reinterpret_cast<InternalPage *>(node)->SetChildParent(node->GetSize() - 1, buffer_pool_manager_);
          reinterpret_cast<InternalPage *>(parent_node)->ReplaceKey(k_vec[idx], k1, comparator_);

          buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(parent_id, true);
        }
      }
    }
  } else {
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  }
}

/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }

  Page *c_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(c_page->GetData());
  while (!node->IsLeafPage()) {
    auto *i_node = reinterpret_cast<InternalPage *>(node);
    page_id_t pid = i_node->FindNextPid(key, comparator_);
    buffer_pool_manager_->UnpinPage(c_page->GetPageId(), false);
    c_page = buffer_pool_manager_->FetchPage(pid);
    node = reinterpret_cast<BPlusTreePage *>(c_page->GetData());
  }

  auto *l_node = reinterpret_cast<LeafPage *>(node);
  this->DeleteEntry(l_node, key);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageIdWrapper(page_id_t root_page_id, int insert_record) {
  root_page_id_ = root_page_id;
  UpdateRootPageId(insert_record);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    // std::cout << "key is " << key << std::endl;
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    // input >> key;
    std::cout << "key is " << key << std::endl;
    KeyType index_key;
    index_key.SetFromInteger(key);
    BPlusTree::Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
