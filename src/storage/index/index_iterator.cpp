/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int curr_sz, BufferPoolManager *bpm)
    : page_id_(page_id), curr_sz_(curr_sz), bpm_(bpm) {
  if (page_id != INVALID_PAGE_ID) {
    Page *p = bpm->FetchPage(page_id);
    page_ = p;
    page_->RLatch();
    node_ = reinterpret_cast<LeafPage *>(p);
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return (node_->GetArray())[curr_sz_]; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (page_id_ == INVALID_PAGE_ID) {
    return *this;
  }

  if (curr_sz_ < (node_->GetSize() - 1)) {
    curr_sz_++;
  } else {
    curr_sz_ = 0;
    page_id_ = node_->GetNextPageId();
    if (page_id_ != INVALID_PAGE_ID) {
      page_->RUnlatch();
      bpm_->UnpinPage(node_->GetPageId(), false);
      page_ = bpm_->FetchPage(page_id_);
      page_->RLatch();
      node_ = reinterpret_cast<LeafPage *>(page_);
    } else {
      page_->RUnlatch();
      bpm_->UnpinPage(node_->GetPageId(), false);
      node_ = nullptr;
    }
  }

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return curr_sz_ == itr.curr_sz_ && page_id_ == itr.page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !operator==(itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
