/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/exception.h"
#include "storage/index/index_iterator.h"

#define THROW_OOM(msg) throw Exception(ExceptionType::OUT_OF_MEMORY, (msg));

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page, int index, BufferPoolManager *bpm, bool is_end)
    : leaf_page_{leaf_page}, index_{index}, bpm_{bpm}, is_end_{is_end} {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (bpm_ != nullptr) {
    assert(leaf_page_ != nullptr);
    //! this assumes the lifetime of the bpm_ is longer than the index iterator.
    bpm_->UnpinPage(leaf_page_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() const { return is_end_; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() const { return leaf_page_->GetItem(index_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (isEnd()) {
    return *this;
  }

  if (index_ < leaf_page_->GetSize() - 1) {
    ++index_;
  } else {
    if (leaf_page_->GetNextPageId() == INVALID_PAGE_ID) {
      is_end_ = true;
      return *this;
    }

    Page *page = bpm_->FetchPage(leaf_page_->GetNextPageId());
    if (page == nullptr) {
      THROW_OOM("FetchPage fail");
    }
    bpm_->UnpinPage(leaf_page_->GetPageId(), false);
    leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());

    index_ = 0;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &other) const {
  /// TODO(bayes): To simplify the logic.
  bool res;
  if (this->isEnd()) {
    res = other.isEnd();
  } else if (other.isEnd()) {
    res = this->isEnd();
  } else {
    res = (leaf_page_ == other.leaf_page_ && index_ == other.index_);
  }
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &other) const { return !operator==(other); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
