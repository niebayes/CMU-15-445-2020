//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  explicit IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page, int index, BufferPoolManager *bpm, bool is_end = false);
  ~IndexIterator();

  bool isEnd() const;

  const MappingType &operator*() const;

  IndexIterator &operator++();

  bool operator==(const IndexIterator &other) const;

  bool operator!=(const IndexIterator &other) const;

 private:
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_{nullptr};
  int index_{0};
  BufferPoolManager *bpm_{nullptr};
  bool is_end_;
};

}  // namespace bustub
