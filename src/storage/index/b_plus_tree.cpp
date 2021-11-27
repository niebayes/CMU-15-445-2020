//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

#define THROW_OOM(msg) throw Exception(ExceptionType::OUT_OF_MEMORY, (msg));

#define NODE_IS_LEFT 0
#define NODE_IS_RIGHT 1

namespace bustub {

void LatchPage(Page *page, const OP_TYPE &op_type) {
  if (op_type == OP_TYPE::READ) {
    page->RLatch();
  } else {
    page->WLatch();
  }
}

void UnlatchPage(Page *page, const OP_TYPE &op_type) {
  if (op_type == OP_TYPE::READ) {
    page->RUnlatch();
  } else {
    page->WUnlatch();
  }
}

bool IsNodeSafe(BPlusTreePage *node, const OP_TYPE &op_type) {
  bool is_safe{true};
  if (op_type == OP_TYPE::INSERT) {
    is_safe = (node->GetSize() < node->GetMaxSize() - 1);
  } else if (op_type == OP_TYPE::DELETE) {
    is_safe = (node->GetSize() > node->GetMinSize());
  }
  return is_safe;
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

INDEX_TEMPLATE_ARGUMENTS
thread_local uint32_t BPLUSTREE_TYPE::root_latch_cnt_ = 0;

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  LatchRoot(OP_TYPE::READ);
  if (IsEmpty()) {
    TryUnlatchRoot(OP_TYPE::READ);
    return false;
  }
  TryUnlatchRoot(OP_TYPE::READ);

  Page *page = FindLeafPageCrabbing(key, transaction, OP_TYPE::READ);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  ValueType value;
  const bool key_exist = leaf_page->Lookup(key, &value, comparator_);

  TryUnlatchRoot(OP_TYPE::READ);
  UnlatchPage(page, OP_TYPE::READ);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

  if (key_exist) {
    result->clear();
    result->emplace_back(std::move(value));
  }

  return key_exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  LatchRoot(OP_TYPE::INSERT);
  if (IsEmpty()) {
    StartNewTree(key, value);

    TryUnlatchRoot(OP_TYPE::INSERT);
    return true;
  }
  TryUnlatchRoot(OP_TYPE::INSERT);

  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id{INVALID_PAGE_ID};
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    THROW_OOM("NewPage fail");
  }

  root_page_id_ = page_id;
  UpdateRootPageId(true);

  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  leaf_page->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *page = FindLeafPageCrabbing(key, transaction, OP_TYPE::INSERT);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  const int old_size = leaf_page->GetSize();
  const int size = leaf_page->Insert(key, value, comparator_);
  const bool is_dirty = (size > old_size);

  if (size == leaf_page->GetMaxSize()) {
    LeafPage *split_leaf_page = Split(leaf_page);

    const KeyType &mid_key = split_leaf_page->KeyAt(0);
    InsertIntoParent(leaf_page, mid_key, split_leaf_page);
  }
  // else {
  //   buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), is_dirty);
  // }

  TryUnlatchRoot(OP_TYPE::INSERT);
  ReleaseAllPages(transaction);

  return is_dirty;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id{INVALID_PAGE_ID};
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    THROW_OOM("NewPage fail");
  }

  N *split_node = reinterpret_cast<N *>(page->GetData());
  split_node->Init(page_id, node->GetParentPageId(), (node->IsLeafPage() ? leaf_max_size_ : internal_max_size_));

  if (node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *split_leaf_node = reinterpret_cast<LeafPage *>(split_node);

    leaf_node->MoveHalfTo(split_leaf_node);

    split_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->SetNextPageId(page_id);

  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *split_internal_node = reinterpret_cast<InternalPage *>(split_node);

    internal_node->MoveHalfTo(split_internal_node, buffer_pool_manager_);
  }

  return split_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    page_id_t page_id{INVALID_PAGE_ID};
    Page *page = buffer_pool_manager_->NewPage(&page_id);
    if (page == nullptr) {
      THROW_OOM("NewPage fail");
    }

    root_page_id_ = page_id;
    UpdateRootPageId(false);

    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(page->GetData());
    new_root_node->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(page_id);
    new_node->SetParentPageId(page_id);

    // buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(page_id, true);

    return;
  }

  Page *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  if (page == nullptr) {
    THROW_OOM("FetchPage fail");
  }
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page->GetData());

  const int size = parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  // buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

  if (size == parent_page->GetMaxSize()) {
    InternalPage *split_page = Split(parent_page);

    const KeyType &mid_key = split_page->KeyAt(0);
    InsertIntoParent(parent_page, mid_key, split_page);
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LatchRoot(OP_TYPE::DELETE);
  if (IsEmpty()) {
    TryUnlatchRoot(OP_TYPE::DELETE);
    return;
  }
  TryUnlatchRoot(OP_TYPE::DELETE);

  Page *page = FindLeafPageCrabbing(key, transaction, OP_TYPE::DELETE);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  const int size = leaf_page->RemoveAndDeleteRecord(key, comparator_);

  if (size < leaf_page->GetMinSize()) {
    const bool shall_delete_leaf = CoalesceOrRedistribute(leaf_page, transaction);

    if (shall_delete_leaf) {
      transaction->AddIntoDeletedPageSet(page->GetPageId());
    }
  }

  TryUnlatchRoot(OP_TYPE::DELETE);
  FreeAllPages(transaction);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    const bool shall_delete_root = AdjustRoot(node);
    if (shall_delete_root && !node->IsLeafPage()) {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    }
    return shall_delete_root && node->IsLeafPage();
  }

  Page *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (page == nullptr) {
    THROW_OOM("FetchPage fail");
  }
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page->GetData());

  const int node_index = parent_page->ValueIndex(node->GetPageId());
  const int left_sibling_index = node_index - 1;
  const int right_sibling_index = node_index + 1;

  Page *left_sibling_page{nullptr};
  Page *right_sibling_page{nullptr};

  N *left_sibling_node{nullptr};
  N *right_sibling_node{nullptr};

  if (left_sibling_index >= 0) {
    const page_id_t &left_sibling_page_id = parent_page->ValueAt(left_sibling_index);
    left_sibling_page = buffer_pool_manager_->FetchPage(left_sibling_page_id);
    if (left_sibling_page == nullptr) {
      THROW_OOM("FetchPage fail");
    }
    LatchPage(left_sibling_page, OP_TYPE::DELETE);
    transaction->AddIntoPageSet(left_sibling_page);

    left_sibling_node = reinterpret_cast<N *>(left_sibling_page->GetData());
  }

  if (right_sibling_index < parent_page->GetSize()) {
    const page_id_t &right_sibling_page_id = parent_page->ValueAt(right_sibling_index);
    right_sibling_page = buffer_pool_manager_->FetchPage(right_sibling_page_id);
    if (right_sibling_page == nullptr) {
      THROW_OOM("FetchPage fail");
    }
    LatchPage(right_sibling_page, OP_TYPE::DELETE);
    transaction->AddIntoPageSet(right_sibling_page);

    right_sibling_node = reinterpret_cast<N *>(right_sibling_page->GetData());
  }

  // first, try to coalesce with the left sibling.
  if (left_sibling_node != nullptr) {
    if (node->GetSize() + left_sibling_node->GetSize() < node->GetMaxSize()) {
      Coalesce(&left_sibling_node, &node, &parent_page, NODE_IS_RIGHT, transaction);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return node->IsLeafPage();
    }
  }

  //  If cannot, try to coalesce with the right sibling.
  if (right_sibling_node != nullptr) {
    if (node->GetSize() + right_sibling_node->GetSize() < node->GetMaxSize()) {
      Coalesce(&right_sibling_node, &node, &parent_page, NODE_IS_LEFT, transaction);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return false;
    }
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);

  // if coalescing is impossible, try to redistribute with the left or the right sibling.
  if (left_sibling_node != nullptr && node->GetSize() + left_sibling_node->GetSize() >= node->GetMaxSize()) {
    Redistribute(left_sibling_node, node, NODE_IS_RIGHT);

  } else {
    Redistribute(right_sibling_node, node, NODE_IS_LEFT);
  }

  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
/// FIXME(bayes): why pass in pointer to pointer?
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node_, N **node_, InternalPage **parent_, int index,
                              Transaction *transaction) {
  N *neighbor_node = *neighbor_node_;
  N *node = *node_;
  InternalPage *parent = *parent_;

  if (index == NODE_IS_LEFT) {
    std::swap(node, neighbor_node);
  }

  const int node_index = parent->ValueIndex(node->GetPageId());
  if (node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);

    leaf_node->MoveAllTo(neighbor_leaf_node);

  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);

    const KeyType &mid_key = parent->KeyAt(node_index);
    internal_node->MoveAllTo(neighbor_internal_node, mid_key, buffer_pool_manager_);
  }

  transaction->AddIntoDeletedPageSet(node->GetPageId());

  parent->Remove(node_index);
  const int size = parent->GetSize();
  if (size < parent->GetMinSize()) {
    const bool shall_delete_parent = CoalesceOrRedistribute(parent, transaction);
    if (shall_delete_parent) {
      transaction->AddIntoDeletedPageSet(parent->GetPageId());
    }
  }

  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  Page *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (page == nullptr) {
    THROW_OOM("FetchPage fail");
  }
  InternalPage *parent = reinterpret_cast<InternalPage *>(page->GetData());

  const int node_index = parent->ValueIndex(node->GetPageId());

  if (node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);

    if (index == NODE_IS_LEFT) {
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
      parent->SetKeyAt(node_index + 1, neighbor_leaf_node->KeyAt(0));
    } else {
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      parent->SetKeyAt(node_index, leaf_node->KeyAt(0));
    }

  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);

    const KeyType mid_key = parent->KeyAt(node_index + (index == NODE_IS_LEFT));

    if (index == NODE_IS_LEFT) {
      parent->SetKeyAt(node_index + 1, neighbor_internal_node->KeyAt(1));
      neighbor_internal_node->MoveFirstToEndOf(internal_node, mid_key, buffer_pool_manager_);
    } else {
      parent->SetKeyAt(node_index, neighbor_internal_node->KeyAt(neighbor_internal_node->GetSize() - 1));
      neighbor_internal_node->MoveLastToFrontOf(internal_node, mid_key, buffer_pool_manager_);
    }
  }

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  bool shall_delete_root{false};
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    const page_id_t &child_page_id = reinterpret_cast<InternalPage *>(old_root_node)->RemoveAndReturnOnlyChild();
    root_page_id_ = child_page_id;
    UpdateRootPageId(false);

    Page *page = buffer_pool_manager_->FetchPage(child_page_id);
    if (page == nullptr) {
      THROW_OOM("FetchPage fail");
    }
    reinterpret_cast<BPlusTreePage *>(page->GetData())->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(child_page_id, true);

    shall_delete_root = true;

  } else if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(false);
    shall_delete_root = true;
  }

  return shall_delete_root;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType unused_key{};
  Page *page = FindLeafPage(unused_key, true);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  IndexIterator iter(leaf_page, 0, buffer_pool_manager_);
  return iter;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *page = FindLeafPage(key);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  const int key_index = leaf_page->KeyIndex(key, comparator_);
  IndexIterator iter(leaf_page, key_index, buffer_pool_manager_);
  return iter;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  LeafPage *leaf_page{nullptr};
  BufferPoolManager *bpm{nullptr};
  IndexIterator iter(leaf_page, 0, bpm, true);
  return iter;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if left_most flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool left_most) {
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    THROW_OOM("FetchPage fail");
  }
  BPlusTreePage *root_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  Page *leaf_page{page};

  BPlusTreePage *node{root_page};
  while (!node->IsLeafPage()) {
    page_id_t child_page_id{INVALID_PAGE_ID};
    if (!left_most) {
      child_page_id = reinterpret_cast<InternalPage *>(node)->Lookup(key, comparator_);
    } else {
      child_page_id = reinterpret_cast<InternalPage *>(node)->ValueAt(0);
    }
    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);

    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);

    node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    leaf_page = child_page;
  }
  return leaf_page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LatchRoot(const OP_TYPE &op_type) {
  if (op_type == OP_TYPE::READ) {
    root_latch_.lock_shared();
  } else {
    root_latch_.lock();
  }
  ++root_latch_cnt_;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::TryUnlatchRoot(const OP_TYPE &op_type) {
  if (root_latch_cnt_ > 0) {
    if (op_type == OP_TYPE::READ) {
      root_latch_.unlock_shared();
    } else {
      root_latch_.unlock();
    }
    --root_latch_cnt_;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAllPages(Transaction *transaction) {
  auto page_set = transaction->GetPageSet();
  for (Page *page : *page_set) {
    UnlatchPage(page, OP_TYPE::INSERT);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
  page_set->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreeAllPages(Transaction *transaction) {
  auto page_set = transaction->GetPageSet();
  auto deleted_page_set = transaction->GetDeletedPageSet();
  for (Page *page : *page_set) {
    UnlatchPage(page, OP_TYPE::DELETE);
    const page_id_t &page_id = page->GetPageId();
    buffer_pool_manager_->UnpinPage(page_id, true);
    if (deleted_page_set->count(page_id) > 0) {
      if (page->GetPinCount() > 0) {
        throw Exception(ExceptionType::INVALID, "GetPinCount exception");
      }
      buffer_pool_manager_->DeletePage(page_id);
      deleted_page_set->erase(page_id);
    }
  }
  page_set->clear();
  assert(deleted_page_set->empty());
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPageCrabbing(const KeyType &key, Transaction *transaction, const OP_TYPE &op_type) {
  LatchRoot(op_type);

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    THROW_OOM("FetchPage fail");
  }

  LatchPage(page, op_type);
  if (op_type != OP_TYPE::READ) {
    transaction->AddIntoPageSet(page);
  }

  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) {
    const page_id_t child_page_id = reinterpret_cast<InternalPage *>(node)->Lookup(key, comparator_);
    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    if (child_page == nullptr) {
      THROW_OOM("FetchPage fail");
    }
    node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    LatchPage(child_page, op_type);
    if (op_type == OP_TYPE::READ) {
      TryUnlatchRoot(op_type);
      UnlatchPage(page, op_type);
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      if (IsNodeSafe(node, op_type)) {
        TryUnlatchRoot(op_type);
        ReleaseAllPages(transaction);
      }
      transaction->AddIntoPageSet(child_page);
    }

    page = child_page;
  }
  return page;
}

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
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

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
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
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
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
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
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
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
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
      std::cout << "  ";
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
