# 2020 Project 2 B+ Tree Index
B+ tree 索引
- 是什么：一个 M-way search tree，用于大量数据中的快速检索和组织。

### 节点布局
- 逻辑构成（理论上的空间布局）：
  - internal nodes: M 个 keys，M+1 个 values，每个 value 对应一个 child pointer。
  - leaf nodes: M 个 keys，M+1 个 values，前 M 个 values 对应 full tuple 或 该 tuple 关联的 record ID。最后一个 value 为指向 leaf node chain 中的下一个 leaf node 的 pointer.
- 物理构成（磁盘或内存中的空间布局）：
  - 不管是 internal nodes 还是 leaf nodes，其都占据一个 disk page 大小。
  - 空间布局（忽略 header info）都是 [I][I]..[I]，其中 I 是 item，表示一个 key-value pair。展开即为：[K][V][K][V]..[K][V]。基于此物理空间布局，internal nodes 和 leaf nodes 的设计应为：
    - 对于 internal nodes: 应设第一个 item 的 key 为 invalid。理由：考虑最简单的情形，即 2-way search tree，即 BST。其逻辑空间布局为：[L][K][R]，对应 left child, key, right child，写成 key-value pair 的形式即为：[V][K][V]。显然为使物理布局与逻辑布局一致，第一个 item 的 key 不应该使用，即为 invalid。
    - 对于 leaf nodes: bustub 直接将指向 leaf node chain 中的下一个 leaf node 的 pointer 存到 header info 中，因此维持了原物理空间布局，即 [I][I]..[I]。

- GetValue
  - 从根节点开始 traverse down the tree。遇到 internal nodes，则将其中的 keys 作为 sign posts，以 direct 进下一层。重复直到 leaf node。
  - 利用 node 内部 keys 的有序性，设计一个 LowerBound 函数，找到给定 key 的下界，即该 node 中第一个大于或等于给定 key 的位置。该 LowerBound 函数所采用的算法与 C++ STL 中的 `std::lower_bound` 一致。
  - internal nodes 与 leaf nodes 都需要该函数，其实现基本一致，唯一的区别在于：由于 internal nodes 的第一个 key 为 invalid，故应跳过。反映在 LowerBound 的算法上，即将 search space 的长度减一，将 search space 初始左端点 lo 加一。
  - 当 node 中所有 keys 都小于给定 key 时，LowerBound 会返回 size_ ，此时应做合适处理。

### node max_size 的设计
- internal nodes: `SetMaxSize(internal_max_size_ + 1)`.
- leaf nodes: `SetMaxSize(leaf_max_size_)`.
- 如何理解：
  - 我所设计的 B+ tree 不变量之一：除根节点之外，所有节点在插入、删除之前和之后，其节点数目都为 [half_full, full)，即最低半满，最多差一满。如此可保证 先插入再分裂。
  - 为了配合该不变量，以及根据测试文件所给的 internal_max_size_，leaf_max_size_，发现应该如此设计。与 internal nodes 的第一个 item 的 key 为 invalid 无关。

### node min_size 的设计
不管是 internal nodes 还是 leaf nodes，均为 `GetMaxSize() >> 1`. 

### Insert 
- 如果树为空，则创建一个新的树，只包含一个根节点（必为叶节点），并在 header page 中更新树索引信息。将待插入的 kv pair 插入其中。此次插入必成功，因为不可能有 duplicate key。(StartNewTree, UpdateRootPageId)
- 如果树不为空，则先找到该 key 应插入的叶节点 leaf_page，再将该 kv pair 插入其中。(InsertIntoLeaf, FindLeafPage, leaf_page::Insert)
  - 通过观察 size 是否增加，判断此次插入是否因为存在 duplicate key 而失败。
  - 通过比较 size 与叶节点 max_size 是否相等，判断该叶节点是否因此次插入而满。如果是，则 split。(Split)
- 如果 split，将 separating kv pair 插入到父节点 parent_node 中。(InsertIntoParent, parent_node::InsertNodeAfter)
- 如果分裂的节点是根节点，需要创建一个新的根节点 new_root_node，并在 header page 中更新树索引信息。(new_root_node::PopulateNewRoot, UpdateRootPageId)
- 注意分裂可能递归进行。

### Split
- 创建一个新的 new_node，初始化它。(new_node::Init)
- 将 old_node 中的一半 items 移动到 new_node。(old_node::MoveHalfTo, new_node::CopyNFrom)
- 注意：
  - 由于 internal nodes 与 leaf nodes 的 MoveHalfTo 接口不一致，可以自己修改接口，或利用 reinterpret_cast，将 `N *` 转换为相应的类型。
  - new_node 总在 old_node 的右边，故 `InsertNodeAfter` 。
  - 对于 leaf nodes，注意将 new_node 添加到 leaf node chain 的紧邻 old_node 的后面。
  - 对于 internal nodes，应将移动后的 new_node 中的第一个 key push up 到 parent_node 中。所谓的 push up，即在 new_node 中删除该 key，并移动到 parent_node 中。然而由于 internal node 中的第一个 item 的 key 为 invalid，因此无需做真正的删除。故 internal nodes 与 leaf nodes 的 MoveHalfTo 与 CopyNFrom 的实现完全一致（除了 internal nodes 需要更改被移动的 children 的 parent id 之外）。

### Remove 
- 如果树为空，则立即返回。
- 如果树不为空，首先找到给定 key 应存在的 leaf_page，再删除该 key 对应的 kv pair。(FindLeafPage, leaf_page::RemoveAndDeleteRecord)
  - 通过观察 size 是否减少，判断此次删除是否因 key 不存在而失败。
  - 通过比较 size 与叶节点的 min_size 的大小，判断该叶节点是否因此次删除而少于半满。如果是，则尝试合并或重分配。(CoalesceOrRedistribute)
- 由于根节点的 min_size 不同于其他节点，因此需要特殊处理，且需要区分 internal node 与 leaf node。如果根节点需要被删除，注意更新树索引信息。(AdjustRoot, UpdateRootPageId)
- 注意先尝试合并，如合并不可能，再尝试重分配。（此处存疑，不同资料说法不一）
- 注意合并可能递归进行，重分配不会递归进行。
- 合并或重分配的对象可以是 left_sibling 或 right_sibling。但是有些节点没有 left_sibling，有些节点没有 right_sibling，但至少有其一。
  - 对于合并的特殊限制：合并后为空的节点一定是右节点（理论上当然也可以是左节点，但是 bustub 的接口是这样设计的）。如果所找到的 sibling 为 right_sibling，应利用 `std::swap` 将其与当前节点交换。

### Coalesce
- 将右节点 node 的所有 items 全部移动到左节点 neighbor_node 中。(node->MoveAllTo, neighbor_node::CopyNFrom)
  - 对于 internal nodes: 将 parent 中的 separating key push down 到左节点中。且注意更新所移动的 children 的 parent id.
  - 对于 leaf nodes: 删除 parent 中的 separating key。且注意更新 leaf node chain.
- 判断 parent 是否因该 separating key 被删除，而少于半满。如果是，则递归尝试合并或重分配。(CoalesceOrRedistribute)

### Redistribute
- 对于 leaf nodes: trivial. 注意更新 parent 中的 separating key，其总为移动后右节点的第一个 key（即 invalid key）
- 对于 internal nodes:
  - 如果 node 是左节点，则做左旋。(neighbor_node::MoveFirstToEndOf, node::CopyLastFrom)
  - 如果 node 是右节点，则做右旋。(neighbor_node::MoveLastToFrontOf, node::CopyFirstFrom)
  - 注意更新被移动的 child 的 parent id. 

### IndexIterator
- begin
- Begin(key)
- end

### Latch Crabbing