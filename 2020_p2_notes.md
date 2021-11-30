# 2020 Project2 B+ Tree Index 实现总结
### 节点布局
- 逻辑构成（理论上的空间布局）：
  - internal nodes: M 个 keys，M+1 个 values，每个 value 对应一个 child pointer。
  - leaf nodes: M 个 keys，M+1 个 values，前 M 个 values 对应 full tuple 或该 tuple 关联的 record ID。最后一个 value 为指向 leaf node chain 中的下一个 leaf node 的 pointer.
- 物理构成（磁盘或内存中的空间布局）：
  - 不管是 internal nodes 还是 leaf nodes，其都占据一个 disk page 大小。在 fetch 到 memory 中后，占据一个 frame 的 data 部分。
  - 空间布局（忽略 header info）都是 [I][I]..[I]，其中 I 是 item，表示一个 key-value pair。展开即为：[K][V][K][V]..[K][V]。基于此物理空间布局，internal nodes 和 leaf nodes 的设计应为：
    - 对于 internal nodes: 应设第一个 item 的 key 为 invalid。理由：考虑最简单的情形，即 2-way search tree，即 BST。其逻辑空间布局为：[L][K][R]，对应 left child, key, right child，写成 key-value pair 的形式即为：[V][K][V]。显然为使物理布局与逻辑布局一致，第一个 item 的 key 不应该使用，即为 invalid。
    - 对于 leaf nodes: bustub 直接将指向 leaf node chain 中的下一个 leaf node 的 pointer 存到 header info 中，因此维持了原物理空间布局。

### node max_size 的设计
- internal nodes: `SetMaxSize(internal_max_size_ + 1)`.
- leaf nodes: `SetMaxSize(leaf_max_size_)`.
- 如何理解：
  - 我所设计的 B+ tree 不变量之一：除根节点之外，所有节点在插入、删除之前和之后，其节点数目都为 [half_full, full)，即最低半满，最多差一满。如此可保证 先插入再分裂。
  - 为了配合该不变量，以及根据测试文件所给的 internal_max_size_，leaf_max_size_，发现应该如此设计。

### node min_size 的设计
不管是 internal nodes 还是 leaf nodes，均为 `GetMaxSize() >> 1`. 

### GetValue
实现的重点在于 internal pages 和 leaf pages 的 Lookup 函数。由于 keys 有序，因此推荐使用二分查找：设计一个 LowerBound 函数，找到给定 key 的下界，即该 node 中第一个大于或等于给定 key 的位置。该 LowerBound 函数所采用的算法与 C++ STL 中的 `std::lower_bound` 一致。internal nodes 与 leaf nodes 都需要该函数，其实现基本一致，唯一的区别在于：由于 internal nodes 的第一个 key 为 invalid，故应跳过。反映在 LowerBound 的算法上，即将 search space 的长度减一，将 search space 初始左端点 lo 加一。

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
  - 通过比较 size 与叶节点的 min_size 的大小，判断该叶节点是否因此次删除而少于半满。如果是，则尝试合并或重分配。(CoalesceOrRedistribute, Coalesce, Redistribute)
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
- 对于 leaf nodes: trivial. 注意更新 parent 中的 separating key，其总为移动后右节点的第一个 key。
- 对于 internal nodes:
  - 如果 node 是左节点，则做左旋。(neighbor_node::MoveFirstToEndOf, node::CopyLastFrom)
  - 如果 node 是右节点，则做右旋。(neighbor_node::MoveLastToFrontOf, node::CopyFirstFrom)
  - 注意更新被移动的 child 的 parent id. 

### IndexIterator 
iterator 内部维护 leaf page 和 index，用以指向下一个要访问的 record。如果当前的 leaf page 是 leaf node chain 的最后一个 node，且 index 已经等于这个 node 的 size，则这个 iterator 到达了 end。为了方便，可以用一个 flag `is_end_` 表示。
- begin: 找到最左的 leaf page，并将 index 设为 0。
- Begin(key): 根据 key 找到 leaf page，并将 index 设为 key 所在的 index。
- end: 直接将 `is_end_` 设为 true 即可。

### Latch Crabbing
将 GetValue, Insert 和 Remove 中所采用的 FindLeafPage 替换为 FindLeafPageCrabbing，其要点包括：
- 设 GetValue 为 READ 操作，设 Insert 和 Remove 为 WRITE 操作。
- 为 root page id 实现一个读写锁，例如可以用 `std::shared_mutex` 和一个 thread count 变量组合实现。
- 首先对 root page id 加锁，再 fetch root page，再对其加锁。如果是 WRITE 操作，需要添加至 transaction 的 page set 中。
- 对于 READ 操作，先获取 child page 的锁，再解锁 root page id 以及当前层 page 的锁，再 unpin 当前层的 page。在循环中重复该操作序列，直至 leaf page。在 READ 操作的末尾，再重复一遍该操作序列，以解除 leaf page 的锁定。
- 对于 WRITE 操作，先获取 child page 的锁，再判断 child node 是否安全。如果不安全，则继续持有当前的所有锁；如果安全，则首先释放 root page id 的锁，再释放当前持有的所有锁，再 unpin 所有 pages。这些逻辑可以 wrap 进一个辅助函数 ReleaseAllPages 中。在此之后，将 child page 添加到 page set 中。在 WRITE 操作的末尾，再重复一次该操作序列，以确保解除了所有锁。对于 Remove，还需要在此基础上删除 deleted page set 中的 pages。

其他地方的修改要点：
- 不需要对 split page 加锁。
- coalesce 和 redistribute 都需要对 sibling 加锁。
- 在 sequential 中采用的 unpin 策略，需要修改。因为 unpin 操作必须在 unlatch 操作之后，而很多 pages 的 unlatch 操作都被 defer 到 WRITE 操作的末尾处执行。故 unpin 也应该 defer 到此时进行。对于非 crabbing 阶段 fetch 的 pages，可以在使用完毕后，立即 unpin，因为它们已经被锁住了，且 pin count > 1.
- 同样地，delete page 策略也需要修改。从给出的函数注释中可以看出，很多函数的 return value 说明了其传入的某个 page 是否需要在函数结束后删除。在 concurrent 版本中，可以忽略这些说明，而仅在真正删除 page 的时候，将 page 添加至 deleted page set 中，再在 WRITE 操作的末尾处统一删除。

### Optimistic optimization (optional)
实现一个新函数 FindLeafPageOptimistic，主要逻辑参考 lecture。当判断 leaf node 安全时，直接返回 leaf node。如果不安全，解除 leaf node 的写锁并 unpin，再递归调用 FindLeafPageCrabbing。加上该优化后，平均时间（3次提交平均值）从 5.0s 降低到 4.0s。