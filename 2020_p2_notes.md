# 2020 Project 2 B+ Tree Index
B+ tree 数据结构：
- 是什么：一个 M-way search tree，用于大量数据中的快速检索和组织。
- 逻辑构成（理论上的空间布局）：
  - internal nodes: M 个 keys，M+1 个 values，每个 value 对应一个 child pointer。
  - leaf nodes: M 个 keys，M+1 个 values，前 M 个 values 对应 full tuple 或 该 tuple 关联的 record ID。最后一个 value 为指向 leaf node chain 中的下一个 leaf node 的 pointer.
- 物理构成（磁盘或内存中的空间布局）：
  - 不管是 internal nodes 还是 leaf nodes，其都占据一个 disk page 大小。
  - 空间布局（忽略 header info）都是 [I][I]..[I]，其中 I 是 item，表示一个 key-value pair。展开即为：[K][V][K][V]..[K][V]。基于此物理空间布局，internal nodes 和 leaf nodes 的设计应为：
    - 对于 internal nodes: 应设第一个 item 的 key 为 invalid。理由：考虑最简单的情形，即 2-way search tree，即 BST。其逻辑空间布局为：[L][K][R]，对应 left child, key, right child，写成 key-value pair 的形式即为：[V][K][V]。显然为使物理布局与逻辑布局一致，第一个 item 的 key 不应该使用，即为 invalid。
    - 对于 leaf nodes: bustub 直接将指向 leaf node chain 中的下一个 leaf node 的 pointer 存到 header info 中，因此维持了原物理空间布局，即 [I][I]..[I]。
- 
