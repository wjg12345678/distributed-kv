# distributed-kv 文档导航

这份文档是 distributed-kv 的阅读入口。分布式 KV 最容易学散：一会儿看 Raft 论文，一会儿看 RocksDB，一会儿看 HTTP API，最后主线不清。建议按“Raft 安全性 -> 请求链路 -> 故障演练 -> 生产化边界”的顺序学习。

## 1. 项目一句话

```text
distributed-kv 是一个基于 C++17 的强一致键值存储项目，核心用 Raft 实现 leader 选举、日志复制、多数派提交、状态机 apply、线性一致读、snapshot/log compaction，并扩展 RocksDB 持久化、libuv TCP RPC、HTTP 接入、锁服务、MVCC、成员变更接口和基础 metrics。
```

学习主线：

```text
写：leader 排序 -> 日志复制 -> 多数派提交 -> 状态机 apply
读：leader 多数派确认 -> 本地状态机读取
恢复：选举、冲突修复、snapshot、RocksDB 持久化
边界：joint consensus、请求幂等、lease lock、MVCC 生产化
```

## 2. 文档分类

| 目标 | 推荐文档 |
| --- | --- |
| 整体架构 | [architecture.md](architecture.md) |
| 实现要点速记 | [implementation_notes.md](implementation_notes.md) |
| 性能记录 | [performance.md](performance.md) |
| 系统学习 | [project-study-guide-complete.md](project-study-guide-complete.md) |
| 面试问答 | [面试问题完整回答.md](面试问题完整回答.md) |
| 源码导读 | [source-code-walkthrough-complete.md](source-code-walkthrough-complete.md) |
| 一致性答辩 | [consistency-defense-playbook.md](consistency-defense-playbook.md) |
| 生产化路线 | [production-hardening-roadmap.md](production-hardening-roadmap.md) |
| 故障演练 | [failure-drill-runbook.md](failure-drill-runbook.md) |

## 3. 新手阅读顺序

第一次看项目：

```text
1. README.md
2. docs/architecture.md
3. docs/implementation_notes.md
4. docs/project-study-guide-complete.md 的 1-14 章
5. docs/source-code-walkthrough-complete.md
```

先不要急着看所有测试。先搞懂：RaftNode 内部状态有哪些、写请求怎么变成日志、多数派提交怎么推进、committed log 怎么 apply 到状态机、读请求为什么也要确认 leader、snapshot 为什么不会破坏日志索引。

## 4. 面试准备路径

### 4.1 只有 30 分钟

只看：

```text
1. README.md 的已实现能力和架构概览
2. docs/implementation_notes.md
3. docs/consistency-defense-playbook.md 的最终 2 分钟回答
4. docs/面试问题完整回答.md 的项目整体、Raft 基础、生产化追问
```

必须会讲：写为什么必须走 leader、多数派提交为什么安全、旧 leader 为什么不能返回旧读、snapshot 解决什么问题、成员变更为什么需要 joint consensus。

### 4.2 有 2 小时

按这个顺序：

```text
1. docs/project-study-guide-complete.md
2. docs/source-code-walkthrough-complete.md
3. docs/consistency-defense-playbook.md
4. docs/failure-drill-runbook.md
5. docs/production-hardening-roadmap.md
```

重点掌握：Pre-Vote 解决什么、no-op barrier entry 为什么存在、只提交当前 term 日志的原因、ReadIndex 风格读的意义、未提交日志为什么可以被覆盖、RocksDB 和 Raft 的职责边界。

## 5. 源码阅读路径

### 5.1 核心类型

```text
include/core/raft_types.h
include/core/raft_node.h
include/core/raft_persistence.h
include/core/raft_transport.h
```

要回答：role、term、log entry、command、metrics 分别是什么，哪些状态必须持久化，哪些状态是 leader 专属的 volatile state。

### 5.2 Raft 主逻辑

```text
src/core/raft_node.cpp
```

按函数读：

```text
Tick
StartPreVote
StartElection
HandleRequestVote
BecomeLeader
ReplicateCommandLocked
HandleAppendEntries
AdvanceCommitIndex
ApplyCommittedEntries
ConfirmLeaderForReadLocked
MaybeTakeSnapshot
HandleInstallSnapshot
```

要回答：选举如何开始、leader 如何追加日志、follower 如何处理冲突、commit_index 如何推进、apply 和 commit 为什么分开。

### 5.3 网络和 HTTP

```text
src/network/network_transport.cpp
src/network/raft_rpc_server.cpp
src/network/network_codec.cpp
src/network/node_http_server.cpp
src/network/libuv_http_server.cpp
```

要回答：Raft RPC 怎么编码、多进程节点怎么通信、follower 为什么返回 redirect、HTTP API 和 Raft 状态机怎么衔接。

### 5.4 存储

```text
src/storage/key_value_store.cpp
src/core/raft_persistence.cpp
```

要回答：RocksDB 存什么、KV 状态机和 Raft 元数据怎么区分、snapshot 和 log compaction 怎么影响恢复。

## 6. 故障演练路径

优先看：

```text
docs/failure-drill-runbook.md
```

至少要能解释 leader 崩溃、follower 崩溃、多数派不可用、网络分区、旧 leader 恢复、follower 日志冲突、snapshot 追赶和旧 leader 读防护。

分布式项目的说服力来自故障场景，不是只跑通正常 PUT/GET。

## 7. 最重要的 10 个问题

1. Raft 如何保证同一 term 最多一个 leader？
2. `voted_for` 为什么必须持久化？
3. 日志新旧检查怎么做？
4. AppendEntries 怎么修复 follower 冲突日志？
5. 为什么只直接提交当前 term 日志？
6. no-op barrier entry 的意义是什么？
7. 线性一致读为什么不能直接读 leader 本地？
8. snapshot 安装后日志索引怎么保持连续语义？
9. 成员变更为什么需要 joint consensus？
10. 请求幂等为什么不是 Raft 自动解决的？

## 8. 生产化边界

优先看：

```text
docs/production-hardening-roadmap.md
docs/consistency-defense-playbook.md
```

必须主动承认：当前不等价 etcd，成员变更需要 joint consensus，锁服务还需要 lease 和 fencing token，MVCC 还不是完整数据库事务，请求幂等、TLS、认证、备份恢复、Jepsen 风格测试还需要补。

更好的表达：

```text
项目实现了 Raft KV 核心链路和多个扩展能力，但生产化优先级应该是安全性、可恢复性和故障验证，而不是先堆更多 API。
```

## 9. 最终建议

distributed-kv 的学习重点是：

```text
Raft 安全性：选举、日志匹配、提交规则
读写链路：HTTP -> leader -> log -> majority -> apply
故障恢复：leader 切换、冲突覆盖、snapshot
生产边界：joint consensus、幂等、lease、认证、备份
```

能把这四条讲清楚，比背所有接口更重要。
