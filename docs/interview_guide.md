# Distributed KV Interview Guide

这份文档面向秋招面试场景，目标不是解释全部代码细节，而是帮你把项目讲得准确、稳定、有重点。

## 简历描述

### 1 行版

基于 C++17 实现了一个支持 Raft 共识、RocksDB 持久化、快照压缩、线性一致读和多进程 TCP 通信的分布式 KV 存储原型。

### 2 行版

基于 C++17 从零实现分布式 KV 存储，完成 Raft Leader 选举、日志复制、冲突回退、快照压缩、RocksDB 持久化和多进程 TCP 通信。  
补充 Pre-Vote 与 ReadIndex 风格线性一致读，并在状态机上扩展锁服务、MVCC、HTTP API 和功能测试。

### 3 条项目亮点

- 实现 Raft 核心链路，包括选举、复制、冲突覆盖、快照、InstallSnapshot 和持久化恢复
- 为正确性边界补齐 Pre-Vote 与线性一致读，处理隔离节点恢复扰动和旧 leader 脏读问题
- 搭建多进程真实网络版，提供 HTTP API、Prometheus 风格指标、锁服务、MVCC、`ctest` 回归和本地 benchmark 脚本

## 30 秒介绍

我做了一个基于 Raft 的分布式 KV 原型，核心用 C++17 实现了 Leader 选举、日志复制、快照和 RocksDB 持久化。项目不是单机 demo，还支持多进程 TCP 通信、HTTP API、锁服务和 MVCC。后面我又补了 Pre-Vote 和线性一致读，把面试里最常被追问的正确性边界也做了。

## 90 秒介绍

这个项目的目标是从零走通一个分布式存储系统的主链路。Raft 核心层负责选举、复制、冲突回退、当前 Term commit 约束和快照安装；状态机会把已提交日志应用到 KV、锁服务和简化版 MVCC；存储层用 RocksDB 持久化 KV 数据和 Raft 元数据；网络层则提供多进程 TCP RPC 和最小 HTTP 接口。

我后面重点补了两块边界。第一块是 Pre-Vote，用来避免网络隔离节点恢复后无意义地抬高 term 干扰现有 leader。第二块是线性一致读，leader 在读之前会先做多数派确认，因此失去多数派的旧 leader 不会继续返回旧数据。为了证明这些行为，我还补了分区测试、`ctest` 回归、多进程 demo 脚本和本地 `wrk` benchmark。

## Top 5 高频问答稿

### Q1. 这个项目和普通 KV demo 最大的区别是什么？

普通 KV demo 往往只实现单机读写或者简单主从复制，但这个项目把共识、状态机、持久化和网络都真正串起来了。我不只是做了 put/get，还实现了 Raft 选主、日志复制、冲突回退、快照、RocksDB 持久化和多进程 TCP 通信，另外还补了 Pre-Vote 和线性一致读这两个正确性边界。

### Q2. 为什么要加 Pre-Vote？

因为没有 Pre-Vote 时，隔离节点恢复后可能先把自己的 term 提高，再去请求投票。即使它最终选不上 leader，也会迫使现有 leader 因为更大 term 被动退位，造成不必要抖动。Pre-Vote 先试探多数派意见，拿不到多数派就不进入正式选举，所以 term 不会被无意义地抬高。

### Q3. 线性一致读具体怎么保证？

我没有让 leader 直接读本地状态机，而是在读之前先向多数派做一次确认。只有在多数派仍然承认它是 leader 的情况下，才允许它读取本地状态机；如果它已经失去多数派，就直接拒绝读请求。这样可以避免旧 leader 在网络分区后继续返回旧数据。

### Q4. 快照和日志冲突恢复是怎么配合的？

如果 follower 只是落后少量日志，leader 会根据冲突 term 和冲突 index 快速回退 `next_index`，然后继续 AppendEntries；如果 follower 落后太多，已经追不上被压缩掉的日志了，leader 就会直接发 InstallSnapshot。也就是说，先尝试日志级恢复，恢复不了再走快照级恢复。

### Q5. 这个项目目前最大的不足是什么？

我会主动说三点。第一，成员变更现在还是简化实现，没有做 joint consensus。第二，线性一致读做的是多数派确认方案，还没做 lease read 优化。第三，工程层面还缺优雅退出、配置文件化、跨机器压测和更系统的混沌测试。这些不足我会明确区分，不会把教学版实现说成生产级方案。

## 架构速记

```text
Client
  |
  v
HTTP API
  |
  v
Leader RaftNode
  |
  +--> append log entry
  +--> replicate over TCP + Protobuf
  +--> majority commit
  +--> apply to state machine
              |
              +--> KV
              +--> Lock
              +--> MVCC
  |
  +--> persist metadata / snapshot via RocksDB
```

面试时可以按这条线讲：

- 控制面：选主、term、投票、成员关系
- 数据面：日志复制、commit、apply、状态机更新
- 恢复面：RocksDB 持久化、snapshot、InstallSnapshot
- 服务面：HTTP、重定向、metrics、锁与 MVCC

## 高频问题

### 1. 为什么要加 Pre-Vote？

没有 Pre-Vote 时，隔离节点恢复后可能先把自己的 term 加 1，再去请求投票。即使它选不上 leader，也可能让正常 leader 因为看到更大的 term 被迫退位，造成不必要抖动。Pre-Vote 的作用是在真正递增 term 前先试探一次，多数派认可后才进入正式选举。

### 2. 线性一致读是怎么保证的？

当前实现不是 follower 本地读，也不是 leader 直接读内存，而是 leader 在读之前先确认自己仍然持有多数派。只有在“当前 leader 身份仍被多数派确认”的前提下，才读取本地状态机，因此失去多数派的旧 leader 会拒绝读请求。

### 3. 为什么只推进当前 Term 的日志 commit？

这是 Raft 的关键约束。旧 Term 的日志即使已经复制到多数派，也不能单独据此直接推进 commit；必须等当前 Term 的某条日志被提交后，才能间接确认之前的日志也安全。这样可以避免领导者切换时对提交状态的错误判断。

### 4. 日志冲突是怎么处理的？

follower 会校验 `prev_log_index` 和 `prev_log_term`。如果冲突，就返回冲突 term 和冲突起点，leader 依据这些信息快速回退 `next_index`，而不是一条条试错。这样可以降低冲突恢复的往返次数。

### 5. 快照是怎么做的？

当已提交日志达到阈值后，状态机会导出当前 KV 数据和 peer 信息，生成 snapshot，并截断已经被快照覆盖的日志。落后过多的 follower 不能再通过普通 AppendEntries 追平时，leader 会发送 InstallSnapshot。

### 6. RocksDB 里持久化了什么？

- 状态机数据：KV、锁服务、MVCC 版本数据
- Raft 元数据：`current_term`、`voted_for`、日志、`commit_index`
- 快照数据：快照索引、快照 term、状态机内容和 peer 集合

### 7. 锁服务为什么也是一致的？

锁的 acquire/release 不是直接改内存，而是作为状态机命令进入 Raft 日志。只有提交后才会真正更新 owner，所以多个节点对锁状态的观察是一致的。

### 8. MVCC 做到了什么程度？

当前是教学版简化 MVCC，支持事务开始、按快照时间读取历史版本、提交时做写写冲突检测。它适合说明“状态机不只是 KV put/get”，但不是完整数据库级事务实现。

### 9. 成员变更做到哪一步了？

当前是简化版成员变更，通过日志命令 add/remove peer 修改配置。它能演示动态扩缩容的基本过程，但不是生产级 joint consensus。这个点要主动说清楚，不要把它讲成完整生产方案。

### 10. 当前项目最大的不足是什么？

我会主动提 3 个：

- 成员变更还是简化实现，没有 joint consensus
- 线性一致读做的是多数派确认方案，还没做 lease read 优化
- 工程层面还缺优雅退出、配置文件化、跨机器压测和更系统的混沌测试

## 面试时推荐主动强调的点

- 这个项目不只是“读论文后写 demo”，而是把共识、状态机、持久化、网络和接口都贯通了
- 我不仅实现 happy path，也专门补了网络分区恢复和旧 leader 读这两个边界
- 我知道它不是生产级数据库，所以会明确区分“已完成能力”和“当前限制”

## 容易说错的地方

- 不要直接说“我实现了完整 Raft 成员变更”，因为当前还不是 joint consensus
- 不要笼统说“读一定强一致”，要补一句“当前是 leader 多数派确认后再读”
- 不要把 MVCC 讲成完整事务系统，当前重点是快照读和写写冲突检测
- 不要只讲 API 和功能，要先讲正确性，再讲工程化

## 如果面试官继续深挖

可以顺着这条路线回答：

1. 为什么需要 Pre-Vote
2. 为什么旧 leader 不能继续线性一致读
3. 为什么只提交当前 Term 日志
4. 冲突回退和 InstallSnapshot 如何配合
5. 如果继续做，会优先补 joint consensus、优雅退出 / 配置文件化，以及跨机器 benchmark 和混沌测试
