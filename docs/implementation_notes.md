# 分布式 KV 实现要点

这份文档用于概括系统能力、关键一致性机制和当前实现范围，便于快速了解项目主链路。

## 概要

`distributed-kv` 基于 C++17 实现，覆盖了 Raft 共识、RocksDB 持久化、快照压缩、线性一致读、基于 libuv TCP 长连接 + Protobuf 的多进程通信、基于 libuv + llhttp 的 HTTP 接入层，以及面向 KV / 锁服务 / MVCC 的状态机扩展。

## 核心能力

- 完整的 Raft 主链路：Leader 选举、日志复制、冲突回退、当前 Term commit 约束、快照与 InstallSnapshot
- 一致性边界补充：Pre-Vote 与 ReadIndex 风格线性一致读
- 多进程网络形态：独立节点进程、libuv TCP 长连接 + Protobuf 节点通信、libuv + llhttp HTTP 接口与纯文本指标
- 状态机扩展：KV、分布式锁、MVCC 读写与写写冲突检测
- 工程配套：`ctest` 回归测试、多进程 smoke / e2e 脚本、`wrk` 压测脚本

## 架构速记

```text
客户端
  |
  v
HTTP 接口
  |
  v
主节点 RaftNode
  |
  +--> 追加日志
  +--> 通过 libuv TCP 长连接 + Protobuf 复制
  +--> 多数派提交
  +--> 应用到状态机
              |
              +--> KV
              +--> 锁服务
              +--> MVCC
  |
  +--> 通过 RocksDB 持久化元数据 / 快照
```

可以按下面四条主线理解系统实现：

- 控制面：选主、term、投票、成员关系
- 数据面：日志复制、commit、apply、状态机更新
- 恢复面：RocksDB 持久化、snapshot、InstallSnapshot
- 服务面：HTTP、重定向、指标、锁与 MVCC

## 关键机制

### Pre-Vote

隔离节点恢复后，如果直接提升 term 并发起正式选举，可能会无意义地干扰现有 leader。当前实现先进行 Pre-Vote 探测，只有拿到多数派认可才进入正式选举，从而避免无效 term 抬升。

### 线性一致读

读取路径不是 leader 直接访问本地状态机，而是先完成多数派确认。只有在多数派仍承认当前 leader 身份的前提下，节点才会返回状态机结果，因此失去多数派的旧 leader 不会继续返回旧数据。

### 冲突恢复与快照

日志落后较少时，leader 会根据冲突 term 和冲突 index 快速回退 `next_index` 并继续 AppendEntries；日志落后过多、已超出保留窗口时，则切换到 InstallSnapshot 路径完成追平。

### 当前 Term 提交约束

旧 Term 日志即使已经复制到多数派，也不能单独据此推进 commit。只有当前 Term 的日志被提交后，之前的日志才能被间接确认安全，这是避免领导者切换时错误提交判断的关键约束。

## 状态机扩展

### 锁服务

锁的 acquire / release 作为状态机命令进入 Raft 日志，只有提交后才会更新 owner，因此多个节点对锁状态的观察保持一致。

### MVCC

MVCC 提供事务开始、基于 `snapshot_ts` 的历史版本读取以及提交阶段的写写冲突检测，适合承载轻量事务语义和状态快照读取场景。

### 成员变更

当前成员变更通过 `BeginJointConfig` / `FinalizeConfig` 两阶段日志命令完成 add / remove peer，能够覆盖 joint consensus 风格的动态扩缩容主流程。当前运维层仍然依赖脚本完成新节点拉起、端口分配和数据目录准备。

## 工程范围

- 线性一致读当前采用多数派确认路径，未引入 lease read
- HTTP 接口与 TCP RPC 组件聚焦核心数据链路，鉴权、TLS、限流等能力可继续扩展
- 压测结果基于单机回环网络，适合观察本地链路行为和性能基线
- 单进程 HTTP 入口启动时会清理既有 `/tmp` 数据目录

## 可扩展方向

- 补齐更完整的节点 bootstrap / decommission 编排与运维自动化
- 增加优雅退出、配置文件化和更完整的部署管理能力
- 引入跨机器压测、故障注入和系统化混沌验证
