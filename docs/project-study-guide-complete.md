# distributed-kv 项目学习指南

这份文档的目标不是把代码逐行翻译一遍，而是帮你建立一套能讲清楚、能定位代码、能回答追问的项目理解方式。面试时你需要证明三件事：

1. 你知道这个项目为什么存在，它解决的核心问题是什么。
2. 你能把一次请求从 HTTP 入口讲到 Raft 提交、状态机应用、持久化和返回。
3. 你知道当前实现和生产级分布式 KV 的差距在哪里，并且能说出后续怎么演进。

## 1. 项目定位

`distributed-kv` 是一个 C++17 实现的分布式键值存储系统，核心重点是 Raft 共识和一致性状态机。它不是简单的 HTTP KV 服务，也不是单机 RocksDB wrapper，而是把客户端写入包装成 Raft 日志，在多数派确认后再应用到状态机，从而让多个节点最终保持同一份状态。

可以这样介绍：

> 这个项目实现了一个基于 Raft 的分布式 KV。客户端请求先进入 leader，leader 把写操作追加为日志并复制给 follower，多数派确认后推进 commit index，再按日志顺序应用到状态机。项目还补充了 Pre-Vote、ReadIndex 风格线性一致读、快照压缩、InstallSnapshot、RocksDB 持久化、TCP + Protobuf 多进程通信、HTTP 接口、分布式锁、MVCC 和成员变更接口。

这段介绍要突出：

- 分布式一致性是核心。
- KV、锁、MVCC 都是状态机能力。
- RocksDB 是状态机数据和 Raft 元数据的落盘实现。
- HTTP 只是接入层，不是项目的技术核心。

## 2. 能力边界

### 已实现

- Raft follower / candidate / leader 三角色。
- 随机化选举超时。
- Pre-Vote，降低网络分区恢复时的 term 抖动。
- RequestVote 日志新旧判断。
- AppendEntries 心跳和日志复制。
- 日志冲突检测、冲突 term 回退、日志截断覆盖。
- leader 当选后追加 no-op barrier entry。
- 只提交当前 term 日志的规则。
- commit index 推进和状态机 apply。
- ReadIndex 风格线性一致读：leader 读前确认多数派。
- Snapshot / log compaction。
- InstallSnapshot 给落后 follower 补状态。
- RocksDB 持久化状态机和 Raft 元数据。
- 多进程 TCP + Protobuf Raft RPC。
- HTTP KV / lock / MVCC / admin / metrics 接口。
- 锁服务和 MVCC 写写冲突检测。
- 成员变更接口。
- CTest 单元测试和本地集群脚本。

### 必须主动承认的边界

- 成员变更是直接配置变更，还不是 Raft joint consensus。
- 没有完整生产级认证、鉴权、TLS、限流和审计。
- HTTP parser 是项目内简化实现，不是完整工业级 HTTP 栈。
- 本地脚本适合单机多进程验证，不等于跨机器生产部署方案。
- RocksDB 和 Raft 状态已经持久化，但故障恢复、数据校验、备份恢复、磁盘空间治理仍比较基础。
- 没有完善的运维控制面，比如自动扩缩容、滚动升级、配置中心、告警系统。

面试中不要把它包装成生产级 etcd。更合适的说法是：

> 这是一个面向学习和工程展示的强一致 KV 原型，重点把 Raft 的关键链路和本地多进程验证跑通。生产化还需要 joint consensus、鉴权 TLS、跨机部署、灾备、监控告警、限流保护和更完整的故障注入。

## 3. 仓库结构

```text
apps/
  single_process_main.cpp      单进程内存集群入口
  http_server_main.cpp         单进程 RocksDB + HTTP 入口
  network_node_main.cpp        多进程 TCP 节点入口

include/core/
  raft_types.h                 Raft 核心数据结构
  raft_node.h                  RaftNode 对外接口和内部状态
  raft_transport.h             Raft RPC 抽象
  raft_persistence.h           Raft 元数据持久化抽象
  cluster.h                    单进程内存集群辅助

src/core/
  raft_node.cpp                Raft 核心逻辑
  raft_persistence.cpp         RocksDB 元数据持久化
  cluster.cpp                  单进程节点编排

include/network/
  http_server.h                简化 HTTP server
  node_http_server.h           面向节点的 HTTP API
  network_transport.h          多进程 Raft transport
  raft_rpc_server.h            Raft RPC server
  network_codec.h              Protobuf 编解码
  task_executor.h              HTTP worker 执行器

src/network/
  node_http_server.cpp         HTTP API 路由和业务调用
  network_transport.cpp        TCP Raft RPC client
  raft_rpc_server.cpp          TCP Raft RPC server
  network_codec.cpp            Protobuf 编解码

include/storage/
  key_value_store.h            状态机存储接口

src/storage/
  key_value_store.cpp          InMemory / RocksDB 实现

tests/unit/
  raft_*_test.cpp              Raft、快照、MVCC、锁、成员变更等测试

scripts/
  run_local_cluster.sh         本地多进程集群启动/停止
  verify_local_cluster.sh      本地接口验证
```

## 4. 先跑起来

### 构建

```bash
cmake -S . -B build
cmake --build build -j
```

依赖包括：

- C++17 编译器
- CMake
- Protobuf
- pkg-config
- RocksDB

当前 CMake 会链接 RocksDB，所以即使先看内存版，也需要本机装 RocksDB 开发库。

### 单进程内存版

```bash
./build/distributed_kv_single_process
```

这个入口把 3 个节点放到一个进程里，Raft RPC 通过 `Cluster` 直接调用，适合调试选举、复制和状态机应用。

### 单进程 HTTP 版

```bash
./build/distributed_kv_http_server
curl -X PUT http://127.0.0.1:9006/kv/name -d 'alice'
curl http://127.0.0.1:9006/kv/name
```

这个入口仍然是单进程 3 节点，但状态机和 Raft 元数据使用 RocksDB。

### 多进程集群

```bash
./scripts/run_local_cluster.sh start
./scripts/verify_local_cluster.sh
./scripts/run_local_cluster.sh stop
```

多进程版最适合简历展示，因为它包含真实 TCP、节点独立进程、独立数据目录和 HTTP 端口。

## 5. 一句话架构

```text
Client
  -> HTTP API
  -> Leader RaftNode
  -> append local log
  -> send AppendEntries to followers
  -> majority replicated
  -> advance commit index
  -> apply command to state machine
  -> RocksDB / memory store
```

所有写操作都要经过 Raft 日志。KV 写入、加锁、释放锁、MVCC commit、成员变更，本质上都是不同类型的 `Command`。

## 6. 核心数据结构

先看 `include/core/raft_types.h`。

### Role

```cpp
enum class Role {
  Follower,
  Candidate,
  Leader,
};
```

这是 Raft 的三种角色：

- follower 被动接收 leader 心跳和投票请求。
- candidate 选举超时后发起投票。
- leader 处理客户端写入并复制日志。

### CommandType

```cpp
enum class CommandType {
  Noop,
  Put,
  AddPeer,
  RemovePeer,
  AcquireLock,
  ReleaseLock,
  MvccCommit,
};
```

`CommandType` 是状态机命令类型。理解它很关键，因为它说明项目不是只支持 KV，而是把锁、MVCC、成员变更也做成 Raft 日志命令。

### LogEntry

```cpp
struct LogEntry {
  int term = 0;
  Command command;
};
```

每条日志都带 term。Raft 通过 `(index, term)` 判断日志是否匹配，用 term 处理冲突覆盖和提交安全。

### RPC 结构

- `RequestVoteRequest / Response`：选举投票。
- `AppendEntriesRequest / Response`：心跳和日志复制。
- `InstallSnapshotRequest / Response`：快照安装。

这些结构对应 Raft 论文里的核心 RPC。

### Metrics

`RaftMetricsSnapshot` 记录 PreVote、RequestVote、AppendEntries、InstallSnapshot、线性一致读、客户端写、锁、MVCC、commit/apply 等计数，最后通过 `/metrics` 暴露。

## 7. RaftNode 内部状态

看 `include/core/raft_node.h`。

重要字段分几类：

### 身份和集群

- `id_`：当前节点 ID。
- `peers_`：其他节点 ID。
- `transport_`：Raft RPC 发送接口。

### 持久化 Raft 状态

- `current_term_`：当前任期。
- `voted_for_`：当前 term 投给谁。
- `log_`：日志数组。
- `log_base_index_`：快照压缩后日志数组起点对应的全局 index。

这些必须持久化，否则节点重启后可能重复投票或丢日志。

### 易失状态

- `commit_index_`：已经被多数派确认可提交的最大日志 index。
- `last_applied_`：已经应用到状态机的最大日志 index。

`commit_index_` 代表 Raft 层提交，`last_applied_` 代表状态机实际执行。正常情况下 `last_applied_` 会追赶 `commit_index_`。

### leader 专属复制状态

- `next_index_`：leader 认为下次应该发给某个 follower 的日志 index。
- `match_index_`：leader 确认某个 follower 已复制到的最大 index。

leader 用 `match_index_` 判断某条日志是否达到多数派，从而推进 `commit_index_`。

### 选举计时

- `election_elapsed_`
- `base_election_timeout_ticks_`
- `randomized_election_timeout_ticks_`
- `heartbeat_elapsed_`
- `heartbeat_interval_ticks_`

随机化选举超时可以降低多个 follower 同时发起选举导致 split vote 的概率。

### 状态机和持久化

- `store_`：KV 状态机。
- `persistence_`：Raft 元数据和 snapshot 持久化。

## 8. 写请求完整路径

以 `PUT /kv/name` 为例。

### 入口：HTTP 路由

代码在 `src/network/node_http_server.cpp`。

`HandleRequest()` 根据路径分发：

- `/kv/<key>` + `PUT` 调用 `HandlePut()`。
- `/kv/<key>` + `GET` 调用 `HandleGet()`。
- `/lock/*` 调锁接口。
- `/mvcc/*` 调 MVCC 接口。
- `/admin/*` 调成员变更接口。

### leader 检查

`HandlePut()` 会先判断：

```cpp
if (node_->role() != Role::Leader) {
  auto redirect = RedirectToLeader("/kv/" + key);
  ...
}
```

这说明写请求只由 leader 处理。follower 如果知道 leader，会返回 307 redirect；如果不知道 leader，返回 503。

### 生成 Raft Command

`RaftNode::Propose()` 生成：

```cpp
Command command;
command.type = CommandType::Put;
command.key = key;
command.value = value;
```

然后进入 `ReplicateCommand()`。

### 追加本地日志

`ReplicateCommandLocked()` 中：

- 检查当前必须是 leader。
- 生成目标日志 index。
- `log_.push_back(LogEntry{current_term_, std::move(command)})`。
- `PersistState()` 持久化 Raft 状态。
- 更新 leader 自己的 `match_index_` 和 `next_index_`。

### 复制给 follower

leader 遍历 `peers_` 调用 `ReplicateToPeer(peer_id)`。如果 follower 的日志落后到快照之前，则调用 `SendSnapshotToPeer()`；否则发送 AppendEntries。

### 推进提交

复制之后调用 `AdvanceCommitIndex()`：

- 从 `LastLogIndex()` 往回找 candidate index。
- 只考虑当前 term 的日志。
- 统计包含该 index 的副本数。
- 达到多数派则更新 `commit_index_`。
- 调用 `ApplyCommittedEntries()`。

### 应用到状态机

`ApplyCommittedEntries()` 按顺序把 `last_applied_ + 1` 到 `commit_index_` 的日志交给 `ApplyCommand()`。

对 `CommandType::Put` 来说：

```cpp
store_->Put(command.key, command.value);
```

如果 store 是 RocksDB 实现，最终进入 `RocksDbKeyValueStore::Put()`。

## 9. 读请求完整路径

以 `GET /kv/name` 为例。

### 为什么读也不能直接读本地

如果旧 leader 被网络隔离，它本地可能还以为自己是 leader。如果它直接读本地状态机，就可能返回旧数据。为了线性一致读，leader 在读之前必须确认自己仍然能联系多数派。

### HTTP 层处理

`HandleGet()`：

- follower 返回 redirect 或 503。
- leader 调用 `ConfirmLeaderForRead()`。
- 确认成功后调用 `GetValue()` 读状态机。

### ReadIndex 风格确认

`ConfirmLeaderForReadLocked()` 做几件事：

1. 当前节点必须是 leader。
2. 如果还没有当前 term 已提交日志，先发心跳推动 no-op barrier。
3. 向 follower 发送 AppendEntries 心跳。
4. 收到多数派确认后才允许读取。
5. 如果 `last_applied_ < commit_index_`，先补 apply。

这个实现没有完整实现 etcd 那种 ReadIndex 上下文队列，但核心思想是相同的：读前确认 leader 仍握有多数派。

## 10. 选举流程

选举入口在 `Tick()`。

### Tick

每次 tick：

- leader 增加 heartbeat 计时，达到间隔就 `SendHeartbeats()`。
- follower/candidate 增加 election 计时，达到随机超时就 `StartPreVote()`。

### Pre-Vote

`StartPreVote()` 使用 `current_term_ + 1` 构造预投票请求。它不会立刻增加自己的 term，只有拿到多数派预投票才进入正式选举。

`HandlePreVote()` 会拒绝：

- 请求 term 不大于当前 term。
- 当前节点最近刚收到 leader 心跳。
- candidate 日志不够新。

Pre-Vote 的价值是：隔离节点恢复时不会随便把 term 拉高，减少对正常 leader 的扰动。

### RequestVote

`StartElection()`：

- 角色变成 candidate。
- `current_term_++`。
- 投票给自己。
- 持久化状态。
- 向 peers 发送 RequestVote。
- 多数派同意后 `BecomeLeader()`。

`HandleRequestVote()`：

- 小 term 拒绝。
- 大 term 先转 follower。
- 检查是否已经投过票。
- 检查 candidate 日志是否至少一样新。
- 满足条件才投票，并重置选举计时。

### BecomeLeader

leader 当选后：

- 设置 `role_ = Leader`。
- `leader_id_ = id_`。
- 追加当前 term 的 no-op 日志。
- 初始化每个 peer 的 `next_index_` 和 `match_index_`。
- 尝试推进 commit。
- 发送心跳。

no-op 的作用是建立当前 term 的提交屏障，让后续线性一致读和旧日志提交更安全。

## 11. 日志复制和冲突修复

Raft 复制依赖 `(prev_log_index, prev_log_term)`：

- follower 如果没有 `prev_log_index`，说明日志太短，返回冲突信息。
- follower 如果该 index 的 term 不一致，说明日志分叉，返回冲突 term 和冲突 index。
- leader 根据冲突信息调整 `next_index_`，然后重试。

当前实现中的优化点：

- follower 返回 `conflict_term` 和 `conflict_index`。
- leader 如果本地有该 `conflict_term`，可以跳到该 term 最后一个 index 后面。
- 如果不匹配，就回退到 follower 给出的 conflict index。
- 如果回退到快照之前，改发 InstallSnapshot。

这个比简单 `next_index--` 更快。

## 12. 提交规则

`AdvanceCommitIndex()` 有一个重要判断：

```cpp
if (TermAt(candidate) != current_term_) {
  continue;
}
```

意思是 leader 只能通过多数派复制直接提交当前 term 的日志，不能直接用计数提交旧 term 日志。旧 term 日志可以随着当前 term 新日志一起间接提交。

这是 Raft 的安全性关键点。面试中如果被问“为什么不能直接提交旧 term 日志”，要能举例说明：旧 leader 的日志可能被新 leader 覆盖，只有当前 term 日志达到多数派，才能建立 leader completeness。

## 13. 状态机 apply

Raft 的原则是：

- 只有 committed log 才能 apply。
- apply 必须按日志 index 顺序。
- 同一份日志序列应用到确定性状态机，所有节点得到同样状态。

当前 `ApplyCommand()` 支持：

- `Noop`：不改变状态。
- `Put`：写 KV。
- `AddPeer` / `RemovePeer`：改节点成员配置。
- `AcquireLock` / `ReleaseLock`：改锁 owner。
- `MvccCommit`：写 MVCC 版本数据和版本索引。

## 14. Snapshot 和 InstallSnapshot

### 为什么需要快照

如果日志无限增长：

- 重启恢复要 replay 很多日志。
- 新 follower 追赶很慢。
- 磁盘占用不可控。

快照把已经应用的状态机压缩成一个状态点，然后丢弃旧日志。

### 当前触发条件

`kSnapshotThreshold = 4`，即提交日志超过阈值后触发 `MaybeTakeSnapshot()`。这个阈值明显是测试友好值，生产环境会大很多，通常按日志大小、条数或时间综合触发。

### 快照内容

`SnapshotState` 包含：

- `last_included_index`
- `last_included_term`
- 状态机全部 key-value 数据
- 当前 peers 配置

### InstallSnapshot

如果 leader 发现 follower 的 `next_index_` 已经小于等于快照 index，就不能再用日志补齐，只能发送 snapshot。

follower 收到后：

- 替换状态机内容。
- 替换 peer 配置。
- 调整 log、`log_base_index_`、`commit_index_`、`last_applied_`。
- 保存 snapshot 和持久化状态。

## 15. RocksDB 存储

`IKeyValueStore` 抽象了状态机存储。当前有两个实现：

- `InMemoryKeyValueStore`：适合测试和单进程演示。
- `RocksDbKeyValueStore`：适合本地持久化验证。

RocksDB 配置包括：

- write buffer size
- background jobs
- write buffer number
- max open files
- compaction style
- bloom filter
- compression
- WAL 开关

注意，Raft 日志和状态机存储不是同一回事：

- 状态机数据放在 `store_`。
- Raft 元数据通过 `IRaftPersistence` 保存。
- snapshot 里保存状态机和 peer 配置。

## 16. 锁服务

锁服务是状态机命令的一种。

### AcquireLock

`AcquireLock(name, owner)`：

- 生成 `CommandType::AcquireLock`。
- 复制提交后在 `ApplyCommand()` 中执行。
- 如果当前没有 owner，或者 owner 等于当前请求 owner，则成功。
- 成功后写入 `__lock__/owner/<name>`。

### ReleaseLock

`ReleaseLock(name, owner)`：

- 只有当前 owner 匹配时才能释放。
- 当前实现释放时把 owner 写成空字符串。

### 幂等性

同一个 owner 重复 acquire 同一把锁会成功，因此 acquire 对同 owner 是幂等的。release 在第一次成功后锁被释放，再重复 release 会因为 owner 不存在而失败，所以 release 不是严格幂等接口。

当前没有租约、TTL、会话心跳和 fencing token，所以不能作为完整生产分布式锁使用。面试时要主动说清楚。

## 17. MVCC

MVCC 相关接口：

- `BeginMvccTransaction()`
- `StageMvccWrite(tx_id, key, value)`
- `CommitMvccTransaction(tx_id)`
- `ReadMvcc(key, snapshot_ts)`

### Begin

leader 先做线性一致读确认，然后拿当前最新 timestamp 作为事务快照版本。

### Stage

写入先暂存在 leader 内存的 `pending_mvcc_transactions_`，还没有进入 Raft 日志。

### Commit

提交时检查每个 key 的最新版本是否大于事务开始时的 snapshot ts。如果大于，说明有并发写入，返回写写冲突。

没有冲突则生成 `CommandType::MvccCommit`，通过 Raft 复制提交。真正持久化发生在 apply 阶段。

### Read

读取某个 key 时，扫描版本索引，找不大于 `snapshot_ts` 的最新版本。

### 边界

当前 MVCC 更像演示性事务能力：

- 只处理写写冲突。
- 没有 SQL 层事务语义。
- pending transaction 存在 leader 内存，leader 切换会丢。
- 没有事务超时清理。
- 没有范围查询和完整隔离级别。

## 18. 成员变更

HTTP 接口：

- `POST /admin/add-peer`
- `POST /admin/remove-peer`

内部变成：

- `CommandType::AddPeer`
- `CommandType::RemovePeer`

提交后在状态机 apply 阶段更新 `peers_` 和 `transport_`。

重要边界：当前是直接改配置，不是 joint consensus。生产级 Raft 成员变更需要新旧配置联合多数派阶段，避免配置切换期间出现两个多数派。

面试里可以说：

> 当前项目实现了成员变更命令复制和 apply 的基本链路，但还没有做 joint consensus，因此只能作为接口和思路展示。生产环境必须补联合共识，保证变更过程中不会破坏 quorum 安全。

## 19. HTTP 接口速查

### KV

```bash
curl -X PUT http://127.0.0.1:9201/kv/name -d 'alice'
curl http://127.0.0.1:9201/kv/name
```

### 状态

```bash
curl http://127.0.0.1:9201/status
curl http://127.0.0.1:9201/metrics
```

### 锁

```bash
curl -X POST http://127.0.0.1:9201/lock/acquire -d 'name=job&owner=node-a'
curl http://127.0.0.1:9201/lock/job
curl -X POST http://127.0.0.1:9201/lock/release -d 'name=job&owner=node-a'
```

### MVCC

```bash
curl -X POST http://127.0.0.1:9201/mvcc/begin
curl -X PUT http://127.0.0.1:9201/mvcc/tx/<tx_id>/kv/account -d '100'
curl -X POST http://127.0.0.1:9201/mvcc/commit -d 'tx_id=<tx_id>'
curl 'http://127.0.0.1:9201/mvcc/kv/account?snapshot_ts=<ts>'
```

### 成员变更

```bash
curl -X POST http://127.0.0.1:9201/admin/add-peer \
  -d 'id=4&host=127.0.0.1&raft_port=9104&http_port=9204'

curl -X POST http://127.0.0.1:9201/admin/remove-peer -d 'id=4'
```

## 20. 测试怎么读

建议按这个顺序看测试：

1. `raft_smoke_test.cpp`：基本选举、复制、提交。
2. `raft_conflict_test.cpp`：日志冲突覆盖。
3. `raft_prevote_test.cpp`：Pre-Vote 行为。
4. `raft_linearizable_read_test.cpp`：读多数派确认。
5. `raft_snapshot_test.cpp`：快照和 InstallSnapshot。
6. `raft_persistence_test.cpp`：持久化恢复。
7. `raft_lock_test.cpp`：锁状态机。
8. `raft_mvcc_test.cpp`：MVCC begin/write/commit/read。
9. `raft_membership_test.cpp`：成员变更。

测试是最好的学习入口，因为它们把预期行为写得比主流程更集中。

## 21. 推荐读代码路线

### 第一遍：只看主流程

1. `include/core/raft_types.h`
2. `include/core/raft_node.h`
3. `src/core/raft_node.cpp` 中的 `Tick()`、`StartElection()`、`BecomeLeader()`
4. `ReplicateCommandLocked()`
5. `ReplicateToPeer()`
6. `AdvanceCommitIndex()`
7. `ApplyCommittedEntries()` 和 `ApplyCommand()`

目标是能讲出写请求怎么复制提交。

### 第二遍：看读一致性和边界

1. `ConfirmLeaderForReadLocked()`
2. `HasCommittedCurrentTermEntry()`
3. `HandleGet()`
4. follower redirect 逻辑

目标是能解释为什么读也需要多数派确认。

### 第三遍：看异常和修复

1. `HandleAppendEntries()`
2. `FindFirstIndexOfTerm()`
3. `FindLastIndexOfTerm()`
4. `SendSnapshotToPeer()`
5. `HandleInstallSnapshot()`

目标是能解释 follower 日志不一致和严重落后时怎么追平。

### 第四遍：看扩展能力

1. 锁：`AcquireLock()`、`ReleaseLock()`、`LockOwnerUnlocked()`
2. MVCC：`BeginMvccTransaction()`、`StageMvccWrite()`、`CommitMvccTransaction()`
3. 成员变更：`AddPeer()`、`RemovePeer()`、`ApplyCommand()`
4. HTTP：`NodeHttpServer::HandleRequest()`
5. 存储：`RocksDbKeyValueStore`

## 22. 学习重点排序

### 第一优先级

- Raft 选举。
- 日志复制。
- commit index 推进。
- apply 状态机。
- leader 线性一致读。
- 快照压缩。

这些是项目最核心的面试点。

### 第二优先级

- TCP + Protobuf transport。
- RocksDB 持久化。
- HTTP API 和 redirect。
- metrics。
- 本地多进程脚本。

这些体现工程完整度。

### 第三优先级

- 锁服务。
- MVCC。
- 成员变更。

这些是加分项，但要清楚边界。

## 23. 面试讲法模板

### 30 秒版本

> 我做了一个 C++17 的分布式 KV，核心是 Raft。写请求只能由 leader 处理，leader 追加日志并通过 AppendEntries 复制到 follower，多数派确认后推进 commit index，再按顺序 apply 到 KV 状态机。为了提高完整度，我还做了 Pre-Vote、当前 term no-op、线性一致读、快照压缩、InstallSnapshot、RocksDB 持久化、多进程 TCP 通信、HTTP 接口、锁和 MVCC。

### 2 分钟版本

> 项目分为 HTTP 接入层、Raft 共识层、网络 RPC 层和状态机存储层。HTTP 层负责把 KV、锁、MVCC、成员变更请求转换成状态机命令；RaftNode 负责选举、日志复制、提交和 apply；网络层用 TCP + Protobuf 传输 Raft RPC；存储层用 RocksDB 持久化状态机和 Raft 元数据。写路径会经过 leader 追加日志、复制、多数派提交、状态机 apply。读路径不是简单读本地，而是 leader 先向多数派确认自己仍然有效，再读本地状态机。项目也实现了 snapshot 和 InstallSnapshot，避免日志无限增长。生产边界上，成员变更还不是 joint consensus，HTTP 和安全治理也不是生产级。

## 24. 常见误区

- 不要说“每个节点都能写”。只有 leader 能处理写，follower redirect。
- 不要说“读不需要 Raft”。线性一致读需要 leader 确认多数派。
- 不要把 `commit_index_` 和 `last_applied_` 混为一谈。
- 不要说 RocksDB 自动解决分布式一致性。RocksDB 只负责单节点存储。
- 不要把当前成员变更说成生产级 joint consensus。
- 不要把锁服务说成完整生产分布式锁。当前没有租约、TTL 和 fencing token。
- 不要把 MVCC 说成完整数据库事务。当前只是基础多版本读和写写冲突检测。

## 25. 可以继续增强的方向

如果后续要继续做，优先级建议：

1. 实现 joint consensus 成员变更。
2. 增加 request id 去重表，让客户端重试具备更强幂等语义。
3. 给锁增加 lease、TTL、续约和 fencing token。
4. MVCC pending transaction 持久化或 leader 切换恢复。
5. 使用成熟 HTTP 框架或 gRPC 管理 API。
6. 加 TLS、认证、鉴权和限流。
7. 加故障注入测试：kill leader、网络隔离、磁盘错误、慢 follower。
8. 完善 Prometheus metrics 和 dashboard。
9. 做跨机器部署文档和数据备份恢复方案。
10. 支持 range scan、delete、batch write、compare-and-swap。

## 26. 你现在怎么学最高效

不用一行行读完整项目。更好的方式是按链路读：

1. 先用脚本跑三节点集群，确认自己知道怎么操作。
2. 把一次 `PUT /kv/a` 的路径画出来。
3. 对照代码读 `HandlePut()` 到 `ApplyCommand()`。
4. 把一次 `GET /kv/a` 的路径画出来。
5. 对照代码读 `ConfirmLeaderForReadLocked()`。
6. 再集中看选举、复制、快照三个 Raft 难点。
7. 最后看锁、MVCC、成员变更这些扩展功能。

面试准备时，目标不是背代码，而是做到“问到一个行为，能指出大概在哪个文件、哪个函数、为什么这样做、边界是什么”。
