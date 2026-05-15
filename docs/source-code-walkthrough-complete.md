# distributed-kv 源码逐文件导读

这份文档用于最后一轮项目冲刺：不是泛泛讲 Raft，而是把代码文件、核心函数、状态变量、请求路径和面试追问对应起来。你不需要逐行背代码，但要做到问到一个行为时，能快速说出它在什么文件、哪个函数、为什么这样写。

## 1. 阅读总路线

推荐顺序：

1. `include/core/raft_types.h`：先看所有数据结构。
2. `include/core/raft_node.h`：看 RaftNode 对外 API 和内部状态。
3. `src/core/raft_node.cpp`：看选举、复制、提交、apply、快照。
4. `src/network/node_http_server.cpp`：看 HTTP API 怎么转成 Raft 命令。
5. `src/network/libuv_http_server.cpp`：看 libuv + llhttp 接入层。
6. `src/network/network_transport.cpp`：看 leader 如何复用 TCP 长连接发 RPC。
7. `src/network/raft_rpc_server.cpp`：看 follower 如何接收长度前缀帧并分发。
8. `src/network/network_codec.cpp`：看 Protobuf 和内部结构如何转换。
9. `src/storage/key_value_store.cpp`：看状态机数据如何落 RocksDB。
10. `tests/unit/*.cpp`：用测试反推行为边界。

## 2. `include/core/raft_types.h`

这个文件定义所有跨模块共享的数据结构。

重点结构：

- `PeerEndpoint`：节点 ID、host、Raft 端口、HTTP 端口。
- `Role`：Follower、Candidate、Leader。
- `CommandType`：状态机命令类型。
- `Command`：Raft 日志真正承载的业务命令。
- `LogEntry`：日志 term + command。
- `RequestVoteRequest/Response`：正式投票。
- `AppendEntriesRequest/Response`：心跳和日志复制。
- `InstallSnapshotRequest/Response`：快照安装。
- `MvccTransaction/MvccCommitResult`：MVCC 事务返回。
- `RaftMetricsSnapshot`：指标快照。

面试追问：

- 为什么日志必须带 term？
- 为什么 Command 要覆盖 KV、锁、MVCC、成员变更？
- 为什么 PeerEndpoint 里同时有 raft_port 和 http_port？

回答要点：

- term 用于日志匹配、冲突修复和提交安全。
- Raft 复制的是状态机命令，不是只复制 KV 字符串。
- Raft RPC 和客户端 HTTP 是两类通信端口，不能混在一起。

## 3. `include/core/raft_node.h`

这个文件是 RaftNode 的地图。

### 对外 API

- `Tick()`：驱动选举和心跳。
- `Propose()`：KV 写入。
- `ConfirmLeaderForRead()`：线性一致读前确认。
- `AcquireLock()/ReleaseLock()`：锁状态机入口。
- `BeginMvccTransaction()/StageMvccWrite()/CommitMvccTransaction()/ReadMvcc()`：MVCC 入口。
- `HandlePreVote()/HandleRequestVote()/HandleAppendEntries()/HandleInstallSnapshot()`：Raft RPC handler。
- `metrics()`：指标导出。

### 内部核心状态

- `role_`：当前角色。
- `current_term_`：当前任期。
- `voted_for_`：当前 term 投给谁。
- `leader_id_`：当前认为的 leader。
- `log_`：当前节点保留的日志。
- `log_base_index_`：快照后的日志全局起点。
- `commit_index_`：已提交日志最大 index。
- `last_applied_`：已应用到状态机最大 index。
- `next_index_`：leader 认为每个 follower 下一条应接收的 index。
- `match_index_`：leader 确认每个 follower 已复制到的 index。

面试追问：

- `commit_index_` 和 `last_applied_` 有什么区别？
- `next_index_` 和 `match_index_` 有什么区别？
- 为什么有 `log_base_index_`？

## 4. `src/core/raft_node.cpp` 主链路

### `Tick()`

职责：

- leader 到 heartbeat interval 后发心跳。
- follower/candidate 到 election timeout 后发起 PreVote。

关键点：

- 选举超时是随机化的。
- leader 不参与选举超时，只发送心跳。

### `StartPreVote()`

职责：

- 用 `current_term_ + 1` 发预投票。
- 不直接增加当前 term。
- 多数派预投票成功后才 `StartElection()`。

追问：

- 为什么 PreVote 不直接增加 term？
- 隔离节点恢复时 PreVote 有什么价值？

### `StartElection()`

职责：

- 角色变 Candidate。
- `current_term_++`。
- 投票给自己。
- 持久化 term 和 voted_for。
- 发送 RequestVote。
- 多数派成功后 `BecomeLeader()`。

### `HandleRequestVote()`

职责：

- 小 term 拒绝。
- 大 term 转 follower。
- 检查是否已经投过票。
- 检查 candidate 日志是否足够新。
- 同意后持久化 voted_for 并重置选举计时。

关键函数：

- `IsCandidateLogUpToDate()`

### `BecomeLeader()`

职责：

- 设置 leader 身份。
- 追加当前 term no-op 日志。
- 初始化 `next_index_` 和 `match_index_`。
- 尝试提交并发心跳。

追问：

- no-op entry 的作用是什么？
- 为什么当前 term 提交屏障影响线性一致读？

### `ReplicateCommandLocked()`

这是写路径核心。

流程：

1. 检查当前节点必须是 leader。
2. 给需要结果的命令生成 request id。
3. 更新指标。
4. 追加 `LogEntry{current_term_, command}`。
5. `PersistState()`。
6. 更新 leader 自己的 match/next index。
7. 遍历 peers 调 `ReplicateToPeer()`。
8. `AdvanceCommitIndex()`。
9. `SendHeartbeats()`。
10. 如果目标 index 没提交，返回失败。
11. 如果命令需要结果，从 `command_results_` 取 apply 阶段结果。

追问：

- 为什么结果要等 apply 阶段产生？
- 为什么追加日志后要先持久化？
- 为什么 leader 自己也要有 match_index？

### `ReplicateToPeer()`

职责：

- 如果 follower 落后到 snapshot 之前，发 `SendSnapshotToPeer()`。
- 否则构造 AppendEntries。
- 成功后更新 follower 的 match/next。
- 失败后根据 conflict index/term 回退 next index 并重试。

追问：

- 为什么冲突回退比简单 `next_index--` 更快？
- 什么情况下进入 InstallSnapshot？

### `HandleAppendEntries()`

职责：

- 处理 leader 心跳和日志复制。
- 检查 term。
- 校验 `prev_log_index` 和 `prev_log_term`。
- 冲突时返回 conflict 信息。
- 匹配后截断冲突日志并追加 leader 日志。
- 根据 leader commit 推进 follower commit。

关键点：

- follower 以 leader 日志为准。
- 冲突位置及之后日志必须截断。

### `AdvanceCommitIndex()`

职责：

- 从最后日志向前找可提交 index。
- 只允许直接提交当前 term 日志。
- 统计 match_index 达到 candidate 的副本数。
- 多数派成功后更新 commit index 并 apply。

追问：

- 为什么不能直接提交旧 term 日志？
- 多数派交集如何保证已提交日志不丢？

### `ApplyCommittedEntries()` 和 `ApplyCommand()`

职责：

- 按日志 index 顺序把 committed entry 应用到状态机。
- 不允许跳过 index。

命令处理：

- `Noop`：不改变业务状态。
- `Put`：写 KV。
- `AddPeer/RemovePeer`：更新 peers 和 transport。
- `AcquireLock/ReleaseLock`：写锁 owner。
- `MvccCommit`：写 MVCC 版本数据和版本索引。

追问：

- 为什么 apply 必须顺序执行？
- 状态机为什么必须确定性？

### `ConfirmLeaderForReadLocked()`

职责：

- leader 读前确认自己仍有多数派。
- 如果当前 term 还没有提交日志，先发送心跳推动 no-op。
- 向 peers 发 AppendEntries，拿到多数派确认。
- 确保状态机追上 commit index。

追问：

- 为什么旧 leader 直接读会出问题？
- 这个实现和 etcd ReadIndex 有什么异同？

### `MaybeTakeSnapshot()` 和 `HandleInstallSnapshot()`

职责：

- committed 日志超过阈值后保存 snapshot。
- snapshot 包含状态机 entries 和 peers。
- 安装 snapshot 时替换状态机、peers、log base、commit/apply 位置。

追问：

- 为什么 snapshot 中要带 last included term？
- 为什么 snapshot 要带 peers？
- `log_base_index_` 怎么影响日志访问？

## 5. `src/network/node_http_server.cpp`

这个文件是业务 API 到 RaftNode 的转换层。

### 路由职责

- `/status`：节点状态。
- `/metrics`：指标。
- `/kv/<key>`：KV GET/PUT。
- `/lock/acquire`、`/lock/release`、`/lock/<name>`：锁服务。
- `/mvcc/begin`、`/mvcc/tx/<tx_id>/kv/<key>`、`/mvcc/commit`、`/mvcc/kv/<key>`：MVCC。
- `/admin/add-peer`、`/admin/remove-peer`：成员变更。

### leader redirect

follower 收到需要 leader 的请求时：

- 知道 leader：返回 307 Temporary Redirect。
- 不知道 leader：返回 503。

追问：

- 为什么 307 比 302 更合适？
- GET 为什么也要 leader 确认？

## 6. `src/network/libuv_http_server.cpp`

这是最新 HTTP 接入层。

关键对象：

- `HttpConnection`：单连接状态，包含 `uv_tcp_t`、`llhttp_t`、请求、写队列和状态位。
- `HttpServerRuntime`：libuv loop、listen socket、completion async、连接表和 worker executor。

核心流程：

1. `Run()` 初始化 libuv loop、async handle、tcp server。
2. `OnAccept()` 接收连接。
3. `uv_read_start()` 开始读。
4. `llhttp_execute()` 解析请求。
5. `OnMessageComplete()` 暂停 parser。
6. `DispatchRequest()` 把业务 handler 投递到 `TaskExecutor`。
7. worker 完成后通过 `uv_async_send()` 回到 loop 线程。
8. `QueueResponse()` 串行写响应。
9. 根据 keep-alive 决定继续读或关闭。

追问：

- 为什么业务 handler 不直接在 libuv loop 线程执行？
- 为什么完成回调要用 `uv_async_send()` 回到 loop？
- keep-alive 请求怎么避免并发交叉？

回答：

- 避免阻塞事件循环。
- libuv handle 通常由 loop 线程操作，跨线程完成后要切回 loop。
- `request_in_flight` 和 parser pause 保证单连接上一次只处理一个请求。

## 7. `src/network/network_transport.cpp`

这是 Raft RPC client。

关键对象：

- `PendingRpc`：同步等待的 RPC 请求，包含 response、done、condition。
- `PeerConnection`：一个 peer 的 libuv TCP 连接、队列、inflight、read buffer。
- `NetworkTransport::Impl`：独立 libuv loop 线程、任务队列、连接表。

核心流程：

1. RaftNode 调用 transport 发送 RPC。
2. transport 创建 `PendingRpc`。
3. 投递到 libuv loop 线程。
4. 找到或创建 peer 长连接。
5. 如未连接，异步解析 DNS 并 connect。
6. 请求编码为 4 字节长度前缀 + Protobuf payload。
7. 写入 socket。
8. 读到完整响应帧后唤醒等待线程。

追问：

- 为什么要长度前缀？
- 为什么要维护 peer 长连接？
- 同一个 peer 为什么有 queue 和 inflight？

## 8. `src/network/raft_rpc_server.cpp`

这是 Raft RPC server。

流程：

1. libuv listen raft port。
2. accept 后为连接创建 `RpcConnection`。
3. 读 buffer，按 4 字节长度前缀切帧。
4. 每次只 dispatch 一个 request。
5. 业务处理放到 `TaskExecutor`。
6. response 回到 loop 后写回。
7. 写完后继续处理 read buffer 中剩余帧。

追问：

- 为什么 request 处理要放线程池？
- 为什么 `request_in_flight` 为 true 时不继续处理下一帧？

## 9. `src/network/network_codec.cpp`

职责：

- 内部 Raft 结构和 Protobuf message 互转。
- 序列化 RequestVote、AppendEntries、InstallSnapshot。
- 反序列化 response。

追问：

- 为什么不用手写字符串协议？
- Protobuf 的好处是什么？

回答：

- 字段结构清晰，兼容演进更好，避免分隔符解析错误。

## 10. `src/storage/key_value_store.cpp`

`IKeyValueStore` 有内存和 RocksDB 两种实现。

重点：

- `Put/Get/Size/Entries/ReplaceWith` 是状态机接口。
- `Entries()` 用于 snapshot。
- `ReplaceWith()` 用于 InstallSnapshot。
- RocksDB 使用 WriteBatch 替换所有 key。

追问：

- RocksDB 是否保证分布式一致性？
- snapshot 替换状态机为什么要批量写？

回答：

- RocksDB 只保证单机存储，分布式一致性由 Raft 保证。
- 批量替换避免状态机处于半新半旧的中间状态。

## 11. 测试文件对应能力

- `raft_smoke_test.cpp`：基础选举、复制、提交。
- `raft_async_replication_test.cpp`：异步复制行为。
- `raft_conflict_test.cpp`：冲突日志修复。
- `raft_prevote_test.cpp`：预投票。
- `raft_linearizable_read_test.cpp`：线性一致读。
- `raft_snapshot_test.cpp`：快照和 InstallSnapshot。
- `raft_persistence_test.cpp`：持久化恢复。
- `raft_lock_test.cpp`：锁状态机。
- `raft_mvcc_test.cpp`：MVCC。
- `raft_membership_test.cpp`：成员变更。
- `tcp_util_timeout_test.cpp`：TCP 超时工具。

## 12. 面试定位表

| 问题 | 先看文件 | 核心函数 |
| --- | --- | --- |
| leader 怎么选出来 | `src/core/raft_node.cpp` | `Tick`、`StartPreVote`、`StartElection` |
| 为什么投票要比较日志 | `src/core/raft_node.cpp` | `HandleRequestVote`、`IsCandidateLogUpToDate` |
| 写请求怎么提交 | `src/core/raft_node.cpp` | `ReplicateCommandLocked`、`AdvanceCommitIndex` |
| follower 日志冲突怎么办 | `src/core/raft_node.cpp` | `HandleAppendEntries`、`ReplicateToPeer` |
| 读为什么要多数派 | `src/core/raft_node.cpp` | `ConfirmLeaderForReadLocked` |
| 快照怎么安装 | `src/core/raft_node.cpp` | `MaybeTakeSnapshot`、`HandleInstallSnapshot` |
| HTTP 怎么处理 keep-alive | `src/network/libuv_http_server.cpp` | `ProcessInput`、`DispatchRequest`、`FinishRequest` |
| Raft RPC 怎么走网络 | `src/network/network_transport.cpp` | `Submit`、`MaybeConnect`、`MaybeSendNext` |
| RPC server 怎么切帧 | `src/network/raft_rpc_server.cpp` | `ProcessFrames` |
| 状态机怎么落盘 | `src/storage/key_value_store.cpp` | `RocksDbKeyValueStore::Put`、`ReplaceWith` |

## 13. 最后复盘清单

面试前确认自己能不看文档回答：

- term、vote、log index、commit index、apply index 分别是什么。
- leader 当选后为什么追加 no-op。
- 为什么只直接提交当前 term 日志。
- AppendEntries 如何发现并修复冲突。
- ReadIndex 风格读为什么需要多数派确认。
- snapshot 如何改变日志下标。
- libuv HTTP 为什么要把业务处理投递到线程池。
- Protobuf RPC 为什么要加长度前缀。
- RocksDB 和 Raft 的职责边界。
- 成员变更为什么还不是生产级 joint consensus。
