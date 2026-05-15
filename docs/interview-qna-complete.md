# distributed-kv 面试问题与完整回答

这份文档按面试追问顺序组织。建议不要死背，而是掌握每个问题背后的判断点：面试官想确认你是不是真的理解 Raft、一致性读、状态机、持久化和生产边界。

## 一、项目整体

### 1. 这个项目是什么？

这是一个 C++17 实现的分布式 KV 存储系统，核心是 Raft 共识。客户端写请求进入 leader 后会被包装成日志，通过 AppendEntries 复制到 follower，多数派确认后推进 commit index，再按顺序应用到 KV 状态机。项目还实现了 Pre-Vote、线性一致读、snapshot、InstallSnapshot、RocksDB 持久化、多进程 TCP 通信、HTTP API、锁服务、MVCC 和成员变更接口。

### 2. 你为什么做这个项目？

主要是为了系统性展示分布式一致性和 C++ 工程能力。Raft 能覆盖选举、复制、日志一致性、状态机、持久化、快照和故障恢复等核心分布式问题；C++ 实现又能体现网络、存储、并发和工程组织能力。相比只写一个单机 KV，这个项目更能证明我理解“数据为什么在多个节点上保持一致”。

### 3. 项目的核心难点是什么？

核心难点有四个：

1. Raft 日志安全：保证不同节点不会提交不同命令。
2. leader 有效性：读写都不能由过期 leader 返回错误结果。
3. 持久化和快照：节点重启、日志压缩、落后 follower 追赶都要保持一致。
4. 工程联调：多进程 TCP RPC、HTTP API、RocksDB、测试脚本要组合起来跑通。

### 4. 项目分几层？

可以分为五层：

- HTTP 接入层：解析客户端请求，做 leader redirect，返回状态码。
- Raft 共识层：选举、复制、提交、apply、snapshot。
- 网络 RPC 层：TCP + Protobuf 传输 Raft RPC。
- 状态机层：KV、锁、MVCC、成员配置。
- 存储层：内存存储和 RocksDB 持久化。

### 5. 一次写请求怎么走？

以 `PUT /kv/name` 为例：

1. HTTP 层进入 `HandlePut()`。
2. 如果当前节点不是 leader，返回 307 redirect 或 503。
3. leader 调用 `RaftNode::Propose()`。
4. `Propose()` 构造 `CommandType::Put`。
5. `ReplicateCommandLocked()` 追加本地日志并持久化。
6. leader 向 follower 发送 AppendEntries。
7. follower 校验 `prev_log_index` 和 `prev_log_term`，匹配后追加日志。
8. leader 发现多数派复制成功后推进 `commit_index_`。
9. `ApplyCommittedEntries()` 按顺序执行 `ApplyCommand()`。
10. `Put` 命令最终写入状态机 store。

### 6. 一次读请求怎么走？

以 `GET /kv/name` 为例：

1. HTTP 层进入 `HandleGet()`。
2. follower 返回 redirect 或 503。
3. leader 调用 `ConfirmLeaderForRead()`。
4. leader 确认当前 term 有提交日志；如果没有，先发心跳尝试提交 no-op。
5. leader 向多数派发送心跳确认自己仍然有效。
6. 确认成功后，如果状态机未追上 commit index，先 apply。
7. 从本地状态机读取 key。

### 7. 为什么写必须走 leader？

Raft 的设计是强 leader 模型。所有写由 leader 统一排序，并以日志 index 的形式复制给 follower。这样可以保证所有节点以同样顺序应用同样命令。如果多个节点都能随便写，就无法保证全局顺序，也无法通过 Raft 的日志匹配属性维护一致性。

### 8. follower 收到写请求怎么办？

follower 不直接处理写。如果它知道 leader，会通过 HTTP 307 redirect 把客户端引导到 leader；如果不知道 leader，则返回 503。这样能避免 follower 本地写破坏一致性。

### 9. 这个项目和 etcd 有什么区别？

相同点是都基于 Raft，都把写操作复制成日志并应用到状态机，也都关注线性一致读、快照和成员变更。区别是 etcd 是成熟生产系统，有完整的 gRPC API、watch、lease、auth、TLS、joint consensus、压缩、运维工具、故障恢复和长期验证。本项目是学习和面试展示级实现，覆盖核心 Raft 链路，但生产化能力还不完整。

### 10. 这个项目是不是生产级？

不能说是完整生产级。它实现了核心 Raft 和本地多进程验证，但生产环境还需要 joint consensus、认证鉴权、TLS、限流、监控告警、备份恢复、跨机器部署、磁盘故障处理、数据校验、故障注入和更完整的运维能力。面试中我会主动承认这个边界。

## 二、Raft 基础

### 11. Raft 解决什么问题？

Raft 解决的是复制状态机一致性问题。多个节点从同一条日志序列开始，按相同顺序应用确定性命令，就能得到相同状态。Raft 负责在故障、网络延迟和节点重启情况下，仍然选出 leader、复制日志、提交日志，并保证已经提交的日志不会丢失或被覆盖。

### 12. Raft 有哪些角色？

有 follower、candidate、leader。follower 被动接收 RPC；candidate 在选举超时后发起投票；leader 处理客户端请求并复制日志。任意节点发现更高 term 都要退回 follower。

### 13. term 有什么作用？

term 是 Raft 的逻辑时钟。它用于识别过期 leader、约束投票、标记日志产生时期、处理 RPC 新旧关系。节点收到更高 term 的 RPC 或响应时必须更新 term 并转为 follower。

### 14. voted_for 为什么要持久化？

一个节点在同一 term 内最多只能投一票。如果 `voted_for` 不持久化，节点宕机重启后可能在同一 term 给多个 candidate 投票，导致一个 term 内出现多个 leader，破坏安全性。

### 15. 日志为什么要带 term？

日志 term 用来判断日志是否匹配以及处理冲突。Raft 的日志匹配属性是：如果两个日志在同一 index 上 term 相同，那么该 index 之前的日志也相同。AppendEntries 通过 `prev_log_index` 和 `prev_log_term` 验证这一点。

### 16. 选举为什么需要随机超时？

如果所有 follower 同时超时，就会同时变成 candidate 并互相抢票，造成 split vote。随机化选举超时可以让某个节点更早发起选举，更容易拿到多数派。

### 17. 什么是多数派？

多数派是超过半数节点。对于 N 个节点，多数派是 `N / 2 + 1`。任意两个多数派必然有交集，所以只要一条日志被多数派确认，未来 leader 的选举也会与这个多数派相交，从而保护已提交日志。

### 18. 为什么 Raft 需要 leader？

leader 负责统一排序客户端命令。这样一致性问题从“多个节点并发排序”变成“leader 排序后复制”，理解和实现都更简单。Raft 的一个重要目标就是比 Paxos 更容易理解。

## 三、选举和 Pre-Vote

### 19. 当前项目的选举流程是什么？

节点在 `Tick()` 中累计选举时间。非 leader 超过随机选举超时后先 `StartPreVote()`。如果预投票拿到多数派，再 `StartElection()`：增加 term、投票给自己、持久化状态、发送 RequestVote。拿到多数派正式投票后调用 `BecomeLeader()`。

### 20. RequestVote 会检查什么？

检查三件事：

1. 请求 term 是否小于当前 term，小于则拒绝。
2. 当前 term 是否已经投给别人。
3. candidate 日志是否至少和自己一样新。

只有没投过票或投给同一个 candidate，并且 candidate 日志足够新，才会投票。

### 21. 怎么判断 candidate 日志是否足够新？

先比较最后一条日志的 term，term 大的更新；如果 term 相同，再比较最后日志 index，index 大的更新。代码对应 `IsCandidateLogUpToDate()`。

### 22. Pre-Vote 是什么？

Pre-Vote 是正式选举前的预投票。节点先询问其他节点：“如果我用下一个 term 参选，你会不会投我？”只有拿到多数派预投票，才真正增加 term 发起 RequestVote。

### 23. Pre-Vote 解决什么问题？

它减少网络分区恢复时的 term 抖动。比如一个隔离节点长期收不到心跳，如果没有 Pre-Vote，它会不断增加 term。网络恢复后，它的高 term 会迫使正常 leader 退位，造成无意义的可用性抖动。有 Pre-Vote 后，它拿不到多数派就不会增加 term。

### 24. 当前 HandlePreVote 为什么会看最近 leader 联系？

如果 follower 最近刚收到 leader 心跳，说明当前 leader 可能仍然有效。此时拒绝 PreVote 可以降低不必要选举，保持集群稳定。

### 25. leader 当选后为什么追加 no-op？

leader 当选后追加当前 term 的 no-op 日志，是为了尽快提交一条当前 term 日志。Raft 规定 leader 只能通过多数派复制直接提交当前 term 的日志。提交了当前 term 日志后，leader 可以安全地知道之前日志也一起提交，并且线性一致读可以基于当前 term 提交屏障。

## 四、日志复制

### 26. AppendEntries 有什么作用？

AppendEntries 同时用于心跳和日志复制。没有 entries 时是心跳；有 entries 时是复制日志。它携带 leader term、leader id、前一条日志 index/term、要追加的日志 entries 和 leader commit index。

### 27. follower 收到 AppendEntries 怎么处理？

先检查 term，小 term 拒绝，大 term 转 follower。然后检查 `prev_log_index` 是否存在、`prev_log_term` 是否匹配。不匹配则返回冲突信息；匹配则追加新日志，如发现当前位置已有不同 term 的日志，会截断后面的日志再追加。最后根据 leader commit 推进本地 commit index 并 apply。

### 28. 日志冲突怎么修复？

follower 返回 `conflict_term` 和 `conflict_index`。leader 根据这个信息调整该 follower 的 `next_index_`。如果 leader 有相同 conflict term，就跳到该 term 最后一条日志之后；否则跳到 follower 给的 conflict index。然后重新发送 AppendEntries。这样比每次只回退一个 index 更快。

### 29. 什么情况下发送 snapshot？

如果 follower 落后太多，它需要的日志 index 已经被 leader 压缩进 snapshot 了，此时 leader 无法再发送旧日志，只能发送 InstallSnapshot。代码中判断是 `next_index_[peer_id] <= snapshot_state_.last_included_index`。

### 30. next_index 和 match_index 区别是什么？

`next_index` 是 leader 下次要发给某个 follower 的日志 index；`match_index` 是 leader 已确认该 follower 复制成功的最大日志 index。`next_index` 用于复制过程，`match_index` 用于计算多数派提交。

### 31. 为什么 follower 要截断冲突日志？

因为 follower 上冲突位置之后的日志可能来自旧 leader，不能再保留。Raft 保证 follower 会接受 leader 的日志作为权威日志，冲突位置及之后都要被 leader 日志覆盖。

### 32. AppendEntries 成功是否代表日志已经提交？

不一定。AppendEntries 成功只说明某个 follower 接收了日志。只有 leader 确认某条日志被多数派复制，并满足当前 term 提交规则，才能推进 commit index。

## 五、提交和状态机

### 33. commit_index 是什么？

`commit_index` 是 Raft 层确认已经提交的最大日志 index。提交意味着这条日志已经安全，不会被未来 leader 覆盖，可以应用到状态机。

### 34. last_applied 是什么？

`last_applied` 是状态机已经执行到的最大日志 index。它可能落后于 `commit_index`，但不能超过 `commit_index`。状态机会按顺序 apply 日志，保证所有节点状态一致。

### 35. 为什么 commit 和 apply 要分开？

commit 是共识层概念，表示日志已经被多数派确认；apply 是状态机执行概念，表示命令已经改变本地状态。分开可以让 Raft 层先确定安全性，再由状态机按顺序执行。

### 36. 为什么只能直接提交当前 term 日志？

这是 Raft 安全性要求。旧 term 日志即使在多数节点上存在，也可能在某些选举场景下被覆盖。leader 只有通过复制并提交当前 term 日志，才能保证自己拥有足够新的日志，从而间接保证旧日志安全。

### 37. ApplyCommand 做了什么？

根据命令类型修改状态机：

- `Put` 写入 KV。
- `AcquireLock` 设置锁 owner。
- `ReleaseLock` 清空锁 owner。
- `MvccCommit` 写版本数据和版本索引。
- `AddPeer` / `RemovePeer` 更新成员和 transport。
- `Noop` 不做业务修改。

### 38. 状态机为什么必须确定性？

因为 Raft 只保证每个节点应用相同日志序列。如果状态机执行同一命令却产生不同结果，节点状态仍然会分叉。因此状态机逻辑不能依赖本地随机数、当前时间或不可控外部副作用，除非这些值已经写入日志。

## 六、线性一致读

### 39. 为什么读也有一致性问题？

如果旧 leader 被网络隔离，它本地状态可能落后，但还没意识到自己失去 leader 身份。如果直接读本地，就可能返回旧数据，违反线性一致性。

### 40. 当前项目怎么做线性一致读？

leader 在读前调用 `ConfirmLeaderForReadLocked()`：

1. 确认自己是 leader。
2. 确认当前 term 有已提交日志，没有则发送心跳推动 no-op。
3. 向多数派发送 AppendEntries 心跳确认 leader 身份。
4. 多数派确认后再读本地状态机。

### 41. 这和 ReadIndex 有什么关系？

思想类似 ReadIndex：读不进入日志，但要通过多数派确认 leader 仍有效，并确保状态机 apply 到足够新的位置。当前项目是简化版，没有实现完整的 ReadIndex 请求上下文队列。

### 42. 为什么读前要确认当前 term 有提交日志？

leader 刚当选时可能还不知道哪些旧日志安全。提交一条当前 term 日志后，可以建立当前 leader 的提交屏障，保证后续读基于安全的 commit index。

### 43. 为什么不把每次读也写成日志？

把读写成日志可以保证线性一致，但性能较差，因为每次读都要走复制和提交。ReadIndex 风格读通过多数派心跳确认 leader 有效，避免把读写入日志，是性能和一致性的折中。

### 44. follower 能不能做读？

当前项目不让 follower 直接读，follower 会 redirect 到 leader。生产系统可以支持 follower stale read 或 lease read，但要明确一致性级别。当前项目主打线性一致读，所以走 leader。

## 七、快照和持久化

### 45. 为什么需要 snapshot？

Raft 日志会持续增长。如果不压缩，重启 replay 成本、磁盘占用、落后 follower 追赶成本都会越来越高。snapshot 把某个 index 前的状态机压缩成状态点，旧日志可以删除。

### 46. snapshot 包含什么？

当前包含：

- `last_included_index`
- `last_included_term`
- 状态机 key-value 数据
- peers 配置

这些足够让节点恢复到某个已提交状态。

### 47. log_base_index 是什么？

快照后 `log_` 不再从全局 index 0 开始。`log_base_index_` 表示当前 `log_[0]` 对应的全局 index。访问日志时要用 `ToLocalIndex(global_index)` 转换。

### 48. InstallSnapshot 怎么处理？

follower 收到 snapshot 后替换本地状态机、更新 peers、调整日志、设置 `log_base_index_`、`commit_index_` 和 `last_applied_`，然后持久化 snapshot 和 Raft 状态。

### 49. current_term、voted_for、log 为什么要持久化？

这些是 Raft 安全性状态。丢失后可能导致重复投票、日志回退、错误选举等问题。持久化能保证节点重启后不会违反 Raft 的基本约束。

### 50. RocksDB 在项目中扮演什么角色？

RocksDB 是单节点本地持久化引擎。它保存状态机 KV 数据，也用于 Raft 元数据持久化实现。它不负责分布式一致性，一致性由 Raft 保证。

## 八、锁和 MVCC

### 51. 锁服务怎么实现？

锁是状态机命令。`AcquireLock` 和 `ReleaseLock` 都要经过 Raft 复制提交。apply 时用内部 key `__lock__/owner/<name>` 保存 owner。这样所有节点看到的锁 owner 一致。

### 52. 当前锁是生产级分布式锁吗？

不是。它没有 lease、TTL、自动过期、续约、fencing token 和客户端会话。节点或客户端异常时，锁可能需要人工释放。它适合展示“锁状态通过 Raft 保持一致”，不适合直接说成生产级锁服务。

### 53. acquire lock 是否幂等？

对同一个 owner 重复 acquire 同一把锁是幂等的，因为当前 owner 等于请求 owner 时仍返回成功。对不同 owner 会返回冲突。

### 54. release lock 是否幂等？

不是严格幂等。第一次 release 成功后 owner 被清空，再次 release 会因为 owner 不匹配而失败。

### 55. MVCC 怎么实现？

Begin 时拿当前最新版本作为 snapshot ts；Stage 时把写集合暂存在 leader 内存；Commit 时检查写集合中每个 key 是否有大于 snapshot ts 的新版本，如果有则写写冲突；没有冲突就把 `MvccCommit` 命令复制到 Raft 日志，提交后写入版本数据和版本索引。

### 56. MVCC 读怎么找版本？

每个 key 有一个版本索引，记录 commit ts 列表。读取时从后往前找不大于 snapshot ts 的最新版本，再读取对应数据 key。

### 57. 当前 MVCC 有什么边界？

它不是完整事务数据库。它只做基础多版本读和写写冲突检测；pending transaction 在 leader 内存中，leader 切换会丢；没有事务超时清理、范围查询、SQL 隔离级别和持久化事务状态。

## 九、成员变更

### 58. 成员变更怎么做？

HTTP admin 请求生成 `AddPeer` 或 `RemovePeer` 命令，经过 Raft 复制提交后，在 `ApplyCommand()` 中更新 `peers_` 和 transport peer 列表。

### 59. 当前成员变更是否安全？

它实现了成员变更的基本链路，但不是完整生产安全版本。生产级 Raft 需要 joint consensus，先进入新旧配置联合多数派，再切到新配置。当前直接改配置，在复杂故障时可能破坏 quorum 安全。

### 60. 什么是 joint consensus？

joint consensus 是 Raft 处理成员变更的安全方案。配置变更分两阶段：先提交包含旧配置和新配置的联合配置，提交需要同时满足旧多数派和新多数派；再提交纯新配置。这样避免变更过程中出现两个不相交多数派。

## 十、网络和 HTTP

### 61. 为什么要做 TCP + Protobuf？

单进程内存调用只能验证算法，不能展示真实部署中的序列化、网络连接、独立进程和端口。TCP + Protobuf 让每个节点作为独立进程通信，更接近真实分布式系统。

### 62. HTTP 层有什么作用？

HTTP 层给客户端提供可操作接口，包括 KV、锁、MVCC、成员变更、状态和 metrics。它把外部请求转换成 RaftNode 方法调用，并处理 leader redirect、状态码和输入校验。

### 63. 为什么 follower 返回 307？

307 Temporary Redirect 能保留原 HTTP 方法和请求体，比 302 更适合 PUT/POST 这类写请求。客户端可以按 Location 继续访问 leader。

### 64. `/metrics` 暴露了什么？

包括 Raft term、commit index、log base index、角色、RPC 计数、线性一致读成功/失败、客户端写、锁、MVCC、commit/apply 计数、HTTP 请求数和平均 QPS 等。

## 十一、测试和验证

### 65. 你怎么验证 Raft 正确性？

通过单元测试覆盖选举、复制、冲突覆盖、PreVote、线性一致读、快照、持久化、锁、MVCC 和成员变更。再通过本地多进程脚本验证真实 TCP 节点的启动、leader 查询、KV 写读和接口路径。

### 66. 如果 leader 挂了会怎样？

follower 收不到心跳后触发 PreVote 和正式选举。新 leader 当选后继续处理写请求。旧 leader 如果恢复并收到更高 term，会转为 follower。

### 67. 如果 follower 落后很多怎么办？

leader 先尝试 AppendEntries。如果 follower 需要的日志已经被快照压缩，leader 会发送 InstallSnapshot，让 follower 直接安装状态机快照，然后从快照之后继续复制日志。

### 68. 如果网络分区怎么办？

多数派分区可以选出 leader 并继续服务；少数派分区无法获得多数派，不能提交写，也不能通过线性一致读确认。旧 leader 如果失去多数派，读写都应该失败或降级。

### 69. 如何证明写不会丢？

一条写只有被多数派复制后才提交。未来任何 leader 也必须由多数派选出。两个多数派有交集，且 RequestVote 会检查日志新旧，因此已提交日志会保留在未来 leader 的日志中。

### 70. 如何证明不会乱序 apply？

状态机只从 `last_applied_ + 1` 循环 apply 到 `commit_index_`，严格按日志 index 顺序执行。所有节点在相同日志序列上按相同顺序执行确定性命令，因此状态一致。

## 十二、生产化追问

### 71. 多实例部署要注意什么？

每个节点要有独立数据目录、独立 Raft 端口和 HTTP 端口。跨机器部署时要处理网络地址、磁盘持久化、进程守护、日志收集、监控告警、节点替换、数据备份和安全认证。当前脚本主要用于单机多进程演示。

### 72. 机器挂了怎么办？

如果挂掉的是少数节点，剩余多数派可以继续选举并服务；挂掉节点恢复后通过 AppendEntries 或 InstallSnapshot 追赶。如果多数派不可用，系统不能保证强一致写入，需要拒绝写请求。

### 73. 磁盘损坏怎么办？

当前项目没有完整的数据校验和自动恢复流程。生产系统需要 checksum、备份、快照复制、坏盘替换和节点重建机制。单个节点磁盘损坏时，可以从集群其他节点重新同步，但前提是还有健康多数派。

### 74. 怎么做请求幂等？

可以给客户端请求带全局唯一 request id。leader 把 request id 一起写入日志，状态机维护去重表，重复请求直接返回第一次结果。当前锁和 MVCC 内部有局部 request id 结果记录，但还不是完整客户端幂等机制。

### 75. 怎么提升性能？

可以做批量 AppendEntries、批量 apply、异步磁盘刷写、pipeline replication、读请求 lease 优化、RocksDB 参数调优、连接复用、线程模型优化和减少 HTTP 解析开销。但性能优化必须在正确性之后做。

### 76. 怎么做监控？

核心指标包括 leader 变化次数、term、commit index、apply index、日志复制延迟、follower match index、snapshot 次数、读 quorum 失败、请求延迟、QPS、RocksDB 写延迟、磁盘空间和错误码分布。

### 77. 怎么做安全？

生产中需要 TLS 加密节点间和客户端通信，认证客户端身份，基于角色做鉴权，限制 admin API，增加审计日志，并对外部请求做限流和输入大小限制。

### 78. 这个项目最能体现你的能力在哪里？

最能体现的是我能把 Raft 论文里的选举、日志复制、提交、安全读、快照和持久化落到 C++ 工程代码里，并通过多进程 TCP、HTTP API、RocksDB、测试和脚本形成可运行项目。同时我知道它和生产级系统的差距，不会过度包装。

### 79. 如果只能讲一个亮点，你讲什么？

我会讲“线性一致读和提交安全”。很多 KV demo 只做写复制，但读一致性和当前 term 提交规则容易被忽略。这个项目里 leader 读前会确认多数派，并且 commit 只直接推进当前 term 日志，这体现了对 Raft 安全性的理解。

### 80. 如果面试官质疑项目不够生产怎么办？

我会承认它不是生产级 etcd，然后说明它的目标是实现和验证核心一致性链路。接着主动列出生产化差距：joint consensus、安全、运维、故障注入、备份恢复、租约锁、完整事务和跨机器部署。最后说明如果继续演进，我会优先做 joint consensus、请求幂等、租约锁和故障注入。

## 十三、简历表达

### 推荐写法一

基于 C++17 实现分布式 KV 存储系统，核心采用 Raft 复制状态机模型，完成 leader 选举、日志复制、冲突回退、commit/apply、Pre-Vote、线性一致读、snapshot/log compaction 和 InstallSnapshot。

### 推荐写法二

设计多进程 TCP + Protobuf Raft RPC 通信和 HTTP 接入层，支持 follower leader redirect、KV 读写、锁服务、MVCC 读写、成员变更接口、状态查询和 metrics 暴露，并提供本地 3 节点启动与验证脚本。

### 推荐写法三

接入 RocksDB 持久化状态机数据和 Raft 元数据，补充快照恢复、落后节点追赶、冲突日志覆盖、线性一致读回归测试、MVCC 写写冲突测试和锁状态机测试，使用 CTest/GitHub Actions 做持续验证。

### 面试一句话

> 这个项目的重点不是写了一个 KV API，而是把 KV、锁和 MVCC 都抽象成状态机命令，通过 Raft 日志复制保证多节点按同一顺序执行，从而获得强一致状态。
