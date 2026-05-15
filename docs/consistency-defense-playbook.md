# distributed-kv 一致性答辩手册

这份文档专门准备 distributed-kv 面试中最难的一类问题：你怎么证明这个 KV 是一致的？Raft 细节为什么这么做？哪些能力只是演示版，哪些能力必须承认边界？

普通项目可以只讲功能，分布式 KV 不行。面试官会追问选举、日志匹配、提交规则、线性一致读、快照、成员变更、锁、MVCC、幂等和故障恢复。回答时要有“安全性逻辑”，不能只背 API。

## 1. 一句话定位

推荐说法：

```text
distributed-kv 是一个基于 C++17 实现的强一致键值存储项目，核心是 Raft 日志复制和确定性状态机。写请求通过 leader 追加日志并复制到多数派，提交后应用到 KV/RocksDB；读请求由 leader 做 ReadIndex 风格多数派确认后读取本地状态机，避免旧 leader 返回过期数据。项目还扩展了 Pre-Vote、snapshot、锁服务、MVCC、多进程 libuv RPC、HTTP 接入和基础 metrics。
```

这句话要突出三点：

- 不是普通 KV，而是 Raft 状态机。
- 写走日志复制，多数派提交。
- 读也考虑线性一致性。

## 2. 面试官最关心的安全性

| 问题 | 核心回答 |
| --- | --- |
| 会不会两个 leader 同时写？ | term、投票持久化、多数派交集保证同一 term 最多一个 leader |
| leader 崩了会不会丢已提交写？ | 已提交日志已经复制到多数派，新 leader 必须拥有足够新的日志 |
| follower 日志冲突怎么办？ | AppendEntries 携带 prev_log_index/term，不匹配就回退并截断冲突 |
| 为什么只能提交当前 term 日志？ | 避免旧 term 日志被错误认为提交，符合 Raft 安全提交规则 |
| 读为什么不能直接本地读？ | 旧 leader 或 follower 可能返回过期状态 |
| snapshot 会不会破坏日志连续性？ | 用 log_base_index / last_included_index 维护压缩后的逻辑索引 |
| 成员变更是否完整安全？ | 当前有接口和基础能力，但生产级需要 joint consensus |

## 3. Leader 选举怎么保证安全

### 3.1 term 的作用

term 是 Raft 的逻辑时钟。每次新一轮选举，term 增加。节点收到更高 term 的 RPC 时必须更新本地 term 并退回 follower。

面试回答：

```text
term 用来区分不同任期的 leader 和日志。任何节点看到更高 term，都说明自己信息落后，必须更新 term 并转成 follower。这样可以避免旧 leader 在新任期继续以 leader 身份写入。
```

### 3.2 voted_for 为什么要持久化

同一 term 内，一个节点最多投一票。如果 `voted_for` 不持久化，节点崩溃重启后可能忘记自己投过票，在同一 term 给多个 candidate 投票，破坏“同一 term 最多一个 leader”的前提。

正确回答：

```text
current_term 和 voted_for 是 Raft 安全性的核心持久化状态。它们必须在回复投票成功前落盘，否则崩溃恢复后可能重复投票。
```

### 3.3 多数派交集

Raft 依赖多数派交集：

```text
5 节点集群，多数派是 3
任意两个多数派集合一定至少有 1 个交集节点
```

一个 candidate 获得多数派投票才能成为 leader。由于每个节点同一 term 只投一票，因此同一 term 不可能有两个不同 candidate 同时获得多数派。

### 3.4 日志新旧检查

投票不只看 term，还要看 candidate 日志是否足够新。比较规则：

```text
last_log_term 大者更新
last_log_term 相等时 last_log_index 大者更新
```

这样能防止日志落后的 candidate 当选，降低已提交日志被覆盖的风险。

## 4. Pre-Vote 的答法

Pre-Vote 不是 Raft 最小版本必须有的能力，但工程上很重要。

场景：

```text
一个 follower 被网络隔离很久
它收不到 leader heartbeat
如果直接增加 term 发起正式选举
恢复网络后可能用更高 term 让健康 leader 退位
造成无意义抖动
```

Pre-Vote 的作用：

```text
节点先不增加 term，只询问其他节点：如果我发起选举，你们会不会投我？
只有预投票能拿到多数派，才进入正式 RequestVote。
```

面试回答：

```text
Pre-Vote 主要降低隔离节点恢复时对正常 leader 的扰动。它不是为了保证 Raft 基本安全性，而是提升可用性和稳定性。
```

## 5. 写请求一致性

写请求链路：

```text
client PUT
  -> HTTP leader
  -> ReplicateCommandLocked
  -> append local log
  -> AppendEntries to followers
  -> majority ack
  -> advance commit_index
  -> ApplyCommittedEntries
  -> apply to KV state machine
  -> response success
```

### 5.1 为什么写必须走 leader

Raft 是强 leader 模型。所有写都由 leader 排序，形成唯一日志序列。follower 如果接受写，会破坏全局顺序。

回答：

```text
写请求必须由 leader 统一排序。follower 收到写请求只能拒绝或重定向到 leader。这样所有节点最终应用同一条日志序列，状态机才能保持一致。
```

### 5.2 AppendEntries 如何处理冲突

leader 发送日志时带上：

```text
prev_log_index
prev_log_term
entries[]
leader_commit
```

follower 检查 `prev_log_index` 位置上的 term 是否匹配：

- 匹配：可以追加后续 entries。
- 不匹配：拒绝，leader 回退 `next_index` 后重试。

如果 follower 在某个位置之后有冲突日志，要截断冲突部分，再追加 leader 日志。

### 5.3 为什么不能复制成功就算提交

单个 follower 复制成功不够，必须多数派复制成功才提交。否则 leader 写到少数节点后崩溃，新 leader 可能不包含这条日志。

### 5.4 为什么只提交当前 term 日志

Raft 有一个重要规则：leader 只能通过“复制到多数派”直接提交当前 term 的日志。旧 term 日志可以随当前 term 日志一起间接提交。

回答：

```text
如果允许 leader 仅根据旧 term 日志复制到多数派就提交，某些选举交错场景下可能违反 Leader Completeness。追加当前 term no-op entry 并提交后，可以建立当前 leader 的提交屏障，后续提交判断更安全。
```

项目中 leader 当选后追加 no-op barrier entry，就是为了这个语义。

## 6. 状态机一致性

Raft 保证日志序列一致，状态机还必须确定性。

确定性意味着：

```text
相同初始状态 + 相同日志顺序 = 相同最终状态
```

状态机不能依赖：

- 本地随机数。
- 当前系统时间。
- 节点私有状态。
- 非确定性外部调用。

KV 的 `PUT/DELETE/LOCK/MVCC` 命令都应该以日志命令中的参数为准。

## 7. 线性一致读

### 7.1 为什么读不能随便读

如果 leader 失去多数派，但它自己还没意识到，直接读本地状态可能返回旧数据。

典型场景：

```text
旧 leader A 与多数派隔离
多数派选出新 leader B
B 写入 x=2
A 本地仍然是 x=1
客户端读 A，如果 A 直接返回，就读到旧值
```

### 7.2 项目读法

项目采用 ReadIndex 风格的确认：

```text
leader 在读前确认自己仍能联系多数派
确认通过后读取本地状态机
```

这比把每次读都写入日志成本低，同时避免旧 leader 返回旧值。

### 7.3 读前 no-op barrier 的意义

leader 新当选后，需要确认当前 term 有已提交日志。no-op entry 可以作为当前 term 的提交屏障。这样 leader 对自己的合法性和日志提交位置有更强把握。

面试回答：

```text
线性一致读的关键不是“从 leader 读”这么简单，而是 leader 必须确认自己仍然是 leader，并且提交状态没有落后。项目用多数派确认来避免旧 leader 读。
```

## 8. Snapshot 和日志压缩

如果日志无限增长：

- 启动恢复慢。
- follower 落后太多时追日志成本高。
- 磁盘占用不断增加。

snapshot 的作用：

```text
把某个 index 之前的状态机结果压缩成快照
删除旧日志
保留 last_included_index / last_included_term
```

### 8.1 snapshot 安全点

只可以 snapshot 已提交且已应用到状态机的日志。不能压缩未提交日志。

### 8.2 InstallSnapshot

落后 follower 如果 `next_index` 已经早于 leader 的日志起点，leader 无法继续用 AppendEntries 补日志，就要发送 InstallSnapshot。

follower 安装 snapshot 后：

```text
1. 替换状态机
2. 更新 log_base_index
3. 丢弃快照覆盖的旧日志
4. 更新 commit_index / last_applied
```

## 9. RocksDB 的角色

RocksDB 在项目里不是“让系统自动分布式”的组件。它只是本地持久化存储：

- KV 状态机数据。
- Raft 元数据。
- snapshot 相关状态。

分布式一致性来自 Raft，不来自 RocksDB。

面试回答：

```text
RocksDB 负责单节点本地持久化，Raft 负责多节点复制和一致性。不能说用了 RocksDB 就天然高可用，也不能说 RocksDB 替代了 Raft 日志。
```

## 10. 锁服务边界

项目扩展了锁服务，但要主动说明边界。

### 10.1 当前锁能展示什么

- 锁命令通过 Raft 日志复制。
- 多节点状态机应用同一条锁命令。
- acquire / release 可以在线性一致状态机中排序。

### 10.2 当前不是完整生产级分布式锁

生产级锁还需要：

- lease 租约。
- fencing token。
- 自动过期。
- 客户端会话。
- 时钟和续约策略。
- 锁持有者崩溃后的释放。

面试回答：

```text
当前锁服务展示的是“锁状态可以放在 Raft 状态机里强一致复制”，但还不是完整生产级分布式锁。生产上必须补 lease、fencing token 和会话失效处理，否则客户端拿到锁后长时间卡住会影响资源释放。
```

## 11. MVCC 边界

当前 MVCC 能展示：

- 版本化读写。
- 写写冲突检测。
- 事务命令通过 Raft 状态机排序。

但还不是完整数据库事务系统。

生产级 MVCC 还需要：

- 持久化事务状态。
- 事务超时清理。
- 读写集管理。
- 隔离级别定义。
- 大事务回滚。
- 垃圾回收。

面试回答：

```text
当前 MVCC 是在 Raft KV 上扩展的轻量事务能力，重点展示版本化和冲突检测。它不是完整数据库内核，不能夸大成支持复杂 SQL 事务。
```

## 12. 成员变更边界

成员变更是 Raft 最容易被追问的点之一。

### 12.1 当前能说什么

项目有成员动态变更接口和基础日志命令，可以展示配置作为状态机命令复制。

### 12.2 必须承认什么

生产级 Raft 成员变更不能简单“一次把旧配置替换成新配置”。正确方案通常是 joint consensus：

```text
C_old
  -> C_old,new 联合配置
  -> C_new
```

联合配置阶段需要旧多数派和新多数派都确认，避免新旧配置各自选出 leader。

面试回答：

```text
当前成员变更是项目扩展能力，但生产安全性还需要 joint consensus。这个边界我会主动承认，因为成员变更比普通日志复制更容易出安全问题。
```

## 13. 幂等问题

### 13.1 写请求是否幂等

普通 `PUT key value` 从结果上看可以幂等，因为重复写同一个值最终状态一样。但 Raft 日志层仍会追加多条日志。

`LOCK`、`MVCC COMMIT` 等操作不一定幂等。

### 13.2 生产化怎么做请求幂等

建议引入：

```text
client_id
request_id
last_applied_request
cached_response
```

状态机记录每个客户端最近处理过的请求 id。如果重复请求到来，直接返回缓存结果，不重复执行副作用。

## 14. 答辩禁区

| 不要这样说 | 正确说法 |
| --- | --- |
| “完全等价 etcd” | “实现了 Raft KV 核心链路，和 etcd 还有生产化差距” |
| “成员变更已经生产安全” | “当前有基础接口，生产级需要 joint consensus” |
| “锁就是生产级分布式锁” | “展示强一致状态机锁，生产还需 lease 和 fencing token” |
| “读从 leader 读就线性一致” | “leader 读前要确认多数派，避免旧 leader” |
| “RocksDB 保证分布式一致性” | “RocksDB 是本地持久化，一致性由 Raft 保证” |

## 15. 最终 2 分钟回答

```text
distributed-kv 的核心是把 KV 操作放进 Raft 日志，leader 负责给写请求排序，日志复制到多数派后推进 commit_index，再按日志顺序 apply 到状态机。这样所有节点只要应用相同日志序列，就能得到相同状态。

安全性上，我重点做了几件事：选举时用 term、voted_for 和日志新旧检查保证同一任期最多一个合法 leader；日志复制用 prev_log_index 和 prev_log_term 修复 follower 冲突；提交时只直接提交当前 term 的日志，并在 leader 当选后追加 no-op barrier；读请求不是简单读本地，而是 leader 做多数派确认后再读，避免旧 leader 返回旧数据。

项目还扩展了 snapshot、RocksDB 持久化、多进程 libuv RPC、HTTP 接入、锁服务、MVCC 和成员变更接口。但我不会把它说成 etcd 级生产系统：成员变更还需要 joint consensus，锁服务需要 lease 和 fencing token，MVCC 还不是完整数据库事务，生产还要补请求幂等、TLS、认证、监控、备份和故障演练。
```
