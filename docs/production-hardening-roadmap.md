# distributed-kv 生产化加固路线

distributed-kv 当前适合展示 Raft、状态机、RocksDB、libuv 网络和 HTTP 接入的完整学习型分布式 KV。它已经比“单机 map + HTTP”复杂很多，但距离 etcd/Consul 这类生产系统仍有明显边界。

这份文档按生产化优先级列出后续要补的能力，并说明为什么这些能力重要。

## 1. 当前能力和边界

| 方向 | 当前能力 | 生产化差距 |
| --- | --- | --- |
| Raft 选举 | 随机超时、Pre-Vote、RequestVote | 需要更完整的 lease、时钟和异常场景验证 |
| 日志复制 | AppendEntries、冲突回退、多数派提交 | 需要 backpressure、批量复制、pipeline、磁盘 fsync 策略 |
| 读一致性 | ReadIndex 风格多数派确认 | 可扩展 follower read、lease read，但要严格证明安全 |
| 快照 | snapshot/log compaction、InstallSnapshot | 需要流式传输、校验、断点恢复、限速 |
| 持久化 | RocksDB 状态机和元数据 | 需要 WAL 完整性、崩溃恢复测试、备份恢复 |
| 成员变更 | 基础接口 | 需要 joint consensus |
| 锁服务 | Raft 状态机锁 | 需要 lease、fencing token、session |
| MVCC | 版本化和冲突检测 | 需要事务持久化、GC、隔离级别 |
| 网络 | libuv TCP + Protobuf、HTTP | 需要 TLS、认证、连接限流、协议版本兼容 |
| 运维 | 本地脚本和基础 metrics | 需要 Prometheus 指标、告警、故障演练、数据修复工具 |

## 2. P0：崩溃恢复和持久化语义

分布式 KV 第一生产化优先级不是加更多 API，而是证明节点崩溃恢复后不会破坏 Raft 安全性。

### 2.1 必须持久化的状态

Raft 至少要求：

```text
current_term
voted_for
log entries
snapshot metadata
```

这些状态必须在回复关键 RPC 前持久化。例如投票前必须持久化 `voted_for`，否则崩溃后可能重复投票。

### 2.2 fsync 策略

需要明确：

- 每条日志是否 fsync。
- 批量 fsync 的窗口多大。
- 性能和可靠性如何取舍。
- 断电后可能丢失哪些已经响应给客户端的数据。

生产级说法必须清楚：

```text
只有已经持久化并复制到多数派的日志，才能对客户端返回成功。
```

如果为了性能做 group commit，也要说明崩溃窗口。

### 2.3 崩溃恢复测试

需要增加：

```text
1. leader 写入后立刻 kill -9
2. follower 复制一半时 kill -9
3. snapshot 写到一半时 kill -9
4. 元数据损坏时启动
5. RocksDB 目录丢失时启动
6. 重启后重新选举并读回数据
```

这类测试比单纯功能测试更能证明生产化能力。

## 3. P0：请求幂等

客户端超时后可能重试。如果第一次请求已经提交，但响应丢失，第二次重试会再次追加日志。

### 3.1 问题场景

```text
client PUT x=1
leader 提交成功
leader 返回响应时网络断开
client 以为失败，重试 PUT x=1
系统追加第二条日志
```

对 `PUT x=1` 可能结果相同，但对锁、事务、计数器、队列类命令就可能产生重复副作用。

### 3.2 建议方案

请求携带：

```text
client_id
request_id
```

状态机保存：

```text
last_request_id_by_client
cached_response_by_client
```

如果请求 id 已处理过，直接返回缓存响应，不重复执行命令。

### 3.3 面试说法

```text
Raft 解决的是日志复制一致性，不自动解决客户端重试幂等。生产系统要在状态机层记录 client_id/request_id，保证同一个客户端请求最多执行一次。
```

## 4. P0：Joint Consensus 成员变更

成员变更不能简单替换配置。

### 4.1 风险

如果从旧配置 `C_old` 直接切到新配置 `C_new`，可能出现两个配置各自形成多数派并选出 leader。

### 4.2 正确路线

Raft joint consensus：

```text
C_old
  -> C_old,new
  -> C_new
```

联合配置阶段提交日志需要同时满足：

- 旧配置多数派确认。
- 新配置多数派确认。

这样新旧配置不会各自独立形成安全冲突。

### 4.3 实现任务

需要补：

- 配置日志 entry 类型。
- `old_config` / `new_config` 存储。
- 联合配置下的多数派判断。
- leader 转移或新节点追日志策略。
- 删除节点的退出策略。
- 成员变更期间禁止并发变更。

## 5. P1：复制性能和 backpressure

当前教学实现更关注正确性。生产要考虑复制效率。

### 5.1 批量 AppendEntries

不要每条日志都单独 RPC。可以批量发送：

```text
entries[batch_start, batch_end]
```

控制参数：

- 最大 entries 数。
- 最大字节数。
- 最大等待时间。

### 5.2 Pipeline

leader 可以对 follower 维护 inflight 窗口，不必等上一批完全响应再发下一批。

但要限制：

- 每个 follower inflight 字节数。
- 慢 follower 不拖垮 leader。
- 落后太多时切 snapshot。

### 5.3 backpressure

如果 leader 写入速度超过多数派复制能力，要限制客户端写入：

- 返回 `503 busy`。
- 阻塞写队列。
- 降低批量大小。
- 对慢 follower 限速。

否则日志和内存可能无限增长。

## 6. P1：快照生产化

当前 snapshot 能展示 log compaction，但生产还要补完整生命周期。

### 6.1 快照文件格式

需要包含：

```text
magic/version
last_included_index
last_included_term
cluster_config
state_machine_data
checksum
```

版本号方便升级兼容，checksum 用于检测损坏。

### 6.2 流式 InstallSnapshot

大快照不能一次性全部进内存。应支持分块：

```text
InstallSnapshotRequest {
  offset
  data
  done
}
```

支持：

- 分块校验。
- 断点续传。
- 限速。
- 安装到临时文件，完成后原子 rename。

### 6.3 snapshot 与写入并发

生成 snapshot 时要避免阻塞所有写太久。可以：

- 复制 RocksDB checkpoint。
- 在一致视图上生成 snapshot。
- 后台压缩旧日志。

## 7. P1：读路径升级

### 7.1 当前 ReadIndex 风格读

leader 读前做多数派确认，安全但有网络往返。

### 7.2 Lease Read

lease read 可以减少读延迟，但依赖时钟假设。必须保证：

- 选举超时远大于时钟漂移和网络延迟。
- leader lease 未过期。
- 时钟单调性。

如果无法严格证明，不要轻易宣传。

### 7.3 Follower Read

Follower read 可以分担读压力，但要保证读到足够新的状态：

- follower 向 leader 获取 read index。
- 等本地 apply 到该 index。
- 再返回读结果。

## 8. P1：锁服务生产化

当前锁服务是“强一致状态机锁”的雏形。生产需要：

### 8.1 Lease

锁必须有租约：

```text
lock_key
owner_id
lease_id
expire_at
```

租约过期后锁自动释放。

### 8.2 Fencing Token

每次成功获取锁返回递增 token。下游资源只接受更大的 token，防止旧持有者暂停后恢复继续写。

场景：

```text
client A 获得锁 token=10
A STW 暂停
锁过期，client B 获得 token=11
A 恢复后继续写下游
下游看到 token=10 < 11，拒绝
```

### 8.3 Session

客户端和集群建立 session，session 失效时释放相关锁。

## 9. P1：MVCC 生产化

当前 MVCC 可展示事务思想，但生产要补：

- 事务元数据持久化。
- 事务超时和清理。
- read timestamp / commit timestamp 分配。
- 写集合冲突检测。
- 版本 GC。
- 隔离级别说明。

不要把它说成完整数据库事务。更稳的定位：

```text
在 Raft KV 上演示 MVCC 版本化和写写冲突检测。
```

## 10. P1：安全

生产必须补：

| 项 | 原因 |
| --- | --- |
| TLS | 节点间 Raft RPC 和客户端 HTTP 都要加密 |
| 节点认证 | 防止伪造 Raft RPC |
| 客户端鉴权 | 防止任意人读写 KV |
| 权限模型 | 区分读、写、管理、成员变更 |
| 审计日志 | 记录管理操作和高风险写入 |
| 配额 | 防止单客户端写爆存储 |

Raft RPC 特别敏感。伪造 AppendEntries 或 RequestVote 会破坏集群行为，必须做节点身份认证。

## 11. P2：可观测性

建议 Prometheus 指标：

### 11.1 Raft 状态

- 当前 role
- current_term
- leader_id
- commit_index
- last_applied
- last_log_index
- snapshot_index

### 11.2 复制指标

- 每个 follower 的 match_index
- 每个 follower 的 next_index
- replication lag
- append entries latency
- rejected append count
- install snapshot count

### 11.3 请求指标

- PUT/GET QPS
- 写提交延迟
- 读确认延迟
- HTTP 4xx/5xx
- leader redirect 次数

### 11.4 存储指标

- RocksDB write latency
- RocksDB read latency
- WAL size
- snapshot size
- compaction time

## 12. P2：备份恢复

强一致不等于不需要备份。

需要：

- 定期 snapshot 备份。
- RocksDB checkpoint。
- 元数据备份。
- 恢复演练。
- 备份校验。

灾难恢复流程要能回答：

```text
如果 3 节点都误删数据，Raft 会把错误复制得很一致。备份是另一层保护。
```

## 13. P2：运维工具

需要补：

- `kvctl status`
- `kvctl member list/add/remove`
- `kvctl snapshot save/restore`
- `kvctl endpoint health`
- `kvctl alarm list`
- `kvctl defrag`

HTTP API 适合 demo，生产更需要稳定 CLI。

## 14. 测试升级

### 14.1 单元测试

继续覆盖：

- 选举。
- 日志冲突。
- snapshot。
- MVCC。
- 锁。
- 成员变更。

### 14.2 集成测试

新增：

- 多进程 kill/restart。
- 网络延迟和断连。
- follower 落后后追赶。
- snapshot 安装中断。
- leader 切换期间持续写。

### 14.3 Jepsen 思路

不一定完整引入 Jepsen，但要按 Jepsen 思维设计：

- 并发读写。
- 随机故障。
- 记录历史。
- 线性一致性检查。

这对分布式 KV 的说服力远高于“跑通几条 curl”。

## 15. 优先级路线

### 15.1 一周内

```text
1. 补请求幂等设计文档和简单实现
2. 增加 leader kill/restart 集成测试
3. 增加 snapshot 损坏/恢复测试
4. 完善 metrics 输出
5. 明确成员变更边界
```

### 15.2 两到三周

```text
1. 实现 joint consensus
2. 实现批量 AppendEntries
3. 增加复制 backpressure
4. 增加 snapshot checksum
5. 增加基础认证
```

### 15.3 长期

```text
1. TLS 和节点证书
2. follower read
3. lease read
4. 生产级锁 lease + fencing token
5. MVCC GC
6. 备份恢复工具
7. Jepsen 风格测试
```

## 16. 面试边界表达

被问“和 etcd 差多少”，可以答：

```text
这个项目实现了 Raft KV 的核心链路：选举、日志复制、多数派提交、线性一致读、snapshot、RocksDB 和 HTTP/RPC 接入。但 etcd 是生产级系统，还包括成熟的 joint consensus、lease、watch、auth、TLS、压缩、告警、备份、运维工具和大量故障验证。我的项目更适合展示底层原理和工程实现，不会说已经达到 etcd 完整能力。
```

被问“下一步做什么”，可以答：

```text
我会先补请求幂等、崩溃恢复测试和 joint consensus，因为这些直接影响一致性安全。然后再做复制性能、snapshot 流式传输、锁 lease/fencing token、认证 TLS 和可观测性。
```

## 17. 最终结论

distributed-kv 继续升级时，优先级应该是：

```text
安全性 > 可恢复性 > 可观测性 > 性能 > 更多功能
```

分布式系统最怕“功能看起来多，但故障下不清楚”。所以后续不是先加 watch、SQL 或复杂 API，而是先把崩溃恢复、成员变更、请求幂等、snapshot 校验和故障演练做扎实。
