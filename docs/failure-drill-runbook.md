# distributed-kv 故障演练手册

这份文档用于准备 distributed-kv 的故障演示和面试追问。分布式 KV 的可信度不来自“正常 curl 能成功”，而来自故障场景下仍能解释清楚系统行为。

## 1. 演练前准备

### 1.1 构建

```bash
cmake -S . -B build
cmake --build build -j
```

如果构建失败，优先检查：

- Protobuf 开发库。
- RocksDB 开发库。
- libuv。
- llhttp。
- pkg-config。

### 1.2 启动本地 3 节点

推荐用脚本：

```bash
./scripts/run_local_cluster.sh start
./scripts/run_local_cluster.sh status
```

手动启动时，三个节点分别有 raft port、http port 和 data dir。

### 1.3 查看 leader

```bash
curl http://127.0.0.1:9201/status
curl http://127.0.0.1:9202/status
curl http://127.0.0.1:9203/status
```

预期只有一个 leader。

### 1.4 基础读写

```bash
curl -X PUT http://127.0.0.1:9201/kv/alpha -d 'one'
curl http://127.0.0.1:9201/kv/alpha
```

如果打到 follower，可能返回 `307 Temporary Redirect`，可以使用：

```bash
curl -L -X PUT http://127.0.0.1:9202/kv/alpha -d 'one'
```

## 2. 演练总原则

每个故障演练都要记录：

```text
1. 故障前 leader 是谁
2. 故障前 commit_index / last_applied
3. 故障动作是什么
4. 故障期间读写表现
5. 故障恢复后 leader 是谁
6. 数据是否仍然一致
7. 是否发生重复提交或丢写
```

如果只说“服务恢复了”，不够。要说清楚恢复过程中 Raft 做了什么。

## 3. 演练一：leader 崩溃

### 3.1 目标

验证 leader 宕机后，剩余多数派可以重新选举 leader，并继续服务。

### 3.2 操作

```text
1. 找到当前 leader
2. 写入 key: before_failover=1
3. kill leader 进程
4. 等待选举超时
5. 查询剩余两个节点 status
6. 向新 leader 写入 key: after_failover=2
7. 重启旧 leader
8. 验证旧 leader 追上数据
```

### 3.3 预期

- 短暂不可写。
- 新 leader 在多数派中产生。
- 已提交数据不丢。
- 旧 leader 重启后成为 follower。
- 旧 leader 通过 AppendEntries 或 snapshot 追上日志。

### 3.4 面试回答

```text
leader 崩溃后，follower 在选举超时后发起选举。由于 3 节点中剩余 2 个构成多数派，可以选出新 leader。已经提交的日志复制到了多数派，新 leader 必须拥有这些日志；未提交日志可能被覆盖，这是 Raft 允许的。
```

## 4. 演练二：follower 崩溃

### 4.1 目标

验证少数派 follower 崩溃不影响 leader 对多数派提交。

### 4.2 操作

```text
1. 找到 follower
2. kill follower
3. 向 leader 连续写入多个 key
4. 读取确认
5. 重启 follower
6. 验证 follower 追上
```

### 4.3 预期

- 3 节点中 1 个 follower 挂掉，leader + 另一个 follower 仍是多数派。
- 写请求仍能提交。
- follower 恢复后根据 next_index/match_index 补日志。

### 4.4 面试回答

```text
Raft 写提交需要多数派，不需要所有节点都成功。一个 follower 宕机不会影响 3 节点集群继续写入。恢复后 leader 会根据 follower 的日志匹配情况继续发送 AppendEntries；如果落后超过压缩点，则通过 InstallSnapshot 追赶。
```

## 5. 演练三：多数派不可用

### 5.1 目标

验证失去多数派时系统不应该继续接受写成功。

### 5.2 操作

```text
1. 3 节点集群中停止两个节点
2. 保留一个旧 leader 或 follower
3. 尝试 PUT
4. 尝试 GET
```

### 5.3 预期

- 写不能成功提交。
- 如果是旧 leader，也不能在无法联系多数派时承诺写成功。
- 线性一致读也应失败或超时，而不是返回可能过期的数据。

### 5.4 面试回答

```text
强一致系统在失去多数派时必须牺牲可用性，不能继续承诺写成功。CAP 语境下这是选择一致性和分区容忍性。单个节点即使自认为是 leader，也无法获得多数派确认，因此不能提交新日志。
```

## 6. 演练四：网络分区

### 6.1 目标

验证多数派分区可以选 leader，少数派不能继续写。

### 6.2 场景

```text
3 节点：A, B, C
分区 1：A
分区 2：B, C
```

预期：

- `B, C` 可以形成多数派，选出 leader。
- `A` 如果原来是 leader，也应该因为无法联系多数派而不能提交。

### 6.3 面试回答

```text
Raft 依赖多数派交集。少数派分区不能提交写，即使旧 leader 在少数派里也不行。多数派分区可以选出新 leader 并继续服务。网络恢复后，旧 leader 看到更高 term 会退回 follower，并接受新 leader 的日志修复。
```

## 7. 演练五：旧 leader 恢复

### 7.1 目标

验证旧 leader 不会继续以 leader 身份写入。

### 7.2 操作

```text
1. kill 当前 leader
2. 等新 leader 产生并写入 key
3. 重启旧 leader
4. 查询旧 leader status
5. 向旧 leader 发送写请求
```

### 7.3 预期

- 旧 leader 恢复后看到更高 term，变成 follower。
- 写请求应重定向到新 leader 或拒绝。
- 不应出现两个 leader 同时接受写成功。

### 7.4 面试回答

```text
Raft RPC 都带 term。旧 leader 恢复后只要收到更高 term 的 AppendEntries 或 RequestVote，就会更新 term 并退回 follower。它不能继续用旧 term 提交写。
```

## 8. 演练六：follower 日志冲突

### 8.1 目标

验证 follower 上未提交的冲突日志会被新 leader 覆盖。

### 8.2 背景

可能场景：

```text
旧 leader 写入一条日志但未复制到多数派
旧 leader 崩溃
新 leader 在同一位置写入不同日志并提交
旧 leader 恢复
```

### 8.3 预期

旧 leader 恢复为 follower 后，它本地未提交的冲突日志会被新 leader 的日志覆盖。

### 8.4 面试回答

```text
Raft 只保证已提交日志不会丢。未提交日志可以被覆盖。AppendEntries 通过 prev_log_index 和 prev_log_term 检查日志前缀是否匹配，不匹配则 follower 拒绝，leader 回退 next_index，最终找到共同前缀并截断 follower 冲突日志。
```

## 9. 演练七：snapshot 追赶

### 9.1 目标

验证 follower 落后太多时可以通过 snapshot 追上。

### 9.2 操作思路

```text
1. 停止一个 follower
2. leader 持续写入大量 key
3. 触发 snapshot / log compaction
4. 重启 follower
5. 观察是否 InstallSnapshot
6. 验证 follower 数据一致
```

### 9.3 预期

- follower 的 next_index 早于 leader 日志起点。
- leader 发送 snapshot。
- follower 安装 snapshot 后继续接收后续日志。

### 9.4 面试回答

```text
日志压缩后，leader 已经没有 follower 缺失的早期日志，不能再靠 AppendEntries 从头补齐。这时要发送 InstallSnapshot，让 follower 先恢复到 snapshot 覆盖的状态，再接后续日志。
```

## 10. 演练八：ReadIndex 防旧读

### 10.1 目标

验证旧 leader 失去多数派后不能返回线性一致读。

### 10.2 场景

```text
A 是 leader
A 与 B/C 分区
B/C 选出新 leader B
B 写入 x=2
客户端读 A
```

### 10.3 预期

A 不能联系多数派，因此 ReadIndex 风格确认失败，不应返回旧值。

### 10.4 面试回答

```text
只从 leader 本地读不一定线性一致，因为旧 leader 可能已经失去多数派。读前做多数派确认可以证明当前 leader 仍然有效，从而避免返回过期状态。
```

## 11. 演练九：磁盘数据损坏

### 11.1 目标

验证节点本地 RocksDB 或元数据损坏时的边界。

### 11.2 操作

```text
1. 停止某个 follower
2. 备份并破坏它的数据目录
3. 重启节点
4. 观察启动行为
```

### 11.3 预期

当前项目如果没有完整的数据修复工具，应该明确报错或重新同步，而不是静默以错误状态加入集群。

### 11.4 面试回答

```text
单节点磁盘损坏不应该破坏已提交数据，因为多数派仍有数据。但损坏节点不能带着错误状态直接加入，需要校验本地元数据和 snapshot。生产化会增加 checksum、备份恢复和重新拉取 snapshot 的流程。
```

## 12. 演练十：成员变更风险

### 12.1 目标

明确当前成员变更边界，避免夸大。

### 12.2 应验证

- 添加节点前，新节点需要追上日志。
- 删除节点时，不能让集群失去多数派。
- 变更过程中不要并发发起第二次变更。

### 12.3 面试回答

```text
当前成员变更是基础接口，生产安全需要 joint consensus。故障演练中我会主动说明这个边界，不把它说成完整安全的动态扩缩容。
```

## 13. 故障观察指标

演练时建议记录：

```text
role
term
leader_id
commit_index
last_applied
last_log_index
snapshot_index
match_index
next_index
append_entries_success/failure
read_index_success/failure
```

没有指标就很难解释故障过程。当前如果指标有限，可以通过 `/status` 和日志先观察，后续生产化补 Prometheus。

## 14. 常见误判

| 现象 | 正确解释 |
| --- | --- |
| leader kill 后短暂失败 | 正常，选举需要时间 |
| 少数派不能写 | 正常，保证一致性 |
| 未提交日志丢失 | 正常，Raft 只保证已提交日志 |
| 旧 leader 变 follower | 正常，看到更高 term |
| follower 落后后装 snapshot | 正常，日志已压缩 |
| follower 收到写返回 redirect | 正常，写必须走 leader |

## 15. 演练报告模板

每次演练后可以记录：

```text
演练名称：
集群规模：
初始 leader：
初始数据：
故障动作：
故障期间表现：
恢复动作：
最终 leader：
最终数据校验：
是否符合预期：
发现的问题：
后续改进：
```

## 16. 面试总结话术

```text
我验证 distributed-kv 时不会只跑正常读写，还会专门演练 leader 崩溃、follower 崩溃、多数派不可用、网络分区、旧 leader 恢复、日志冲突、snapshot 追赶和磁盘损坏。因为分布式 KV 的核心不是接口跑通，而是故障下能否维持 Raft 的安全性：同一任期最多一个 leader，已提交日志不丢，未提交日志允许覆盖，读请求不能由旧 leader 返回过期数据。
```

## 17. 最终结论

distributed-kv 的故障演练重点是：

- leader 崩溃后能重新选举。
- follower 崩溃不影响多数派。
- 失去多数派不能继续写。
- 旧 leader 恢复后必须退位。
- 未提交冲突日志可以被覆盖。
- snapshot 能让落后节点追上。
- 读路径要防旧 leader。
- 成员变更要承认 joint consensus 边界。

这些演练比单纯展示 API 更能说明你理解分布式系统。
