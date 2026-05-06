# Distributed KV

[![Release](https://img.shields.io/github/v/release/wjg12345678/distributed-kv?display_name=tag)](https://github.com/wjg12345678/distributed-kv/releases)
[![Stars](https://img.shields.io/github/stars/wjg12345678/distributed-kv)](https://github.com/wjg12345678/distributed-kv/stargazers)
![C++17](https://img.shields.io/badge/C%2B%2B-17-00599C)
![Raft](https://img.shields.io/badge/Consensus-Raft-F28E2B)
![RocksDB](https://img.shields.io/badge/Storage-RocksDB-2E8B57)

`distributed-kv` 是一个面向教学和实验的 C++17 分布式 KV 原型。项目把 Raft 共识、KV 状态机、RocksDB 持久化、最小 HTTP 接口以及多进程 TCP 通信放在同一个仓库里，方便同时观察算法行为和系统边界。

它不是生产级数据库，更适合作为以下场景的参考实现：

- 阅读 Raft 选举、日志复制、冲突回退和快照流程
- 验证状态机复制如何落到 KV、锁服务和 MVCC 接口上
- 在单进程和多进程两种运行方式之间切换，做功能实验

## 秋招速览

如果把这个仓库当作秋招面试项目，可以先抓这几个点：

- 分布式核心：Leader 选举、日志复制、冲突回退、快照、持久化
- 正确性边界：Pre-Vote、仅推进当前 Term 日志 commit、ReadIndex 风格线性一致读
- 工程形态：单进程 demo + 多进程真实 TCP 集群 + HTTP API + Prometheus 风格 metrics
- 状态机扩展：KV、分布式锁、简化版 MVCC
- 当前边界：成员变更仍是教学版简化实现，不是 joint consensus

如果你是为了准备面试，建议配合阅读 [docs/interview_guide.md](docs/interview_guide.md)。

进一步的图示和压测结果可以看：

- [docs/architecture.md](docs/architecture.md)
- [docs/performance.md](docs/performance.md)

## 已实现能力

- Raft Leader 选举和随机化选举超时
- Pre-Vote 预投票，降低隔离节点恢复后的扰动
- ReadIndex 风格的线性一致读（leader 多数派确认后再读）
- 日志复制、日志冲突覆盖与回退
- Leader no-op barrier entry
- 仅推进当前 Term 日志的 commit
- Snapshot / Log Compaction
- 状态机应用到 KV 存储
- RocksDB 持久化状态机数据和 Raft 元数据
- 基于真实 TCP 的多进程 Raft RPC
- 最小 HTTP 接口
- 锁服务
- 简化版 MVCC 读写与写写冲突检测
- 简化版成员动态变更接口
- 基础 metrics 输出

## 架构概览

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
  +--> replicate to followers over TCP + Protobuf
  +--> commit after majority ack
  +--> apply to state machine
              |
              +--> KV
              +--> Lock
              +--> MVCC
  |
  +--> persist Raft metadata and state machine snapshots via RocksDB
```

读写路径可以概括为：

- 写：客户端请求先到 leader，再进入 Raft 日志复制，提交后应用到状态机
- 读：leader 先做多数派确认，再读取本地状态机，避免失去多数派的旧 leader 返回旧数据
- 快照：已提交状态机会周期性压缩日志，落成 snapshot，落后 follower 会通过 InstallSnapshot 追平

## 仓库结构

```text
apps/                 可执行入口
include/
  core/               Raft 核心类型、节点、持久化接口
  network/            TCP、RPC、HTTP、任务执行器
  storage/            KV 存储抽象与 RocksDB 实现
protos/               Protobuf 协议
src/
  core/               Raft 核心实现
  network/            网络与 HTTP 服务实现
  storage/            KV 存储实现
tests/unit/           功能测试
```

## 依赖

构建需要以下依赖：

- 支持 C++17 的编译器
- CMake 3.16+
- Protobuf
- `pkg-config`
- RocksDB

注意：当前 `CMakeLists.txt` 会始终链接 RocksDB，所以即使你只想运行内存版 demo，也需要本机先安装 RocksDB 开发库和头文件。

## 构建

```bash
cmake -S . -B build
cmake --build build -j
```

构建完成后会生成这些主要二进制：

- `build/distributed_kv_demo`：单进程、内存版 3 节点演示
- `build/distributed_kv_http_server`：单进程、RocksDB 版 HTTP 服务
- `build/distributed_kv_network_node`：多进程、真实 TCP 网络节点
- `build/raft_*_test`：功能测试

## 运行方式

### 1. 单进程内存版 Demo

这个入口把 3 个节点放在同一进程里，通过 `Cluster` 直接转发 Raft RPC，便于阅读和调试。

```bash
./build/distributed_kv_demo
```

它会自动完成一次 leader 选举，并向状态机写入两条 KV 数据。

### 2. 单进程 HTTP Server

这个入口仍然是单进程 3 节点集群，但底层状态机和 Raft 元数据使用 RocksDB 持久化。启动时会清空以下目录，因此它更适合本地演示，不适合保留历史数据：

- `/tmp/distributed-kv-rocksdb-node1`
- `/tmp/distributed-kv-rocksdb-node2`
- `/tmp/distributed-kv-rocksdb-node3`
- `/tmp/distributed-kv-rocksdb-node1-meta`
- `/tmp/distributed-kv-rocksdb-node2-meta`
- `/tmp/distributed-kv-rocksdb-node3-meta`

启动：

```bash
./build/distributed_kv_http_server
```

访问：

```bash
curl -X PUT http://127.0.0.1:9006/kv/name -d 'mrw'
curl http://127.0.0.1:9006/kv/name
```

这个入口只暴露最小 KV API：

- `PUT /kv/<key>`：请求体原样作为 value
- `GET /kv/<key>`：读取 value

### 3. 多进程真实网络版

这是最接近“分布式系统”形态的入口。每个节点是独立进程，拥有：

- 独立的 Raft RPC 端口
- 独立的 HTTP 端口
- 独立的 RocksDB 数据目录
- 基于 TCP + Protobuf 的节点间通信

启动一个 3 节点集群，分别在 3 个终端中运行：

终端 1：

```bash
./build/distributed_kv_network_node \
  --node-id 1 \
  --raft-port 9101 \
  --http-port 9201 \
  --data-dir /tmp/distributed-kv-node1 \
  --peer 2:127.0.0.1:9102:9202 \
  --peer 3:127.0.0.1:9103:9203
```

终端 2：

```bash
./build/distributed_kv_network_node \
  --node-id 2 \
  --raft-port 9102 \
  --http-port 9202 \
  --data-dir /tmp/distributed-kv-node2 \
  --peer 1:127.0.0.1:9101:9201 \
  --peer 3:127.0.0.1:9103:9203
```

终端 3：

```bash
./build/distributed_kv_network_node \
  --node-id 3 \
  --raft-port 9103 \
  --http-port 9203 \
  --data-dir /tmp/distributed-kv-node3 \
  --peer 1:127.0.0.1:9101:9201 \
  --peer 2:127.0.0.1:9102:9202
```

节点会把数据写到：

- `<data-dir>/store`：KV 状态机
- `<data-dir>/meta`：Raft 持久化状态

查看 leader：

```bash
curl http://127.0.0.1:9201/status
curl http://127.0.0.1:9202/status
curl http://127.0.0.1:9203/status
```

写入数据：

```bash
curl -X PUT http://127.0.0.1:9201/kv/alpha -d 'one'
curl http://127.0.0.1:9201/kv/alpha
```

如果请求打到 follower，且它知道 leader，服务会返回 `307 Temporary Redirect`。想对任意节点发请求时，可以使用 `curl -L` 自动跟随跳转。

## 多进程节点 HTTP API

以下接口由 `distributed_kv_network_node` 暴露。

### 集群状态

- `GET /status`
  - 返回节点 ID、角色、term、leader、commit index、peer 数等文本信息
- `GET /metrics`
  - 返回 Prometheus 风格的纯文本指标，例如 RPC 次数、提交条数、HTTP 请求数、平均 QPS

### KV

- `PUT /kv/<key>`
  - 请求体原样作为 value
- `GET /kv/<key>`

示例：

```bash
curl -L -X PUT http://127.0.0.1:9201/kv/service -d 'distributed-kv'
curl -L http://127.0.0.1:9202/kv/service
```

### 锁服务

- `POST /lock/acquire`
  - 表单参数：`name=<lock_name>&owner=<owner_id>`
- `POST /lock/release`
  - 表单参数：`name=<lock_name>&owner=<owner_id>`
- `GET /lock/<name>`
  - 返回 `unlocked` 或 `owner=<owner>`

示例：

```bash
curl -L -X POST http://127.0.0.1:9201/lock/acquire -d 'name=deploy&owner=worker-a'
curl -L http://127.0.0.1:9202/lock/deploy
curl -L -X POST http://127.0.0.1:9201/lock/release -d 'name=deploy&owner=worker-a'
```

### MVCC

- `POST /mvcc/begin`
  - 返回 `tx_id` 和 `snapshot_ts`
- `PUT /mvcc/tx/<tx_id>/kv/<key>`
  - 请求体原样作为待提交 value
- `POST /mvcc/commit`
  - 表单参数：`tx_id=<tx_id>`
  - 成功返回 `commit_ts=<ts>`
  - 写写冲突返回 `409 Conflict`
- `GET /mvcc/kv/<key>?snapshot_ts=<ts>`
  - 按快照时间读取历史版本

示例：

```bash
curl -L -X POST http://127.0.0.1:9201/mvcc/begin
curl -L -X PUT http://127.0.0.1:9201/mvcc/tx/mvcc-tx-1/kv/order -d 'v1'
curl -L -X POST http://127.0.0.1:9201/mvcc/commit -d 'tx_id=mvcc-tx-1'
curl -L "http://127.0.0.1:9201/mvcc/kv/order?snapshot_ts=1"
```

说明：`tx_id` 是运行时生成的，实际使用时应当取自 `/mvcc/begin` 的返回值，上面的 `mvcc-tx-1` 只是示例。

### 成员变更

当前实现的是教学版的简化成员变更，不是 Raft joint consensus。

- `POST /admin/add-peer`
  - 表单参数：`id=<id>&host=<host>&raft_port=<raft_port>&http_port=<http_port>`
- `POST /admin/remove-peer`
  - 表单参数：`id=<id>`

示例：

```bash
curl -L -X POST http://127.0.0.1:9201/admin/add-peer \
  -d 'id=4&host=127.0.0.1&raft_port=9104&http_port=9204'

curl -L -X POST http://127.0.0.1:9201/admin/remove-peer \
  -d 'id=3'
```

## RocksDB 参数

`distributed_kv_network_node` 支持这些常用 RocksDB 配置参数：

```bash
--disable-wal
--disable-compression
--disable-bloom-filter
--write-buffer-size 67108864
--max-background-jobs 2
--max-write-buffer-number 3
--max-open-files -1
--max-subcompactions 2
--level0-compaction-trigger 4
--target-file-size-base 67108864
--bloom-bits-per-key 10
--compaction-style level
```

示例：

```bash
./build/distributed_kv_network_node \
  --node-id 1 \
  --raft-port 9101 \
  --http-port 9201 \
  --data-dir /tmp/distributed-kv-node1 \
  --peer 2:127.0.0.1:9102:9202 \
  --peer 3:127.0.0.1:9103:9203 \
  --disable-wal \
  --write-buffer-size 33554432
```

## 测试

当前仓库提供的功能测试包括：

- `raft_smoke_test`：基础 leader 选举和日志复制
- `raft_prevote_test`：隔离节点恢复后不会靠抬高 term 扰动现有 leader
- `raft_linearizable_read_test`：失去多数派的旧 leader 不能继续提供线性一致读
- `raft_conflict_test`：日志冲突覆盖
- `raft_persistence_test`：RocksDB 持久化恢复
- `raft_snapshot_test`：快照与日志压缩
- `raft_membership_test`：简化成员变更
- `raft_lock_test`：分布式锁
- `raft_mvcc_test`：MVCC 和写写冲突

运行：

```bash
./build/raft_smoke_test
./build/raft_prevote_test
./build/raft_linearizable_read_test
./build/raft_conflict_test
./build/raft_persistence_test
./build/raft_snapshot_test
./build/raft_membership_test
./build/raft_lock_test
./build/raft_mvcc_test
```

## 面试建议

如果面试官追问“这个项目和普通 KV demo 的区别”，建议优先讲下面 4 点：

- 不是单机内存玩具，而是有多进程 TCP 通信、独立数据目录和真实 leader/follower 角色切换
- 不只实现了基础选举和复制，还补了 Pre-Vote 与线性一致读这两个常见正确性边界
- 状态机不只有 KV，还扩展了锁服务和 MVCC，因此可以展示 Raft 之上的业务语义
- 有成组功能测试覆盖选举、冲突、快照、持久化、成员变更和分区场景

更细的话术和高频问题可以看 [docs/interview_guide.md](docs/interview_guide.md)。

## 当前限制

- 这是教学和实验代码，不是生产级数据库实现
- 成员变更是简化版本，没有实现 joint consensus
- 线性一致读当前走的是 ReadIndex 风格多数派确认，不是 lease read
- HTTP server 和 TCP RPC server 都是最小手写实现，没有鉴权、TLS 或复杂流控
- 单进程 `distributed_kv_http_server` 启动时会删除已有 `/tmp` 数据目录
