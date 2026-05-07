# 分布式 KV

[![Release](https://img.shields.io/github/v/release/wjg12345678/distributed-kv?display_name=tag)](https://github.com/wjg12345678/distributed-kv/releases)
[![Stars](https://img.shields.io/github/stars/wjg12345678/distributed-kv)](https://github.com/wjg12345678/distributed-kv/stargazers)
![C++17](https://img.shields.io/badge/C%2B%2B-17-00599C)
![Raft](https://img.shields.io/badge/Consensus-Raft-F28E2B)
![RocksDB](https://img.shields.io/badge/Storage-RocksDB-2E8B57)

`distributed-kv` 是一个基于 C++17 实现的分布式键值存储系统，聚焦高一致性数据复制与状态机编排，覆盖了 Raft 共识、KV 状态机、RocksDB 持久化、`libuv + llhttp` HTTP 接入层以及基于 `libuv` 的多进程 TCP 通信。

仓库同时提供单进程和多进程两种运行形态，便于在本地完成集群启动、读写验证、日志复制、快照恢复和状态机能力测试。

进一步的图示、实现说明和压测结果可以参考：

- [docs/architecture.md](docs/architecture.md)
- [docs/implementation_notes.md](docs/implementation_notes.md)
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
- 基于 `libuv` 长连接的多进程 Raft RPC
- 基于 `libuv + llhttp` 的 HTTP 接入层
- 基础输入校验和 `400 Bad Request` 错误返回
- 锁服务
- MVCC 读写与写写冲突检测
- 成员动态变更接口
- 基础指标输出
- `CTest` 回归测试和 GitHub Actions 持续集成
- 本地 3 节点启动 / 快速验证 / 压测脚本

## 适用场景

- 配置中心、元数据管理等需要强一致写入确认的场景
- 分布式锁、协调状态、轻量事务等需要一致性状态机承载的场景
- 本地集群验证、协议联调和存储链路性能分析

## 架构概览

```text
客户端
  |
  v
HTTP 接口
  |
  v
Leader 节点
  |
  +--> 追加日志
  +--> 通过 libuv TCP 长连接 + Protobuf 复制到 follower
  +--> 多数派确认后提交
  +--> 应用到状态机
              |
              +--> KV
              +--> 锁服务
              +--> MVCC
  |
  +--> 通过 RocksDB 持久化 Raft 元数据与状态机快照
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
  network/            TCP 工具、libuv RPC、llhttp HTTP、任务执行器
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
- `libuv`
- `llhttp`

注意：当前 `CMakeLists.txt` 会始终链接 RocksDB，所以即使你只想运行内存版单进程节点，也需要本机先安装 RocksDB 开发库和头文件。

## 构建

```bash
cmake -S . -B build
cmake --build build -j
```

构建完成后会生成这些主要二进制：

- `build/distributed_kv_single_process`：单进程、内存版 3 节点实例
- `build/distributed_kv_http_server`：单进程、RocksDB 版 HTTP 服务
- `build/distributed_kv_network_node`：多进程、真实 TCP 网络节点
- `build/raft_*_test`：功能测试

## 运行方式

### 1. 单进程内存版实例

这个入口把 3 个节点放在同一进程里，通过 `Cluster` 直接转发 Raft RPC，便于阅读和调试。

```bash
./build/distributed_kv_single_process
```

它会自动完成一次 leader 选举，并向状态机写入两条 KV 数据。

### 2. 单进程 HTTP 服务

这个入口仍然是单进程 3 节点集群，但底层状态机和 Raft 元数据使用 RocksDB 持久化。启动时会清空以下目录，适合本地验证与接口调试：

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

- `PUT /kv/<key>`：请求体原样作为值
- `GET /kv/<key>`：leader 先做多数派确认，再读取值

### 3. 多进程集群版

这是最接近实际部署形态的入口。每个节点是独立进程，拥有：

- 独立的 Raft RPC 端口
- 独立的 HTTP 端口
- 独立的 RocksDB 数据目录
- 基于 `libuv` 长连接 + Protobuf 的节点间通信

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

查看主节点：

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

### 4. 本地 3 节点脚本

如果想快速启动本地 3 节点集群，而不手动打开 3 个终端，可以直接使用仓库里的脚本：

```bash
./scripts/run_local_cluster.sh start
./scripts/run_local_cluster.sh status
./scripts/verify_local_cluster.sh
./scripts/verify_network_e2e.sh
./scripts/run_local_cluster.sh stop
```

说明：

- `scripts/run_local_cluster.sh` 支持 `start|stop|restart|status`
- `scripts/verify_local_cluster.sh` 是多进程 KV smoke 脚本，会自动启动集群、写入一条 KV、验证从节点返回的 `307 Temporary Redirect`，再读取指标
- `scripts/verify_network_e2e.sh` 是多进程回归脚本，覆盖 failover、锁、MVCC 和成员变更场景，CI 会执行它
- `scripts/verify_local_cluster.sh` 默认运行目录是 `/tmp/distributed-kv-local-cluster`
- `scripts/verify_network_e2e.sh` 默认运行目录是 `/tmp/distributed-kv-e2e`

## 多进程节点 HTTP 接口

以下接口由 `distributed_kv_network_node` 暴露。

### 集群状态

- `GET /status`
  - 返回节点 ID、角色、term、leader、commit index、peer 数等文本信息
- `GET /metrics`
  - 返回纯文本指标，例如 RPC 次数、提交条数、HTTP 请求数、平均 QPS

### KV

- `PUT /kv/<key>`
  - 请求体原样作为值
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
  - 请求体原样作为待提交的值
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

当前成员变更通过 `BeginJointConfig` / `FinalizeConfig` 两阶段日志命令完成，覆盖 add / remove peer 的 joint consensus 风格配置切换。

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
- `raft_membership_test`：两阶段成员变更、移除 leader 和重启恢复
- `raft_async_replication_test`：异步复制路径与超时场景
- `tcp_util_timeout_test`：TCP 往返超时控制
- `raft_lock_test`：分布式锁
- `raft_mvcc_test`：MVCC 和写写冲突

多进程脚本回归包括：

- `scripts/verify_local_cluster.sh`：KV smoke、follower redirect、指标读取
- `scripts/verify_network_e2e.sh`：failover、锁、MVCC、成员变更

运行：

```bash
ctest --test-dir build --output-on-failure
```

如果想单独跑某个二进制，也可以直接执行：

```bash
./build/raft_prevote_test
./build/raft_linearizable_read_test
```

仓库还包含 GitHub Actions 持续集成配置：

- `.github/workflows/ci.yml`
- 默认流程：安装依赖、`cmake` 构建、`ctest` 回归，以及多进程 `scripts/verify_network_e2e.sh`

## 压测

当前仓库提供了多进程网络版的本地压测脚本：

```bash
./bench/run_network_bench.sh get
./bench/run_network_bench.sh put
```

脚本会：

- 自动拉起本地 3 节点集群
- 自动探测当前主节点
- 预热测试 key
- 将 `wrk` 原始输出保存到 `/tmp/distributed-kv-local-cluster/bench`

当前记录的一组本地结果见 [docs/performance.md](docs/performance.md)。截至 2026-05-07，这组本地基线约为：线性一致 `GET` `11.4k req/s`，`PUT` `281 req/s`。

说明：这组结果是单机回环网络上的 correctness-first 基线，用于观察链路与尾延迟，不适合作为跨机器或生产环境吞吐结论。

## 工程说明

- 成员变更当前通过两阶段配置日志完成 add / remove peer，接口仍然偏底层，节点 bootstrap / decommission 需要脚本配合
- 线性一致读采用 ReadIndex 风格多数派确认路径，未引入 lease read
- HTTP 接口与 TCP RPC 组件当前已经切到 `libuv` 事件循环与 `llhttp` 解析，但仍未覆盖鉴权、TLS、限流、批处理和 RPC 多路复用
- 压测结果基于单机回环网络，适合观察节点行为与本地性能特征
- 单进程 `distributed_kv_http_server` 启动时会清理既有 `/tmp` 数据目录，适合本地验证环境
