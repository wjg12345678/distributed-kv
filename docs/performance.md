# 分布式 KV 性能记录

这份文档记录一组可复现的本地压测基线，用于观察多进程集群在回环网络下的吞吐和延迟特征，而不是给出跨机器或生产环境的性能结论。

## 测试对象

- 可执行文件：`./build/distributed_kv_network_node`
- 运行形态：本机回环网络上的 3 节点多进程集群
- 状态机后端：RocksDB
- 接口：
  - `GET /kv/bench`
  - `PUT /kv/bench`

说明：

- 每轮压测都在全新集群上单独运行，避免上一轮压测污染下一轮结果
- `GET /kv/<key>` 不是“leader 直接读本地内存”，当前实现会先做 leader 多数派确认，再读取状态机
- HTTP 接入层基于 `libuv + llhttp`，默认支持 keep-alive
- Raft RPC client/server 基于 `libuv`，并复用 peer 间长连接，而不是每次请求新建 TCP 连接

## 环境

- 操作系统：macOS 14.7.2
- 内核：Darwin 23.6.0 x86_64
- 日期：2026-05-07

## 复现方式

先构建：

```bash
cmake -S . -B build
cmake --build build -j
```

然后分别运行 GET / PUT 压测：

```bash
./bench/run_network_bench.sh get
./bench/run_network_bench.sh put
```

脚本会自动：

- 拉起本地 3 节点集群
- 探测当前主节点
- 预热 `bench` key
- 将原始输出保存到 `/tmp/distributed-kv-local-cluster/bench/get.txt` 和 `/tmp/distributed-kv-local-cluster/bench/put.txt`

## 结果

| 场景 | 并发 | 时长 | Requests/sec | p50 | p90 | p99 |
|------|------|------|--------------|-----|-----|-----|
| `GET /kv/bench` | 16 | 15s | 11438.37 | 1.23ms | 1.99ms | 43.03ms |
| `PUT /kv/bench` | 8 | 15s | 280.51 | 2.67ms | 4.09ms | 8.18ms |

补充观察：

- `GET` 相比旧版手写 HTTP + `Connection: close` 基线有明显提升，主要收益来自 keep-alive、`llhttp` 解析和连接复用
- `PUT` 只做了温和提升，说明写路径仍然主要受 Raft 复制、提交等待和 RocksDB apply 成本影响
- `GET` 的中位延迟低于 `PUT`，但它仍然不是纯本地缓存读，而是带多数派确认的线性一致读
- 这仍然是单机回环网络结果，适合观察实现链路，不适合作为跨机器部署结论

## 原始输出

### GET

```text
Running 15s test @ http://127.0.0.1:9201
  4 threads and 16 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.59ms    7.59ms 119.68ms   96.49%
    Req/Sec     2.88k   667.06     4.00k    78.06%
  Latency Distribution
     50%    1.23ms
     75%    1.55ms
     90%    1.99ms
     99%   43.03ms
  172665 requests in 15.10s, 17.13MB read
Requests/sec:  11438.37
Transfer/sec:      1.13MB
```

### PUT

```text
Running 15s test @ http://127.0.0.1:9202
  4 threads and 8 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     3.15ms    3.97ms  76.35ms   98.82%
    Req/Sec   619.37    214.22     0.97k    70.59%
  Latency Distribution
     50%    2.67ms
     75%    3.33ms
     90%    4.09ms
     99%    8.18ms
  4234 requests in 15.09s, 388.67KB read
Requests/sec:    280.51
Transfer/sec:     25.75KB
```

## 结果解读

本地 3 节点多进程集群当前一组 `wrk` 基线结果为：线性一致 `GET` 约 `11.4k req/s`，`PUT` 约 `281 req/s`。这组数据说明网络热路径从“每次新建连接、手写 HTTP 解析”切换到“`libuv + llhttp + keep-alive`”后，读路径的连接与解析开销显著下降；但写路径仍然需要完整经过复制、提交和状态机应用，所以提升幅度明显更小。

## 测试说明

- 这是单机回环网络环境，不是跨机器生产网络
- 当前结果已经不再受 `Connection: close` 和每 RPC 新建 TCP 连接的旧实现约束
- 现阶段仍然没有做请求批处理、RPC 多路复用、鉴权、TLS 和限流
- 这些结果更适合作为当前实现的本地性能基线，而不是跨环境横向对比指标
