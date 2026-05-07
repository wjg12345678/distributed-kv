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
- HTTP 服务仍然是最小手写实现，响应默认 `Connection: close`

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
| `GET /kv/bench` | 16 | 15s | 539.31 | 1.64ms | 7.71ms | 134.60ms |
| `PUT /kv/bench` | 8 | 15s | 216.79 | 4.67ms | 13.23ms | 154.77ms |

补充观察：

- GET 的中位延迟明显低于 PUT，但它仍然要先做多数派确认，所以吞吐不会像“裸本地读”那样高
- PUT 吞吐显著低于 GET，因为它要真正走日志复制、提交和状态机 apply
- 多进程回环网络结果比单进程本地入口更接近真实分布式形态，但仍然不是跨机器部署基准
- p99 明显高于 p50，说明在最小实现里尾延迟仍受连接关闭、线程调度、RocksDB 写入和 Raft RPC 往返影响
- 这组数字更适合回答“当前实现的链路代价在哪里”，不适合直接包装成通用性能卖点

## 原始输出

### GET

```text
Running 15s test @ http://127.0.0.1:9201
  4 threads and 16 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     5.85ms   22.94ms 295.84ms   97.30%
    Req/Sec     1.07k   363.04     1.66k    80.00%
  Latency Distribution
     50%    1.64ms
     75%    2.01ms
     90%    7.71ms
     99%  134.60ms
  8144 requests in 15.10s, 787.36KB read
Requests/sec:    539.31
Transfer/sec:     52.14KB
```

### PUT

```text
Running 15s test @ http://127.0.0.1:9201
  4 threads and 8 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    12.15ms   31.33ms 408.33ms   94.04%
    Req/Sec   234.93    101.45   444.00     64.44%
  Latency Distribution
     50%    4.67ms
     75%    5.45ms
     90%   13.23ms
     99%  154.77ms
  3255 requests in 15.01s, 282.91KB read
Requests/sec:    216.79
Transfer/sec:     18.84KB
```

## 结果解读

本地 3 节点多进程集群上一组 `wrk` 基线结果如下：线性一致 `GET` 约 540 req/s，`PUT` 约 217 req/s。由于 `GET` 仍需先完成多数派确认，因此吞吐不会接近纯本地缓存读；而 `PUT` 需要完整经过复制、提交和状态机应用路径，所以整体延迟更高。更准确地说，这组数据描述的是“当前最小网络实现的本地基线”，不是系统的最终性能上限。

## 测试说明

- 这是单机回环网络环境，不是跨机器生产网络
- 当前 HTTP 服务默认 `Connection: close`，没有 keep-alive、连接池和批量提交优化
- 这些结果更适合作为本地性能基线和链路观察数据，而不是跨环境横向对比指标
