# Distributed KV Performance Notes

这份文档记录当前仓库一组可复现的本地 benchmark，目标是给秋招面试提供“我不只实现了功能，还真正拉起多进程集群跑过压测”的证据。

## 测试对象

- 可执行文件：`./build/distributed_kv_network_node`
- 运行形态：本机 loopback 上的 3 节点多进程集群
- 状态机后端：RocksDB
- 接口：
  - `GET /kv/bench`
  - `PUT /kv/bench`

说明：

- 每轮 benchmark 都在 fresh cluster 上单独运行，避免上一轮压测污染下一轮结果
- `GET /kv/<key>` 不是“leader 直接读本地内存”，当前实现会先做 leader 多数派确认，再读取状态机
- HTTP server 仍然是最小手写实现，响应默认 `Connection: close`

## 环境

- OS：macOS 14.7.2
- Kernel：Darwin 23.6.0 x86_64
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
- 探测当前 leader
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
- 多进程 loopback 结果比单进程演示入口更接近真实分布式形态，但仍然不是跨机器部署基准
- p99 明显高于 p50，说明在最小实现里尾延迟仍受连接关闭、线程调度、RocksDB 写入和 Raft RPC 往返影响

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

## 面试时怎么说

可以直接概括成下面这句话：

我用本地 3 节点多进程集群做过一轮 `wrk` 压测。线性一致 `GET` 大约 540 req/s，`PUT` 大约 217 req/s。因为 GET 不是裸本地读，而是先做多数派确认，所以它的吞吐不会像缓存那样高，但中位延迟仍明显低于 PUT。

## 局限

- 这是单机 loopback 环境，不是跨机器生产网络
- 当前 HTTP server 默认 `Connection: close`，没有 keep-alive、连接池和批量提交优化
- benchmark 更适合作为“做过性能验证”的证据，而不是对外宣称的最终性能指标
