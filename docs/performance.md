# Distributed KV Performance Notes

这份文档记录一次可复现的本地压测结果，目标是给秋招面试提供“我不只实现了功能，还实际跑过压测”的证据。

## 测试对象

- 可执行文件：`./build/distributed_kv_http_server`
- 运行形态：单进程 3 节点集群
- 状态机后端：RocksDB
- 接口：
  - `GET /kv/bench`
  - `PUT /kv/bench`

说明：

- 这个入口本质上仍然是 Raft 集群，只是 3 个节点运行在同一进程里，便于稳定复现
- `GET /kv/<key>` 不是“裸本地读”，当前实现会先做 leader 多数派确认，再读取本地状态机
- 因为 HTTP server 是最小手写实现，响应默认 `Connection: close`，压测脚本也显式带了 `Connection: close`

## 环境

- OS：macOS 14.7.2
- Kernel：Darwin 23.6.0 x86_64
- 日期：2026-05-06

## 压测脚本

仓库中提供了可复现脚本：

- [bench/wrk_get.lua](/Users/mac/distributed-kv/bench/wrk_get.lua)
- [bench/wrk_put.lua](/Users/mac/distributed-kv/bench/wrk_put.lua)

## 压测方法

先启动服务：

```bash
./build/distributed_kv_http_server
```

预热并写入测试 key：

```bash
curl -X PUT http://127.0.0.1:9006/kv/bench -d 'benchmark-value'
```

读压测：

```bash
wrk -t4 -c16 -d15s --latency -s bench/wrk_get.lua http://127.0.0.1:9006
```

写压测：

```bash
wrk -t4 -c8 -d15s --latency -s bench/wrk_put.lua http://127.0.0.1:9006
```

## 结果

| 场景 | 并发 | 时长 | Requests/sec | p50 | p90 | p99 |
|------|------|------|--------------|-----|-----|-----|
| `GET /kv/bench` | 16 | 15s | 1087.74 | 321us | 442us | 600us |
| `PUT /kv/bench` | 8 | 15s | 1080.61 | 1.58ms | 3.67ms | 41.59ms |

补充观察：

- GET 的中位延迟显著低于 PUT，因为 PUT 需要真正提交日志并应用状态机
- GET 吞吐没有明显高于 PUT，是因为当前实现的线性一致读需要先做多数派确认，不是“leader 直接本地读”
- PUT 的 p99 明显高于 p50，说明在最小实现里尾延迟受日志复制、线程调度和 RocksDB 写入影响更大

## 原始输出

### GET

```text
Running 15s test @ http://127.0.0.1:9006
  4 threads and 16 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   336.68us   81.46us   1.03ms   73.73%
    Req/Sec     3.72k     1.89k    5.05k    81.82%
  Latency Distribution
     50%  321.00us
     75%  379.00us
     90%  442.00us
     99%  600.00us
  16323 requests in 15.01s, 1.54MB read
Requests/sec:   1087.74
Transfer/sec:    105.16KB
```

### PUT

```text
Running 15s test @ http://127.0.0.1:9006
  4 threads and 8 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.90ms    8.15ms 137.29ms   97.62%
    Req/Sec     0.92k   305.05     1.37k    74.16%
  Latency Distribution
     50%    1.58ms
     75%    2.43ms
     90%    3.67ms
     99%   41.59ms
  16311 requests in 15.09s, 1.38MB read
Requests/sec:   1080.61
Transfer/sec:     93.92KB
```

## 面试时怎么说

可以直接概括成下面这句话：

我用本地 3 节点单进程集群做过一轮 `wrk` 压测，GET 和 PUT 都大约在 1k req/s 量级。因为 GET 当前实现的是线性一致读，不是裸本地读，所以它的吞吐没有和 PUT 拉开数量级差距，但中位延迟仍明显更低。

## 局限

- 这是开发机上的本地 benchmark，不是生产环境基准
- 测试对象是单进程 HTTP server，不是多进程跨机器部署
- 当前 HTTP 实现是最小版本，没有 keep-alive、连接池、线程模型优化和批量提交
- 结果更适合作为“做过性能验证”的证据，而不是对外宣称的最终性能指标
