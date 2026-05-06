# Distributed KV Architecture

这份文档面向“快速讲清项目结构”的场景，适合在面试前过一遍，或者直接给面试官展示。

## 总体架构图

```mermaid
flowchart LR
  Client[Client / curl / benchmark]

  subgraph Service[Node Service]
    HTTP[HTTP API]
    Raft[RaftNode]
    RPC[TCP RPC + Protobuf]
    Metrics[/metrics]
  end

  subgraph StateMachine[Replicated State Machine]
    KV[KV]
    Lock[Lock Service]
    MVCC[MVCC]
  end

  subgraph Storage[Persistence]
    Meta[Raft Meta]
    Snap[Snapshot]
    Rocks[RocksDB]
  end

  subgraph Cluster[Peer Nodes]
    Peer1[Peer 1]
    Peer2[Peer 2]
  end

  Client --> HTTP
  HTTP --> Raft
  HTTP --> Metrics
  Raft --> RPC
  RPC --> Peer1
  RPC --> Peer2
  Raft --> KV
  Raft --> Lock
  Raft --> MVCC
  Raft --> Meta
  Raft --> Snap
  Meta --> Rocks
  Snap --> Rocks
  KV --> Rocks
  Lock --> Rocks
  MVCC --> Rocks
```

## 模块职责

- `RaftNode`：选举、日志复制、commit、apply、snapshot、成员关系维护
- `IRaftTransport` / `NetworkTransport`：抽象传输层，支持单进程 mock 和真实 TCP 网络版
- `NodeHttpServer` / `HttpServer`：暴露 KV、锁、MVCC、状态和指标接口
- `RocksDbKeyValueStore`：持久化状态机数据
- `RocksDbRaftPersistence`：持久化 `current_term`、`voted_for`、日志和 snapshot 元数据

## 写路径

```mermaid
sequenceDiagram
  participant C as Client
  participant L as Leader
  participant F1 as Follower 1
  participant F2 as Follower 2
  participant SM as State Machine

  C->>L: PUT /kv/key
  L->>L: append log entry
  L->>F1: AppendEntries
  L->>F2: AppendEntries
  F1-->>L: ack
  F2-->>L: ack
  L->>L: majority commit
  L->>SM: apply committed entry
  L-->>C: 200 stored
```

## 读路径

当前实现不是“leader 直接读本地内存”，而是 ReadIndex 风格的线性一致读：

```mermaid
sequenceDiagram
  participant C as Client
  participant L as Leader
  participant F1 as Follower 1
  participant F2 as Follower 2
  participant SM as State Machine

  C->>L: GET /kv/key
  L->>F1: heartbeat / quorum confirm
  L->>F2: heartbeat / quorum confirm
  F1-->>L: ack
  F2-->>L: ack
  L->>SM: read local state machine
  L-->>C: 200 value
```

如果 leader 失去多数派，这个 quorum confirm 会失败，节点会拒绝读请求，而不是返回旧数据。

## 分区 / 故障恢复流程图

这个流程对应当前仓库里已经实现并测试过的 `Pre-Vote + 恢复追平` 场景。

```mermaid
sequenceDiagram
  participant L as Current Leader
  participant M as Majority Follower
  participant I as Isolated Follower

  Note over L,M,I: 集群已经完成选主并稳定运行
  L->>M: AppendEntries / Heartbeat
  L--xI: Heartbeat lost due to partition

  Note over I: election timeout
  I->>L: PreVote(term+1)
  I->>M: PreVote(term+1)
  L--xI: no response
  M--xI: no response
  Note over I: 无法拿到多数派，不进入正式选举，不抬高 term

  L->>M: continue heartbeat and replication
  Note over L,M: 多数派一侧继续稳定提供写入和线性一致读

  Note over I: 网络恢复
  L->>I: AppendEntries or InstallSnapshot
  I-->>L: ack
  Note over I: 追平日志 / snapshot 后重新加入集群
```

## 面试时可以这样讲

- 架构上我把“共识层、传输层、状态机、持久化层、接口层”拆开了，所以既能跑单进程教学版，也能跑多进程真实网络版
- 写路径强调的是“日志复制后提交再 apply”
- 读路径强调的是“leader 先做多数派确认再读”
- 恢复路径强调的是“隔离节点先 Pre-Vote，不乱抬 term；恢复后由 leader 通过 AppendEntries 或 InstallSnapshot 追平”
