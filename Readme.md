# 直播弹幕实时性系统（Go分布式架构）产品设计文档

## 1. 项目背景与目标

### 1.1 背景
随着直播行业的快速发展，弹幕作为直播互动的核心功能，对实时性、稳定性和高并发能力提出了极高要求。在高并发场景下（如万人以上同时在线），现有系统存在延迟高、消息堆积和部分消息丢失的问题，严重影响用户体验。

### 1.2 目标
- **核心目标**：设计并实现一个支持100万并发连接的弹幕实时性系统，确保消息从发送到分发的延迟控制在100ms以内。
- **业务目标**：
    - 提升用户体验：实现弹幕实时显示，无明显延迟。
    - 支持高并发：单场直播支持100万用户同时在线，每秒处理100万条弹幕消息。
    - 降低成本：优化数据传输和存储，减少带宽与存储开销。
- **技术目标**：
    - 消息分发延迟：≤100ms（P99）。
    - 系统吞吐量：≥100万条/秒。
    - 系统可用性：99.99%（年宕机时间≤52分钟）。

### 1.3 用户需求
- **主播**：实时查看观众弹幕，及时获取反馈。
- **观众**：发送的弹幕即时显示，与其他观众互动。
- **运营团队**：监控系统运行状态，快速发现并解决问题。

---

## 2. 系统架构设计

### 2.1 系统组件
| 组件名                        | 职责说明                                  |
|----------------------------|---------------------------------------|
| `edulive-gateway`          | WebSocket/QUIC接入，管理连接、协议解析、路由消息至Kafka |
| `edulive-client`           | 创建房间，发送弹幕，查看弹幕                        |
| `edulive-worker`           | 消费Kafka消息，执行解码、过滤、入库及排行榜更新            |
| `edulive-dispatcher`       | 从Redis拉取实时/热门弹幕，合并压缩后推送至客户端           |
| Redis Cluster              | 缓存排行榜、弹幕池，提供高并发读写                     |
| Kafka                      | 缓冲弹幕消息流，实现生产者与消费者异步解耦                 |
| CDN/QUIC                   | 分发弹幕至客户端，降低核心服务压力                     |
| OpenTelemetry + Prometheus | 全链路追踪、延迟监控、性能指标采集                     |

### 2.2 架构图
```
[edulive-client] → [edulive-gateway] → Kafka（按直播间分区）
       ↘ [edulive-worker] → Redis Cluster
       ↘ [edulive-dispatcher] → CDN/QUIC → [edulive-client]
监控：[OpenTelemetry + Prometheus]
```

### 2.3 数据流
1. **实时弹幕流**：`edulive-client → edulive-gateway → Kafka → edulive-worker → Redis → edulive-dispatcher → edulive-client`
2. **监控流**：各组件通过Prometheus Exporter暴露指标，OpenTelemetry实现全链路追踪。
3. **配置流**：支持本地热更新文件或Admin API动态管理配置。

---

## 3. 组件实现与技术选型

### 3.1 edulive-gateway
- **职责**：管理WebSocket连接（支持百万级并发），维护`LiveRoomID → Connection`映射，路由消息至Kafka。
- **关键点**：
    - 使用连接池多路复用，提升连接上限。
    - 基于Protobuf的二进制协议，减少带宽占用。
    - 内置健康检查与限流（IP/User级别）。

### 3.2 edulive-worker
- **职责**：消费Kafka消息，进行协议解析、时间戳校验、内容过滤，存储至Redis并更新排行榜。
- **优化点**：
    - Pipeline批量写Redis，提升吞吐量。
    - 按直播间分区并行处理，支持横向扩展。
    - Lua脚本实现频率控制，防止刷屏。

#### 代码实现：批量写入Redis
```go
func BatchWriteToRedis(votes []BulletMessage) error {
    pipe := redisClient.Pipeline()
    for _, vote := range votes {
        pipe.ZIncrBy(ctx, "LIVE:ranking", 1, fmt.Sprintf("%d", vote.UserID))
    }
    _, err := pipe.Exec(ctx)
    return err
}
```

#### Lua脚本：频率控制
```lua
-- KEYS[1]：用户发送频率key，KEYS[2]：排行榜key
local count = redis.call("INCR", KEYS[1])
if count > 10 then return 0 end -- 限制用户每秒发送频率
redis.call("EXPIRE", KEYS[1], 60)
return redis.call("ZINCRBY", KEYS[2], ARGV[1], ARGV[2])
```

### 3.3 edulive-dispatcher
- **职责**：管理QUIC连接，并每10ms从Redis拉取Top10000弹幕，合并压缩（zstd）后推送至客户端edulive-client。
- **推送策略**：
  - Dispatcher节点注册到Redis，维护心跳。
  - 客户端edulive-client连接时，随机选择Dispatcher。
  - 由Dispatcher直连客户端edulive-client，进行弹幕推送。


#### 代码实现：路由表管理
```go
type RouteTable struct {
    sync.RWMutex
    Users map[uint64]*websocket.Conn // key: 用户ID
}

func (rt *RouteTable) AddUser(userID uint64, conn *websocket.Conn) {
    rt.Lock()
    defer rt.Unlock()
    rt.Users[userID] = conn
}

func (rt *RouteTable) RemoveUser(userID uint64) {
    rt.Lock()
    defer rt.Unlock()
    delete(rt.Users, userID)
}
```

#### 代码实现：本地排行榜缓存
```go
type LocalRankCache struct {
    data   []RankItem
    expiry time.Time
    mutex  sync.RWMutex
}

func (c *LocalRankCache) GetTop100() []RankItem {
    if time.Now().After(c.expiry) {
        go c.refresh() // 异步刷新
        c.mutex.RLock()
        defer c.mutex.RUnlock()
        return c.data[:100]
    }
    c.mutex.RLock()
    defer c.mutex.RUnlock()
    return c.data[:100]
}

func (c *LocalRankCache) refresh() {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    items, err := redisClient.ZRevRangeWithScores(ctx, "LIVE:ranking", 0, 9999).Result()
    if err != nil {
        log.Printf("Failed to refresh ranking: %v", err)
        return
    }
    c.data = parseRankItems(items)
    c.expiry = time.Now().Add(10 * time.Millisecond)
}
```

#### 代码实现：批量消息分发
```go
func BatchSender(ch <-chan []byte) {
    buffer := make([]byte, 0, 100)
    timer := time.NewTicker(10 * time.Millisecond)
    for {
        select {
        case msg := <-ch:
            buffer = append(buffer, msg...)
            if len(buffer) >= 100 {
                sendBatch(buffer)
                buffer = buffer[:0]
            }
        case <-timer.C:
            if len(buffer) > 0 {
                sendBatch(buffer)
                buffer = buffer[:0]
            }
        }
    }
}

func sendBatch(buffer []byte) {
    err := quicClient.Send(buffer)
    if err != nil {
        log.Printf("Failed to send batch: %v", err)
    }
}
```

### 3.4 技术选型
| 组件         | 技术方案          | 理由                                      |
|--------------|-------------------|-------------------------------------------|
| WebSocket    | nhooyr.io/websocket | 支持百万并发，低内存占用                  |
| 消息队列     | Kafka             | 高吞吐量、可靠性，分布式分区              |
| 实时存储     | Redis Cluster     | SortedSet支持排行榜，高并发读写           |
| 序列化       | Protobuf          | 数据量减少60%，编解码高效                |
| 传输协议     | QUIC              | 低延迟、高吞吐，支持快速连接恢复          |
| 监控         | OpenTelemetry     | 分布式追踪，实时监控性能                  |

### 3.5 二进制协议设计
#### 协议格式
| 字段    | 长度（字节） | 描述                    |
|---------|--------------|-------------------------|
| 魔数    | 2            | `0xABCD`，标识协议开始   |
| 版本    | 1            | 当前为`0x01`            |
| 类型    | 1            | `0x01`（弹幕）/`0x02`（心跳） |
| 时间戳  | 8            | Unix毫秒时间戳          |
| 直播间ID | 8            | 直播间标识              |
| 用户ID  | 8            | 发送者ID                |
| 内容长度 | 2            | 弹幕内容字节数（N）      |
| 内容    | N            | UTF-8编码字符串         |

- **总长度**：22 + N字节。
- **示例**：`"Hello"`（用户ID: 123456，时间戳: 1696118400000）编码为：
  ```
  ABCD 01 01 0000018A2B8C4C00 00000000001E240 00000000001E240 0005 48656C6C6F
  ```

#### Go实现
```go
type BulletMessage struct {
    Magic      uint16
    Version    uint8
    Type       uint8
    Timestamp  int64
    LiveID     uint64
    UserID     uint64
    ContentLen uint16
    Content    string
}

const (
    MagicNumber   = 0xABCD
    CurrentVersion = 0x01
    TypeBullet    = 0x01
    TypeHeartbeat = 0x02
)

func ParseBulletMessage(data []byte) (*BulletMessage, error) {
    if len(data) < 22 {
        return nil, errors.New("data too short")
    }
    reader := bytes.NewReader(data)
    msg := &BulletMessage{}

    if err := binary.Read(reader, binary.BigEndian, &msg.Magic); err != nil {
        return nil, err
    }
    if msg.Magic != MagicNumber {
        return nil, errors.New("invalid magic number")
    }

    if err := binary.Read(reader, binary.BigEndian, &msg.Version); err != nil {
        return nil, err
    }
    if msg.Version != CurrentVersion {
        return nil, errors.New("unsupported version")
    }

    if err := binary.Read(reader, binary.BigEndian, &msg.Type); err != nil {
        return nil, err
    }
    if msg.Type != TypeBullet && msg.Type != TypeHeartbeat {
        return nil, errors.New("invalid message type")
    }

    if err := binary.Read(reader, binary.BigEndian, &msg.Timestamp); err != nil {
        return nil, err
    }

    if err := binary.Read(reader, binary.BigEndian, &msg.LiveID); err != nil {
        return nil, err
    }

    if err := binary.Read(reader, binary.BigEndian, &msg.UserID); err != nil {
        return nil, err
    }

    if err := binary.Read(reader, binary.BigEndian, &msg.ContentLen); err != nil {
        return nil, err
    }

    content := make([]byte, msg.ContentLen)
    if err := binary.Read(reader, binary.BigEndian, &content); err != nil {
        return nil, err
    }
    msg.Content = string(content)

    return msg, nil
}

func (msg *BulletMessage) Encode() ([]byte, error) {
    buf := new(bytes.Buffer)

    if err := binary.Write(buf, binary.BigEndian, msg.Magic); err != nil {
        return nil, err
    }
    if err := binary.Write(buf, binary.BigEndian, msg.Version); err != nil {
        return nil, err
    }
    if err := binary.Write(buf, binary.BigEndian, msg.Type); err != nil {
        return nil, err
    }
    if err := binary.Write(buf, binary.BigEndian, msg.Timestamp); err != nil {
        return nil, err
    }
    if err := binary.Write(buf, binary.BigEndian, msg.LiveID); err != nil {
        return nil, err
    }
    if err := binary.Write(buf, binary.BigEndian, msg.UserID); err != nil {
        return nil, err
    }
    if err := binary.Write(buf, binary.BigEndian, msg.ContentLen); err != nil {
        return nil, err
    }
    if err := binary.Write(buf, binary.BigEndian, []byte(msg.Content)); err != nil {
        return nil, err
    }

    return buf.Bytes(), nil
}
```

#### 使用示例
**客户端发送**：
```go
msg := &BulletMessage{
    Magic:      MagicNumber,
    Version:    CurrentVersion,
    Type:       TypeBullet,
    Timestamp:  time.Now().UnixMilli(),
    LiveID:     123456,
    UserID:     123456,
    Content:    "Hello",
    ContentLen: uint16(len("Hello")),
}

data, err := msg.Encode()
if err != nil {
    log.Printf("Encode error: %v", err)
    return
}

err = wsConn.WriteMessage(websocket.BinaryMessage, data)
if err != nil {
    log.Printf("Send error: %v", err)
}
```

**服务端接收**：
```go
_, data, err := wsConn.ReadMessage()
if err != nil {
    log.Printf("Read error: %v", err)
    return
}

msg, err := ParseBulletMessage(data)
if err != nil {
    log.Printf("Parse error: %v", err)
    return
}

log.Printf("Received bullet: user=%d, content=%s", msg.UserID, msg.Content)
```

### 3.6 弹幕过滤机制
| 类型         | 实现方式                                    |
|--------------|---------------------------------------------|
| 敏感词过滤   | Trie树或Aho-Corasick算法，快速检测          |
| 重复限制     | 5s内重复弹幕丢弃（用户/内容维度）           |
| 用户等级限制 | 低等级用户限制广告弹幕（基于标签）          |
| AI辅助       | 可扩展接入LLM分析语义，判断情绪/攻击性      |
| 黑名单管理   | Redis维护用户/IP封禁，支持热更新            |

#### 代码实现：频率控制
```go
func RateLimit(userID string) bool {
    result, err := redisClient.Do(ctx, "CL.THROTTLE", userID, "10", "60", "1").Result()
    if err != nil {
        log.Printf("Rate limit error: %v", err)
        return false
    }
    return result.([]interface{})[0].(int64) == 0
}
```

---

## 4. 性能优化

| 模块              | 优化措施                                       |
|-------------------|------------------------------------------------|
| WebSocket接入     | 连接多路复用，自动回收空闲连接                 |
| Kafka → Redis    | 批量消费、异步处理、Pipeline写入              |
| Redis → Dispatcher | 本地缓存Top10000，减少Redis访问              |
| Dispatcher → 客户端 | zstd压缩，QUIC推送降低握手与丢包             |
| 数据压缩         | 二进制协议+zstd，消息体积减少80%              |
| 批量处理         | 10ms批量拉取与发送，减少请求次数              |

---

## 5. 运维与监控

### 5.1 部署方案
- **WebSocket接入**：10台服务器，每台支持10万连接。
- **Kafka集群**：3节点，3副本。
- **Redis Cluster**：6节点（3主3从），分片存储。
- **Go处理模块**：Kubernetes部署，支持动态扩缩容。

### 5.2 监控指标
- 延迟（目标：≤100ms）
- 吞吐量（目标：≥100万条/秒）
- 错误率（目标：≤0.01%）
- 资源使用率（CPU/内存/带宽）
- 协议解析错误（目标：≤0.001%）

### 5.3 应急预案
1. **Kafka积压**：扩容消费者，优先处理热门直播间。
2. **Redis压力**：增加从节点，启用本地缓存。
3. **连接断开**：客户端5s重连，记录日志分析。
4. **协议失败**：记录日志，忽略异常消息。

---

## 6. 测试与评估

### 6.1 测试项
- **功能**：协议解析、弹幕路由分发、排行榜更新。
- **性能**：100万连接稳定性，吞吐量≥1M TPS，延迟≤100ms。
- **安全**：敏感词命中率、重复/刷屏策略有效性。

### 6.2 成功指标
- 延迟：≤100ms（P99）。
- 吞吐量：≥100万条/秒。
- 可用性：99.99%。
- 用户满意度：延迟投诉减少90%。
- 带宽节省：比JSON节省60%以上。

---

## 7. 未来优化方向
1. 热点自动分片与调度。
2. LLM语义感知，识别有害弹幕。
3. 个性化弹幕推送与过滤。
4. eBPF监控网络丢包。
5. 全球分布式部署+QUIC CDN加速。