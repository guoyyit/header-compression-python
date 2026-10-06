# header-compression-python

连接层有状态头部压缩（QPACK 风格）：静态表 + 动态表、逐连接压缩上下文、
阻塞流恢复、容量协商与并发安全，并集成到 FastAPI 的请求处理与连接生命周期。

## 架构

```
header_compression/
├── integers.py     前缀整数编解码（RFC 7541 §5.1 风格）
├── strings.py      字符串字面量编解码（长度前缀 + UTF-8，无 Huffman，确定性）
├── tables.py       静态表（61 项）与动态表（容量、FIFO 逐出、绝对索引）
├── wire.py         线格式原语：字段行 / 编码器指令 / 解码器指令 / 块前缀
├── encoder.py      EncoderEndpoint：编码头部块，发出插入/容量指令，跟踪 ack
├── decoder.py      DecoderEndpoint：按序应用指令，解码/停放头部块，发出 ack
├── connection.py   ConnectionCompressionContext：每连接的编码+解码端点对、
│                   编码器流序号整形（乱序缓冲、重复丢弃）、生命周期与错误记录
└── registry.py     ConnectionRegistry：连接级上下文的创建、重置、关闭
main.py             FastAPI 集成：中间件解析连接 id，REST 端点驱动压缩层
```

每个连接持有**一对**端点：本端编码器（出站头部）与本端解码器（入站头部）。
上下文之间不共享任何可变状态（静态表不可变），连接之间完全隔离。

## 同步语义

### 插入顺序唯一且确定

动态表项只通过**编码器流指令**插入，解码端严格按指令到达顺序应用。
表项以绝对索引寻址（从 0 单调递增），两端对同一索引始终解析到同一表项。
任一时刻的解码状态都对应插入序列的某个确定前缀。

连接层为每个编码器流批次分配单调序号 `seq`：乱序批次被缓冲、重复批次被
丢弃，缺失序号到达前后续批次不会生效——乱序与重复传输不会使两端表状态分歧。

### 头部块与阻塞

头部块前缀携带：

- **Required Insert Count (RIC)**：本块引用的最大绝对索引 + 1；
- **块序号**：连接级单调，解码端在 ack 中原样回传，使乱序解码的 ack
  也能精确匹配（阻塞流恢复后解码顺序与编码顺序无关）。

解码端收到 `RIC > received_insert_count` 的块时，将该流**逻辑停放**
（保存字节，不占线程、不持锁等待），依赖到达后自动解码并按流 id 顺序
返回结果。无依赖的流不受影响。停放数量上限由
`max_blocked_streams` 配置，超限抛出 `BlockedStreamLimitError`——
等待有界且可配置，结构上无死锁（不存在跨锁等待）。

只有引用了动态表项（RIC > 0）的块会被确认（ack）；纯静态块无需确认，
重复投递的块由有限结果缓存去重，不会重复 ack。

### 容量协商

- 解码端的最大容量即内存上限（`max_table_capacity`）；编码端不得超过，
  否则 `CapacityExceededError`。
- 容量通过编码器流的 `Set Dynamic Table Capacity` 指令调整，**生效时点
  是指令流中的确定位置**，两端在同一插入序位置应用，逐出集合一致。
- 编码端不会逐出仍被未确认头部块引用的表项：插入触发逐出前做守卫检查，
  不可逐出则退化为字面量编码（不插入）；容量下调若会逐出被引用表项则抛出
  `CapacityReductionBlockedError`，调用方在 ack 到达后重试。因此调整期间
  不会出现无法解码的引用，也不会静默丢弃后续解码所需的状态。

### 内存上限与淘汰

表项大小 = 名字字节 + 值字节 + 32（记账开销）。插入使表超出容量时按
FIFO 逐出最旧表项；单项超过当前容量时解码端抛 `EntryTooLargeError`、
编码端自动退化为字面量。各类拒绝原因均为可区分的异常类型（见下）。

### 错误模型

所有异常派生自 `CompressionError`，可观测（异常 + 连接上下文内
`record_error` 的有界记录）且可复现（相同输入产生相同错误）：

| 类型 | 含义 |
| --- | --- |
| `TruncatedDataError` | 输入截断 |
| `IntegerDecodingError` | 整数溢出/畸形 |
| `StringDecodingError` | 字符串编码非法 |
| `InvalidRepresentationError` | 保留位模式/未知表示 |
| `UnknownIndexError` | 引用已逐出或从未插入的表项 |
| `CapacityExceededError` | 容量超过上限 |
| `EntryTooLargeError` | 单项超过表容量 |
| `CapacityReductionBlockedError` | 容量下调被未确认引用阻塞 |
| `BlockedStreamLimitError` | 阻塞流数量超限 |
| `DecoderStreamError` | ack/计数增量协议错误 |

单个连接的解码失败只影响该连接自己的上下文；其他连接的编码状态不受污染。

### 确定性

相同输入序列（含相同的 ack 投递顺序）产生**逐字节一致**的输出：
静态表精确命中优先于动态表、动态表最新命中优先、插入策略与逐出守卫
均为纯函数式决策。测试 `TestDeterminism` 固化该性质。

## 配置项

`CompressionConfig`（也可经环境变量注入 FastAPI 层）：

| 字段 | 环境变量 | 默认 | 含义 |
| --- | --- | --- | --- |
| `max_table_capacity` | `HC_MAX_TABLE_CAPACITY` | 4096 | 动态表硬上限（字节） |
| `table_capacity` | — | 4096 | 本端编码器开启表时使用的容量 |
| `max_blocked_streams` | `HC_MAX_BLOCKED_STREAMS` | 64 | 阻塞流等待上限（有界） |
| `sensitive_headers` | — | authorization / proxy-authorization / cookie | 永不索引、永不入表的头部名 |

## HTTP API

连接 id 解析顺序：路径 `/connections/{id}` → 请求头 `X-Connection-ID`
→ 自动生成（响应头回传）。压缩上下文在**首次压缩操作时**确定性地创建；
`POST /connections/{id}/reset` 确定性重建；`DELETE /connections/{id}`
释放且幂等。

| 方法/路径 | 说明 |
| --- | --- |
| `GET /` | 原有示例端点 |
| `GET /connections` | 活动连接列表 |
| `GET /connections/{id}/state` | 压缩状态快照（表、计数、阻塞流、错误记录） |
| `POST /connections/{id}/encode` | 编码头部：`{stream_id, headers}` → `{block, encoder_stream}` |
| `POST /connections/{id}/encoder-stream` | 投递编码器流批次 `{seq, data(hex)}`，返回被解除阻塞的流 |
| `POST /connections/{id}/streams/{sid}/headers` | 解码头部块 `{block(hex)}`，返回解码结果或 `blocked` |
| `POST /connections/{id}/reset` | 重置压缩状态 |
| `DELETE /connections/{id}` | 关闭并释放（不存在返回 404） |

压缩错误以 HTTP 422 返回 `{error, reason, connection_id}`，并记录到该连接
的错误日志（`GET .../state` 可见，有界 256 条）。

## 运行

```bash
uv sync
uv run pytest            # 141 个测试
uv run uvicorn main:app  # 启动服务
```

测试覆盖：容量淘汰与调整（`test_tables.py` / `test_capacity.py`）、
阻塞流恢复与有界等待（`test_blocked.py`）、并发与乱序/重复更新
（`test_concurrency.py`）、异常与截断输入（`test_errors.py`）、
编解码确定性与往返（`test_codec.py`）、HTTP 集成（`test_api.py`）。
