# SseModule

SSE（Server-Sent Events）长连接模块，用于接收服务端事件流推送。

一次接入即可获得生产级的完整长连接生命周期管理——断线重连、心跳看门狗、断点续传、断连恢复通知均由模块内部统一处理，业务层只需关心「连上、收帧、关掉」三件事。

## 架构

```
业务代码（commonMain，四端同一份调用代码）
        │
        ▼
core SseModule（commonMain：重连/退避/看门狗/Last-Event-ID 统一实现）
        │ toNative("connect" / "close")
        ▼
各端 KRSseModule（native：流式 HTTP 读取 + SSE 协议解析 + 分帧）
        │ open / data / heartbeat / error 帧事件
        ▼
core NotifyModule 持久订阅通道（"KRSseModule.frame"）→ 回传 core SseModule 分发
```

帧投递刻意不使用 `toNative(keepCallbackAlive = true)` 的单次回调——该回调在连接出错后可能被渲染层回收导致丢帧；NotifyModule 持久订阅是 core 为「native 重复推送」设计的官方通道，连接重试后依然可达。

## 内置能力

| 能力 | 说明 |
|:----|:----|
| 标准 SSE 解析 | `\n\n` 分帧；同一帧多行 `data:` 按规范以 `\n` 拼接；`id:` 行自动追踪 |
| 多字节 UTF-8 安全 | iOS 按字节缓冲拼完整帧再解码；HarmonyOS 用流式 TextDecoder；中文/emoji 跨 chunk 不乱码 |
| 断线自动重连 | 指数退避：第 n 次延迟 = ``reconnectBaseDelayMs`` × 2^(n-1)，封顶 ``reconnectMaxDelayMs``；可用 ``maxReconnectAttempts`` 限次 |
| 连接建立事件 | native 收到 2xx 响应头（H5 为 ``EventSource.onopen``）上报 ``open``，**复位重连计数**——否则空闲但健康的连接会持续累积重连次数直到耗尽 |
| 断连恢复通知 | ``connect`` 的 ``onReconnected`` 回调：断连后重连成功时触发一次（首次连接不触发）。断连窗口内丢失的帧服务端通常不补发，上层可在此回调内做全量补拉 |
| 心跳看门狗 | 超过 ``heartbeatTimeoutMs`` 无任何帧（数据帧/心跳帧/open 均计入）判为断线并触发重连；``<= 0`` 关闭 |
| 服务端心跳透传 | native 将纯注释帧（``: ping``）透传为 ``heartbeat`` 事件喂看门狗。**注意 H5 浏览器 EventSource 会吞掉注释帧**（见下文「服务端配合要求」） |
| Last-Event-ID 断点续传 | 自动追踪最近帧 id，重连请求自动携带 ``Last-Event-ID`` 头，服务端可据此补发 |
| EOF 快速失败 | 服务端正常关闭连接（读到流末尾）时 Android 端立即上报走重连，不必等看门狗超时 |

## connect方法

建立 SSE 连接。重复调用会先关闭旧连接并重置重连计数。每收到一条 data 帧回调一次 ``onEvent``；重连次数耗尽或连接最终失败时回调一次 ``onError``；断连后重连成功时回调一次 ``onReconnected``（首次连接不触发）。

<br/>

**参数**

| 参数  | 描述     | 类型 |
|:----|:-------|:--|
| url <Badge text="必需" type="warn"/> | SSE 服务端地址  | String |
| headers <Badge text="非必需" type="warn"/> | 请求头参数（H5 端 EventSource 不支持自定义请求头，该参数仅原生三端生效）  | JSONObject |
| options <Badge text="非必需" type="warn"/> | 连接选项，见 [SseOptions](#sseoptions)，全部有默认值  | SseOptions |
| onEvent <Badge text="必需" type="warn"/> | 事件回调闭包，每条 data 帧一次  | SseEventCallback |
| onError <Badge text="必需" type="warn"/> | 错误回调闭包，重连耗尽后调用一次  | SseErrorCallback |
| onReconnected <Badge text="非必需" type="warn"/> | 断连恢复回调闭包：断连后重连成功时触发一次，供上层补拉断连窗口丢失的数据  | (() -> Unit)? |

**示例**

```kotlin
@Page("test")
class SseTestPage : Pager() {

    private lateinit var sse: SseModule

    override fun created() {
        super.created()
        sse = acquireModule<SseModule>(SseModule.MODULE_NAME)
        sse.connect(
            url = "https://api.example.com/events?token=xxx",
            options = SseOptions(heartbeatTimeoutMs = 30000),
            onEvent = { data ->
                // 每收到一条 data 帧回调一次
            },
            onError = { msg ->
                // 连接最终失败（重连次数耗尽）
            },
            onReconnected = {
                // 断连后重连成功：断连窗口内丢失的帧服务端不补发，
                // 这里做一次全量数据拉取自愈
                reloadAll()
            },
        )
    }

    override fun pageWillDestroy() {
        super.pageWillDestroy()
        sse.close()
    }

}
```

::: tip 提示
服务端需要鉴权时，由于浏览器 EventSource 不能设置请求头，token 通常通过 query 传递（如上例的 ``?token=xxx``）；其余端可用 ``headers`` 传鉴权头
:::

## close方法

关闭连接并停止一切重连/看门狗（幂等）

<br/>

**参数**

无

## 类型说明

### SseOptions

SSE 连接选项（全部有默认值，零配置可用）

| 参数  | 描述     | 类型 | 默认值 |
|:----|:-------|:--|:--|
| autoReconnect | 断线自动重连  | Boolean | true |
| maxReconnectAttempts | 最大重连次数（-1 不限；耗尽后回调 ``onError``）  | Int | -1 |
| reconnectBaseDelayMs | 退避基数 ms：第 n 次重连延迟 = base × 2^(n-1)，封顶 ``reconnectMaxDelayMs``  | Int | 1000 |
| reconnectMaxDelayMs | 退避上限 ms  | Int | 30000 |
| heartbeatTimeoutMs | 心跳超时 ms：超过该时长无任何帧（含服务端心跳）判为断线并触发重连；<= 0 关闭看门狗。应大于服务端心跳间隔  | Int | 45000 |

### SseEventCallback

```kotlin
SseEventCallback = (data: String) -> Unit
```

### SseErrorCallback

```kotlin
SseErrorCallback = (message: String) -> Unit
```

## 服务端配合要求

::: warning 重要
开启看门狗（默认 45s）时，服务端必须在空闲期发送心跳，间隔建议 25~30s——否则健康空闲连接会被误判为断线，陷入周期性重连。
:::

- **心跳帧格式推荐数据帧**（如 ``data: {"type":"heartbeat"}\n\n``）而非注释帧：H5 浏览器 EventSource 会吞掉注释帧，H5 端收不到注释心跳；数据帧四端都能喂住看门狗，业务侧忽略未知类型即可。原生三端两种心跳都支持（注释帧透传为内部 ``heartbeat`` 事件）。
- **首帧建议立即写一帧**（如 ``: connected\n\n``）：让客户端立即确认连通并启动看门狗计时。
- **反向代理场景**加响应头 ``X-Accel-Buffering: no``，否则 nginx 等默认缓冲会导致帧延迟成批到达。
- **断点补发（可选）**：客户端重连会自动携带 ``Last-Event-ID`` 请求头，服务端如需补发断连窗口的帧，给每条帧加 ``id:`` 行并按该头续传即可；不补发时客户端可靠 ``onReconnected`` 回调自行全量补拉。

## 各端实现说明

| 端 | 实现 | 注意事项 |
|:--|:----|:-------|
| Android | ``HttpURLConnection`` 子线程流式读取 | 读到 EOF（服务端正常关闭）立即上报走重连，不等看门狗 |
| iOS | ``NSURLSessionDataTask`` 流式代理 | 按字节缓冲拼完整帧再 UTF-8 解码（多字节字符跨 chunk 安全）；``didReceiveResponse`` 上报 ``open`` |
| HarmonyOS | ``@ohos.net.http`` 流式接收 | **必须用 ``requestInStream``**：普通 ``request()`` 会在 body 到达时走整段写回路径，报 2300023（CURLE_WRITE_ERROR）且 ``dataReceive`` 不回调 |
| H5 | 浏览器 ``EventSource`` 封装 | 不支持自定义请求头（鉴权走 query）；浏览器吞注释帧（服务端心跳须用数据帧）；浏览器自带重连与模块退避形成双保险 |

## 支持平台

Android / iOS / HarmonyOS / H5。小程序端无 EventSource 等价能力，暂不支持
