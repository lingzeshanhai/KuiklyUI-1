/*
 * Tencent is pleased to support the open source community by making KuiklyUI
 * available.
 * Copyright (C) 2025 Tencent. All rights reserved.
 * Licensed under the License of KuiklyUI;
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * https://github.com/Tencent-TDS/KuiklyUI/blob/main/LICENSE
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.kuikly.core.module

import com.tencent.kuikly.core.nvi.serialization.json.JSONObject
import com.tencent.kuikly.core.timer.clearTimeout
import com.tencent.kuikly.core.timer.setTimeout

/** SSE 事件回调（每条 data 帧一次） */
typealias SseEventCallback = (data: String) -> Unit

/** SSE 错误回调（重连次数耗尽或连接最终失败时调用一次） */
typealias SseErrorCallback = (message: String) -> Unit

/**
 * SSE 连接选项（全部有默认值，零配置可用）。
 */
data class SseOptions(
    /** 断线自动重连 */
    val autoReconnect: Boolean = true,
    /** 最大重连次数（-1 不限） */
    val maxReconnectAttempts: Int = -1,
    /** 退避基数 ms：第 n 次重连延迟 = base * 2^(n-1)，封顶 [reconnectMaxDelayMs] */
    val reconnectBaseDelayMs: Int = 1000,
    /** 退避上限 ms */
    val reconnectMaxDelayMs: Int = 30000,
    /**
     * 心跳超时 ms：超过该时长无任何帧（含服务端注释心跳）判为断线并触发重连；
     * <= 0 关闭看门狗。应大于服务端心跳间隔
     */
    val heartbeatTimeoutMs: Int = 45000,
)

/**
 * SSE（Server-Sent Events）长连接模块。
 *
 * 提供标准 SSE 客户端能力：建立连接后，native 每收到一条 data 帧回调一次
 * [SseEventCallback]。帧投递经 [NotifyModule] 持久订阅通道完成——该通道为
 * 「native 重复推送」的官方机制，不受单次模块回调生命周期限制
 * （[toNative] 的 keepCallbackAlive 回调在连接出错后可能被渲染层回收导致丢帧）。
 *
 * 能力：
 * - 断线自动重连（指数退避，commonMain 统一实现，各端 native 零重复逻辑）；
 * - Last-Event-ID 断点续传（自动追踪帧 id，重连时自动携带，符合 SSE 规范）；
 * - 心跳/超时看门狗（超过 [SseOptions.heartbeatTimeoutMs] 无任何帧判为断线并重连）；
 * - 连接建立事件（native 收到 2xx 响应头 / H5 EventSource.onopen 上报 "open"）：
 *   复位重连计数，避免空闲健康连接累积重连次数直到耗尽；
 * - 断连恢复通知（[connect] 的 onReconnected）：断连窗口内丢失的帧服务端通常不补发，
 *   上层可在此回调内做全量补拉；
 * - 服务端心跳支持：native 透传注释帧（`: ping`）为 "heartbeat" 事件喂看门狗
 *   （注意 H5 浏览器 EventSource 会吞掉注释帧，H5 需服务端用数据帧心跳）。
 *
 * 用法（任意 Pager 内）：
 * ```
 * val sse = getPager().acquireModule<SseModule>(SseModule.MODULE_NAME)
 * sse.connect("https://api.example.com/events?token=xxx",
 *   options = SseOptions(heartbeatTimeoutMs = 30000),
 *   onEvent = { data -> ... },
 *   onError = { msg -> ... })
 * sse.close()
 * ```
 *
 * 各端实现：core-render-android / core-render-ios / core-render-ohos / core-render-web
 * 中的 KRSseModule（同名注册）。
 */
open class SseModule : Module() {

    override fun moduleName(): String = MODULE_NAME

    private var lastEventId = ""
    private var reconnectAttempts = 0
    private var closedByUser = true
    private var connecting = false
    private var watchdogRef = ""
    private var reconnectRef = ""

    private var currentUrl = ""
    private var currentHeaders: JSONObject? = null
    private var options = SseOptions()
    private var eventCallback: SseEventCallback? = null
    private var errorCallback: SseErrorCallback? = null
    /** 断连后重连成功时回调一次（供上层补拉断连窗口丢失的数据） */
    private var reconnectedCallback: (() -> Unit)? = null
    private var notifyModule: NotifyModule? = null
    private var notifyRef: CallbackRef? = null

    /**
     * 建立 SSE 连接（重复调用会先关闭旧连接并重置重连计数）。
     * @param onReconnected 断连后重连成功时触发一次（首次连接不触发）——
     *        断连窗口内丢失的帧服务端通常不补发，上层可在此回调内做全量补拉
     */
    fun connect(
        url: String,
        headers: JSONObject? = null,
        options: SseOptions = SseOptions(),
        onEvent: SseEventCallback,
        onError: SseErrorCallback,
        onReconnected: (() -> Unit)? = null,
    ) {
        closeInternal(userAction = true)
        closedByUser = false
        currentUrl = url
        currentHeaders = headers
        this.options = options
        eventCallback = onEvent
        errorCallback = onError
        reconnectedCallback = onReconnected
        reconnectAttempts = 0
        openConnection()
    }

    /** 关闭连接并停止一切重连/看门狗（幂等） */
    fun close() {
        closeInternal(userAction = true)
    }

    // ---------- 内部实现 ----------

    private fun openConnection() {
        if (closedByUser || connecting) return
        connecting = true
        val headers = currentHeaders?.let { JSONObject(it.toString()) } ?: JSONObject()
        // Last-Event-ID 断点续传：重连时自动携带（SSE 规范）
        if (lastEventId.isNotEmpty()) {
            headers.put("Last-Event-ID", lastEventId)
        }
        val params = JSONObject().apply {
            put("url", currentUrl)
            put("headers", headers)
        }
        ensureNotifyChannel()
        toNative(
            false,
            methodName = METHOD_CONNECT,
            param = params.toString(),
        )
        armWatchdog()
    }

    /**
     * 帧投递通道：core NotifyModule（为「native 重复推送」设计的持久订阅，
     * 不受单次模块回调生命周期限制）。native 每帧 postNotify(EVENT_FRAME)。
     */
    private fun ensureNotifyChannel() {
        if (notifyRef != null) return
        // 必须使用与自身相同 pager 上下文的 NotifyModule：直接 new 的实例 pagerId 为空，
        // 其 toNative 调用会被渲染层丢弃，且 callback 注册在空 pagerId 下收不到 native 回推
        val nm = notifyModule ?: NotifyModule().also {
            it.injectVar(pagerId, pageData ?: return, pageTrace)
            notifyModule = it
        }
        notifyRef = nm.addNotify(EVENT_FRAME) { data ->
            connecting = false
            when (data?.optString("event", "")) {
                // 连接建立（native 收到 2xx 响应头，H5 为 EventSource.onopen）：
                // 复位重连计数并启动看门狗，否则空闲但健康的连接会一直累积重连次数直到耗尽；
                // 复位前计数 >0 说明本次是断连后的重连成功，回调上层补拉断连窗口丢失的数据
                "open" -> {
                    val wasReconnect = reconnectAttempts > 0
                    reconnectAttempts = 0
                    armWatchdog()
                    if (wasReconnect) reconnectedCallback?.invoke()
                }
                "data" -> {
                    // 追踪 Last-Event-ID；收到数据即重置重连计数与看门狗
                    val id = data.optString("id", "")
                    if (id.isNotEmpty()) lastEventId = id
                    reconnectAttempts = 0
                    armWatchdog()
                    eventCallback?.invoke(data.optString("data", ""))
                }
                "heartbeat" -> armWatchdog() // 服务端注释心跳（native 可选上报）
                "error" -> handleError(data.optString("message", "SSE 连接异常"))
                else -> Unit
            }
        }
    }

    private fun handleError(message: String) {
        if (closedByUser) return
        val max = options.maxReconnectAttempts
        if (!options.autoReconnect || (max >= 0 && reconnectAttempts >= max)) {
            errorCallback?.invoke(message)
            return
        }
        scheduleReconnect()
    }

    private fun scheduleReconnect() {
        if (closedByUser) return
        reconnectAttempts++
        // 指数退避：base * 2^(n-1)，封顶 maxDelay
        var delay = options.reconnectBaseDelayMs.toLong()
        repeat(reconnectAttempts - 1) {
            delay = (delay * 2).coerceAtMost(options.reconnectMaxDelayMs.toLong())
        }
        cancelTimer(reconnectRef)
        reconnectRef = setTimeout(pagerId, delay.toInt()) {
            if (!closedByUser) openConnection()
        }
    }

    /** 心跳看门狗：超过 heartbeatTimeoutMs 无任何帧判为断线并触发重连 */
    private fun armWatchdog() {
        if (options.heartbeatTimeoutMs <= 0 || closedByUser) return
        cancelTimer(watchdogRef)
        watchdogRef = setTimeout(pagerId, options.heartbeatTimeoutMs) {
            if (!closedByUser) {
                closeNative()
                connecting = false
                scheduleReconnect()
            }
        }
    }

    private fun closeInternal(userAction: Boolean) {
        if (userAction) closedByUser = true
        cancelTimer(watchdogRef)
        cancelTimer(reconnectRef)
        connecting = false
        closeNative()
        notifyRef?.also { ref ->
            notifyModule?.removeNotify(EVENT_FRAME, ref)
            notifyRef = null
        }
    }

    private fun closeNative() {
        toNative(
            false,
            methodName = METHOD_CLOSE,
            param = JSONObject().toString(),
        )
    }

    private fun cancelTimer(ref: String) {
        if (ref.isNotEmpty()) clearTimeout(pagerId, ref)
    }

    companion object {
        const val MODULE_NAME = ModuleConst.SSE
        private const val METHOD_CONNECT = "connect"
        private const val METHOD_CLOSE = "close"

        /** 帧投递事件名（native postNotify → NotifyModule 订阅者） */
        const val EVENT_FRAME = "KRSseModule.frame"
    }
}
