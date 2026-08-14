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

package com.tencent.kuikly.core.render.android.expand.module

import com.tencent.kuikly.core.render.android.adapter.KuiklyRenderAdapterManager
import com.tencent.kuikly.core.render.android.adapter.KuiklyRenderLog
import com.tencent.kuikly.core.render.android.export.KuiklyRenderBaseModule
import com.tencent.kuikly.core.render.android.export.KuiklyRenderCallback
import org.json.JSONObject
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL

/**
 * SSE（Server-Sent Events）模块 Android 端实现（HttpURLConnection 流式读取）。
 * 对应 core 模块 com.tencent.kuikly.core.module.SseModule。
 * 帧投递经 core NotifyModule 持久订阅通道（[sendKuiklyEvent]）。
 */
class KRSseModule : KuiklyRenderBaseModule() {

    @Volatile
    private var connection: HttpURLConnection? = null

    override fun call(method: String, params: String?, callback: KuiklyRenderCallback?): Any? {
        return when (method) {
            METHOD_CONNECT -> {
                connect(params)
                null
            }
            METHOD_CLOSE -> {
                closeConnection()
                null
            }
            else -> super.call(method, params, callback)
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        closeConnection()
    }

    private fun closeConnection() {
        connection?.disconnect()
        connection = null
    }

    private fun connect(params: String?) {
        closeConnection() // 同一模块实例同时只允许一条连接
        val json = params?.let { JSONObject(it) }
        val url = json?.optString("url").orEmpty()
        if (url.isEmpty()) {
            postFrame(JSONObject().put("event", "error").put("message", "url 为空"))
            return
        }
        KuiklyRenderAdapterManager.krThreadAdapter?.executeOnSubThread {
            stream(url, json?.optJSONObject("headers"))
        }
    }

    /** 子线程阻塞读流：SSE 以 \n\n 分帧，透传 data: 内容与 id:（Last-Event-ID 用） */
    private fun stream(url: String, headers: JSONObject?) {
        var conn: HttpURLConnection? = null
        var reader: BufferedReader? = null
        try {
            conn = (URL(url).openConnection() as HttpURLConnection).apply {
                connectTimeout = CONNECT_TIMEOUT_MS
                readTimeout = 0 // SSE 长连接，不设读超时
                useCaches = false
                doInput = true
                setRequestProperty("Accept", "text/event-stream")
                headers?.also {
                    val keys = it.keys()
                    for (key in keys) {
                        setRequestProperty(key, it.optString(key))
                    }
                }
            }
            connection = conn
            val responseCode = conn.responseCode
            if (responseCode < 200 || responseCode > 299) {
                postFrame(JSONObject().put("event", "error").put("message", "HTTP $responseCode"))
                return
            }
            // 连接建立事件：通知 core 侧复位重连计数并启动看门狗，
            // 否则空闲但健康的连接会一直累积重连次数直到耗尽
            postFrame(JSONObject().put("event", "open"))
            reader = BufferedReader(InputStreamReader(conn.inputStream, Charsets.UTF_8))
            val dataLines = StringBuilder()
            var frameId = ""
            var sawComment = false
            while (true) {
                val line = reader.readLine() ?: break
                if (line.isEmpty()) {
                    if (dataLines.isNotEmpty()) {
                        postFrame(
                            JSONObject()
                                .put("event", "data")
                                .put("data", dataLines.toString().trimEnd('\n'))
                                .put("id", frameId),
                        )
                        dataLines.setLength(0)
                    } else if (sawComment) {
                        // 纯注释帧（服务端心跳 `: ping`）：透传 heartbeat 喂 core 侧看门狗
                        postFrame(JSONObject().put("event", "heartbeat"))
                    }
                    sawComment = false
                } else if (line.startsWith(":")) {
                    sawComment = true // 注释行（SSE 规范的心跳/keep-alive）
                } else if (line.startsWith("data:")) {
                    dataLines.append(line.removePrefix("data:").trimStart()).append('\n')
                } else if (line.startsWith("id:")) {
                    frameId = line.removePrefix("id:").trim()
                }
            }
            // 服务端正常结束连接（读到 EOF）：立即通知 core 走重连，无需等看门狗超时；
            // 连接已被置换（close/重连）说明是主动断开，不上报
            if (connection === conn) {
                postFrame(JSONObject().put("event", "error").put("message", "连接已结束"))
            }
        } catch (e: Exception) {
            if (connection === conn) {
                postFrame(JSONObject().put("event", "error").put("message", e.message ?: "连接中断"))
            }
        } finally {
            try {
                reader?.close()
            } catch (e: IOException) {
                KuiklyRenderLog.e(MODULE_NAME, "Sse module close error: $e")
            }
            if (connection === conn) {
                connection = null
            }
            conn?.disconnect()
        }
    }

    /** 帧投递：core NotifyModule 持久订阅通道（与 core SseModule 订阅事件名一致） */
    private fun postFrame(frame: JSONObject) {
        context?.sendKuiklyEvent(EVENT_FRAME, frame)
    }

    companion object {
        const val MODULE_NAME = "KRSseModule"
        private const val METHOD_CONNECT = "connect"
        private const val METHOD_CLOSE = "close"
        private const val EVENT_FRAME = "KRSseModule.frame"
        private const val CONNECT_TIMEOUT_MS = 10 * 1000
    }
}
