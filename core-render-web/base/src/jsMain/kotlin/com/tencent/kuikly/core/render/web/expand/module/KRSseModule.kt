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

package com.tencent.kuikly.core.render.web.expand.module

import com.tencent.kuikly.core.render.web.export.KuiklyRenderBaseModule
import com.tencent.kuikly.core.render.web.ktx.KuiklyRenderCallback
import com.tencent.kuikly.core.render.web.ktx.toJSONObjectSafely
import com.tencent.kuikly.core.render.web.nvi.serialization.json.JSONObject

/**
 * SSE（Server-Sent Events）模块 H5 端实现（浏览器 EventSource 封装）。
 * 对应 core 模块 com.tencent.kuikly.core.module.SseModule。
 * 帧投递经 core NotifyModule 全局通道（[KRNotifyModule.dispatchGlobalEvent]）——
 * 该通道为「native 重复推送」的官方机制，不受单次模块回调生命周期限制。
 */
class KRSseModule : KuiklyRenderBaseModule() {

    private var source: dynamic = null

    override fun call(method: String, params: String?, callback: KuiklyRenderCallback?): Any? {
        return when (method) {
            METHOD_CONNECT -> {
                connect(params)
                null
            }
            METHOD_CLOSE -> {
                closeSource()
                null
            }
            else -> super.call(method, params, callback)
        }
    }

    /** 建立 EventSource 连接；帧经 NotifyModule 全局通道投递 */
    private fun connect(params: String?) {
        closeSource() // 同一模块实例同时只允许一条连接
        val url = params?.toJSONObjectSafely()?.optString("url").orEmpty()
        if (url.isEmpty()) {
            postFrame("error", "", "url 为空")
            return
        }
        val es = newEventSource(url)
        es.onopen = { _: dynamic ->
            // 连接建立（含浏览器自动重连成功）：复位 core 侧重连计数并启动看门狗，
            // 否则空闲但健康的连接会一直累积重连次数直到耗尽
            postFrame("open", "", "")
            null
        }
        es.onmessage = { e: dynamic ->
            // lastEventId 透传（Last-Event-ID 断点续传用）
            postFrame("data", (e.data as? String) ?: "", (e.lastEventId as? String) ?: "")
            null
        }
        es.onerror = { _: dynamic ->
            // 浏览器会自动重连；同时上报 error 由上层（看门狗/退避）决策
            postFrame("error", "", "")
            null
        }
        source = es
    }

    /** 模块销毁时关闭 EventSource，避免泄漏 */
    override fun onDestroy() {
        closeSource()
    }

    private fun closeSource() {
        val es = source
        if (es != null) {
            es.close()
        }
        source = null
    }

    /** 经 KRNotifyModule 全局通道投递帧（event/data/id），连接重试后依然可达 */
    private fun postFrame(event: String, data: String, id: String) {
        try {
            KRNotifyModule.dispatchGlobalEvent(
                EVENT_FRAME,
                JSONObject()
                    .put("event", event)
                    .put("data", data)
                    .put("id", id),
            )
        } catch (e: Throwable) {
            // 投递失败静默（页面销毁等场景）
        }
    }

    companion object {
        const val MODULE_NAME = "KRSseModule"
        private const val METHOD_CONNECT = "connect"
        private const val METHOD_CLOSE = "close"
        private const val EVENT_FRAME = "KRSseModule.frame"
    }
}

/** 构造 EventSource（js() 代码体内可直接引用本函数参数） */
private fun newEventSource(url: String): dynamic = js("new EventSource(url)")
