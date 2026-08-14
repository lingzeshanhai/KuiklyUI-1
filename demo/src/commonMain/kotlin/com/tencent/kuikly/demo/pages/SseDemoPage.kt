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

package com.tencent.kuikly.demo.pages

import com.tencent.kuikly.core.annotations.Page
import com.tencent.kuikly.core.base.Color
import com.tencent.kuikly.core.base.ViewBuilder
import com.tencent.kuikly.core.directives.vfor
import com.tencent.kuikly.core.module.SseModule
import com.tencent.kuikly.core.module.SseOptions
import com.tencent.kuikly.core.reactive.handler.observable
import com.tencent.kuikly.core.reactive.handler.observableList
import com.tencent.kuikly.core.views.Input
import com.tencent.kuikly.core.views.Scroller
import com.tencent.kuikly.core.views.Text
import com.tencent.kuikly.core.views.View
import com.tencent.kuikly.core.views.compose.Button
import com.tencent.kuikly.demo.pages.base.BasePager
import com.tencent.kuikly.demo.pages.demo.base.NavBar

/**
 * SSE（Server-Sent Events）模块演示页。
 * 演示：建立连接、接收事件流（含 Last-Event-ID）、断开连接、断线自动重连（指数退避）、心跳看门狗。
 */
@Page("SseDemoPage")
internal class SseDemoPage : BasePager() {

    // 示例地址：本地演示 SSE 服务；token 经 query 传递（EventSource 不能设 header）
    // 注意：Android 模拟器访问宿主机需改用 10.0.2.2
    private var url by observable("http://localhost:8080/events?token=")
    private var statusText by observable("未连接")
    private var eventCount by observable(0)
    private val events by observableList<String>()

    private var sse: SseModule? = null

    override fun body(): ViewBuilder {
        val ctx = this
        return {
            NavBar {
                attr {
                    title = "SseDemoPage"
                }
            }

            View {
                attr {
                    alignSelfStretch()
                    flexDirectionColumn()
                    padding(16f)
                }

                // URL 输入
                Input {
                    attr {
                        alignSelfStretch()
                        height(44f)
                        fontSize(14f)
                        color(Color.BLACK)
                        placeholder("输入 SSE 地址（如 http://localhost:8080/events?token=xxx）")
                        text(ctx.url)
                    }
                    event {
                        textDidChange { params ->
                            ctx.url = params.text
                        }
                    }
                }

                // 操作按钮
                View {
                    attr {
                        flexDirectionRow()
                        marginTop(12f)
                    }
                    Button {
                        attr {
                            size(120f, 40f)
                            borderRadius(20f)
                            backgroundColor(Color(0xFF00B42A))
                            titleAttr {
                                text("连接")
                                fontSize(14f)
                                color(Color.WHITE)
                            }
                        }
                        event {
                            click { ctx.connect() }
                        }
                    }
                    View { attr { width(12f) } }
                    Button {
                        attr {
                            size(120f, 40f)
                            borderRadius(20f)
                            backgroundColor(Color(0xFFFF3B30))
                            titleAttr {
                                text("断开")
                                fontSize(14f)
                                color(Color.WHITE)
                            }
                        }
                        event {
                            click { ctx.disconnect() }
                        }
                    }
                }

                // 状态
                Text {
                    attr {
                        marginTop(12f)
                        text("状态：${ctx.statusText} · 已接收 ${ctx.eventCount} 条事件")
                        fontSize(13f)
                        color(Color.GRAY)
                    }
                }

                // 事件流
                Scroller {
                    attr {
                        alignSelfStretch()
                        flex(1f)
                        marginTop(12f)
                    }
                    View {
                        attr {
                            alignSelfStretch()
                            flexDirectionColumn()
                        }
                        vfor({ ctx.events }) { item ->
                            View {
                                attr {
                                    alignSelfStretch()
                                    marginTop(6f)
                                    padding(8f)
                                    backgroundColor(Color(0xFFF5F5F5))
                                    borderRadius(6f)
                                }
                                Text {
                                    attr {
                                        text(item)
                                        fontSize(12f)
                                        color(Color.BLACK)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fun connect() {
        disconnect()
        statusText = "连接中…"
        // acquireModule 获取每 Pager 单例（SSE 已在 Pager.initCoreModules 注册）；
        // 直接 new SseModule() 会因 pagerId 为空导致 toNative 调用被静默丢弃
        val module = getPager().acquireModule<SseModule>(SseModule.MODULE_NAME)
        sse = module
        module.connect(
            url = url,
            options = SseOptions(
                autoReconnect = true,
                reconnectBaseDelayMs = 1000,
                reconnectMaxDelayMs = 10000,
                heartbeatTimeoutMs = 45000,
            ),
            onEvent = { data ->
                statusText = "已连接"
                eventCount += 1
                events.add(0, "#$eventCount $data")
            },
            onError = { msg ->
                statusText = "连接失败：$msg"
            },
            onReconnected = {
                // 断连后重连成功：断连窗口内丢失的帧服务端不补发，业务可在此全量补拉
                statusText = "已重连（可在此补拉断连期间的数据）"
            },
        )
    }

    fun disconnect() {
        sse?.close()
        sse = null
        statusText = "已断开"
    }

    override fun pageDidDisappear() {
        super.pageDidDisappear()
        disconnect()
    }
}
