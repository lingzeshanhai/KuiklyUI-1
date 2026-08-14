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

#import <Foundation/Foundation.h>
#import "KuiklyRenderModuleExportProtocol.h"
#import "KRBaseModule.h"

NS_ASSUME_NONNULL_BEGIN

/**
 * SSE（Server-Sent Events）模块 iOS 端实现（NSURLSession 流式代理）。
 * 对应 core 模块 com.tencent.kuikly.core.module.SseModule。
 * 帧投递经 NSNotificationCenter（core NotifyModule 持久订阅通道）。
 */
@interface KRSseModule : KRBaseModule <NSURLSessionDataDelegate>

@end

NS_ASSUME_NONNULL_END
