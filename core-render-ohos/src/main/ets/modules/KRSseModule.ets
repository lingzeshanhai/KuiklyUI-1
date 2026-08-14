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

import { http } from '@kit.NetworkKit';
import { BusinessError } from '@kit.BasicServicesKit';
import emitter from '@ohos.events.emitter';
import util from '@ohos.util';
import { KRAny, KuiklyRenderCallback } from '../utils/KRDataTypes';
import { KuiklyRenderBaseModule } from './export';

/**
 * SSE（Server-Sent Events）模块 HarmonyOS 端实现（@ohos.net.http 流式接收）。
 * 对应 core 模块 com.tencent.kuikly.core.module.SseModule。
 * 帧投递经 @ohos.events.emitter（core NotifyModule 持久订阅通道，
 * 投递数据结构与 KRNotifyModule.postNotify 完全同构）。
 *
 * 注意：流式响应必须用 requestInStream（官方明确 dataReceive 需搭配它使用），
 * 普通 request() 会在 body 到达时走整段写回路径，报 2300023（CURLE_WRITE_ERROR）
 * 且 dataReceive 不回调。
 */
export class KRSseModule extends KuiklyRenderBaseModule {
  static readonly MODULE_NAME: string = 'KRSseModule';
  private static readonly METHOD_CONNECT: string = 'connect';
  private static readonly METHOD_CLOSE: string = 'close';
  private static readonly EVENT_FRAME: string = 'KRSseModule.frame';

  private client?: http.HttpRequest;
  // 流式 UTF-8 解码器（chunk 边界切断多字节序列也不会乱码），同一实例跨 chunk 复用
  private decoder: util.TextDecoder = util.TextDecoder.create('utf-8');
  private buffer: string = '';

  // 异步模式：module 运行在 UI 线程，http 流式回调在 UI 线程处理
  syncMode(): boolean {
    return false;
  }

  call(method: string, params: KRAny, callback: KuiklyRenderCallback | null): KRAny {
    switch (method) {
      case KRSseModule.METHOD_CONNECT: {
        this.connect(params as string);
        return null;
      }
      case KRSseModule.METHOD_CLOSE: {
        this.close();
        return null;
      }
      default:
        return super.call(method, params, callback);
    }
  }

  /** 模块销毁时释放 SSE 连接 */
  onDestroy(): void {
    this.close();
  }

  /** 建立 SSE 连接（requestInStream 流式接收，帧经 emitter 投递） */
  private connect(params: string): void {
    this.close(); // 同一模块实例同时只允许一条连接
    let url = '';
    let headers: Record<string, string> = {};
    try {
      const paramsObj = JSON.parse(params) as Record<string, Object>;
      url = (paramsObj['url'] as string) ?? '';
      headers = (paramsObj['headers'] as Record<string, string>) ?? {};
    } catch (e) {
      this.postFrame('error', '', '', '参数解析失败');
      return;
    }
    if (!url) {
      this.postFrame('error', '', '', 'url 为空');
      return;
    }
    const req = http.createHttp();
    this.client = req;

    // 流式接收：SSE 以 \n\n 分帧，仅透传 data: 内容（多行 data 按规范以 \n 拼接）
    req.on('dataReceive', (data: ArrayBuffer) => {
      // decodeToString({ stream: true })：流式解码，保留跨 chunk 的半截多字节序列
      this.buffer += this.decoder.decodeToString(new Uint8Array(data), { stream: true });
      const parts = this.buffer.split('\n\n');
      this.buffer = parts.pop() ?? '';
      parts.forEach((frame) => this.fireFrame(frame));
    });
    // 连接结束（服务端关闭/网络中断）：通知 core 侧触发自动重连
    req.on('dataEnd', () => {
      this.postFrame('error', '', '', '连接已关闭');
    });

    // ArkTS 禁止对象展开运算符（arkts-no-spread），逐键合并请求头
    const header: Record<string, string> = { 'Accept': 'text/event-stream' };
    Object.keys(headers).forEach((key: string) => { header[key] = headers[key]; });
    req.requestInStream(url, {
      method: http.RequestMethod.GET,
      header: header,
      readTimeout: 0, // SSE 长连接不设读超时
      connectTimeout: 15000,
    }, (err: BusinessError, code: number) => {
      // 响应头到达即回调（此后经 dataReceive/dataEnd 读流）
      if (err) {
        this.postFrame('error', '', '', err.message ?? '连接失败');
        return;
      }
      if (code < 200 || code >= 300) {
        this.postFrame('error', '', '', `HTTP ${code}`);
        return;
      }
      // 连接建立事件：通知 core 侧复位重连计数并启动看门狗，
      // 否则空闲但健康的连接会一直累积重连次数直到耗尽
      this.postFrame('open', '', '', '');
    });
  }

  private close(): void {
    const req = this.client;
    this.client = undefined;
    if (req) {
      req.off('dataReceive');
      req.off('dataEnd');
      req.destroy();
    }
    this.buffer = '';
  }

  private fireFrame(frame: string): void {
    const payload = frame
      .split('\n')
      .filter((line) => line.startsWith('data:'))
      .map((line) => line.slice(5).trim())
      .join('\n');
    const idLine = frame.split('\n').find((line) => line.startsWith('id:'));
    const frameId = idLine ? idLine.slice(3).trim() : '';
    const sawComment = frame.split('\n').some((line) => line.startsWith(':'));
    if (payload.length > 0) {
      this.postFrame('data', payload, frameId, '');
    } else if (sawComment) {
      // 纯注释帧（服务端心跳 `: ping`）：透传 heartbeat 喂 core 侧看门狗
      this.postFrame('heartbeat', '', '', '');
    }
  }

  /** 帧投递：emitter（core NotifyModule 持久订阅通道，与 SseModule 订阅事件名一致） */
  private postFrame(event: string, data: string, id: string, message: string): void {
    try {
      const frame: Record<string, string> = message
        ? { 'event': event, 'data': data, 'id': id, 'message': message }
        : { 'event': event, 'data': data, 'id': id };
      emitter.emit(KRSseModule.EVENT_FRAME, {
        data: {
          'eventName': KRSseModule.EVENT_FRAME,
          'stringify': JSON.stringify(frame),
        },
      });
    } catch (e) {
      // 投递失败静默（页面销毁等场景）
    }
  }
}
