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

#import "KRSseModule.h"
#import "NSObject+KR.h"

#define SSE_EVENT_FRAME @"KRSseModule.frame"

@interface KRSseModule ()

@property (nonatomic, strong, nullable) NSURLSession *session;
@property (nonatomic, strong, nullable) NSURLSessionDataTask *task;
// 原始字节缓冲：多字节 UTF-8 字符可能跨 chunk，按字节拼到完整帧再解码
@property (nonatomic, strong) NSMutableData *pendingBytes;

@end

@implementation KRSseModule

- (instancetype)init {
    if (self = [super init]) {
        _pendingBytes = [NSMutableData data];
    }
    return self;
}

// core SseModule.connect(url, headers) 分发到本方法
- (id)connect:(NSDictionary *)args {
    [self close:nil];

    NSDictionary *param = [args[KR_PARAM_KEY] hr_stringToDictionary];
    NSString *url = param[@"url"];
    if (![url isKindOfClass:NSString.class] || url.length == 0) {
        [self postFrame:@{ @"event": @"error", @"message": @"url 为空" }];
        return nil;
    }

    NSMutableURLRequest *request = [NSMutableURLRequest requestWithURL:[NSURL URLWithString:url]];
    [request setValue:@"text/event-stream" forHTTPHeaderField:@"Accept"];
    NSDictionary *headers = param[@"headers"];
    if ([headers isKindOfClass:NSDictionary.class]) {
        [headers enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
            [request setValue:[NSString stringWithFormat:@"%@", obj] forHTTPHeaderField:key];
        }];
    }

    NSURLSessionConfiguration *config = [NSURLSessionConfiguration defaultSessionConfiguration];
    config.timeoutIntervalForResource = 0; // SSE 长连接不设整体超时
    self.session = [NSURLSession sessionWithConfiguration:config delegate:self delegateQueue:nil];
    self.task = [self.session dataTaskWithRequest:request];
    [self.task resume];
    return nil;
}

// core SseModule.close() 分发到本方法
- (id)close:(NSDictionary *)args {
    [self.task cancel];
    self.task = nil;
    [self.session invalidateAndCancel];
    self.session = nil;
    self.pendingBytes = [NSMutableData data];
    return nil;
}

// 页面销毁时释放长连接（TDFBaseModule 生命周期钩子）
- (void)invalidate {
    [self close:nil];
    [super invalidate];
}

#pragma mark - NSURLSessionDataDelegate

- (void)URLSession:(NSURLSession *)session dataTask:(NSURLSessionDataTask *)dataTask didReceiveData:(NSData *)data {
    [self.pendingBytes appendData:data];
    // SSE 以 \n\n 分帧（按字节扫描，完整帧再 UTF-8 解码，避免多字节字符跨 chunk 被切断）
    NSData *separator = [@"\n\n" dataUsingEncoding:NSUTF8StringEncoding];
    while (YES) {
        NSRange range = [self.pendingBytes rangeOfData:separator options:0 range:NSMakeRange(0, self.pendingBytes.length)];
        if (range.location == NSNotFound) break;
        NSData *frameData = [self.pendingBytes subdataWithRange:NSMakeRange(0, range.location)];
        [self.pendingBytes replaceBytesInRange:NSMakeRange(0, range.location + range.length) withBytes:NULL length:0];
        NSString *frame = [[NSString alloc] initWithData:frameData encoding:NSUTF8StringEncoding];
        if (frame) [self fireFrame:frame];
    }
}

- (void)URLSession:(NSURLSession *)session task:(NSURLSessionTask *)task didCompleteWithError:(NSError *)error {
    if (error && error.code != NSURLErrorCancelled) {
        [self postFrame:@{ @"event": @"error", @"message": error.localizedDescription ?: @"连接中断" }];
    }
    self.task = nil;
}

- (void)URLSession:(NSURLSession *)session dataTask:(NSURLSessionDataTask *)dataTask
didReceiveResponse:(NSURLResponse *)response completionHandler:(void (^)(NSURLSessionResponseDisposition))completionHandler {
    NSInteger code = [(NSHTTPURLResponse *)response statusCode];
    if (code < 200 || code >= 300) {
        [self postFrame:@{ @"event": @"error", @"message": [NSString stringWithFormat:@"HTTP %ld", (long)code] }];
        completionHandler(NSURLSessionResponseCancel);
        return;
    }
    // 连接建立事件：通知 core 侧复位重连计数并启动看门狗，
    // 否则空闲但健康的连接会一直累积重连次数直到耗尽
    [self postFrame:@{ @"event": @"open" }];
    completionHandler(NSURLSessionResponseAllow);
}

#pragma mark - private

- (void)fireFrame:(NSString *)frame {
    NSMutableString *payload = [NSMutableString string];
    __block NSString *frameId = @"";
    __block BOOL sawComment = NO;
    [frame enumerateLinesUsingBlock:^(NSString *line, BOOL *stop) {
        if ([line hasPrefix:@"data:"]) {
            NSString *content = [line substringFromIndex:5];
            [payload appendString:[content stringByTrimmingCharactersInSet:NSCharacterSet.whitespaceCharacterSet]];
            [payload appendString:@"\n"];
        } else if ([line hasPrefix:@"id:"]) {
            frameId = [[line substringFromIndex:3] stringByTrimmingCharactersInSet:NSCharacterSet.whitespaceCharacterSet];
        } else if ([line hasPrefix:@":"]) {
            sawComment = YES; // 注释行（SSE 规范的心跳/keep-alive）
        }
    }];
    if (payload.length > 0) {
        [self postFrame:@{ @"event": @"data",
                           @"data": [payload stringByTrimmingCharactersInSet:NSCharacterSet.newlineCharacterSet],
                           @"id": frameId }];
    } else if (sawComment) {
        // 纯注释帧（服务端心跳 `: ping`）：透传 heartbeat 喂 core 侧看门狗
        [self postFrame:@{ @"event": @"heartbeat" }];
    }
}

// 帧投递：NSNotificationCenter（core NotifyModule 持久订阅通道，与 SseModule 订阅事件名一致）
- (void)postFrame:(NSDictionary *)frame {
    [[NSNotificationCenter defaultCenter] postNotificationName:SSE_EVENT_FRAME object:nil userInfo:frame];
}

@end
