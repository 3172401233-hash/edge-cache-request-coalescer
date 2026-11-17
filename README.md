# edge-cache-request-coalescer

Node.js 边缘缓存请求合并器（request coalescing / single-flight）。

热门 key 未命中时，几十个并发请求只放行 **一个** in-flight 回源请求，
其余请求订阅同一份响应与流式 chunk；响应完整后按缓存策略提交。
每个等待者持有独立的 `AbortSignal` 和独立队列，单个客户端取消只摘除自己，
不会让其他等待者一起失败。

## 解决的问题

- 缓存击穿：并发未命中瞬间打爆回源。
- 朴素 Promise 合并的缺陷：一个等待者取消会 abort 共享 Promise，拖垮全部等待者。
- 流式响应的 fan-out：迟到订阅者需要先拿到已缓冲的 chunk，再接后续实时 chunk。
- 失败不能永久缓存：错误只允许在很短的窗口内合并。

## 安装与测试

```bash
npm test        # 等价于 node --test
node --test     # 直接运行
```

## 快速开始

```js
import { RequestCoalescer } from './index.js';

const coalescer = new RequestCoalescer({
  origin: async (input, { signal }) => {
    const res = await fetch(input, { signal });
    return {
      status: res.status,
      headers: Object.fromEntries(res.headers),
      body: res.body, // 任何 async iterable（ReadableStream / 生成器）
      ttl: 60_000,    // 可选：覆盖默认 ttl
      staleTtl: 30_000,
    };
  },
  maxConcurrent: 32, // 全局回源并发上限
  ttl: 60_000,
  staleTtl: 0,
  errorTtl: 5_000,
});

const controller = new AbortController();
const response = await coalescer.fetch('https://cdn.example/file', {
  signal: controller.signal,
});
for await (const chunk of response.body) {
  // 每个等待者独立消费，互不影响
}
```

## 缓存键（cache key）

合并与缓存都以规范化后的字符串 key 为准，`fetch` 的第一个参数会先经过
`normalizeKey` 处理。默认规范化器（`lib/key.js`）规则：

- `string` / `URL` / 类 `Request`（带 `url` 字段的对象）统一按 URL 规范化。
- 查询参数按字典序排序，因此 `?b=2&a=1` 与 `?a=1&b=2` 命中同一 key。
- 默认丢弃 `#hash`（fragment 不会发送到源站）。
- 其他对象输入使用 `stableStringify`（对象键排序、数组保持顺序）。

可传入自定义规范化器，例如把请求方法、头部或 vary 字段纳入 key：

```js
const coalescer = new RequestCoalescer({
  origin,
  normalizeKey: (input) => `${input.method}:${new URL(input.url).pathname}`,
});
```

注意：规范化必须是确定性的。同一资源的不同表达若生成不同 key，
合并会退化成多次回源；不同资源若碰撞成同一 key，则会返回错误内容。

## 缓存状态与策略

`getState(input)` 返回五种状态：

- `hit`：fresh，直接返回缓存 chunk，不回源。
- `stale`：在 `staleTtl` 窗口内，立即返回旧值（`response.stale === true`），
  同时后台静默重验证；后台失败不会覆盖仍可用的旧值。
- `miss`：fresh 与 stale 窗口均过期（或无条目），走合并回源。
- `inflight`：该 key 已有回源进行中，新请求订阅同一结果。
- `error`：处于错误窗口内的负缓存（见下）。

是否缓存由 `cacheable(response)` 决定，默认缓存 `2xx/3xx`；
不满足策略的响应会完整广播给当前等待者，但不会写入缓存。

## 错误窗口（error window）

回源 **调用失败**（拒绝、网络错误、5xx 抛出等）时：

- 失败信息在 `errorTtl`（默认 5000ms）内做短暂负缓存，窗口内的新请求
  直接收到同一个错误，不再打向源站；错误对象带 `__coalescerCachedError` 标记。
- 窗口过期后条目自动删除，下一次请求正常重试回源。
- `AbortError`（客户端取消导致的中断）**绝不** 写入负缓存。
- 流式响应 **中途断开** 不写入负缓存——部分 chunk 已经广播，无法安全重放，
  下一个请求应重新回源。
- 后台 stale 重验证失败不写入错误条目，避免覆盖仍可用的 stale 旧值。
- 已有可用（hit/stale）条目时，失败不会覆盖它。

## 取消语义

- 每个等待者独立 `AbortController`：取消一个等待者只关闭它自己的 chunk 队列。
- 仍有其他等待者时，共享回源请求继续执行，已完成的响应照常提交缓存。
- 最后一个外部等待者也取消时，合并器才 abort 源站请求，并跳过缓存提交。
- 已在并发上限队列中等待的请求可以独立取消，只移除自己的等待位。

## 模块结构

- `lib/coalescer.js`：`RequestCoalescer` 主类，状态分流与订阅管理。
- `lib/cache.js`：`TTLCache`，hit / stale / error 三态条目。
- `lib/semaphore.js`：可中止的并发上限信号量。
- `lib/async-queue.js`：每订阅者独立的异步推送队列。
- `lib/key.js`：缓存键规范化工具。
- `test/`：`node --test` 用例，覆盖并发未命中、尾随请求、客户端取消、
  回源断开、流式部分数据、stale、错误窗口与并发上限。
