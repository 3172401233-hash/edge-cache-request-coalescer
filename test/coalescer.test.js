import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RequestCoalescer } from '../index.js';
import {
  createFakeOrigin,
  createClock,
  collect,
  delay,
  createChunkStream,
} from './helpers.js';

const okResponse = (overrides = {}) => ({
  status: 200,
  headers: { 'content-type': 'text/plain' },
  body: createChunkStream(['a', 'b']),
  ...overrides,
});

test('并发未命中：N 个请求只回源一次，全部拿到同一份流式结果', async () => {
  const origin = createFakeOrigin();
  const coalescer = new RequestCoalescer({ origin });

  const responses = Promise.all(
    Array.from({ length: 10 }, () => coalescer.fetch('https://cdn.example/a')),
  );
  await delay(10);
  assert.equal(origin.calls.length, 1);
  assert.equal(coalescer.getState('https://cdn.example/a'), 'inflight');

  origin.last().resolve(okResponse());
  const settled = await responses;
  assert.equal(settled.length, 10);

  const bodies = await Promise.all(settled.map((response) => collect(response.body)));
  assert.equal(coalescer.inflightSize, 0);
  for (const chunks of bodies) assert.deepEqual(chunks, ['a', 'b']);
});

test('尾随请求：in-flight 期间加入的订阅者拿到缓冲 chunk + 后续 chunk', async () => {
  const origin = createFakeOrigin();
  const coalescer = new RequestCoalescer({ origin });

  const first = coalescer.fetch('https://cdn.example/late');
  await delay(10);
  origin.last().resolve(
    okResponse({
      body: createChunkStream(['c1', 'c2', 'c3', 'c4']),
    }),
  );

  const firstResponse = await first;
  await delay(20); // 前两个 chunk 已经被泵出并缓冲

  const lateResponse = await coalescer.fetch('https://cdn.example/late');
  assert.equal(origin.calls.length, 1);
  assert.deepEqual(await collect(lateResponse.body), ['c1', 'c2', 'c3', 'c4']);
  assert.deepEqual(await collect(firstResponse.body), ['c1', 'c2', 'c3', 'c4']);
});

test('单个等待者取消：其他等待者与回源请求不受影响', async () => {
  const origin = createFakeOrigin();
  const coalescer = new RequestCoalescer({ origin });

  const c1 = new AbortController();
  const c2 = new AbortController();
  const cancelled = coalescer.fetch('https://cdn.example/cancel', {
    signal: c1.signal,
  });
  const survivor = coalescer.fetch('https://cdn.example/cancel', {
    signal: c2.signal,
  });
  await delay(10);

  c1.abort();
  await assert.rejects(cancelled, { name: 'AbortError' });
  await delay(5);

  assert.equal(origin.last().aborted, false);
  assert.equal(coalescer.inflightSize, 1);

  origin.last().resolve(okResponse());
  const response = await survivor;
  assert.deepEqual(await collect(response.body), ['a', 'b']);
  assert.equal(origin.calls.length, 1);
});

test('全部等待者取消：回源被 abort，响应不写入缓存', async () => {
  const origin = createFakeOrigin();
  const coalescer = new RequestCoalescer({ origin });

  const c1 = new AbortController();
  const c2 = new AbortController();
  const p1 = coalescer.fetch('https://cdn.example/gone', { signal: c1.signal });
  const p2 = coalescer.fetch('https://cdn.example/gone', { signal: c2.signal });
  await delay(10);

  c1.abort();
  c2.abort();
  await Promise.allSettled([p1, p2]);
  await delay(5);

  assert.equal(origin.last().aborted, true);
  assert.equal(coalescer.inflightSize, 0);
  assert.equal(coalescer.getState('https://cdn.example/gone'), 'miss');
});

test('回源断开（流中途失败）：所有等待者收到错误，且不缓存失败', async () => {
  const origin = createFakeOrigin();
  const coalescer = new RequestCoalescer({ origin });

  const p1 = coalescer.fetch('https://cdn.example/break');
  const p2 = coalescer.fetch('https://cdn.example/break');
  await delay(10);

  origin.last().resolve({
    status: 200,
    headers: {},
    body: createChunkStream(['partial-1', 'partial-2'], { failAfter: 2 }),
  });

  const responses = await Promise.all([p1, p2]);
  const bodyResults = await Promise.allSettled(
    responses.map((response) => collect(response.body)),
  );
  for (const result of bodyResults) {
    assert.equal(result.status, 'rejected');
    assert.match(result.reason.message, /disconnected/);
  }
  await delay(5);
  assert.equal(coalescer.getState('https://cdn.example/break'), 'miss');
});

test('回源失败：错误在 errorTtl 窗口内短暂合并，窗口过后允许重试', async () => {
  const clock = createClock();
  const origin = createFakeOrigin();
  const coalescer = new RequestCoalescer({
    origin,
    now: clock.now,
    errorTtl: 1000,
  });

  const p1 = coalescer.fetch('https://cdn.example/boom');
  const p2 = coalescer.fetch('https://cdn.example/boom');
  await delay(10);
  origin.last().reject(new Error('origin 503'));

  const results = await Promise.allSettled([p1, p2]);
  assert.equal(origin.calls.length, 1);
  for (const result of results) {
    assert.equal(result.status, 'rejected');
    assert.equal(result.reason.message, 'origin 503');
  }

  assert.equal(coalescer.getState('https://cdn.example/boom'), 'error');
  await assert.rejects(coalescer.fetch('https://cdn.example/boom'), (error) => {
    assert.equal(error.__coalescerCachedError, true);
    return true;
  });
  assert.equal(origin.calls.length, 1);

  clock.advance(1001);
  assert.equal(coalescer.getState('https://cdn.example/boom'), 'miss');
  const retry = coalescer.fetch('https://cdn.example/boom');
  await delay(10);
  assert.equal(origin.calls.length, 2);
  origin.last().resolve(okResponse());
  assert.deepEqual(await collect((await retry).body), ['a', 'b']);
});

test('流式部分数据：早加入的订阅者实时收 chunk，慢订阅者取消不影响其他人', async () => {
  const origin = createFakeOrigin();
  const coalescer = new RequestCoalescer({ origin });

  const slowController = new AbortController();
  const fast = coalescer.fetch('https://cdn.example/stream');
  const slow = coalescer.fetch('https://cdn.example/stream', {
    signal: slowController.signal,
  });
  await delay(10);

  origin.last().resolve({
    status: 200,
    headers: {},
    body: createChunkStream(['s1', 's2', 's3']),
  });

  const fastResponse = await fast;
  const slowResponse = await slow;
  slowController.abort();
  await assert.rejects(collect(slowResponse.body), { name: 'AbortError' });

  assert.deepEqual(await collect(fastResponse.body), ['s1', 's2', 's3']);
  assert.equal(origin.calls.length, 1);
});

test('stale 结果：先返回 stale 旧值，后台重验证成功后写入新值', async () => {
  const clock = createClock();
  const origin = createFakeOrigin();
  const coalescer = new RequestCoalescer({
    origin,
    now: clock.now,
    ttl: 1000,
    staleTtl: 5000,
  });

  const warmup = coalescer.fetch('https://cdn.example/stale');
  await delay(10);
  origin.last().resolve(
    okResponse({ body: createChunkStream(['v1']) }),
  );
  const warmupResponse = await warmup;
  assert.deepEqual(await collect(warmupResponse.body), ['v1']);
  assert.equal(warmupResponse.stale, false);

  clock.advance(1500); // fresh 过期、仍在 stale 窗口
  assert.equal(coalescer.getState('https://cdn.example/stale'), 'stale');

  const staleResponse = await coalescer.fetch('https://cdn.example/stale');
  assert.equal(staleResponse.stale, true);
  assert.deepEqual(await collect(staleResponse.body), ['v1']);

  await delay(10);
  assert.equal(origin.calls.length, 2);
  origin.last().resolve(
    okResponse({ body: createChunkStream(['v2']) }),
  );
  await delay(20);

  const refreshed = await coalescer.fetch('https://cdn.example/stale');
  assert.equal(refreshed.stale, false);
  assert.deepEqual(await collect(refreshed.body), ['v2']);
});

test('并发上限：超出 maxConcurrent 的请求排队，排队中的取消只影响自己', async () => {
  const origin = createFakeOrigin();
  const coalescer = new RequestCoalescer({ origin, maxConcurrent: 1 });

  const first = coalescer.fetch('https://cdn.example/one');
  const queuedController = new AbortController();
  const queued = coalescer.fetch('https://cdn.example/two', {
    signal: queuedController.signal,
  });
  const third = coalescer.fetch('https://cdn.example/three');
  await delay(10);

  assert.equal(coalescer.activeOriginRequests, 1);
  assert.equal(coalescer.queuedRequests, 2);

  queuedController.abort();
  await assert.rejects(queued, { name: 'AbortError' });
  assert.equal(coalescer.queuedRequests, 1);

  origin.calls[0].resolve(okResponse());
  assert.deepEqual(await collect((await first).body), ['a', 'b']);

  await delay(10);
  assert.equal(origin.calls.length, 2);
  origin.last().resolve(okResponse());
  assert.deepEqual(await collect((await third).body), ['a', 'b']);
});

test('缓存键规范化：URL 字符串/URL 对象/Request 等价输入合并为一次回源', async () => {
  const origin = createFakeOrigin();
  const coalescer = new RequestCoalescer({ origin });

  const inputs = [
    'https://cdn.example/asset?b=2&a=1',
    new URL('https://cdn.example/asset?a=1&b=2'),
    { url: 'https://cdn.example/asset?b=2&a=1#frag' },
  ];
  const responses = Promise.all(inputs.map((input) => coalescer.fetch(input)));
  await delay(10);
  assert.equal(origin.calls.length, 1);

  origin.last().resolve(okResponse());
  const settled = await responses;
  for (const response of settled) {
    assert.deepEqual(await collect(response.body), ['a', 'b']);
  }
});
