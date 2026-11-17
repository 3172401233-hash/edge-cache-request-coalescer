import { test } from 'node:test';
import assert from 'node:assert/strict';
import { TTLCache, Semaphore, AsyncPushQueue, normalizeUrl, stableStringify } from '../index.js';

test('stableStringify：键顺序无关，数组顺序敏感', () => {
  assert.equal(stableStringify({ b: 1, a: 2 }), stableStringify({ a: 2, b: 1 }));
  assert.notEqual(stableStringify([1, 2]), stableStringify([2, 1]));
  assert.equal(stableStringify({ a: undefined }), '{}');
});

test('normalizeUrl：排序查询参数并丢弃 hash', () => {
  assert.equal(
    normalizeUrl('https://x.dev/p?z=9&a=1#part'),
    'https://x.dev/p?a=1&z=9',
  );
});

test('TTLCache：hit / stale / expired 三态切换', () => {
  let now = 0;
  const cache = new TTLCache({ now: () => now });
  const entry = {
    kind: 'value',
    chunks: ['x'],
    expiresAt: 100,
    staleUntilAt: 200,
  };
  cache.set('k', entry);

  now = 50;
  assert.equal(cache.get('k').status, 'hit');
  now = 150;
  assert.equal(cache.get('k').status, 'stale');
  now = 250;
  assert.equal(cache.get('k').status, 'miss');
  assert.equal(cache.size, 0);
});

test('TTLCache：错误条目过期自动删除', () => {
  let now = 0;
  const cache = new TTLCache({ now: () => now });
  cache.set('e', { kind: 'error', error: new Error('x'), expiresAt: 100 });
  now = 99;
  assert.equal(cache.get('e').status, 'error');
  now = 100;
  assert.equal(cache.get('e').status, 'miss');
});

test('Semaphore：超过上限排队，释放后按序放行', async () => {
  const semaphore = new Semaphore(2);
  const releases = [await semaphore.acquire(), await semaphore.acquire()];
  const queued = [semaphore.acquire(), semaphore.acquire()];
  assert.equal(semaphore.active, 2);
  assert.equal(semaphore.pending, 2);

  releases[0]();
  releases.push(await queued[0]);
  assert.equal(semaphore.active, 2);
  assert.equal(semaphore.pending, 1);
  releases[1]();
  releases.push(await queued[1]);
  assert.equal(semaphore.pending, 0);
  for (const release of releases.slice(2)) release();
});

test('Semaphore：排队者可独立 abort，不影响其他等待者', async () => {
  const semaphore = new Semaphore(1);
  const release = await semaphore.acquire();

  const c1 = new AbortController();
  const c2 = new AbortController();
  const p1 = semaphore.acquire(c1.signal);
  const p2 = semaphore.acquire(c2.signal);

  c1.abort();
  await assert.rejects(p1, { name: 'AbortError' });

  release();
  const release2 = await p2;
  assert.equal(typeof release2, 'function');
  assert.equal(semaphore.pending, 0);
  release2();
});

test('AsyncPushQueue：push/fail/end 与异步迭代', async () => {
  const queue = new AsyncPushQueue();
  queue.push('x');
  queue.push('y');
  queue.end();
  const chunks = [];
  for await (const chunk of queue) chunks.push(chunk);
  assert.deepEqual(chunks, ['x', 'y']);

  const failing = new AsyncPushQueue();
  const consumer = (async () => {
    for await (const chunk of failing) void chunk;
  })();
  failing.fail(new Error('boom'));
  await assert.rejects(consumer, /boom/);
  assert.equal(failing.push('late'), false);
});
