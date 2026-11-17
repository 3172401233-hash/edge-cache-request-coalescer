import { AsyncPushQueue } from './async-queue.js';
import { Semaphore } from './semaphore.js';
import { TTLCache } from './cache.js';
import { defaultNormalizeKey } from './key.js';

function abortError() {
  return new DOMException('The operation was aborted', 'AbortError');
}

/**
 * 边缘缓存请求合并器。
 *
 * 同一个 cache key 的并发未命中只放行一个 in-flight 回源请求，
 * 其余请求订阅同一份元数据与流式 chunk；每个等待者持有独立队列与
 * AbortSignal，单个客户端取消只摘除自己，不会波及其他等待者。
 */
export class RequestCoalescer {
  constructor(options = {}) {
    if (typeof options.origin !== 'function') {
      throw new TypeError('options.origin 必须是回源函数');
    }
    this._origin = options.origin;
    this._normalizeKey = options.normalizeKey ?? defaultNormalizeKey;
    this._now = options.now ?? (() => Date.now());
    this._cache = options.cache ?? new TTLCache({ now: this._now });
    this._semaphore = new Semaphore(options.maxConcurrent ?? 16);
    this._policy = {
      ttl: options.ttl ?? 60_000,
      staleTtl: options.staleTtl ?? 0,
      errorTtl: options.errorTtl ?? 5_000,
      cacheable:
        options.cacheable ??
        ((response) => response.status >= 200 && response.status < 400),
    };
    this._inflight = new Map();
  }

  /** 当前进行中的回源 key 数量 */
  get inflightSize() {
    return this._inflight.size;
  }

  /** 已获得并发许可、正在回源的请求数 */
  get activeOriginRequests() {
    return this._semaphore.active;
  }

  /** 等待并发许可的请求数 */
  get queuedRequests() {
    return this._semaphore.pending;
  }

  /** 查看 key 的缓存状态：hit / stale / error / miss / inflight */
  getState(input) {
    const key = this._normalizeKey(input);
    if (this._inflight.has(key)) return 'inflight';
    return this._cache.get(key).status;
  }

  /**
   * 入口：按缓存状态分流到 hit / stale / miss 合并。
   */
  async fetch(input, options = {}) {
    const key = this._normalizeKey(input);
    const signal = options.signal;
    if (signal?.aborted) throw signal.reason ?? abortError();

    const looked = this._cache.get(key);
    if (looked.status === 'hit') {
      return this._cachedResponse(looked.entry, false);
    }
    if (looked.status === 'stale') {
      const response = this._cachedResponse(looked.entry, true);
      this._scheduleRevalidate(key, input, options);
      return response;
    }
    if (looked.status === 'error') {
      this._throwCachedError(looked.entry);
    }

    const existing = this._inflight.get(key);
    if (existing) return this._subscribe(existing, signal);

    const record = this._createRecord(key, input, options, false);
    this._inflight.set(key, record);
    this._run(record);
    return this._subscribe(record, signal);
  }

  _createRecord(key, input, options, internal) {
    const record = {
      key,
      input,
      options,
      internal,
      originController: new AbortController(),
      subscribers: new Set(),
      meta: null,
      bufferedChunks: null,
      done: false,
      abortedByAll: false,
      streaming: false,
      error: null,
    };
    record.metaPromise = new Promise((resolve, reject) => {
      record.metaResolve = resolve;
      record.metaReject = reject;
    });
    record.metaPromise.catch(() => {});
    return record;
  }

  _scheduleRevalidate(key, input, options) {
    if (this._inflight.has(key)) return;
    const record = this._createRecord(key, input, options, true);
    this._inflight.set(key, record);
    this._run(record);
  }

  /**
   * 等待者订阅一个 in-flight 记录：独立队列 + 独立取消。
   */
  _subscribe(record, signal) {
    if (signal?.aborted) throw signal.reason ?? abortError();

    const subscriber = {
      signal,
      queue: new AsyncPushQueue(),
      aborted: false,
      onAbort: null,
    };
    record.subscribers.add(subscriber);
    if (signal) {
      subscriber.onAbort = () => this._onSubscriberAbort(record, subscriber);
      signal.addEventListener('abort', subscriber.onAbort, { once: true });
    }

    return this._abortableMeta(record, subscriber).then(
      () => this._buildResponse(record, subscriber),
      (error) => {
        this._detach(record, subscriber);
        throw error;
      },
    );
  }

  _abortableMeta(record, subscriber) {
    const { signal } = subscriber;
    if (!signal) return record.metaPromise;
    if (signal.aborted) return Promise.reject(signal.reason ?? abortError());

    return new Promise((resolve, reject) => {
      const onAbort = () => reject(signal.reason ?? abortError());
      signal.addEventListener('abort', onAbort, { once: true });
      record.metaPromise.then(
        (value) => {
          signal.removeEventListener('abort', onAbort);
          resolve(value);
        },
        (error) => {
          signal.removeEventListener('abort', onAbort);
          reject(error);
        },
      );
    });
  }

  _buildResponse(record, subscriber) {
    if (record.bufferedChunks) {
      for (const chunk of record.bufferedChunks) subscriber.queue.push(chunk);
    }
    return {
      status: record.meta.status,
      headers: record.meta.headers ?? {},
      stale: false,
      body: this._iterate(record, subscriber),
    };
  }

  async *_iterate(record, subscriber) {
    try {
      for await (const chunk of subscriber.queue) {
        yield chunk;
      }
    } finally {
      if (!subscriber.aborted && subscriber.signal?.aborted) {
        this._onSubscriberAbort(record, subscriber);
      }
    }
  }

  _onSubscriberAbort(record, subscriber) {
    if (subscriber.aborted) return;
    subscriber.aborted = true;
    subscriber.signal?.removeEventListener('abort', subscriber.onAbort);
    record.subscribers.delete(subscriber);
    subscriber.queue.fail(subscriber.signal.reason ?? abortError());

    // 外部记录的最后一个等待者离开：终止回源，禁止提交缓存。
    if (!record.internal && !record.done && record.subscribers.size === 0) {
      record.abortedByAll = true;
      if (this._inflight.get(record.key) === record) {
        this._inflight.delete(record.key);
      }
      record.originController.abort(abortError());
    }
  }

  _detach(record, subscriber) {
    if (subscriber.aborted) return;
    subscriber.aborted = true;
    record.subscribers.delete(subscriber);
    subscriber.signal?.removeEventListener('abort', subscriber.onAbort);
  }

  /**
   * 实际回源流程：受并发信号量约束，单个 key 只会进入这里一次。
   */
  async _run(record) {
    let release;
    try {
      release = await this._semaphore.acquire(record.originController.signal);
    } catch (error) {
      this._finishWithError(record, error);
      return;
    }

    try {
      const response = await this._origin(record.input, {
        ...record.options,
        signal: record.originController.signal,
      });
      const meta = {
        status: response.status,
        headers: response.headers ?? {},
        ttl: response.ttl,
        staleTtl: response.staleTtl,
      };
      record.meta = meta;
      record.metaResolve(meta);
      record.streaming = true;
      await this._pump(record, response.body);
      this._complete(record, release);
    } catch (error) {
      this._finishWithError(record, error, release);
    }
  }

  async _pump(record, body) {
    const buffered = [];
    record.bufferedChunks = buffered;
    for await (const chunk of body) {
      buffered.push(chunk);
      for (const subscriber of [...record.subscribers]) {
        subscriber.queue.push(chunk);
      }
    }
  }

  _complete(record, release) {
    record.done = true;
    for (const subscriber of [...record.subscribers]) subscriber.queue.end();
    this._storeOnComplete(record);
    if (this._inflight.get(record.key) === record) {
      this._inflight.delete(record.key);
    }
    release();
  }

  _storeOnComplete(record) {
    if (record.abortedByAll) return;
    if (!record.internal && record.subscribers.size === 0) return;
    if (!this._policy.cacheable(record.meta)) return;

    const ttl = record.meta.ttl ?? this._policy.ttl;
    const staleTtl = record.meta.staleTtl ?? this._policy.staleTtl;
    const now = this._now();
    this._cache.set(record.key, {
      kind: 'value',
      status: record.meta.status,
      headers: record.meta.headers,
      chunks: record.bufferedChunks.slice(),
      createdAt: now,
      expiresAt: now + ttl,
      staleUntilAt: now + ttl + staleTtl,
    });
  }

  _finishWithError(record, error, release) {
    record.done = true;
    record.error = error;
    for (const subscriber of [...record.subscribers]) {
      subscriber.queue.fail(error);
    }
    record.metaReject(error);
    this._storeError(record, error);
    if (this._inflight.get(record.key) === record) {
      this._inflight.delete(record.key);
    }
    release?.();
  }

  /**
   * 错误只做短暂负缓存（errorTtl 窗口），AbortError 与后台重验证
   * 失败不写入；已有可用旧值时也不覆盖。
   */
  _storeError(record, error) {
    if (record.abortedByAll || record.internal) return;
    if (error?.name === 'AbortError') return;
    // 流中途断开时部分数据已经广播，无法安全重放，不做负缓存
    if (record.streaming) return;
    const current = this._cache.get(record.key);
    if (current.status === 'hit' || current.status === 'stale') return;

    const now = this._now();
    this._cache.set(record.key, {
      kind: 'error',
      error,
      createdAt: now,
      expiresAt: now + this._policy.errorTtl,
    });
  }

  _cachedResponse(entry, stale) {
    const chunks = entry.chunks;
    return {
      status: entry.status,
      headers: entry.headers,
      stale,
      body: (async function* () {
        for (const chunk of chunks) yield chunk;
      })(),
    };
  }

  _throwCachedError(entry) {
    const error = entry.error;
    if (!error.__coalescerCachedError) {
      Object.defineProperty(error, '__coalescerCachedError', {
        value: true,
        configurable: true,
      });
    }
    throw error;
  }
}
