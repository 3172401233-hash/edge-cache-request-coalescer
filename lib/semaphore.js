/**
 * 并发上限信号量。
 * 等待获取许可的请求可以被各自的 AbortSignal 独立取消，
 * 取消只移除自己的等待位，不影响队列中的其他请求。
 */
export class Semaphore {
  constructor(max) {
    if (!Number.isInteger(max) || max <= 0) {
      throw new TypeError('max 必须是正整数');
    }
    this._max = max;
    this._active = 0;
    this._queue = [];
  }

  get max() {
    return this._max;
  }

  get active() {
    return this._active;
  }

  get pending() {
    return this._queue.length;
  }

  acquire(signal) {
    if (signal?.aborted) {
      return Promise.reject(signal.reason ?? new DOMException('Aborted', 'AbortError'));
    }
    if (this._active < this._max) {
      this._active += 1;
      return Promise.resolve(this._release.bind(this));
    }
    return new Promise((resolve, reject) => {
      const waiter = { resolve, reject, signal };
      const onAbort = () => {
        const index = this._queue.indexOf(waiter);
        if (index !== -1) this._queue.splice(index, 1);
        signal.removeEventListener('abort', onAbort);
        reject(signal.reason ?? new DOMException('Aborted', 'AbortError'));
      };
      signal?.addEventListener('abort', onAbort, { once: true });
      waiter.onAbort = onAbort;
      this._queue.push(waiter);
    });
  }

  _release() {
    if (this._active > 0) this._active -= 1;
    while (this._queue.length > 0 && this._active < this._max) {
      const waiter = this._queue.shift();
      waiter.signal?.removeEventListener('abort', waiter.onAbort);
      if (waiter.signal?.aborted) continue;
      this._active += 1;
      waiter.resolve(this._release.bind(this));
    }
  }
}
