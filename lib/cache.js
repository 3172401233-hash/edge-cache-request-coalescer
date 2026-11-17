/**
 * 带 TTL / stale-while-revalidate / 错误窗口的内存缓存。
 *
 * 条目状态：
 * - fresh：now < expiresAt，直接命中
 * - stale：expiresAt <= now < staleUntilAt，返回旧值并后台回源
 * - expired：now >= staleUntilAt，视为未命中
 * - error：回源失败的短暂负缓存，过期后自动放行重试，绝不长期保留
 */
export class TTLCache {
  constructor({ now = () => Date.now() } = {}) {
    this._store = new Map();
    this._now = now;
  }

  get size() {
    return this._store.size;
  }

  set(key, entry) {
    this._store.set(key, entry);
  }

  get(key) {
    const entry = this._store.get(key);
    if (!entry) return { status: 'miss' };

    const now = this._now();
    if (entry.kind === 'error') {
      if (now >= entry.expiresAt) {
        this._store.delete(key);
        return { status: 'miss' };
      }
      return { status: 'error', entry };
    }

    if (now < entry.expiresAt) {
      return { status: 'hit', entry };
    }
    if (now < entry.staleUntilAt) {
      return { status: 'stale', entry };
    }
    this._store.delete(key);
    return { status: 'miss' };
  }

  delete(key) {
    return this._store.delete(key);
  }

  clear() {
    this._store.clear();
  }
}
