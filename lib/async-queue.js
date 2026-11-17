/**
 * 单消费者异步推送队列。
 * 每个等待者（订阅者）拥有独立队列，互不影响：
 * 生产者 push 数据，消费者通过 for await 读取；
 * 消费者取消（abort）后，push 方会收到 false 以便停止生产。
 */
export class AsyncPushQueue {
  constructor() {
    this._chunks = [];
    this._waiters = null;
    this._closed = false;
    this._error = null;
  }

  get closed() {
    return this._closed;
  }

  /**
   * 投递一个 chunk；队列已关闭/已取消时返回 false。
   */
  push(chunk) {
    if (this._closed) return false;
    if (this._waiters) {
      const { resolve } = this._waiters;
      this._waiters = null;
      resolve({ value: chunk, done: false });
    } else {
      this._chunks.push(chunk);
    }
    return true;
  }

  fail(error) {
    if (this._closed) return false;
    this._closed = true;
    this._error = error;
    if (this._waiters) {
      const { reject } = this._waiters;
      this._waiters = null;
      reject(error);
    }
    return true;
  }

  end() {
    if (this._closed) return false;
    this._closed = true;
    if (this._waiters) {
      const { resolve } = this._waiters;
      this._waiters = null;
      resolve({ value: undefined, done: true });
    }
    return true;
  }

  [Symbol.asyncIterator]() {
    return {
      next: () => {
        if (this._chunks.length > 0) {
          return Promise.resolve({ value: this._chunks.shift(), done: false });
        }
        if (this._closed) {
          if (this._error) return Promise.reject(this._error);
          return Promise.resolve({ value: undefined, done: true });
        }
        return new Promise((resolve, reject) => {
          this._waiters = { resolve, reject };
        });
      },
    };
  }
}
