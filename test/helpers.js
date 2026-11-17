/**
 * 可控回源：每次调用进入队列，测试手动 resolve/reject，
 * 并记录调用次数与 abort 事件。
 */
export function createFakeOrigin(handler) {
  const calls = [];

  const origin = (input, options = {}) => {
    const call = {
      input,
      options,
      aborted: false,
      signal: options.signal,
    };
    calls.push(call);

    return new Promise((resolve, reject) => {
      call.resolve = resolve;
      call.reject = reject;
      const signal = options.signal;
      if (signal) {
        call.onAbort = () => {
          call.aborted = true;
          reject(signal.reason ?? new DOMException('Aborted', 'AbortError'));
        };
        if (signal.aborted) {
          call.onAbort();
        } else {
          signal.addEventListener('abort', call.onAbort, { once: true });
        }
      }
      handler?.(call);
    });
  };

  origin.calls = calls;
  origin.last = () => calls[calls.length - 1];
  return origin;
}

/** 可控时钟，供 TTLCache / 合并器注入 */
export function createClock(start = 1_000_000) {
  let current = start;
  return {
    now: () => current,
    advance(ms) {
      current += ms;
    },
  };
}

/** 收集异步 body 的全部 chunk */
export async function collect(body) {
  const chunks = [];
  for await (const chunk of body) chunks.push(chunk);
  return chunks;
}

/** 延迟，便于真实异步时序测试 */
export const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** 简单的可中止异步流，可中途模拟断开 */
export function createChunkStream(chunks, { failAfter = null, signal } = {}) {
  return (async function* () {
    let emitted = 0;
    for (const chunk of chunks) {
      if (signal?.aborted) {
        throw signal.reason ?? new DOMException('Aborted', 'AbortError');
      }
      await delay(5);
      yield chunk;
      emitted += 1;
      if (failAfter !== null && emitted >= failAfter) {
        throw new Error('origin stream disconnected');
      }
    }
  })();
}
