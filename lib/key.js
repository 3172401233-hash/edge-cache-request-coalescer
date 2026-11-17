/**
 * 缓存键规范化。
 * 同一个资源无论以 URL 字符串、URL 对象还是 fetch 的 Request 传入，
 * 都必须映射到稳定的字符串 key，否则合并会退化成多次回源。
 */

/**
 * 稳定 JSON 序列化：对象键排序后输出，数组保持顺序。
 */
export function stableStringify(value) {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value ?? null);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  }
  const keys = Object.keys(value).sort();
  const body = keys
    .filter((key) => value[key] !== undefined)
    .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`)
    .join(',');
  return `{${body}}`;
}

/**
 * 规范化 URL：默认丢弃 hash，查询参数按字典序排序。
 */
export function normalizeUrl(input, { sortQuery = true, dropHash = true } = {}) {
  const url = new URL(input);
  if (dropHash) url.hash = '';
  if (sortQuery) {
    const params = [...url.searchParams.entries()].sort(([a], [b]) =>
      a < b ? -1 : a > b ? 1 : 0,
    );
    url.search = '';
    for (const [name, value] of params) url.searchParams.append(name, value);
  }
  return url.toString();
}

/**
 * 默认规范化器：识别 fetch Request / URL / 字符串，
 * 其他输入退化为 stableStringify。
 */
export function defaultNormalizeKey(input) {
  if (input && typeof input === 'object' && typeof input.url === 'string') {
    return normalizeUrl(input.url);
  }
  if (input instanceof URL) return normalizeUrl(input);
  if (typeof input === 'string') return normalizeUrl(input);
  return stableStringify(input);
}
