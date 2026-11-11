/** Bound response bytes and body time before a shared adapter parses JSON. */
export function boundedJsonFetch(source: string, maxBytes: number, timeoutMs: number): typeof fetch {
  return async (input, init) => {
    const response = await fetch(input, init);
    const reader = response.body?.getReader();
    if (!reader) return response;
    const decoder = new TextDecoder("utf-8", { fatal: true }); const parts: string[] = [];
    let bytes = 0; let expired = false;
    const timer = setTimeout(() => { expired = true; void reader.cancel().catch(() => {}); }, timeoutMs);
    try {
      while (true) {
        const part = await reader.read(); if (part.done) break;
        bytes += part.value.byteLength;
        if (bytes > maxBytes) throw new Error(`${source} response exceeds the local ${maxBytes}-byte limit`);
        parts.push(decoder.decode(part.value, { stream: true }));
      }
      if (expired) throw new Error(`${source} response body timed out`);
      parts.push(decoder.decode());
      return new Response(parts.join(""), { status: response.status, statusText: response.statusText, headers: response.headers });
    } finally { clearTimeout(timer); void reader.cancel().catch(() => {}); }
  };
}
