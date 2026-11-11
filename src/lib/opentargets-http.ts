/** Bound each native response before the shared adapter parses or formats it. */
export const boundedOpenTargetsFetch: typeof fetch = async (input, init) => {
  const response = await fetch(input, init);
  const reader = response.body?.getReader();
  if (!reader) return response;
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const parts: string[] = [];
  let bytes = 0; let expired = false;
  const timer = setTimeout(() => { expired = true; void reader.cancel().catch(() => {}); }, 4000);
  try {
    while (true) {
      const part = await reader.read(); if (part.done) break;
      bytes += part.value.byteLength;
      if (bytes > 4 * 1024 * 1024) throw new Error("OpenTargets response exceeds the local 4 MiB limit");
      parts.push(decoder.decode(part.value, { stream: true }));
    }
    if (expired) throw new Error("OpenTargets response body timed out");
    parts.push(decoder.decode());
    return new Response(parts.join(""), { status: response.status, statusText: response.statusText, headers: response.headers });
  } finally { clearTimeout(timer); void reader.cancel().catch(() => {}); }
};
