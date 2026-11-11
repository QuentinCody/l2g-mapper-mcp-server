import { afterEach, expect, it, vi } from "vitest";
import { boundedOpenTargetsFetch } from "./opentargets-http";
afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers(); });

it("preserves native JSON bytes, status and headers while forwarding the request signal", async () => {
  const text = '{"native":"α","errors":[{"message":"fixture"}]}';
  const fetch = vi.fn().mockResolvedValue(new Response(text, { status: 429, headers: { "retry-after": "20" } })); vi.stubGlobal("fetch", fetch);
  const init = { method: "POST", body: "fixture", signal: new AbortController().signal };
  const response = await boundedOpenTargetsFetch("https://fixture.example", init);
  expect(fetch).toHaveBeenCalledWith("https://fixture.example", init);
  expect(response.status).toBe(429); expect(response.headers.get("retry-after")).toBe("20");
  expect(await response.text()).toBe(text);
});

it("rejects oversized and invalid UTF-8 bodies without partial data", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValueOnce(new Response("α".repeat(2100000))).mockResolvedValueOnce(new Response(new Uint8Array([255]))));
  await expect(boundedOpenTargetsFetch("https://fixture.example")).rejects.toThrow("4 MiB");
  await expect(boundedOpenTargetsFetch("https://fixture.example")).rejects.toThrow();
});

it("cancels stalled streams and retains bodyless status responses", async () => {
  vi.useFakeTimers(); const cancel = vi.fn();
  vi.stubGlobal("fetch", vi.fn().mockResolvedValueOnce(new Response(new ReadableStream({ cancel }))).mockResolvedValueOnce(new Response(null, { status: 503 })));
  const assertion = expect(boundedOpenTargetsFetch("https://fixture.example")).rejects.toThrow("timed out");
  await vi.advanceTimersByTimeAsync(4001); await assertion; expect(cancel).toHaveBeenCalled();
  expect((await boundedOpenTargetsFetch("https://fixture.example")).status).toBe(503);
});
