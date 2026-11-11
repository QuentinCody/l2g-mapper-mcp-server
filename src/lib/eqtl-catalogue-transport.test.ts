import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { eqtlNative } from "../../test/eqtl-catalogue-fixture";

beforeEach(() => { vi.resetModules(); vi.useFakeTimers(); vi.setSystemTime(new Date("2026-10-01T00:00:00Z")); });
afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers(); });
const url = eqtlNative().dataset.sumstats_url;

it("spaces FTP requests across concurrent callers and forwards exact ranges with cancellation", async () => {
  const { eqtlCatalogueTransport } = await import("./eqtl-catalogue-transport");
  const times: number[] = []; const fetcher = vi.fn(async () => { times.push(Date.now()); return new Response("bytes"); });
  vi.stubGlobal("fetch", fetcher);
  const controller = new AbortController(); const io = eqtlCatalogueTransport(controller.signal);
  const first = io.fetchFile(`${url}.tbi`);
  const second = eqtlCatalogueTransport(controller.signal).fetchFile(url, { range: { start: 0, end: 99 } });
  await vi.advanceTimersByTimeAsync(0); await first; expect(fetcher).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(1999); expect(fetcher).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(1); await second;
  expect(times[1] - times[0]).toBe(2000);
  expect(fetcher).toHaveBeenLastCalledWith(url, { signal: controller.signal, redirect: "manual", headers: { Accept: "application/octet-stream", Range: "bytes=0-99" } });
});

it("cancels queued FTP work without issuing a request", async () => {
  const { eqtlCatalogueTransport } = await import("./eqtl-catalogue-transport");
  const fetcher = vi.fn(async () => new Response("bytes")); vi.stubGlobal("fetch", fetcher);
  const controller = new AbortController(); const io = eqtlCatalogueTransport(controller.signal);
  const first = io.fetchFile(url); await vi.advanceTimersByTimeAsync(0); await first;
  const pending = io.fetchFile(url); const check = expect(pending).rejects.toThrow("stopped");
  controller.abort(new Error("stopped")); await check; await vi.advanceTimersByTimeAsync(3000);
  expect(fetcher).toHaveBeenCalledTimes(1);
});

it.each(["https://example.org/file", `${url}?token=x`, `${url}#fragment`, url.replace("QTD000021.all", "QTD000022.all"), url.replace("https:", "http:")])("rejects unsupported file destinations before fetching (%s)", async invalid => {
  const { eqtlCatalogueTransport } = await import("./eqtl-catalogue-transport");
  const fetcher = vi.fn(); vi.stubGlobal("fetch", fetcher);
  await expect(eqtlCatalogueTransport(new AbortController().signal).fetchFile(invalid)).rejects.toThrow("Unsupported");
  expect(fetcher).not.toHaveBeenCalled();
});

it("allows only the pinned r7 metadata paths, rejects redirects and does not retry", async () => {
  const { eqtlCatalogueTransport } = await import("./eqtl-catalogue-transport");
  const cancel = vi.fn(); const fetcher = vi.fn(async () => new Response(new ReadableStream({ cancel }), { status: 302, headers: { Location: "https://example.org/elsewhere" } }));
  vi.stubGlobal("fetch", fetcher); const io = eqtlCatalogueTransport(new AbortController().signal);
  await expect(io.resourcesFetch("/data_tables/dataset_metadata_r8.tsv")).rejects.toThrow("Unsupported");
  expect(fetcher).not.toHaveBeenCalled();
  await expect(io.resourcesFetch("/data_tables/dataset_metadata_r7.tsv")).rejects.toThrow("302");
  expect(fetcher).toHaveBeenCalledTimes(1); expect(cancel).toHaveBeenCalledOnce();
});
