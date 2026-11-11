import { afterEach, expect, it, vi } from "vitest";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { fetchFinnGenContext } from "./finngen-context";
import { gatherInputs } from "./gather-inputs";
import { projectedSubrequests } from "./gather-budget";

const anchor = { rsid: "rs1", locus_id: "L1", grch38: { chr: "1", pos: 10, ref: "A", alt: "G" } };
afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers(); });

it.each(["R12", "R13"] as const)("requests only the selected %s host and preserves native phenotype/effect/QC fields", async release => {
  const native = { variant: { chrom: 1, pos: 10, ref: "A", alt: "G", annotation: "native" }, results: [{ phenocode: "I9_TEST", pval: 0, beta: -2, af: 0.8, num_cases: 50 }], regions: [{ pip: 0.4 }], future: "unchanged" };
  const fetcher = vi.fn(async () => Response.json(native)); vi.stubGlobal("fetch", fetcher);
  const warnings: string[] = [];
  const [row] = await fetchFinnGenContext([anchor], release, warnings);
  expect(fetcher).toHaveBeenCalledExactlyOnceWith(`https://${release.toLowerCase()}.finngen.fi/api/variant/1%3A10-A-G`, expect.objectContaining({ redirect: "manual", signal: expect.any(AbortSignal) }));
  expect(JSON.parse(row.native_response_json!)).toEqual(native);
  expect(row).toMatchObject({ response_sha256: await hashCanonicalJson(native), status: "observed", requested_variant: "1:10-A-G", release_requested: release, assembly_requested: "GRCh38", context_only: true, identity_verified: false, coverage_complete: false, release_basis: "requested_host", http_status: 200 });
  expect(warnings.join(" ")).toContain("not qualified for L2G scoring");
});

it.each([null, { ...anchor.grch38, alt: null }, { ...anchor.grch38, ref: "A/T" }, { ...anchor.grch38, alt: "A" }, { ...anchor.grch38, chr: "1/../../" }, { ...anchor.grch38, pos: 0 }, { ...anchor.grch38, pos: 1.1 }])("skips unresolved or invalid coordinates without rsID fallback: %j", async grch38 => {
  const fetcher = vi.fn(); vi.stubGlobal("fetch", fetcher);
  const [row] = await fetchFinnGenContext([{ ...anchor, grch38 }], "R13", []);
  expect(row).toMatchObject({ status: "skipped", source_url: null, native_response_json: null, response_sha256: null });
  expect(fetcher).not.toHaveBeenCalled();
});

it.each([null, [], { qc_variant_results: { reason: "QC" } }, { error: "missing" }])("retains unusual native JSON without claiming association availability: %j", async native => {
  vi.stubGlobal("fetch", vi.fn(async () => Response.json(native)));
  const [row] = await fetchFinnGenContext([anchor], "R13", []);
  expect(row.status).toBe("error"); expect(JSON.parse(row.native_response_json!)).toEqual(native);
  expect(row.response_sha256).toBe(await hashCanonicalJson(native));
});

it.each([404, 429, 503, 302, 206])("retains native HTTP %s errors with no retries, redirects or release fallback", async status => {
  const native = { error: "provider failure" }; const fetcher = vi.fn(async () => Response.json(native, { status })); vi.stubGlobal("fetch", fetcher);
  const [row] = await fetchFinnGenContext([anchor], "R13", []);
  expect(row).toMatchObject({ status: "error", http_status: status, native_response_json: JSON.stringify(native) });
  expect(fetcher).toHaveBeenCalledTimes(1);
});

it("keeps an empty native results array as an observation with unknown coverage", async () => {
  vi.stubGlobal("fetch", async () => Response.json({ results: [] }));
  expect((await fetchFinnGenContext([anchor], "R13", []))[0]).toMatchObject({ status: "observed", coverage_complete: false });
});

it("reports non-JSON and oversized bodies without retaining partial responses", async () => {
  for (const body of ["<html>unavailable</html>", JSON.stringify({ results: [], extra: "α".repeat(524288) })]) {
    vi.stubGlobal("fetch", async () => new Response(body));
    expect((await fetchFinnGenContext([anchor], "R13", []))[0]).toMatchObject({ status: "error", native_response_json: null, response_sha256: null });
  }
});

it("cancels a stalled body and bounds request time", async () => {
  vi.useFakeTimers();
  const cancel = vi.fn();
  vi.stubGlobal("fetch", async () => new Response(new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode('{"results":[')); }, cancel })));
  const pending = fetchFinnGenContext([anchor], "R13", []);
  await vi.advanceTimersByTimeAsync(4001);
  expect((await pending)[0]).toMatchObject({ status: "error", native_response_json: null });
  expect(cancel).toHaveBeenCalledTimes(1);
  vi.stubGlobal("fetch", (_url: string, init: RequestInit) => new Promise((_resolve, reject) => init.signal!.addEventListener("abort", () => reject(new Error("aborted")))));
  const waiting = fetchFinnGenContext([anchor], "R13", []);
  await vi.advanceTimersByTimeAsync(4001);
  expect((await waiting)[0].error).toBe("aborted");
});

it("rejects unknown releases and excess anchors before network access, and budgets the optional requests", async () => {
  const fetcher = vi.fn(); vi.stubGlobal("fetch", fetcher);
  for (const release of [null, "r13", "R14", "latest", "https://example.com"]) expect(() => gatherInputs({ finngen_release: release })).toThrow("finngen_release");
  await expect(fetchFinnGenContext([anchor], "R14" as "R13", [])).rejects.toThrow("finngen_release");
  await expect(fetchFinnGenContext(Array(26).fill(anchor), "R13", [])).rejects.toThrow("at most 25");
  expect(fetcher).not.toHaveBeenCalled();
  expect(gatherInputs({}).finngen_release).toBeUndefined();
  const base = gatherInputs({ max_loci: 25, max_anchor_associations: 0 });
  expect(projectedSubrequests({ ...base, finngen_release: "R13" }, 1) - projectedSubrequests(base, 1)).toBe(25);
});
