import { hashCanonicalJson } from "@bio-mcp/shared";
import { afterEach, expect, it, vi } from "vitest";
import { fetchHpaLane } from "./hpa-context";
import { hpaProjection } from "./hpa-projection";

const id = "ENSG00000134057";
// Selected fields from the provider's documented individual-entry example, observed October 1.
const native = { Gene: "CCNB1", Ensembl: id, "RNA tissue specific nTPM": { "bone marrow": "49.9", "lymphoid tissue": "70.9" }, "Subcellular location": ["Cytosol"], "Protein tissue specific Intensity": null };
afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers(); });
const run = () => fetchHpaLane(new Map([["CCNB1", id]]), []);

it("uses current HPA field names/string values and preserves RNA separately from protein context", async () => {
  const fetcher = vi.fn(async () => Response.json(native)); vi.stubGlobal("fetch", fetcher);
  const [row] = await run();
  expect(row).toMatchObject({ ensembl_id: id, observed_symbol: "CCNB1", tissues: "lymphoid tissue | bone marrow", subcellular: "Cytosol", status: "observed", identity_verified: true, context_only: true, coverage_complete: false, release_pinned: false, response_sha256: await hashCanonicalJson(native) });
  expect(JSON.parse(row.native_response_json!)).toEqual(native);
  expect(JSON.parse(row.rna_tissue_specific_ntpm_json)).toEqual(native["RNA tissue specific nTPM"]);
  expect(fetcher).toHaveBeenCalledWith(`https://www.proteinatlas.org/${id}.json`, expect.objectContaining({ redirect: "manual", signal: expect.any(AbortSignal) }));
});

it("bounds display summaries without clipping native values or converting invalid values to expression", () => {
  const rna = { null_value: null, blank: "", negative: -1, missing: "NA", invalid: "4 units", low: "0.5", zero: 0, ...Object.fromEntries(Array.from({ length: 12 }, (_, i) => [`t${i}`, String(i + 1)])) };
  const result = hpaProjection({ ...native, "RNA tissue specific nTPM": rna }, id);
  expect(result.tissues.split(" | ")).toEqual(["t11", "t10", "t9", "t8", "t7", "t6", "t5", "t4"]);
  expect(JSON.parse(result.rna_tissue_specific_ntpm_json)).toEqual(rna);
});

it.each([{}, null, [], { ...native, Ensembl: "ENSG00000141510" }, { ...native, "RNA tissue specific nTPM": [] }])("retains unqualified native responses without attaching another gene's tissues: %j", async payload => {
  vi.stubGlobal("fetch", async () => Response.json(payload));
  const [row] = await run();
  expect(row).toMatchObject({ status: "error", identity_verified: false, ensembl_id: null, tissues: "", native_response_json: JSON.stringify(payload) });
});

it("preserves HTTP failure evidence and records unsupported IDs without a request", async () => {
  const fetcher = vi.fn(async () => Response.json({ error: "unavailable" }, { status: 503 })); vi.stubGlobal("fetch", fetcher);
  const rows = await fetchHpaLane(new Map([["CCNB1", id], ["BAD", "CCNB1"]]), []);
  expect(rows[0]).toMatchObject({ status: "error", http_status: 503, native_response_json: '{"error":"unavailable"}' });
  expect(rows[1]).toMatchObject({ status: "skipped", source_url: null, identity_verified: false });
  expect(fetcher).toHaveBeenCalledTimes(1);
});

it("caps optional gene requests explicitly", async () => {
  const fetcher = vi.fn(async () => Response.json({})); vi.stubGlobal("fetch", fetcher);
  const warnings: string[] = [];
  const rows = await fetchHpaLane(new Map(Array.from({ length: 21 }, (_, i) => [`G${i}`, `ENSG${String(i).padStart(11, "0")}`])), warnings);
  expect(rows).toHaveLength(20); expect(fetcher).toHaveBeenCalledTimes(20);
  expect(warnings.some(warning => warning.includes("capped at 20"))).toBe(true);
});

it("bounds oversized and stalled bodies while retaining an explicit failure", async () => {
  vi.stubGlobal("fetch", async () => Response.json({ ...native, large: "x".repeat(1024 * 1024) }));
  expect((await run())[0]).toMatchObject({ status: "error", error: expect.stringContaining("limit") });
  vi.useFakeTimers(); const cancel = vi.fn();
  vi.stubGlobal("fetch", async () => new Response(new ReadableStream({ cancel })));
  const pending = run(); await vi.advanceTimersByTimeAsync(4000);
  expect((await pending)[0]).toMatchObject({ status: "error", error: expect.stringContaining("timed out") });
  expect(cancel).toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
});
