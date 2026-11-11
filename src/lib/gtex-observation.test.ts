import { afterEach, expect, it, vi } from "vitest";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { fetchGtexEqtls } from "./gtex-evidence";

afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers(); });
const anchors = [{ rsid: "rs1", grch38: { chr: "1", pos: 100, ref: "A", alt: "C" } }];
const row = { variantId: "chr1_100_A_C_b38", geneSymbol: "GENE", gencodeId: "ENSG1.7", tissueSiteDetailId: "Liver", pValue: 1e-8, nes: -0.4, maf: 0.17 };

it.each(["gtex_v8", "gtex_v10"] as const)("preserves the full %s page and native gene annotation", async dataset => {
  const native = { data: [row, { ...row, variantId: "chr1_100_A_G_b38" },
    { ...row, datasetId: dataset === "gtex_v8" ? "gtex_v10" : "gtex_v8" }],
    paging_info: { page: 0, totalNumberOfItems: 999 }, provider_extension: { unknown: "retained" } };
  const fetcher = vi.fn(async () => Response.json(native)); vi.stubGlobal("fetch", fetcher);
  const result = await fetchGtexEqtls(anchors, 200, [], dataset);
  const hash = await hashCanonicalJson(native);
  expect(result.rows).toEqual([{
    lead_rsid: "rs1", variant_id: row.variantId, genome_assembly: "GRCh38", dataset_id_requested: dataset,
    gencode_id: "ENSG1.7", ensembl_id: "ENSG1", symbol: "GENE", tissue: "Liver", p_value: 1e-8, slope: -0.4,
    native_row_json: JSON.stringify(row), source_row_index: 0, source_response_sha256: hash,
  }]);
  expect(result.evidence).toEqual([{ source_url: expect.any(String), dataset_id_requested: dataset,
    request_json: JSON.stringify({ variantId: [row.variantId], datasetId: dataset, page: 0, itemsPerPage: 200 }),
    response_json: JSON.stringify(native), response_sha256: hash, native_text: JSON.stringify(native), http_status: 200,
    retrieved_at: expect.any(String), usable: true, error: null, coverage_complete: false, release_pinned: false,
  }]);
  const url = new URL(result.evidence[0].source_url);
  expect(url.searchParams.getAll("variantId")).toEqual([row.variantId]);
  expect(url.searchParams.get("datasetId")).toBe(dataset);
  expect(fetcher).toHaveBeenCalledTimes(1);
  expect(fetcher).toHaveBeenCalledWith(url.href, expect.objectContaining({ redirect: "manual" }));
});

it("defaults explicitly to v8 and caps the single page at 250", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => Response.json({ data: [] })));
  const result = await fetchGtexEqtls(anchors, 500, []);
  expect(JSON.parse(result.evidence[0].request_json)).toEqual({ variantId: [row.variantId], datasetId: "gtex_v8", page: 0, itemsPerPage: 250 });
});

it.each([{ data: [row], datasetId: "gtex_v10" }, { message: "maintenance" }, null])("retains unusable responses without scoring them: %j", async native => {
  vi.stubGlobal("fetch", vi.fn(async () => Response.json(native)));
  const warnings: string[] = []; const result = await fetchGtexEqtls(anchors, 200, warnings);
  expect(result.rows).toEqual([]);
  expect(result.evidence[0].usable).toBe(false);
  expect(result.evidence[0].response_json).toBe(JSON.stringify(native));
  expect(result.evidence[0].response_sha256).toBe(await hashCanonicalJson(native));
  expect(warnings[0]).toContain("GTEx eQTL batch failed:");
});

it.each([400, 429, 503])("preserves complete HTTP %s errors and never retries", async status => {
  const native = { error: "unavailable", details: "α".repeat(20000) };
  const fetcher = vi.fn(async () => Response.json(native, { status })); vi.stubGlobal("fetch", fetcher);
  const result = await fetchGtexEqtls(anchors, 200, []);
  expect(result.rows).toEqual([]);
  expect(result.evidence[0]).toMatchObject({ http_status: status, usable: false, response_json: JSON.stringify(native), native_text: JSON.stringify(native), response_sha256: await hashCanonicalJson(native) });
  expect(fetcher).toHaveBeenCalledTimes(1);
});

it("preserves non-JSON errors and transport failures separately from empty data", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response("<html>maintenance</html>", { status: 503 })));
  expect((await fetchGtexEqtls(anchors, 200, [])).evidence[0]).toMatchObject({ native_text: "<html>maintenance</html>", response_json: null, usable: false });
  vi.stubGlobal("fetch", vi.fn(async () => { throw new Error("offline"); }));
  expect((await fetchGtexEqtls(anchors, 200, [])).evidence[0]).toMatchObject({ native_text: null, http_status: null, error: "offline", usable: false });
});

it("rejects oversized responses instead of staging partial evidence", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response("x".repeat(4 * 1024 * 1024 + 1))));
  const result = await fetchGtexEqtls(anchors, 200, []);
  expect(result.rows).toEqual([]);
  expect(result.evidence[0].error).toContain("limit");
  expect(result.evidence[0].response_json).toBeNull();
});

it("bounds stalled bodies and records source failure", async () => {
  vi.useFakeTimers(); const cancel = vi.fn();
  vi.stubGlobal("fetch", vi.fn(async () => new Response(new ReadableStream({ cancel }))));
  const pending = fetchGtexEqtls(anchors, 200, []);
  await vi.advanceTimersByTimeAsync(4001);
  expect((await pending).evidence[0].error).toContain("timed out");
  expect(cancel).toHaveBeenCalled();
});
