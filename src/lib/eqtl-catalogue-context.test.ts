import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { fetchDatasets } from "@bio-mcp/shared/adapters/eqtl/ftp-catalogue";
import { queryRegion } from "@bio-mcp/shared/adapters/eqtl/sumstats";
import { eqtlAnchor, eqtlInput, eqtlNative } from "../../test/eqtl-catalogue-fixture";
import { fetchEqtlCatalogueContext, matchingEqtlRows } from "./eqtl-catalogue-context";
import { gatherInputs } from "./gather-inputs";
import { projectedSubrequests } from "./gather-budget";

vi.mock("@bio-mcp/shared/adapters/eqtl/ftp-catalogue", () => ({ fetchDatasets: vi.fn() }));
vi.mock("@bio-mcp/shared/adapters/eqtl/sumstats", () => ({ queryRegion: vi.fn() }));
beforeEach(() => {
  vi.mocked(fetchDatasets).mockResolvedValue([eqtlNative().dataset]);
  vi.mocked(queryRegion).mockResolvedValue(eqtlNative().region);
});
afterEach(() => { vi.resetAllMocks(); vi.unstubAllGlobals(); vi.useRealTimers(); });

it("makes no upstream request unless an explicit dataset and resolved anchor are selected", async () => {
  expect(await fetchEqtlCatalogueContext([eqtlAnchor], undefined, [])).toEqual([]);
  for (const anchors of [[], [eqtlAnchor, eqtlAnchor], [{ ...eqtlAnchor, grch38: null }], [{ ...eqtlAnchor, grch38: { ...eqtlAnchor.grch38, alt: "G,T" } }]]) {
    expect(await fetchEqtlCatalogueContext(anchors, eqtlInput, [])).toMatchObject([{ status: "skipped", source_url: null, requested_variant: null }]);
  }
  expect(fetchDatasets).not.toHaveBeenCalled(); expect(queryRegion).not.toHaveBeenCalled();
});

it.each([{ release: "r8_beta" }, { metadata_sha256: undefined }, { paths_sha256: "bad" }, { dataset_id: "latest" }, { max_rows: 501 }])("rejects ambiguous or unbounded selection before requesting data (%j)", async change => {
  await expect(fetchEqtlCatalogueContext([eqtlAnchor], { ...eqtlInput, ...change } as never, [])).rejects.toThrow();
  expect(fetchDatasets).not.toHaveBeenCalled();
});

it("retains native precision and duplicate rsID rows while matching exact alleles", async () => {
  const warnings: string[] = []; const native = eqtlNative();
  const [row] = await fetchEqtlCatalogueContext([eqtlAnchor], eqtlInput, warnings);
  expect(queryRegion).toHaveBeenCalledWith({ dataset: native.dataset, chromosome: "1", start: 10, end: 10, maxRows: 200 }, expect.objectContaining({ fetchFile: expect.any(Function), loadIndexEvidence: expect.any(Function) }));
  expect(row).toMatchObject({ status: "observed", requested_variant: "1_10_A_G", allele_matching_row_count: 2, context_only: true, coverage_complete: false });
  expect(JSON.parse(row.native_response_json!)).toEqual(native);
  expect(row.response_sha256).toBe(await hashCanonicalJson(native));
  expect(row.native_response_json).toContain("1e-400");
  expect(warnings.join(" ")).toContain("not colocalisation or independent GTEx replication");
  expect(matchingEqtlRows(native.region, "1_10_G_A")).toEqual([]);
  expect(matchingEqtlRows(native.region, "1_11_A_G")).toEqual([]);
});

it.each(["metadata_sha256", "paths_sha256"] as const)("rejects changed %s before reading association files", async field => {
  const native = eqtlNative(); native.dataset.catalogue_snapshot![field] = "d".repeat(64);
  vi.mocked(fetchDatasets).mockResolvedValue([native.dataset]);
  expect(await fetchEqtlCatalogueContext([eqtlAnchor], eqtlInput, [])).toMatchObject([{ status: "error", error: expect.stringContaining("changed"), native_response_json: null }]);
  expect(queryRegion).not.toHaveBeenCalled();
});

it("records a missing dataset or failed reader without claiming an empty successful query", async () => {
  vi.mocked(fetchDatasets).mockResolvedValueOnce([]);
  expect(await fetchEqtlCatalogueContext([eqtlAnchor], eqtlInput, [])).toMatchObject([{ status: "error", error: expect.stringContaining("absent") }]);
  vi.mocked(queryRegion).mockRejectedValueOnce(new Error("range request requires HTTP 206"));
  expect(await fetchEqtlCatalogueContext([eqtlAnchor], eqtlInput, [])).toMatchObject([{ status: "error", source_url: eqtlNative().dataset.sumstats_url, native_response_json: null, error: expect.stringContaining("206") }]);
});

it("aborts the request-scoped transport at the lane deadline", async () => {
  vi.useFakeTimers();
  vi.mocked(fetchDatasets).mockImplementationOnce(async transport => {
    await transport!("/data_tables/dataset_metadata_r7.tsv"); return [];
  });
  let signal: AbortSignal | undefined;
  vi.stubGlobal("fetch", vi.fn((_url, init) => new Promise((_resolve, reject) => {
    signal = init.signal; signal!.addEventListener("abort", () => reject(signal!.reason), { once: true });
  })));
  const pending = fetchEqtlCatalogueContext([eqtlAnchor], eqtlInput, []);
  await vi.advanceTimersByTimeAsync(15_000);
  expect(await pending).toMatchObject([{ status: "error", error: expect.stringContaining("15 seconds") }]);
  expect(signal?.aborted).toBe(true); expect(queryRegion).not.toHaveBeenCalled();
});

it("budgets five direct requests only for the selected optional lane", () => {
  const base = gatherInputs({ seed_rsids: ["rs1"] });
  const enabled = gatherInputs({ seed_rsids: ["rs1"], eqtl_catalogue: eqtlInput });
  expect(projectedSubrequests(enabled, 1) - projectedSubrequests(base, 1)).toBe(5);
});

it("cancels pending peer metadata requests when another metadata read fails", async () => {
  let signal: AbortSignal | undefined;
  vi.stubGlobal("fetch", vi.fn((_url, init) => new Promise((_resolve, reject) => {
    signal = init.signal; signal!.addEventListener("abort", () => reject(signal!.reason), { once: true });
  })));
  vi.mocked(fetchDatasets).mockImplementationOnce(async transport => {
    await Promise.all([transport!("/data_tables/dataset_metadata_r7.tsv"), Promise.reject(new Error("file map unavailable"))]); return [];
  });
  expect(await fetchEqtlCatalogueContext([eqtlAnchor], eqtlInput, [])).toMatchObject([{ status: "error", error: "file map unavailable" }]);
  expect(signal?.aborted).toBe(true); expect(queryRegion).not.toHaveBeenCalled();
});
