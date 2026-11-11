import { afterEach, expect, it, vi } from "vitest";
import { eqtlCaps, eqtlInput, eqtlNative, eqtlObservation, eqtlStagedAnchor } from "../../test/eqtl-catalogue-fixture";
import { readCompleteTable } from "./complete-table";
import { loadEqtlCatalogueContext } from "./eqtl-catalogue-synthesis";

vi.mock("./complete-table", () => ({ readCompleteTable: vi.fn() }));
afterEach(() => vi.resetAllMocks());
const namespace = {} as DurableObjectNamespace;
const load = (caps = eqtlCaps, anchors = [eqtlStagedAnchor]) => loadEqtlCatalogueContext(namespace, "saved", caps, anchors);

it("does not query legacy gathers or disabled lanes", async () => {
  expect((await load("{}")).size).toBe(0); expect(readCompleteTable).not.toHaveBeenCalled();
});

it("validates older nominal-only observations but does not upgrade them to fine-mapping requests", async () => {
  vi.mocked(readCompleteTable).mockResolvedValue([await eqtlObservation()]);
  const caps = { eqtl_catalogue: eqtlInput, eqtl_catalogue_context_version: "2026-10-01-r7-nominal" };
  expect((await load(JSON.stringify(caps))).get("rs1")).toMatchObject([{ context_only: true, fine_mapping_requested: false }]);
  await expect(load(JSON.stringify({ ...caps, eqtl_catalogue: { ...eqtlInput, fine_mapping_sha256: "d".repeat(64) } }))).rejects.toThrow("Unsupported");
});

it("rehashes native context, recalculates exact-allele row references, and retains file evidence", async () => {
  const row = await eqtlObservation(); row.allele_matching_row_count = 999;
  vi.mocked(readCompleteTable).mockResolvedValue([row]);
  const context = (await load()).get("rs1")![0];
  expect(context).toMatchObject({ source_data_access_id: "saved", source_table: "eqtl_catalogue_evidence", matching_row_indexes: [0, 1],
    allele_matching_row_count: 2, context_only: true, coverage_complete: false, dataset: eqtlNative().dataset,
    file_evidence: eqtlNative().region.file_evidence, effect_allele_basis: "ALT; native effects unchanged" });
  expect(readCompleteTable).toHaveBeenCalledWith(namespace, "saved", "eqtl_catalogue_evidence");
});

it("rejects altered native content, request provenance, or resolved anchors", async () => {
  const row = await eqtlObservation();
  vi.mocked(readCompleteTable).mockResolvedValue([{ ...row, native_response_json: row.native_response_json!.replace("BLUEPRINT", "altered") }]);
  await expect(load()).rejects.toThrow("hash mismatch");
  vi.mocked(readCompleteTable).mockResolvedValue([{ ...row, release_requested: "r8_beta" }]);
  await expect(load()).rejects.toThrow("request mismatch");
  vi.mocked(readCompleteTable).mockResolvedValue([row]);
  await expect(load(eqtlCaps, [{ ...eqtlStagedAnchor, grch38_alt: "T" }])).rejects.toThrow("anchor mismatch");
});

it.each(["position", "release", "count", "cap", "source", "index", "raw", "coverage", "files"])("checks the native contract even when its hash is valid (%s)", async mode => {
  const native = eqtlNative();
  if (mode === "position") native.region.end = 11;
  if (mode === "release") native.dataset.catalogue_snapshot!.release = "r8_beta" as never;
  if (mode === "count") native.region.row_count = 100;
  if (mode === "cap") { native.region.rows = Array(201).fill(native.region.rows[0]); native.region.raw_rows = Array(201).fill(native.region.raw_rows[0]); native.region.row_count = 201; }
  if (mode === "source") { native.dataset.sumstats_url = "https://example.org/fake"; native.region.source_file = native.dataset.sumstats_url; }
  if (mode === "index") native.region.index_file = "other";
  if (mode === "raw") native.region.raw_rows = [];
  if (mode === "coverage") native.region.coverage_complete = true as never;
  if (mode === "files") native.region.file_evidence.header.sha256 = "bad";
  vi.mocked(readCompleteTable).mockResolvedValue([await eqtlObservation(native)]);
  await expect(load()).rejects.toThrow();
});

it("rejects missing or multiple observations and unsupported policy versions", async () => {
  for (const rows of [[], [await eqtlObservation(), await eqtlObservation()]]) {
    vi.mocked(readCompleteTable).mockResolvedValue(rows); await expect(load()).rejects.toThrow("one explicit");
  }
  await expect(load(JSON.stringify({ eqtl_catalogue: eqtlInput, eqtl_catalogue_context_version: "future" }))).rejects.toThrow("Unsupported");
});

it("keeps failed reads explicit and refuses an observed status without native evidence", async () => {
  const row = { ...await eqtlObservation(), status: "error", error: "HTTP 429", native_response_json: null, response_sha256: null };
  vi.mocked(readCompleteTable).mockResolvedValue([row]);
  expect((await load()).get("rs1")).toMatchObject([{ status: "error", error: "HTTP 429", allele_matching_row_count: 0, dataset: null }]);
  vi.mocked(readCompleteTable).mockResolvedValue([{ ...row, status: "observed" }]);
  await expect(load()).rejects.toThrow("lacks native evidence");
});
