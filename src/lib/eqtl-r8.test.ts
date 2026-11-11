import { afterEach, expect, it, vi } from "vitest";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { r8Anchor, r8Caps, r8Input, r8Native, r8Observation, r8StagedAnchor } from "../../test/eqtl-r8-fixture";
import { readCompleteTable } from "./complete-table";
import { fetchEqtlR8Context } from "./eqtl-r8-context";
import { gatherInputs } from "./gather-inputs";
import { projectedSubrequests } from "./gather-budget";
import { loadEqtlR8Observation, r8SynthesisContext } from "./eqtl-r8-synthesis";
import { qualifiedR8Catalogue } from "./eqtl-r8-qualified";
import { mergeEqtlSupport } from "./eqtl-support";

vi.mock("./complete-table", () => ({ readCompleteTable: vi.fn() }));
afterEach(() => { vi.resetAllMocks(); vi.unstubAllGlobals(); vi.useRealTimers(); });
const env = { EQTL_PARQUET_URL: "https://private.invalid/query", EQTL_PARQUET_TOKEN: "synthetic-private-token" };
const namespace = {} as DurableObjectNamespace;
const load = (caps = r8Caps, anchors = [r8StagedAnchor]) => loadEqtlR8Observation(namespace, "saved", caps, anchors);

it("defaults to context only, budgets one read, and rejects combined release selections", () => {
  const base = gatherInputs({});
  const parsed = gatherInputs({ eqtl_catalogue_r8: { ...r8Input, score_fine_mapping: undefined } });
  expect(parsed.eqtl_catalogue_r8?.score_fine_mapping).toBe(false);
  expect(projectedSubrequests(parsed, 1) - projectedSubrequests(base, 1)).toBe(1);
  expect(() => gatherInputs({ eqtl_catalogue: {}, eqtl_catalogue_r8: r8Input })).toThrow("Select only one");
});

it.each([{ release: "r7" }, { file_sha256: "bad" }, { metadata_sha256: undefined }, { dataset_id: "latest" }, { max_rows: 501 }, { score_fine_mapping: "yes" }])("rejects invalid r8 selection %#", change => {
  expect(() => gatherInputs({ eqtl_catalogue_r8: { ...r8Input, ...change } })).toThrow();
});

it("does not fetch for disabled, unresolved or ambiguous anchors", async () => {
  const fetcher = vi.fn(); vi.stubGlobal("fetch", fetcher);
  expect(await fetchEqtlR8Context([r8Anchor], undefined, [], env)).toEqual([]);
  for (const anchors of [[], [r8Anchor, r8Anchor], [{ ...r8Anchor, grch38: null }]]) {
    expect(await fetchEqtlR8Context(anchors, r8Input, [], env)).toMatchObject([{ status: "skipped", requested_variant: null }]);
  }
  expect(fetcher).not.toHaveBeenCalled();
});

it("retains the exact bounded native result and uses the existing authenticated backend contract", async () => {
  const native = r8Native(); const fetcher = vi.fn(async () => Response.json(native)); vi.stubGlobal("fetch", fetcher);
  const [row] = await fetchEqtlR8Context([r8Anchor], r8Input, [], env);
  expect(row).toMatchObject({ status: "observed", response_sha256: await hashCanonicalJson(native), coverage_complete: false });
  expect(JSON.parse(row.native_response_json!)).toEqual(native);
  expect(fetcher).toHaveBeenCalledOnce();
  const call = vi.mocked(fetch).mock.calls[0];
  expect(call[0]).toBe(env.EQTL_PARQUET_URL);
  expect(JSON.parse(call[1]!.body as string)).toMatchObject({ release: "r8_beta", file_role: "credible_sets", filters: [{ column: "variant", op: "eq", value: "chr1_10_A_G" }] });
  expect(JSON.stringify(row)).not.toContain(env.EQTL_PARQUET_TOKEN);
  expect(JSON.stringify(row)).not.toContain(env.EQTL_PARQUET_URL);
});

it("records unconfigured or private transport errors without leaking credentials", async () => {
  expect(await fetchEqtlR8Context([r8Anchor], r8Input, [])).toMatchObject([{ status: "error", error: expect.stringContaining("not configured") }]);
  vi.stubGlobal("fetch", vi.fn(async () => { throw new Error(env.EQTL_PARQUET_TOKEN + env.EQTL_PARQUET_URL); }));
  const rows = await fetchEqtlR8Context([r8Anchor], r8Input, [], env);
  expect(rows).toMatchObject([{ status: "error", native_response_json: null }]);
  expect(JSON.stringify(rows)).not.toContain(env.EQTL_PARQUET_TOKEN);
  expect(JSON.stringify(rows)).not.toContain(env.EQTL_PARQUET_URL);
});

it("bounds a hanging backend read by the shared deadline", async () => {
  vi.useFakeTimers(); vi.stubGlobal("fetch", vi.fn(() => new Promise(() => {})));
  const pending = fetchEqtlR8Context([r8Anchor], r8Input, [], env);
  await vi.advanceTimersByTimeAsync(35_001);
  expect(await pending).toMatchObject([{ status: "error", error: expect.stringContaining("deadline") }]);
});

it.each(["release", "source", "file", "metadata", "filter", "allele", "coordinate", "variant", "count"])("revalidates native %s even after it is rehashed", async mode => {
  const native = r8Native();
  if (mode === "release") native.release = "r7";
  if (mode === "source") native.source_url = "https://example.org/file";
  if (mode === "file") native.file_sha256 = "c".repeat(64);
  if (mode === "metadata") native.metadata_sha256 = "c".repeat(64);
  if (mode === "filter") native.filters[0].value = "other";
  if (mode === "allele") native.rows[0].alt = "T";
  if (mode === "coordinate") native.rows[0].position = 11;
  if (mode === "variant") native.rows[0].variant = "chr1_11_A_G";
  if (mode === "count") native.row_count = 0;
  vi.mocked(readCompleteTable).mockResolvedValue([await r8Observation(native)]);
  await expect(load()).rejects.toThrow();
});

it("checks staged hashes, anchors, selection/version and complete observation cardinality", async () => {
  const row = await r8Observation();
  vi.mocked(readCompleteTable).mockResolvedValue([{ ...row, response_sha256: "0".repeat(64) }]);
  await expect(load()).rejects.toThrow("hash mismatch");
  vi.mocked(readCompleteTable).mockResolvedValue([row]);
  await expect(load(r8Caps, [{ ...r8StagedAnchor, grch38_alt: "T" }])).rejects.toThrow("anchor mismatch");
  await expect(load(r8Caps.replace("2026-10-06-r8-parquet", "unknown"))).rejects.toThrow("version");
  await expect(load(JSON.stringify({ ...JSON.parse(r8Caps), eqtl_catalogue: {} }))).rejects.toThrow("Select only one");
  for (const rows of [[], [row, row]]) { vi.mocked(readCompleteTable).mockResolvedValue(rows); await expect(load()).rejects.toThrow("one explicit"); }
});

it("keeps error/skip context visible and leaves disabled gathers alone", async () => {
  expect(await load("{}")).toBeNull(); expect(readCompleteTable).not.toHaveBeenCalled();
  const [row] = await fetchEqtlR8Context([r8Anchor], r8Input, []);
  vi.mocked(readCompleteTable).mockResolvedValue([row]);
  const checked = await load(); expect(qualifiedR8Catalogue(checked, "saved").size).toBe(0);
  expect(r8SynthesisContext(checked, "saved").get("rs1")).toMatchObject([{ status: "error", release_status: "pre-release", coverage_complete: false }]);
});

it("qualifies once per native trait/credible-set/allele and preserves release/hash evidence", async () => {
  const native = r8Native(); native.rows.push({ ...native.rows[0], rsid: "rs2" }); native.row_count++;
  vi.mocked(readCompleteTable).mockResolvedValue([await r8Observation(native)]);
  const checked = await load(); const support = [...qualifiedR8Catalogue(checked, "saved").values()][0];
  expect(support.references).toHaveLength(1);
  expect(support.references[0]).toMatchObject({ release_requested: "r8_beta", release_status: "pre-release", snapshot_pinned: true,
    source_table: "eqtl_catalogue_r8_evidence", credible_set_file_sha256: r8Input.file_sha256, native_fine_mapping_row: native.rows[0] });
  checked!.input.score_fine_mapping = false; expect(qualifiedR8Catalogue(checked, "saved").size).toBe(0);
});

it.each([{ pip: 0.94 }, { pip: "1" }, { pip: 1.01 }, { cs_min_r2: 0.24 }, { cs_min_r2: 1.01 }, { cs_size: 0 }, { cs_id: "" },
  { gene_id: "bad" }, { molecular_trait_id: "" }, { beta: null }, { se: 0 }, { pvalue: -1 }, { pvalue: 1.1 }])("excludes unqualified fine-mapping statistics %#", async change => {
  const native = r8Native(); Object.assign(native.rows[0], change);
  vi.mocked(readCompleteTable).mockResolvedValue([await r8Observation(native)]);
  expect(qualifiedR8Catalogue(await load(), "saved").size).toBe(0);
});

it.each(["GTEx_v8", "GTEx_v10"])("deduplicates %s sample groups despite differing ontology granularity", async study => {
  const native = r8Native(); native.metadata.study_label = study;
  vi.mocked(readCompleteTable).mockResolvedValue([await r8Observation(native)]);
  const support = [...qualifiedR8Catalogue(await load(), "saved").values()][0];
  const merged = mergeEqtlSupport({ tissues: new Set(["Adipose_Subcutaneous"]), references: [{ tissue_ontology_id: "UBERON:0002190" }] }, support);
  expect(merged.tissues.size).toBe(1); expect(merged.references[1]).toMatchObject({ adds_tissue: false, tissue_count_basis: "already_counted_tissue" });
});
