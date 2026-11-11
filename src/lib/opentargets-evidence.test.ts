import { afterEach, expect, it, vi } from "vitest";
import { opentargetsGraphql } from "@bio-mcp/shared/adapters/opentargets";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { applyOpenTargetsBundle, fetchOpenTargetsBundle as fetchBundle } from "./opentargets-evidence";
import { qualifiedL2gScores } from "./qualified-l2g";
import { qualifiedColocScores } from "./qualified-coloc";
import { OT_BATCH_DETAIL_QUERY, OT_SEARCH_STUDY_QUERY, OT_STUDY_CS_QUERY } from "./opentargets-contract";
import { boundedOpenTargetsFetch } from "./opentargets-http";
import { createOpenTargetsObservations } from "./opentargets-observations";

vi.mock("@bio-mcp/shared/adapters/opentargets", () => ({ opentargetsGraphql: vi.fn() }));
afterEach(() => vi.resetAllMocks());
const anchors = () => [{ rsid: "rs1", grch38: { chr: "1", pos: 10, ref: "A", alt: "G" } }];
const fetchOpenTargetsBundle = (a: ReturnType<typeof anchors>, terms: string[], cap: number, warnings: string[]) => fetchBundle(a, terms, cap, warnings, "EFO_1");
const meta = { name: "Open Targets", dataVersion: { year: "2026", month: "09", iteration: "0" }, apiVersion: { x: "4", y: "0", z: "0", suffix: null } };
function fixtures() {
  const study = { id: "S1", projectId: "fixture-project", studyType: "gwas", traitFromSource: "native trait", traitFromSourceMappedIds: ["EFO_1"], nSamples: 123, discoverySamples: [{ ancestry: "native ancestry", sampleSize: 123 }], qualityControls: ["source warning"], analysisFlags: ["source flag"] };
  const pred = { score: 0.8, shapBaseValue: 0.2, features: [{ name: "transPQtlColocH4Maximum", value: 0.9, shapValue: 0.1 }, { name: "vepMean", value: 0.3, shapValue: -0.1 }], target: { id: "ENSG1", approvedSymbol: "GENE" } };
  const coloc = { colocalisationMethod: "COLOC_PIP", h3: 0.02, h4: 0.95, clpp: null, rightStudyType: "eqtl", otherStudyLocus: { studyId: "Q1", studyLocusId: "QL1", studyType: "eqtl", qtlGeneId: "ENSG1", isTransQtl: false, study: { id: "Q1", studyType: "eqtl", target: pred.target, biosample: { biosampleId: "UBERON:fixture", biosampleName: "fixture" } } } };
  const search = { data: { meta, search: { total: 1, hits: [{ score: 3, object: { ...study, hasSumstats: true } }] } } };
  const sets = { data: { meta, study: { credibleSets: { count: 1, rows: [{ studyLocusId: "L1", variant: { id: "1_10_A_G", rsIds: ["RS1"] } }] } } } };
  const detail = { data: { meta, credibleSets: { count: 1, rows: [{ studyLocusId: "L1", studyId: "S1", studyType: "gwas", confidence: "native", qualityControls: ["flag"], variant: { id: "1_10_A_G", rsIds: ["rs1"] }, study, l2GPredictions: { count: 1, rows: [pred] }, colocalisation: { count: 1, rows: [coloc] } }] } } };
  return { search, sets, detail, pred, study, coloc };
}
function enqueue(...responses: unknown[]) {
  for (const response of responses) vi.mocked(opentargetsGraphql).mockResolvedValueOnce(response as Awaited<ReturnType<typeof opentargetsGraphql>>);
}

it("captures release, native study/ancestry and model features without assigning an unobserved model version", async () => {
  const f = fixtures(); enqueue(f.search, f.sets, f.detail);
  const warnings: string[] = [];
  const result = await fetchOpenTargetsBundle(anchors(), ["trait"], 175, warnings);
  expect(warnings).toEqual([]);
  expect(result.l2g).toEqual([expect.objectContaining({ symbol: "GENE", ensembl_id: "ENSG1", score: 0.8, model_version: null, data_version_json: JSON.stringify(meta.dataVersion), variant_id: "1_10_A_G", study_json: JSON.stringify(f.study), features_json: JSON.stringify(f.pred.features), shap_base_value: 0.2 })]);
  expect(result.evidence).toHaveLength(3);
  expect(result.coloc[0]).toMatchObject({ ensembl_id: "ENSG1", requested_trait_id: "EFO_1", trait_match: "exact_mapped_trait_id", data_version_json: JSON.stringify(meta.dataVersion), native_coloc_json: JSON.stringify(f.coloc) });
  expect([...qualifiedColocScores(result.coloc, result.l2g, "EFO_1").scores.values()]).toMatchObject([{ h4: { value: 0.95, biosample_id: "UBERON:fixture" }, clpp: null }]);
  for (const [index, native] of [f.search, f.sets, f.detail].entries()) {
    expect(result.evidence[index]).toMatchObject({ response_json: JSON.stringify(native), response_sha256: await hashCanonicalJson(native), data_version_json: JSON.stringify(meta.dataVersion), usable: true });
  }
  expect(vi.mocked(opentargetsGraphql).mock.calls.map(call => call.slice(0, 2))).toEqual([
    [OT_SEARCH_STUDY_QUERY, { q: "trait" }], [OT_STUDY_CS_QUERY, { id: "S1" }],
    [OT_BATCH_DETAIL_QUERY, { ids: ["L1"], size: 1, colocSize: 175 }],
  ]);
  expect(vi.mocked(opentargetsGraphql).mock.calls.every(call => call[2]?.fetchImpl === boundedOpenTargetsFetch && call[2]?.timeoutMs === 4000)).toBe(true);
  const tables = { l2g_predictions: [], coloc: [], opentargets_evidence: [] };
  const symbols = new Map<string, string>();
  applyOpenTargetsBundle(result, [{ rsid: "rs1", locus_id: "chr1:1-1000010" }], tables, symbols);
  expect(tables.l2g_predictions).toMatchObject([{ locus_id: "chr1:1-1000010" }]);
  expect(tables.opentargets_evidence).toEqual(result.evidence);
  expect(symbols.get("GENE")).toBe("ENSG1");
});

it("excludes all Open Targets ranking rows when an observed release changes mid-gather", async () => {
  const f = fixtures(); f.detail.data.meta = { ...meta, dataVersion: { ...meta.dataVersion, month: "10" } };
  enqueue(f.search, f.sets, f.detail);
  const warnings: string[] = []; const result = await fetchOpenTargetsBundle(anchors(), ["trait"], 100, warnings);
  expect(result.l2g).toEqual([]); expect(result.coloc).toEqual([]); expect(result.symbolsNeedingEnsembl.size).toBe(0);
  expect(result.evidence).toHaveLength(3); expect(result.evidence.every(row => !row.usable)).toBe(true);
  expect(warnings.join(" ")).toContain("data release changed");
});

it.each(["search", "detail"])("retains partial GraphQL failures from %s instead of admitting their data", async phase => {
  const f = fixtures(); const failed = { ...(phase === "search" ? f.search : f.detail), errors: [{ message: "field denied", path: ["search"], extensions: { code: "FIXTURE" } }] };
  enqueue(...(phase === "search" ? [failed] : [f.search, f.sets, failed]));
  const warnings: string[] = []; const result = await fetchOpenTargetsBundle(anchors(), ["trait"], 100, warnings);
  expect(result.l2g).toEqual([]);
  expect(result.evidence).toContainEqual(expect.objectContaining({ usable: false, response_json: JSON.stringify(failed) }));
  expect(warnings.join(" ")).toContain("GraphQL returned errors");
});

it.each([undefined, {}, { dataVersion: { year: 2026, month: 9 } }])("does not guess a release when metadata is missing or malformed %#", async broken => {
  const f = fixtures(); enqueue({ data: { ...f.search.data, meta: broken } });
  const warnings: string[] = []; const result = await fetchOpenTargetsBundle(anchors(), ["trait"], 100, warnings);
  expect(result.l2g).toEqual([]); expect(result.evidence[0].usable).toBe(false);
  expect(warnings.join(" ")).toContain("Missing Open Targets data-release");
});

it("reports each truncated page without changing native counts or inventing completeness", async () => {
  const f = fixtures(); f.search.data.search.total = 30; f.sets.data.study.credibleSets.count = 450;
  f.detail.data.credibleSets.count = 3; f.detail.data.credibleSets.rows[0].l2GPredictions.count = 150; f.detail.data.credibleSets.rows[0].colocalisation.count = 300;
  enqueue(f.search, f.sets, f.detail);
  const warnings: string[] = []; const result = await fetchOpenTargetsBundle(anchors(), ["trait"], 100, warnings);
  expect(warnings).toHaveLength(5);
  expect(result.l2g).toHaveLength(1);
  expect(JSON.parse(result.evidence[2].response_json!)).toEqual(f.detail);
});

it("does not turn missing, nonfinite or out-of-range prediction scores into zero evidence", async () => {
  const f = fixtures();
  const rows = [null, undefined, -1, 2, Number.NaN].map(score => ({ ...f.pred, score }));
  enqueue(f.search, f.sets, { data: { ...f.detail.data, credibleSets: { count: 1, rows: [{ ...f.detail.data.credibleSets.rows[0], l2GPredictions: { count: rows.length, rows } }] } } });
  expect((await fetchOpenTargetsBundle(anchors(), ["trait"], 100, [])).l2g).toEqual([]);
});

it("retains request failure state and performs no work without anchors or traits", async () => {
  await fetchOpenTargetsBundle([], ["trait"], 100, []); await fetchOpenTargetsBundle(anchors(), [], 100, []);
  expect(opentargetsGraphql).not.toHaveBeenCalled();
  vi.mocked(opentargetsGraphql).mockRejectedValueOnce(new Error("HTTP 503 unavailable"));
  const warnings: string[] = []; const result = await fetchOpenTargetsBundle(anchors(), ["trait"], 100, warnings);
  expect(result.evidence[0]).toMatchObject({ usable: false, response_json: null, error: "HTTP 503 unavailable" });
});

it("does not join predictions for unrelated returned study-locus IDs", async () => {
  const f = fixtures(); f.detail.data.credibleSets.rows[0].studyLocusId = "OTHER";
  enqueue(f.search, f.sets, f.detail);
  const result = await fetchOpenTargetsBundle(anchors(), ["trait"], 100, []);
  expect(result.l2g).toEqual([]); expect(result.coloc).toEqual([]);
});

it("caps the synchronous detail fan-out and reports the selected subset", async () => {
  const f = fixtures();
  const rows = Array.from({ length: 201 }, (_, index) => ({ studyLocusId: `L${index}`, variant: { id: "1_10_A_G", rsIds: ["rs1"] } }));
  enqueue(f.search, { data: { meta, study: { credibleSets: { count: 201, rows } } } }, ...Array.from({ length: 5 }, () => ({ data: { meta, credibleSets: { count: 0, rows: [] } } })));
  const warnings: string[] = []; await fetchOpenTargetsBundle(anchors(), ["trait"], 100, warnings);
  const details = vi.mocked(opentargetsGraphql).mock.calls.filter(call => call[0] === OT_BATCH_DETAIL_QUERY);
  expect(details).toHaveLength(5); expect(details.every(call => call[1]?.size === 40)).toBe(true);
  expect(warnings.join(" ")).toContain("capped at 200 study loci");
});

it("bounds cumulative retained native evidence and excludes overflow from ranking", async () => {
  const native = { data: { meta, fixture: "a".repeat(3 * 1024 * 1024) } };
  enqueue(native, native, native, native);
  const warnings: string[] = []; const observations = createOpenTargetsObservations(warnings);
  for (let index = 0; index < 4; index++) await observations.request("fixture", "query", {});
  expect(observations.evidence.filter(row => row.usable)).toHaveLength(3);
  expect(observations.evidence[3]).toMatchObject({ usable: false, response_json: null, error: expect.stringContaining("12 MiB") });
});

it.each([false, true])("rejects conflicting detail alleles, including duplicate rows in either order (%s)", async reverse => {
  const f = fixtures(); const row = f.detail.data.credibleSets.rows[0];
  f.detail.data.credibleSets.rows.push({ ...row, variant: { id: "1_10_A_T", rsIds: ["rs1"] } });
  if (reverse) f.detail.data.credibleSets.rows.reverse();
  enqueue(f.search, f.sets, f.detail);
  const warnings: string[] = []; const result = await fetchOpenTargetsBundle(anchors(), ["trait"], 100, warnings);
  expect(result.l2g).toEqual([]); expect(result.coloc).toEqual([]);
  expect(warnings.join(" ")).toContain("detail lead variant differs");
  expect(result.evidence).toHaveLength(3);
});

it("requires resolved alleles before querying and records exact matching on accepted rows", async () => {
  const warnings: string[] = [];
  await fetchBundle([{ rsid: "rs1" }], ["trait"], 100, warnings, "EFO_1");
  expect(opentargetsGraphql).not.toHaveBeenCalled();
  const f = fixtures(); enqueue(f.search, f.sets, f.detail);
  const result = await fetchOpenTargetsBundle(anchors(), ["trait"], 100, []);
  for (const row of [...result.l2g, ...result.coloc]) expect(row).toMatchObject({ genome_assembly: "GRCh38", anchor_match: "exact_resolved_variant_id" });
});

it("rejects conflicting detail rows across batch responses", async () => {
  const f = fixtures(); const row = f.detail.data.credibleSets.rows[0];
  const rows = Array.from({ length: 41 }, (_, index) => ({ studyLocusId: `L${index}`, variant: row.variant }));
  enqueue(f.search, { data: { meta, study: { credibleSets: { count: 41, rows } } } }, f.detail,
    { data: { meta, credibleSets: { rows: [{ ...row, variant: { id: "1_10_A_T" } }] } } });
  const warnings: string[] = []; const result = await fetchOpenTargetsBundle(anchors(), ["trait"], 100, warnings);
  expect(result.l2g).toEqual([]); expect(result.coloc).toEqual([]);
  expect(warnings.join(" ")).toContain("detail lead variant differs");
});

it("requires the detail variant ID even when rsID aliases match", async () => {
  const f = fixtures(); const row = f.detail.data.credibleSets.rows[0];
  enqueue(f.search, f.sets, { data: { meta, credibleSets: { rows: [{ ...row, variant: { rsIds: ["rs1"] } }] } } });
  const result = await fetchOpenTargetsBundle(anchors(), ["trait"], 100, []);
  expect(result.l2g).toEqual([]); expect(result.coloc).toEqual([]);
});

it("retains nonmatching and unresolved predictions as context without admitting their scores", async () => {
  for (const selected of ["EFO_2", null]) {
    const f = fixtures(); enqueue(f.search, f.sets, f.detail);
    const warnings: string[] = []; const result = await fetchBundle(anchors(), ["native trait"], 100, warnings, selected);
    expect(result.l2g).toHaveLength(1);
    expect(result.evidence).toHaveLength(3);
    expect(result.l2g[0]).toMatchObject({ requested_trait_id: selected, trait_match: selected ? "unmatched" : "unresolved" });
    expect(qualifiedL2gScores(result.l2g)).toMatchObject({ excluded: 1, scores: new Map() });
    expect(warnings.join(" ")).toContain("context only");
  }
});

it("prioritizes exact mapped GWAS studies before applying the six-study cap", async () => {
  const hits = Array.from({ length: 6 }, (_, i) => ({ object: { id: `WRONG${i}`, hasSumstats: true, studyType: "gwas", traitFromSourceMappedIds: ["EFO_2"] } }));
  hits.push({ object: { id: "MATCH", hasSumstats: false, studyType: "gwas", traitFromSourceMappedIds: ["EFO_1"] } });
  enqueue({ data: { meta, search: { total: 7, hits } } }, ...Array.from({ length: 6 }, () => ({ data: { meta, study: { credibleSets: { count: 0, rows: [] } } } })));
  await fetchBundle(anchors(), ["native trait"], 100, [], "EFO_1");
  const calls = vi.mocked(opentargetsGraphql).mock.calls.filter(call => call[0] === OT_STUDY_CS_QUERY);
  expect(calls).toHaveLength(6);
  expect(calls[0][1]).toEqual({ id: "MATCH" });
});
