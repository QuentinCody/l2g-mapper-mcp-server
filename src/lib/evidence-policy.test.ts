import { afterEach, expect, it, vi } from "vitest";
import { McpServer } from "@bio-mcp/shared/mcp";
import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";
import { gtexFixture } from "../../test/gtex-fixture";
import { GATHER_POLICY_VERSION } from "./evidence-policy";
import { registerL2gScore } from "../tools/l2g-score";

vi.mock("@bio-mcp/shared/staging/utils", async importOriginal => ({ ...await importOriginal<typeof import("@bio-mcp/shared/staging/utils")>(), queryDataFromDo: vi.fn() }));
afterEach(() => vi.resetAllMocks());

function registry(stored: unknown[], writeStatus = 200) {
  const server = new McpServer({ name: "fixture", version: "1" });
  const namespace = { idFromName: (name: string) => name, get: () => ({ fetch: async (request: Request) => {
    if (new URL(request.url).pathname === "/schema") return Response.json({ schema: { tables: { anchors: {}, candidate_genes: {}, l2g_predictions: {}, coloc: {}, eqtl: {}, gtex_evidence: {}, l2g_meta: {} } } });
    stored.push(await request.json());
    return Response.json({ success: writeStatus === 200 }, { status: writeStatus });
  } }) };
  // SAFETY: the stub implements the Durable Object methods used by scoring.
  registerL2gScore(server, { L2G_DATA_DO: namespace as unknown as DurableObjectNamespace });
  // SAFETY: inspect SDK registrations to invoke both actual tool handlers.
  return (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
}

async function fixtureRows() {
  const gtex = await gtexFixture();
  const tables: Record<string, unknown[]> = {
    l2g_meta: [{ gather_policy_version: GATHER_POLICY_VERSION, efo_id: "EFO_1", caps_json: JSON.stringify({ gtex_dataset_id: "gtex_v10" }) }],
    anchors: [gtex.anchor],
    candidate_genes: [{ locus_id: "L1", lead_rsid: "rs1", symbol: "GENE", ensembl_id: "ENSG1" }],
    l2g_predictions: [{ locus_id: "L1", lead_rsid: "rs1", symbol: "GENE", ensembl_id: "ENSG1", score: 1, variant_id: "1_10_A_G", study_locus_id: "SL1", genome_assembly: "GRCh38", anchor_match: "exact_resolved_variant_id", data_version_json: JSON.stringify({ year: "2026", month: "09" }), requested_trait_id: "EFO_1", trait_match: "exact_mapped_trait_id", study_json: '{"id":"S1","studyType":"gwas","traitFromSourceMappedIds":["EFO_1"]}', credible_set_json: '{"studyId":"S1","studyType":"gwas"}' }],
    coloc: [{ locus_id: "L1", h4: 0.99, clpp: null }],
    eqtl: [gtex.row],
    gtex_evidence: [gtex.observation],
    burden: [{ symbol: "GENE", p_value: 1e-20, phenotype: "unrelated trait" }],
    clinvar: [{ rsid: "rs1", clinical_significance: "Likely benign" }],
  };
  vi.mocked(queryDataFromDo).mockImplementation(async (_namespace, id, sql) => ({
    rows: tables[sql.split(" FROM ")[1]] ?? [], row_count: 1, sql,
    data_access_id: id, executed_at: "2026-09-29T00:00:00Z",
  }));
  return tables;
}

it("does not convert locus-only, unrelated-trait or prediction context into gene evidence", async () => {
  await fixtureRows();
  const stored: unknown[] = []; const tools = registry(stored);
  expect(Object.keys(tools).sort()).toEqual(["l2g_score", "mcp_l2g_score"]);
  for (const tool of Object.values(tools)) {
    const result = await tool.handler({ data_access_id: "fixture" }, {});
    expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true, data: {
      evidence_policy_version: "2026-10-06-r8-finemap", warnings: expect.arrayContaining([expect.stringContaining("not calibrated causal probabilities")]),
      top_5_cross_locus: [{ max_score: 0.45, ensembl_id: "ENSG1" }],
      per_locus_top_gene: [{ ensembl_id: "ENSG1" }],
    } } });
  }
  expect(stored).toHaveLength(2);
  expect(stored[0]).toMatchObject({ data: { scored_candidate_genes: [{
    overall_score: 0.45, coloc_component: 0, burden_component: 0, coding_component: 0,
    coloc_max_h4: null, burden_best_p: null, coding_support: "none", clinvar_support: "none",
    evidence_policy_version: "2026-10-06-r8-finemap",
  }] } });
});

it("retains both response fields and isError on invalid input and persistence failure", async () => {
  await fixtureRows();
  const tools = registry([], 500);
  for (const tool of Object.values(tools)) {
    for (const args of [{}, { data_access_id: "fixture" }]) {
      expect(await tool.handler(args, {})).toMatchObject({ content: expect.any(Array), structuredContent: { success: false }, isError: true });
    }
  }
});

it.each(["COLOC_PIP", "eCAVIAR"])("scores only qualified gene colocalisation and persists the selected %s support through both aliases", async method => {
  const tables = await fixtureRows(); const pred = tables.l2g_predictions[0] as Record<string, unknown>;
  const native = { colocalisationMethod: method, h4: method === "COLOC_PIP" ? 0.9 : null, clpp: method === "eCAVIAR" ? 0.8 : null, rightStudyType: "eqtl", otherStudyLocus: { studyId: "Q1", studyLocusId: "QL1", studyType: "eqtl", qtlGeneId: "ENSG1", study: { id: "Q1", studyType: "eqtl", target: { id: "ENSG1" }, biosample: { biosampleId: "UBERON_1" } } } };
  const row = { ...pred, lead_variant_id: "1_10_A_G", method, h4: native.h4, clpp: native.clpp, other_study_id: "Q1", credible_set_json: '{"studyId":"S1","studyType":"gwas","studyLocusId":"SL1","variant":{"id":"1_10_A_G"}}', native_coloc_json: JSON.stringify(native) };
  tables.coloc = [row, { ...row, ensembl_id: "ENSG2", h4: 1 }];
  const stored: unknown[] = [];
  for (const tool of Object.values(registry(stored))) {
    const result = await tool.handler({ data_access_id: "fixture" }, {});
    expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true, data: { top_5_cross_locus: [{ max_score: method === "COLOC_PIP" ? 0.675 : 0.65 }], warnings: expect.arrayContaining(["1 unqualified colocalisation rows excluded from scoring."]) } } });
  }
  for (const result of stored) {
    const saved = (result as { data: { scored_candidate_genes: Array<Record<string, unknown>> } }).data.scored_candidate_genes[0];
    expect(saved.coloc_component).toBe(method === "COLOC_PIP" ? 0.9 : 0.8);
    const support = JSON.parse(saved.coloc_support_json as string);
    expect(support[method === "COLOC_PIP" ? "h4" : "clpp"]).toMatchObject({ method, other_study_locus_id: "QL1", ensembl_id: "ENSG1", biosample_id: "UBERON_1" });
  }
});

it.each([undefined, "2026-09-29", "2026-09-30-exact-alleles", "2026-09-30-stable-candidates", "2026-10-01-trait-matching", "2026-10-01-trait-resolution", "2026-10-01-native-mappings"])("refuses legacy gathers (%s) before writing new-policy scores", async version => {
  const tables = await fixtureRows(); tables.l2g_meta = [{ gather_policy_version: version }];
  const stored: unknown[] = [];
  for (const tool of Object.values(registry(stored))) {
    expect(await tool.handler({ data_access_id: "fixture" }, {})).toMatchObject({
      content: expect.any(Array), isError: true, structuredContent: { success: false, error: { message: expect.stringContaining("Re-run l2g_gather") } },
    });
  }
  expect(stored).toEqual([]);
});

it.each([{ ensembl_id: "ENSG2" }, { lead_rsid: "rs2" }, { locus_id: "L2" }])("does not transfer a same-symbol prediction for another identity %j", async change => {
  const tables = await fixtureRows();
  tables.l2g_predictions = tables.l2g_predictions.map(row => ({ ...row as object, ...change }));
  const stored: unknown[] = []; await registry(stored).l2g_score.handler({ data_access_id: "fixture" }, {});
  expect(stored[0]).toMatchObject({ data: { scored_candidate_genes: [{ l2g_max: 0, l2g_support_json: "null" }] } });
});

it("persists source identifiers for the selected maximum and reports malformed rows", async () => {
  const tables = await fixtureRows(); tables.l2g_predictions.push({ locus_id: "L1", symbol: "GENE", score: 1 });
  const stored: unknown[] = []; const result = await registry(stored).l2g_score.handler({ data_access_id: "fixture" }, {});
  expect(result).toMatchObject({ structuredContent: { data: { warnings: expect.arrayContaining(["1 unqualified Open Targets rows excluded from scoring."]) } } });
  const saved = stored[0] as { data: { scored_candidate_genes: Array<{ l2g_support_json: string }> } };
  expect(JSON.parse(saved.data.scored_candidate_genes[0].l2g_support_json)).toMatchObject({ study_locus_id: "SL1", variant_id: "1_10_A_G", ensembl_id: "ENSG1", score: 1, requested_trait_id: "EFO_1", trait_match: "exact_mapped_trait_id" });
});

it("requires prediction traits to agree with staged gather metadata through both aliases", async () => {
  const tables = await fixtureRows(); tables.l2g_meta = [{ gather_policy_version: GATHER_POLICY_VERSION, efo_id: "EFO_2" }];
  const stored: unknown[] = [];
  for (const tool of Object.values(registry(stored))) {
    expect(await tool.handler({ data_access_id: "fixture" }, {})).toMatchObject({ structuredContent: { success: true, data: { warnings: expect.arrayContaining(["1 unqualified Open Targets rows excluded from scoring."]) } } });
  }
  expect(stored).toHaveLength(2);
  for (const result of stored) expect(result).toMatchObject({ data: { scored_candidate_genes: [{ l2g_max: 0, l2g_support_json: "null" }] } });
});

it("does not transfer same-symbol eQTL tissue support between stable gene IDs", async () => {
  const tables = await fixtureRows(); tables.eqtl = [{ lead_rsid: "rs1", symbol: "GENE", ensembl_id: "ENSG2", tissue: "Liver" }];
  const stored: unknown[] = []; await registry(stored).l2g_score.handler({ data_access_id: "fixture" }, {});
  expect(stored[0]).toMatchObject({ data: { scored_candidate_genes: [{ eqtl_tissue_hits: 0, overall_score: 0.4 }] } });
});

it.each(["l2g_predictions", "gtex_evidence", "eqtl", "anchors", "l2g_meta"].flatMap(table => ["failure", "cap", "count_cap"].map(mode => ({ table, mode }))))("refuses incomplete $table reads ($mode) without persisting replacement scores", async ({ table, mode }) => {
  await fixtureRows(); const original = vi.mocked(queryDataFromDo).getMockImplementation()!;
  vi.mocked(queryDataFromDo).mockImplementation(async (...args) => {
    const result = await original(...args);
    if (args[2].includes(`FROM ${table}`)) {
      if (mode === "failure") throw new Error("fixture query failed");
      return { ...result, ...(mode === "cap" ? { total_matching: 20000 } : { count_capped: true }) };
    }
    return result;
  });
  const stored: unknown[] = [];
  for (const tool of Object.values(registry(stored))) expect(await tool.handler({ data_access_id: "fixture" }, {})).toMatchObject({ isError: true, content: expect.any(Array), structuredContent: { success: false } });
  expect(stored).toEqual([]);
});

it("excludes legacy GTEx tissue claims without rewriting their source rows", async () => {
  const tables = await fixtureRows();
  tables.eqtl = [{ lead_rsid: "rs1", ensembl_id: "ENSG1", tissue: "Liver" }];
  const original = JSON.stringify(tables); const stored: unknown[] = [];
  for (const tool of Object.values(registry(stored))) {
    expect(await tool.handler({ data_access_id: "fixture" }, {})).toMatchObject({ structuredContent: { success: true, data: {
      warnings: expect.arrayContaining([expect.stringContaining("Re-gather legacy inputs")]),
    } } });
  }
  for (const value of stored) expect(value).toMatchObject({ data: { scored_candidate_genes: [{ eqtl_tissue_hits: 0, eqtl_support_json: "[]", overall_score: 0.4 }] } });
  expect(JSON.stringify(tables)).toBe(original);
});
