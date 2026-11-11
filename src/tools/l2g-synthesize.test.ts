import { afterEach, expect, it, vi } from "vitest";
import { McpServer } from "@bio-mcp/shared/mcp";
import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";
import { registerL2gSynthesize } from "./l2g-synthesize";
import { eqtlCaps, eqtlObservation, eqtlStagedAnchor } from "../../test/eqtl-catalogue-fixture";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { GNOMAD_GENE_QUERY } from "../lib/gnomad-context";

vi.mock("@bio-mcp/shared/staging/utils", async importOriginal => ({ ...await importOriginal<typeof import("@bio-mcp/shared/staging/utils")>(), queryDataFromDo: vi.fn() }));
afterEach(() => vi.resetAllMocks());

function registry(policy?: string, collisions = false, options: { warnings?: string; namespace?: DurableObjectNamespace; gnomad?: Record<string, unknown>; coloc?: boolean } = {}) {
  const scored = {
      evidence_policy_version: policy, locus_id: "L1", lead_rsid: "rs1", symbol: "GENE", ensembl_id: "ENSG1",
      l2g_support_json: JSON.stringify({ study_locus_id: "SL1", variant_id: "1_10_A_G", data_version_json: "2026-09", requested_trait_id: "EFO_1", trait_match: "exact_mapped_trait_id" }),
      eqtl_support_json: JSON.stringify([{ source_data_access_id: "source-evidence", source_table: "gtex_evidence", response_sha256: "a".repeat(64), source_row_index: 0, dataset_id_requested: "gtex_v10", tissue: "Liver", normalized_effect_size: -0.4, p_value: 0 }]),
      l2g_max: 1, coloc_max_h4: null, coloc_max_clpp: null, eqtl_tissue_hits: 1, eqtl_tissues: "Liver",
      burden_best_p: null, coding_support: "none", clinvar_support: "none", l2g_component: 1,
      coloc_component: 0, eqtl_component: 1 / 3, burden_component: 0, coding_component: 0, overall_score: 0.45, confidence: "Low",
      ...(options.coloc ? { coloc_max_clpp: 0.8, coloc_component: 0.8, overall_score: 0.65, confidence: "Medium", coloc_support_json: '{"h4":null,"clpp":{"method":"eCAVIAR","value":0.8,"other_study_locus_id":"QL1","ensembl_id":"ENSG1"}}' } : {}),
  };
  const scores = [scored, ...(collisions ? [{ ...scored, ensembl_id: "ENSG2", lead_rsid: "rs2", l2g_max: 0.2, l2g_component: 0.2, overall_score: 0.1, eqtl_tissues: "Brain" }] : [])];
  vi.mocked(queryDataFromDo).mockImplementation(async (_namespace, id, sql) => ({
    rows: sql.includes("FROM scored_candidate_genes") ? scores : sql.includes("FROM cross_locus_ranked_genes") ? scores.map(row => ({ symbol: row.symbol, ensembl_id: row.ensembl_id, supporting_loci: 1, mean_score: row.overall_score, max_score: row.overall_score })) : sql.includes("FROM gnomad") ? [options.gnomad] : sql.includes("FROM l2g_meta") && (options.warnings || options.gnomad) ? [{ warnings_json: JSON.stringify(options.warnings ? [options.warnings] : []), caps_json: JSON.stringify({ include_gnomad_context: Boolean(options.gnomad) }), sources_queried: options.gnomad ? "gnomad" : "" }] : [],
    row_count: 1, sql, data_access_id: id, executed_at: "2026-09-29T00:00:00Z",
  }));
  const server = new McpServer({ name: "fixture", version: "1" });
  // SAFETY: all DO queries are mocked; this handler only checks namespace availability.
  registerL2gSynthesize(server, { L2G_DATA_DO: options.namespace ?? {} as DurableObjectNamespace });
  // SAFETY: invoke actual handlers from the SDK tool registry.
  return (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
}

it.each([undefined, "2026-09-29", "2026-09-30", "2026-09-30-stable-candidates", "2026-10-01-trait-matching", "2026-10-01-trait-resolution", "2026-10-01-native-mappings", "2026-10-01-gene-coloc", "2026-10-01-gtex-provenance"])("requires rescoring legacy outputs (%s) rather than labeling them with the new policy", async policy => {
  const tools = registry(policy);
  expect(Object.keys(tools).sort()).toEqual(["l2g_synthesize", "mcp_l2g_synthesize"]);
  for (const tool of Object.values(tools)) {
    expect(await tool.handler({ data_access_id: "fixture" }, {})).toMatchObject({
      content: expect.any(Array), isError: true, structuredContent: { success: false, error: { message: expect.stringContaining("Re-run l2g_score") } },
    });
  }
});

it("carries evidence limitations into the final research summary", async () => {
  for (const tool of Object.values(registry("2026-10-06-r8-finemap"))) {
    expect(await tool.handler({ data_access_id: "fixture" }, {})).toMatchObject({ content: expect.any(Array), structuredContent: { success: true, data: {
      json: { meta: { evidence_policy_version: "2026-10-06-r8-finemap" }, limitations: expect.arrayContaining([expect.stringContaining("not calibrated causal probabilities")]) },
      markdown_summary: expect.stringContaining("no variant-to-gene consequence"),
    } } });
  }
});

it("includes checked Catalogue references at the anchor without changing gene scores through either alias", async () => {
  const tools = registry("2026-10-06-r8-finemap");
  const original = vi.mocked(queryDataFromDo).getMockImplementation()!;
  const observation = await eqtlObservation();
  vi.mocked(queryDataFromDo).mockImplementation(async (...args) => {
    const result = await original(...args); const sql = args[2];
    if (sql.includes("FROM l2g_meta")) return { ...result, rows: [{ caps_json: eqtlCaps }] };
    if (sql.includes("FROM anchors")) return { ...result, rows: [eqtlStagedAnchor] };
    if (sql.includes("FROM eqtl_catalogue_evidence")) return { ...result, rows: [observation] };
    return result;
  });
  for (const tool of Object.values(tools)) {
    expect(await tool.handler({ data_access_id: "saved" }, {})).toMatchObject({ content: expect.any(Array), structuredContent: { success: true, data: { json: {
      anchors: [{ rsid: "rs1", eqtl_catalogue_context: [{ source_data_access_id: "saved", source_table: "eqtl_catalogue_evidence", matching_row_indexes: [0, 1], context_only: true, response_sha256: observation.response_sha256 }] }],
      loci: [{ candidate_genes: [{ overall_score: 0.45 }] }],
    } } } });
  }
  observation.response_sha256 = "d".repeat(64);
  for (const tool of Object.values(tools)) expect(await tool.handler({ data_access_id: "saved" }, {})).toMatchObject({ isError: true, content: expect.any(Array), structuredContent: { success: false } });
});

it("preserves selected colocalisation references and labels CLPP correctly in synthesis", async () => {
  for (const tool of Object.values(registry("2026-10-06-r8-finemap", false, { coloc: true }))) {
    const result = await tool.handler({ data_access_id: "fixture" }, {}) as { structuredContent: { data: { json: { loci: Array<{ candidate_genes: Array<{ evidence: { coloc_support_json: string; coloc_max_h4: null; coloc_max_clpp: number }; rationale: string[] }> }> }; markdown_summary: string } } };
    const data = result.structuredContent.data; const gene = data.json.loci[0].candidate_genes[0];
    expect(JSON.parse(gene.evidence.coloc_support_json)).toEqual({ h4: null, clpp: { method: "eCAVIAR", value: 0.8, other_study_locus_id: "QL1", ensembl_id: "ENSG1" } });
    expect(gene.evidence).toMatchObject({ coloc_max_h4: null, coloc_max_clpp: 0.8 });
    expect(data.markdown_summary).toContain("coloc=CLPP:0.800");
    expect(JSON.stringify(data.json)).toContain("eCAVIAR CLPP=0.800");
    expect(data.markdown_summary).toContain("double-count evidence");
  }
});

it("keeps colliding symbols distinct in cross-locus JSON, Markdown and figures", async () => {
  for (const tool of Object.values(registry("2026-10-06-r8-finemap", true))) {
    const result = await tool.handler({ data_access_id: "fixture" }, {}) as { structuredContent: { data: { json: { cross_locus_ranked_genes: unknown[] }; markdown_summary: string; mermaid_figures: Array<{ mermaid: string }> } } };
    const data = result.structuredContent.data;
    expect(data.json).toMatchObject({ loci: [{ lead_rsids: ["rs1", "rs2"], candidate_genes: [{ lead_rsid: "rs1" }, { lead_rsid: "rs2" }] }] });
    expect(data.markdown_summary).toContain("anchors: `rs1`, `rs2`");
    expect(data.json.cross_locus_ranked_genes).toMatchObject([{ ensembl_id: "ENSG1", max_score: 0.45 }, { ensembl_id: "ENSG2", max_score: 0.1 }]);
    expect(data.markdown_summary).toContain("`GENE` (ENSG1)");
    expect(data.markdown_summary).toContain("`GENE` (ENSG2)");
    expect(data.mermaid_figures[0].mermaid).toContain("GENE (ENSG1)");
    expect(data.mermaid_figures[0].mermaid).toContain("GENE (ENSG2)");
    expect(data.mermaid_figures[0].mermaid).toContain('G0C0["1.00"]');
    expect(data.mermaid_figures[0].mermaid).toContain('G1C0["0.20"]');
    expect(data.mermaid_figures[2].mermaid).toContain("GENE (ENSG2)");
  }
});

it("stages large synthesis output with source references and the original evidence handle", async () => {
  const stored: unknown[] = [];
  const namespace = { idFromName: (id: string) => id, get: () => ({ fetch: async (request: Request) => {
    if (new URL(request.url).pathname === "/process") {
      stored.push((await request.json() as { data: unknown }).data);
      return Response.json({ success: true, tables_created: ["result"], total_rows: 1 });
    }
    return Response.json({ success: true, schema: { tables: {} } });
  } }) } as unknown as DurableObjectNamespace;
  const warning = "α".repeat(22000);
  for (const tool of Object.values(registry("2026-10-06-r8-finemap", false, { namespace, warnings: warning }))) {
    const result = await tool.handler({ data_access_id: "source-evidence" }, { sessionId: "fixture" });
    expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true, staged: true, _staging: expect.any(Object) } });
    expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(30000);
    const payloads = stored.filter((value): value is string => typeof value === "string");
    const original = JSON.parse(payloads[payloads.length - 1]);
    expect(original.structuredContent.data.data_access_id).toBe("source-evidence");
    expect(original.structuredContent.data.json.warnings).toEqual([warning]);
    expect(JSON.parse(original.structuredContent.data.json.loci[0].candidate_genes[0].evidence.eqtl_support_json)).toEqual([{ source_data_access_id: "source-evidence", source_table: "gtex_evidence", response_sha256: "a".repeat(64), source_row_index: 0, dataset_id_requested: "gtex_v10", tissue: "Liver", normalized_effect_size: -0.4, p_value: 0 }]);
    expect(original.structuredContent.data.markdown_summary).toContain(warning);
    expect(JSON.parse(original.structuredContent.data.json.loci[0].candidate_genes[0].evidence.l2g_support_json)).toMatchObject({ study_locus_id: "SL1", variant_id: "1_10_A_G", requested_trait_id: "EFO_1", trait_match: "exact_mapped_trait_id" });
  }
});

it.each(["failure", "cap", "count_cap"])("does not turn an incomplete required SQL read into a successful summary (%s)", async mode => {
  const tools = registry("2026-10-06-r8-finemap");
  const original = vi.mocked(queryDataFromDo).getMockImplementation()!;
  vi.mocked(queryDataFromDo).mockImplementation(async (...args) => {
    if (mode === "failure") throw new Error("fixture SQL unavailable");
    return { ...await original(...args), ...(mode === "cap" ? { total_matching: 20000 } : { count_capped: true }) };
  });
  for (const tool of Object.values(tools)) {
    expect(await tool.handler({ data_access_id: "fixture" }, {})).toMatchObject({ isError: true, content: expect.any(Array), structuredContent: { success: false, error: { code: "SQL_EXECUTION_ERROR" } } });
  }
});

it("includes optional exact-gene constraint context without changing the score", async () => {
  const native = { data: { gene: { gene_id: "ENSG1", reference_genome: "GRCh38", flags: ["low_exome_coverage"], gnomad_constraint: { pli: 0, oe_lof_upper: 0.9 } } } };
  const gnomad = { usable: 1, error: null, requested_ensembl_id: "ENSG1", reference_genome_requested: "GRCh38", source_url: "https://gnomad.broadinstitute.org/api", query: GNOMAD_GENE_QUERY, variables_json: '{"id":"ENSG1","ref":"GRCh38"}', retrieved_at: "2026-10-06T00:00:00Z", native_response_json: JSON.stringify(native), response_sha256: await hashCanonicalJson(native) };
  for (const tool of Object.values(registry("2026-10-06-r8-finemap", true, { gnomad }))) {
    expect(await tool.handler({ data_access_id: "fixture" }, {})).toMatchObject({ structuredContent: { success: true, data: { json: { loci: [{ candidate_genes: [
      { ensembl_id: "ENSG1", overall_score: 0.45, evidence: { gnomad_context: { context_only: true, pli: 0, oe_lof_upper: 0.9, source_data_access_id: "fixture", source_table: "gnomad" } } },
      { ensembl_id: "ENSG2", overall_score: 0.1, evidence: { gnomad_context: null } },
    ] }] } } } });
  }
});
