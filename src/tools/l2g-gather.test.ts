import { afterEach, expect, it, vi } from "vitest";
import { McpServer } from "@bio-mcp/shared/mcp";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { registerL2gGather } from "./l2g-gather";
import { nativeBurden } from "../../test/burden-fixture";
import { stageGatherResult } from "../lib/gather-result";
import { withDeadline } from "../lib/with-deadline";
import { ols4Search } from "@bio-mcp/shared/adapters/efo_ols4";
import { fetchGtexEqtls } from "../lib/gtex-evidence";
import { fetchDatasets } from "@bio-mcp/shared/adapters/eqtl/ftp-catalogue";
import { queryRegion } from "@bio-mcp/shared/adapters/eqtl/sumstats";
import { eqtlInput, eqtlNative } from "../../test/eqtl-catalogue-fixture";

vi.mock("@bio-mcp/shared/adapters/eqtl/ftp-catalogue", () => ({ fetchDatasets: vi.fn() }));
vi.mock("@bio-mcp/shared/adapters/eqtl/sumstats", () => ({ queryRegion: vi.fn() }));

it("returns complete input errors through both aliases before any upstream work", async () => {
  const server = new McpServer({ name: "fixture", version: "1" });
  registerL2gGather(server, {});
  const tools = (server as any)._registeredTools;
  for (const name of ["l2g_gather", "mcp_l2g_gather"]) {
    expect(await tools[name].handler({ genebass_burden_sets: ["a", "b", "c", "d", "e"] }, {})).toMatchObject({
      isError: true, content: expect.any(Array), structuredContent: { success: false, _meta: { citation: expect.any(Object) } },
    });
  }
  expect(ols4Search).not.toHaveBeenCalled();
});

vi.mock("@bio-mcp/shared/adapters/efo_ols4", () => ({ ols4Search: vi.fn() }));
vi.mock("../lib/coordinates", () => ({ resolveAnchor: async () => ({ grch38: { chr: "1", pos: 10, ref: "A", alt: "G" }, grch37: null, warnings: [], observations: [{ status: "resolved", native_response_json: "native mapping evidence" }] }) }));
vi.mock("../lib/gtex-evidence", () => ({ fetchGtexEqtls: vi.fn(async () => ({ rows: [], evidence: [] })) }));
vi.mock("@bio-mcp/shared/adapters/gwas_catalog", () => ({ gwasAssociations: async () => ({ _embedded: { associations: [] } }) }));
vi.mock("@bio-mcp/shared/adapters/genebass", () => ({ genebassGeneBurden: async () => ({ associations: [] }) }));
vi.mock("@bio-mcp/shared/adapters/gnomad", () => ({ gnomadGraphql: async (_query: string, variables: { id: string }) => ({ data: { gene: { gene_id: variables.id, symbol: "GENE", reference_genome: "GRCh38", flags: ["low_exome_coverage"], canonical_transcript_id: "ENST1", gnomad_constraint: { pli: 0, oe_lof_upper: 0.9, flags: ["outlier_syn"] } } } }) }));
vi.mock("@bio-mcp/shared/adapters/opentargets", () => ({ opentargetsGraphql: async (query: string) => {
  if (query.includes("query L2GBurden")) return nativeBurden();
  const meta = { dataVersion: { year: "2026", month: "09", iteration: "0" } };
  if (query.includes("query L2GSearch")) return { data: { meta, search: { total: 1, hits: [{ object: { id: "S1", hasSumstats: true } }] } } };
  if (query.includes("query L2GStudy")) return { data: { meta, study: { credibleSets: { count: 1, rows: [{ studyLocusId: "L1", variant: { id: "1_10_A_G", rsIds: ["rs1"] } }] } } } };
  return { data: { meta, credibleSets: { count: 1, rows: [{ studyLocusId: "L1", studyId: "S1", studyType: "gwas", variant: { id: "1_10_A_G" }, study: { id: "S1", studyType: "gwas", traitFromSourceMappedIds: ["EFO_1"], qualityControls: ["α".repeat(20000)] }, l2GPredictions: { count: 1, rows: [{ score: 0.8, target: { id: "ENSG1", approvedSymbol: "GENE" } }] }, colocalisation: { count: 1, rows: [{ colocalisationMethod: "COLOC_PIP", h4: 0.9, rightStudyType: "eqtl", otherStudyLocus: { studyLocusId: "QL1", studyId: "Q1", studyType: "eqtl", qtlGeneId: "ENSG1", study: { id: "Q1", studyType: "eqtl", target: { id: "ENSG1" } } } }] } }] } } };
} }));
afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers(); vi.mocked(ols4Search).mockReset(); vi.mocked(fetchGtexEqtls).mockReset(); vi.mocked(fetchGtexEqtls).mockResolvedValue({ rows: [], evidence: [] }); });

function storage(stored: unknown[], fail = false) {
  return { idFromName: (name: string) => name, get: () => ({ fetch: async (request: Request) => {
    if (new URL(request.url).pathname === "/process") {
      stored.push((await request.json() as { data: unknown }).data);
      return Response.json({ success: !fail, tables_created: ["anchors", "l2g_predictions", "opentargets_evidence", "l2g_meta"], total_rows: 6 }, { status: fail ? 500 : 200 });
    }
    return Response.json({ success: true, schema: { tables: {} } });
  } }) } as unknown as DurableObjectNamespace;
}

it("stages one explicit FinnGen phenotype definition alongside variants through both aliases", async () => {
  const stored: unknown[] = []; const server = new McpServer({name: "fixture", version: "1"});
  registerL2gGather(server, {L2G_DATA_DO: storage(stored)});
  const definition = {phenocode: "T2D", phenostring: "Synthetic phenotype", extension: [null, "native"]};
  const fetcher = vi.fn(async (url: string) => Response.json(url.includes("/pheno/") ? definition : {
    variant: {chr: 1, pos: 10, ref: "A", alt: "G"}, results: [{phenocode: "T2D", phenostring: definition.phenostring, pval: 0.01, beta: -1}],
  }));
  vi.stubGlobal("fetch", fetcher);
  const tools = (server as any)._registeredTools;
  for (const prefix of ["", "mcp_"]) {
    const result = await tools[`${prefix}l2g_gather`].handler({seed_rsids: ["rs1"], efo_id: "EFO_1", finngen_release: "R13", finngen_phenocode: "T2D", genebass_burden_sets: []}, {});
    expect(result).toMatchObject({structuredContent: {success: true}, content: expect.any(Array)});
    const payloads = stored.filter((value): value is string => typeof value === "string");
    const payload = JSON.parse(payloads[payloads.length - 1]);
    expect(payload.finngen_phenotype_evidence).toHaveLength(1);
    expect(JSON.parse(payload.finngen_phenotype_evidence[0].native_response_json)).toEqual(definition);
    expect(JSON.parse(payload.l2g_meta[0].caps_json)).toMatchObject({finngen_release: "R13", finngen_phenocode: "T2D", finngen_join_version: "2026-10-06-native-phenocode"});
    expect(payload.l2g_meta[0].sources_queried).toContain("finngen");
    expect(await tools[`${prefix}l2g_gather`].handler({seed_rsids: ["rs1"], finngen_phenocode: "T2D"}, {})).toMatchObject({isError: true, structuredContent: {success: false}});
  }
  expect(fetcher).toHaveBeenCalledTimes(4);
});

it("stages the explicit Catalogue lane through both aliases without changing ranking inputs", async () => {
  const stored: unknown[] = []; const server = new McpServer({ name: "fixture", version: "1" });
  registerL2gGather(server, { L2G_DATA_DO: storage(stored) });
  const native = eqtlNative(); native.dataset.study_label = "α".repeat(20000);
  vi.mocked(fetchDatasets).mockResolvedValue([native.dataset]); vi.mocked(queryRegion).mockResolvedValue(native.region);
  const latest = () => { const payloads = stored.filter((value): value is string => typeof value === "string"); return JSON.parse(payloads[payloads.length - 1]); };
  for (const tool of Object.values((server as any)._registeredTools) as any[]) {
    await tool.handler({ seed_rsids: ["rs1"], efo_id: "EFO_1" }, {}); const baseline = latest();
    const result = await tool.handler({ seed_rsids: ["rs1"], efo_id: "EFO_1", eqtl_catalogue: eqtlInput }, {});
    expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true, _staging: expect.any(Object) } });
    expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(30000);
    const payload = latest();
    expect(JSON.parse(payload.eqtl_catalogue_evidence[0].native_response_json)).toEqual(native);
    expect(JSON.parse(payload.l2g_meta[0].caps_json)).toMatchObject({ eqtl_catalogue: eqtlInput });
    expect(payload.l2g_meta[0].sources_queried).toContain("eqtl_catalogue");
    for (const table of ["candidate_genes", "candidate_selection", "l2g_predictions", "eqtl", "coloc", "burden"]) expect(payload[table]).toEqual(baseline[table]);
    expect(result.structuredContent._meta.citation.result_hash).toBe(await hashCanonicalJson(payload));
    expect(await tool.handler({ seed_rsids: ["rs1"], eqtl_catalogue: { ...eqtlInput, release: "r8_beta" } }, {})).toMatchObject({ isError: true, content: expect.any(Array), structuredContent: { success: false } });
  }
});

it("stages ClinVar native observations and context policy through both gather aliases", async () => {
  const stored: unknown[] = []; const server = new McpServer({ name: "fixture", version: "1" });
  registerL2gGather(server, { L2G_DATA_DO: storage(stored) });
  const tools = (server as any)._registeredTools;
  const native = { result: { uids: ["1"], "1": { uid: "1", germline_classification: { description: "Uncertain significance" }, extension: "α".repeat(20000) } } };
  const fetcher = vi.fn(async (url: string) => Response.json(url.includes("esearch") ? { esearchresult: { count: "1", idlist: ["1"] } } : native));
  vi.stubGlobal("fetch", fetcher);
  for (const name of ["l2g_gather", "mcp_l2g_gather"]) {
    const result = await tools[name].handler({ seed_rsids: ["rs1"], efo_id: "EFO_1", include_clinvar: true }, {});
    expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true, _staging: expect.any(Object) } });
    const payloads = stored.filter((value): value is string => typeof value === "string");
    const payload = JSON.parse(payloads[payloads.length - 1]);
    expect(payload.clinvar).toMatchObject([{ rsid: "rs1", variation_id: "1", clinical_significance: "Uncertain significance", exact_allele_match: false }]);
    expect(JSON.parse(payload.clinvar_evidence[1].native_response_json)).toEqual(native);
    expect(JSON.parse(payload.l2g_meta[0].caps_json)).toMatchObject({ include_clinvar: true, clinvar_context_version: 1 });
    expect(payload.l2g_meta[0].sources_queried).toContain("clinvar");
    expect(result.structuredContent._meta.citation.result_hash).toBe(await hashCanonicalJson(payload));
  }
  expect(fetcher).toHaveBeenCalledTimes(4);
});

it("stages HPA observation policy and explicit skipped identities through both gather aliases", async () => {
  const stored: unknown[] = []; const server = new McpServer({ name: "fixture", version: "1" });
  registerL2gGather(server, { L2G_DATA_DO: storage(stored) });
  const tools = (server as any)._registeredTools;
  const fetcher = vi.fn(); vi.stubGlobal("fetch", fetcher);
  for (const name of ["l2g_gather", "mcp_l2g_gather"]) {
    const result = await tools[name].handler({ seed_rsids: ["rs1"], efo_id: "EFO_1", include_hpa_tissue_context: true }, {});
    expect(result).toMatchObject({ structuredContent: { success: true, _staging: expect.any(Object) } });
    const payloads = stored.filter((value): value is string => typeof value === "string");
    const payload = JSON.parse(payloads[payloads.length - 1]);
    // The shared fixture's ENSG1 is deliberately not a stable human gene identifier.
    expect(payload.hpa).toMatchObject([{ requested_ensembl_id: "ENSG1", source_url: null, status: "skipped", identity_verified: false }]);
    expect(JSON.parse(payload.l2g_meta[0].caps_json)).toMatchObject({ include_hpa_tissue_context: true, hpa_context_version: 1 });
    expect(payload.l2g_meta[0].sources_queried).not.toContain("hpa");
  }
  expect(fetcher).not.toHaveBeenCalled();
});

it("stages optional FinnGen evidence through both gather aliases and leaves default gathers without network calls", async () => {
  const stored: unknown[] = []; const server = new McpServer({ name: "fixture", version: "1" });
  registerL2gGather(server, { L2G_DATA_DO: storage(stored) });
  const tools = (server as any)._registeredTools;
  const native = { results: [{ phenocode: "TEST", beta: -1, pval: 0 }], extension: "α".repeat(20000) };
  const fetcher = vi.fn(async () => Response.json(native)); vi.stubGlobal("fetch", fetcher);
  for (const name of ["l2g_gather", "mcp_l2g_gather"]) {
    for (const release of [undefined, "R12", "R13"]) {
      fetcher.mockClear();
      const result = await tools[name].handler({ seed_rsids: ["rs1"], efo_id: "EFO_1", ...(release ? { finngen_release: release } : {}) }, {});
      expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true, _staging: expect.any(Object) } });
      expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(30 * 1024);
      const payloads = stored.filter((value): value is string => typeof value === "string");
      const payload = JSON.parse(payloads[payloads.length - 1]);
      expect(JSON.parse(payload.l2g_meta[0].caps_json).finngen_release).toBe(release ?? null);
      expect(fetcher).toHaveBeenCalledTimes(release ? 1 : 0);
      if (release) {
        expect(JSON.parse(payload.finngen_evidence[0].native_response_json)).toEqual(native);
        expect(payload.finngen_evidence[0].release_requested).toBe(release);
        expect(payload.l2g_meta[0].sources_queried).toContain("finngen");
      } else expect(payload.finngen_evidence).toEqual([]);
      expect(result.structuredContent._meta.citation.result_hash).toBe(await hashCanonicalJson(payload));
    }
    fetcher.mockClear(); const before = stored.length;
    expect(await tools[name].handler({ seed_rsids: ["rs1"], finngen_release: "latest" }, {})).toMatchObject({ content: expect.any(Array), isError: true, structuredContent: { success: false } });
    expect(fetcher).not.toHaveBeenCalled(); expect(stored.length).toBe(before);
  }
});

it("stages native Open Targets evidence, ranking rows and metadata together through both gather aliases", async () => {
  const stored: unknown[] = []; const server = new McpServer({ name: "fixture", version: "1" });
  registerL2gGather(server, { L2G_DATA_DO: storage(stored) });
  const tools = (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
  const args = { seed_rsids: ["rs1"], efo_id: "EFO_1", disable_default_seeds: true };
  expect(Object.keys(tools).sort()).toEqual(["l2g_gather", "mcp_l2g_gather"]);
  for (const tool of Object.values(tools)) {
    const result = await tool.handler(args, { sessionId: "fixture" });
    expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true, _staging: expect.any(Object), data: { candidate_genes_count: 1 }, _meta: { citation: { result_scope: "staged:full_result", result_hash: expect.stringMatching(/^[a-f0-9]{64}$/) } } } });
    expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(30 * 1024);
    const payloads = stored.filter((value): value is string => typeof value === "string");
    const payload = JSON.parse(payloads[payloads.length - 1]);
    expect(payload).toMatchObject({ l2g_predictions: [{ symbol: "GENE", model_version: null, variant_id: "1_10_A_G", genome_assembly: "GRCh38", anchor_match: "exact_resolved_variant_id", requested_trait_id: "EFO_1", trait_match: "exact_mapped_trait_id" }], opentargets_evidence: [{ phase: "study search" }, { phase: "study credible sets" }, { phase: "credible-set detail" }, { phase: "gene burden" }], l2g_meta: [{ efo_id: "EFO_1", gather_policy_version: "2026-10-01-gene-coloc" }] });
    expect(JSON.stringify(payload)).toContain("α".repeat(20000));
    expect(payload.coloc[0]).toMatchObject({ ensembl_id: "ENSG1", requested_trait_id: "EFO_1", trait_match: "exact_mapped_trait_id" });
    expect(JSON.parse(payload.coloc[0].native_coloc_json).otherStudyLocus.studyLocusId).toBe("QL1");
    expect(payload.coordinate_resolution).toEqual([{ status: "resolved", native_response_json: "native mapping evidence" }]);
    const citation = (result as { structuredContent: { _meta: { citation: unknown } } }).structuredContent._meta.citation;
    expect(citation).toMatchObject({ query_hash: await hashCanonicalJson(args), result_hash: await hashCanonicalJson(payload) });
  }
});

it("stages the selected GTEx dataset and complete native page evidence through both aliases", async () => {
  const stored: unknown[] = []; const server = new McpServer({ name: "fixture", version: "1" });
  registerL2gGather(server, { L2G_DATA_DO: storage(stored) });
  const tools = (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
  const native = { data: [], paging_info: { totalNumberOfItems: 0 }, extension: "α".repeat(20000) };
  for (const dataset of ["gtex_v8", "gtex_v10"] as const) {
    const evidence = { source_url: "https://gtexportal.org/api/v2/association/singleTissueEqtl", dataset_id_requested: dataset,
      request_json: JSON.stringify({ datasetId: dataset }), response_json: JSON.stringify(native),
      response_sha256: await hashCanonicalJson(native), native_text: JSON.stringify(native), http_status: 200,
      retrieved_at: "2026-10-01T00:00:00Z", usable: true, error: null, coverage_complete: false as const, release_pinned: false as const };
    vi.mocked(fetchGtexEqtls).mockResolvedValue({ rows: [], evidence: [evidence] });
    for (const tool of Object.values(tools)) {
      const result = await tool.handler({ seed_rsids: ["rs1"], efo_id: "EFO_1", ...(dataset === "gtex_v10" ? { gtex_dataset_id: dataset } : {}) }, {});
      expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true, _staging: expect.any(Object) } });
      expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(30 * 1024);
      const payloads = stored.filter((value): value is string => typeof value === "string");
      const payload = JSON.parse(payloads[payloads.length - 1]);
      expect(payload.gtex_evidence).toEqual([evidence]);
      expect(JSON.parse(payload.l2g_meta[0].caps_json).gtex_dataset_id).toBe(dataset);
      expect(fetchGtexEqtls).toHaveBeenLastCalledWith(expect.any(Array), 200, expect.any(Array), dataset);
      expect(result).toMatchObject({ structuredContent: { _meta: { citation: { result_hash: await hashCanonicalJson(payload) } } } });
    }
  }
});

it("rejects unsupported GTEx datasets through both aliases before querying or staging", async () => {
  const stored: unknown[] = []; const server = new McpServer({ name: "fixture", version: "1" });
  registerL2gGather(server, { L2G_DATA_DO: storage(stored) });
  const tools = (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
  for (const tool of Object.values(tools)) {
    expect(await tool.handler({ seed_rsids: ["rs1"], gtex_dataset_id: "latest" }, {})).toMatchObject({ content: expect.any(Array), isError: true, structuredContent: { success: false } });
  }
  expect(fetchGtexEqtls).not.toHaveBeenCalled();
  expect(stored).toEqual([]);
});

it("returns both fields and isError if complete gather persistence fails", async () => {
  const server = new McpServer({ name: "fixture", version: "1" }); registerL2gGather(server, { L2G_DATA_DO: storage([], true) });
  const tools = (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
  for (const tool of Object.values(tools)) expect(await tool.handler({ seed_rsids: ["rs1"], efo_id: "EFO_1" }, {})).toMatchObject({ isError: true, content: expect.any(Array), structuredContent: { success: false } });
});

it("stages exact and ambiguous OLS lookup evidence through both aliases, only ranking selected traits", async () => {
  const stored: unknown[] = []; const server = new McpServer({ name: "fixture", version: "1" });
  registerL2gGather(server, { L2G_DATA_DO: storage(stored) });
  const tools = (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
  for (const ambiguous of [false, true]) {
    const docs = (ambiguous ? ["EFO:1", "EFO:2"] : ["EFO:1"]).map(obo_id => ({ obo_id, label: "asthma", type: "class", ontology_name: "efo" }));
    const native = { response: { numFound: docs.length, start: 0, docs }, extra: "α".repeat(20000) };
    vi.mocked(ols4Search).mockResolvedValue(native);
    for (const tool of Object.values(tools)) {
      const result = await tool.handler({ trait_query: "asthma", seed_rsids: ["rs1"], disable_default_seeds: true }, {});
      expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { _staging: expect.any(Object), data: { trait: { efo_id: ambiguous ? null : "EFO_1", resolution_status: ambiguous ? "ambiguous" : "exact_label" }, candidate_genes_count: ambiguous ? 0 : 1 } } });
      expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(30 * 1024);
      const payloads = stored.filter((value): value is string => typeof value === "string"); const payload = JSON.parse(payloads[payloads.length - 1]);
      expect(JSON.parse(payload.trait_resolution[0].native_response_json)).toEqual(native);
      expect(payload.l2g_meta[0].efo_id).toBe(ambiguous ? null : "EFO_1");
      expect(payload.l2g_predictions[0].trait_match).toBe(ambiguous ? "unresolved" : "exact_mapped_trait_id");
    }
  }
});

it("rejects malformed explicit trait IDs with the normal error envelope before lookup", async () => {
  const server = new McpServer({ name: "fixture", version: "1" }); registerL2gGather(server, { L2G_DATA_DO: storage([]) });
  const tools = (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
  for (const tool of Object.values(tools)) expect(await tool.handler({ seed_rsids: ["rs1"], efo_id: "asthma" }, {})).toMatchObject({ isError: true, content: expect.any(Array), structuredContent: { success: false } });
  expect(ols4Search).not.toHaveBeenCalled();
});

it("preserves unresolved lookup evidence even when no anchors remain, staging large errors", async () => {
  const stored: unknown[] = []; const server = new McpServer({ name: "fixture", version: "1" });
  registerL2gGather(server, { L2G_DATA_DO: storage(stored) });
  const tools = (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
  for (const large of [false, true]) {
    const native = { response: { numFound: 0, docs: [] }, extra: large ? "α".repeat(20000) : "small" };
    vi.mocked(ols4Search).mockResolvedValue(native);
    for (const tool of Object.values(tools)) {
      const result = await tool.handler({ trait_query: "unknown trait", disable_default_seeds: true }, {});
      expect(result).toMatchObject({ isError: true, content: expect.any(Array), structuredContent: { success: false } });
      expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(30 * 1024);
      let complete = result;
      if (large) {
        expect(result).toMatchObject({ structuredContent: { _staging: expect.any(Object) } });
        const payloads = stored.filter((value): value is string => typeof value === "string"); complete = JSON.parse(payloads[payloads.length - 1]);
      }
      expect(complete).toMatchObject({ structuredContent: { error: { code: "NOT_FOUND", details: { trait_resolution: { status: "unmatched", native_response_json: JSON.stringify(native) } } } } });
    }
  }
});

it("stages optional gnomAD native context and provenance through both gather aliases", async () => {
  const stored: unknown[] = []; const server = new McpServer({ name: "fixture", version: "1" });
  registerL2gGather(server, { L2G_DATA_DO: storage(stored) });
  const tools = (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
  for (const tool of Object.values(tools)) {
    expect(await tool.handler({ seed_rsids: ["rs1"], efo_id: "EFO_1", include_gnomad_context: true }, {})).toMatchObject({ structuredContent: { success: true, _staging: expect.any(Object) } });
    const payloads = stored.filter((value): value is string => typeof value === "string");
    const payload = JSON.parse(payloads[payloads.length - 1]);
    expect(payload.gnomad).toMatchObject([{ requested_ensembl_id: "ENSG1", usable: true, pli: 0, oe_lof_upper: 0.9, canonical_transcript_id: "ENST1" }]);
    expect(JSON.parse(payload.gnomad[0].native_response_json).data.gene.flags).toEqual(["low_exome_coverage"]);
    expect(JSON.parse(payload.gnomad[0].constraint_provenance_json)).toMatchObject({ runtime_metric_release: null, release_pinned: false });
    expect(JSON.parse(payload.l2g_meta[0].caps_json).include_gnomad_context).toBe(true);
  }
});

it("preserves large warning sets in the staged metadata instead of overflowing transport", async () => {
  const stored: unknown[] = []; const warnings = ["α".repeat(20000)];
  const result = await stageGatherResult({ tables: { anchors: [], loci: [], candidate_genes: [] }, meta: { trait_query: "α".repeat(20000), efo_id: null, efo_label: null, generated_at: new Date().toISOString(), sources_queried: [], caps: {}, target_gene: null }, warnings, args: {}, elapsedMs: 1, namespace: storage(stored), extra: {} });
  expect(result).toMatchObject({ structuredContent: { data: { warnings_staged: true, warning_count: 1 }, _staging: expect.any(Object) } });
  expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(30 * 1024);
  const payload = JSON.parse(stored.find((value): value is string => typeof value === "string")!);
  expect(JSON.parse(payload.l2g_meta[0].warnings_json)).toEqual(warnings);
  expect(payload.l2g_meta[0].trait_query).toBe("α".repeat(20000));
  expect(result.structuredContent?.data.trait.display_truncated).toBe(true);
});

it("clears completed deadlines and reports only actual failures/timeouts", async () => {
  vi.useFakeTimers(); const warnings: string[] = [];
  expect(await withDeadline(Promise.resolve(1), 10, "done", warnings)).toBe(1);
  await vi.advanceTimersByTimeAsync(20); expect(warnings).toEqual([]);
  expect(await withDeadline(Promise.reject(new Error("failed")), 10, "bad", warnings)).toBeNull();
  const pending = withDeadline(new Promise(() => {}), 10, "slow", warnings);
  await vi.advanceTimersByTimeAsync(20); expect(await pending).toBeNull();
  expect(warnings).toEqual(["bad failed: failed", "slow exceeded 10ms budget — skipped"]);
});
