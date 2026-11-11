import { afterEach, expect, it, vi } from "vitest";
vi.mock("cloudflare:workers", () => ({ RpcTarget: class {}, WorkerEntrypoint: class {}, DurableObject: class { constructor(public ctx: unknown, public env: unknown) {} } }));
import { McpServer } from "@bio-mcp/shared/mcp";
import { storage } from "../../test/l2g-storage";
import { fetchGenebassContext } from "./genebass-context";
import { stageGatherResult } from "./gather-result";
import { readCompleteTable } from "./complete-table";
import { registerL2gScore } from "../tools/l2g-score";
import { registerL2gSynthesize } from "../tools/l2g-synthesize";
afterEach(() => vi.unstubAllGlobals());

it.each([false, true])("recovers complete Genebass evidence through staged SQL and both synthesis aliases without affecting scores (native PheWAS: %s)", async current => {
  const store = storage();
  try {
    const gene = "ENSG00000134057";
    const summary = current ? { pValue: 1e-12, phenotype: null, associationIndex: 1, phenotypeId: "continuous-2-both_sexes--", associationArray: "phewas" }
      : { pValue: 1e-12, phenotype: "B", associationIndex: 1 };
    const native = { ...(current ? { phewas: [
      { trait_type: "continuous", phenocode: "1", pheno_sex: "both_sexes", coding: "", modifier: "", Pvalue: 0.01 },
      { trait_type: "continuous", phenocode: "2", pheno_sex: "both_sexes", coding: "", modifier: "", Pvalue: 1e-12 },
    ] } : { associations: [
      { phenotype_description: "A", skat_o_pvalue: 0.01, beta: "0.1234567890123456", qc: ["native"] },
      { phenotype_description: "B", skat_o_pvalue: 1e-12 },
    ] }), extension: 'α😀"'.repeat(current ? 160000 : 20000) };
    const failed = { error: "fixture failure", details: "retained" };
    const fetcher = vi.fn(async (url: RequestInfo | URL) => new URL(String(url)).searchParams.get("burdenSet") === "pLoF"
      ? Response.json(native) : Response.json(failed, { status: 503 }));
    vi.stubGlobal("fetch", fetcher);
    const warnings: string[] = [];
    const result = await fetchGenebassContext(new Map([["CCNB1", gene]]), ["pLoF", "missense|LC"], warnings);
    const server = new McpServer({ name: "fixture", version: "1" });
    registerL2gScore(server, { L2G_DATA_DO: store.namespace }); registerL2gSynthesize(server, { L2G_DATA_DO: store.namespace });
    const tools = (server as any)._registeredTools;
    const scores: number[] = [];
    for (const enabled of [false, true]) {
      const gathered = await stageGatherResult({ tables: {
        anchors: [{ rsid: "rs1", locus_id: "L1", p_value: 1e-10, lead_trait: null, grch38: { chr: "1", pos: 10, ref: "A", alt: "G" } }], loci: [{ locus_id: "L1" }],
        candidate_genes: [{ locus_id: "L1", lead_rsid: "rs1", symbol: "CCNB1", ensembl_id: gene }],
        burden: enabled ? result.rows : [], genebass_evidence: enabled ? result.evidence : [],
      }, meta: { trait_query: "fixture", efo_id: null, efo_label: null, generated_at: "2026-10-01T00:00:00Z", sources_queried: enabled ? ["genebass"] : [], caps: { genebass_context_version: 2 }, target_gene: null }, warnings, args: {}, elapsedMs: 1, namespace: store.namespace, extra: {} });
      const id = (gathered.structuredContent as any).data.data_access_id;
      for (const prefix of ["", "mcp_"]) {
        expect(await tools[`${prefix}l2g_score`].handler({ data_access_id: id }, {})).toMatchObject({ structuredContent: { success: true } });
        const synthesized = await tools[`${prefix}l2g_synthesize`].handler({ data_access_id: id }, {});
        expect(synthesized, JSON.stringify(synthesized)).toMatchObject({ content: expect.any(Array), structuredContent: { success: true } });
        const candidate = synthesized.structuredContent.data.json.loci[0].candidate_genes[0];
        scores.push(candidate.overall_score);
        if (enabled) {
          expect(candidate.evidence.genebass_context).toEqual(expect.arrayContaining([
            expect.objectContaining({ summary, source_data_access_id: id,
              source_table: "genebass_evidence", context_only: true, identity_verified: false, trait_matched: false }),
            expect.objectContaining({ status: "error", summary: null, http_status: 503 }),
          ]));
          const recovered = await readCompleteTable<{ burden_set: string; native_response_json: string }>(store.namespace, id, "genebass_evidence");
          expect(JSON.parse(recovered.find(row => row.burden_set === "pLoF")!.native_response_json)).toEqual(native);
          expect(JSON.parse(recovered.find(row => row.burden_set === "missense|LC")!.native_response_json)).toEqual(failed);
        } else expect(candidate.evidence.genebass_context).toEqual([]);
        expect(JSON.stringify(synthesized)).not.toContain(native.extension);
        expect(new TextEncoder().encode(JSON.stringify(synthesized)).byteLength).toBeLessThan(100_000);
      }
    }
    expect(scores).toEqual([0, 0, 0, 0]); expect(fetcher).toHaveBeenCalledTimes(2);
  } finally { store.close(); }
});
