import { afterEach, expect, it, vi } from "vitest";
vi.mock("cloudflare:workers", () => ({ RpcTarget: class {}, WorkerEntrypoint: class {}, DurableObject: class { constructor(public ctx: unknown, public env: unknown) {} } }));
import { McpServer } from "@bio-mcp/shared/mcp";
import { storage } from "../../test/l2g-storage";
import { fetchClinvarLane } from "./clinvar-context";
import { stageGatherResult } from "./gather-result";
import { readCompleteTable } from "./complete-table";
import { registerL2gScore } from "../tools/l2g-score";
import { registerL2gSynthesize } from "../tools/l2g-synthesize";
afterEach(() => vi.unstubAllGlobals());

it("retains native ClinVar summaries through real SQL and both synthesis aliases without changing scores", async () => {
  const store = storage();
  try {
    const native = { result: { uids: ["1"], "1": { uid: "1", germline_classification: { description: "Pathogenic", trait_set: [{ trait_name: "other disease" }] }, extension: 'α😀"'.repeat(20000) } } };
    vi.stubGlobal("fetch", vi.fn(async (url: string) => Response.json(url.includes("esearch") ? { esearchresult: { count: "1", idlist: ["1"] } } : native)));
    const warnings: string[] = []; const lane = await fetchClinvarLane(["rs1"], warnings);
    const server = new McpServer({ name: "fixture", version: "1" });
    registerL2gScore(server, { L2G_DATA_DO: store.namespace }); registerL2gSynthesize(server, { L2G_DATA_DO: store.namespace });
    const tools = (server as any)._registeredTools; const scores: number[] = [];
    for (const enabled of [false, true]) {
      const gathered = await stageGatherResult({ tables: {
        anchors: [{ rsid: "rs1", locus_id: "L1", p_value: 1e-10, lead_trait: null, grch38: { chr: "1", pos: 10, ref: "A", alt: "G" } }], loci: [{ locus_id: "L1" }],
        candidate_genes: [{ locus_id: "L1", lead_rsid: "rs1", symbol: "GENE", ensembl_id: "ENSG1" }],
        clinvar: enabled ? lane.rows : [], clinvar_evidence: enabled ? lane.evidence : [],
      }, meta: { trait_query: "fixture", efo_id: null, efo_label: null, generated_at: "2026-10-01T00:00:00Z", sources_queried: enabled ? ["clinvar"] : [],
        caps: { include_clinvar: enabled, clinvar_context_version: 1 }, target_gene: null }, warnings, args: {}, elapsedMs: 1, namespace: store.namespace, extra: {} });
      const id = (gathered.structuredContent as any).data.data_access_id;
      for (const prefix of ["", "mcp_"]) {
        expect(await tools[`${prefix}l2g_score`].handler({ data_access_id: id }, {})).toMatchObject({ structuredContent: { success: true } });
        const result = await tools[`${prefix}l2g_synthesize`].handler({ data_access_id: id }, {});
        expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true } });
        const candidate = result.structuredContent.data.json.loci[0].candidate_genes[0];
        scores.push(candidate.overall_score); expect(candidate.evidence.clinvar_support).toBe("none");
        const context = result.structuredContent.data.json.anchors[0].clinvar_context;
        if (enabled) {
          expect(context).toMatchObject([{ source_data_access_id: id, source_table: "clinvar_evidence", records: [{ clinical_significance: "Pathogenic", exact_allele_match: false, trait_match: false }] }]);
          const recovered = await readCompleteTable<{ phase: string; native_response_json: string }>(store.namespace, id, "clinvar_evidence");
          expect(JSON.parse(recovered.find(row => row.phase === "summary")!.native_response_json)).toEqual(native);
        } else expect(context).toEqual([]);
        expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(100_000);
      }
    }
    expect(scores).toEqual([0, 0, 0, 0]);
  } finally { store.close(); }
});
