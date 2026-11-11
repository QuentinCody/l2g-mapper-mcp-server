import { afterEach, expect, it, vi } from "vitest";
vi.mock("cloudflare:workers", () => ({ RpcTarget: class {}, WorkerEntrypoint: class {}, DurableObject: class { constructor(public ctx: unknown, public env: unknown) {} } }));
import { McpServer } from "@bio-mcp/shared/mcp";
import { storage } from "../../test/l2g-storage";
import { fetchHpaLane } from "./hpa-context";
import { stageGatherResult } from "./gather-result";
import { readCompleteTable } from "./complete-table";
import { registerL2gScore } from "../tools/l2g-score";
import { registerL2gSynthesize } from "../tools/l2g-synthesize";
afterEach(() => vi.unstubAllGlobals());

it("retains complete HPA observations through real staged SQL and both synthesis aliases without changing scores", async () => {
  const store = storage();
  try {
    const gene = "ENSG00000134057";
    const native = { Ensembl: gene, Gene: "CCNB1", "RNA tissue specific nTPM": { "bone marrow": "49.9", "lymphoid tissue": "70.9" }, "Subcellular location": ["Cytosol"], extension: 'α😀"'.repeat(20000) };
    const fetcher = vi.fn(async () => Response.json(native)); vi.stubGlobal("fetch", fetcher);
    const warnings: string[] = []; const evidence = await fetchHpaLane(new Map([["CCNB1", gene]]), warnings);
    const server = new McpServer({ name: "fixture", version: "1" });
    registerL2gScore(server, { L2G_DATA_DO: store.namespace }); registerL2gSynthesize(server, { L2G_DATA_DO: store.namespace });
    const tools = (server as any)._registeredTools;
    const scores: number[] = [];
    for (const enabled of [false, true]) {
      const gathered = await stageGatherResult({ tables: {
        anchors: [{ rsid: "rs1", locus_id: "L1", p_value: 1e-10, lead_trait: null, grch38: { chr: "1", pos: 10, ref: "A", alt: "G" } }], loci: [{ locus_id: "L1" }],
        candidate_genes: [{ locus_id: "L1", lead_rsid: "rs1", symbol: "CCNB1", ensembl_id: gene }], hpa: enabled ? evidence : [],
      }, meta: { trait_query: "fixture", efo_id: null, efo_label: null, generated_at: "2026-10-01T00:00:00Z", sources_queried: enabled ? ["hpa"] : [], caps: { include_hpa_tissue_context: enabled, hpa_context_version: 1 }, target_gene: null }, warnings, args: {}, elapsedMs: 1, namespace: store.namespace, extra: {} });
      expect(gathered.structuredContent).toHaveProperty("_staging");
      const id = (gathered.structuredContent as any).data.data_access_id;
      for (const prefix of ["", "mcp_"]) {
        expect(await tools[`${prefix}l2g_score`].handler({ data_access_id: id }, {})).toMatchObject({ structuredContent: { success: true } });
        const result = await tools[`${prefix}l2g_synthesize`].handler({ data_access_id: id }, {});
        expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true } });
        const candidate = result.structuredContent.data.json.loci[0].candidate_genes[0];
        scores.push(candidate.overall_score);
        if (enabled) {
          expect(candidate.evidence.hpa_context).toMatchObject([{ source_data_access_id: id, source_table: "hpa", response_sha256: evidence[0].response_sha256, tissues: "lymphoid tissue | bone marrow", context_only: true, identity_verified: true, release_pinned: false }]);
          expect(candidate.evidence.hpa_context[0]).not.toHaveProperty("native_response_json");
          const recovered = await readCompleteTable<{ native_response_json: string }>(store.namespace, id, "hpa");
          expect(JSON.parse(recovered[0].native_response_json)).toEqual(native);
        } else expect(candidate.evidence.hpa_context).toEqual([]);
        expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(100_000);
      }
    }
    expect(scores).toEqual([0, 0, 0, 0]); expect(fetcher).toHaveBeenCalledTimes(1);
  } finally { store.close(); }
});
