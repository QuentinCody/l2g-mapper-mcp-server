import { afterEach, expect, it, vi } from "vitest";
vi.mock("cloudflare:workers", () => ({ RpcTarget: class {}, WorkerEntrypoint: class {}, DurableObject: class { constructor(public ctx: unknown, public env: unknown) {} } }));
import { McpServer } from "@bio-mcp/shared/mcp";
import { storage } from "../../test/l2g-storage";
import { eqtlAnchor, eqtlFineNative, eqtlInput, eqtlObservation } from "../../test/eqtl-catalogue-fixture";
import { EQTL_CATALOGUE_CONTEXT_VERSION } from "./eqtl-catalogue-input";
import { stageGatherResult } from "./gather-result";
import { readCompleteTable } from "./complete-table";
import { registerL2gScore } from "../tools/l2g-score";
import { registerL2gSynthesize } from "../tools/l2g-synthesize";
afterEach(() => vi.unstubAllGlobals());

it("scores qualified Catalogue evidence once and preserves complete source references through both aliases", async () => {
  const store = storage();
  try {
    const server = new McpServer({ name: "fixture", version: "1" });
    registerL2gScore(server, { L2G_DATA_DO: store.namespace }); registerL2gSynthesize(server, { L2G_DATA_DO: store.namespace });
    const tools = (server as any)._registeredTools;
    for (const mode of ["nominal", "qualified", "low-pip", "fine-map-error"]) {
      const native = eqtlFineNative();
      if (mode === "nominal") delete native.fine_mapping;
      if (mode === "low-pip") {
        const table = native.fine_mapping!.result!; table.rows[0].pip = 0.1;
        table.raw_rows = table.rows.map(row => table.columns.map(c => row[c]).join("\t"));
      }
      if (mode === "fine-map-error") Object.assign(native.fine_mapping!, { status: "error", error: "HTTP 503", result: null });
      const evidence = await eqtlObservation(native);
      const input = { ...eqtlInput, ...(mode === "nominal" ? {} : { fine_mapping_sha256: "d".repeat(64) }) };
      const gathered = await stageGatherResult({ tables: {
        anchors: [{ ...eqtlAnchor, p_value: 1e-10, lead_trait: null }], loci: [{ locus_id: "L1" }],
        candidate_genes: [{ locus_id: "L1", lead_rsid: "rs1", symbol: "CCNB1", ensembl_id: "ENSG00000134057" }], eqtl_catalogue_evidence: [evidence],
      }, meta: { trait_query: "fixture", efo_id: null, efo_label: null, generated_at: "2026-10-06T00:00:00Z", sources_queried: ["eqtl_catalogue"], caps: { eqtl_catalogue: input, eqtl_catalogue_context_version: EQTL_CATALOGUE_CONTEXT_VERSION }, target_gene: null }, warnings: [], args: {}, elapsedMs: 1, namespace: store.namespace, extra: {} });
      const id = (gathered.structuredContent as any).data.data_access_id;
      for (const prefix of ["", "mcp_"]) {
        const scored = await tools[`${prefix}l2g_score`].handler({ data_access_id: id }, {});
        expect(scored, JSON.stringify(scored)).toMatchObject({ structuredContent: { success: true } });
        const rows = await readCompleteTable<any>(store.namespace, id, "scored_candidate_genes");
        expect(rows[0].eqtl_tissue_hits).toBe(mode === "qualified" ? 1 : 0);
        expect(rows[0].overall_score).toBeCloseTo(mode === "qualified" ? 0.05 : 0, 12);
        const result = await tools[`${prefix}l2g_synthesize`].handler({ data_access_id: id }, {});
        expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true } });
        const refs = JSON.parse(result.structuredContent.data.json.loci[0].candidate_genes[0].evidence.eqtl_support_json);
        if (mode === "qualified") expect(refs).toMatchObject([{ source_data_access_id: id, source_table: "eqtl_catalogue_evidence", response_sha256: evidence.response_sha256, adds_tissue: true, native_fine_mapping_row: { pip: 0.99 } }]);
        else expect(refs).toEqual([]);
      }
      const recovered = await readCompleteTable<any>(store.namespace, id, "eqtl_catalogue_evidence");
      expect(JSON.parse(recovered[0].native_response_json)).toEqual(native);
    }
  } finally { store.close(); }
});
