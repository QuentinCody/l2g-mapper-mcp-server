import { afterEach, expect, it, vi } from "vitest";
vi.mock("cloudflare:workers", () => ({ RpcTarget: class {}, WorkerEntrypoint: class {}, DurableObject: class { constructor(public ctx: unknown, public env: unknown) {} } }));
import { McpServer } from "@bio-mcp/shared/mcp";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";
import { storage } from "../../test/l2g-storage";
import { fetchGtexEqtls } from "./gtex-evidence";
import { stageGatherResult } from "./gather-result";
import { registerL2gScore } from "../tools/l2g-score";
import { registerL2gSynthesize } from "../tools/l2g-synthesize";

afterEach(() => vi.unstubAllGlobals());


it("keeps ordinary staging append semantics and rejects empty derived replacements", async () => {
  const store = storage();
  try {
    const stub = store.namespace.get(store.namespace.idFromName("ordinary"));
    for (const value of ["first", "second"]) {
      expect((await stub.fetch(new Request("https://fixture/process", { method: "POST", body: JSON.stringify({ data: { observations: [{ value }] } }) }))).status).toBe(200);
    }
    expect((await queryDataFromDo(store.namespace, "ordinary", "SELECT value FROM observations ORDER BY _rowid")).rows).toEqual([{ value: "first" }, { value: "second" }]);
    const failed = await stub.fetch(new Request("https://fixture/process", { method: "POST", body: JSON.stringify({ data: { scored_candidate_genes: [], cross_locus_ranked_genes: [] } }) }));
    expect(failed.ok).toBe(false);
    const renamed = await stub.fetch(new Request("https://fixture/process", { method: "POST", body: JSON.stringify({ data: { scored_candidate_genes: [{ value: "score" }], cross_locus_ranked_genes: [{ value: "rank" }] }, schema_hints: { tableName: "observations" } }) }));
    expect(renamed.ok).toBe(false);
    expect((await queryDataFromDo(store.namespace, "ordinary", "SELECT value FROM observations ORDER BY _rowid")).rows).toHaveLength(2);
  } finally { store.close(); }
});

it.each([1, 120])("retains %s native associations through real staging, scoring and synthesis without inflating tissue counts", async count => {
  const store = storage();
  try {
    const native = { data: Array.from({ length: count }, (_, i) => ({ variantId: "chr1_10_A_G_b38", geneSymbol: "GENE", gencodeId: "ENSG1.7", tissueSiteDetailId: "Liver", pValue: i / 100000, nes: -0.4, extra: "α".repeat(100) })), paging_info: { page: 0, totalNumberOfItems: count + 10 } };
    const fetcher = vi.fn(async () => Response.json(native)); vi.stubGlobal("fetch", fetcher);
    const anchors = [{ rsid: "rs1", locus_id: "L1", p_value: 1e-10, lead_trait: null, grch38: { chr: "1", pos: 10, ref: "A", alt: "G" } }];
    const gtex = await fetchGtexEqtls(anchors, 200, [], "gtex_v10");
    const gathered = await stageGatherResult({ tables: { anchors, loci: [{ locus_id: "L1" }], candidate_genes: [{ locus_id: "L1", lead_rsid: "rs1", symbol: "GENE", ensembl_id: "ENSG1" }], eqtl: gtex.rows, gtex_evidence: gtex.evidence },
      meta: { trait_query: "fixture", efo_id: null, efo_label: null, generated_at: "2026-10-01T00:00:00Z", sources_queried: ["gtex"], caps: { gtex_dataset_id: "gtex_v10" }, target_gene: null }, warnings: [], args: {}, elapsedMs: 1, namespace: store.namespace, extra: {} });
    const id = (gathered.structuredContent as any).data.data_access_id as string;
    const server = new McpServer({ name: "fixture", version: "1" });
    registerL2gScore(server, { L2G_DATA_DO: store.namespace }); registerL2gSynthesize(server, { L2G_DATA_DO: store.namespace });
    const tools = (server as any)._registeredTools;
    for (const prefix of ["", "mcp_"]) {
      expect(await tools[`${prefix}l2g_score`].handler({ data_access_id: id }, {})).toMatchObject({ structuredContent: { success: true }, content: expect.any(Array) });
      const scored = (await queryDataFromDo(store.namespace, id, "SELECT * FROM scored_candidate_genes")).rows[0] as any;
      expect(scored).toMatchObject({ eqtl_tissue_hits: 1, eqtl_tissues: "Liver" });
      expect(scored.overall_score).toBeCloseTo(0.05, 12);
      const references = JSON.parse(scored.eqtl_support_json);
      expect(references).toHaveLength(count);
      expect(references.map((ref: any) => ref.source_row_index).sort((a: number, b: number) => a - b)).toEqual(Array.from({ length: count }, (_, i) => i));
      for (const ref of references) expect(ref).toMatchObject({ source_data_access_id: id, response_sha256: await hashCanonicalJson(native), dataset_id_requested: "gtex_v10", normalized_effect_size: -0.4 });
      let result = await tools[`${prefix}l2g_synthesize`].handler({ data_access_id: id }, {});
      expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true } });
      if (count > 1) {
        expect(result.structuredContent._staging).toBeDefined();
        expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(30 * 1024);
        const resultId = result.structuredContent.data_access_id;
        const saved = (await queryDataFromDo(store.namespace, resultId, "SELECT root_json FROM payloads")).rows[0] as any;
        const contentId = saved.root_json.replace("__CHUNKED__:", "");
        expect(contentId).toMatch(/^chunk_[a-f0-9]+$/);
        const metadata = (await queryDataFromDo(store.namespace, resultId, `SELECT total_chunks FROM chunk_metadata WHERE content_id = '${contentId}'`)).rows[0] as any;
        const chunks: string[] = [];
        for (let index = 0; index < metadata.total_chunks; index++) {
          const chunk = (await queryDataFromDo(store.namespace, resultId, `SELECT chunk_data FROM content_chunks WHERE content_id = '${contentId}' AND chunk_index = ${index}`)).rows[0] as any;
          chunks.push(chunk.chunk_data);
        }
        result = JSON.parse(JSON.parse(chunks.join("")));
      }
      expect(JSON.parse(result.structuredContent.data.json.loci[0].candidate_genes[0].evidence.eqtl_support_json)).toEqual(references);
    }
    const saved = (await queryDataFromDo(store.namespace, id, "SELECT response_json FROM gtex_evidence")).rows[0] as any;
    expect(JSON.parse(saved.response_json)).toEqual(native);
    store.failScoreInsert();
    expect(await tools.l2g_score.handler({ data_access_id: id }, {})).toMatchObject({ content: expect.any(Array), isError: true, structuredContent: { success: false, error: { code: "STAGING_ERROR" } } });
    const lastGood = await queryDataFromDo(store.namespace, id, "SELECT eqtl_tissue_hits, eqtl_support_json FROM scored_candidate_genes");
    expect(lastGood.rows).toHaveLength(1);
    expect(JSON.parse((lastGood.rows[0] as any).eqtl_support_json)).toHaveLength(count);
    expect((await queryDataFromDo(store.namespace, id, "SELECT * FROM cross_locus_ranked_genes")).rows).toHaveLength(1);
    expect(fetcher).toHaveBeenCalledTimes(1);
  } finally { store.close(); }
});
