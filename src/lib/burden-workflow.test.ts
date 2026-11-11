import { afterEach, expect, it, vi } from "vitest";
vi.mock("cloudflare:workers", () => ({ RpcTarget: class {}, WorkerEntrypoint: class {}, DurableObject: class { constructor(public ctx: unknown, public env: unknown) {} } }));
import { McpServer } from "@bio-mcp/shared/mcp";
import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";
import { storage } from "../../test/l2g-storage";
import { nativeBurden } from "../../test/burden-fixture";
import { fetchBurdenEvidence } from "./burden-evidence";
import { stageGatherResult } from "./gather-result";
import { registerL2gScore } from "../tools/l2g-score";
import { registerL2gSynthesize } from "../tools/l2g-synthesize";
import { readCompleteTable } from "./complete-table";

afterEach(() => vi.unstubAllGlobals());

it.each([1, 100])("retains %s native burden rows through real staging, both scoring aliases and synthesis", async count => {
  const store = storage();
  try {
    const native = nativeBurden(count);
    const fetcher = vi.fn(async () => Response.json(native)); vi.stubGlobal("fetch", fetcher);
    const candidate = { locus_id: "L1", lead_rsid: "rs1", symbol: "GENE", ensembl_id: "ENSG1" };
    const evidence = await fetchBurdenEvidence([candidate], "EFO_1", []);
    expect(JSON.parse((fetcher.mock.calls[0] as any)[1].body).variables).toEqual({ genes: ["ENSG1"], trait: "EFO_1" });
    const gathered = await stageGatherResult({ tables: {
      anchors: [{ rsid: "rs1", locus_id: "L1", lead_trait: "fixture", p_value: 1e-10 }], loci: [{ locus_id: "L1" }],
      candidate_genes: [candidate], opentargets_evidence: evidence,
    }, meta: { trait_query: "fixture", efo_id: "EFO_1", efo_label: "fixture", generated_at: "2026-10-01T00:00:00Z", sources_queried: ["opentargets"], caps: {}, target_gene: null }, warnings: [], args: {}, elapsedMs: 1, namespace: store.namespace, extra: {} });
    const id = (gathered.structuredContent as any).data.data_access_id;
    const server = new McpServer({ name: "fixture", version: "1" });
    registerL2gScore(server, { L2G_DATA_DO: store.namespace }); registerL2gSynthesize(server, { L2G_DATA_DO: store.namespace });
    const tools = (server as any)._registeredTools;
    for (const prefix of ["", "mcp_"]) {
      expect(await tools[`${prefix}l2g_score`].handler({ data_access_id: id }, {})).toMatchObject({ content: expect.any(Array), structuredContent: { success: true } });
      const scored = await readCompleteTable<any>(store.namespace, id, "scored_candidate_genes");
      expect(scored).toHaveLength(1);
      expect(scored[0]).toMatchObject({ burden_best_p: 2e-8, burden_component: 1, overall_score: 0.1, evidence_policy_version: "2026-10-06-r8-finemap" });
      const refs = JSON.parse(scored[0].burden_support_json); expect(refs).toHaveLength(count);
      expect(refs[0]).toMatchObject({ source_data_access_id: id, native_evidence: native.data.disease.evidences.rows[0] });
      let synthesized = await tools[`${prefix}l2g_synthesize`].handler({ data_access_id: id }, {});
      expect(synthesized).toMatchObject({ content: expect.any(Array), structuredContent: { success: true } });
      if (count > 1) {
        expect(synthesized.structuredContent._staging).toBeDefined();
        expect(new TextEncoder().encode(JSON.stringify(synthesized)).byteLength).toBeLessThan(30 * 1024);
        const outputId = synthesized.structuredContent.data_access_id;
        const saved = (await queryDataFromDo(store.namespace, outputId, "SELECT root_json FROM payloads")).rows[0] as any;
        const chunkId = saved.root_json.replace("__CHUNKED__:", "");
        expect(chunkId).toMatch(/^chunk_[a-f0-9]+$/);
        const metadata = (await queryDataFromDo(store.namespace, outputId, `SELECT total_chunks FROM chunk_metadata WHERE content_id = '${chunkId}'`)).rows[0] as any;
        const chunks: string[] = [];
        for (let index = 0; index < metadata.total_chunks; index++) {
          const chunk = (await queryDataFromDo(store.namespace, outputId, `SELECT chunk_data FROM content_chunks WHERE content_id = '${chunkId}' AND chunk_index = ${index}`)).rows[0] as any;
          chunks.push(chunk.chunk_data);
        }
        synthesized = JSON.parse(JSON.parse(chunks.join("")));
      }
      expect(JSON.parse(synthesized.structuredContent.data.json.loci[0].candidate_genes[0].evidence.burden_support_json)).toEqual(refs);
    }
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(JSON.parse((await readCompleteTable<any>(store.namespace, id, "opentargets_evidence"))[0].response_json)).toEqual(native);
  } finally { store.close(); }
});

it("skips unresolved traits and empty candidate sets; limits the query to 25 stable gene IDs", async () => {
  const fetcher = vi.fn(async () => Response.json(nativeBurden())); vi.stubGlobal("fetch", fetcher);
  const candidates = Array.from({ length: 30 }, (_, index) => ({ ensembl_id: `ENSG${index}` }));
  expect(await fetchBurdenEvidence(candidates, null, [])).toEqual([]);
  expect(await fetchBurdenEvidence([], "EFO_1", [])).toEqual([]);
  expect(fetcher).not.toHaveBeenCalled();
  const warnings: string[] = []; await fetchBurdenEvidence(candidates, "EFO_1", warnings);
  expect(JSON.parse((fetcher.mock.calls[0] as any)[1].body).variables.genes).toHaveLength(25);
  expect(warnings).toContainEqual(expect.stringContaining("25 selected"));
});
