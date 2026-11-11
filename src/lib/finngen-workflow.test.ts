import { afterEach, expect, it, vi } from "vitest";
vi.mock("cloudflare:workers", () => ({ RpcTarget: class {}, WorkerEntrypoint: class {}, DurableObject: class { constructor(public ctx: unknown, public env: unknown) {} } }));
import { McpServer } from "@bio-mcp/shared/mcp";
import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";
import { storage } from "../../test/l2g-storage";
import { fetchFinnGenContext } from "./finngen-context";
import { readCompleteTable } from "./complete-table";
import { stageGatherResult } from "./gather-result";
import { registerL2gScore } from "../tools/l2g-score";
import { registerL2gSynthesize } from "../tools/l2g-synthesize";
afterEach(() => vi.unstubAllGlobals());

it("retains native FinnGen data and synthesis references through real staging without adding a score contribution", async () => {
  const store = storage();
  try {
    const native = { variant: { chrom: 1, pos: 10, ref: "A", alt: "G" }, results: [{ phenocode: "UNMATCHED_TRAIT", pval: "1e-500", beta: "-0.020" }], extension: '😀"\\'.repeat(70000) };
    const fetcher = vi.fn(async () => Response.json(native)); vi.stubGlobal("fetch", fetcher);
    const anchors = [{ rsid: "rs1", locus_id: "L1", p_value: 1e-10, lead_trait: null, grch38: { chr: "1", pos: 10, ref: "A", alt: "G" } }];
    const warnings: string[] = []; const evidence = await fetchFinnGenContext(anchors, "R13", warnings);
    const server = new McpServer({ name: "fixture", version: "1" });
    registerL2gScore(server, { L2G_DATA_DO: store.namespace }); registerL2gSynthesize(server, { L2G_DATA_DO: store.namespace });
    const tools = (server as any)._registeredTools;
    const scores: number[] = [];
    for (const enabled of [false, true]) {
      const result = await stageGatherResult({ tables: { anchors, loci: [{ locus_id: "L1" }], candidate_genes: [{ locus_id: "L1", lead_rsid: "rs1", symbol: "GENE", ensembl_id: "ENSG1" }], finngen_evidence: enabled ? evidence : [] },
        meta: { trait_query: "fixture", efo_id: null, efo_label: null, generated_at: "2026-10-01T00:00:00Z", sources_queried: enabled ? ["finngen"] : [], caps: enabled ? { finngen_release: "R13" } : {}, target_gene: null }, warnings: enabled ? warnings : [], args: {}, elapsedMs: 1, namespace: store.namespace, extra: {} });
      expect(result.structuredContent).toHaveProperty("_staging");
      expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(30 * 1024);
      const id = (result.structuredContent as any).data.data_access_id;
      for (const prefix of ["", "mcp_"]) {
        expect(await tools[`${prefix}l2g_score`].handler({ data_access_id: id }, {})).toMatchObject({ structuredContent: { success: true } });
        const scored = (await queryDataFromDo(store.namespace, id, "SELECT overall_score FROM scored_candidate_genes")).rows[0] as any;
        scores.push(scored.overall_score);
        const synthesized = await tools[`${prefix}l2g_synthesize`].handler({ data_access_id: id }, {});
        expect(synthesized).toMatchObject({ content: expect.any(Array), structuredContent: { success: true } });
        const refs = synthesized.structuredContent.data.json.anchors[0].finngen_context;
        if (enabled) {
          expect(refs).toMatchObject([{ source_data_access_id: id, source_table: "finngen_evidence", release_requested: "R13", requested_variant: "1:10-A-G", response_sha256: evidence[0].response_sha256, source_hash_verified: true, request_context_verified: true, association_count: 1, context_only: true, identity_verified: true, coverage_complete: false }]);
          expect(refs[0]).not.toHaveProperty("native_response_json");
          await expect(queryDataFromDo(store.namespace, id, "SELECT native_response_json FROM finngen_evidence")).rejects.toThrow("LOSSLESS_QUERY_RESULT_TOO_LARGE");
          const recovered = await readCompleteTable<{native_response_json: string}>(store.namespace, id, "finngen_evidence");
          expect(JSON.parse(recovered[0].native_response_json)).toEqual(native);
        } else expect(refs).toEqual([]);
      }
    }
    expect(scores).toEqual([0, 0, 0, 0]); expect(fetcher).toHaveBeenCalledTimes(1);
  } finally { store.close(); }
});

it.each(["native payload", "anchor allele"])("returns complete MCP errors through both synthesis aliases when %s changes", async change => {
  const store = storage();
  try {
    vi.stubGlobal("fetch", async () => Response.json({ results: [{ phenocode: "FIXTURE", pval: "1e-500" }] }));
    const anchor = { rsid: "rs1", locus_id: "L1", p_value: 1e-10, lead_trait: null, grch38: { chr: "1", pos: 10, ref: "A", alt: "G" } };
    const evidence = await fetchFinnGenContext([anchor], "R13", []);
    if (change === "native payload") evidence[0].native_response_json = '{"results":[]}';
    else anchor.grch38.alt = "T";
    const staged = await stageGatherResult({ tables: { anchors: [anchor], loci: [{ locus_id: "L1" }],
      candidate_genes: [{ locus_id: "L1", lead_rsid: "rs1", symbol: "GENE", ensembl_id: "ENSG1" }], finngen_evidence: evidence },
      meta: { trait_query: "fixture", efo_id: null, efo_label: null, generated_at: "2026-10-02T00:00:00Z",
        sources_queried: ["finngen"], caps: { finngen_release: "R13" }, target_gene: null },
      warnings: [], args: {}, elapsedMs: 1, namespace: store.namespace, extra: {} });
    const server = new McpServer({ name: "fixture", version: "1" });
    registerL2gScore(server, { L2G_DATA_DO: store.namespace }); registerL2gSynthesize(server, { L2G_DATA_DO: store.namespace });
    const tools = (server as any)._registeredTools;
    const input = { data_access_id: (staged.structuredContent as any).data.data_access_id };
    expect(await tools.l2g_score.handler(input, {})).toMatchObject({ structuredContent: { success: true } });
    for (const prefix of ["", "mcp_"]) {
      const result = await tools[`${prefix}l2g_synthesize`].handler(input, {});
      expect(result).toMatchObject({ content: expect.any(Array), isError: true, structuredContent: { success: false } });
      expect(JSON.stringify(result)).toContain(change === "native payload" ? "hash mismatch" : "does not match its anchor");
      expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(30 * 1024);
    }
  } finally { store.close(); }
});
