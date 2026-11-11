import { afterEach, expect, it, vi } from "vitest";
import { McpServer } from "@bio-mcp/shared/mcp";
import { registerL2gGather } from "./l2g-gather";

vi.mock("../lib/coordinates", () => ({ resolveAnchor: async () => ({ grch38: null, grch37: null, warnings: [], observations: [] }) }));
vi.mock("../lib/gtex-evidence", () => ({ fetchGtexEqtls: async () => ({ rows: [], evidence: [] }) }));
afterEach(() => vi.unstubAllGlobals());

it.each(["content", "hal"].flatMap(format => [true, false, undefined].map(descendants => ({ format, descendants }))))("filters and projects $format GWAS anchors through both aliases ($descendants)", async ({ format, descendants }) => {
  const stored: unknown[] = [];
  const fetcher = vi.fn(async (input: RequestInfo | URL) => {
    const url = new URL(String(input));
    expect(url.pathname).toBe("/gwas/rest/api/v2/associations");
    expect(Object.fromEntries(url.searchParams)).toEqual({ efo_id: "EFO_0001360", show_child_trait: String(descendants ?? true), size: "4", page: "0" });
    const associations = [
      { snp_allele: [{ rs_id: "rs123" }], p_value: 0.01 },
      { snp_allele: [{ rs_id: "rs123" }], p_value: 1e-9, efo_traits: [{ efo_trait: "type 2 diabetes mellitus" }] },
      { snp_allele: [{ rs_id: "rs456" }, { rs_id: "rs789" }], p_value: 1e-20, multi_snp_haplotype: true },
      { rs_id: "prefix-rs999", p_value: 1e-30 },
    ];
    return Response.json(format === "content" ? { content: associations } : { _embedded: { associations } });
  });
  vi.stubGlobal("fetch", fetcher);
  const namespace = { idFromName: (id: string) => id, get: () => ({ fetch: async (request: Request) => {
    if (new URL(request.url).pathname === "/process") {
      stored.push((await request.json() as { data: unknown }).data);
      return Response.json({ success: true, tables_created: ["anchors", "l2g_meta"], total_rows: 2 });
    }
    return Response.json({ success: true, schema: { tables: {} } });
  } }) } as unknown as DurableObjectNamespace;
  const server = new McpServer({ name: "fixture", version: "1" });
  registerL2gGather(server, { L2G_DATA_DO: namespace });
  const tools = (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
  for (const name of ["l2g_gather", "mcp_l2g_gather"]) {
    const result = await tools[name].handler({ efo_id: "EFO_0001360", trait_query: "ignored label", show_child_traits: descendants, max_anchor_associations: 4, disable_default_seeds: true, genebass_burden_sets: [] }, {});
    expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true, _staging: expect.any(Object) } });
    const payloads = stored.filter((value): value is string => typeof value === "string");
    const payload = JSON.parse(payloads[payloads.length - 1]);
    expect(payload.anchors).toMatchObject([{ rsid: "rs123", source: "gwas", p_value: 1e-9 }]);
    expect(payload.anchors).toHaveLength(1);
    expect(JSON.parse(payload.l2g_meta[0].warnings_json).join(" ")).toContain("skipped 2");
    expect(JSON.parse(payload.l2g_meta[0].caps_json).show_child_traits).toBe(descendants ?? true);
    expect(payload.l2g_meta[0].sources_queried).toContain("gwas_catalog");
  }
  expect(fetcher).toHaveBeenCalledTimes(2);
});
