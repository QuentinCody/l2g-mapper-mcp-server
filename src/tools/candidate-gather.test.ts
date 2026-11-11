import { expect, it, vi } from "vitest";
import { McpServer } from "@bio-mcp/shared/mcp";
import { registerL2gGather } from "./l2g-gather";

vi.mock("../lib/coordinates", () => ({ resolveAnchor: async (rsid: string) => ({ grch38: { chr: "1", pos: 10, ref: "A", alt: rsid === "rs1" ? "G" : "T" }, grch37: null, warnings: [], observations: [{ status: "resolved", native_response_json: "native mapping evidence" }] }) }));
vi.mock("../lib/gtex-evidence", () => ({ fetchGtexEqtls: async () => ({ rows: [], evidence: [] }) }));
vi.mock("@bio-mcp/shared/adapters/gwas_catalog", () => ({ gwasAssociations: async () => ({ _embedded: { associations: [] } }) }));
vi.mock("@bio-mcp/shared/adapters/genebass", () => ({ genebassGeneBurden: async () => ({ associations: [] }) }));
vi.mock("@bio-mcp/shared/adapters/opentargets", () => ({ opentargetsGraphql: async (query: string) => {
  const meta = { dataVersion: { year: "2026", month: "09" } };
  const sets = ["G", "T"].map((alt, i) => ({ studyLocusId: `SL${i + 1}`, studyId: "S1", studyType: "gwas", study: { id: "S1", studyType: "gwas", traitFromSourceMappedIds: ["EFO_1"] }, variant: { id: `1_10_A_${alt}`, rsIds: [`rs${i + 1}`] } }));
  const pred = (id: string, score: number) => ({ target: { id, approvedSymbol: "SHARED" }, score });
  if (query.includes("query L2GSearch")) return { data: { meta, search: { hits: [{ object: { id: "S1" } }] } } };
  if (query.includes("query L2GStudy")) return { data: { meta, study: { credibleSets: { rows: sets } } } };
  return { data: { meta, credibleSets: { rows: sets.map((row, i) => ({ ...row, l2GPredictions: { rows: i === 0 ? [pred("ENSG1", 0.2), pred("ENSG3", 0.1)] : [pred("ENSG2", 0.9), pred("ENSG1", 0.8)] } })) } } };
} }));

it.each([1, 2])("stages strongest stable genes and their actual anchors with a %s-gene cap", async cap => {
  const stored: unknown[] = [];
  const namespace = { idFromName: (id: string) => id, get: () => ({ fetch: async (request: Request) => {
    if (new URL(request.url).pathname === "/process") {
      stored.push((await request.json() as { data: unknown }).data);
      return Response.json({ success: true, tables_created: ["candidate_genes", "candidate_selection"], total_rows: 4 });
    }
    return Response.json({ success: true, schema: { tables: {} } });
  } }) } as unknown as DurableObjectNamespace;
  const server = new McpServer({ name: "fixture", version: "1" }); registerL2gGather(server, { L2G_DATA_DO: namespace });
  const tools = (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
  expect(Object.keys(tools).sort()).toEqual(["l2g_gather", "mcp_l2g_gather"]);
  for (const tool of Object.values(tools)) {
    expect(await tool.handler({ seed_rsids: ["rs1", "rs2"], efo_id: "EFO_1", max_genes_per_locus: cap, disable_default_seeds: true }, {})).toMatchObject({ structuredContent: { success: true, _staging: expect.any(Object) } });
    const payloads = stored.filter((value): value is string => typeof value === "string");
    const payload = JSON.parse(payloads[payloads.length - 1]);
    const pairs = payload.candidate_genes.map((row: { ensembl_id: string; lead_rsid: string }) => [row.ensembl_id, row.lead_rsid]);
    expect(pairs).toEqual(cap === 1 ? [["ENSG2", "rs2"]] : [["ENSG2", "rs2"], ["ENSG1", "rs1"], ["ENSG1", "rs2"]]);
    expect(payload.candidate_selection).toMatchObject([{ eligible_genes: 3, selected_genes: cap, omitted_genes: 3 - cap }]);
    expect(payload.l2g_predictions).toHaveLength(4);
    expect(JSON.parse(payload.l2g_meta[0].warnings_json).join(" ")).toContain(`retained ${cap} of 3`);
  }
});
