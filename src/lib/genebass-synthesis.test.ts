import { hashCanonicalJson } from "@bio-mcp/shared";
import { afterEach, expect, it, vi } from "vitest";
import { readCompleteTable } from "./complete-table";
import { genebassSourceUrl } from "./genebass-observation";
import { loadGenebassContext } from "./genebass-synthesis";
vi.mock("./complete-table", () => ({ readCompleteTable: vi.fn() }));
afterEach(() => vi.resetAllMocks());
const ns = {} as DurableObjectNamespace;
const caps = '{"genebass_context_version":1}';
const gene = "ENSG00000134057";
const native = { associations: [{ skat_o_pvalue: 0.01, phenotype_description: "A" }, { skat_o_pvalue: 1e-9, phenotype_description: "B" }] };
async function row() { return { requested_ensembl_id: gene, burden_set: "pLoF", source_url: genebassSourceUrl(gene, "pLoF", 1), http_status: 200,
  native_response_json: JSON.stringify(native), response_sha256: await hashCanonicalJson(native), status: "observed" }; }

it("keeps legacy and uncollected gathers readable without a new table", async () => {
  await loadGenebassContext(ns, "fixture"); await loadGenebassContext(ns, "fixture", caps, false);
  expect(readCompleteTable).not.toHaveBeenCalled();
});

it("rederives the minimum and exact native row reference without promoting identity or trait matching", async () => {
  vi.mocked(readCompleteTable).mockResolvedValue([{ ...await row(), summary: { pValue: 0 }, context_only: false, identity_verified: true, trait_matched: true }]);
  const context = (await loadGenebassContext(ns, "fixture", caps)).get(gene)!;
  expect(context).toMatchObject([{ summary: { pValue: 1e-9, phenotype: "B", associationIndex: 1 }, context_only: true,
    identity_verified: false, trait_matched: false, contract_verified: false, release_pinned: false,
    source_data_access_id: "fixture", source_table: "genebass_evidence" }]);
  expect(context[0]).not.toHaveProperty("native_response_json");
});

it.each([{ response_sha256: "bad" }, { native_response_json: null }, { source_url: "https://example.org" },
  { http_status: 503 }, { requested_ensembl_id: "different" }, { burden_set: "different" }, { status: "invalid" }])("rejects inconsistent staged evidence: %j", async overrides => {
  vi.mocked(readCompleteTable).mockResolvedValue([{ ...await row(), ...overrides }]);
  await expect(loadGenebassContext(ns, "fixture", caps)).rejects.toThrow();
});

it("preserves a failed source read without treating it as a negative association", async () => {
  vi.mocked(readCompleteTable).mockResolvedValue([{ ...await row(), status: "error", error: "partial", http_status: 503 }]);
  expect((await loadGenebassContext(ns, "fixture", caps)).get(gene)).toMatchObject([{ status: "error", summary: null, error: "partial" }]);
});

it("rejects unsupported versions, over-budget observations and incomplete reads", async () => {
  await expect(loadGenebassContext(ns, "fixture", '{"genebass_context_version":99}')).rejects.toThrow("version");
  vi.mocked(readCompleteTable).mockResolvedValue(Array(101).fill(await row()));
  await expect(loadGenebassContext(ns, "fixture", caps)).rejects.toThrow("100-request");
  vi.mocked(readCompleteTable).mockRejectedValue(new Error("Incomplete table"));
  await expect(loadGenebassContext(ns, "fixture", caps)).rejects.toThrow("Incomplete");
});
