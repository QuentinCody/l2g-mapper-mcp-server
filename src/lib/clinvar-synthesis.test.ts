import { hashCanonicalJson } from "@bio-mcp/shared";
import { afterEach, expect, it, vi } from "vitest";
import { readCompleteTable } from "./complete-table";
import { loadClinvarContext } from "./clinvar-synthesis";
import { clinvarUrl, type ClinvarObservation } from "./clinvar-observation";
vi.mock("./complete-table", () => ({ readCompleteTable: vi.fn() }));
afterEach(() => vi.resetAllMocks());
const ns = {} as DurableObjectNamespace;
const caps = JSON.stringify({ include_clinvar: true, clinvar_context_version: 1 });
async function row(phase: "search" | "summary", native: unknown): Promise<ClinvarObservation> {
  const rsid = phase === "search" ? "rs1" : null; const ids = phase === "search" ? [] : ["1"];
  return { phase, rsid, requested_ids_json: JSON.stringify(ids), native_response_json: JSON.stringify(native), response_sha256: await hashCanonicalJson(native),
    status: "observed", http_status: 200, source_url: clinvarUrl(phase, rsid, ids), error: null, retrieved_at: "2026-10-01T00:00:00Z",
    context_only: true, coverage_complete: false, release_pinned: false };
}
const search = () => row("search", { esearchresult: { count: "1", idlist: ["1"] } });
const summary = () => row("summary", { result: { uids: ["1"], "1": { uid: "1", germline_classification: { description: "Benign" } } } });

it("does not query legacy or disabled gathers", async () => {
  for (const value of [undefined, '{}', '{"include_clinvar":true}']) expect((await loadClinvarContext(ns, "fixture", value)).size).toBe(0);
  expect((await loadClinvarContext(ns, "fixture", caps, false)).size).toBe(0);
  expect(readCompleteTable).not.toHaveBeenCalled();
});
it("rebuilds context from native observations with recoverable references", async () => {
  vi.mocked(readCompleteTable).mockResolvedValue([await search(), await summary()]);
  const context = (await loadClinvarContext(ns, "fixture", caps)).get("rs1")!;
  expect(context).toMatchObject([{ source_data_access_id: "fixture", source_table: "clinvar_evidence", context_only: true,
    records: [{ variation_id: "1", clinical_significance: "Benign", exact_allele_match: false, trait_match: false }], projection_errors: [],
  }]);
  expect(context[0]).not.toHaveProperty("native_response_json");
  expect(context[0].summary).not.toHaveProperty("native_response_json");
});
it.each([{ response_sha256: "bad" }, { native_response_json: null }, { source_url: "https://untrusted.test" },
  { http_status: 404 }, { rsid: "rs2" }, { requested_ids_json: '["2"]' },
])("rejects inconsistent native/source references %#", async overrides => {
  vi.mocked(readCompleteTable).mockResolvedValue([{ ...await search(), ...overrides }, await summary()]);
  await expect(loadClinvarContext(ns, "fixture", caps)).rejects.toThrow();
});
it("rejects substituted batch IDs even when its URL and native hash are consistent", async () => {
  vi.mocked(readCompleteTable).mockResolvedValue([await search(), { ...await summary(), requested_ids_json: '["2"]', source_url: clinvarUrl("summary", null, ["2"]) }]);
  await expect(loadClinvarContext(ns, "fixture", caps)).rejects.toThrow("searched IDs");
});
it("retains provider denial and missing-record diagnostics without promoting them to evidence", async () => {
  vi.mocked(readCompleteTable).mockResolvedValue([await search(), { ...await row("summary", { error: "limited" }), status: "error", http_status: 429 }]);
  expect((await loadClinvarContext(ns, "fixture", caps)).get("rs1")).toMatchObject([{ records: [], projection_errors: ["Summary unavailable for ClinVar UID 1"], summary: { status: "error", http_status: 429 } }]);
  vi.mocked(readCompleteTable).mockResolvedValue([await search(), await row("summary", { result: { uids: [] } })]);
  expect((await loadClinvarContext(ns, "fixture", caps)).get("rs1")).toMatchObject([{ records: [], projection_errors: [expect.stringContaining("UID 1")] }]);
});
it("rejects duplicate requests and incomplete staged reads", async () => {
  vi.mocked(readCompleteTable).mockResolvedValue([await search(), await search()]);
  await expect(loadClinvarContext(ns, "fixture", caps)).rejects.toThrow("Duplicate");
  vi.mocked(readCompleteTable).mockRejectedValue(new Error("Complete table required"));
  await expect(loadClinvarContext(ns, "fixture", caps)).rejects.toThrow("Complete table");
  await expect(loadClinvarContext(ns, "fixture", '{"include_clinvar":true,"clinvar_context_version":2}')).rejects.toThrow("version");
});
