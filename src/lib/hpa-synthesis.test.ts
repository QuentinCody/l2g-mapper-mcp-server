import { hashCanonicalJson } from "@bio-mcp/shared";
import { afterEach, expect, it, vi } from "vitest";
import { readCompleteTable } from "./complete-table";
import { loadHpaContext } from "./hpa-synthesis";
vi.mock("./complete-table", () => ({ readCompleteTable: vi.fn() }));
afterEach(() => vi.resetAllMocks());
const ns = {} as DurableObjectNamespace;
const caps = JSON.stringify({ include_hpa_tissue_context: true, hpa_context_version: 1 });
const id = "ENSG00000134057";
const native = { Ensembl: id, Gene: "CCNB1", "RNA tissue specific nTPM": { liver: "3.00" } };
async function row() { return { requested_ensembl_id: id, native_response_json: JSON.stringify(native), response_sha256: await hashCanonicalJson(native), status: "observed", http_status: 200, source_url: `https://www.proteinatlas.org/${id}.json`, tissues: "forged", rna_tissue_specific_ntpm_json: '{"forged":99}' }; }

it("does not query absent or legacy HPA gathers", async () => {
  await loadHpaContext(ns, "fixture");
  await loadHpaContext(ns, "fixture", '{"include_hpa_tissue_context":true}');
  await loadHpaContext(ns, "fixture", caps, false);
  expect(readCompleteTable).not.toHaveBeenCalled();
});

it("rederives display values from verified native evidence and retains staged references", async () => {
  vi.mocked(readCompleteTable).mockResolvedValue([await row()]);
  const contexts = await loadHpaContext(ns, "fixture", caps);
  expect(contexts.get(id)).toMatchObject([{ tissues: "liver", rna_tissue_specific_ntpm_json: '{"liver":"3.00"}', identity_verified: true, source_data_access_id: "fixture", source_table: "hpa", context_only: true, release_pinned: false }]);
  expect(contexts.get(id)![0]).not.toHaveProperty("native_response_json");
});

it.each([{ response_sha256: "bad" }, { native_response_json: null }, { source_url: "https://untrusted.test" }, { http_status: 404 }, { requested_ensembl_id: "ENSG00000141510" }])("rejects inconsistent observation references: %j", async overrides => {
  vi.mocked(readCompleteTable).mockResolvedValue([{ ...await row(), ...overrides }]);
  await expect(loadHpaContext(ns, "fixture", caps)).rejects.toThrow();
});

it("retains error context without promoting forged derived fields", async () => {
  vi.mocked(readCompleteTable).mockResolvedValue([{ ...await row(), status: "error", error: "schema mismatch" }]);
  expect((await loadHpaContext(ns, "fixture", caps)).get(id)).toMatchObject([{ identity_verified: false, tissues: "", ensembl_id: null, status: "error", error: "schema mismatch" }]);
});

it("rejects incomplete reads and unknown policy versions", async () => {
  vi.mocked(readCompleteTable).mockRejectedValue(new Error("Complete hpa input required"));
  await expect(loadHpaContext(ns, "fixture", caps)).rejects.toThrow("Complete");
  await expect(loadHpaContext(ns, "fixture", '{"include_hpa_tissue_context":true,"hpa_context_version":2}')).rejects.toThrow("version");
});
