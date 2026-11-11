import { afterEach, expect, it, vi } from "vitest";
import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { loadFinnGenContext } from "./finngen-synthesis";
vi.mock("@bio-mcp/shared/staging/utils", () => ({ queryDataFromDo: vi.fn() }));
afterEach(() => vi.resetAllMocks());
const ns = {} as DurableObjectNamespace;
const caps = JSON.stringify({ finngen_release: "R13" });
const anchor = { rsid: "rs1", locus_id: "L1", grch38_chr: "1", grch38_pos: 10, grch38_ref: "A", grch38_alt: "G" };
const native = { results: [{ phenocode: "FIXTURE", pval: "1e-500", beta: "-0.020" }], future: [null, false, "α"] };
async function row(data: unknown = native, extra = {}) {
  return { rsid: "rs1", locus_id: "L1", requested_variant: "1:10-A-G", release_requested: "R13", assembly_requested: "GRCh38",
    release_basis: "requested_host", source_url: "https://r13.finngen.fi/api/variant/1%3A10-A-G", retrieved_at: "2026-10-02T00:00:00Z",
    status: "observed", error: null, http_status: 200, native_response_json: JSON.stringify(data), response_sha256: await hashCanonicalJson(data), ...extra };
}
const load = (anchors = [anchor], selection = caps) => loadFinnGenContext(ns, "fixture", selection, anchors);
function reply(rows: unknown[], extra = {}) { vi.mocked(queryDataFromDo).mockResolvedValue({ rows, row_count: rows.length, data_access_id: "fixture", sql: "fixture", executed_at: "fixture", ...extra }); }
it("does not query absent optional context in existing gathers", async () => {
  await loadFinnGenContext(ns, "fixture", undefined, []); await load([], '{"finngen_release":null}');
  expect(queryDataFromDo).not.toHaveBeenCalled();
});
it.each(["R12", "R13"])("checks complete %s source evidence and returns references without promoting scientific identity", async release => {
  const evidence = await row(native, { release_requested: release, source_url: `https://${release.toLowerCase()}.finngen.fi/api/variant/1%3A10-A-G` });
  reply([evidence]); const references = (await load([anchor], JSON.stringify({finngen_release: release}))).get("rs1");
  expect(references).toMatchObject([{ source_data_access_id: "fixture", source_table: "finngen_evidence", source_hash_verified: true,
    request_context_verified: true, response_sha256: evidence.response_sha256, association_count: 1,
    context_only: true, identity_verified: false, trait_equivalence_verified: false, coverage_complete: false }]);
  expect(references![0]).not.toHaveProperty("native_response_json");
  expect(vi.mocked(queryDataFromDo).mock.calls[0][2]).toBe("SELECT * FROM finngen_evidence");
});
it.each([404, 200])("retains hash-checked HTTP/QC failure observations (%i)", async http_status => {
  reply([await row({ error: "unavailable", native: "retained" }, { status: "error", error: "Provider failure", http_status })]);
  expect((await load()).get("rs1")).toMatchObject([{ status: "error", source_hash_verified: true, association_count: null }]);
});
it("retains empty associations, transport failures and skipped unresolved anchors distinctly", async () => {
  reply([await row({ results: [] })]);
  expect((await load()).get("rs1")).toMatchObject([{ source_hash_verified: true, association_count: 0 }]);
  const noNative = { native_response_json: null, response_sha256: null, http_status: null, error: "Unavailable" };
  reply([await row(null, { ...noNative, status: "error" })]);
  expect((await load()).get("rs1")).toMatchObject([{ status: "error", source_hash_verified: false, association_count: null }]);
  reply([await row(null, { ...noNative, status: "skipped", source_url: null, requested_variant: null })]);
  expect((await load([{ ...anchor, grch38_pos: 0 }])).get("rs1")).toMatchObject([{ status: "skipped", source_hash_verified: false }]);
});
it.each([{ release_requested: "R12" }, { assembly_requested: "GRCh37" }, { release_basis: "immutable" }, { locus_id: "OTHER" },
  { requested_variant: "1:10-G-A" }, { source_url: "https://r12.finngen.fi/api/variant/1%3A10-A-G" },
  { source_url: "https://example.org/" }, { retrieved_at: "invalid" }, { status: "unknown" },
  { http_status: 404 }, { http_status: null }, { http_status: 999 }, { error: "not successful" },
  { status: "skipped", error: "No request" }, { status: "error", error: null }])("rejects inconsistent staged request/state evidence: %j", async extra => {
  reply([await row(native, extra)]); await expect(load()).rejects.toThrow();
});
it.each([{ native_response_json: null }, { response_sha256: "changed" }, { native_response_json: '{"results":[]}' },
  { native_response_json: "invalid JSON" }, { native_response_json: {} }])("rejects missing, malformed or changed native evidence: %j", async extra => {
  reply([await row(native, extra)]); await expect(load()).rejects.toThrow();
});
it.each([null, [], { results: {} }, { qc_variant_results: {} }])("rejects a hash-valid non-association payload marked observed: %j", async value => {
  reply([await row(value)]); await expect(load()).rejects.toThrow("native association array");
});
it("rejects a hash without native data, and a retained response without HTTP context", async () => {
  reply([await row(native, { status: "error", error: "Unavailable", native_response_json: null })]);
  await expect(load()).rejects.toThrow("lacks native evidence");
  reply([await row(native, { status: "error", error: "Unavailable", http_status: null })]);
  await expect(load()).rejects.toThrow("native evidence");
});
it("rejects unknown, missing, duplicated and over-budget anchor sets", async () => {
  reply([await row(native, { rsid: "rs2" })]); await expect(load()).rejects.toThrow("unknown or duplicate");
  reply([]); await expect(load()).rejects.toThrow("Incomplete");
  reply([await row(), await row()]); await expect(load([anchor, anchor])).rejects.toThrow("ambiguous");
  await expect(load([anchor, { ...anchor, rsid: "rs2" }])).rejects.toThrow("duplicate");
  const anchors = Array.from({length: 26}, (_, i) => ({ ...anchor, rsid: `rs${i}` }));
  reply(await Promise.all(anchors.map(a => row(native, {rsid: a.rsid}))));
  await expect(load(anchors)).rejects.toThrow("Incomplete");
});
it("rejects unsupported release selections before any storage read", async () => {
  await expect(load([], '{"finngen_release":"R14"}')).rejects.toThrow("Unrecognized");
  expect(queryDataFromDo).not.toHaveBeenCalled();
});
it.each([{ truncated: true }, { count_capped: true }, { total_matching: 26 }])("rejects incomplete reference reads: %j", async extra => {
  reply([await row()], extra); await expect(load()).rejects.toThrow("capped");
});
