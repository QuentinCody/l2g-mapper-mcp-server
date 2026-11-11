import { afterEach, expect, it, vi } from "vitest";
import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";
import { loadGnomadContext } from "./gnomad-synthesis";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { fetchGnomadLane, GNOMAD_CONTEXT_VERSION } from "./gnomad-context";
vi.mock("@bio-mcp/shared/staging/utils", () => ({ queryDataFromDo: vi.fn() }));
afterEach(() => { vi.resetAllMocks(); vi.unstubAllGlobals(); });
const ns = {} as DurableObjectNamespace;
const enabled = JSON.stringify({ include_gnomad_context: true });
const gene = { gene_id: "ENSG00000134057", symbol: "CCNB1", reference_genome: "GRCh38", flags: ["native_flag"], gnomad_constraint: { oe_lof_upper: 0.9, pli: 0, flags: ["constraint_flag"] } };
async function observation() {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json({ data: { gene } })));
  return (await fetchGnomadLane([{ ensembl_id: gene.gene_id }], []))[0];
}
function reply(rows: unknown[], extra = {}) {
  vi.mocked(queryDataFromDo).mockResolvedValue({ rows, row_count: rows.length, data_access_id: "fixture", sql: "fixture", executed_at: "fixture", ...extra });
}

it("does not query an optional table when context was not requested", async () => {
  await loadGnomadContext(ns, "fixture"); await loadGnomadContext(ns, "fixture", "{}");
  await loadGnomadContext(ns, "fixture", enabled, false);
  expect(queryDataFromDo).not.toHaveBeenCalled();
});
it("rederives summaries and provenance from native evidence, including older complete gathers", async () => {
  const row = await observation();
  reply([{ ...row, ensembl_id: "ENSG2", reference_genome: "GRCh37", oe_lof_upper: 99, gene_flags_json: "[]", constraint_provenance_json: '{"release_pinned":true}' }]);
  const context = await loadGnomadContext(ns, "fixture", enabled);
  expect(context.size).toBe(1);
  const value = context.get(gene.gene_id)!;
  expect(value).toMatchObject({ ensembl_id: gene.gene_id, reference_genome: "GRCh38", oe_lof_upper: 0.9, pli: 0, gene_flags_json: '["native_flag"]', identity_verified: true, status: "observed", context_only: true, release_pinned: false, source_data_access_id: "fixture", source_table: "gnomad", response_sha256: row.response_sha256 });
  expect(value).not.toHaveProperty("native_response_json");
  expect(JSON.parse(value.constraint_provenance_json as string)).toMatchObject({ runtime_metric_release: null, release_pinned: false });
  expect(vi.mocked(queryDataFromDo).mock.calls[0][2]).toBe("SELECT * FROM gnomad");
});
it("rejects failed or incomplete context reads", async () => {
  const row = await observation();
  reply([row], { total_matching: 101 });
  await expect(loadGnomadContext(ns, "fixture", enabled)).rejects.toThrow("capped");
  vi.mocked(queryDataFromDo).mockRejectedValueOnce(new Error("unavailable"));
  await expect(loadGnomadContext(ns, "fixture", enabled)).rejects.toThrow("unavailable");
});

it.each([
  { response_sha256: "changed" }, { native_response_json: null }, { native_response_json: "{}" },
  { source_url: "https://example.test/api" }, { query: "other query" },
  { variables_json: '{"id":"other","ref":"GRCh38"}' }, { variables_json: '{"id":"ENSG00000134057","ref":"GRCh37"}' },
  { variables_json: '{"id":"ENSG00000134057","ref":"GRCh38","extra":true}' },
  { reference_genome_requested: "GRCh37" }, { retrieved_at: "invalid" },
  { usable: "true" }, { error: "conflicting error" }, { requested_ensembl_id: "" },
])("rejects corrupt source and request evidence %#", async changes => {
  reply([{ ...await observation(), ...changes }]);
  await expect(loadGnomadContext(ns, "fixture", enabled)).rejects.toThrow();
});

it.each([
  { data: { gene: { ...gene, gene_id: "other" } } },
  { data: { gene: { ...gene, reference_genome: "GRCh37" } } },
  { data: { gene }, errors: [{ message: "partial" }] },
  { data: { gene }, errors: {} }, { data: { gene: null } },
  { data: { gene: { ...gene, gnomad_constraint: [] } } },
])("rejects a claimed usable row with incompatible native evidence despite a matching hash %#", async native => {
  reply([{ ...await observation(), native_response_json: JSON.stringify(native), response_sha256: await hashCanonicalJson(native) }]);
  await expect(loadGnomadContext(ns, "fixture", enabled)).rejects.toThrow();
});

it.each([null, undefined])("retains native null versus unreported constraints (%s)", async constraint => {
  const native = { data: { gene: { ...gene, gnomad_constraint: constraint } } };
  reply([{ ...await observation(), usable: 1, native_response_json: JSON.stringify(native), response_sha256: await hashCanonicalJson(native) }]);
  const value = (await loadGnomadContext(ns, "fixture", enabled)).get(gene.gene_id);
  expect(value).toMatchObject({ identity_verified: true, constraint_available: constraint === null ? false : null, oe_lof_upper: null });
});

it.each([false, 0])("keeps failed observations distinct without promoting their stored metrics (%s)", async usable => {
  const row = await observation();
  reply([{ ...row, usable, error: "HTTP 503", native_response_json: null, response_sha256: null }]);
  expect((await loadGnomadContext(ns, "fixture", enabled)).get(gene.gene_id)).toMatchObject({ status: "error", error: "HTTP 503", identity_verified: false, ensembl_id: null, oe_lof_upper: null, constraint_available: null });
});

it("retains and hashes failed native GraphQL evidence", async () => {
  const native = { data: { gene }, errors: [{ message: "partial" }] };
  const row = { ...await observation(), usable: false, error: "partial", native_response_json: JSON.stringify(native), response_sha256: await hashCanonicalJson(native) };
  reply([row]);
  expect((await loadGnomadContext(ns, "fixture", enabled)).get(gene.gene_id)).toMatchObject({ identity_verified: false, response_sha256: row.response_sha256, oe_lof_upper: null });
  reply([{ ...row, response_sha256: "tampered" }]);
  await expect(loadGnomadContext(ns, "fixture", enabled)).rejects.toThrow("hash mismatch");
});

it("rejects missing, duplicate and excess observations and unsupported context versions", async () => {
  const row = await observation();
  for (const rows of [[], [row, row], Array(41).fill(row)]) {
    reply(rows); await expect(loadGnomadContext(ns, "fixture", enabled)).rejects.toThrow();
  }
  reply([row]);
  await expect(loadGnomadContext(ns, "fixture", JSON.stringify({ include_gnomad_context: true, gnomad_context_version: "unknown" }))).rejects.toThrow("version");
  expect((await loadGnomadContext(ns, "fixture", JSON.stringify({ include_gnomad_context: true, gnomad_context_version: GNOMAD_CONTEXT_VERSION }))).size).toBe(1);
});
