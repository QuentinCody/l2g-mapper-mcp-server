import { afterEach, expect, it, vi } from "vitest";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { gnomadGraphql } from "@bio-mcp/shared/adapters/gnomad";
import { boundedGnomadFetch, fetchGnomadLane, GNOMAD_GENE_QUERY } from "./gnomad-context";

vi.mock("@bio-mcp/shared/adapters/gnomad", () => ({ gnomadGraphql: vi.fn() }));
afterEach(() => { vi.resetAllMocks(); vi.unstubAllGlobals(); vi.useRealTimers(); });
const gene = {
  gene_id: "ENSG1", symbol: "CURRENT", reference_genome: "GRCh38", flags: ["low_exome_coverage"],
  canonical_transcript_id: "ENST1", mane_select_transcript: { ensembl_id: "ENST2", ensembl_version: "3", refseq_id: "NM_1", refseq_version: "2" },
  gnomad_constraint: { pli: 0, oe_lof: 0.4, oe_lof_lower: 0.2, oe_lof_upper: 0.9, oe_lof_percentile: 40, lof_z: 1.2, mis_z: null, flags: ["outlier_syn"] },
};

it("queries deduplicated exact gene IDs and retains native metrics, flags, transcripts and qualified provenance", async () => {
  const native = { data: { gene } }; vi.mocked(gnomadGraphql).mockResolvedValue(native);
  const warnings: string[] = []; const rows = await fetchGnomadLane([{ ensembl_id: "ENSG1" }, { ensembl_id: "ENSG1" }], warnings);
  expect(warnings).toEqual([]); expect(rows).toHaveLength(1);
  expect(gnomadGraphql).toHaveBeenCalledExactlyOnceWith(GNOMAD_GENE_QUERY, { id: "ENSG1", ref: "GRCh38" }, { fetchImpl: boundedGnomadFetch, timeoutMs: 4000 });
  expect(GNOMAD_GENE_QUERY).toContain("gene(gene_id: $id"); expect(GNOMAD_GENE_QUERY).not.toContain("pLI");
  expect(rows[0]).toMatchObject({ usable: true, requested_ensembl_id: "ENSG1", ensembl_id: "ENSG1", symbol: "CURRENT", reference_genome: "GRCh38", pli: 0, oe_lof_lower: 0.2, oe_lof_upper: 0.9, oe_lof_percentile: 40, mis_z: null, gene_flags_json: JSON.stringify(gene.flags), constraint_flags_json: JSON.stringify(gene.gnomad_constraint.flags), canonical_transcript_id: "ENST1", mane_select_transcript_json: JSON.stringify(gene.mane_select_transcript), native_response_json: JSON.stringify(native), response_sha256: await hashCanonicalJson(native) });
  expect(JSON.parse(rows[0].constraint_provenance_json)).toMatchObject({ documented_metric_release: "4.1.1", runtime_metric_release: null, release_pinned: false, release_basis: "provider_pipeline" });
  expect(JSON.parse(rows[0].constraint_provenance_json).published_release_notice).toMatchObject({ release: "4.1.2", published_on: "2026-09-28", runtime_application_verified: false });
});

it.each([null, undefined])("preserves unavailable versus unreported constraints (%s)", async constraint => {
  vi.mocked(gnomadGraphql).mockResolvedValue({ data: { gene: { ...gene, gnomad_constraint: constraint } } });
  expect((await fetchGnomadLane([{ ensembl_id: "ENSG1" }], []))[0]).toMatchObject({ usable: true, constraint_available: constraint === null ? false : null, oe_lof: null, pli: null });
});

it.each([
  { data: { gene }, errors: [{ message: "partial failure" }] },
  { data: { gene: { ...gene, gene_id: "ENSG2" } } },
  { data: { gene: { ...gene, reference_genome: "GRCh37" } } },
  { data: { gene: null } },
])("does not admit partial errors or incompatible identities %#", async native => {
  vi.mocked(gnomadGraphql).mockResolvedValue(native);
  const warnings: string[] = []; const rows = await fetchGnomadLane([{ ensembl_id: "ENSG1" }], warnings);
  expect(rows[0]).toMatchObject({ usable: false, pli: null, oe_lof_upper: null, native_response_json: JSON.stringify(native), error: expect.any(String) });
  expect(warnings).toHaveLength(1);
});

it("records transport errors and explicitly caps candidate coverage without symbol fallback", async () => {
  vi.mocked(gnomadGraphql).mockRejectedValue(new Error("HTTP 429"));
  const warnings: string[] = [];
  const rows = await fetchGnomadLane([{ ensembl_id: null }, ...Array.from({ length: 41 }, (_, i) => ({ ensembl_id: `ENSG${i}` }))], warnings);
  expect(gnomadGraphql).toHaveBeenCalledTimes(40); expect(rows).toHaveLength(40);
  expect(rows.every(row => !row.usable && row.native_response_json === null && row.error === "HTTP 429")).toBe(true);
  expect(warnings.join(" ")).toContain("no symbol fallback"); expect(warnings.join(" ")).toContain("40 distinct");
});

it("bounds native body bytes before JSON parsing", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("x".repeat(256 * 1024 + 1))));
  await expect(boundedGnomadFetch("https://fixture.test")).rejects.toThrow("262144-byte limit");
});

it.each([{ status: 206 }, { status: 200, headers: { "content-range": "bytes 0-10/100" } }])("rejects partial native HTTP responses %#", async init => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json({ data: { gene } }, init)));
  await expect(boundedGnomadFetch("https://fixture.test")).rejects.toThrow("complete HTTP 200");
});

it("rejects invalid UTF-8 and cancels stalled native bodies", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValueOnce(new Response(new Uint8Array([0xff]))));
  await expect(boundedGnomadFetch("https://fixture.test")).rejects.toThrow();
  vi.useFakeTimers(); const cancel = vi.fn();
  vi.mocked(fetch).mockResolvedValueOnce(new Response(new ReadableStream({ cancel })));
  const check = expect(boundedGnomadFetch("https://fixture.test")).rejects.toThrow("body timed out");
  await vi.advanceTimersByTimeAsync(4001); await check;
  expect(cancel).toHaveBeenCalled();
});
