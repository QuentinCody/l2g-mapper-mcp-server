import { afterEach, expect, it, vi } from "vitest";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { fetchClinvarLane, clinvarProjection } from "./clinvar-context";
import { observeClinvar, searchIds } from "./clinvar-observation";
import { gatherInputs } from "./gather-inputs";
import { projectedSubrequests } from "./gather-budget";
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.useRealTimers(); });

const search = { header: { type: "esearch" }, esearchresult: { count: "8", retstart: "0", retmax: "3", idlist: ["65533"], querytranslation: "fixture related variants", warninglist: { outputmessages: ["fixture"] } } };
const summary = { header: { type: "esummary", version: "0.3" }, result: { uids: ["65533"], "65533": {
  uid: "65533", title: "fixture", accession_version: "VCV000065533.11",
  germline_classification: { description: "Pathogenic", review_status: "criteria provided, multiple submitters, no conflicts", trait_set: [{ trait_name: "fixture condition" }] },
  clinical_impact_classification: { description: "Tier II" }, oncogenicity_classification: { description: "Likely oncogenic" },
  clinical_significance: { description: "legacy value" }, genes: [{ symbol: "LYST", source: "submitted" }],
  variation_set: [{ variation_xrefs: [{ db_source: "dbSNP", db_id: "80338645" }], canonical_spdi: "NC_000001.11:235809277:G:A" }],
} } };

it("preserves native search/summary evidence and every rsID link to a shared variation ID", async () => {
  const fetcher = vi.fn(async (url: string, init: RequestInit) => {
    expect(init.redirect).toBe("manual");
    return Response.json(url.includes("esearch") ? search : summary);
  }); vi.stubGlobal("fetch", fetcher);
  const warnings: string[] = []; const result = await fetchClinvarLane(["rs80338645", "rs2", "rs80338645"], warnings);
  expect(result.rows.map(row => [row.rsid, row.variation_id])).toEqual([["rs80338645", "65533"], ["rs2", "65533"]]);
  expect(result.rows[0]).toMatchObject({ clinical_significance: "Pathogenic", clinical_significance_basis: "germline_classification",
    review_status: "criteria provided, multiple submitters, no conflicts", somatic_clinical_impact: "Tier II", oncogenicity: "Likely oncogenic",
    context_only: true, exact_allele_match: false, trait_match: false, accession_version: "VCV000065533.11" });
  expect(result.evidence.map(row => JSON.parse(row.native_response_json!))).toEqual([search, search, summary]);
  expect(result.evidence[2].response_sha256).toBe(await hashCanonicalJson(summary));
  expect(new URL(fetcher.mock.calls[0][0]).searchParams.get("term")).toBe("rs80338645");
  expect(new URL(fetcher.mock.calls[0][0]).searchParams.get("retmax")).toBe("3");
  expect(new URL(fetcher.mock.calls[2][0]).searchParams.get("id")).toBe("65533");
  expect(fetcher).toHaveBeenCalledTimes(3); expect(warnings.join(" ")).toContain("not an exact allele");
});

it.each([{ esearchresult: { count: "0", idlist: ["1"] } }, { error: "API limit" }, {},
  { esearchresult: { count: "3", idlist: ["1", "1"] } }, { esearchresult: { count: "3", idlist: [1] } },
  { esearchresult: { count: "4", idlist: ["1", "2", "3", "4"] } },
])("rejects malformed search data %#", native => expect(() => searchIds(native)).toThrow("ClinVar"));

it("keeps zero hits, provider errors and invalid identifiers distinct", async () => {
  const fetcher = vi.fn().mockResolvedValueOnce(Response.json({ esearchresult: { count: "0", idlist: [] } }))
    .mockResolvedValueOnce(Response.json({ error: "API rate limit exceeded" }));
  vi.stubGlobal("fetch", fetcher);
  const result = await fetchClinvarLane(["rs1", "rs2", "rs3 OR all"], []);
  expect(result.rows).toEqual([]); expect(result.evidence.map(row => row.status)).toEqual(["observed", "error", "skipped"]);
  expect(result.evidence[1].native_response_json).toBe(JSON.stringify({ error: "API rate limit exceeded" }));
  expect(result.evidence[2].source_url).toBeNull(); expect(fetcher).toHaveBeenCalledTimes(2);
});

it("retains a missing summary UID as a warning without inventing its record", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValueOnce(Response.json(search)).mockResolvedValueOnce(Response.json({ result: { uids: [] } })));
  const warnings: string[] = []; const result = await fetchClinvarLane(["rs1"], warnings);
  expect(result.rows).toEqual([]); expect(result.evidence).toHaveLength(2);
  expect(warnings.join(" ")).toContain("missing/mismatched UID 65533");
});

it.each([429, 503])("retains native HTTP %s errors", async status => {
  vi.stubGlobal("fetch", vi.fn(async () => Response.json({ error: "fixture" }, { status })));
  const result = await observeClinvar("search", "rs1", []);
  expect(result).toMatchObject({ status: "error", http_status: status, native_response_json: '{"error":"fixture"}' });
});

it("bounds response bytes and preserves HTTP status for non-JSON responses", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValueOnce(new Response("x".repeat(4 * 1024 * 1024 + 1))).mockResolvedValueOnce(new Response("bad", { status: 502 })));
  expect(await observeClinvar("search", "rs1", [])).toMatchObject({ status: "error", native_response_json: null, error: expect.stringContaining("byte limit") });
  expect(await observeClinvar("search", "rs1", [])).toMatchObject({ status: "error", http_status: 502, native_response_json: null });
});

it("limits request fanout, spaces requests and retains explicit deadline skips", async () => {
  vi.useFakeTimers();
  // Keep native crypto/stream I/O out of virtual-clock scheduling. Real hashes
  // and response streams are exercised by the provenance tests above.
  vi.spyOn(crypto.subtle, "digest").mockResolvedValue(new ArrayBuffer(32));
  const starts: number[] = []; vi.stubGlobal("fetch", vi.fn(async () => {
    starts.push(Date.now());
    return { body: null, status: 200, ok: true, json: async () => ({ esearchresult: { count: "0", idlist: [] } }) } as Response;
  }));
  const warnings: string[] = []; const promise = fetchClinvarLane(Array.from({ length: 31 }, (_, i) => `rs${i + 1}`), warnings);
  await vi.advanceTimersByTimeAsync(14000); const result = await promise;
  expect(result.evidence).toHaveLength(30); expect(result.evidence.some(row => row.status === "skipped")).toBe(true);
  expect(starts.length).toBeGreaterThan(1);
  expect(result.evidence.filter(row => row.status === "observed")).toHaveLength(starts.length);
  for (let i = 1; i < starts.length; i++) expect(starts[i] - starts[i - 1]).toBeGreaterThanOrEqual(350);
  expect(warnings.join(" ")).toContain("capped at 30");
  const base = gatherInputs({ max_loci: 10, max_anchor_associations: 100 });
  expect(projectedSubrequests({ ...base, include_clinvar: true }, 40) - projectedSubrequests({ ...base, include_clinvar: false }, 40)).toBe(31);
});

it("does not read somatic classification as germline support and checks the requested UID", () => {
  const native = { result: { uids: ["1"], "1": { uid: "1", clinical_impact_classification: { description: "Tier I" } } } };
  expect(clinvarProjection("rs1", "1", native, null, null)).toMatchObject({ clinical_significance: null, clinical_significance_basis: null, somatic_clinical_impact: "Tier I" });
  expect(() => clinvarProjection("rs1", "2", native, null, null)).toThrow("UID 2");
});
