import { hashCanonicalJson } from "@bio-mcp/shared";
import { afterEach, expect, it, vi } from "vitest";
import { fetchGenebassContext } from "./genebass-context";
import { genebassSourceUrl, observeGenebass } from "./genebass-observation";
afterEach(() => vi.unstubAllGlobals());
const gene = "ENSG00000134057";
const native = { associations: [
  { phenotype_description: "other phenotype", skat_o_pvalue: 0.001, beta: "0.0123", qc: ["flag"] },
  { phenotype_description: "lowest p-value", skat_o_pvalue: 1e-8, future: { retained: true } },
], metadata: { test: "fixture", release: "unverified-native-label" } };

it("keeps every native association and request alongside the legacy minimum summary", async () => {
  const fetcher = vi.fn(async () => Response.json(native)); vi.stubGlobal("fetch", fetcher);
  const result = await fetchGenebassContext(new Map([["CCNB1", gene]]), ["missense|LC"], []);
  expect(result.rows).toMatchObject([{ p_value: 1e-8, phenotype: "lowest p-value" }]);
  expect(result.evidence).toMatchObject([{ requested_ensembl_id: gene, burden_set: "missense|LC", http_status: 200,
    source_url: genebassSourceUrl(gene, "missense|LC"), response_sha256: await hashCanonicalJson(native),
    status: "observed", context_only: true, identity_verified: false, trait_matched: false, release_pinned: false, contract_verified: false }]);
  expect(JSON.parse(result.evidence[0].native_response_json!)).toEqual(native);
  expect(fetcher).toHaveBeenCalledWith(genebassSourceUrl(gene, "missense|LC"), expect.objectContaining({ redirect: "manual", signal: expect.any(AbortSignal) }));
});

it.each([200, 403, 503])("retains parsed errors at HTTP %s without fabricating a no-support row", async status => {
  const body = { error: "fixture failure", associations: [], details: { retained: true } };
  vi.stubGlobal("fetch", vi.fn(async () => Response.json(body, { status })));
  const warnings: string[] = [];
  const result = await fetchGenebassContext(new Map([["CCNB1", gene]]), ["pLoF"], warnings);
  expect(result.rows).toEqual([]);
  expect(result.evidence).toMatchObject([{ status: "error", http_status: status, response_sha256: await hashCanonicalJson(body) }]);
  expect(JSON.parse(result.evidence[0].native_response_json!)).toEqual(body);
  expect(warnings).toHaveLength(1);
});

it("retains malformed successful JSON as unqualified error evidence", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => Response.json({ unexpected: "native" })));
  const result = await fetchGenebassContext(new Map([["CCNB1", gene]]), ["pLoF"], []);
  expect(result.rows).toEqual([]);
  expect(result.evidence).toMatchObject([{ status: "error", native_response_json: '{"unexpected":"native"}' }]);
});

it.each(["network", "non-json", "oversized", "redirect"])("records %s failure without a successful native response", async kind => {
  vi.stubGlobal("fetch", vi.fn(async () => {
    if (kind === "network") throw new Error("offline");
    if (kind === "non-json") return new Response("not JSON");
    if (kind === "redirect") return new Response(null, { status: 302, headers: { location: "https://example.org" } });
    return Response.json({ payload: "α".repeat(1100000) });
  }));
  const result = await observeGenebass("CCNB1", gene, "pLoF");
  expect(result).toMatchObject({ status: "error", native_response_json: null, response_sha256: null, error: expect.any(String) });
});
