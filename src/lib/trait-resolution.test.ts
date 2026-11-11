import { afterEach, expect, it, vi } from "vitest";
import { resolveTrait, selectExactTrait } from "./trait-resolution";

const doc = (id = "EFO:0000270", label = "asthma") => ({ obo_id: id, label, ontology_name: "efo", type: "class" });
const response = (docs: unknown[], extra = {}) => ({ response: { numFound: docs.length, start: 0, docs, ...extra } });
afterEach(() => vi.unstubAllGlobals());

it("selects a unique complete exact label regardless of search rank, preserving its namespace", () => {
  expect(selectExactTrait("  ASTHMA  ", response([doc("EFO:2", "allergic asthma"), doc()]))).toEqual({ efo_id: "EFO_0000270", label: "asthma", status: "exact_label" });
  expect(selectExactTrait("asthma", response([doc("MONDO:0004979")]))).toMatchObject({ efo_id: "MONDO_0004979" });
  expect(selectExactTrait("type 2 diabetes", response([doc("EFO:1", "type   2 diabetes")]))).toMatchObject({ status: "exact_label" });
});

it("does not choose by rank or infer identity from synonyms, partial labels or empty text", () => {
  for (const [query, rows] of [["asthma", [doc("EFO:2", "allergic asthma")]], ["asthma", [{ ...doc("EFO:2", "other"), synonym: ["asthma"] }]], ["", [doc("EFO:2", "")]]] as const) {
    expect(selectExactTrait(query, response([...rows]))).toMatchObject({ efo_id: null });
  }
});

it("leaves multiple exact identifiers ambiguous; repeated copies of one ID are consistent", () => {
  expect(selectExactTrait("asthma", response([doc("EFO:1"), doc("EFO:2")]))).toMatchObject({ efo_id: null, status: "ambiguous" });
  expect(selectExactTrait("asthma", response([doc("EFO:1"), doc("EFO_1")]))).toMatchObject({ efo_id: "EFO_1", status: "exact_label" });
});

it.each([{ numFound: 26 }, { numFound: "1" }, { numFound: undefined }, { numFound: 0 }, { numFoundExact: false }, { start: 1 }])("does not establish uniqueness from incomplete/invalid paging %j", extra => {
  expect(selectExactTrait("asthma", response([doc()], extra))).toEqual({ efo_id: null, label: null, status: "incomplete" });
});

it.each([{ obo_id: null }, { obo_id: "asthma" }, { type: "property" }, { ontology_name: "other" }, { is_obsolete: true }, { label: null }])("requires a usable native class identity %j", patch => {
  expect(selectExactTrait("asthma", response([{ ...doc(), ...patch }]))).toMatchObject({ efo_id: null, status: "incomplete" });
});

it("distinguishes no matches from malformed responses", () => {
  expect(selectExactTrait("asthma", response([]))).toMatchObject({ status: "unmatched" });
  for (const native of [null, {}, { response: { docs: "bad", numFound: 1 } }, response([doc(), null])]) {
    expect(selectExactTrait("asthma", native)).toMatchObject({ status: "incomplete" });
  }
});

it("uses the bounded existing OLS adapter with exact search, retaining all native fields", async () => {
  const native = { ...response([doc()]), provider_extension: ["retained"] };
  const fetchMock = vi.fn(async (_input: RequestInfo | URL) => Response.json(native)); vi.stubGlobal("fetch", fetchMock);
  const warnings: string[] = []; const result = await resolveTrait("asthma", null, warnings);
  const url = new URL(String(fetchMock.mock.calls[0][0]));
  expect(url.origin + url.pathname).toBe("https://www.ebi.ac.uk/ols4/api/search");
  expect(Object.fromEntries(url.searchParams)).toMatchObject({ q: "asthma", exact: "true", rows: "25", ontology: "efo", type: "class", local: "true" });
  expect(result).toMatchObject({ status: "exact_label", efo_id: "EFO_0000270", query: "asthma", retrieved_at: expect.any(String) });
  expect(JSON.parse(result.native_response_json!)).toEqual(native); expect(warnings).toEqual([]);
});

it("records ambiguity with native candidates and an actionable warning", async () => {
  const native = response([doc("EFO:1"), doc("EFO:2")]); vi.stubGlobal("fetch", vi.fn(async () => Response.json(native)));
  const warnings: string[] = []; const result = await resolveTrait("asthma", null, warnings);
  expect(result).toMatchObject({ status: "ambiguous", efo_id: null });
  expect(JSON.parse(result.native_response_json!)).toEqual(native);
  expect(warnings.join(" ")).toContain("explicit efo_id");
});

it("honors explicit identifiers without OLS requests or implying they were resolved", async () => {
  const fetchMock = vi.fn(); vi.stubGlobal("fetch", fetchMock);
  expect(await resolveTrait("different query", "EFO:1", [])).toMatchObject({ status: "explicit_id", efo_id: "EFO_1", native_response_json: null, retrieved_at: null });
  expect(await resolveTrait(null, null, [])).toMatchObject({ status: "not_requested", efo_id: null });
  await expect(resolveTrait(null, "asthma", [])).rejects.toThrow("ontology identifier");
  expect(fetchMock).not.toHaveBeenCalled();
});

it("leaves unavailable or oversized responses unresolved", async () => {
  for (const reply of [() => new Response("error", { status: 503 }), () => new Response("x".repeat(1024 * 1024 + 1))]) {
    vi.stubGlobal("fetch", vi.fn(async () => reply()));
    const warnings: string[] = [];
    expect(await resolveTrait("asthma", null, warnings)).toMatchObject({ status: "failed", efo_id: null, native_response_json: null });
    expect(warnings[0]).toContain("OLS4 trait resolution failed");
  }
});
