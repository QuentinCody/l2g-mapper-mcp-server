import { afterEach, expect, it, vi } from "vitest";
import { bestGenebassAssociation, fetchGenebassContext } from "./genebass-context";

afterEach(() => vi.unstubAllGlobals());

// First APOE/pLoF row captured from the provider's /api/phewas endpoint on 2026-10-01.
const row = { gene_id: "ENSG00000130203", gene_symbol: "APOE", annotation: "pLoF",
  trait_type: "categorical", phenocode: "120", pheno_sex: "both_sexes", coding: "1", modifier: "",
  Pvalue: 0.107292256142719, Pvalue_Burden: 0.74379759975012,
  Pvalue_SKAT: 0.0653837654752666, BETA_Burden: -0.00687663843052551,
  total_variants: 9, chrom: "19", start: 44905755 };

it("selects native SKAT-O and preserves composite identity without inventing a description", () => {
  expect(bestGenebassAssociation({ phewas: [row, { ...row, Pvalue: 0.01, coding: "2", Pvalue_Burden: 0.9 }] })).toEqual({
    pValue: 0.01, phenotype: null, phenotypeId: "categorical-120-both_sexes-2-", associationIndex: 1, associationArray: "phewas",
  });
  expect(bestGenebassAssociation({ phewas: [row] }).pValue).toBe(row.Pvalue);
});

it.each([{ ...row, Pvalue: "0.1" }, { ...row, modifier: null }, { ...row, Pvalue: -1 }])("rejects malformed native rows %#", invalid => {
  expect(() => bestGenebassAssociation({ phewas: [invalid] })).toThrow();
});

it("retains a realistic 1.3 MB response and caps memory and concurrent fetches with explicit skipped evidence", async () => {
  const native = { gene: { gene_id: row.gene_id }, phewas: [row], extension: "x".repeat(1_300_000) };
  let active = 0; let peak = 0;
  const fetcher = vi.fn(async () => {
    active++; peak = Math.max(peak, active);
    await new Promise(resolve => setTimeout(resolve, 0)); active--;
    return Response.json(native);
  });
  vi.stubGlobal("fetch", fetcher);
  const warnings: string[] = [];
  const result = await fetchGenebassContext(new Map(Array.from({ length: 10 }, (_, i) => [`G${i}`, `ENSG${i}`])), ["pLoF"], warnings);
  expect(peak).toBe(2); expect(fetcher).toHaveBeenCalledTimes(4);
  expect(result.rows).toHaveLength(4); expect(result.evidence).toHaveLength(10);
  expect(JSON.parse(result.evidence[0].native_response_json!)).toEqual(native);
  expect(result.evidence.slice(4)).toEqual(Array.from({ length: 6 }, () => expect.objectContaining({ status: "error", http_status: null,
    native_response_json: null, error: expect.stringContaining("budget") })));
  expect(warnings).toHaveLength(6);
});

it("retains completed evidence when the lane has no time for another bounded request", async () => {
  const started = Date.now(); let now = started;
  const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
  const fetcher = vi.fn(async () => { now = started + 7100; return Response.json({ phewas: [row] }); });
  vi.stubGlobal("fetch", fetcher);
  try {
    const result = await fetchGenebassContext(new Map([["APOE", row.gene_id], ["G2", "ENSG2"]]), ["pLoF", "synonymous"], []);
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(result.rows).toHaveLength(1);
    expect(result.evidence.slice(1).every(item => item.error?.includes("deadline"))).toBe(true);
  } finally { clock.mockRestore(); }
});
