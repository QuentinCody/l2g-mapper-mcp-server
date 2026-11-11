import { afterEach, expect, it, vi } from "vitest";
import { z } from "zod";
import { genebassGeneBurden } from "@bio-mcp/shared/adapters/genebass";
import { bestGenebassAssociation, fetchGenebassContext } from "./genebass-context";
import { gatherInputs, inputSchema } from "./gather-inputs";

vi.mock("@bio-mcp/shared/adapters/genebass", () => ({ genebassGeneBurden: vi.fn() }));
afterEach(() => vi.resetAllMocks());
const genes = new Map([["GENE", "ENSG1"]]);

it.each([null, {}, { error: "upstream failed" }, { associations: {} },
  { associations: [], error: "upstream failed" }, { associations: [], errors: ["partial"] },
  { associations: [null] }, { associations: [{}] },
])("reports unrecognized or failed responses instead of manufacturing no-support rows %#", async native => {
  vi.mocked(genebassGeneBurden).mockResolvedValue(native);
  const warnings: string[] = [];
  expect((await fetchGenebassContext(genes, ["pLoF"], warnings)).rows).toEqual([]);
  expect(warnings).toEqual([expect.stringContaining("Genebass failed for GENE/pLoF")]);
});

it.each([-1, 0, Number.NaN, Number.POSITIVE_INFINITY, 1.1, "1e-9"])("rejects invalid p-values, including after a valid association (%s)", async p => {
  expect(() => bestGenebassAssociation({ associations: [
    { phenotype_description: "valid", skat_o_pvalue: 1e-8 },
    { phenotype_description: "invalid", skat_o_pvalue: p },
  ] })).toThrow("Invalid Genebass SKAT-O p-value");
});

it("keeps the phenotype paired with its p-value and deduplicates requests", async () => {
  vi.mocked(genebassGeneBurden).mockResolvedValue({ associations: [
    { phenotype_description: "nominal", skat_o_pvalue: 0.01 },
    { phenotype_description: "strong", skat_o_pvalue: 1e-8 },
    { phenotype_description: "missing", skat_o_pvalue: null },
  ] });
  const warnings: string[] = [];
  expect((await fetchGenebassContext(genes, ["pLoF", "pLoF"], warnings)).rows).toEqual([
    { symbol: "GENE", ensembl_id: "ENSG1", burden_set: "pLoF", phenotype: "strong", p_value: 1e-8, support: "strong" },
  ]);
  expect(genebassGeneBurden).toHaveBeenCalledExactlyOnceWith("ENSG1", "pLoF", expect.objectContaining({ timeoutMs: 4000, fetchImpl: expect.any(Function) }));
  expect(warnings).toEqual([]);
});

it.each([{ associations: [] }, { associations: [{ skat_o_pvalue: null }] }])("preserves genuine empty or unavailable results %#", async ({ associations }) => {
  vi.mocked(genebassGeneBurden).mockResolvedValue({ associations });
  const warnings: string[] = [];
  expect((await fetchGenebassContext(genes, ["pLoF"], warnings)).rows).toMatchObject([{ phenotype: null, p_value: null, support: "none" }]);
  expect(warnings).toEqual([]);
});

it("enforces bounded input without breaking JSON schema discovery, defaults, or opting out", () => {
  expect(z.toJSONSchema(z.object(inputSchema)).properties?.genebass_burden_sets).toMatchObject({ type: "array", maxItems: 4 });
  expect(gatherInputs({}).genebass_burden_sets).toEqual(["pLoF", "missense|LC"]);
  expect(gatherInputs({ genebass_burden_sets: [] }).genebass_burden_sets).toEqual([]);
  expect(gatherInputs({ genebass_burden_sets: ["pLoF", " pLoF "] }).genebass_burden_sets).toEqual(["pLoF"]);
  for (const invalid of [null, "pLoF", [1], [" "], ["x".repeat(65)], ["a", "b", "c", "d", "e"]]) {
    expect(() => gatherInputs({ genebass_burden_sets: invalid })).toThrow();
  }
});

it("caps gene fan-out, reports truncated coverage, and rejects excessive sets before requests", async () => {
  vi.mocked(genebassGeneBurden).mockResolvedValue({ associations: [] });
  const warnings: string[] = [];
  await expect(fetchGenebassContext(genes, ["a", "b", "c", "d", "e"], warnings)).rejects.toThrow("at most 4");
  expect(genebassGeneBurden).not.toHaveBeenCalled();
  expect((await fetchGenebassContext(new Map(Array.from({ length: 26 }, (_, i) => [`G${i}`, `ENSG${i}`])), ["pLoF"], warnings)).rows).toHaveLength(25);
  expect(genebassGeneBurden).toHaveBeenCalledTimes(25);
  expect(warnings).toEqual(["Genebass context is limited to the first 25 candidate genes"]);
});

it("makes no requests or truncation warnings when Genebass is disabled", async () => {
  const warnings: string[] = [];
  expect(await fetchGenebassContext(new Map(Array.from({ length: 26 }, (_, i) => [`G${i}`, `ENSG${i}`])), [], warnings)).toEqual({ rows: [], evidence: [] });
  expect(genebassGeneBurden).not.toHaveBeenCalled();
  expect(warnings).toEqual([]);
});
