import { expect, it } from "vitest";
import { projectGwasAnchors } from "./gwas-anchors";

const association = (rsid: string, p: unknown, trait = "trait") => ({ snp_allele: [{ rs_id: rsid }], p_value: p, efo_traits: [{ efo_trait: trait }] });

it("retains the strongest same-SNP association and its own trait, independent of page order", () => {
  const rows = [association("rs1", 1e-6, "weaker"), association("RS1", 1e-12, "stronger"), association("rs2", 0)];
  for (const content of [rows, [...rows].reverse()]) {
    const result = projectGwasAnchors({ content }, 20, []);
    expect(result.find(row => row.rsid === "rs1")).toMatchObject({ p_value: 1e-12, lead_trait: "stronger" });
    expect(result.find(row => row.rsid === "rs2")?.p_value).toBe(0);
    expect(result).toHaveLength(2);
  }
});

it.each([-1, 2, Number.POSITIVE_INFINITY, Number.NaN, "1e-9", null, undefined])("keeps invalid/unknown p-values out of numeric ranking (%s)", p => {
  expect(projectGwasAnchors({ content: [association("rs1", p)] }, 20, [])[0].p_value).toBeNull();
});

it("does not assign a haplotype/interaction association to an arbitrary constituent SNP", () => {
  const content = [
    { ...association("rs1", 1e-9), multi_snp_haplotype: true },
    { ...association("rs2", 1e-9), snp_interaction: true },
    { snp_allele: [{ rs_id: "rs3" }, { rs_id: "rs4" }] },
    { rs_id: "rs5-rs6" }, { rs_id: "prefix-rs7" }, { rs_id: 8 },
    { snp_allele: [null] }, null,
    { rs_id: "rs9", snp_allele: [{ rs_id: "rs10" }] },
  ];
  const warnings: string[] = [];
  expect(projectGwasAnchors({ content }, 20, warnings)).toEqual([]);
  expect(warnings[0]).toContain("skipped 9");
});

it("keeps exact legacy rsIDs and repeated native alleles without extracting substrings", () => {
  expect(projectGwasAnchors({ _embedded: { associations: [
    { rs_id: "RS1", reported_trait: ["reported"] },
    { snp_allele: [{ rs_id: "rs2" }, { rs_id: "RS2" }], reported_trait: "label" },
  ] } }, 20, [])).toMatchObject([{ rsid: "rs1", lead_trait: "reported" }, { rsid: "rs2", lead_trait: "label" }]);
});

it.each([null, {}, { content: {} }, { content: null, page: { totalElements: 0 } }, { error: "upstream error", content: [] }, { content: [], _embedded: { associations: [] } }])("rejects malformed/error/ambiguous pages instead of reporting an empty search (%j)", data => {
  expect(() => projectGwasAnchors(data, 20, [])).toThrow("GWAS response");
});

it.each([{ content: [] }, { _embedded: { associations: [] } }, { page: { totalElements: 0 } }])("accepts an explicitly empty page (%j)", data => {
  expect(projectGwasAnchors(data, 20, [])).toEqual([]);
});

it("enforces the requested page cap and discloses partial ranking", () => {
  const warnings: string[] = [];
  expect(projectGwasAnchors({ content: [association("rs1", 0.1), association("rs2", 1e-9)] }, 1, warnings)).toMatchObject([{ rsid: "rs1" }]);
  expect(warnings.join(" ")).toContain("exceeded the 1-association cap");
  expect(warnings.join(" ")).toContain("not the complete trait association set");
});
