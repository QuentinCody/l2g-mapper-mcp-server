import { expect, it } from "vitest";
import { indexOpenTargetsAnchors, matchOpenTargetsLoci, openTargetsVariantId } from "./opentargets-matching";

const coord = { chr: "chr1", pos: 10, ref: "a", alt: "g" };
it("matches exact GRCh38 alleles without requiring an rsID alias from the provider", () => {
  const index = indexOpenTargetsAnchors([{ rsid: "RS1", grch38: coord }, { rsid: "rs2", grch38: coord }], []);
  expect(matchOpenTargetsLoci([{ studyLocusId: "L", variant: { id: "1_10_A_G" } }], index, []).get("L"))
    .toEqual({ variantId: "1_10_A_G", rsids: new Set(["rs1", "rs2"]) });
});

it.each(["1_10_A_T", "1_11_A_G", "2_10_A_G", "1_10_C_G", "OTVAR_HASH", undefined])("does not use rsID overlap to rescue variant %s", id => {
  const index = indexOpenTargetsAnchors([{ rsid: "rs1", grch38: coord }], []);
  expect(matchOpenTargetsLoci([{ studyLocusId: "L", variant: { id, rsIds: ["rs1"] } }], index, []).size).toBe(0);
});

it.each([
  null, { ...coord, alt: null }, { ...coord, alt: "G,T" }, { ...coord, alt: "<DEL>" },
  { ...coord, ref: "N" }, { ...coord, alt: "a" }, { ...coord, pos: 0 }, { ...coord, pos: 1.5 },
  { ...coord, chr: "contig1" },
])("excludes unresolved or unsupported coordinates %#", value => {
  expect(openTargetsVariantId(value)).toBeNull();
});

it("does not fall back to GRCh37 and snapshots coordinates before asynchronous work", () => {
  const warnings: string[] = [];
  const missing = { rsid: "rs0", grch37: coord };
  const resolved = { rsid: "rs1", grch38: { ...coord } };
  const index = indexOpenTargetsAnchors([missing, resolved], warnings);
  resolved.grch38.alt = "T";
  expect([...index.keys()]).toEqual(["1_10_A_G"]);
  expect(warnings.join(" ")).toContain("rs0: unambiguous GRCh38");
});

it.each([false, true])("rejects contradictory discovery rows regardless of order (%s)", reverse => {
  const index = indexOpenTargetsAnchors([{ rsid: "rs1", grch38: coord }], []);
  const rows = ["1_10_A_G", "1_10_A_T"].map(id => ({ studyLocusId: "L", variant: { id } }));
  const warnings: string[] = [];
  expect(matchOpenTargetsLoci(reverse ? rows.reverse() : rows, index, warnings).size).toBe(0);
  expect(warnings).toEqual([expect.stringContaining("conflicting lead variant IDs")]);
});
