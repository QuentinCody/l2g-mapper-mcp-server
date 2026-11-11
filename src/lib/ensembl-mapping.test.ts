import { expect, it } from "vitest";
import { selectEnsemblMapping } from "./ensembl-mapping";

const mapping = (changes = {}) => ({ assembly_name: "GRCh38", coord_system: "chromosome", seq_region_name: "1", start: 100, end: 100, strand: 1, allele_string: "A/G", ...changes });
const native = (mappings: unknown[], extra = {}) => ({ name: "rs1", synonyms: [], mappings, ...extra });

it("selects a unique primary-chromosome mapping in the requested assembly, independently of order", () => {
  const mappings = [mapping({ assembly_name: "GRCh37", start: 80, end: 80, allele_string: "T/C" }), mapping({ seq_region_name: "HSCHR1_PATCH" }), mapping()];
  for (const rows of [mappings, [...mappings].reverse()]) {
    expect(selectEnsemblMapping(native(rows), "rs1", "GRCh38")).toEqual({ status: "resolved", coord: { chr: "1", pos: 100, ref: "A", alt: "G" } });
    expect(selectEnsemblMapping(native(rows), "rs1", "GRCh37")).toEqual({ status: "resolved", coord: { chr: "1", pos: 80, ref: "T", alt: "C" } });
  }
});

it("accepts a native alias without inferring equivalence between unrelated rsIDs", () => {
  expect(selectEnsemblMapping(native([mapping()], { name: "rs2", synonyms: ["rs1"] }), "rs1", "GRCh38").status).toBe("resolved");
  expect(selectEnsemblMapping(native([mapping()], { name: "rs2" }), "rs1", "GRCh38")).toEqual({ status: "identity_mismatch", coord: null });
});

it.each([{ start: 200, end: 200 }, { seq_region_name: "2" }, { allele_string: "A/T" }, { strand: -1 }])("rejects conflicting primary mappings %j", change => {
  const rows = [mapping(), mapping(change)];
  for (const mappings of [rows, [...rows].reverse()]) expect(selectEnsemblMapping(native(mappings), "rs1", "GRCh38")).toEqual({ status: "ambiguous_mapping", coord: null });
});

it("deduplicates only identical coordinate/strand/allele mappings", () => {
  expect(selectEnsemblMapping(native([mapping(), mapping({ annotation: "extra" })]), "rs1", "GRCh38").status).toBe("resolved");
});

it.each([{ start: 0 }, { start: 1.5 }, { start: "100" }, { end: undefined }, { end: 98 }, { start: Number.MAX_SAFE_INTEGER + 1 }])("rejects invalid coordinates %j", change => {
  expect(selectEnsemblMapping(native([mapping(change)]), "rs1", "GRCh38")).toEqual({ status: "invalid_coordinates", coord: null });
});

it.each([{ strand: -1 }, { strand: undefined }, { allele_string: "A/G/T" }, { allele_string: "A/A" }, { allele_string: "-/A", end: 99 }, { allele_string: "N/G" }, { allele_string: "A/<DEL>" }, { allele_string: "AA/G" }, { allele_string: null }])("keeps locus context but does not invent forward biallelic sequence alleles %j", change => {
  expect(selectEnsemblMapping(native([mapping(change)]), "rs1", "GRCh38")).toEqual({ status: "unresolved_alleles", coord: { chr: "1", pos: 100, ref: null, alt: null } });
});

it("retains explicit sequence alleles whose reference span is consistent", () => {
  expect(selectEnsemblMapping(native([mapping({ allele_string: "AT/A", end: 101 })]), "rs1", "GRCh38")).toMatchObject({ status: "resolved", coord: { ref: "AT", alt: "A" } });
});

it("does not borrow assemblies or reinterpret non-primary contigs", () => {
  for (const mappings of [[], [mapping({ assembly_name: "GRCh37" })], [mapping({ seq_region_name: "GL0001" })]]) {
    expect(selectEnsemblMapping(native(mappings), "rs1", "GRCh38")).toEqual({ status: "no_primary_mapping", coord: null });
  }
  for (const mappings of [[null], [mapping({ assembly_name: undefined })], [mapping({ coord_system: "contig" })]]) {
    expect(selectEnsemblMapping(native(mappings), "rs1", "GRCh38")).toEqual({ status: "invalid_response", coord: null });
  }
});

it("leaves the provider's documented multiallelic rs116035550 example unresolved", () => {
  const example = { name: "rs116035550", synonyms: [], mappings: [{ location: "11:212464-212464", start: 212464, allele_string: "G/A/C", assembly_name: "GRCh37", strand: 1, coord_system: "chromosome", ancestral_allele: "G", seq_region_name: "11", end: 212464 }] };
  expect(selectEnsemblMapping(example, "rs116035550", "GRCh37")).toEqual({ status: "unresolved_alleles", coord: { chr: "11", pos: 212464, ref: null, alt: null } });
});
