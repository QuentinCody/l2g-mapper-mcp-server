import type { GenomeBuild, VariantCoord } from "@bio-mcp/shared/variants/resolve";

type Mapping = Record<string, unknown>;
export interface MappingSelection {
  status: "resolved" | "identity_mismatch" | "invalid_response" | "no_primary_mapping" | "ambiguous_mapping" | "invalid_coordinates" | "unresolved_alleles";
  coord: VariantCoord | null;
}
const record = (value: unknown): Mapping => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Mapping : {};
const primaryChromosome = (value: unknown): value is string => typeof value === "string" && /^(?:[1-9]|1[0-9]|2[0-2]|X|Y|MT)$/.test(value);

function mappingCoordinate(mapping: Mapping): MappingSelection {
  const { start, end, seq_region_name: chr } = mapping;
  if (!primaryChromosome(chr) || typeof start !== "number" || !Number.isSafeInteger(start) || start <= 0 ||
      typeof end !== "number" || !Number.isSafeInteger(end) || end < start - 1) return { status: "invalid_coordinates", coord: null };
  const coord: VariantCoord = { chr, pos: start, ref: null, alt: null };
  const alleles = typeof mapping.allele_string === "string" ? mapping.allele_string.split("/") : [];
  // Ensembl's first allele is reference. Only forward, explicit sequence alleles
  // with a consistent reference span can be reused without normalization.
  if (mapping.strand !== 1 || alleles.length !== 2 || !alleles.every(allele => /^[ACGT]+$/.test(allele)) ||
      alleles[0] === alleles[1] || end - start + 1 !== alleles[0].length) return { status: "unresolved_alleles", coord };
  return { status: "resolved", coord: { ...coord, ref: alleles[0], alt: alleles[1] } };
}

/** Select by native identity and assembly, never by order of mappings. */
export function selectEnsemblMapping(native: unknown, rsid: string, build: GenomeBuild): MappingSelection {
  const value = record(native);
  const identities = [value.name, ...(Array.isArray(value.synonyms) ? value.synonyms : [])];
  if (!identities.includes(rsid)) return { status: "identity_mismatch", coord: null };
  if (!Array.isArray(value.mappings) || value.mappings.some(mapping => typeof record(mapping).assembly_name !== "string" || typeof record(mapping).seq_region_name !== "string")) {
    return { status: "invalid_response", coord: null };
  }
  const mappings = value.mappings.map(record).filter(mapping => mapping.assembly_name === build && primaryChromosome(mapping.seq_region_name));
  if (!mappings.length) return { status: "no_primary_mapping", coord: null };
  if (mappings.some(mapping => mapping.coord_system !== "chromosome")) return { status: "invalid_response", coord: null };
  const distinct = new Map(mappings.map(mapping => [JSON.stringify([mapping.seq_region_name, mapping.start, mapping.end, mapping.strand, mapping.allele_string]), mapping]));
  if (distinct.size !== 1) return { status: "ambiguous_mapping", coord: null };
  return mappingCoordinate(mappings[0]);
}
