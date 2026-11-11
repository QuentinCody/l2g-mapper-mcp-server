import { finnGenRecord } from "./finngen-phenotype";

const chromosome = (value: unknown): string | null => {
  if (typeof value !== "string" && typeof value !== "number") return null;
  const text = String(value);
  if (text === "23") return "X";
  return /^(?:[1-9]|1\d|2[0-2]|X|Y)$/.test(text) ? text : null;
};

/** Compare explicit native GRCh38 coordinates/alleles. No strand/indel normalization. */
export function finnGenVariantIdentity(native: unknown, requested: string | null) {
  const value = finnGenRecord(finnGenRecord(native).variant);
  const expected = requested?.match(/^([^:]+):(\d+)-([ACGT]+)-([ACGT]+)$/);
  if (!expected) return {verified: false, reason: "unresolved_anchor"};
  const fields = [value.chr, value.chrom].filter(v => v !== undefined);
  if (!fields.length || !Number.isSafeInteger(value.pos) || !value.ref || !value.alt) return {verified: false, reason: "missing_native_identity"};
  if (fields.some(c => chromosome(c) !== expected[1]) || value.pos !== Number(expected[2]) || value.ref !== expected[3] || value.alt !== expected[4]) {
    return {verified: false, reason: "native_identity_mismatch"};
  }
  if (value.varid !== undefined) {
    const parts = typeof value.varid === "string" ? value.varid.split(":") : [];
    if (parts.length !== 4 || chromosome(parts[0]) !== expected[1] || parts[1] !== expected[2] || parts[2] !== expected[3] || parts[3] !== expected[4]) {
      return {verified: false, reason: "native_varid_mismatch"};
    }
  }
  return {verified: true, reason: "exact_native_coordinates_and_alleles"};
}

/** Exact native phenocode join, with no claim of EFO or clinical equivalence. */
export function joinFinnGenPhenotype(native: unknown, variantVerified: boolean, code: string, definition: Record<string, unknown> | null) {
  const payload = finnGenRecord(native);
  const associations = Array.isArray(payload.results) ? payload.results : [];
  const indexes = associations.flatMap((value, index) => finnGenRecord(value).phenocode === code ? [index] : []);
  const association = indexes.length === 1 ? finnGenRecord(associations[indexes[0]]) : null;
  const validStats = association && typeof association.pval === "number" && Number.isFinite(association.pval) && association.pval >= 0 && association.pval <= 1 &&
    typeof association.beta === "number" && Number.isFinite(association.beta);
  const reason = !variantVerified ? "variant_identity_unverified" : !definition ? "definition_unavailable" : definition.phenocode !== code ? "definition_phenocode_mismatch" :
    indexes.length === 0 ? "phenocode_not_returned" : indexes.length !== 1 ? "ambiguous_phenocode" :
    association?.phenostring !== definition.phenostring ? "phenotype_label_mismatch" : !validStats ? "invalid_association_statistics" : "exact_native_phenocode";
  return {phenocode: code, joined: reason === "exact_native_phenocode", reason, association_row_indexes: indexes,
    association: reason === "exact_native_phenocode" ? association : null,
    phenotype_label: definition?.phenostring ?? null, trait_equivalence_verified: false,
    selection_basis: "explicit_finngen_phenocode", coverage_complete: false, score_contribution: 0};
}
