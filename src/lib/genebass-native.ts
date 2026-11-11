/** Provider client maps Pvalue to SKAT-O; burden and SKAT are distinct tests.
 * Keep the composite phenotype ID separate from a human-readable description. */
export function projectGenebassPhewas(data: Record<string, unknown>) {
  if (!Array.isArray(data.phewas)) throw new Error("Unrecognized Genebass phewas array");
  return data.phewas.map(row => {
    if (!row || typeof row !== "object" || !("Pvalue" in row)) throw new Error("Malformed Genebass association");
    const fields = [row.trait_type, row.phenocode, row.pheno_sex, row.coding, row.modifier];
    if (fields.some(value => typeof value !== "string")) throw new Error("Malformed Genebass phenotype identity");
    return { skat_o_pvalue: row.Pvalue, phenotype_description: null, phenotype_id: fields.join("-") };
  });
}
