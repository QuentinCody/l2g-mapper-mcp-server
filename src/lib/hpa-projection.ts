function finiteExpression(value: unknown): number | null {
  const parsed = typeof value === "number" ? value : typeof value === "string" && /^\d+(?:\.\d+)?(?:e[+-]?\d+)?$/i.test(value) ? Number(value) : Number.NaN;
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

/** Recomputed from the full native record; request identity is not an observation. */
export function hpaProjection(native: unknown, id: string) {
  if (!native || typeof native !== "object" || Array.isArray(native)) throw new Error("HPA returned no gene object");
  const gene = native as Record<string, unknown>;
  if (gene.Ensembl !== id) throw new Error("HPA returned a missing or different Ensembl gene ID");
  const rna = gene["RNA tissue specific nTPM"];
  if (rna !== undefined && rna !== null && (typeof rna !== "object" || Array.isArray(rna))) throw new Error("HPA RNA tissue-specific nTPM has an unrecognized shape");
  const tissues = Object.entries(rna ?? {}).map(([name, value]) => ({ name, value: finiteExpression(value) }))
    .filter((entry): entry is { name: string; value: number } => entry.value !== null && entry.value >= 1)
    .sort((a, b) => b.value - a.value || a.name.localeCompare(b.name)).slice(0, 8).map(entry => entry.name);
  const locations = gene["Subcellular location"];
  const display = typeof locations === "string" ? locations : Array.isArray(locations) && locations.every(value => typeof value === "string") ? locations.join(" | ") : null;
  return {
    ensembl_id: id, observed_symbol: typeof gene.Gene === "string" ? gene.Gene : null,
    tissues: tissues.join(" | "), subcellular: display,
    rna_tissue_specific_ntpm_json: JSON.stringify(rna ?? null), subcellular_location_json: JSON.stringify(locations ?? null),
  };
}
