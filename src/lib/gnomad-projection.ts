const object = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);
const finite = (value: unknown) => typeof value === "number" && Number.isFinite(value) ? value : null;

/** Project only the exact requested gene from a complete successful native envelope. */
export function gnomadProjection(native: unknown, requestedId: string) {
  if (!object(native)) throw new Error("Invalid gnomAD native response");
  if (native.errors !== undefined && (!Array.isArray(native.errors) || native.errors.length > 0)) throw new Error("GraphQL returned errors; native context retained but not qualified");
  const gene = object(native.data) ? native.data.gene : null;
  if (!object(gene)) throw new Error("No gene returned; constraint availability is unknown");
  if (gene.gene_id !== requestedId || gene.reference_genome !== "GRCh38") throw new Error("Returned gene ID or assembly does not match the request");
  const constraint = gene.gnomad_constraint;
  if (constraint != null && !object(constraint)) throw new Error("Invalid gnomAD native constraint object");
  const c = object(constraint) ? constraint : {};
  return {
    ensembl_id: requestedId, symbol: typeof gene.symbol === "string" ? gene.symbol : null,
    reference_genome: "GRCh38", canonical_transcript_id: typeof gene.canonical_transcript_id === "string" ? gene.canonical_transcript_id : null,
    mane_select_transcript_json: JSON.stringify(gene.mane_select_transcript ?? null),
    gene_flags_json: JSON.stringify(gene.flags ?? null), constraint_flags_json: JSON.stringify(c.flags ?? null),
    oe_lof: finite(c.oe_lof), oe_lof_lower: finite(c.oe_lof_lower), oe_lof_upper: finite(c.oe_lof_upper),
    oe_lof_percentile: finite(c.oe_lof_percentile), lof_z: finite(c.lof_z), mis_z: finite(c.mis_z), pli: finite(c.pli),
    constraint_available: constraint == null ? (constraint === null ? false : null) : true,
  };
}
