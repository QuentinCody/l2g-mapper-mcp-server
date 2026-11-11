import type { gatherInputs } from "./gather-inputs";

/** Conservative fan-out estimate before upstream requests start. */
export function projectedSubrequests(parsed: ReturnType<typeof gatherInputs>, seedCount: number) {
  const anchors = Math.min(parsed.max_anchor_associations, Math.max(seedCount, parsed.max_loci * 4));
  const genes = parsed.max_loci * parsed.max_genes_per_locus;
  const coordinates = anchors * 2;
  const openTargets = 16 + Math.ceil(anchors / 40) * 3 + Math.ceil(parsed.max_loci / 5);
  const gtex = Math.max(1, Math.ceil(anchors / 40));
  const clinvar = parsed.include_clinvar ? Math.min(anchors, 30) + 1 : 0;
  const gnomad = parsed.include_gnomad_context ? genes : 0;
  const hpa = parsed.include_hpa_tissue_context ? genes : 0;
  const genebass = genes * parsed.genebass_burden_sets.length;
  const finngen = parsed.finngen_release ? parsed.max_loci + (parsed.finngen_phenocode ? 1 : 0) : 0;
  const eqtlCatalogue = parsed.eqtl_catalogue ? 5 + (parsed.eqtl_catalogue.fine_mapping_sha256 ? 1 : 0) : 0; // Metadata, tabix and optional complete credible-set file; no retries.
  return 2 + coordinates + openTargets + gtex + clinvar + gnomad + hpa + genebass + finngen + eqtlCatalogue + (parsed.eqtl_catalogue_r8 ? 1 : 0);
}
