import type { EqtlCatalogueNative } from "./eqtl-catalogue-synthesis";
import type { EqtlCatalogueObservation } from "./eqtl-catalogue-context";
import { gtexGeneKey } from "./gtex-qualified";

export const CATALOGUE_SCORE_POLICY = { version: "2026-10-06-susie", min_pip: 0.95, min_cs_r2: 0.25 } as const;
export const ontologyKey = (value: unknown) => typeof value === "string" && /^[A-Z][A-Z0-9]*[:_]\d+$/.test(value) ? value.replace("_", ":") : null;
const finite = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);
const geneKey = (value: unknown) => typeof value === "string" && /^ENSG\d{11}(?:\.\d+)?$/.test(value) ? value.split(".")[0] : null;

export interface CatalogueSupport { tissue_id: string; sample_group: string; study_label: string; references: Record<string, unknown>[] }

/** High-PIP native molQTL support, with no inference from nominal p-values alone. */
export function qualifiedCatalogue(native: EqtlCatalogueNative | null, observation: EqtlCatalogueObservation, indexes: number[], dataAccessId: string) {
  const support = new Map<string, CatalogueSupport>();
  const table = native?.fine_mapping?.status === "observed" ? native.fine_mapping.result : null;
  if (!native || !table) return support;
  const dataset = native.dataset; const tissue = ontologyKey(dataset.tissue_id);
  if (!tissue || !dataset.sample_group || !dataset.tissue_label) return support;
  const seen = new Set<string>();
  for (let index = 0; index < table.rows.length; index++) {
    const fine = table.rows[index]; const gene = geneKey(fine.gene_id);
    if (!gene || typeof fine.molecular_trait_id !== "string" || !fine.molecular_trait_id || typeof fine.cs_id !== "string" || !fine.cs_id
      || !finite(fine.pip) || fine.pip < CATALOGUE_SCORE_POLICY.min_pip || fine.pip > 1
      || !finite(fine.cs_min_r2) || fine.cs_min_r2 < CATALOGUE_SCORE_POLICY.min_cs_r2 || fine.cs_min_r2 > 1
      || !Number.isSafeInteger(fine.cs_size) || Number(fine.cs_size) < 1) continue;
    const matches = matchingNominalRows(indexes, native, gene, fine);
    if (!matches.length) continue;
    // rsID aliases are duplicate source rows, not additional tissue observations.
    const identity = JSON.stringify([gene, fine.molecular_trait_id, fine.cs_id, fine.variant]);
    if (seen.has(identity)) continue;
    seen.add(identity);
    const key = gtexGeneKey({ lead_rsid: observation.rsid, ensembl_id: gene });
    const current = support.get(key) ?? { tissue_id: tissue, sample_group: dataset.sample_group, study_label: dataset.study_label, references: [] };
    appendCatalogueReference(current, dataAccessId, observation, table, native, index, matches, fine, gene, dataset, tissue);
    support.set(key, current);
  }
  return support;
}

function appendCatalogueReference(current: CatalogueSupport, dataAccessId: string, observation: EqtlCatalogueObservation, table: NonNullable<NonNullable<EqtlCatalogueNative["fine_mapping"]>["result"]>, native: EqtlCatalogueNative, index: number, matches: number[], fine: Record<string, unknown>, gene: string, dataset: EqtlCatalogueNative["dataset"], tissue: string) {
  current.references.push({
    source_data_access_id: dataAccessId, source_table: "eqtl_catalogue_evidence", response_sha256: observation.response_sha256,
    source_url: table.source_file, nominal_source_url: native.region.source_file, retrieved_at: observation.retrieved_at,
    source_row_index: index, nominal_row_indexes: matches, native_fine_mapping_row: fine, lead_rsid: observation.rsid,
    ensembl_id: gene, variant_id: observation.requested_variant, genome_assembly: "GRCh38", release_requested: "r7",
    dataset_id_requested: dataset.dataset_id, study_id: dataset.study_id, sample_group: dataset.sample_group,
    tissue: dataset.tissue_label, tissue_ontology_id: tissue, condition: dataset.condition_label, quant_method: dataset.quant_method,
    credible_set_file_sha256: table.file_evidence.sha256, metadata_sha256: observation.metadata_sha256_requested,
    paths_sha256: observation.paths_sha256_requested, qualification_policy: CATALOGUE_SCORE_POLICY,
    effect_allele_basis: "ALT; native effects unchanged", coverage_complete: false, release_pinned: false,
    evidence_type: "fine-mapped molecular cis-QTL; not trait colocalisation or independent replication"
  });
}

function matchingNominalRows(indexes: number[], native: EqtlCatalogueNative, gene: string, fine: Record<string, unknown>) {
  return indexes.filter(i => {
    const nominal = native.region.rows[i];
    return geneKey(nominal.gene_id) === gene && nominal.gene_id === fine.gene_id && nominal.molecular_trait_id === fine.molecular_trait_id
      && finite(nominal.beta) && finite(nominal.pvalue) && nominal.pvalue >= 0 && nominal.pvalue <= 1
      && ["pvalue", "beta", "se"].every(field => fine[field] === undefined || fine[field] === nominal[field]);
  });
}
