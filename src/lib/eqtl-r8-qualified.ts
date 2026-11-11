import { CATALOGUE_SCORE_POLICY, ontologyKey, type CatalogueSupport } from "./eqtl-catalogue-qualified";
import type { EqtlR8Observation } from "./eqtl-r8-context";
import type { loadEqtlR8Observation } from "./eqtl-r8-synthesis";
import { gtexGeneKey } from "./gtex-qualified";

const finite = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);
const text = (v: unknown): v is string => typeof v === "string" && v.trim().length > 0;

/** Native r8 credible-set files carry their own association statistics. */
export function qualifiedR8Catalogue(checked: Awaited<ReturnType<typeof loadEqtlR8Observation>>, dataAccessId: string) {
  const support = new Map<string, CatalogueSupport>();
  if (!checked?.input.score_fine_mapping || !checked.native) return support;
  const { native, row, input } = checked;
  const dataset = native.metadata; const tissue = ontologyKey(dataset.tissue_id);
  if (!tissue || !text(dataset.sample_group) || !text(dataset.study_label) || !text(dataset.tissue_label)) return support;
  const seen = new Set<string>();
  native.rows.forEach((fine, index) => {
    if (typeof fine.gene_id !== "string" || !/^ENSG\d{11}(?:\.\d+)?$/.test(fine.gene_id) || !text(fine.molecular_trait_id) || !text(fine.cs_id) ||
      !finite(fine.pip) || fine.pip < CATALOGUE_SCORE_POLICY.min_pip || fine.pip > 1 ||
      !finite(fine.cs_min_r2) || fine.cs_min_r2 < CATALOGUE_SCORE_POLICY.min_cs_r2 || fine.cs_min_r2 > 1 ||
      !Number.isSafeInteger(fine.cs_size) || Number(fine.cs_size) < 1 || !finite(fine.beta) || !finite(fine.se) || fine.se <= 0 ||
      !finite(fine.pvalue) || fine.pvalue < 0 || fine.pvalue > 1) return;
    const gene = fine.gene_id.split(".")[0];
    const identity = JSON.stringify([gene, fine.molecular_trait_id, fine.cs_id, fine.variant]);
    if (seen.has(identity)) return;
    seen.add(identity);
    const key = gtexGeneKey({ lead_rsid: row.rsid, ensembl_id: gene });
    const current = support.get(key) ?? { tissue_id: tissue, sample_group: dataset.sample_group as string, study_label: dataset.study_label as string, references: [] };
    appendR8Reference(current, dataAccessId, row, index, fine, gene, input, dataset, tissue);
    support.set(key, current);
  });
  return support;
}

function appendR8Reference(current: CatalogueSupport, dataAccessId: string, row: EqtlR8Observation, index: number, fine: Record<string, unknown>, gene: string, input: { release: "r8_beta"; dataset_id: string; rsid: string; metadata_sha256: string; file_sha256: string; max_rows: number; score_fine_mapping: boolean; }, dataset: Record<string, unknown>, tissue: string) {
  current.references.push({
    source_data_access_id: dataAccessId, source_table: "eqtl_catalogue_r8_evidence", response_sha256: row.response_sha256,
    source_url: row.source_url, retrieved_at: row.retrieved_at, source_row_index: index, native_fine_mapping_row: fine,
    lead_rsid: row.rsid, ensembl_id: gene, variant_id: row.requested_variant, genome_assembly: "GRCh38",
    release_requested: "r8_beta", release_status: "pre-release", dataset_id_requested: input.dataset_id, study_id: dataset.study_id,
    sample_group: dataset.sample_group, tissue: dataset.tissue_label, tissue_ontology_id: tissue, condition: dataset.condition_label,
    quant_method: dataset.quant_method, credible_set_file_sha256: input.file_sha256, metadata_sha256: input.metadata_sha256,
    qualification_policy: CATALOGUE_SCORE_POLICY, effect_allele_basis: "ALT; native effects unchanged", coverage_complete: false,
    snapshot_pinned: true, evidence_type: "fine-mapped molecular cis-QTL; not trait colocalisation or independent replication"
  });
}
