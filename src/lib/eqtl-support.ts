import { ontologyKey, type CatalogueSupport } from "./eqtl-catalogue-qualified";

interface GtexSupport { tissues: Set<string>; references: Record<string, unknown>[] }
const labelKey = (value: string) => value.toLowerCase().replace(/[^a-z0-9]/g, "");

/** Use exact native ontology/sample-group identities; unresolved overlap adds no tissue. */
export function mergeEqtlSupport(gtex?: GtexSupport, catalogue?: CatalogueSupport) {
  const tissues = new Set(gtex?.tissues ?? []);
  const references = [...(gtex?.references ?? [])];
  if (!catalogue) return { tissues, references };
  const gtexReferences = gtex?.references ?? [];
  const sameOntology = gtexReferences.some(row => ontologyKey(row.tissue_ontology_id) === catalogue.tissue_id);
  const sameGtexGroup = /^(?:GTEx|GTEx_v8|GTEx_v10)$/.test(catalogue.study_label) && [...tissues].some(tissue => labelKey(tissue) === labelKey(catalogue.sample_group));
  const knownGtexOntologies = gtexReferences.length > 0 && gtexReferences.every(row => ontologyKey(row.tissue_ontology_id) !== null);
  const count = tissues.size === 0 || (!sameOntology && !sameGtexGroup && knownGtexOntologies);
  if (count) tissues.add(`Catalogue:${catalogue.tissue_id}`);
  const basis = count ? "distinct_observed_tissue_context" : sameOntology || sameGtexGroup ? "already_counted_tissue" : "cross_source_tissue_overlap_unresolved";
  references.push(...catalogue.references.map(row => ({ ...row, adds_tissue: count, tissue_count_basis: basis })));
  return { tissues, references };
}
