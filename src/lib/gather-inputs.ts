import { z } from "zod";
import type { GtexDataset } from "./gtex-observation";
import type { FinnGenRelease } from "./finngen-context";
import { eqtlCatalogueSchema } from "./eqtl-catalogue-input";
import { assertCatalogueSelection, eqtlR8Schema } from "./eqtl-r8-input";
import { finnGenPhenocode } from "./finngen-phenotype";

const burdenSetsSchema = z.array(z.string().trim().min(1).max(64)).max(4)
	.default(["pLoF", "missense|LC"]);

export const inputSchema = {
	trait_query: z.string().optional().describe("Trait label; auto-selection requires a unique exact OLS label. Otherwise provide efo_id."),
	efo_id: z.string().regex(/^[A-Za-z][A-Za-z0-9]*[_:]\d+$/).optional().describe("Explicit selected ontology ID (e.g. 'EFO_0001360'); takes precedence over trait_query."),
	seed_rsids: z.array(z.string()).optional().describe("Seed lead rsIDs"),
	target_gene: z.string().optional().describe("Optional gene of interest for highlighting"),
	show_child_traits: z.boolean().default(true).optional().describe("Include descendant ontology traits in GWAS Catalog anchor discovery; does not relax exact trait checks for scoring."),
	phenotype_terms: z.array(z.string()).optional(),
	max_loci: z.number().int().positive().max(25).default(8).optional()
		.describe("Max loci to analyse (synchronous cap; full-scope mode deferred)"),
	max_genes_per_locus: z.number().int().positive().max(10).default(5).optional(),
	max_anchor_associations: z.number().int().positive().max(1200).default(300).optional(),
	max_coloc_rows_per_locus: z.number().int().positive().max(200).default(100).optional(),
	max_eqtl_rows_per_variant: z.number().int().positive().max(500).default(200).optional(),
	gtex_dataset_id: z.enum(["gtex_v8", "gtex_v10"]).default("gtex_v8").optional().describe("Explicit GTEx dataset; v8 preserves the default, v10 is opt-in. Inspect gtex_evidence for native page evidence."),
	finngen_release: z.enum(["R12", "R13"]).optional().describe("Optional release-specific variant context for resolved GRCh38 anchors; omitted by default. Native observations stage in finngen_evidence and do not affect gene scores."),
	finngen_phenocode: finnGenPhenocode.optional().describe("Explicit native FinnGen phenotype code, such as T2D; requires finngen_release. Joins exact native alleles and the selected definition/association, without asserting EFO equivalence or changing gene scores."),
	eqtl_catalogue: eqtlCatalogueSchema.optional().describe("Optional one-dataset/one-anchor r7 QTL context. Discover dataset and metadata/file-map hashes with eqtl_execute first. Requires exact resolved GRCh38 alleles; scoring additionally requires fine_mapping_sha256 and qualified native fine mapping."),
	eqtl_catalogue_r8: eqtlR8Schema.optional().describe("Optional checksum-pinned r8_beta credible-set context for one dataset/anchor, exclusive with eqtl_catalogue. Requires configured EQTL_PARQUET_URL/TOKEN and an imported snapshot. Qualified score support requires score_fine_mapping=true."),
	genebass_burden_sets: burdenSetsSchema.describe("Up to 4 burden sets; duplicates are removed. Empty array disables Genebass context. This cross-phenotype context does not contribute to gene scores."),
	include_clinvar: z.boolean().default(false).optional().describe("Optional rsID-related ClinVar context. Preserves native search/summary evidence; not an exact allele or trait match and does not affect gene scores."),
	include_gnomad_context: z.boolean().default(false).optional(),
	include_hpa_tissue_context: z.boolean().default(false).optional(),
	disable_default_seeds: z.boolean().default(false).optional(),
};

export function gatherInputs(args: Record<string, unknown>) {
  assertCatalogueSelection(args);
  const gtexDataset = args.gtex_dataset_id ?? "gtex_v8";
  if (gtexDataset !== "gtex_v8" && gtexDataset !== "gtex_v10") throw new Error("gtex_dataset_id must be gtex_v8 or gtex_v10");
  const finngenRelease = args.finngen_release;
  if (finngenRelease !== undefined && finngenRelease !== "R12" && finngenRelease !== "R13") throw new Error("finngen_release must be R12 or R13");
  const phenocode = args.finngen_phenocode === undefined ? undefined : finnGenPhenocode.parse(args.finngen_phenocode);
  if (phenocode && !finngenRelease) throw new Error("finngen_phenocode requires finngen_release");
  return {
    trait_query: (args.trait_query as string | undefined) ?? null,
    efo_id: (args.efo_id as string | undefined) ?? null,
    show_child_traits: z.boolean().parse(args.show_child_traits ?? true),
    seed_rsids: (args.seed_rsids as string[] | undefined) ?? [],
    target_gene: (args.target_gene as string | undefined) ?? null,
    max_loci: Number(args.max_loci ?? 8),
    max_genes_per_locus: Number(args.max_genes_per_locus ?? 5),
    max_anchor_associations: Number(args.max_anchor_associations ?? 300),
    max_coloc_rows_per_locus: Number(args.max_coloc_rows_per_locus ?? 100),
    max_eqtl_rows_per_variant: Number(args.max_eqtl_rows_per_variant ?? 200),
    gtex_dataset_id: gtexDataset as GtexDataset,
    finngen_release: finngenRelease as FinnGenRelease | undefined,
    finngen_phenocode: phenocode,
    eqtl_catalogue: args.eqtl_catalogue === undefined ? undefined : eqtlCatalogueSchema.parse(args.eqtl_catalogue),
    eqtl_catalogue_r8: args.eqtl_catalogue_r8 === undefined ? undefined : eqtlR8Schema.parse(args.eqtl_catalogue_r8),
    genebass_burden_sets: [...new Set(burdenSetsSchema.parse(args.genebass_burden_sets))],
    include_clinvar: Boolean(args.include_clinvar ?? false),
    include_gnomad_context: Boolean(args.include_gnomad_context ?? false),
    include_hpa_tissue_context: Boolean(args.include_hpa_tissue_context ?? false),
    disable_default_seeds: Boolean(args.disable_default_seeds ?? false),
  };
}
