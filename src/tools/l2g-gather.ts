/**
 * l2g_gather — Tier 1 of the 3-tier synchronous L2G composer.
 *
 * Fan-out strategy (inside a ~30s Worker budget):
 *   Stage 1 (sequential, small): EFO resolve (OLS4) + GWAS anchor fetch.
 *   Stage 2 (parallel): coordinate normalization for all anchors via
 *                       Promise.all(resolveRsid…), Promise.race against 7s.
 *   Stage 3 (parallel): OpenTargets batched GraphQL + GTEx batch eQTL
 *                       + optional ClinVar / gnomAD / HPA, Promise.race 15s.
 *   Stage 4 (parallel): Genebass burden for the candidate genes.
 *
 * Projects a subrequest count up front; rejects if >800 to leave headroom
 * below Workers' 1000-subrequest limit.
 *
 * Direct upstream HTTP — does NOT call other MCP servers. Uses the shared
 * adapters in `packages/mcp-shared/src/adapters/`.
 */

import type { McpServer } from "@bio-mcp/shared/mcp";
import { inputSchema, gatherInputs } from "../lib/gather-inputs";
import { projectedSubrequests } from "../lib/gather-budget";
import {
	createCodeModeError,
} from "@bio-mcp/shared/codemode/response";
import { selectCandidateGenes, type CandidateGene, type CandidateSelection } from "../lib/candidate-genes";
import { withDeadline } from "../lib/with-deadline";
import { stageGatherResult } from "../lib/gather-result";
import { l2gResponse } from "../lib/tool-result";
import type { CoordinateObservation } from "../lib/coordinates";
import { gatherCoordinates } from "../lib/gather-coordinates";
import { fetchGtexEqtls, type EqtlRow } from "../lib/gtex-evidence";
import type { GtexObservation } from "../lib/gtex-observation";
import { resolveTrait, type TraitResolution } from "../lib/trait-resolution";
import { traitId } from "../lib/opentargets-traits";
import { gwasAssociations } from "@bio-mcp/shared/adapters/gwas_catalog";
import { projectGwasAnchors } from "../lib/gwas-anchors";
import { fetchOpenTargetsBundle, applyOpenTargetsBundle, type L2gPredictionRow, type ColocRow, type OpenTargetsEvidenceRow } from "../lib/opentargets-evidence";
import { fetchClinvarLane, type ClinvarRow } from "../lib/clinvar-context";
import { CLINVAR_CONTEXT_VERSION, type ClinvarObservation } from "../lib/clinvar-observation";
import { fetchGnomadLane, GNOMAD_CONTEXT_VERSION, type GnomadRow } from "../lib/gnomad-context";
import { fetchHpaLane, HPA_CONTEXT_VERSION, type HpaRow } from "../lib/hpa-context";
import { fetchGenebassContext, type BurdenRow } from "../lib/genebass-context";
import { GENEBASS_CONTEXT_VERSION, type GenebassObservation } from "../lib/genebass-observation";
import { fetchBurdenEvidence } from "../lib/burden-evidence";
import { fetchFinnGenContext, type FinnGenObservation } from "../lib/finngen-context";
import { fetchFinnGenPhenotype, FINNGEN_JOIN_VERSION, type FinnGenPhenotypeObservation } from "../lib/finngen-phenotype";
import { fetchEqtlCatalogueContext, type EqtlCatalogueObservation } from "../lib/eqtl-catalogue-context";
import { EQTL_CATALOGUE_CONTEXT_VERSION } from "../lib/eqtl-catalogue-input";
import { fetchEqtlR8Context, type EqtlR8Observation } from "../lib/eqtl-r8-context";
import { EQTL_R8_CONTEXT_VERSION } from "../lib/eqtl-r8-input";
import type { ParquetEnv } from "@bio-mcp/shared/adapters/eqtl/parquet";
import { lookupDefaultSeeds } from "../lib/well-known-traits";
interface GatherEnv extends ParquetEnv {
	L2G_DATA_DO?: DurableObjectNamespace;
}


interface Anchor {
	rsid: string;
	source: "seed" | "gwas";
	p_value: number | null;
	lead_trait: string | null;
	cohort: string | null;
	grch38?: { chr: string; pos: number; ref: string | null; alt: string | null } | null;
	grch37?: { chr: string; pos: number; ref: string | null; alt: string | null } | null;
	locus_id?: string;
}

interface Locus {
	locus_id: string;
	lead_rsid: string;
	chr: string | null;
	start: number | null;
	end: number | null;
}



interface GatherBundle {
	meta: {
		trait_query: string | null;
		efo_id: string | null;
		efo_label: string | null;
		generated_at: string;
		sources_queried: string[];
		caps: Record<string, unknown>;
	};
	tables: {
		trait_resolution: TraitResolution[];
		coordinate_resolution: CoordinateObservation[];
		anchors: Anchor[];
		loci: Locus[];
		candidate_genes: CandidateGene[];
		candidate_selection: CandidateSelection[];
		l2g_predictions: L2gPredictionRow[];
		opentargets_evidence: OpenTargetsEvidenceRow[];
		coloc: ColocRow[];
		eqtl: EqtlRow[];
		gtex_evidence: GtexObservation[];
		finngen_evidence: FinnGenObservation[];
		finngen_phenotype_evidence: FinnGenPhenotypeObservation[];
		eqtl_catalogue_evidence: EqtlCatalogueObservation[];
		eqtl_catalogue_r8_evidence: EqtlR8Observation[];
		burden: BurdenRow[];
		genebass_evidence: GenebassObservation[];
		clinvar: ClinvarRow[];
		clinvar_evidence: ClinvarObservation[];
		gnomad: GnomadRow[];
		hpa: HpaRow[];
	};
	warnings: string[];
}

const LOCUS_PADDING_BP = 1_000_000;

async function fetchGwasAnchors(
	efoId: string | null,
	traitQuery: string | null,
	size: number,
	showChildTraits: boolean,
	warnings: string[],
): Promise<Anchor[]> {
	if (!efoId && !traitQuery) return [];
	try {
		const data = await gwasAssociations({
			efoId: efoId ?? undefined,
			traitName: efoId ? undefined : traitQuery ?? undefined,
			showChildTraits,
			size,
		});
		return projectGwasAnchors(data, size, warnings);
	} catch (err) {
		warnings.push(`GWAS Catalog anchor fetch failed: ${err instanceof Error ? err.message : String(err)}`);
		return [];
	}
}

// ── Optional lanes ──────────────────────────────────────────────────────

// ── Genebass burden (stage 4) ───────────────────────────────────────────
// ── Main tool ───────────────────────────────────────────────────────────

function createGatherBundle(parsed: ReturnType<typeof gatherInputs>, projected: number): GatherBundle {
	return {
		meta: {
			trait_query: parsed.trait_query,
			efo_id: parsed.efo_id,
			efo_label: null,
			generated_at: new Date().toISOString(),
			sources_queried: [],
			caps: {
				max_loci: parsed.max_loci,
				max_genes_per_locus: parsed.max_genes_per_locus,
				max_anchor_associations: parsed.max_anchor_associations,
				show_child_traits: parsed.show_child_traits,
				max_coloc_rows_per_locus: parsed.max_coloc_rows_per_locus,
				max_eqtl_rows_per_variant: parsed.max_eqtl_rows_per_variant,
				gtex_dataset_id: parsed.gtex_dataset_id,
				finngen_release: parsed.finngen_release ?? null,
				finngen_phenocode: parsed.finngen_phenocode ?? null,
				finngen_join_version: FINNGEN_JOIN_VERSION,
				eqtl_catalogue: parsed.eqtl_catalogue ?? null,
				eqtl_catalogue_context_version: EQTL_CATALOGUE_CONTEXT_VERSION,
				eqtl_catalogue_r8: parsed.eqtl_catalogue_r8 ?? null,
				eqtl_catalogue_r8_context_version: EQTL_R8_CONTEXT_VERSION,
				include_clinvar: parsed.include_clinvar,
				clinvar_context_version: CLINVAR_CONTEXT_VERSION,
				include_gnomad_context: parsed.include_gnomad_context,
				gnomad_context_version: GNOMAD_CONTEXT_VERSION,
				include_hpa_tissue_context: parsed.include_hpa_tissue_context,
				hpa_context_version: HPA_CONTEXT_VERSION,
				genebass_context_version: GENEBASS_CONTEXT_VERSION,
				projected_subrequests: projected,
			},
		},
		tables: {
			trait_resolution: [],
			coordinate_resolution: [],
			anchors: [],
			loci: [],
			candidate_genes: [],
			candidate_selection: [],
			l2g_predictions: [],
			opentargets_evidence: [],
			coloc: [],
			eqtl: [],
			gtex_evidence: [],
			finngen_evidence: [],
			finngen_phenotype_evidence: [],
			eqtl_catalogue_evidence: [],
			eqtl_catalogue_r8_evidence: [],
			burden: [],
			genebass_evidence: [],
			clinvar: [],
			clinvar_evidence: [],
			gnomad: [],
			hpa: [],
		},
		warnings: [],
	};
}

export function registerL2gGather(server: McpServer, env?: GatherEnv): void {
	const handler = async (args: Record<string, unknown>, extra: unknown) => {
		const runtimeEnv = env || (extra as { env?: GatherEnv })?.env;
		const warnings: string[] = [];
		const startedAt = Date.now();

		try {
			const parsed = gatherInputs(args);

			// Validate input
			if (parsed.efo_id !== null && !traitId(parsed.efo_id)) return createCodeModeError("INVALID_ARGUMENTS", "efo_id must be an ontology ID such as EFO_0001360 or EFO:0001360.");
			let seedRsids = parsed.seed_rsids.slice();
			const hasAnyAnchor = parsed.trait_query || parsed.efo_id || seedRsids.length > 0;
			if (!hasAnyAnchor) {
				return createCodeModeError(
					"INVALID_ARGUMENTS",
					"l2g_gather requires at least one of: trait_query, efo_id, or seed_rsids.",
				);
			}
			// Default seeds fallback when trait is known
			if (parsed.trait_query && seedRsids.length === 0 && !parsed.disable_default_seeds) {
				const defaults = lookupDefaultSeeds(parsed.trait_query);
				if (defaults.length > 0) {
					seedRsids = defaults;
					warnings.push(`Using built-in default seeds for trait '${parsed.trait_query}'`);
				}
			}

			const projected = projectedSubrequests(parsed, seedRsids.length);
			if (projected > 800) {
				return createCodeModeError(
					"INVALID_ARGUMENTS",
					`Projected subrequest count (${projected}) exceeds the 800 Worker-subrequest budget. Reduce max_loci, max_genes_per_locus, max_anchor_associations, or drop optional lanes (include_clinvar/include_gnomad_context/include_hpa_tissue_context).`,
				);
			}

			const bundle = createGatherBundle(parsed, projected);

			// ── Stage 1: EFO resolve + GWAS anchor fetch ──
			const resolution = await resolveTrait(parsed.trait_query, parsed.efo_id, warnings);
			const { efo_id: efoId, label: efoLabel } = resolution;
			bundle.tables.trait_resolution = [resolution];
			if (parsed.trait_query && !parsed.efo_id) bundle.meta.sources_queried.push("ols4");
			bundle.meta.efo_id = efoId;
			bundle.meta.efo_label = efoLabel;

			const gwasAnchors = parsed.max_anchor_associations > 0
				? await fetchGwasAnchors(efoId, parsed.trait_query, Math.min(parsed.max_anchor_associations, 300), parsed.show_child_traits, warnings)
				: [];
			if (gwasAnchors.length > 0) bundle.meta.sources_queried.push("gwas_catalog");

			// Merge seed + GWAS anchors, cap to max_loci
			const merged = new Map<string, Anchor>();
			for (const rsid of seedRsids) {
				merged.set(rsid.toLowerCase(), {
					rsid: rsid.toLowerCase(),
					source: "seed",
					p_value: null,
					lead_trait: parsed.trait_query,
					cohort: null,
				});
			}
			for (const a of gwasAnchors) {
				if (!merged.has(a.rsid)) merged.set(a.rsid, a);
			}
			// Rank: seeds first, then GWAS by p_value
			const anchorsList = [...merged.values()].sort((a, b) => {
				if (a.source === "seed" && b.source !== "seed") return -1;
				if (b.source === "seed" && a.source !== "seed") return 1;
				return (a.p_value ?? 1) - (b.p_value ?? 1);
			}).slice(0, parsed.max_loci);

			if (anchorsList.length === 0) {
				warnings.push("No anchors after normalization; nothing to gather.");
				return createCodeModeError(
					"NOT_FOUND",
					"No anchors remained after normalization. Provide seed_rsids or try a different trait_query.",
					{ trait_resolution: resolution, warnings },
				);
			}

			// ── Stage 2: Coordinate normalization (parallel, 7s deadline) ──
			bundle.meta.sources_queried.push("ensembl");
			bundle.tables.coordinate_resolution = await gatherCoordinates(anchorsList, warnings);

			// Build loci rows
			for (const a of anchorsList) {
				const g38 = a.grch38;
				let locus_id: string;
				let chr: string | null = null;
				let start: number | null = null;
				let end: number | null = null;
				if (g38?.chr && g38.pos) {
					chr = String(g38.chr);
					start = Math.max(1, g38.pos - LOCUS_PADDING_BP);
					end = g38.pos + LOCUS_PADDING_BP;
					locus_id = `chr${chr}:${start}-${end}`;
				} else {
					locus_id = `rsid:${a.rsid}`;
				}
				a.locus_id = locus_id;
				bundle.tables.loci.push({ locus_id, lead_rsid: a.rsid, chr, start, end });
			}
			bundle.tables.anchors = anchorsList;

			// ── Stage 3: parallel fan-out (15s deadline) ──
			bundle.meta.sources_queried.push("opentargets", "gtex");
			const traitTerms: string[] = [];
			if (parsed.trait_query) traitTerms.push(parsed.trait_query);
			if (efoLabel && efoLabel !== parsed.trait_query) traitTerms.push(efoLabel);
			if (efoId) traitTerms.push(efoId);

			const rsids = anchorsList.map((a) => a.rsid);

			const otPromise = fetchOpenTargetsBundle(anchorsList, traitTerms, parsed.max_coloc_rows_per_locus, warnings, efoId);
			const gtexPromise = fetchGtexEqtls(anchorsList, parsed.max_eqtl_rows_per_variant, warnings, parsed.gtex_dataset_id);
			const eqtlCataloguePromise = fetchEqtlCatalogueContext(anchorsList, parsed.eqtl_catalogue, warnings);
			const eqtlR8Promise = fetchEqtlR8Context(anchorsList, parsed.eqtl_catalogue_r8, warnings, runtimeEnv);
			const finngenPromise = parsed.finngen_release ? fetchFinnGenContext(anchorsList, parsed.finngen_release, warnings) : Promise.resolve([]);
			const phenotypePromise = parsed.finngen_release && parsed.finngen_phenocode ? fetchFinnGenPhenotype(parsed.finngen_release, parsed.finngen_phenocode) : Promise.resolve([]);
			const clinvarPromise = parsed.include_clinvar
				? fetchClinvarLane(rsids, warnings)
				: Promise.resolve({ rows: [] as ClinvarRow[], evidence: [] as ClinvarObservation[] });

			const stage3 = await withDeadline(
				Promise.all([otPromise, gtexPromise]),
				15_000,
				"stage-3 fan-out",
				warnings,
			);

			bundle.tables.eqtl_catalogue_evidence = await eqtlCataloguePromise;
			bundle.tables.eqtl_catalogue_r8_evidence = await eqtlR8Promise;
			if (bundle.tables.eqtl_catalogue_r8_evidence.some(row => row.source_url)) bundle.meta.sources_queried.push("eqtl_catalogue_r8");
			if (bundle.tables.eqtl_catalogue_evidence.some(row => row.source_url)) bundle.meta.sources_queried.push("eqtl_catalogue");
			bundle.tables.finngen_evidence = await finngenPromise;
			bundle.tables.finngen_phenotype_evidence = await phenotypePromise;
			for (const row of bundle.tables.finngen_phenotype_evidence) if (row.error) warnings.push(`FinnGen phenotype ${row.requested_phenocode}: ${row.error}`);
			if (bundle.tables.finngen_evidence.some(row => row.source_url) || bundle.tables.finngen_phenotype_evidence.length) bundle.meta.sources_queried.push("finngen");
			const cvRes = await clinvarPromise;
			bundle.tables.clinvar = cvRes.rows;
			bundle.tables.clinvar_evidence = cvRes.evidence;
			if (cvRes.evidence.some(row => row.source_url)) bundle.meta.sources_queried.push("clinvar");
			const symbolToEnsembl = new Map<string, string>();
			if (stage3) {
				const [otRes, gtexRes] = stage3;
				if (otRes) applyOpenTargetsBundle(otRes, anchorsList, bundle.tables, symbolToEnsembl);
				if (gtexRes) {
					bundle.tables.eqtl = gtexRes.rows;
					bundle.tables.gtex_evidence = gtexRes.evidence;
					for (const r of gtexRes.rows) if (r.ensembl_id) symbolToEnsembl.set(r.symbol, r.ensembl_id);
				}
			}

			const selected = selectCandidateGenes(bundle.tables.l2g_predictions, anchorsList, parsed.max_genes_per_locus, warnings);
			bundle.tables.candidate_genes = selected.candidates;
			bundle.tables.candidate_selection = selected.selection;

			// ── Stage 4: gnomAD + HPA + Genebass (all parallel, 12s deadline) ──
			const gnomadPromise = parsed.include_gnomad_context
				? fetchGnomadLane(bundle.tables.candidate_genes, warnings)
				: Promise.resolve([] as GnomadRow[]);
			const hpaPromise = parsed.include_hpa_tissue_context
				? fetchHpaLane(symbolToEnsembl, warnings)
				: Promise.resolve([] as HpaRow[]);
			const genebassPromise = fetchGenebassContext(symbolToEnsembl, parsed.genebass_burden_sets, warnings);

			const stage4 = await withDeadline(
				Promise.all([gnomadPromise, hpaPromise, genebassPromise, fetchBurdenEvidence(selected.candidates, efoId, warnings)]),
				12_000,
				"stage-4 fan-out",
				warnings,
			);
			if (stage4) {
				const [gnomadRes, hpaRes, burdenRes, burdenEvidence] = stage4;
				bundle.tables.opentargets_evidence.push(...burdenEvidence);
				bundle.tables.gnomad = gnomadRes ?? [];
				if (parsed.include_gnomad_context && (gnomadRes?.length ?? 0) > 0) bundle.meta.sources_queried.push("gnomad");
				bundle.tables.hpa = hpaRes ?? [];
				if (parsed.include_hpa_tissue_context && hpaRes?.some(row => row.source_url)) bundle.meta.sources_queried.push("hpa");
				bundle.tables.burden = burdenRes.rows;
				bundle.tables.genebass_evidence = burdenRes.evidence;
				if (burdenRes.evidence.length > 0) bundle.meta.sources_queried.push("genebass");
			}

			bundle.warnings = warnings;

			// Stage bundle into DO so l2g_score and l2g_synthesize can query it.
			if (!runtimeEnv?.L2G_DATA_DO) {
				return createCodeModeError(
					"DATA_ACCESS_ERROR",
					"L2G_DATA_DO namespace unavailable — cannot stage gather results.",
				);
			}
			return await stageGatherResult({
				tables: bundle.tables, meta: { ...bundle.meta, target_gene: parsed.target_gene },
				warnings, args, elapsedMs: Date.now() - startedAt, namespace: runtimeEnv.L2G_DATA_DO, extra,
			});
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			return createCodeModeError("API_ERROR", `l2g_gather failed: ${msg}`);
		}
	};

	const reg = (name: string) =>
		server.registerTool(
			name,
			{
				title: "L2G Gather Evidence",
				description:
                    "Gather bounded locus-to-gene evidence from Open Targets, GTEx, GWAS Catalog, Genebass and optional context lanes. " +
                    "Stages native Open Targets responses, observed releases, study/model features and incomplete-page warnings. " +
                    "Missing or mixed release evidence cannot feed ranking. Model artifact versions remain unknown. " +
                    "Inspect trait_resolution, opentargets_evidence and l2g_meta using l2g_query_data before l2g_score. Optional finngen_release adds native GRCh38 variant context in finngen_evidence without changing gene scores. Optional eqtl_catalogue selects one pinned r7 dataset/anchor; nominal rows alone do not score. Add fine_mapping_sha256 to qualify exact-gene/allele credible-set support with native PIP >= 0.95. Ambiguous labels require explicit efo_id. Default max_loci=8; optional lanes off.",
				inputSchema,
			},
			(args, extra) => l2gResponse(() => handler(args as Record<string, unknown>, extra), env || (extra as { env?: GatherEnv })?.env || {}, name, args, extra),
		);

	reg("mcp_l2g_gather");
	reg("l2g_gather");
}
