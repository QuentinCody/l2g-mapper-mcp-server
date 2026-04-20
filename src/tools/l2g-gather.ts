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

import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import {
	createCodeModeResponse,
	createCodeModeError,
} from "@bio-mcp/shared/codemode/response";
import { stageToDoAndRespond } from "@bio-mcp/shared/staging/utils";
import { resolveRsid } from "@bio-mcp/shared/variants/resolve";
import { ols4Search } from "@bio-mcp/shared/adapters/efo_ols4";
import { gwasAssociations } from "@bio-mcp/shared/adapters/gwas_catalog";
import { opentargetsGraphql } from "@bio-mcp/shared/adapters/opentargets";
import { gtexEqtlsByVariants } from "@bio-mcp/shared/adapters/gtex";
import { clinvarEsearchByRsid, clinvarEsummary } from "@bio-mcp/shared/adapters/clinvar";
import { gnomadGraphql } from "@bio-mcp/shared/adapters/gnomad";
import { hpaGene } from "@bio-mcp/shared/adapters/hpa";
import { genebassGeneBurden } from "@bio-mcp/shared/adapters/genebass";
import { lookupDefaultSeeds } from "../lib/well-known-traits";

interface GatherEnv {
	L2G_DATA_DO?: DurableObjectNamespace;
}

const inputSchema = {
	trait_query: z.string().optional().describe("Free-text trait label (e.g. 'type 2 diabetes')"),
	efo_id: z.string().optional().describe("EFO term ID (e.g. 'EFO_0001360')"),
	seed_rsids: z.array(z.string()).optional().describe("Seed lead rsIDs"),
	target_gene: z.string().optional().describe("Optional gene of interest for highlighting"),
	show_child_traits: z.boolean().default(true).optional(),
	phenotype_terms: z.array(z.string()).optional(),
	max_loci: z.number().int().positive().max(25).default(8).optional()
		.describe("Max loci to analyse (synchronous cap; full-scope mode deferred)"),
	max_genes_per_locus: z.number().int().positive().max(10).default(5).optional(),
	max_anchor_associations: z.number().int().positive().max(1200).default(300).optional(),
	max_coloc_rows_per_locus: z.number().int().positive().max(200).default(100).optional(),
	max_eqtl_rows_per_variant: z.number().int().positive().max(500).default(200).optional(),
	genebass_burden_sets: z.array(z.string()).default(["pLoF", "missense|LC"]).optional(),
	include_clinvar: z.boolean().default(false).optional(),
	include_gnomad_context: z.boolean().default(false).optional(),
	include_hpa_tissue_context: z.boolean().default(false).optional(),
	disable_default_seeds: z.boolean().default(false).optional(),
};

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

interface CandidateGene {
	locus_id: string;
	lead_rsid: string;
	symbol: string;
	ensembl_id: string | null;
}

interface L2gPredictionRow {
	locus_id: string;
	lead_rsid: string;
	study_locus_id: string | null;
	symbol: string;
	ensembl_id: string | null;
	score: number;
}

interface ColocRow {
	locus_id: string;
	lead_rsid: string;
	study_locus_id: string | null;
	method: string | null;
	h4: number | null;
	clpp: number | null;
	other_study_id: string | null;
}

interface EqtlRow {
	lead_rsid: string;
	symbol: string;
	ensembl_id: string | null;
	tissue: string;
	p_value: number | null;
	slope: number | null;
}

interface BurdenRow {
	symbol: string;
	ensembl_id: string;
	burden_set: string;
	phenotype: string | null;
	p_value: number | null;
	support: "strong" | "nominal" | "none";
}

interface ClinvarRow {
	rsid: string;
	variation_id: string;
	title: string | null;
	clinical_significance: string | null;
	review_status: string | null;
}

interface GnomadRow {
	symbol: string;
	oe_lof: number | null;
	oe_lof_upper: number | null;
	lof_z: number | null;
	mis_z: number | null;
	pli: number | null;
}

interface HpaRow {
	symbol: string;
	ensembl_id: string | null;
	tissues: string; // pipe-delimited
	subcellular: string | null;
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
		anchors: Anchor[];
		loci: Locus[];
		candidate_genes: CandidateGene[];
		l2g_predictions: L2gPredictionRow[];
		coloc: ColocRow[];
		eqtl: EqtlRow[];
		burden: BurdenRow[];
		clinvar: ClinvarRow[];
		gnomad: GnomadRow[];
		hpa: HpaRow[];
	};
	warnings: string[];
}

const LOCUS_PADDING_BP = 1_000_000;

/** Race a promise against a timeout; resolves to `null` on timeout. */
async function withDeadline<T>(p: Promise<T>, ms: number, label: string, warnings: string[]): Promise<T | null> {
	const timeout = new Promise<null>((resolve) =>
		setTimeout(() => {
			warnings.push(`${label} exceeded ${ms}ms budget — skipped`);
			resolve(null);
		}, ms),
	);
	try {
		return await Promise.race([p, timeout]);
	} catch (err) {
		warnings.push(`${label} failed: ${err instanceof Error ? err.message : String(err)}`);
		return null;
	}
}

interface Ols4Doc {
	iri?: string;
	label?: string;
	obo_id?: string;
}
interface Ols4Response {
	response?: { docs?: Ols4Doc[] };
}

async function resolveEfo(
	query: string,
	warnings: string[],
): Promise<{ efo_id: string | null; label: string | null }> {
	try {
		const data = (await ols4Search(query)) as Ols4Response;
		const doc = data?.response?.docs?.[0];
		if (!doc) {
			warnings.push("OLS4: no EFO hit for trait_query");
			return { efo_id: null, label: query };
		}
		return { efo_id: doc.obo_id ?? null, label: doc.label ?? query };
	} catch (err) {
		warnings.push(`OLS4 lookup failed: ${err instanceof Error ? err.message : String(err)}`);
		return { efo_id: null, label: query };
	}
}

interface GwasAssocRow {
	rs_id?: string;
	p_value?: number;
	efo_traits?: Array<{ efo_trait?: string }>;
	reported_trait?: string | string[];
	snp_allele?: Array<{ rs_id?: string }>;
}
interface GwasAssocResp {
	_embedded?: { associations?: GwasAssocRow[] };
}

async function fetchGwasAnchors(
	efoId: string | null,
	traitQuery: string | null,
	size: number,
	warnings: string[],
): Promise<Anchor[]> {
	if (!efoId && !traitQuery) return [];
	try {
		const data = (await gwasAssociations({
			efoTrait: efoId ?? undefined,
			traitName: efoId ? undefined : traitQuery ?? undefined,
			size,
		})) as GwasAssocResp;
		const rows = data?._embedded?.associations ?? [];
		const seen = new Set<string>();
		const out: Anchor[] = [];
		for (const r of rows) {
			const rsid = extractRsidFromAssoc(r);
			if (!rsid || seen.has(rsid)) continue;
			seen.add(rsid);
			const trait = Array.isArray(r.efo_traits) && r.efo_traits[0]?.efo_trait
				? String(r.efo_traits[0].efo_trait)
				: Array.isArray(r.reported_trait)
					? String(r.reported_trait[0])
					: typeof r.reported_trait === "string"
						? r.reported_trait
						: null;
			out.push({
				rsid,
				source: "gwas",
				p_value: typeof r.p_value === "number" ? r.p_value : null,
				lead_trait: trait,
				cohort: null,
			});
		}
		return out;
	} catch (err) {
		warnings.push(`GWAS Catalog anchor fetch failed: ${err instanceof Error ? err.message : String(err)}`);
		return [];
	}
}

function extractRsidFromAssoc(r: GwasAssocRow): string | null {
	if (r.rs_id && /rs\d+/i.test(r.rs_id)) return r.rs_id.toLowerCase();
	if (Array.isArray(r.snp_allele)) {
		for (const s of r.snp_allele) {
			const m = s.rs_id && /rs\d+/i.exec(s.rs_id);
			if (m) return m[0].toLowerCase();
		}
	}
	return null;
}

// ── OpenTargets GraphQL: batched credible-set + L2G + coloc ─────────────
const OT_SEARCH_STUDY_QUERY = /* GraphQL */ `
query L2GSearch($q: String!) {
  search(queryString: $q, entityNames: ["study"], page: {index: 0, size: 20}) {
    hits { score object { ... on Study { id projectId hasSumstats } } }
  }
}`;

const OT_STUDY_CS_QUERY = /* GraphQL */ `
query L2GStudy($id: String!) {
  study(studyId: $id) {
    credibleSets(page: {index: 0, size: 400}) {
      rows { studyLocusId variant { id rsIds } }
    }
  }
}`;

const OT_BATCH_DETAIL_QUERY = /* GraphQL */ `
query L2GBatch($ids: [String!]!) {
  credibleSets(studyLocusIds: $ids) {
    rows {
      studyLocusId
      l2GPredictions { rows { score target { id approvedSymbol } } }
      colocalisation(page: {index: 0, size: 100}) {
        rows { colocalisationMethod h4 clpp otherStudyLocus { studyId studyLocusId } }
      }
    }
  }
}`;

interface OtStudy { id: string; projectId?: string; hasSumstats?: boolean; }
interface OtSearchResp {
	search?: { hits?: Array<{ score?: number; object?: OtStudy }> };
}
interface OtCsRow {
	studyLocusId?: string;
	variant?: { id?: string; rsIds?: string[] };
}
interface OtStudyResp { study?: { credibleSets?: { rows?: OtCsRow[] } }; }
interface OtBatchResp {
	credibleSets?: {
		rows?: Array<{
			studyLocusId?: string;
			l2GPredictions?: { rows?: Array<{ score?: number; target?: { id?: string; approvedSymbol?: string } }> };
			colocalisation?: { rows?: Array<{ colocalisationMethod?: string; h4?: number; clpp?: number; otherStudyLocus?: { studyId?: string } }> };
		}>;
	};
}

async function fetchOpenTargetsBundle(
	anchorRsids: string[],
	traitTerms: string[],
	maxColocRows: number,
	warnings: string[],
): Promise<{ l2g: L2gPredictionRow[]; coloc: ColocRow[]; symbolsNeedingEnsembl: Map<string, string> }> {
	const l2gRows: L2gPredictionRow[] = [];
	const colocRows: ColocRow[] = [];
	const symbolsNeedingEnsembl = new Map<string, string>();
	if (anchorRsids.length === 0 || traitTerms.length === 0) return { l2g: l2gRows, coloc: colocRows, symbolsNeedingEnsembl };

	// Step 1: search studies for each trait term in parallel (batched)
	const studyResults = await Promise.all(
		traitTerms.map((t) =>
			opentargetsGraphql<OtSearchResp>(OT_SEARCH_STUDY_QUERY, { q: t }).catch((err) => {
				warnings.push(`OpenTargets search failed for "${t}": ${err instanceof Error ? err.message : String(err)}`);
				return { data: undefined } as const;
			}),
		),
	);

	const studies = new Map<string, OtStudy>();
	for (const sr of studyResults) {
		const hits = sr?.data?.search?.hits ?? [];
		for (const hit of hits) {
			const obj = hit?.object;
			if (obj?.id) studies.set(obj.id, obj);
		}
	}
	const preferredStudies = [...studies.values()].filter((s) => s.hasSumstats).slice(0, 6);
	const chosen = preferredStudies.length > 0 ? preferredStudies : [...studies.values()].slice(0, 6);

	// Step 2: for each study, fetch credible sets (in parallel)
	const anchorSet = new Set(anchorRsids.map((r) => r.toLowerCase()));
	const studyLocusAnchorMap = new Map<string, Set<string>>();

	const csResults = await Promise.all(
		chosen.map((s) =>
			opentargetsGraphql<OtStudyResp>(OT_STUDY_CS_QUERY, { id: s.id }).catch((err) => {
				warnings.push(`OpenTargets study ${s.id} CS fetch failed: ${err instanceof Error ? err.message : String(err)}`);
				return { data: undefined } as const;
			}),
		),
	);
	for (const cs of csResults) {
		const rows = cs?.data?.study?.credibleSets?.rows ?? [];
		for (const row of rows) {
			if (!row.studyLocusId) continue;
			const rsids = (row.variant?.rsIds ?? []).map((r) => r.toLowerCase());
			const matched = rsids.filter((r) => anchorSet.has(r));
			if (matched.length > 0) {
				const existing = studyLocusAnchorMap.get(row.studyLocusId) ?? new Set<string>();
				for (const m of matched) existing.add(m);
				studyLocusAnchorMap.set(row.studyLocusId, existing);
			}
		}
	}

	// Step 3: batched detail fetch (chunks of 40)
	const studyLocusIds = [...studyLocusAnchorMap.keys()];
	const chunks: string[][] = [];
	for (let i = 0; i < studyLocusIds.length; i += 40) chunks.push(studyLocusIds.slice(i, i + 40));
	const detailResults = await Promise.all(
		chunks.map((chunk) =>
			opentargetsGraphql<OtBatchResp>(OT_BATCH_DETAIL_QUERY, { ids: chunk }).catch((err) => {
				warnings.push(`OpenTargets batch detail failed: ${err instanceof Error ? err.message : String(err)}`);
				return { data: undefined } as const;
			}),
		),
	);
	for (const dr of detailResults) {
		const rows = dr?.data?.credibleSets?.rows ?? [];
		for (const row of rows) {
			const slId = row.studyLocusId;
			if (!slId) continue;
			const anchors = studyLocusAnchorMap.get(slId) ?? new Set<string>();
			if (anchors.size === 0) continue;

			const l2gList = row.l2GPredictions?.rows ?? [];
			const colocList = (row.colocalisation?.rows ?? []).slice(0, maxColocRows);

			for (const anchor of anchors) {
				for (const pred of l2gList) {
					const symbol = pred.target?.approvedSymbol;
					if (!symbol) continue;
					const ensembl = pred.target?.id ?? null;
					if (symbol && ensembl) symbolsNeedingEnsembl.set(symbol, ensembl);
					l2gRows.push({
						locus_id: `rsid:${anchor}`, // placeholder — enriched with coords later
						lead_rsid: anchor,
						study_locus_id: slId,
						symbol,
						ensembl_id: ensembl,
						score: Number(pred.score ?? 0),
					});
				}
				for (const coloc of colocList) {
					colocRows.push({
						locus_id: `rsid:${anchor}`,
						lead_rsid: anchor,
						study_locus_id: slId,
						method: coloc.colocalisationMethod ?? null,
						h4: typeof coloc.h4 === "number" ? coloc.h4 : null,
						clpp: typeof coloc.clpp === "number" ? coloc.clpp : null,
						other_study_id: coloc.otherStudyLocus?.studyId ?? null,
					});
				}
			}
		}
	}

	return { l2g: l2gRows, coloc: colocRows, symbolsNeedingEnsembl };
}

// ── GTEx: batched eQTLs for the anchor variantIds ───────────────────────
interface GtexSingleTissueEqtl {
	variantId?: string;
	snpId?: string;
	geneSymbol?: string;
	gencodeId?: string;
	tissueSiteDetailId?: string;
	pValue?: number;
	nes?: number;
}
interface GtexEqtlResp { data?: GtexSingleTissueEqtl[]; }

async function fetchGtexEqtls(
	anchors: Anchor[],
	maxRowsPerVariant: number,
	warnings: string[],
): Promise<EqtlRow[]> {
	const variantIds: string[] = [];
	const vidToRsid = new Map<string, string>();
	for (const a of anchors) {
		const g38 = a.grch38;
		if (!g38?.chr || !g38.pos || !g38.ref || !g38.alt) continue;
		const vid = `chr${g38.chr}_${g38.pos}_${g38.ref}_${g38.alt}_b38`;
		variantIds.push(vid);
		vidToRsid.set(vid, a.rsid);
	}
	if (variantIds.length === 0) return [];

	// GTEx accepts multiple variantId values as repeated query params; cap per-batch
	try {
		const resp = (await gtexEqtlsByVariants(variantIds, { itemsPerPage: Math.min(250, maxRowsPerVariant * variantIds.length) })) as GtexEqtlResp;
		const rows = resp?.data ?? [];
		const out: EqtlRow[] = [];
		for (const r of rows) {
			const vid = r.variantId ?? "";
			const rsid = vidToRsid.get(vid) ?? (r.snpId ?? "").toLowerCase();
			if (!rsid || !r.geneSymbol) continue;
			out.push({
				lead_rsid: rsid,
				symbol: r.geneSymbol,
				ensembl_id: r.gencodeId ? r.gencodeId.split(".")[0] : null,
				tissue: r.tissueSiteDetailId ?? ".",
				p_value: typeof r.pValue === "number" ? r.pValue : null,
				slope: typeof r.nes === "number" ? r.nes : null,
			});
		}
		return out;
	} catch (err) {
		warnings.push(`GTEx eQTL batch failed: ${err instanceof Error ? err.message : String(err)}`);
		return [];
	}
}

// ── Optional lanes ──────────────────────────────────────────────────────
async function fetchClinvarLane(rsids: string[], warnings: string[]): Promise<ClinvarRow[]> {
	const out: ClinvarRow[] = [];
	// Step 1: esearch each rsid (in parallel; capped)
	const searches = await Promise.all(
		rsids.slice(0, 30).map(async (rsid) => {
			try {
				const res = (await clinvarEsearchByRsid(rsid)) as { esearchresult?: { idlist?: string[] } };
				const ids = res?.esearchresult?.idlist ?? [];
				return { rsid, ids: ids.slice(0, 3) };
			} catch (err) {
				warnings.push(`ClinVar esearch failed for ${rsid}: ${err instanceof Error ? err.message : String(err)}`);
				return { rsid, ids: [] as string[] };
			}
		}),
	);
	const idToRsid = new Map<string, string>();
	const allIds: string[] = [];
	for (const s of searches) {
		for (const id of s.ids) {
			idToRsid.set(id, s.rsid);
			allIds.push(id);
		}
	}
	if (allIds.length === 0) return out;
	// Step 2: esummary in one batch
	try {
		const sum = (await clinvarEsummary(allIds)) as {
			result?: Record<string, { title?: string; clinical_significance?: { description?: string }; review_status?: string }>;
		};
		const records = sum?.result ?? {};
		for (const [id, rec] of Object.entries(records)) {
			if (id === "uids") continue;
			const rsid = idToRsid.get(id);
			if (!rsid) continue;
			out.push({
				rsid,
				variation_id: id,
				title: rec?.title ?? null,
				clinical_significance: rec?.clinical_significance?.description ?? null,
				review_status: rec?.review_status ?? null,
			});
		}
	} catch (err) {
		warnings.push(`ClinVar esummary failed: ${err instanceof Error ? err.message : String(err)}`);
	}
	return out;
}

const GNOMAD_GENE_QUERY = /* GraphQL */ `
query L2GGene($symbol: String!, $ref: ReferenceGenomeId!) {
  gene(gene_symbol: $symbol, reference_genome: $ref) {
    symbol
    gnomad_constraint { oe_lof oe_lof_upper lof_z mis_z pLI }
  }
}`;
interface GnomadGeneResp { gene?: { symbol?: string; gnomad_constraint?: { oe_lof?: number; oe_lof_upper?: number; lof_z?: number; mis_z?: number; pLI?: number } }; }

async function fetchGnomadLane(symbols: string[], warnings: string[]): Promise<GnomadRow[]> {
	const out: GnomadRow[] = [];
	const capped = symbols.slice(0, 40);
	const results = await Promise.all(
		capped.map((symbol) =>
			gnomadGraphql<GnomadGeneResp>(GNOMAD_GENE_QUERY, { symbol, ref: "GRCh38" }).catch((err) => {
				warnings.push(`gnomAD failed for ${symbol}: ${err instanceof Error ? err.message : String(err)}`);
				return { data: undefined } as const;
			}),
		),
	);
	for (const r of results) {
		const g = r?.data?.gene;
		const c = g?.gnomad_constraint;
		if (!g?.symbol) continue;
		out.push({
			symbol: g.symbol,
			oe_lof: c?.oe_lof ?? null,
			oe_lof_upper: c?.oe_lof_upper ?? null,
			lof_z: c?.lof_z ?? null,
			mis_z: c?.mis_z ?? null,
			pli: c?.pLI ?? null,
		});
	}
	return out;
}

interface HpaGeneRecord {
	Gene?: string;
	Ensembl?: string;
	"Tissue expression - nTPM"?: Record<string, number>;
	"Subcellular location"?: string;
}

async function fetchHpaLane(
	symbolToEnsembl: Map<string, string>,
	warnings: string[],
): Promise<HpaRow[]> {
	const out: HpaRow[] = [];
	const entries = [...symbolToEnsembl.entries()].slice(0, 20);
	const results = await Promise.all(
		entries.map(async ([symbol, ensembl]) => {
			try {
				const data = (await hpaGene(ensembl)) as HpaGeneRecord;
				const tissues = data?.["Tissue expression - nTPM"];
				const tissueList = tissues && typeof tissues === "object"
					? Object.entries(tissues)
						.filter(([, v]) => typeof v === "number" && v >= 1)
						.sort((a, b) => Number(b[1]) - Number(a[1]))
						.slice(0, 8)
						.map(([k]) => k)
					: [];
				return {
					symbol,
					ensembl_id: ensembl,
					tissues: tissueList.join(" | "),
					subcellular: data?.["Subcellular location"] ?? null,
				} satisfies HpaRow;
			} catch (err) {
				warnings.push(`HPA failed for ${symbol}: ${err instanceof Error ? err.message : String(err)}`);
				return null;
			}
		}),
	);
	for (const r of results) if (r) out.push(r);
	return out;
}

// ── Genebass burden (stage 4) ───────────────────────────────────────────
interface GenebassResp { associations?: Array<{ phenotype_description?: string; skat_o_pvalue?: number }>; }

async function fetchGenebassLane(
	symbolToEnsembl: Map<string, string>,
	burdenSets: string[],
	warnings: string[],
): Promise<BurdenRow[]> {
	const out: BurdenRow[] = [];
	const entries = [...symbolToEnsembl.entries()].slice(0, 25); // hard cap for subrequest budget
	const jobs: Promise<void>[] = [];
	for (const [symbol, ensembl] of entries) {
		for (const burdenSet of burdenSets) {
			jobs.push(
				(async () => {
					try {
						const data = (await genebassGeneBurden(ensembl, burdenSet)) as GenebassResp;
						const assocs = data?.associations ?? [];
						let best_p: number | null = null;
						let best_phenotype: string | null = null;
						for (const row of assocs) {
							if (typeof row.skat_o_pvalue !== "number") continue;
							if (best_p == null || row.skat_o_pvalue < best_p) {
								best_p = row.skat_o_pvalue;
								best_phenotype = row.phenotype_description ?? null;
							}
						}
						let support: BurdenRow["support"] = "none";
						if (best_p != null && best_p < 2.5e-6) support = "strong";
						else if (best_p != null && best_p < 0.05) support = "nominal";
						out.push({
							symbol,
							ensembl_id: ensembl,
							burden_set: burdenSet,
							phenotype: best_phenotype,
							p_value: best_p,
							support,
						});
					} catch (err) {
						warnings.push(`Genebass failed for ${symbol}/${burdenSet}: ${err instanceof Error ? err.message : String(err)}`);
					}
				})(),
			);
		}
	}
	await Promise.all(jobs);
	return out;
}

// ── Main tool ───────────────────────────────────────────────────────────

export function registerL2gGather(server: McpServer, env?: GatherEnv): void {
	const handler = async (args: Record<string, unknown>, extra: unknown) => {
		const runtimeEnv = env || (extra as { env?: GatherEnv })?.env;
		const warnings: string[] = [];
		const startedAt = Date.now();

		try {
			const parsed = {
				trait_query: (args.trait_query as string | undefined) ?? null,
				efo_id: (args.efo_id as string | undefined) ?? null,
				seed_rsids: (args.seed_rsids as string[] | undefined) ?? [],
				target_gene: (args.target_gene as string | undefined) ?? null,
				max_loci: Number(args.max_loci ?? 8),
				max_genes_per_locus: Number(args.max_genes_per_locus ?? 5),
				max_anchor_associations: Number(args.max_anchor_associations ?? 300),
				max_coloc_rows_per_locus: Number(args.max_coloc_rows_per_locus ?? 100),
				max_eqtl_rows_per_variant: Number(args.max_eqtl_rows_per_variant ?? 200),
				genebass_burden_sets: (args.genebass_burden_sets as string[] | undefined) ?? ["pLoF", "missense|LC"],
				include_clinvar: Boolean(args.include_clinvar ?? false),
				include_gnomad_context: Boolean(args.include_gnomad_context ?? false),
				include_hpa_tissue_context: Boolean(args.include_hpa_tissue_context ?? false),
				disable_default_seeds: Boolean(args.disable_default_seeds ?? false),
			};

			// Validate input
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

			// Project subrequest budget up front.
			// Upper bounds reflect the worst-case fan-out per stage assuming every
			// anchor survives normalization and every candidate gene is queried.
			// The previous formula under-counted by ~4-5× because it used
			// max_loci as the anchor ceiling instead of max_anchor_associations,
			// and ignored per-candidate gnomAD/HPA fan-out.
			const anchorCeiling = Math.min(
				parsed.max_anchor_associations,
				Math.max(seedRsids.length, parsed.max_loci * 4),
			);
			const candidateGenesCeiling = parsed.max_loci * parsed.max_genes_per_locus;
			const coordBudget = anchorCeiling * 2;
			const otBudget =
				15 + Math.ceil(anchorCeiling / 40) * 3 + Math.ceil(parsed.max_loci / 5);
			const gtexBudget = Math.max(1, Math.ceil(anchorCeiling / 40));
			// ClinVar: esummary is batched ~20 ids per request
			const clinvarBudget = parsed.include_clinvar
				? Math.ceil(anchorCeiling / 20) + 1
				: 0;
			// gnomAD / HPA fan out per candidate gene
			const gnomadBudget = parsed.include_gnomad_context ? candidateGenesCeiling : 0;
			const hpaBudget = parsed.include_hpa_tissue_context ? candidateGenesCeiling : 0;
			const genebassBudget = candidateGenesCeiling * parsed.genebass_burden_sets.length;
			const projected =
				2 +
				coordBudget +
				otBudget +
				gtexBudget +
				clinvarBudget +
				gnomadBudget +
				hpaBudget +
				genebassBudget;
			if (projected > 800) {
				return createCodeModeError(
					"INVALID_ARGUMENTS",
					`Projected subrequest count (${projected}) exceeds the 800 Worker-subrequest budget. Reduce max_loci, max_genes_per_locus, max_anchor_associations, or drop optional lanes (include_clinvar/include_gnomad_context/include_hpa_tissue_context).`,
				);
			}

			const bundle: GatherBundle = {
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
						max_coloc_rows_per_locus: parsed.max_coloc_rows_per_locus,
						max_eqtl_rows_per_variant: parsed.max_eqtl_rows_per_variant,
						include_clinvar: parsed.include_clinvar,
						include_gnomad_context: parsed.include_gnomad_context,
						include_hpa_tissue_context: parsed.include_hpa_tissue_context,
						projected_subrequests: projected,
					},
				},
				tables: {
					anchors: [],
					loci: [],
					candidate_genes: [],
					l2g_predictions: [],
					coloc: [],
					eqtl: [],
					burden: [],
					clinvar: [],
					gnomad: [],
					hpa: [],
				},
				warnings: [],
			};

			// ── Stage 1: EFO resolve + GWAS anchor fetch ──
			let efoLabel: string | null = null;
			let efoId = parsed.efo_id;
			if (parsed.trait_query && !efoId) {
				const efo = await resolveEfo(parsed.trait_query, warnings);
				efoId = efo.efo_id;
				efoLabel = efo.label;
				bundle.meta.sources_queried.push("ols4");
			}
			bundle.meta.efo_id = efoId;
			bundle.meta.efo_label = efoLabel;

			const gwasAnchors = parsed.max_anchor_associations > 0
				? await fetchGwasAnchors(efoId, parsed.trait_query, Math.min(parsed.max_anchor_associations, 300), warnings)
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
				bundle.warnings = warnings;
				return createCodeModeError(
					"NOT_FOUND",
					"No anchors remained after normalization. Provide seed_rsids or try a different trait_query.",
				);
			}

			// ── Stage 2: Coordinate normalization (parallel, 7s deadline) ──
			bundle.meta.sources_queried.push("ensembl");
			const coordPromise = Promise.all(
				anchorsList.map(async (a) => {
					try {
						const resolved = await resolveRsid(a.rsid);
						a.grch38 = resolved.grch38;
						a.grch37 = resolved.grch37;
					} catch (err) {
						warnings.push(`Ensembl rsid lookup failed for ${a.rsid}: ${err instanceof Error ? err.message : String(err)}`);
					}
				}),
			);
			await withDeadline(coordPromise, 7000, "coordinate normalization", warnings);

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

			const otPromise = fetchOpenTargetsBundle(rsids, traitTerms, parsed.max_coloc_rows_per_locus, warnings);
			const gtexPromise = fetchGtexEqtls(anchorsList, parsed.max_eqtl_rows_per_variant, warnings);
			const clinvarPromise = parsed.include_clinvar
				? fetchClinvarLane(rsids, warnings)
				: Promise.resolve([] as ClinvarRow[]);

			const stage3 = await withDeadline(
				Promise.all([otPromise, gtexPromise, clinvarPromise]),
				15_000,
				"stage-3 fan-out",
				warnings,
			);

			const symbolToEnsembl = new Map<string, string>();
			if (stage3) {
				const [otRes, gtexRes, cvRes] = stage3;
				if (otRes) {
					// Patch locus_id onto l2g/coloc rows from anchor rsids
					for (const row of otRes.l2g) {
						const a = anchorsList.find((x) => x.rsid === row.lead_rsid);
						row.locus_id = a?.locus_id ?? row.locus_id;
						if (row.symbol && row.ensembl_id) symbolToEnsembl.set(row.symbol, row.ensembl_id);
					}
					for (const row of otRes.coloc) {
						const a = anchorsList.find((x) => x.rsid === row.lead_rsid);
						row.locus_id = a?.locus_id ?? row.locus_id;
					}
					bundle.tables.l2g_predictions = otRes.l2g;
					bundle.tables.coloc = otRes.coloc;
					for (const [sym, id] of otRes.symbolsNeedingEnsembl) symbolToEnsembl.set(sym, id);
				}
				if (gtexRes) {
					bundle.tables.eqtl = gtexRes;
					for (const r of gtexRes) if (r.ensembl_id) symbolToEnsembl.set(r.symbol, r.ensembl_id);
				}
				if (cvRes) {
					bundle.tables.clinvar = cvRes;
					if (parsed.include_clinvar) bundle.meta.sources_queried.push("clinvar");
				}
			}

			// Build candidate_genes from l2g predictions (per locus, up to max_genes_per_locus)
			const perLocusGenes = new Map<string, Map<string, string | null>>();
			for (const row of bundle.tables.l2g_predictions) {
				const locus = row.locus_id;
				if (!perLocusGenes.has(locus)) perLocusGenes.set(locus, new Map());
				const m = perLocusGenes.get(locus);
				if (m && !m.has(row.symbol)) m.set(row.symbol, row.ensembl_id ?? null);
			}
			for (const [locus_id, genes] of perLocusGenes) {
				const anchor = anchorsList.find((a) => a.locus_id === locus_id);
				let count = 0;
				for (const [symbol, ensembl_id] of genes) {
					if (count >= parsed.max_genes_per_locus) break;
					bundle.tables.candidate_genes.push({
						locus_id,
						lead_rsid: anchor?.rsid ?? "",
						symbol,
						ensembl_id,
					});
					count++;
				}
			}

			// ── Stage 4: gnomAD + HPA + Genebass (all parallel, 12s deadline) ──
			const symbols = [...symbolToEnsembl.keys()];
			const gnomadPromise = parsed.include_gnomad_context
				? fetchGnomadLane(symbols, warnings)
				: Promise.resolve([] as GnomadRow[]);
			const hpaPromise = parsed.include_hpa_tissue_context
				? fetchHpaLane(symbolToEnsembl, warnings)
				: Promise.resolve([] as HpaRow[]);
			const genebassPromise = fetchGenebassLane(symbolToEnsembl, parsed.genebass_burden_sets, warnings);

			const stage4 = await withDeadline(
				Promise.all([gnomadPromise, hpaPromise, genebassPromise]),
				12_000,
				"stage-4 fan-out",
				warnings,
			);
			if (stage4) {
				const [gnomadRes, hpaRes, burdenRes] = stage4;
				bundle.tables.gnomad = gnomadRes ?? [];
				if (parsed.include_gnomad_context && (gnomadRes?.length ?? 0) > 0) bundle.meta.sources_queried.push("gnomad");
				bundle.tables.hpa = hpaRes ?? [];
				if (parsed.include_hpa_tissue_context && (hpaRes?.length ?? 0) > 0) bundle.meta.sources_queried.push("hpa");
				bundle.tables.burden = burdenRes ?? [];
				if ((burdenRes?.length ?? 0) > 0) bundle.meta.sources_queried.push("genebass");
			}

			bundle.warnings = warnings;

			// Stage bundle into DO so l2g_score and l2g_synthesize can query it.
			if (!runtimeEnv?.L2G_DATA_DO) {
				return createCodeModeError(
					"DATA_ACCESS_ERROR",
					"L2G_DATA_DO namespace unavailable — cannot stage gather results.",
				);
			}
			const staged = await stageToDoAndRespond(
				bundle.tables,
				runtimeEnv.L2G_DATA_DO,
				"l2g",
				undefined,
				{ toolName: "l2g_gather", serverName: "l2g-mapper-mcp-server" },
				"l2g",
				(extra as { sessionId?: string })?.sessionId,
			);

			// Also persist meta + warnings as a small side-table via second /process call.
			try {
				const metaDoId = runtimeEnv.L2G_DATA_DO.idFromName(staged.dataAccessId);
				const metaDo = runtimeEnv.L2G_DATA_DO.get(metaDoId);
				await metaDo.fetch(
					new Request("http://localhost/process", {
						method: "POST",
						headers: { "Content-Type": "application/json" },
						body: JSON.stringify({
							data: {
								l2g_meta: [
									{
										trait_query: bundle.meta.trait_query,
										efo_id: bundle.meta.efo_id,
										efo_label: bundle.meta.efo_label,
										generated_at: bundle.meta.generated_at,
										sources_queried: (bundle.meta.sources_queried ?? []).join(" | "),
										target_gene: parsed.target_gene,
										caps_json: JSON.stringify(bundle.meta.caps),
										warnings_json: JSON.stringify(bundle.warnings),
									},
								],
							},
						}),
					}),
				);
			} catch (err) {
				warnings.push(`Meta side-table write failed: ${err instanceof Error ? err.message : String(err)}`);
			}

			const elapsed_ms = Date.now() - startedAt;
			return createCodeModeResponse(
				{
					data_access_id: staged.dataAccessId,
					trait: {
						query: bundle.meta.trait_query,
						efo_id: bundle.meta.efo_id,
						label: bundle.meta.efo_label,
					},
					anchors_count: bundle.tables.anchors.length,
					loci_count: bundle.tables.loci.length,
					candidate_genes_count: bundle.tables.candidate_genes.length,
					elapsed_ms,
					warnings,
					_staging: staged._staging,
					next_step: `Call l2g_score with data_access_id='${staged.dataAccessId}'.`,
				},
				{
					textSummary: `l2g_gather staged ${bundle.tables.anchors.length} anchors / ${bundle.tables.loci.length} loci in ${elapsed_ms}ms. data_access_id=${staged.dataAccessId}`,
					meta: { data_access_id: staged.dataAccessId, elapsed_ms },
				},
			);
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
					"Stage 1 of the locus-to-gene composer. Resolves a trait, collects GWAS anchors, " +
					"normalizes variant coordinates, and fans out in parallel to OpenTargets (L2G+coloc), " +
					"GTEx eQTL, Genebass burden, and optional ClinVar/gnomAD/HPA lanes. Stages all results " +
					"into a DO keyed by data_access_id. Reduced synchronous caps: max_loci=8, " +
					"max_anchor_associations=300, optional lanes off by default. Fits in the 30s Worker budget.",
				inputSchema,
			},
			(args, extra) => handler(args as Record<string, unknown>, extra),
		);

	reg("mcp_l2g_gather");
	reg("l2g_gather");
}
