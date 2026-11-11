/**
 * l2g_score — Tier 2 of the composer.
 *
 * Pure SQL over the staged gather tables. Applies the SKILL.md scoring
 * formula verbatim and writes two derived tables back into the same DO:
 *   - scored_candidate_genes
 *   - cross_locus_ranked_genes
 *
 * Runs SKILL.md §"Phase 5" QC gates; returns error structuredContent if any fail.
 * No HTTP — expected to finish in hundreds of ms.
 */

import type { McpServer } from "@bio-mcp/shared/mcp";
import { z } from "zod";
import {
	createCodeModeResponse,
	createCodeModeError,
} from "@bio-mcp/shared/codemode/response";
import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";
import {
	computeScore,
	confidenceLabel,
	WEIGHTS,
	type CodingSupport,
	type ClinvarSupport,
	type ScoreBreakdown,
} from "../lib/scoring";
import { runScoringQc } from "../lib/qc";
import { EVIDENCE_LIMITATIONS, EVIDENCE_POLICY_VERSION, GATHER_POLICY_VERSION } from "../lib/evidence-policy";
import { l2gGeneKey, qualifiedL2gScores } from "../lib/qualified-l2g";
import type { L2gPredictionRow, ColocRow } from "../lib/opentargets-contract";
import { qualifiedColocScores } from "../lib/qualified-coloc";
import { crossLocusRanking } from "../lib/cross-locus-ranking";
import { l2gResponse } from "../lib/tool-result";
import { loadGtexSupport } from "../lib/gtex-scoring";
import { gtexGeneKey } from "../lib/gtex-qualified";
import { loadBurdenSupport } from "../lib/burden-scoring";
import { loadEqtlCatalogueObservation, type StagedAnchor } from "../lib/eqtl-catalogue-synthesis";
import { qualifiedCatalogue } from "../lib/eqtl-catalogue-qualified";
import { loadEqtlR8Observation } from "../lib/eqtl-r8-synthesis";
import { qualifiedR8Catalogue } from "../lib/eqtl-r8-qualified";
import { mergeEqtlSupport } from "../lib/eqtl-support";

interface ScoreEnv {
	L2G_DATA_DO?: DurableObjectNamespace;
}

const inputSchema = {
	data_access_id: z.string().min(1).describe("data_access_id from l2g_gather"),
	scoring_overrides: z
		.object({
			l2g: z.number().optional(),
			coloc: z.number().optional(),
			eqtl: z.number().optional(),
			burden: z.number().optional(),
			coding: z.number().optional(),
		})
		.optional()
		.describe("Optional weight overrides — must sum to ~1.0; otherwise defaults from SKILL.md apply"),
};

interface AnchorRow {
	rsid: string;
	locus_id?: string;
	p_value?: number | null;
	lead_trait?: string | null;
}
interface CandidateRow {
	locus_id: string;
	lead_rsid: string;
	symbol: string;
	ensembl_id: string | null;
}

async function selectAll<T>(
	doNamespace: DurableObjectNamespace,
	dataAccessId: string,
	sql: string,
): Promise<T[]> {
	const result = await queryDataFromDo(doNamespace, dataAccessId, sql, 10_000);
	if (result.truncated || result.count_capped || (result.total_matching != null && result.total_matching > result.rows.length)) throw new Error("Scoring requires complete staged input tables; the SQL read was capped.");
	return result.rows as T[];
}

/**
 * Preflight check: distinguishes three failure modes so error messages are
 * truthful instead of defaulting to "No anchors remained".
 *   - DO has zero user tables        → unknown/empty data_access_id
 *   - DO has tables but no `anchors` → gather produced no anchors (QC)
 *   - DO has `anchors`                → proceed with normal scoring
 */
async function preflightDao(
	doNamespace: DurableObjectNamespace,
	dataAccessId: string,
): Promise<{ ok: boolean; message: string; tables?: string[]; selectedTrait?: string | null }> {
	try {
		const doId = doNamespace.idFromName(dataAccessId);
		const doInstance = doNamespace.get(doId);
		const resp = await doInstance.fetch(new Request("http://localhost/schema"));
		if (!resp.ok) {
			return {
				ok: false,
				message: `Unable to resolve data_access_id=${dataAccessId} (DO /schema returned HTTP ${resp.status}). Re-run l2g_gather first.`,
			};
		}
		// DO /schema returns { success, schema: { tables: { "tblName": {...}, ... }, table_count, ... } }
		// NOT an array. The legacy shape check assumed an array and always
		// resolved to 0 user tables, making every preflight report
		// "has no staged gather output (found tables: none)".
		const body = (await resp.json()) as {
			schema?: { tables?: Record<string, unknown> };
			data?: { schema?: { tables?: Record<string, unknown> } };
		};
		const tablesMap =
			body.schema?.tables ?? body.data?.schema?.tables ?? {};
		const userTables = Object.keys(tablesMap).filter(
			(n) =>
				typeof n === "string" &&
				!n.startsWith("_") &&
				!n.startsWith("sqlite_") &&
				!n.startsWith("_staging_"),
		);
		if (userTables.length === 0 || (userTables.length === 1 && userTables[0] === "l2g_meta")) {
			return {
				ok: false,
				message: `data_access_id=${dataAccessId} has no staged gather output (found tables: ${userTables.join(", ") || "none"}). Run l2g_gather first or double-check the id.`,
			};
		}
		if (!userTables.includes("anchors")) {
			return {
				ok: false,
				message: `data_access_id=${dataAccessId} is populated but has no anchors table (found: ${userTables.join(", ")}). The gather phase produced no anchor variants — revise trait_query/seed_rsids and re-run l2g_gather.`,
			};
		}
		const meta = await queryDataFromDo(doNamespace, dataAccessId, "SELECT * FROM l2g_meta", 2);
		const row = meta.rows[0] as { gather_policy_version?: string; efo_id?: string | null } | undefined;
		if (meta.rows.length !== 1 || row?.gather_policy_version !== GATHER_POLICY_VERSION) {
			return { ok: false, message: "Stored gather predates the current allele/trait/candidate qualification. Re-run l2g_gather before scoring." };
		}
		return { ok: true, message: "", tables: userTables, selectedTrait: row.efo_id ?? null };
	} catch (err) {
		const msg = err instanceof Error ? err.message : String(err);
		return { ok: false, message: `Preflight failed: ${msg}` };
	}
}

export function registerL2gScore(server: McpServer, env?: ScoreEnv): void {
	const handler = async (args: Record<string, unknown>, extra: unknown) => {
		const runtimeEnv = env || (extra as { env?: ScoreEnv })?.env;
		const dataAccessId = String(args.data_access_id ?? "");
		const overrides = (args.scoring_overrides ?? {}) as Partial<typeof WEIGHTS>;

		if (!dataAccessId) return createCodeModeError("INVALID_ARGUMENTS", "data_access_id is required");
		if (!runtimeEnv?.L2G_DATA_DO) {
			return createCodeModeError("DATA_ACCESS_ERROR", "L2G_DATA_DO namespace unavailable");
		}
		const doNamespace = runtimeEnv.L2G_DATA_DO;

		try {
			const weights = {
				l2g: Number(overrides.l2g ?? WEIGHTS.l2g),
				coloc: Number(overrides.coloc ?? WEIGHTS.coloc),
				eqtl: Number(overrides.eqtl ?? WEIGHTS.eqtl),
				burden: Number(overrides.burden ?? WEIGHTS.burden),
				coding: Number(overrides.coding ?? WEIGHTS.coding),
			};

			// Preflight: confirm the DAI resolves to a DO that was populated by
			// l2g_gather. Distinguishes between "unknown DAI" (user typo, new
			// session) and "anchors table is genuinely empty" (real no-hits).
			const preflight = await preflightDao(doNamespace, dataAccessId);
			if (!preflight.ok) {
				return createCodeModeError("DATA_ACCESS_ERROR", preflight.message);
			}

			const anchors = await selectAll<AnchorRow>(
				doNamespace,
				dataAccessId,
				"SELECT rsid, locus_id, p_value, lead_trait FROM anchors",
			);
			const candidates = await selectAll<CandidateRow>(
				doNamespace,
				dataAccessId,
				"SELECT locus_id, lead_rsid, symbol, ensembl_id FROM candidate_genes",
			);
			const l2gRows = preflight.tables?.includes("l2g_predictions") ? await selectAll<L2gPredictionRow>(
				doNamespace,
				dataAccessId,
				"SELECT * FROM l2g_predictions",
			) : [];
			const gtex = await loadGtexSupport(doNamespace, dataAccessId, preflight.tables);
            const catalogueInput = await loadEqtlCatalogueObservation(doNamespace, dataAccessId, gtex.caps_json, gtex.anchors as unknown as StagedAnchor[]);
            const r8Input = await loadEqtlR8Observation(doNamespace, dataAccessId, gtex.caps_json, gtex.anchors as unknown as StagedAnchor[]);
            const catalogue = catalogueInput ? qualifiedCatalogue(catalogueInput.native, catalogueInput.row, catalogueInput.indexes, dataAccessId) : qualifiedR8Catalogue(r8Input, dataAccessId);
            const burden = await loadBurdenSupport(doNamespace, dataAccessId, preflight.tables, preflight.selectedTrait);

			const colocRows = preflight.tables?.includes("coloc") ? await selectAll<ColocRow>(doNamespace, dataAccessId, "SELECT * FROM coloc") : [];
			const qualifiedColoc = qualifiedColocScores(colocRows, l2gRows, preflight.selectedTrait ?? null);
			// Build per-(locus, anchor, Ensembl ID) aggregates
			interface ScoredRow {
				locus_id: string;
				lead_rsid: string;
				symbol: string;
				ensembl_id: string | null;
				l2g_max: number;
				l2g_support_json: string;
				coloc_support_json: string;
				coloc_max_h4: number | null;
				coloc_max_clpp: number | null;
				eqtl_tissue_hits: number;
				eqtl_tissues_pipe: string;
				eqtl_support_json: string;
				burden_best_p: number | null;
                burden_support_json: string;
				coding_support: CodingSupport;
				clinvar_support: ClinvarSupport;
				score: ScoreBreakdown;
			}

			const qualified = qualifiedL2gScores(l2gRows, preflight.selectedTrait ?? null);

			const scored: ScoredRow[] = [];
			for (const cg of candidates) {
				const support = cg.ensembl_id ? qualified.scores.get(l2gGeneKey(cg)) : undefined;
				const l2g_max = support?.score ?? 0;
				// Context tables remain queryable; unqualified rows cannot support a gene score.
				const colocSupport = qualifiedColoc.scores.get(l2gGeneKey(cg));
				const coloc_h4 = colocSupport?.h4?.value ?? null;
				const coloc_clpp = colocSupport?.clpp?.value ?? null;
				const eqtlSupport = mergeEqtlSupport(gtex.support.get(gtexGeneKey(cg)), catalogue.get(gtexGeneKey(cg)));
				const tissues = eqtlSupport.tissues;
                const burdenSupport = cg.ensembl_id ? burden.support.get(cg.ensembl_id) : undefined;
				const best_p = burdenSupport?.p_value ?? null;
				const clinvar = "none";
				const coding = "none";
				const breakdown = computeScore(
					{
						l2g_max,
						coloc_max_h4: coloc_h4,
						coloc_max_clpp: coloc_clpp,
						eqtl_tissue_hits: tissues.size,
						burden_best_p: best_p,
						coding_support: coding,
						clinvar_support: clinvar,
					},
					weights,
				);
				scored.push({
					locus_id: cg.locus_id,
					lead_rsid: cg.lead_rsid,
					symbol: cg.symbol,
					ensembl_id: cg.ensembl_id,
					l2g_max,
					l2g_support_json: JSON.stringify(support ?? null),
					coloc_support_json: JSON.stringify(colocSupport ?? null),
					coloc_max_h4: coloc_h4,
					coloc_max_clpp: coloc_clpp,
					eqtl_tissue_hits: tissues.size,
					eqtl_tissues_pipe: [...tissues].join(" | "),
					eqtl_support_json: JSON.stringify(eqtlSupport?.references ?? []),
					burden_support_json: JSON.stringify(burdenSupport?.references ?? []),
					burden_best_p: best_p,
					coding_support: coding,
					clinvar_support: clinvar,
					score: breakdown,
				});
			}

			// Run QC
			const qcRows = scored.map((r) => ({
				locus_id: r.locus_id,
				symbol: r.symbol,
				overall_score: r.score.overall_score,
				l2g_component: r.score.components.l2g,
				coloc_component: r.score.components.coloc,
				eqtl_component: r.score.components.eqtl,
				burden_component: r.score.components.burden,
				coding_component: r.score.components.coding,
				confidence: r.score.confidence,
			}));
			const qc = runScoringQc(anchors, qcRows);
			if (!qc.passed) {
				return createCodeModeError(
					"QC_FAILED",
					`l2g_score QC gates failed (${qc.failures.length}): ${qc.failures.slice(0, 3).join("; ")}`,
					{ failures: qc.failures },
				);
			}

			// Build cross-locus ranking
			const crossLocus = crossLocusRanking(scored);

			// Write scored_candidate_genes + cross_locus_ranked_genes back into the DO
			const scoredFlat = scored.map((r) => ({
				evidence_policy_version: EVIDENCE_POLICY_VERSION,
				locus_id: r.locus_id,
				lead_rsid: r.lead_rsid,
				symbol: r.symbol,
				ensembl_id: r.ensembl_id,
				l2g_max: r.l2g_max,
				l2g_support_json: r.l2g_support_json,
				coloc_support_json: r.coloc_support_json,
				coloc_max_h4: r.coloc_max_h4,
				coloc_max_clpp: r.coloc_max_clpp,
				eqtl_tissue_hits: r.eqtl_tissue_hits,
				eqtl_tissues: r.eqtl_tissues_pipe,
				eqtl_support_json: r.eqtl_support_json,
				burden_support_json: r.burden_support_json,
				burden_best_p: r.burden_best_p,
				coding_support: r.coding_support,
				clinvar_support: r.clinvar_support,
				l2g_component: r.score.components.l2g,
				coloc_component: r.score.components.coloc,
				eqtl_component: r.score.components.eqtl,
				burden_component: r.score.components.burden,
				coding_component: r.score.components.coding,
				overall_score: r.score.overall_score,
				confidence: r.score.confidence,
			}));

			const doId = doNamespace.idFromName(dataAccessId);
			const doStub = doNamespace.get(doId);
			const writeResp = await doStub.fetch(
				new Request("http://localhost/process", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						data: {
							scored_candidate_genes: scoredFlat,
							cross_locus_ranked_genes: crossLocus,
						},
					}),
				}),
			);
			if (!writeResp.ok) {
				return createCodeModeError(
					"STAGING_ERROR",
					`Failed to persist scored tables: HTTP ${writeResp.status}`,
				);
			}

			// Build summary output
			const perLocusTop = new Map<string, typeof scoredFlat[number]>();
			for (const r of scoredFlat) {
				const curr = perLocusTop.get(r.locus_id);
				if (!curr || r.overall_score > curr.overall_score) perLocusTop.set(r.locus_id, r);
			}
			const perLocusTopArr = [...perLocusTop.values()].map((r) => ({
				locus_id: r.locus_id,
				symbol: r.symbol,
				ensembl_id: r.ensembl_id,
				score: Number(r.overall_score.toFixed(4)),
				confidence: r.confidence,
			}));
			const top5 = crossLocus.slice(0, 5).map((r) => ({
				symbol: r.symbol,
				ensembl_id: r.ensembl_id,
				supporting_loci: r.supporting_loci,
				mean_score: Number(r.mean_score.toFixed(4)),
				max_score: Number(r.max_score.toFixed(4)),
				confidence: confidenceLabel(r.max_score),
			}));

			return createCodeModeResponse(
				{
					data_access_id: dataAccessId,
					top_5_cross_locus: top5,
					per_locus_top_gene: perLocusTopArr,
					qc_passed: true,
					evidence_policy_version: EVIDENCE_POLICY_VERSION,
					warnings: [...qc.warnings, ...EVIDENCE_LIMITATIONS, ...gtex.warnings, ...burden.warnings, ...(qualified.excluded ? [`${qualified.excluded} unqualified Open Targets rows excluded from scoring.`] : []), ...(qualifiedColoc.excluded ? [`${qualifiedColoc.excluded} unqualified colocalisation rows excluded from scoring.`] : [])],
					next_step: `Call l2g_synthesize with data_access_id='${dataAccessId}' to emit the full JSON+Markdown contract.`,
				},
				{
					textSummary: `l2g_score: ${scored.length} scored rows across ${perLocusTop.size} loci. Top cross-locus gene: ${top5[0]?.symbol ?? "none"}.`,
					meta: { data_access_id: dataAccessId, scored_count: scored.length },
				},
			);
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			return createCodeModeError("SQL_EXECUTION_ERROR", `l2g_score failed: ${msg}`);
		}
	};

	const reg = (name: string) =>
		server.registerTool(
			name,
			{
				title: "L2G Deterministic Scoring",
				description:
					"Stage 2 of the locus-to-gene composer. Pure SQL over the staged gather tables — " +
					"no network. Applies the SKILL.md scoring formula: overall = 0.40*l2g + 0.25*coloc + " +
					"0.15*eqtl + 0.10*burden + 0.10*coding. Writes scored_candidate_genes and " +
					"cross_locus_ranked_genes back into the DO. Unqualified coloc, burden and coding " +
					"contributions are zero; source context remains queryable. Confidence is heuristic.",
				inputSchema,
			},
			(args, extra) => l2gResponse(() => handler(args as Record<string, unknown>, extra), env || (extra as { env?: ScoreEnv })?.env || {}, name, args, extra),
		);

	reg("mcp_l2g_score");
	reg("l2g_score");
}
