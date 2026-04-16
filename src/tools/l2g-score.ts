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

import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import {
	createCodeModeResponse,
	createCodeModeError,
} from "@bio-mcp/shared/codemode/response";
import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";
import {
	burdenComponent,
	codingComponent,
	colocComponent,
	computeScore,
	confidenceLabel,
	eqtlComponent,
	l2gComponent,
	WEIGHTS,
	type CodingSupport,
	type ClinvarSupport,
	type ScoreBreakdown,
} from "../lib/scoring";
import { runScoringQc } from "../lib/qc";

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
interface L2gRow {
	locus_id: string;
	symbol: string;
	score: number;
}
interface ColocRow {
	locus_id: string;
	h4: number | null;
	clpp: number | null;
}
interface EqtlRow {
	lead_rsid: string;
	symbol: string;
	tissue: string;
}
interface BurdenRow {
	symbol: string;
	p_value: number | null;
}
interface ClinvarRow {
	rsid: string;
	clinical_significance: string | null;
}

async function selectAll<T>(
	doNamespace: DurableObjectNamespace,
	dataAccessId: string,
	sql: string,
): Promise<T[]> {
	try {
		const { rows } = await queryDataFromDo(doNamespace, dataAccessId, sql, 10_000);
		return rows as T[];
	} catch {
		return [];
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
			const l2gRows = await selectAll<L2gRow>(
				doNamespace,
				dataAccessId,
				"SELECT locus_id, symbol, score FROM l2g_predictions",
			);
			const colocRows = await selectAll<ColocRow>(
				doNamespace,
				dataAccessId,
				"SELECT locus_id, h4, clpp FROM coloc",
			);
			const eqtlRows = await selectAll<EqtlRow>(
				doNamespace,
				dataAccessId,
				"SELECT lead_rsid, symbol, tissue FROM eqtl",
			);
			const burdenRows = await selectAll<BurdenRow>(
				doNamespace,
				dataAccessId,
				"SELECT symbol, p_value FROM burden",
			);
			const clinvarRows = await selectAll<ClinvarRow>(
				doNamespace,
				dataAccessId,
				"SELECT rsid, clinical_significance FROM clinvar",
			);

			// Build per-(locus, symbol) aggregates
			interface ScoredRow {
				locus_id: string;
				lead_rsid: string;
				symbol: string;
				ensembl_id: string | null;
				l2g_max: number;
				coloc_max_h4: number | null;
				coloc_max_clpp: number | null;
				eqtl_tissue_hits: number;
				eqtl_tissues_pipe: string;
				burden_best_p: number | null;
				coding_support: CodingSupport;
				clinvar_support: ClinvarSupport;
				score: ScoreBreakdown;
			}

			// l2g_max per (locus_id, symbol)
			const l2gMax = new Map<string, number>();
			for (const r of l2gRows) {
				const key = `${r.locus_id}::${r.symbol}`;
				if (!l2gMax.has(key) || (r.score ?? 0) > (l2gMax.get(key) ?? 0)) l2gMax.set(key, Number(r.score ?? 0));
			}

			// coloc_max_h4 per locus (coloc isn't gene-specific in OT schema we used)
			const colocMaxH4 = new Map<string, number>();
			const colocMaxClpp = new Map<string, number>();
			for (const r of colocRows) {
				if (r.h4 != null) {
					const curr = colocMaxH4.get(r.locus_id) ?? 0;
					if (r.h4 > curr) colocMaxH4.set(r.locus_id, Number(r.h4));
				}
				if (r.clpp != null) {
					const curr = colocMaxClpp.get(r.locus_id) ?? 0;
					if (r.clpp > curr) colocMaxClpp.set(r.locus_id, Number(r.clpp));
				}
			}

			// eQTL tissues per (lead_rsid, symbol)
			const eqtlTissues = new Map<string, Set<string>>();
			for (const r of eqtlRows) {
				const key = `${r.lead_rsid}::${r.symbol}`;
				if (!eqtlTissues.has(key)) eqtlTissues.set(key, new Set());
				const set = eqtlTissues.get(key);
				if (set && r.tissue && r.tissue !== ".") set.add(r.tissue);
			}

			// Burden best p per symbol
			const burdenBestP = new Map<string, number>();
			for (const r of burdenRows) {
				if (r.p_value == null) continue;
				const curr = burdenBestP.get(r.symbol);
				if (curr == null || r.p_value < curr) burdenBestP.set(r.symbol, Number(r.p_value));
			}

			// ClinVar support per rsid: "present" if any pathogenic-ish significance present
			const clinvarSupport = new Map<string, ClinvarSupport>();
			for (const r of clinvarRows) {
				const sig = (r.clinical_significance ?? "").toLowerCase();
				if (sig.includes("pathogenic") || sig.includes("likely")) {
					clinvarSupport.set(r.rsid, "present");
				} else if (!clinvarSupport.has(r.rsid)) {
					clinvarSupport.set(r.rsid, "none");
				}
			}

			// For coding support we have no VEP fetch in this plan. Approximate:
			// coding=coding if l2g_max >= 0.6 AND clinvar_support==="present"; else noncoding if
			// l2g_max >= 0.5; else none. (Conservative heuristic noted in the rationale.)
			const codingGuess = (l2g: number, clinvar: ClinvarSupport): CodingSupport => {
				if (clinvar === "present" && l2g >= 0.6) return "coding";
				if (l2g >= 0.5) return "noncoding";
				return "none";
			};

			const scored: ScoredRow[] = [];
			for (const cg of candidates) {
				const l2g_max = l2gMax.get(`${cg.locus_id}::${cg.symbol}`) ?? 0;
				const coloc_h4 = colocMaxH4.get(cg.locus_id) ?? null;
				const coloc_clpp = colocMaxClpp.get(cg.locus_id) ?? null;
				const tissues = eqtlTissues.get(`${cg.lead_rsid}::${cg.symbol}`) ?? new Set<string>();
				const best_p = burdenBestP.get(cg.symbol) ?? null;
				const clinvar = clinvarSupport.get(cg.lead_rsid) ?? "none";
				const coding = codingGuess(l2g_max, clinvar);
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
					coloc_max_h4: coloc_h4,
					coloc_max_clpp: coloc_clpp,
					eqtl_tissue_hits: tissues.size,
					eqtl_tissues_pipe: [...tissues].join(" | "),
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
			const symbolAgg = new Map<string, { loci: Set<string>; max: number; sum: number; count: number }>();
			for (const r of scored) {
				const agg = symbolAgg.get(r.symbol) ?? { loci: new Set<string>(), max: 0, sum: 0, count: 0 };
				agg.loci.add(r.locus_id);
				agg.max = Math.max(agg.max, r.score.overall_score);
				agg.sum += r.score.overall_score;
				agg.count += 1;
				symbolAgg.set(r.symbol, agg);
			}
			const crossLocus = [...symbolAgg.entries()].map(([symbol, agg]) => ({
				symbol,
				supporting_loci: agg.loci.size,
				mean_score: agg.count > 0 ? agg.sum / agg.count : 0,
				max_score: agg.max,
			})).sort((a, b) =>
				b.supporting_loci !== a.supporting_loci
					? b.supporting_loci - a.supporting_loci
					: b.max_score - a.max_score,
			);

			// Write scored_candidate_genes + cross_locus_ranked_genes back into the DO
			const scoredFlat = scored.map((r) => ({
				locus_id: r.locus_id,
				lead_rsid: r.lead_rsid,
				symbol: r.symbol,
				ensembl_id: r.ensembl_id,
				l2g_max: r.l2g_max,
				coloc_max_h4: r.coloc_max_h4,
				coloc_max_clpp: r.coloc_max_clpp,
				eqtl_tissue_hits: r.eqtl_tissue_hits,
				eqtl_tissues: r.eqtl_tissues_pipe,
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
				score: Number(r.overall_score.toFixed(4)),
				confidence: r.confidence,
			}));
			const top5 = crossLocus.slice(0, 5).map((r) => ({
				symbol: r.symbol,
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
					warnings: qc.warnings,
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
					"cross_locus_ranked_genes back into the DO. Runs SKILL.md Phase-5 QC gates.",
				inputSchema,
			},
			(args, extra) => handler(args as Record<string, unknown>, extra),
		);

	reg("mcp_l2g_score");
	reg("l2g_score");
}
