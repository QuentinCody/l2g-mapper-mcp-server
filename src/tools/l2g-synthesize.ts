/**
 * l2g_synthesize — Tier 3 of the composer.
 *
 * Pure read from the scored tables; emits:
 *   1. Full JSON contract per SKILL.md §"JSON contract"
 *   2. Markdown summary in exact section order per SKILL.md §"Markdown summary contract"
 *   3. Three Mermaid figure strings (locus_gene_heatmap, locus_score_decomposition, tissue_support_dotplot)
 *      inlined under the appropriate sections.
 */

import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import {
	createCodeModeResponse,
	createCodeModeError,
} from "@bio-mcp/shared/codemode/response";
import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";
import {
	locusGeneHeatmap,
	locusScoreDecomposition,
	tissueSupportDotplot,
	type HeatmapCell,
	type LocusDecomposition,
	type TissueDot,
} from "../lib/mermaid";
import { verifySectionOrder } from "../lib/qc";

interface SynthEnv {
	L2G_DATA_DO?: DurableObjectNamespace;
}

const inputSchema = {
	data_access_id: z.string().min(1).describe("data_access_id from l2g_gather (after l2g_score has run)"),
};

interface MetaRow {
	trait_query: string | null;
	efo_id: string | null;
	efo_label: string | null;
	generated_at: string | null;
	sources_queried: string | null;
	target_gene: string | null;
	warnings_json: string | null;
}
interface AnchorRow {
	rsid: string;
	p_value: number | null;
	lead_trait: string | null;
	locus_id: string | null;
	grch38_chr?: string | null;
	grch38_pos?: number | null;
	grch38_ref?: string | null;
	grch38_alt?: string | null;
}
interface ScoredRow {
	locus_id: string;
	lead_rsid: string;
	symbol: string;
	ensembl_id: string | null;
	l2g_max: number;
	coloc_max_h4: number | null;
	coloc_max_clpp: number | null;
	eqtl_tissue_hits: number;
	eqtl_tissues: string | null;
	burden_best_p: number | null;
	coding_support: string;
	clinvar_support: string;
	l2g_component: number;
	coloc_component: number;
	eqtl_component: number;
	burden_component: number;
	coding_component: number;
	overall_score: number;
	confidence: string;
}
interface CrossRow {
	symbol: string;
	supporting_loci: number;
	mean_score: number;
	max_score: number;
}

async function safeQuery<T>(doNs: DurableObjectNamespace, id: string, sql: string): Promise<T[]> {
	try {
		const res = await queryDataFromDo(doNs, id, sql, 10_000);
		return res.rows as T[];
	} catch {
		return [];
	}
}

function parsePipe(value: string | null | undefined): string[] {
	if (!value) return [];
	return value.split("|").map((s) => s.trim()).filter((s) => s.length > 0);
}

export function registerL2gSynthesize(server: McpServer, env?: SynthEnv): void {
	const handler = async (args: Record<string, unknown>, extra: unknown) => {
		const runtimeEnv = env || (extra as { env?: SynthEnv })?.env;
		const dataAccessId = String(args.data_access_id ?? "");
		if (!dataAccessId) return createCodeModeError("INVALID_ARGUMENTS", "data_access_id is required");
		if (!runtimeEnv?.L2G_DATA_DO) {
			return createCodeModeError("DATA_ACCESS_ERROR", "L2G_DATA_DO namespace unavailable");
		}
		const doNs = runtimeEnv.L2G_DATA_DO;

		try {
			const [metaRows, anchorRows, scoredRows, crossRows] = await Promise.all([
				safeQuery<MetaRow>(doNs, dataAccessId, "SELECT * FROM l2g_meta"),
				safeQuery<AnchorRow>(doNs, dataAccessId, "SELECT * FROM anchors"),
				safeQuery<ScoredRow>(doNs, dataAccessId, "SELECT * FROM scored_candidate_genes ORDER BY overall_score DESC"),
				safeQuery<CrossRow>(doNs, dataAccessId, "SELECT * FROM cross_locus_ranked_genes ORDER BY supporting_loci DESC, max_score DESC"),
			]);

			if (scoredRows.length === 0) {
				return createCodeModeError(
					"DATA_ACCESS_ERROR",
					`No scored rows under data_access_id=${dataAccessId}. Run l2g_score first.`,
				);
			}

			const meta = metaRows[0] ?? {
				trait_query: null,
				efo_id: null,
				efo_label: null,
				generated_at: null,
				sources_queried: null,
				target_gene: null,
				warnings_json: null,
			};

			// ── Build JSON contract per SKILL.md ────────────────────────────
			const anchorsJson = anchorRows.map((a) => ({
				rsid: a.rsid,
				grch38: a.grch38_chr && a.grch38_pos
					? { chr: a.grch38_chr, pos: a.grch38_pos, ref: a.grch38_ref, alt: a.grch38_alt }
					: null,
				lead_trait: a.lead_trait,
				p_value: a.p_value,
				cohort: null,
			}));

			// Group scored rows by locus
			const byLocus = new Map<string, ScoredRow[]>();
			for (const r of scoredRows) {
				if (!byLocus.has(r.locus_id)) byLocus.set(r.locus_id, []);
				byLocus.get(r.locus_id)?.push(r);
			}
			const loci = [...byLocus.entries()].map(([locus_id, rows]) => {
				rows.sort((a, b) => b.overall_score - a.overall_score);
				return {
					locus_id,
					lead_rsid: rows[0]?.lead_rsid ?? null,
					candidate_genes: rows.map((r) => ({
						symbol: r.symbol,
						ensembl_id: r.ensembl_id,
						overall_score: Number(r.overall_score.toFixed(4)),
						confidence: r.confidence,
						evidence: {
							l2g_max: Number(r.l2g_max?.toFixed?.(4) ?? r.l2g_max ?? 0),
							coloc_max_h4: r.coloc_max_h4,
							coloc_max_clpp: r.coloc_max_clpp,
							eqtl_tissues: parsePipe(r.eqtl_tissues),
							rare_variant_support:
								r.burden_best_p == null
									? "none"
									: r.burden_best_p < 2.5e-6
										? "strong"
										: r.burden_best_p < 0.05
											? "nominal"
											: "none",
							coding_support: r.coding_support,
							clinvar_support: r.clinvar_support,
							gnomad_context: null,
							hpa_tissue_support: [] as string[],
						},
						rationale: buildRationale(r),
						limitations: buildLimitations(r),
					})),
				};
			});

			const warningsMeta: string[] = meta.warnings_json
				? (() => {
					try {
						const parsed = JSON.parse(meta.warnings_json);
						return Array.isArray(parsed) ? parsed.map(String) : [];
					} catch {
						return [];
					}
				})()
				: [];

			const jsonContract = {
				meta: {
					trait_query: meta.trait_query,
					efo_id: meta.efo_id,
					efo_label: meta.efo_label,
					generated_at: meta.generated_at,
					sources_queried: (meta.sources_queried ?? "").split("|").map((s) => s.trim()).filter(Boolean),
				},
				anchors: anchorsJson,
				loci,
				cross_locus_ranked_genes: crossRows.map((r) => ({
					symbol: r.symbol,
					supporting_loci: r.supporting_loci,
					mean_score: Number(r.mean_score.toFixed(4)),
					max_score: Number(r.max_score.toFixed(4)),
				})),
				warnings: warningsMeta,
				limitations: [
					"coding_support is heuristic (L2G + ClinVar-derived) — synchronous L2G skips live VEP.",
					"Default synchronous caps: max_loci=8, max_anchor_associations=300; full-scope mode deferred (ADR-005).",
				],
			};

			// ── Build Mermaid figures ──────────────────────────────────────
			// Heatmap: top-8 cross-locus genes × 5 evidence components
			const topGenes = crossRows.slice(0, 8).map((r) => r.symbol);
			const heatmapRows = scoredRows
				.filter((r) => topGenes.includes(r.symbol))
				.reduce<Map<string, ScoredRow>>((m, r) => {
					const curr = m.get(r.symbol);
					if (!curr || r.overall_score > curr.overall_score) m.set(r.symbol, r);
					return m;
				}, new Map());
			const heatmapCells: HeatmapCell[] = [];
			for (const sym of topGenes) {
				const r = heatmapRows.get(sym);
				if (!r) continue;
				heatmapCells.push({ row: sym, col: "L2G", value: r.l2g_component });
				heatmapCells.push({ row: sym, col: "coloc", value: r.coloc_component });
				heatmapCells.push({ row: sym, col: "eQTL", value: r.eqtl_component });
				heatmapCells.push({ row: sym, col: "burden", value: r.burden_component });
				heatmapCells.push({ row: sym, col: "coding", value: r.coding_component });
			}
			const heatmap = locusGeneHeatmap(heatmapCells);

			const decompRows: LocusDecomposition[] = [];
			for (const [locus_id, rows] of byLocus) {
				const top = rows[0];
				if (!top) continue;
				decompRows.push({
					locus_id,
					gene: top.symbol,
					components: {
						l2g: Number(top.l2g_component),
						coloc: Number(top.coloc_component),
						eqtl: Number(top.eqtl_component),
						burden: Number(top.burden_component),
						coding: Number(top.coding_component),
					},
				});
			}
			const decomp = locusScoreDecomposition(decompRows);

			const dots: TissueDot[] = [];
			for (const r of scoredRows) {
				if (!topGenes.includes(r.symbol)) continue;
				for (const tissue of parsePipe(r.eqtl_tissues)) {
					dots.push({ gene: r.symbol, tissue, present: true });
				}
			}
			const dotplot = tissueSupportDotplot(dots);

			// ── Markdown summary in SKILL.md section order ─────────────────
			const md = buildMarkdown(jsonContract, heatmap.mermaid, decomp.mermaid, dotplot.mermaid);

			// QC section-order check
			const sectionQc = verifySectionOrder(md);
			if (!sectionQc.passed) {
				return createCodeModeError(
					"QC_FAILED",
					`Markdown section order QC failed: ${sectionQc.failures.join("; ")}`,
				);
			}

			return createCodeModeResponse(
				{
					data_access_id: dataAccessId,
					json: jsonContract,
					markdown_summary: md,
					mermaid_figures: [heatmap, decomp, dotplot],
				},
				{
					textSummary: `l2g_synthesize emitted JSON (${jsonContract.loci.length} loci) + Markdown + 3 Mermaid figures.`,
					meta: { data_access_id: dataAccessId, loci_count: jsonContract.loci.length },
				},
			);
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			return createCodeModeError("SQL_EXECUTION_ERROR", `l2g_synthesize failed: ${msg}`);
		}
	};

	const reg = (name: string) =>
		server.registerTool(
			name,
			{
				title: "L2G Synthesize Final Contract",
				description:
					"Stage 3 of the locus-to-gene composer. Pure read — no network. Emits the full " +
					"SKILL.md JSON contract, the Markdown summary in exact section order (Objective, " +
					"Inputs and scope, Anchor variant summary, Per-locus top genes, Cross-locus " +
					"prioritized genes, Key caveats, Recommended next analyses), and three Mermaid " +
					"figures (locus_gene_heatmap, locus_score_decomposition, tissue_support_dotplot). " +
					"No PNGs — Mermaid only.",
				inputSchema,
			},
			(args, extra) => handler(args as Record<string, unknown>, extra),
		);

	reg("mcp_l2g_synthesize");
	reg("l2g_synthesize");
}

function buildRationale(r: ScoredRow): string[] {
	const out: string[] = [];
	if (r.l2g_max > 0) out.push(`OpenTargets L2G score ${r.l2g_max.toFixed(3)} (component ${r.l2g_component.toFixed(2)} × weight 0.40).`);
	if (r.coloc_max_h4 != null) out.push(`Coloc h4=${r.coloc_max_h4.toFixed(3)} (component ${r.coloc_component.toFixed(2)} × weight 0.25).`);
	if (r.eqtl_tissue_hits > 0) out.push(`GTEx single-tissue eQTL in ${r.eqtl_tissue_hits} tissue(s) (component ${r.eqtl_component.toFixed(2)} × weight 0.15).`);
	if (r.burden_best_p != null) out.push(`Genebass burden best p=${r.burden_best_p.toExponential(2)} (component ${r.burden_component.toFixed(2)} × weight 0.10).`);
	if (r.coding_support !== "none" || r.clinvar_support !== "none") {
		out.push(`Coding/ClinVar support: ${r.coding_support}/${r.clinvar_support} (component ${r.coding_component.toFixed(2)} × weight 0.10).`);
	}
	return out;
}

function buildLimitations(r: ScoredRow): string[] {
	const out: string[] = [];
	if (r.l2g_max === 0) out.push("No OpenTargets L2G score available for this gene at this locus.");
	if (r.coloc_max_h4 == null && r.coloc_max_clpp == null) out.push("No colocalisation evidence available.");
	if (r.eqtl_tissue_hits === 0) out.push("No GTEx single-tissue eQTL support.");
	if (r.burden_best_p == null) out.push("No Genebass burden evidence retrieved.");
	return out;
}

function buildMarkdown(
	json: {
		meta: { trait_query: string | null; efo_id: string | null; efo_label: string | null; generated_at: string | null };
		anchors: Array<{ rsid: string; p_value: number | null; lead_trait: string | null }>;
		loci: Array<{ locus_id: string; lead_rsid: string | null; candidate_genes: Array<{ symbol: string; overall_score: number; confidence: string; evidence: { l2g_max: number; coloc_max_h4: number | null; eqtl_tissues: string[] } }> }>;
		cross_locus_ranked_genes: Array<{ symbol: string; supporting_loci: number; mean_score: number; max_score: number }>;
		warnings: string[];
		limitations: string[];
	},
	heatmapMermaid: string,
	decompMermaid: string,
	dotplotMermaid: string,
): string {
	const lines: string[] = [];
	lines.push("## Objective");
	lines.push(
		`Map GWAS loci for \`${json.meta.trait_query ?? "seeded variants"}\` to ranked candidate genes using a deterministic evidence chain (GWAS, Ensembl coordinates, Open Targets L2G/coloc, GTEx eQTL, Genebass burden, and optional ClinVar/gnomAD/HPA context).`,
	);
	lines.push("");

	lines.push("## Inputs and scope");
	lines.push(`- Trait query: \`${json.meta.trait_query ?? "."}\``);
	lines.push(`- EFO ID: \`${json.meta.efo_id ?? "unresolved"}\``);
	lines.push(`- EFO label: \`${json.meta.efo_label ?? "."}\``);
	lines.push(`- Anchors retained: \`${json.anchors.length}\``);
	lines.push(`- Generated at: \`${json.meta.generated_at ?? "."}\``);
	lines.push("");

	lines.push("## Anchor variant summary");
	if (json.anchors.length === 0) {
		lines.push("No anchors were retained after normalization.");
	} else {
		for (const a of json.anchors.slice(0, 20)) {
			const pTxt = typeof a.p_value === "number" ? a.p_value.toExponential(2) : ".";
			lines.push(`- \`${a.rsid}\` | p=${pTxt} | trait=${a.lead_trait ?? "."}`);
		}
	}
	lines.push("");

	lines.push("## Per-locus top genes");
	lines.push("");
	lines.push("```mermaid");
	lines.push(decompMermaid);
	lines.push("```");
	lines.push("");
	if (json.loci.length === 0) {
		lines.push("No loci available.");
	} else {
		for (const locus of json.loci) {
			lines.push(`### ${locus.locus_id} (lead \`${locus.lead_rsid ?? "."}\`)`);
			if (locus.candidate_genes.length === 0) {
				lines.push("- No candidate genes scored.");
				continue;
			}
			for (const g of locus.candidate_genes.slice(0, 5)) {
				const l2g = g.evidence.l2g_max.toFixed(3);
				const coloc = typeof g.evidence.coloc_max_h4 === "number" ? g.evidence.coloc_max_h4.toFixed(3) : ".";
				const tissues = g.evidence.eqtl_tissues.filter((t) => t && t !== ".").length;
				lines.push(`- \`${g.symbol}\` | score=${g.overall_score.toFixed(3)} (${g.confidence}) | L2G=${l2g} | coloc=${coloc} | eQTL tissues=${tissues}`);
			}
		}
	}
	lines.push("");

	lines.push("## Cross-locus prioritized genes");
	lines.push("");
	lines.push("```mermaid");
	lines.push(heatmapMermaid);
	lines.push("```");
	lines.push("");
	lines.push("```mermaid");
	lines.push(dotplotMermaid);
	lines.push("```");
	lines.push("");
	if (json.cross_locus_ranked_genes.length === 0) {
		lines.push("No cross-locus aggregated ranking available.");
	} else {
		for (const r of json.cross_locus_ranked_genes.slice(0, 15)) {
			lines.push(`- \`${r.symbol}\` | supporting_loci=${r.supporting_loci} | mean_score=${r.mean_score.toFixed(3)} | max_score=${r.max_score.toFixed(3)}`);
		}
	}
	lines.push("");

	lines.push("## Key caveats");
	const caveats = [...json.limitations, ...json.warnings];
	if (caveats.length === 0) {
		lines.push("- No caveats surfaced.");
	} else {
		for (const c of caveats) lines.push(`- ${c}`);
	}
	lines.push("");

	lines.push("## Recommended next analyses");
	lines.push("- Run fine-mapping or targeted credible-set review for top cross-locus genes.");
	lines.push("- Validate coding / rare-variant support via targeted VEP / ClinVar manual review.");
	lines.push("- For tissue-specific hypotheses, cross-check HPA protein-level expression and cell-type scRNA-seq.");
	lines.push("- Consider the deferred full-scope L2G mode (ADR-005) for 1200-anchor / 25-loci runs.");

	return lines.join("\n");
}
