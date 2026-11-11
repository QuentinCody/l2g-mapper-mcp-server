/**
 * l2g_synthesize — Tier 3 of the composer.
 *
 * Pure read from the scored tables; emits:
 *   1. Full JSON contract per SKILL.md §"JSON contract"
 *   2. Markdown summary in exact section order per SKILL.md §"Markdown summary contract"
 *   3. Three Mermaid figure strings (locus_gene_heatmap, locus_score_decomposition, tissue_support_dotplot)
 *      inlined under the appropriate sections.
 */

import { readCompleteTable } from "../lib/complete-table";
import type { McpServer } from "@bio-mcp/shared/mcp";
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
import { EVIDENCE_LIMITATIONS, EVIDENCE_POLICY_VERSION } from "../lib/evidence-policy";
import { l2gResponse } from "../lib/tool-result";
import { synthesisEvidence } from "../lib/synthesis-evidence";
import { loadGnomadContext } from "../lib/gnomad-synthesis";
import { loadFinnGenContext } from "../lib/finngen-synthesis";
import { loadHpaContext } from "../lib/hpa-synthesis";
import { loadGenebassContext } from "../lib/genebass-synthesis";
import { loadClinvarContext } from "../lib/clinvar-synthesis";
import { loadEqtlCatalogueContext } from "../lib/eqtl-catalogue-synthesis";
import { loadEqtlR8Observation, r8SynthesisContext } from "../lib/eqtl-r8-synthesis";

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
	caps_json?: string | null;
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
	evidence_policy_version?: string;
	locus_id: string;
	lead_rsid: string;
	symbol: string;
	ensembl_id: string | null;
	l2g_max: number;
	l2g_support_json?: string | null;
	coloc_support_json?: string | null;
	eqtl_support_json?: string | null;
	burden_support_json?: string | null;
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
	ensembl_id: string | null;
	supporting_loci: number;
	mean_score: number;
	max_score: number;
}

async function readComplete<T>(doNs: DurableObjectNamespace, id: string, sql: string): Promise<T[]> {
	const res = await queryDataFromDo(doNs, id, sql, 10_000);
	if (res.truncated || res.count_capped || (res.total_matching != null && res.total_matching > res.rows.length)) throw new Error("Synthesis requires complete staged input tables; the SQL read was capped.");
	return res.rows as T[];
}

function parsePipe(value: string | null | undefined): string[] {
	if (!value) return [];
	return value.split("|").map((s) => s.trim()).filter((s) => s.length > 0);
}

function parseWarnings(value: string | null): string[] {
	try {
		const parsed: unknown = JSON.parse(value ?? "[]");
		return Array.isArray(parsed) ? parsed.map(String) : [];
	} catch {
		return [];
	}
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
				readComplete<MetaRow>(doNs, dataAccessId, "SELECT * FROM l2g_meta"),
				readComplete<AnchorRow>(doNs, dataAccessId, "SELECT * FROM anchors"),
				readCompleteTable<ScoredRow>(doNs, dataAccessId, "scored_candidate_genes", "SELECT * FROM scored_candidate_genes ORDER BY overall_score DESC"),
				readComplete<CrossRow>(doNs, dataAccessId, "SELECT * FROM cross_locus_ranked_genes ORDER BY supporting_loci DESC, max_score DESC"),
			]);

			if (scoredRows.length === 0) {
				return createCodeModeError(
					"DATA_ACCESS_ERROR",
					`No scored rows under data_access_id=${dataAccessId}. Run l2g_score first.`,
				);
			}

			if (scoredRows.some((row) => row.evidence_policy_version !== EVIDENCE_POLICY_VERSION)) {
				return createCodeModeError("DATA_ACCESS_ERROR", "Stored scores use an older evidence policy. Re-run l2g_score before synthesis.");
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

			const gnomadContext = await loadGnomadContext(doNs, dataAccessId, meta.caps_json, parsePipe(meta.sources_queried).includes("gnomad"));
			const finngenContext = await loadFinnGenContext(doNs, dataAccessId, meta.caps_json, anchorRows);
			const hpaContext = await loadHpaContext(doNs, dataAccessId, meta.caps_json, parsePipe(meta.sources_queried).includes("hpa"));
			const genebassContext = await loadGenebassContext(doNs, dataAccessId, meta.caps_json, parsePipe(meta.sources_queried).includes("genebass"));
			const clinvarContext = await loadClinvarContext(doNs, dataAccessId, meta.caps_json, parsePipe(meta.sources_queried).includes("clinvar"));
			const eqtlCatalogueContext = await loadEqtlCatalogueContext(doNs, dataAccessId, meta.caps_json, anchorRows);
			const eqtlR8Context = r8SynthesisContext(await loadEqtlR8Observation(doNs, dataAccessId, meta.caps_json, anchorRows), dataAccessId);
			const anchorsJson = anchorRows.map((a) => ({
				rsid: a.rsid,
				clinvar_context: clinvarContext.get(a.rsid) ?? [],
				finngen_context: finngenContext.get(a.rsid) ?? [],
				eqtl_catalogue_context: eqtlCatalogueContext.get(a.rsid) ?? [],
				eqtl_catalogue_r8_context: eqtlR8Context.get(a.rsid) ?? [],
				grch38: a.grch38_chr && a.grch38_pos
					? { chr: a.grch38_chr, pos: a.grch38_pos, ref: a.grch38_ref, alt: a.grch38_alt }
					: null,
				lead_trait: a.lead_trait,
				p_value: a.p_value,
				cohort: null,
			}));

			// Group scored rows by locus
			const byLocus = groupScoredLoci(scoredRows);
			const loci = [...byLocus.entries()].map(([locus_id, rows]) => {
				rows.sort((a, b) => b.overall_score - a.overall_score);
				return {
					locus_id,
					lead_rsid: rows[0]?.lead_rsid ?? null,
					lead_rsids: [...new Set(rows.map(r => r.lead_rsid))].sort(),
					candidate_genes: rows.map((r) => ({
						symbol: r.symbol,
						ensembl_id: r.ensembl_id,
						lead_rsid: r.lead_rsid,
						overall_score: Number(r.overall_score.toFixed(4)),
						confidence: r.confidence,
						evidence: { ...synthesisEvidence(r, r.ensembl_id ? gnomadContext.get(r.ensembl_id) ?? null : null),
							hpa_context: r.ensembl_id ? hpaContext.get(r.ensembl_id) ?? [] : [],
							genebass_context: r.ensembl_id ? genebassContext.get(r.ensembl_id) ?? [] : [] },
						rationale: buildRationale(r),
						limitations: buildLimitations(r),
					})),
				};
			});

			const warningsMeta = parseWarnings(meta.warnings_json);

			const jsonContract = {
				meta: {
					evidence_policy_version: EVIDENCE_POLICY_VERSION,
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
					ensembl_id: r.ensembl_id,
					supporting_loci: r.supporting_loci,
					mean_score: Number(r.mean_score.toFixed(4)),
					max_score: Number(r.max_score.toFixed(4)),
				})),
				warnings: warningsMeta,
				limitations: [
					...EVIDENCE_LIMITATIONS,
					"Default synchronous caps: max_loci=8, max_anchor_associations=300; full-scope mode deferred (ADR-005).",
				],
			};

			// ── Build Mermaid figures ──────────────────────────────────────
			// Heatmap: top-8 cross-locus genes × 5 evidence components
			const { decompRows, topGenes, heatmap } = buildSynthesisHeatmap(crossRows, scoredRows);
			for (const [locus_id, rows] of byLocus) {
				const top = rows[0];
				if (!top) continue;
				decompRows.push({
					locus_id,
					gene: `${top.symbol} (${top.ensembl_id ?? "unresolved"})`,
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
				if (!r.ensembl_id || !topGenes.includes(r.ensembl_id)) continue;
				for (const tissue of parsePipe(r.eqtl_tissues)) {
					dots.push({ gene: `${r.symbol} (${r.ensembl_id})`, tissue, present: true });
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
					"Stage 3 of the locus-to-gene composer. Reads staged evidence without upstream requests; large outputs are staged. Emits the full " +
					"SKILL.md JSON contract, the Markdown summary in exact section order (Objective, " +
					"Inputs and scope, Anchor variant summary, Per-locus top genes, Cross-locus " +
					"prioritized genes, Key caveats, Recommended next analyses), and three Mermaid " +
					"figures (locus_gene_heatmap, locus_score_decomposition, tissue_support_dotplot). " +
					"No PNGs — Mermaid only.",
				inputSchema,
			},
			(args, extra) => l2gResponse(() => handler(args as Record<string, unknown>, extra), env || (extra as { env?: SynthEnv })?.env || {}, name, args, extra),
		);

	reg("mcp_l2g_synthesize");
	reg("l2g_synthesize");
}

function groupScoredLoci(scoredRows: ScoredRow[]) {
  const byLocus = new Map<string, ScoredRow[]>();
  for (const r of scoredRows) {
    if (!byLocus.has(r.locus_id)) byLocus.set(r.locus_id, []);
    byLocus.get(r.locus_id)?.push(r);
  }
  return byLocus;
}

function buildSynthesisHeatmap(crossRows: CrossRow[], scoredRows: ScoredRow[]) {
  const topGenes = crossRows.slice(0, 8).map((r) => r.ensembl_id).filter((id): id is string => Boolean(id));
  const heatmapRows = scoredRows
    .filter((r) => r.ensembl_id && topGenes.includes(r.ensembl_id))
    .reduce<Map<string, ScoredRow>>((m, r) => {
      const curr = m.get(r.ensembl_id!);
      if (!curr || r.overall_score > curr.overall_score) m.set(r.ensembl_id!, r);
      return m;
    }, new Map());
  const heatmapCells: HeatmapCell[] = [];
  for (const sym of topGenes) {
    const r = heatmapRows.get(sym);
    if (!r) continue;
    const label = `${r.symbol} (${sym})`;
    heatmapCells.push({ row: label, col: "L2G", value: r.l2g_component });
    heatmapCells.push({ row: label, col: "coloc", value: r.coloc_component });
    heatmapCells.push({ row: label, col: "eQTL", value: r.eqtl_component });
    heatmapCells.push({ row: label, col: "burden", value: r.burden_component });
    heatmapCells.push({ row: label, col: "coding", value: r.coding_component });
  }
  const heatmap = locusGeneHeatmap(heatmapCells);

  const decompRows: LocusDecomposition[] = [];
  return { decompRows, topGenes, heatmap };
}

function buildRationale(r: ScoredRow): string[] {
	const out: string[] = [];
	if (r.l2g_max > 0) out.push(`OpenTargets L2G score ${r.l2g_max.toFixed(3)} (component ${r.l2g_component.toFixed(2)} × weight 0.40).`);
	if (r.coloc_max_h4 != null) out.push(`Coloc h4=${r.coloc_max_h4.toFixed(3)} (component ${r.coloc_component.toFixed(2)} × weight 0.25).`);
	else if (r.coloc_max_clpp != null) out.push(`eCAVIAR CLPP=${r.coloc_max_clpp.toFixed(3)} (component ${r.coloc_component.toFixed(2)} × weight 0.25).`);
	if (r.eqtl_tissue_hits > 0) out.push(`GTEx single-tissue eQTL in ${r.eqtl_tissue_hits} tissue(s) (component ${r.eqtl_component.toFixed(2)} × weight 0.15).`);
	if (r.burden_best_p != null) out.push(`Open Targets exact-trait gene-burden best p=${r.burden_best_p.toExponential(2)} (component ${r.burden_component.toFixed(2)} before weighting).`);
	if (r.coding_support !== "none" || r.clinvar_support !== "none") {
		out.push(`Coding/ClinVar support: ${r.coding_support}/${r.clinvar_support} (component ${r.coding_component.toFixed(2)} × weight 0.10).`);
	}
	return out;
}

function buildLimitations(r: ScoredRow): string[] {
	const out: string[] = [];
	if (r.l2g_max === 0) out.push("No qualified Open Targets L2G contribution for this gene and anchor; this does not establish evidence absence.");
	if (r.coloc_max_h4 == null && r.coloc_max_clpp == null) out.push("No qualified gene-specific colocalisation contribution; inspect the coloc context table.");
	if (r.eqtl_tissue_hits === 0) out.push("No retrieved GTEx tissue support matched this gene ID and anchor; coverage is incomplete.");
	if (r.burden_best_p == null) out.push("No qualified trait-specific burden contribution; inspect opentargets_evidence and the separate Genebass burden context.");
	return out;
}

function buildMarkdown(
	json: {
		meta: { trait_query: string | null; efo_id: string | null; efo_label: string | null; generated_at: string | null };
		anchors: Array<{ rsid: string; p_value: number | null; lead_trait: string | null }>;
		loci: Array<{ locus_id: string; lead_rsid: string | null; lead_rsids: string[]; candidate_genes: Array<{ symbol: string; ensembl_id: string | null; lead_rsid: string; overall_score: number; confidence: string; evidence: { l2g_max: number; coloc_max_h4: number | null; coloc_max_clpp: number | null; eqtl_tissues: string[] } }> }>;
		cross_locus_ranked_genes: Array<{ symbol: string; ensembl_id: string | null; supporting_loci: number; mean_score: number; max_score: number }>;
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

	appendLocusMarkdown(lines, decompMermaid, json);

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
			lines.push(`- \`${r.symbol}\` (${r.ensembl_id ?? "unresolved gene ID"}) | supporting_loci=${r.supporting_loci} | mean_score=${r.mean_score.toFixed(3)} | max_score=${r.max_score.toFixed(3)}`);
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

function appendLocusMarkdown(lines: string[], decompMermaid: string, json: Parameters<typeof buildMarkdown>[0]) {
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
      lines.push(`### ${locus.locus_id} (anchors: ${locus.lead_rsids.map(rsid => `\`${rsid}\``).join(", ")})`);
      if (locus.candidate_genes.length === 0) {
        lines.push("- No candidate genes scored.");
        continue;
      }
      for (const g of locus.candidate_genes.slice(0, 5)) {
        const l2g = g.evidence.l2g_max.toFixed(3);
        const coloc = typeof g.evidence.coloc_max_h4 === "number" ? `H4:${g.evidence.coloc_max_h4.toFixed(3)}` : typeof g.evidence.coloc_max_clpp === "number" ? `CLPP:${g.evidence.coloc_max_clpp.toFixed(3)}` : ".";
        const tissues = g.evidence.eqtl_tissues.filter((t) => t && t !== ".").length;
        lines.push(`- \`${g.symbol}\` (${g.ensembl_id ?? "unresolved"}; anchor ${g.lead_rsid}) | score=${g.overall_score.toFixed(3)} (${g.confidence}) | L2G=${l2g} | coloc=${coloc} | eQTL tissues=${tissues}`);
      }
    }
  }
  lines.push("");
}
