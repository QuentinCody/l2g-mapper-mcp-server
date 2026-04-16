/**
 * QC gates — SKILL.md §"Phase 5".
 * Fail the run when any of these occur:
 *   - No anchors after normalization
 *   - Any locus has candidate genes without score fields
 *   - overall_score outside [0,1]
 *   - Summary section order mismatch (checked at synthesize time)
 *   - Claim of causality without evidence in rationale (best-effort heuristic)
 */

export interface ScoredGeneRow {
	locus_id: string;
	symbol: string;
	overall_score: number | null | undefined;
	l2g_component?: number | null;
	coloc_component?: number | null;
	eqtl_component?: number | null;
	burden_component?: number | null;
	coding_component?: number | null;
	confidence?: string;
}

export interface QcResult {
	passed: boolean;
	failures: string[];
	warnings: string[];
}

export function runScoringQc(
	anchors: Array<unknown>,
	scored: ScoredGeneRow[],
): QcResult {
	const failures: string[] = [];
	const warnings: string[] = [];

	if (!Array.isArray(anchors) || anchors.length === 0) {
		failures.push("No anchors remained after normalization (SKILL.md QC gate).");
	}

	for (const row of scored) {
		if (row.overall_score == null || !Number.isFinite(row.overall_score)) {
			failures.push(`Locus ${row.locus_id} gene ${row.symbol}: missing or non-finite overall_score.`);
			continue;
		}
		if (row.overall_score < 0 || row.overall_score > 1) {
			failures.push(
				`Locus ${row.locus_id} gene ${row.symbol}: overall_score=${row.overall_score} outside [0,1].`,
			);
		}
		const comps = [
			row.l2g_component,
			row.coloc_component,
			row.eqtl_component,
			row.burden_component,
			row.coding_component,
		];
		if (comps.some((c) => c == null || !Number.isFinite(Number(c)))) {
			failures.push(
				`Locus ${row.locus_id} gene ${row.symbol}: one or more score components missing.`,
			);
		}
	}

	if (scored.length === 0) {
		warnings.push("Scoring produced zero candidate-gene rows.");
	}

	return {
		passed: failures.length === 0,
		failures,
		warnings,
	};
}

/** Section-order gate for the Markdown summary (runs inside synthesize). */
export const REQUIRED_SECTIONS = [
	"Objective",
	"Inputs and scope",
	"Anchor variant summary",
	"Per-locus top genes",
	"Cross-locus prioritized genes",
	"Key caveats",
	"Recommended next analyses",
];

export function verifySectionOrder(markdown: string): QcResult {
	const failures: string[] = [];
	let lastIdx = -1;
	for (const section of REQUIRED_SECTIONS) {
		const idx = markdown.indexOf(`## ${section}`);
		if (idx < 0) {
			failures.push(`Markdown summary is missing required section: ${section}`);
			continue;
		}
		if (idx < lastIdx) {
			failures.push(`Markdown summary section out of order: ${section}`);
		}
		lastIdx = idx;
	}
	return { passed: failures.length === 0, failures, warnings: [] };
}
