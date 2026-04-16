/**
 * Deterministic L2G scoring — ports SKILL.md §"Scoring Rules" verbatim.
 *
 *   overall_score = 0.40*l2g + 0.25*coloc + 0.15*eqtl + 0.10*burden + 0.10*coding
 *
 * Confidence buckets:
 *   High    >= 0.75
 *   Medium  >= 0.55
 *   Low     >= 0.35
 *   VeryLow <  0.35
 *
 * Per-component formulas (verbatim from SKILL.md):
 *   - l2g_component:      max L2G score for the gene in the locus (0..1)
 *   - coloc_component:    max h4 (or clpp when only CLPP available), clipped 0..1
 *   - eqtl_component:     min(1, relevant_tissue_hits / 3)
 *   - burden_component:   1.0 if p<2.5e-6 ; 0.6 if 2.5e-6<=p<0.05 ; 0.0 otherwise
 *   - coding_component:
 *       1.0  coding + ClinVar support
 *       0.6  coding alone
 *       0.3  noncoding-in-gene
 *       0.0  otherwise
 */

export interface Weights {
	l2g: number;
	coloc: number;
	eqtl: number;
	burden: number;
	coding: number;
}

export const WEIGHTS: Weights = Object.freeze({
	l2g: 0.40,
	coloc: 0.25,
	eqtl: 0.15,
	burden: 0.10,
	coding: 0.10,
});

export const BURDEN_STRONG_P = 2.5e-6;
export const BURDEN_NOMINAL_P = 0.05;

export const CONFIDENCE_HIGH = 0.75;
export const CONFIDENCE_MEDIUM = 0.55;
export const CONFIDENCE_LOW = 0.35;

export type Confidence = "High" | "Medium" | "Low" | "VeryLow";

export function clamp01(v: number): number {
	if (!Number.isFinite(v)) return 0;
	return Math.max(0, Math.min(1, v));
}

export function l2gComponent(maxL2g: number | null | undefined): number {
	return clamp01(Number(maxL2g ?? 0));
}

/** Prefer h4; fall back to clpp when h4 missing. */
export function colocComponent(maxH4: number | null | undefined, maxClpp?: number | null): number {
	if (maxH4 != null && Number.isFinite(maxH4)) return clamp01(maxH4);
	if (maxClpp != null && Number.isFinite(maxClpp)) return clamp01(maxClpp);
	return 0;
}

export function eqtlComponent(relevantTissueHits: number | null | undefined): number {
	const n = Number(relevantTissueHits ?? 0);
	if (!Number.isFinite(n) || n <= 0) return 0;
	return Math.min(1, n / 3);
}

export function burdenComponent(bestP: number | null | undefined): number {
	if (bestP == null || !Number.isFinite(bestP)) return 0;
	if (bestP < BURDEN_STRONG_P) return 1.0;
	if (bestP < BURDEN_NOMINAL_P) return 0.6;
	return 0;
}

export type CodingSupport = "coding" | "noncoding" | "none";
export type ClinvarSupport = "present" | "none";

export function codingComponent(coding: CodingSupport, clinvar: ClinvarSupport): number {
	if (coding === "coding" && clinvar === "present") return 1.0;
	if (coding === "coding") return 0.6;
	if (coding === "noncoding") return 0.3;
	return 0;
}

export interface ScoreInputs {
	l2g_max?: number | null;
	coloc_max_h4?: number | null;
	coloc_max_clpp?: number | null;
	eqtl_tissue_hits?: number | null;
	burden_best_p?: number | null;
	coding_support?: CodingSupport;
	clinvar_support?: ClinvarSupport;
}

export interface ScoreBreakdown {
	components: {
		l2g: number;
		coloc: number;
		eqtl: number;
		burden: number;
		coding: number;
	};
	overall_score: number;
	confidence: Confidence;
}

export function computeScore(inp: ScoreInputs, weights: Weights = WEIGHTS): ScoreBreakdown {
	const l2g = l2gComponent(inp.l2g_max);
	const coloc = colocComponent(inp.coloc_max_h4, inp.coloc_max_clpp);
	const eqtl = eqtlComponent(inp.eqtl_tissue_hits);
	const burden = burdenComponent(inp.burden_best_p);
	const coding = codingComponent(inp.coding_support ?? "none", inp.clinvar_support ?? "none");

	const overall = clamp01(
		weights.l2g * l2g +
			weights.coloc * coloc +
			weights.eqtl * eqtl +
			weights.burden * burden +
			weights.coding * coding,
	);

	let confidence: Confidence;
	if (overall >= CONFIDENCE_HIGH) confidence = "High";
	else if (overall >= CONFIDENCE_MEDIUM) confidence = "Medium";
	else if (overall >= CONFIDENCE_LOW) confidence = "Low";
	else confidence = "VeryLow";

	return {
		components: { l2g, coloc, eqtl, burden, coding },
		overall_score: overall,
		confidence,
	};
}

export function confidenceLabel(score: number): Confidence {
	if (score >= CONFIDENCE_HIGH) return "High";
	if (score >= CONFIDENCE_MEDIUM) return "Medium";
	if (score >= CONFIDENCE_LOW) return "Low";
	return "VeryLow";
}
