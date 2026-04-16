/**
 * Built-in seed rsIDs per SKILL.md "Built-in default seeds" — verbatim port.
 *
 * Match keys are case-insensitive; spaces collapsed. A trait_query of
 * "Type 2 Diabetes" or "T2D" resolves to the same seed list.
 */

export const DEFAULT_TRAIT_SEEDS: Record<string, string[]> = {
	"type 2 diabetes": [
		"rs7903146",
		"rs13266634",
		"rs7756992",
		"rs5219",
		"rs1801282",
		"rs4402960",
	],
	"type ii diabetes": [
		"rs7903146",
		"rs13266634",
		"rs7756992",
		"rs5219",
		"rs1801282",
		"rs4402960",
	],
	t2d: [
		"rs7903146",
		"rs13266634",
		"rs7756992",
		"rs5219",
		"rs1801282",
		"rs4402960",
	],
	"coronary artery disease": [
		"rs1333049",
		"rs4977574",
		"rs9349379",
		"rs6725887",
		"rs1746048",
		"rs3184504",
	],
	cad: [
		"rs1333049",
		"rs4977574",
		"rs9349379",
		"rs6725887",
		"rs1746048",
		"rs3184504",
	],
	"body mass index": [
		"rs9939609",
		"rs17782313",
		"rs6548238",
		"rs10938397",
		"rs7498665",
		"rs7138803",
	],
	bmi: [
		"rs9939609",
		"rs17782313",
		"rs6548238",
		"rs10938397",
		"rs7498665",
		"rs7138803",
	],
	asthma: ["rs7216389", "rs2305480", "rs9273349"],
	"rheumatoid arthritis": ["rs2476601", "rs3761847", "rs660895"],
	ra: ["rs2476601", "rs3761847", "rs660895"],
	"alzheimer disease": [
		"rs429358",
		"rs7412",
		"rs6733839",
		"rs11136000",
		"rs3851179",
	],
	"alzheimers disease": [
		"rs429358",
		"rs7412",
		"rs6733839",
		"rs11136000",
		"rs3851179",
	],
	"alzheimer's disease": [
		"rs429358",
		"rs7412",
		"rs6733839",
		"rs11136000",
		"rs3851179",
	],
	ad: [
		"rs429358",
		"rs7412",
		"rs6733839",
		"rs11136000",
		"rs3851179",
	],
	"ldl cholesterol": [
		"rs7412",
		"rs429358",
		"rs6511720",
		"rs629301",
		"rs12740374",
		"rs11591147",
	],
	ldl: [
		"rs7412",
		"rs429358",
		"rs6511720",
		"rs629301",
		"rs12740374",
		"rs11591147",
	],
	"total cholesterol": [
		"rs7412",
		"rs429358",
		"rs6511720",
		"rs629301",
		"rs12740374",
		"rs11591147",
	],
};

export function normalizeTraitKey(q: string): string {
	return q.toLowerCase().replace(/[^a-z0-9' ]+/g, " ").replace(/\s+/g, " ").trim();
}

/** Look up default seed rsIDs for a trait query (case-insensitive). */
export function lookupDefaultSeeds(traitQuery: string): string[] {
	if (!traitQuery) return [];
	const key = normalizeTraitKey(traitQuery);
	if (key in DEFAULT_TRAIT_SEEDS) return DEFAULT_TRAIT_SEEDS[key].slice();
	// Also try the collapsed non-apostrophe form
	const alt = key.replace(/'/g, "");
	if (alt in DEFAULT_TRAIT_SEEDS) return DEFAULT_TRAIT_SEEDS[alt].slice();
	return [];
}
