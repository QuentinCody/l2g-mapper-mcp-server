#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SERVER_ROOT = path.resolve(__dirname, "..");

const RED = "\x1b[31m";
const GREEN = "\x1b[32m";
const BLUE = "\x1b[34m";
const RESET = "\x1b[0m";

let total = 0;
let passed = 0;
let failed = 0;

function assert(file, haystack, needle, name) {
	total++;
	if (haystack.includes(needle)) {
		console.log(`${GREEN}✓${RESET} ${name}`);
		passed++;
	} else {
		console.log(`${RED}✗${RESET} ${name}`);
		console.log(`  Missing: ${needle}`);
		console.log(`  File: ${file}`);
		failed++;
	}
}

function read(rel) {
	return fs.readFileSync(path.resolve(SERVER_ROOT, rel), "utf8");
}

console.log(`${BLUE}🧪 L2G Mapper regression tests${RESET}`);

// Tool registration
const indexSrc = read("src/index.ts");
assert("src/index.ts", indexSrc, "L2gDataDO", "index.ts exports L2gDataDO");
assert("src/index.ts", indexSrc, "McpAgent", "index.ts uses McpAgent");
assert("src/index.ts", indexSrc, "registerL2gGather", "index.ts wires l2g_gather");
assert("src/index.ts", indexSrc, "registerL2gScore", "index.ts wires l2g_score");
assert("src/index.ts", indexSrc, "registerL2gSynthesize", "index.ts wires l2g_synthesize");

// Dual registration
const gather = read("src/tools/l2g-gather.ts");
assert("src/tools/l2g-gather.ts", gather, "mcp_l2g_gather", "l2g_gather dual-registered under mcp_l2g_gather");
assert("src/tools/l2g-gather.ts", gather, 'reg("l2g_gather")', "l2g_gather dual-registered under l2g_gather");
assert("src/tools/l2g-gather.ts", gather, "createCodeModeResponse", "l2g_gather returns structuredContent");
assert("src/tools/l2g-gather.ts", gather, "createCodeModeError", "l2g_gather has error structuredContent path");
assert("src/tools/l2g-gather.ts", gather, "stageToDoAndRespond", "l2g_gather stages results to DO");
assert("src/tools/l2g-gather.ts", gather, "Promise.all", "l2g_gather uses Promise.all for parallel fan-out");
assert("src/tools/l2g-gather.ts", gather, "withDeadline", "l2g_gather has per-stage deadline guards");
assert("src/tools/l2g-gather.ts", gather, "projected > 800", "l2g_gather enforces subrequest budget");

const score = read("src/tools/l2g-score.ts");
assert("src/tools/l2g-score.ts", score, "mcp_l2g_score", "l2g_score dual-registered under mcp_l2g_score");
assert("src/tools/l2g-score.ts", score, 'reg("l2g_score")', "l2g_score dual-registered under l2g_score");
assert("src/tools/l2g-score.ts", score, "runScoringQc", "l2g_score runs QC gates");

const synth = read("src/tools/l2g-synthesize.ts");
assert("src/tools/l2g-synthesize.ts", synth, "mcp_l2g_synthesize", "l2g_synthesize dual-registered");
assert("src/tools/l2g-synthesize.ts", synth, 'reg("l2g_synthesize")', "l2g_synthesize second registration");
assert("src/tools/l2g-synthesize.ts", synth, "locusGeneHeatmap", "l2g_synthesize emits locus_gene_heatmap");
assert("src/tools/l2g-synthesize.ts", synth, "locusScoreDecomposition", "l2g_synthesize emits locus_score_decomposition");
assert("src/tools/l2g-synthesize.ts", synth, "tissueSupportDotplot", "l2g_synthesize emits tissue_support_dotplot");
assert("src/tools/l2g-synthesize.ts", synth, "verifySectionOrder", "l2g_synthesize runs section-order QC");

// Scoring formula constants
const scoring = read("src/lib/scoring.ts");
assert("src/lib/scoring.ts", scoring, "l2g: 0.40", "scoring.ts has 0.40 L2G weight");
assert("src/lib/scoring.ts", scoring, "coloc: 0.25", "scoring.ts has 0.25 coloc weight");
assert("src/lib/scoring.ts", scoring, "eqtl: 0.15", "scoring.ts has 0.15 eQTL weight");
assert("src/lib/scoring.ts", scoring, "burden: 0.10", "scoring.ts has 0.10 burden weight");
assert("src/lib/scoring.ts", scoring, "coding: 0.10", "scoring.ts has 0.10 coding weight");
assert("src/lib/scoring.ts", scoring, "CONFIDENCE_HIGH = 0.75", "scoring.ts has 0.75 High threshold");
assert("src/lib/scoring.ts", scoring, "CONFIDENCE_MEDIUM = 0.55", "scoring.ts has 0.55 Medium threshold");
assert("src/lib/scoring.ts", scoring, "CONFIDENCE_LOW = 0.35", "scoring.ts has 0.35 Low threshold");
assert("src/lib/scoring.ts", scoring, "2.5e-6", "scoring.ts has burden strong cutoff");

// Mermaid generators present
const mermaid = read("src/lib/mermaid.ts");
assert("src/lib/mermaid.ts", mermaid, "export function locusGeneHeatmap", "mermaid.ts exports locusGeneHeatmap");
assert("src/lib/mermaid.ts", mermaid, "export function locusScoreDecomposition", "mermaid.ts exports locusScoreDecomposition");
assert("src/lib/mermaid.ts", mermaid, "export function tissueSupportDotplot", "mermaid.ts exports tissueSupportDotplot");

// Built-in seeds
const seeds = read("src/lib/well-known-traits.ts");
assert("src/lib/well-known-traits.ts", seeds, "rs7903146", "seeds include T2D rs7903146");
assert("src/lib/well-known-traits.ts", seeds, "rs1333049", "seeds include CAD rs1333049");
assert("src/lib/well-known-traits.ts", seeds, "rs9939609", "seeds include BMI rs9939609");
assert("src/lib/well-known-traits.ts", seeds, "rs429358", "seeds include AD rs429358");

// QC gates
const qc = read("src/lib/qc.ts");
assert("src/lib/qc.ts", qc, "REQUIRED_SECTIONS", "qc.ts defines REQUIRED_SECTIONS for SKILL.md order");
assert("src/lib/qc.ts", qc, '"Objective"', "qc.ts requires Objective section");
assert("src/lib/qc.ts", qc, '"Recommended next analyses"', "qc.ts requires Recommended next analyses section");

console.log(`\n${BLUE}📊 Test Results Summary${RESET}`);
console.log(`Total tests: ${total}`);
console.log(`${GREEN}Passed: ${passed}${RESET}`);
console.log(`${RED}Failed: ${failed}${RESET}`);

if (failed > 0) {
	console.log(`\n${RED}❌ L2G regression tests failed.${RESET}`);
	process.exit(1);
}
console.log(`\n${GREEN}✅ L2G regression tests passed.${RESET}`);
