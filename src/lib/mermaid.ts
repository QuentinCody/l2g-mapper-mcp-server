/**
 * Mermaid-only figure generators for L2G synthesis. NO PNGs, NO matplotlib.
 * Three figures per SKILL.md §"Optional Figure Contract":
 *   1. locus_gene_heatmap      — grid: rows=genes, cols=evidence components
 *   2. locus_score_decomposition — stacked bar per locus for top genes
 *   3. tissue_support_dotplot   — grid: gene × tissue
 */

export interface HeatmapCell {
	row: string; // e.g. gene symbol
	col: string; // e.g. "L2G" | "coloc" | "eQTL" | "burden" | "coding"
	value: number; // 0..1
}

export interface MermaidFigure {
	id: string;
	mermaid: string;
	caption?: string;
}

function rngBucket(v: number): string {
	if (v >= 0.75) return "█";
	if (v >= 0.5) return "▓";
	if (v >= 0.25) return "▒";
	if (v > 0) return "░";
	return ".";
}

/**
 * Render a gene × evidence heatmap as a Mermaid `flowchart LR`.
 * Mermaid lacks a proper heatmap primitive, so we emit a labeled grid of
 * nodes colored by bucket using classDef styling.
 */
export function locusGeneHeatmap(cells: HeatmapCell[]): MermaidFigure {
	const genes = Array.from(new Set(cells.map((c) => c.row)));
	const cols = Array.from(new Set(cells.map((c) => c.col)));
	const lines: string[] = [];
	lines.push("flowchart LR");
	lines.push("  classDef hi fill:#1a9850,color:#fff,stroke:#333,stroke-width:1px");
	lines.push("  classDef md fill:#fdae61,color:#000,stroke:#333,stroke-width:1px");
	lines.push("  classDef lo fill:#e0f3f8,color:#000,stroke:#aaa,stroke-width:1px");
	lines.push("  classDef zr fill:#f7f7f7,color:#999,stroke:#ddd,stroke-width:1px");

	if (genes.length === 0 || cols.length === 0) {
		lines.push("  EMPTY[\"No heatmap data — gather phase produced no scored candidate genes.\"]:::zr");
		return {
			id: "locus_gene_heatmap",
			mermaid: lines.join("\n"),
			caption: "Top candidate genes by evidence component (empty — no data).",
		};
	}

	// Header row
	const headerIds = cols.map((c, i) => `H${i}`);
	headerIds.forEach((hid, i) => {
		lines.push(`  ${hid}["${cols[i]}"]:::zr`);
	});
	for (let i = 0; i < headerIds.length - 1; i++) {
		lines.push(`  ${headerIds[i]} --- ${headerIds[i + 1]}`);
	}

	genes.forEach((gene, gi) => {
		const rowNode = `G${gi}["${gene}"]:::zr`;
		lines.push(`  ${rowNode}`);
		cols.forEach((col, ci) => {
			const cell = cells.find((c) => c.row === gene && c.col === col);
			const v = cell?.value ?? 0;
			const cls = v >= 0.75 ? "hi" : v >= 0.5 ? "md" : v > 0 ? "lo" : "zr";
			const id = `G${gi}C${ci}`;
			lines.push(`  ${id}["${v.toFixed(2)}"]:::${cls}`);
		});
		for (let ci = 0; ci < cols.length - 1; ci++) {
			lines.push(`  G${gi}C${ci} --- G${gi}C${ci + 1}`);
		}
		lines.push(`  G${gi} --- G${gi}C0`);
	});

	return {
		id: "locus_gene_heatmap",
		mermaid: lines.join("\n"),
		caption: "Top candidate genes by evidence component (L2G / coloc / eQTL / burden / coding).",
	};
}

export interface LocusDecomposition {
	locus_id: string;
	gene: string;
	components: { l2g: number; coloc: number; eqtl: number; burden: number; coding: number };
}

/** Render bar-chart-style score decomposition per locus (Mermaid `xychart-beta`). */
export function locusScoreDecomposition(rows: LocusDecomposition[]): MermaidFigure {
	const lines: string[] = [];
	lines.push("xychart-beta");
	lines.push("  title \"Top-gene score decomposition by locus\"");
	const labels = rows.map((r) => `"${r.locus_id}:${r.gene}"`);
	lines.push(`  x-axis [${labels.join(", ")}]`);
	lines.push("  y-axis \"component score (0..1)\" 0 --> 1");
	const series = ["l2g", "coloc", "eqtl", "burden", "coding"] as const;
	for (const s of series) {
		const vals = rows.map((r) => r.components[s].toFixed(3)).join(", ");
		lines.push(`  bar [${vals}]`);
	}
	return {
		id: "locus_score_decomposition",
		mermaid: lines.join("\n"),
		caption: "Stacked-bar-style decomposition of top-gene scores per locus.",
	};
}

export interface TissueDot {
	gene: string;
	tissue: string;
	present: boolean;
}

/** Dot-plot style grid for gene × tissue eQTL/HPA support. */
export function tissueSupportDotplot(dots: TissueDot[]): MermaidFigure {
	const genes = Array.from(new Set(dots.map((d) => d.gene)));
	const tissues = Array.from(new Set(dots.map((d) => d.tissue)));
	const lines: string[] = [];
	lines.push("flowchart LR");
	lines.push("  classDef yes fill:#4575b4,color:#fff,stroke:#333");
	lines.push("  classDef no  fill:#f7f7f7,color:#999,stroke:#ddd");

	if (genes.length === 0 || tissues.length === 0) {
		lines.push("  EMPTY[\"No tissue support — GTEx eQTL lane returned no hits (may be upstream rate-limit or no eQTL evidence for the anchors).\"]:::no");
		return {
			id: "tissue_support_dotplot",
			mermaid: lines.join("\n"),
			caption: "Gene × tissue evidence (empty — no eQTL/HPA support data available).",
		};
	}

	tissues.forEach((t, ti) => {
		lines.push(`  T${ti}["${t}"]:::no`);
	});
	for (let i = 0; i < tissues.length - 1; i++) {
		lines.push(`  T${i} --- T${i + 1}`);
	}

	genes.forEach((g, gi) => {
		lines.push(`  G${gi}["${g}"]:::no`);
		tissues.forEach((t, ti) => {
			const d = dots.find((x) => x.gene === g && x.tissue === t);
			const cls = d?.present ? "yes" : "no";
			const mark = d?.present ? rngBucket(1) : rngBucket(0);
			lines.push(`  G${gi}T${ti}["${mark}"]:::${cls}`);
		});
		for (let ti = 0; ti < tissues.length - 1; ti++) {
			lines.push(`  G${gi}T${ti} --- G${gi}T${ti + 1}`);
		}
		lines.push(`  G${gi} --- G${gi}T0`);
	});
	return {
		id: "tissue_support_dotplot",
		mermaid: lines.join("\n"),
		caption: "Gene × tissue eQTL/HPA evidence dots.",
	};
}
