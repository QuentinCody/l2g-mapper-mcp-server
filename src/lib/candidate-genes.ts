import type { L2gPredictionRow } from "./opentargets-contract";
import { qualifiedL2gScores } from "./qualified-l2g";

export interface CandidateGene {
  locus_id: string; lead_rsid: string; symbol: string; ensembl_id: string;
  symbols_json: string; selection_rank: number; selection_score: number;
}
export interface CandidateSelection {
  locus_id: string; eligible_genes: number; selected_genes: number; omitted_genes: number;
  max_genes_per_locus: number; selection_basis: "maximum_qualified_l2g_score";
}
interface Gene {
  id: string; max: number; symbols: Set<string>; anchors: Set<string>;
}
const compare = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;

/** Cap unique genes by score per locus, retaining each actual supporting anchor. */
export function selectCandidateGenes(predictions: L2gPredictionRow[], anchors: Array<{ rsid: string; locus_id?: string }>, limit: number, warnings: string[]) {
  const { loci, selection, candidates } = collectQualifiedCandidates(limit, anchors, predictions, warnings);
  for (const [locus_id, genes] of [...loci.entries()].sort(([a], [b]) => compare(a, b))) {
    const sorted = [...genes.values()].sort((a, b) => b.max - a.max || compare(a.id, b.id));
    const selected = sorted.slice(0, limit);
    selection.push({ locus_id, eligible_genes: genes.size, selected_genes: selected.length, omitted_genes: genes.size - selected.length, max_genes_per_locus: limit, selection_basis: "maximum_qualified_l2g_score" });
    if (genes.size > limit) warnings.push(`Candidate selection ${locus_id}: retained ${limit} of ${genes.size} eligible genes by maximum qualified L2G score; remaining predictions stay in l2g_predictions`);
    for (const [index, gene] of selected.entries()) {
      const symbols = [...gene.symbols].sort(compare);
      for (const lead_rsid of [...gene.anchors].sort(compare)) candidates.push({
        locus_id, lead_rsid, symbol: symbols[0], ensembl_id: gene.id,
        symbols_json: JSON.stringify(symbols), selection_rank: index + 1, selection_score: gene.max,
      });
    }
  }
  return { candidates, selection };
}

function collectQualifiedCandidates(limit: number, anchors: { rsid: string; locus_id?: string; }[], predictions: L2gPredictionRow[], warnings: string[]) {
  if (!Number.isInteger(limit) || limit < 1 || limit > 10) throw new Error("max_genes_per_locus must be an integer from 1 to 10");
  const known = new Set(anchors.filter(a => a.locus_id).map(a => JSON.stringify([a.locus_id, a.rsid.toLowerCase()])));
  const qualified = qualifiedL2gScores(predictions);
  const loci = new Map<string, Map<string, Gene>>(); let unmatched = 0;
  for (const row of qualified.eligibleRows) {
    const rsid = row.lead_rsid.toLowerCase();
    if (!known.has(JSON.stringify([row.locus_id, rsid]))) { unmatched++; continue; }
    const genes = loci.get(row.locus_id) ?? new Map<string, Gene>();
    const id = row.ensembl_id!;
    const gene = genes.get(id) ?? { id, max: 0, symbols: new Set<string>(), anchors: new Set<string>() };
    gene.max = Math.max(gene.max, row.score); gene.symbols.add(row.symbol); gene.anchors.add(rsid);
    genes.set(id, gene); loci.set(row.locus_id, genes);
  }
  if (qualified.excluded) warnings.push(`Candidate selection excluded ${qualified.excluded} unqualified Open Targets prediction rows`);
  if (unmatched) warnings.push(`Candidate selection excluded ${unmatched} predictions without the exact gathered locus/anchor pair`);
  const candidates: CandidateGene[] = []; const selection: CandidateSelection[] = [];
  return { loci, selection, candidates };
}
