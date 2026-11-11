interface ScoredGene {
  locus_id: string; lead_rsid: string; symbol: string; ensembl_id: string | null;
  score: { overall_score: number };
}

/** Keep stable gene IDs separate even when symbols collide; unresolved IDs cannot merge loci. */
export function crossLocusRanking(rows: ScoredGene[]) {
  const genes = new Map<string, { symbol: string; ensembl_id: string | null; loci: Map<string, number> }>();
  for (const row of rows) {
    const key = row.ensembl_id ? JSON.stringify(["gene", row.ensembl_id]) : JSON.stringify(["unresolved", row.locus_id, row.lead_rsid, row.symbol]);
    const gene = genes.get(key) ?? { symbol: row.symbol, ensembl_id: row.ensembl_id, loci: new Map() };
    gene.symbol = gene.symbol < row.symbol ? gene.symbol : row.symbol;
    gene.loci.set(row.locus_id, Math.max(gene.loci.get(row.locus_id) ?? 0, row.score.overall_score));
    genes.set(key, gene);
  }
  return [...genes.values()].map(gene => ({
    symbol: gene.symbol, ensembl_id: gene.ensembl_id, supporting_loci: gene.loci.size,
    mean_score: [...gene.loci.values()].reduce((sum, score) => sum + score, 0) / gene.loci.size,
    max_score: Math.max(...gene.loci.values()),
  })).sort((a, b) => b.supporting_loci - a.supporting_loci || b.max_score - a.max_score || (a.ensembl_id ?? a.symbol).localeCompare(b.ensembl_id ?? b.symbol));
}
