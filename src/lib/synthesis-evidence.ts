interface EvidenceRow {
  l2g_max: number; l2g_support_json?: string | null; coloc_support_json?: string | null;
  coloc_max_h4: number | null; coloc_max_clpp: number | null;
  eqtl_tissues: string | null; eqtl_support_json?: string | null;
  burden_best_p: number | null; burden_support_json?: string | null; coding_support: string; clinvar_support: string;
}

export function synthesisEvidence(row: EvidenceRow, gnomad: Record<string, unknown> | null) {
  return {
    l2g_max: Number(row.l2g_max?.toFixed?.(4) ?? row.l2g_max ?? 0),
    l2g_support_json: row.l2g_support_json ?? null,
    coloc_support_json: row.coloc_support_json ?? null,
    coloc_max_h4: row.coloc_max_h4, coloc_max_clpp: row.coloc_max_clpp,
    eqtl_tissues: (row.eqtl_tissues ?? "").split("|").map(value => value.trim()).filter(Boolean),
    eqtl_support_json: row.eqtl_support_json ?? null,
    burden_support_json: row.burden_support_json ?? null,
    rare_variant_support: row.burden_best_p == null ? "none" : row.burden_best_p < 2.5e-6 ? "strong" : row.burden_best_p < 0.05 ? "nominal" : "none",
    coding_support: row.coding_support, clinvar_support: row.clinvar_support,
    gnomad_context: gnomad, hpa_tissue_support: [] as string[],
  };
}
