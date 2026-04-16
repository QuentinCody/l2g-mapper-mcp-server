/**
 * Human Protein Atlas REST adapter.
 * Base: https://www.proteinatlas.org
 * Single-gene detail via /{ensembl_id}.json, search via /api/search_download.php.
 */
export interface HpaFetchOpts {
    fetchImpl?: typeof fetch;
    timeoutMs?: number;
    userAgent?: string;
}
export declare function hpaGene(ensemblId: string, opts?: HpaFetchOpts): Promise<unknown>;
export declare function hpaSearch(query: string, opts?: HpaFetchOpts & {
    columns?: string;
}): Promise<unknown>;
//# sourceMappingURL=hpa.d.ts.map