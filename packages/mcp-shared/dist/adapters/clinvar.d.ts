/**
 * ClinVar (NCBI eutils) adapter.
 * Uses efetch/esummary against the ClinVar database.
 */
export interface ClinvarFetchOpts {
    fetchImpl?: typeof fetch;
    timeoutMs?: number;
    userAgent?: string;
    apiKey?: string;
}
/** esummary returns JSON docsums for ClinVar variation IDs (VCV internal ids). */
export declare function clinvarEsummary(ids: string[], opts?: ClinvarFetchOpts): Promise<unknown>;
/** Search ClinVar by rsID — returns esearch results; caller feeds IDs to esummary. */
export declare function clinvarEsearchByRsid(rsid: string, opts?: ClinvarFetchOpts): Promise<unknown>;
//# sourceMappingURL=clinvar.d.ts.map