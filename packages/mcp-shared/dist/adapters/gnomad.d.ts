/**
 * gnomAD GraphQL adapter.
 * Endpoint: https://gnomad.broadinstitute.org/api
 */
export interface GnomadFetchOpts {
    fetchImpl?: typeof fetch;
    timeoutMs?: number;
    userAgent?: string;
}
export interface GnomadResponse<T = unknown> {
    data?: T;
    errors?: Array<{
        message: string;
    }>;
}
export declare function gnomadGraphql<T = unknown>(query: string, variables?: Record<string, unknown>, opts?: GnomadFetchOpts): Promise<GnomadResponse<T>>;
//# sourceMappingURL=gnomad.d.ts.map