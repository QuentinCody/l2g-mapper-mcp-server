/**
 * Open Targets Platform GraphQL adapter.
 * Endpoint: https://api.platform.opentargets.org/api/v4/graphql
 */
export interface OpentargetsFetchOpts {
    fetchImpl?: typeof fetch;
    timeoutMs?: number;
    userAgent?: string;
    endpoint?: string;
}
export interface OpentargetsResponse<T = unknown> {
    data?: T;
    errors?: Array<{
        message: string;
    }>;
}
export declare function opentargetsGraphql<T = unknown>(query: string, variables?: Record<string, unknown>, opts?: OpentargetsFetchOpts): Promise<OpentargetsResponse<T>>;
//# sourceMappingURL=opentargets.d.ts.map