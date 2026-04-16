/**
 * EBI OLS4 ontology REST adapter (primarily EFO).
 * Base: https://www.ebi.ac.uk/ols4/api
 */
export interface Ols4FetchOpts {
    fetchImpl?: typeof fetch;
    timeoutMs?: number;
    userAgent?: string;
}
export declare function ols4Search(query: string, opts?: Ols4FetchOpts & {
    ontology?: string;
    rows?: number;
    exact?: boolean;
}): Promise<unknown>;
export declare function ols4TermDescendants(ontology: string, iri: string, opts?: Ols4FetchOpts & {
    size?: number;
    page?: number;
}): Promise<unknown>;
//# sourceMappingURL=efo_ols4.d.ts.map