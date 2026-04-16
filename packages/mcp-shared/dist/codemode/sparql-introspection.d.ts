/**
 * SPARQL "introspection" — best-effort discovery of an endpoint's shape via
 * VOID descriptions + sample-query fallback. The result is injected into the
 * V8 isolate as a compact JSON object (analog to GraphQL introspection).
 *
 * Trims to ~5KB to keep isolate prompts cheap.
 */
export type SparqlFetchFn = (query: string, opts?: {
    method?: "GET" | "POST";
    format?: string;
    timeoutMs?: number;
}) => Promise<unknown>;
export interface SparqlEndpointDescription {
    endpointUrl: string;
    /** Named graphs (from VOID, falls back to empty when discovery fails). */
    graphs: string[];
    /** Up to 30 distinct predicates seen on a small probe. */
    predicates: string[];
    /** Up to 30 distinct rdf:type values seen. */
    classes: string[];
    /** Discovery warnings (non-fatal). */
    warnings: string[];
}
export declare function probeSparqlEndpoint(endpointUrl: string, sparqlFetch: SparqlFetchFn): Promise<SparqlEndpointDescription>;
/** Common ontology prefixes used across life-science SPARQL endpoints. */
export declare const COMMON_PREFIXES: Record<string, string>;
export declare function buildPrefixHeader(prefixes: Record<string, string>): string;
//# sourceMappingURL=sparql-introspection.d.ts.map