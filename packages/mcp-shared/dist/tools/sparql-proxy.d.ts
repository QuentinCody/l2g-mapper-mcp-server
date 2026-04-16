/**
 * Hidden __sparql_proxy tool — routes V8 isolate sparql.query() calls
 * through the server's SPARQL fetch function.
 *
 * Only callable from V8 isolates (hidden=true). Auto-stages large responses.
 */
import type { ToolEntry } from "../registry/types";
import type { SparqlFetchFn } from "../codemode/sparql-introspection";
export interface SparqlProxyToolOptions {
    sparqlFetch: SparqlFetchFn;
    doNamespace?: unknown;
    stagingPrefix: string;
    stagingThreshold?: number;
}
export declare function createSparqlProxyTool(options: SparqlProxyToolOptions): ToolEntry;
//# sourceMappingURL=sparql-proxy.d.ts.map