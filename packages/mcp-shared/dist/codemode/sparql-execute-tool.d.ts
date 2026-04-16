/**
 * SPARQL execute tool factory — creates a `<prefix>_execute` tool for
 * SPARQL Code Mode. Sibling of createGraphqlExecuteTool.
 *
 * The isolate gets:
 *   sparql.query(queryString, opts?) — execute SELECT/CONSTRUCT/DESCRIBE
 *   sparql.ask(queryString)          — execute ASK; returns boolean
 *   sparql.raw(queryString, opts?)   — return unparsed JSON envelope
 *   prefixes                         — common ontology prefix map
 *   prefixHeader                     — pre-built `PREFIX ...` block
 *   db.stage(), db.queryStaged(), api.query() — staging helpers
 *
 * The endpoint URL never enters the isolate — all HTTP goes through the
 * host's sparqlFetch via codemode.__sparql_proxy.
 */
import { z } from "zod";
import { type SparqlEndpointDescription, type SparqlFetchFn } from "./sparql-introspection";
export interface SparqlExecuteToolOptions {
    /** Tool name prefix (e.g., "bgee" → "bgee_execute") */
    prefix: string;
    /** Function to execute SPARQL queries on the host */
    sparqlFetch: SparqlFetchFn;
    /** Endpoint URL (used in tool description only — does NOT enter isolate) */
    endpointUrl: string;
    /** DO namespace for auto-staging large responses */
    doNamespace?: unknown;
    /** Worker Loader binding for V8 isolate creation */
    loader: unknown;
    /** Byte threshold for auto-staging (default 30KB) */
    stagingThreshold?: number;
    /** Execution timeout in ms (default 30000) */
    timeout?: number;
    /** Optional JS source injected before user code (domain-specific notes/quirks) */
    preamble?: string;
    /** DO namespace for virtual filesystem (optional) */
    fsDoNamespace?: unknown;
    /** Pre-cached endpoint description. If omitted, probed lazily on first execute. */
    description?: SparqlEndpointDescription;
    /** Display name in tool description (default = prefix) */
    apiName?: string;
    /** Custom prefix map to inject into isolate (defaults to COMMON_PREFIXES) */
    prefixes?: Record<string, string>;
}
export interface SparqlExecuteToolResult {
    name: string;
    description: string;
    schema: {
        code: z.ZodString;
    };
    register: (server: {
        tool: (...args: unknown[]) => void;
    }) => void;
}
export declare function createSparqlExecuteTool(options: SparqlExecuteToolOptions): SparqlExecuteToolResult;
//# sourceMappingURL=sparql-execute-tool.d.ts.map