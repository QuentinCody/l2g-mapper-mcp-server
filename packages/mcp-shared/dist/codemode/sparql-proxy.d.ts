/**
 * SPARQL proxy source — pure JS injected into V8 isolates.
 *
 * Provides:
 *   sparql.query(queryString, opts?) — execute SELECT/CONSTRUCT/DESCRIBE; returns parsed bindings array
 *   sparql.ask(queryString)          — execute ASK; returns boolean
 *   sparql.raw(queryString, opts?)   — execute and return the unparsed JSON envelope
 *   prefixes                         — object of common ontology prefixes
 *   prefixHeader                     — pre-built `PREFIX ...` block as a string
 *   api.query(dataAccessId, sql), db.queryStaged, db.stage — staging helpers
 *
 * The endpoint URL never enters the isolate — all HTTP goes through the host's
 * sparqlFetch via the codemode.__sparql_proxy bridge.
 */
export declare function buildSparqlProxySource(): string;
//# sourceMappingURL=sparql-proxy.d.ts.map