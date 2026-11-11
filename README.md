# l2g-mapper-mcp-server

Synchronous 3-tier locus-to-gene composer (Port 8892). Ports the OpenAI
`locus-to-gene-mapper-skill` into three Code Mode tools that fan out to
~9 upstream scientific APIs in parallel, score candidate genes deterministically,
and emit the SKILL.md JSON + Markdown output contract. Figures are Mermaid.

Tools:
- `l2g_gather` — trait/variant → staged evidence tables (< 30s)
- `l2g_score` — deterministic scoring of staged evidence (SKILL.md formula)
- `l2g_synthesize` — final JSON + Markdown + Mermaid figures

Upstreams: OLS4 (EFO), GWAS Catalog, Ensembl, Open Targets Platform,
GTEx v2, Genebass, ClinVar eutils, gnomAD, HPA. Full-scope resumable mode
is deferred to the Workflow plan (ADR-005).

GWAS anchor discovery uses the v2 `efo_id` filter for a selected ontology ID,
or `efo_trait` for the label when no ID is resolved. `show_child_traits`
(default `true`) now reaches the provider as `show_child_trait` and is recorded
in the gather's caps. This follows the singular parameter in the
[provider's published OpenAPI](https://github.com/EBISPOT/gwas-mcp/blob/main/src/gwascatalog/mcp/data/rest-api-doc.yaml);
live checks on October 1, 2026 confirm singular false/true changes the asthma
association count from 3,234 to 4,154, with childhood-onset asthma in the expanded
sample. The plural spelling in the narrative guide did not expand that sample.
The shared adapter and anchor projector preserve the sampled rsIDs, p-values
and trait labels. This qualifies that discovery path, not the full multi-source
gather or every trait. Discovery still reads one capped page and does not establish
complete trait coverage. Descendant discovery does not relax the separate
exact-trait checks used for scoring.

Anchor parsing accepts the v2 schema's `content` page and the existing HAL
`_embedded.associations` form. Malformed or ambiguous pages produce a fetch
warning. Single-variant discovery excludes flagged haplotypes/interactions,
multiple distinct SNPs and non-exact rsIDs, with skipped-row counts. Repeated
rsIDs retain the lowest valid numeric p-value within the fetched page and the
trait label from that same association. Unknown p-values remain null; this
page-local selection does not establish a globally strongest or complete set.

## Evidence qualification (September 29, 2026)

The composer now resolves alleles separately for GRCh37 and GRCh38. Ambiguous
alternate alleles are left unresolved for allele-specific requests. GTEx queries
accept `gtex_dataset_id: "gtex_v8" | "gtex_v10"` (v8 remains the default);
projected rows retain the exact b38 variant ID,
assembly and versioned GENCODE ID. An rsID-only match no longer admits a response
with a different or unknown allele/build. This remains one capped page, not a
complete tissue inventory or a pinned source snapshot.

The October 1 [GTEx OpenAPI](https://gtexportal.org/api/v2/openapi.json)
documents both datasets for `/association/singleTissueEqtl`, with explicit
zero-based paging. Pass `gtex_dataset_id: "gtex_v10"` to `l2g_gather` to select
v10. The selected dataset is recorded in `l2g_meta.caps_json` and each projected
association; it is a request constraint, not an independently verified snapshot.
Returned dataset IDs that conflict with the request are excluded from projections.

`gtex_evidence` retains the full native page (including unmatched rows and paging
metadata), decoded response text, request URL/parameters, HTTP status, retrieval
time and canonical-JSON response hash. HTTP and parsing failures remain explicit
evidence with no usable association rows. The request uses no automatic retries
or redirects, an 8-second adapter timeout, a 4-second body timer and a 4 MiB body
cap, within the existing gather-phase deadline. Projected `eqtl` rows retain
`native_row_json`, `source_response_sha256` and the zero-based `source_row_index`
into the native page's `data` array. Use the existing SQL tools to join a row's
response hash to `gtex_evidence.response_sha256`. Large native evidence stages
with the existing gather tables and citation hash. Score weights are unchanged;
GTEx tissue counts do not establish trait-specific relevance or independent replication.

Scoring rechecks the staged page hash, native row and requested dataset against
each projected association and resolved GRCh38 anchor. Gene, versioned GENCODE ID,
tissue, p-value and effect direction must agree. Missing or conflicting evidence
contributes zero while remaining queryable; older gathers without native GTEx
pages must be gathered again to support this component. Failed or incomplete
required SQL reads abort scoring before replacing saved scores.

`scored_candidate_genes.eqtl_support_json` and synthesized candidate evidence carry
the original data-access ID, source table/page hash/row index, requested dataset,
variant, gene, tissue, p-value and normalized effect size. Associations in one tissue
retain their distinct references but count once. These checks establish stored
evidence consistency, not independent provider authentication or trait relevance.
Large outputs retain all references through staging. Repeated scoring replaces
both derived tables atomically; failed writes preserve the prior scores and source
tables. Empty or malformed replacements fail explicitly. This uses the existing
staging store; its default append behavior remains unchanged for other payloads.

The score policy `2026-10-01-trait-burden` excludes unqualified contributions while
retaining their gathered context tables for SQL inspection:

- Colocalisation requires an exact molecular-QTL gene and matching GWAS trait,
  allele, credible set and release. GWAS–GWAS overlaps and unqualified rows are excluded.
- Genebass's minimum p-value spans phenotypes without an exact requested-trait match.
- L2G predictions and rsID-level ClinVar classifications do not establish a
  gene-specific coding consequence. The previous substring check also treated
  “likely benign” as support; that inference has been removed.

Excluded contributions are zero, without redistributing their weights. With default
weights, qualified L2G, colocalisation, retrieved-eQTL and gene-burden components contribute at most 0.90.
The confidence buckets are heuristic, not calibrated causal probabilities; zero
for unavailable evidence does not establish absence. Synthesis requires the new
score policy. Re-run gather and score for existing analyses to apply both the
variant-matching and scoring changes.

Scoring requires `l2g_meta.gather_policy_version=2026-10-01-gene-coloc`;
older gathers must be collected again. Open Targets maxima join the exact
Ensembl gene ID, lead anchor, locus and selected mapped trait, and require the
stored allele-match and release fields. Native study evidence is rechecked at
scoring time. Malformed or wrong-trait rows are excluded; mixed accepted releases
reject scoring. GTEx tissue support also joins the gene ID and anchor.
Symbols remain display labels. Cross-locus ranking groups stable gene IDs and uses
one maximum per locus, so duplicate anchors do not inflate the mean. Unresolved
gene IDs remain separate per-locus entries and are omitted from gene-ID figures.
JSON, Markdown and figures distinguish stable IDs that share a symbol.

`scored_candidate_genes.l2g_support_json` records one representative maximum's
study-locus ID, variant, Ensembl ID, release, selected trait and match basis. Ties choose the
lexically first study-locus ID. This is a reference back to the gathered evidence,
not a count of independent replications or an exact trait-equivalence claim.
Other source qualification and full provenance still need work. Synthesis rejects older score-policy versions rather than relabeling them.

## Exact-trait gene-burden evidence

Gather reads one Open Targets `gene_burden` evidence page for up to 25 selected
Ensembl genes and the resolved ontology trait, with `enableIndirect: false`.
The [provider schema](https://api.platform.opentargets.org/api/v4/graphql/schema)
supports these filters and the retained study, method, p-value, source-phenotype,
quality-control and release fields. Runtime datasource discovery must confirm
`gene_burden` as genetic-association evidence before a row contributes.

Scoring rechecks the saved response hash, observed release consistency, request,
native gene and mapped trait, study/method, positive representable p-value and
empty QC/warning fields. It selects the minimum among eligible retrieved tests;
the existing heuristic thresholds do not perform multiple-testing correction.
`burden_support_json` retains native qualifying rows and references through
synthesis. All requested response fields, including excluded rows and cursors,
remain in `opentargets_evidence`. The cross-phenotype Genebass context does not
contribute. Older scores require rescoring; older gathers lack the new lane and
must be gathered again to obtain burden evidence.

This is gene-level support, not proof that a particular lead variant is causal.
The same evidence can recur across loci and cohorts can overlap other sources.
A bounded page is not exhaustive; source absence, QC exclusion and unqueried
genes are not negative associations. Observed versions do not pin snapshots.

Oversized native-evidence and scored-result rows are reconstructed with bounded
SQL substrings before validation/synthesis, preserving Unicode and complete JSON.
The fallback allows at most 300 SQL queries and 24 MiB of reconstructed text per
read; exceeding either bound fails explicitly without partial evidence.

## Upstream opportunities still requiring integration

[Open Targets 26.09](https://blog.opentargets.org/open-targets-platform-26-09-has-been-released/)
(September 25, 2026) adds GTEx v10, IBDverse and MAGE molecular QTL evidence, plus
BRaVa and Genes & Health gene-burden sources. The
[26.06 release](https://blog.opentargets.org/open-targets-platform-26-06-has-been-released/)
also added trans-pQTL features to L2G. These are model/source updates, not proof
that a returned prediction independently replicates its underlying features.

[eQTL Catalogue r8](https://www.ebi.ac.uk/eqtl/Release_notes/) is still a pre-release;
the final release is scheduled for December 2026. GTEx v10 reuses dataset IDs, so
joins must include the release and annotation context.
[FinnGen DF13 is public](https://www.finngen.fi/en/finngen-data-freeze-13-results-now-available);
its availability does not establish which FinnGen release the live Open Targets
endpoint uses.

Next work: validate live Open Targets burden behavior and
FinnGen-to-ontology phenotype equivalence, extend Catalogue coverage beyond the
bounded r7 workflow below, and qualify other scoring and synthesis sources. The composer
does not pin source snapshots; other lanes still need native-response retention.
Complete pagination, cancellation, transport/provenance coverage and end-to-end
live acceptance remain open. These local changes have not been deployed.

## Optional eQTL Catalogue context and scoring (October 6, 2026)

`l2g_gather` accepts `eqtl_catalogue` for one explicitly selected r7 dataset and
one resolved anchor. The default makes no Catalogue requests. Discover the
dataset and its `catalogue_snapshot` with `eqtl_execute` first:

```js
return await api.get("/datasets/QTD000021", { release: "r7" });
```

Then pass the following object to `l2g_gather`, substituting the two hashes from
that discovery result. Select a dataset appropriate to the research question;
the composer does not choose a study, tissue or cell type automatically.

```json
{
  "seed_rsids": ["rs4239702"],
  "eqtl_catalogue": {
    "release": "r7",
    "dataset_id": "QTD000021",
    "rsid": "rs4239702",
    "metadata_sha256": "<64-character metadata_sha256 from discovery>",
    "paths_sha256": "<64-character paths_sha256 from discovery>",
    "max_rows": 200
  }
}
```

The Worker reuses the dedicated eQTL server's shared BGZF/tabix reader. It
checks both catalogue hashes before reading association files, then queries
only the anchor position, with a 500-row maximum. This adds at most five
requests, with a 15-second cancellation deadline and FTP requests spaced by
two seconds within the Worker isolate. Concurrent isolates do not share that
throttle. Missing or ambiguous GRCh38 alleles produce a skipped observation;
upstream failures produce an error observation without failing other sources.

`eqtl_catalogue_evidence` retains native dataset metadata, parsed rows, original
TSV text, file-response hashes, request provenance and the complete payload hash.
Inspect it with `l2g_query_data`. Synthesis rechecks the native payload and request,
then exposes `anchors[].eqtl_catalogue_context` with exact coordinate/REF/ALT
matching row indexes and references to the staged evidence. Large results retain
the existing staging, citation and Code Mode contracts. No new tools or services
are required.

Without `fine_mapping_sha256`, this is nominal cis-QTL context and does not change scores. ALT effects remain
unchanged, duplicate rsID rows remain visible, and `coverage_complete` is always
false. A point query is neither a complete locus analysis nor evidence of
colocalisation or independent replication of GTEx. The provider documents
duplicate rsIDs and request spacing in its [data-access guidance](https://www.ebi.ac.uk/eqtl/Data_access/).
Its [release notes](https://www.ebi.ac.uk/eqtl/Release_notes/) explain that `.cc`
files filter molecular traits and r8 can reuse dataset IDs. This lane accepts
only r7; catalogue hashes identify the observed metadata, not immutable FTP files.

To opt into fine-mapped molecular QTL support, first retrieve the selected
dataset's credible sets with the dedicated server and retain the returned
`file_evidence.sha256`:

```js
return await api.get("/datasets/QTD000021/credible_sets", {
  variant: "chr20_46118447_G_A", min_pip: 0.95, max_rows: 100
});
```

Pass that hash as `eqtl_catalogue.fine_mapping_sha256`, with the corresponding
anchor (`rs745307` for this example) and the two metadata hashes. The extra
request reads and verifies the complete compressed credible-set file before
decoding; it shares the existing 15-second lane deadline and FTP spacing. Files
over 16 MiB compressed or 64 MiB decoded fail explicitly. This bounded reader
does not support every dataset size; large-file resumable access remains open.
Nominal context survives a fine-mapping failure, with the error retained.

Scoring requires an exact resolved GRCh38 allele, stable human Ensembl gene,
matching molecular trait and nominal row, native PIP at least **0.95**, credible
set purity `cs_min_r2` at least **0.25**, and an identified tissue context.
The PIP cutoff is this application's eligibility policy, not a provider
significance threshold. The purity bound follows the provider's
[minimum absolute correlation of 0.5](https://github.com/eQTL-Catalogue/qtlmap/blob/master/bin/run_susie.R).
Native effects and statistical fields are retained; nominal significance alone
never qualifies a contribution. Raw TSV rows, compressed-file hashes, complete
native payload hashes and request identities are rechecked before scoring.

Qualified Catalogue evidence contributes to the existing eQTL tissue feature.
Exact tissue ontology matches and matching GTEx sample groups count once;
unresolved cross-source tissue overlap adds no tissue. Distinct observed tissues
do not establish independent replication, and molecular QTL fine mapping does
not establish colocalisation with the input trait. References to both native
files survive scoring, synthesis and large-response staging. The score policy is
`2026-10-06-r8-finemap`; rescore existing results before synthesis. Older
nominal-only gathers remain readable and cannot acquire a fine-mapping contribution.

Saved October 4 provider bytes pass native replay through actual stateless MCP
HTTP and Node SQLite; the other source lanes in that replay are synthetic.
Fresh provider and edited Worker acceptance remain pending.

### Explicit r8 beta Parquet context

Use `eqtl_catalogue_r8` instead of `eqtl_catalogue` to select one r8 beta
dataset and resolved GRCh38 anchor. They are mutually exclusive: reused QTD
identifiers do not establish analysis equivalence across releases. This option
uses the dedicated eQTL server's existing checksum-verified Parquet backend.
The L2G Worker must have `EQTL_PARQUET_URL` and `EQTL_PARQUET_TOKEN` configured
for an already imported snapshot. This code does not create or deploy that service.

```json
{
  "seed_rsids": ["rs74045218"],
  "eqtl_catalogue_r8": {
    "release": "r8_beta",
    "dataset_id": "QTD000116",
    "rsid": "rs74045218",
    "metadata_sha256": "<SHA-256 of the imported r8 metadata TSV>",
    "file_sha256": "<SHA-256 of the imported credible_sets Parquet file>",
    "max_rows": 200,
    "score_fine_mapping": false
  }
}
```

Context only is the default. `score_fine_mapping: true` enables the same
high-PIP/purity tissue feature, using the native association statistics carried
in the credible-set file itself. A valid stable gene, molecular trait, credible
set ID/size, finite beta, positive standard error, valid p-value and identified
tissue are also required. The r8 path does not join r7 nominal files. GTEx v8/v10
sample-group matches and identical tissue ontology IDs count once, with
unresolved overlap adding no tissue. The statistics do not establish relevance
to the selected GWAS trait or independent replication.

One exact native `chr<chromosome>_<position>_<ref>_<alt>` query adds one
backend request, with a 35-second connection/body deadline, a 2 MiB response
limit and at most 500 rows. Full returned rows, metadata, schema, hashes and
beta status remain in `eqtl_catalogue_r8_evidence`; backend URL and credentials
are not retained. Both scoring and synthesis recheck the complete payload hash,
release/dataset/file/query identity and every returned coordinate/allele.
`anchors[].eqtl_catalogue_r8_context` retains checked source references and
error/skipped observations. Bounded queries always report incomplete coverage.

Saved native QTD000116 data queried by the actual Python backend and compared
with PyArrow passes local stateless MCP/SQLite replay through both aliases,
including large synthetic Unicode extensions and rejection cases. Live HTTPS
backend integration and edited Workerd acceptance remain unverified. See the
[r8 integration review](../../docs/audits/2026-10-06-l2g-eqtl-r8-review.md).

## Optional FinnGen variant and phenotype joins (October 6, 2026)

Pass `finngen_release: "R12"` or `"R13"` to `l2g_gather` to collect native
PheWAS responses for its resolved biallelic GRCh38 anchors. Omitting the option
performs no FinnGen requests. The existing gather/query/score/synthesize tools
and their MCP aliases remain the interface; no new service or secret is needed.
Add `finngen_phenocode: "T2D"` to select an exact native phenotype from the
chosen release. This requires `finngen_release`; labels and EFO identifiers are
not converted to phenocodes. One extra request retrieves `/api/pheno/T2D`, and
`finngen_phenotype_evidence` retains that complete native definition, source URL,
request status and canonical hash. The definition read has a 256 KiB body limit
and four-second request/body deadlines, with no retries or redirect following.

Each `finngen_evidence` row retains the requested variant/release/assembly,
source URL, retrieval time, HTTP status, native JSON and canonical response hash.
Native phenocodes, effects, regions, QC and extension fields are preserved.
The selected release denotes the requested
host, not an immutable snapshot or runtime release attestation. Failed/skipped
requests remain observations with explicit errors. Reads are bounded to 25
anchors, four seconds and 1 MiB per response, with no retries, redirects, release
fallback or additional rsID resolution; oversized bodies are rejected, not truncated.

Synthesis includes `anchors[].finngen_context` references back to these staged
observations. It reconstructs complete large records through bounded SQL reads
and rechecks each native JSON hash, requested host/release/assembly, exact anchor
alleles, locus and response state. Missing, duplicated, changed or capped
evidence fails synthesis instead of silently dropping context. Returned
`source_hash_verified` and `request_context_verified` describe these local
consistency checks; they do not authenticate the provider or attest to an
immutable release. Original JSON remains queryable in `finngen_evidence`.
Reconstruction retains the existing 24 MiB total limit and allows up to 3,300
SQL reads for this table; exceeding either bound fails without partial evidence.

Synthesis also compares explicit native chromosome, position, REF, ALT and any
native `varid` with the gathered anchor. The provider's numeric chromosome 23
matches X; no strand flipping or indel normalization is inferred. A successful
`identity_verified` describes this exact field comparison, not independent
reference-sequence verification. Missing, conflicting and QC-only identities
remain unverified.

With an explicit phenocode, `phenotype_join` selects exactly one matching native
association, checks its label against the selected definition and requires
numeric p-value/effect fields. It retains the entire matched association and
references both source observations. Duplicate matches, missing definitions,
wrong alleles and malformed statistics carry explicit reasons with no positive
join. Definition hashes, request identity and join policy are rechecked from
storage; missing, duplicated or changed source observations fail synthesis.

These observations contribute nothing to gene scores: phenotype equivalence
to the selected ontology trait and association coverage remain unqualified. Missing or
empty results do not establish absence of an association. The provider describes
effects relative to the reference-genome alternate allele; this lane does not
transform or harmonize them.

Sources: [FinnGen release/access guidance](https://www.finngen.fi/en/access_results)
and [provider variant route](https://github.com/FINNGEN/pheweb/blob/master/pheweb/serve/server.py).
The October 6 [join review](../../docs/audits/2026-10-06-l2g-finngen-joins-review.md)
records tests and actual stateless MCP/SQLite replay of the saved October 4 R12
and R13 responses through gather, score and synthesis. All native fields remain
recoverable and scores are unchanged. Other gather sources in that replay are
synthetic; fresh provider, edited Workerd and full multi-source acceptance remain
pending. Source requests are separate observations, not an atomic snapshot.

## Open Targets release and model evidence (September 30, 2026)

Gather now reads `meta` in every Open Targets search, credible-set and detail
request, following the [current provider schema](https://api.platform.opentargets.org/api/v4/graphql/schema).
Missing release metadata and GraphQL errors exclude that response from ranking.
Different observed data releases exclude the entire Open Targets lane. Matching
release metadata is an observation, not an immutable snapshot guarantee.

The `opentargets_evidence` table preserves accepted native GraphQL envelopes,
including partial GraphQL errors, with query/variables, source URL, retrieval time,
canonical-response SHA-256, API/data versions and usability status. HTTP/read failures
have explicit error records; responses exceeding local limits are excluded.

```sql
SELECT phase, data_version_json, usable, error FROM opentargets_evidence;
```

Prediction rows retain the native variant ID, study/phenotype/ancestry/quality
context, SHAP base value and feature values/contributions. `model_version` remains
null because the queried API does not attest a model artifact version. The
[provider's L2G model is release dependent](https://platform-docs.opentargets.org/gentropy/locus-to-gene-l2g).
Its features remain prediction context, without additional heuristic score weight
or an independent-replication claim. Open Targets rows now require an exact match
to the resolved GRCh38 chromosome, position, REF and ALT, following the
[provider's variant ID convention](https://platform-docs.opentargets.org/variant).
The detail response must retain the same ID; conflicting discovery or detail rows
are excluded. Rows carry `genome_assembly` and `anchor_match` for inspection.
Unresolved alleles, hashed IDs and differing indel representations are skipped;
there is no rsID-only fallback or implicit normalization. This qualifies matching
to the resolved anchor. Native mapping and exact-label selection now have separate
qualification checks; other source joins still require work. Re-run gather for existing
analyses: previously staged rows are not upgraded by this change.

The synchronous bounds are explicit: 20 search hits per term, up to six selected
studies, 400 credible sets per study, 200 selected study loci for detail, and 100
predictions per locus. Colocalisation respects the requested cap up to 200. Native
counts and truncation warnings remain available. Each HTTP call uses a four-second
timeout and body deadline with a 4 MiB response cap; retained native observations
are limited to 12 MiB per gather. Exceeding a cap is not evidence of absence.

Evidence tables, warnings and metadata now stage together. The gather response
includes a citation hashing the full stored payload and top-level `_staging`.
Large warning lists and full long trait labels remain in `l2g_meta`; display
shortening is marked. Completed phases no longer emit late timeout warnings.
128 focused tests, TypeScript/lint and 41 static checks pass. Live provider
and MCP acceptance remain pending the environment's network/listener restrictions.

## Output staging and evidence references

All five tool pairs now check the complete UTF-8 response envelope, including
text, errors and metadata. Responses over 30 KiB use the existing L2G Durable
Object staging path. The returned `_staging` points to the saved output; full
JSON, Markdown, figures and original metadata remain in the canonical `payloads`
table. Inline responses carry a citation over their structured result; newly
staged responses carry a citation over the complete stored envelope. Existing
gather citations are preserved. These hashes identify derived output bytes;
they do not prove that every upstream source was release-pinned.

For a staged synthesis or SQL response, its new `data_access_id` identifies the
output. The original gather/evidence ID remains inside the stored result. Use
`l2g_get_schema` and bounded `l2g_query_data` reads to inspect it, and use the
original gather ID for scoring or synthesis again. Storage failures return a
bounded error with both MCP fields and `isError`, not a successful staging claim.

Synthesis includes the selected prediction's `l2g_support_json`. Required SQL
reads fail explicitly on query errors, partial rows or capped counts instead of
silently dropping evidence from a successful report. Source qualification and
complete upstream collection remain separate pending work.

## Optional gnomAD constraint context

`include_gnomad_context` now uses the dedicated gnomAD tool's `pli` field contract
and queries exact candidate Ensembl IDs on GRCh38. It retains LOEUF bounds and
percentile, pLI, Z-scores, gene/constraint flags, canonical and MANE transcript
identifiers, native GraphQL responses, request variables and response hashes.
Null constraints remain unavailable; missing constraint fields remain unknown.
Partial GraphQL errors and mismatched gene IDs/assemblies are retained as failed
observations and cannot supply displayed constraint metrics.

The [v4.1.1 announcement](https://gnomad.broadinstitute.org/news/2026-03-gnomad-v4-1-1/)
describes updated constraint calculations and quality flags. The documented
GRCh38-to-4.1.1 pipeline mapping retains its September 26 verification date;
the browser response does not attest that minor release. Therefore
`runtime_metric_release` remains null and `release_pinned` remains false.
Provider schema/pipeline files could not be re-read during this follow-up;
live contract acceptance remains pending.

Collection is capped at 40 distinct candidate IDs, with four-second request and
body deadlines and a 256 KiB response limit per gene. Missing IDs have no symbol
fallback; skipped genes, caps and failures are reported. Partial HTTP responses
are rejected. Synthesis reconstructs complete rows, rehashes the native response,
checks the endpoint/query/variables and requested assembly, and derives metrics,
flags and transcripts again from the exact returned gene. Changed stored summary
fields cannot override native evidence. Duplicate observations, missing native
evidence for a usable row and incomplete SQL reads fail explicitly. Older gathers
with all retained source/request fields undergo the same checks.

The synthesized context retains failed observations as `status: "error"` with
`identity_verified: false` and null metrics. Valid native null constraints remain
distinct from missing constraints and request failures. References identify the
native `gnomad` table, data-access ID and response hash. These are local evidence
consistency checks; they do not authenticate the provider or pin a release.
This optional annotation does not change gene scores. See the
[October 6 validation review](../../docs/audits/2026-10-06-l2g-gnomad-validation-review.md).

## Stable candidate selection

`max_genes_per_locus` now caps unique Ensembl gene IDs, ordered by maximum
qualified Open Targets score across the gathered anchors at that locus. Stable
IDs break score ties. Different genes sharing a symbol remain separate; multiple
symbols for one ID remain in `symbols_json`, with a deterministic display label.
Predictions without an exact gathered locus/anchor pair cannot create candidates.

For each selected gene, every supporting anchor gets its own candidate row.
Consequently, `candidate_genes_count` counts gene/anchor rows and can exceed the
unique-gene cap. Candidate rows carry `selection_rank` and `selection_score`;
`candidate_selection` records eligible, selected and omitted unique-gene counts
per locus. Counts describe the collected subset, not complete provider coverage.
The full gathered predictions remain queryable after candidate selection.

Final JSON includes each candidate's `lead_rsid` and each locus's `lead_rsids`
for selected candidates; Markdown names the anchors explicitly. The existing
singular locus `lead_rsid` remains a representative anchor. Cross-locus ranking
still counts each gene/locus once. Scoring now rejects failed or capped reads of
present evidence tables; larger-than-supported SQL views need bounded inspection
or a narrower gather until resumable scoring reads are implemented.

Re-run gather and score for older analyses. Gather uses `2026-10-01-gene-coloc`
and scoring uses `2026-10-01-trait-burden`; old candidate selections and scores
are not silently upgraded.

## Open Targets mapped-trait qualification — October 1, 2026

Search relevance alone no longer qualifies a prediction for ranking. The gatherer
prioritizes studies whose native `traitFromSourceMappedIds` contains the selected
ontology ID and whose `studyType` is `gwas`, then applies the existing six-study
cap. Summary-statistics availability is a secondary preference. The
[provider schema](https://api.platform.opentargets.org/api/v4/graphql/schema)
defines mapped phenotype IDs separately from source labels and background traits.

Each prediction retains `requested_trait_id` and `trait_match`. Ranking requires
`exact_mapped_trait_id`, matching credible-set and nested study IDs, and GWAS type
on both records. A source label, background trait, molecular QTL or related term
does not establish this match. CURIE colons normalize to underscores; namespaces
are kept distinct, without child-term expansion or inferred equivalence.

Unmatched or unresolved rows remain in `l2g_predictions` and native observations
for inspection, but cannot select candidate genes or supply their L2G score.
Scoring rechecks the native JSON and the selected trait in `l2g_meta`; it does not
trust a stored match marker alone. Selected support carries the trait through
synthesis and staged output. Earlier gathers and scores must be rerun.

This qualifies the study against the selected ontology term. Inspect
`trait_resolution` and `l2g_meta.efo_id`/`efo_label`, or supply an explicit `efo_id`.
Ontology mapping is not
proof of identical phenotype definitions, ancestry or independent replication.
The collected subset remains bounded and source snapshots are not pinned.

227 focused tests, TypeScript/lint and 41 static checks pass. Live provider and MCP
acceptance remain pending; no deployment was performed.

## Explicit and exact-label trait selection — October 1, 2026

Free-text queries no longer select the first OLS hit. The existing OLS adapter now
requests exact search with a 25-row limit. Local checks require the whole returned
page to account for `numFound`, plus one unique ontology identifier whose native
class label matches the query after case and whitespace normalization. Partial
labels and synonyms alone cannot select a trait. Missing paging/identity fields,
capped pages, conflicting exact IDs and lookup failures leave the trait unresolved.
This is uniqueness within the requested EFO search scope, not all ontologies.

An explicit `efo_id` takes precedence, accepts CURIE colon/underscore punctuation,
and is recorded as caller-selected without implying provider verification.
The `trait_resolution` table retains the original query, selection status, ID,
label, retrieval timestamp and complete native JSON response. The tool summary
reports `trait.resolution_status`; SQL can inspect all candidates. If no anchors
remain, lookup evidence is returned in error details and large errors are staged.
Unresolved selection cannot qualify Open Targets predictions for ranking.

Requests use the existing [OLS search service](https://www.ebi.ac.uk/ols4/ols3help)
with a 1 MiB response cap and seven-second request/body limits. This does not pin
an ontology release or establish clinical phenotype equivalence. Live OLS response
and Worker acceptance remain pending; fixtures cover exact/ambiguous labels,
missing/partial pages, errors, explicit IDs, both tool aliases and complete staging.
Re-gather and re-score older analyses under the current policies above.

## Native Ensembl mapping qualification — October 1, 2026

The L2G resolver now reads the native variation response for each assembly. A
requested rsID must match the native name or an explicit synonym. Selection
requires one distinct primary-chromosome mapping for the requested assembly;
conflicting mappings are unresolved, independent of response order. Identical
coordinate/strand/allele mappings are deduplicated. Alternate contigs are retained
in native evidence but do not stand in for primary chromosomes.

Allele-specific queries require a forward-strand mapping, two different explicit
ACGT sequences and a reference length consistent with the native start/end span.
Multiallelic, reverse/unknown-strand and symbolic representations retain usable
locus coordinates but no selected alleles. No strand conversion, indel padding,
left normalization or independent reference-sequence verification is inferred.
The [Ensembl schema](https://jun2026.archive.ensembl.org/info/docs/api/variation/variation_schema.html)
defines mapping orientation and reference-first allele order; the
[native GRCh37 example](https://grch37.rest.ensembl.org/variation/human/rs116035550?content-type=application/json)
illustrates a multiallelic mapping that this resolver leaves unresolved.

`coordinate_resolution` preserves both assemblies' request URLs, statuses,
retrieval timestamps, selected coordinates, full native responses and errors.
Requests have a six-second abort deadline, bounded body reads and a 1 MiB cap;
redirects are not followed. At the seven-second phase deadline, completed anchors
are retained, while late results cannot mutate inputs used by subsequent phases.
The shared resolver and other servers are unchanged. Native response and assembly
context do not establish a pinned Ensembl release. Live MCP acceptance is pending.

## Gene-qualified colocalisation — October 1, 2026

The existing Open Targets detail query now retains the molecular QTL gene,
study type, biosample, trans-QTL flag and complete native colocalisation row.
Qualification rechecks native GWAS trait/study identity and requires a distinct
molecular-QTL credible set whose `qtlGeneId` and study target both match the
candidate's Ensembl ID. The left credible set, allele, anchor, locus and observed
release must match an eligible L2G prediction. This enriches the existing bounded
candidate pool; it does not discover candidates from colocalisation alone.

COLOC/COLOC_PIP H4 and eCAVIAR CLPP retain separate maxima and references in
`scored_candidate_genes.coloc_support_json` and final synthesis. H4 takes
precedence in the existing heuristic formula; CLPP is a labeled fallback.
Missing, malformed, out-of-range or unsupported-method values cannot contribute.
Ties select a deterministic study-pair reference. SQL retains the gathered rows.

The [provider schema](https://api.platform.opentargets.org/api/v4/graphql/schema)
defines these identities and the [Gentropy colocalisation documentation](https://platform-docs.opentargets.org/gentropy/colocalisation)
distinguishes the methods. These maxima describe retrieved evidence, not independent
replication. L2G can already use colocalisation features, so adding this component
can double-count evidence. Source releases remain observations, not pinned snapshots.
Re-gather and re-score under the current policies above. Existing Code Mode execution,
dual tool aliases, Durable Object staging and SQL inspection remain in use.

The optional gnomAD context includes the [v4.1.2 constraint-file correction](https://gnomad.broadinstitute.org/news/2026-09-gnomad-v4-1-2/) in `constraint_provenance_json`. This published release notice does not attest the live API version or alter native metrics or candidate scores.

## Optional HPA source context (October 1, 2026)

`include_hpa_tissue_context: true` retains the complete Human Protein Atlas gene
JSON in the existing `hpa` table, with requested/observed gene identity, HTTP
status, retrieval time and canonical-JSON hash. Requests require stable human
Ensembl IDs, are capped at 20 candidate genes and 1 MiB per response, and use a
4-second request deadline and body timer without retries or redirects. Failed
and skipped observations remain explicit; defaults perform no HPA requests.

The [provider's documented gene example](https://www.proteinatlas.org/ENSG00000134057.json)
uses `RNA tissue specific nTPM` string values and an array of subcellular locations.
The previous adapter read an obsolete field and therefore missed this RNA context.
The display now uses at most eight named tissues with finite nTPM >= 1, while
retaining every native value separately. This is the provider's tissue-specificity
summary, not a complete tissue-expression profile; RNA nTPM and protein intensity
remain separate evidence. See [HPA data access](https://www.proteinatlas.org/about/help/dataaccess).

Synthesis emits `candidate_genes[].evidence.hpa_context` with source table/hash
references. It rechecks the native hash and exact gene ID and recomputes the
summary from the retained response. Oversized source rows are reconstructed
through bounded SQL reads; incomplete reads fail explicitly. Complete native JSON
stays queryable in the original gather. No HPA score contribution is introduced,
and the legacy `hpa_tissue_support` array remains unchanged. These are consistency
checks, not causal validation, provider authentication or a pinned release.

New gathers record `hpa_context_version: 1`; older gathers without native HPA
provenance remain usable but must be gathered again to populate the new context.
Selected provider fields were observed via web research. Local fixtures and real
staging/SQL tests pass; live Worker acceptance remains pending local port access.

## Optional ClinVar source context (October 1, 2026)

`include_clinvar: true` retains native ESearch and ESummary JSON, canonical hashes,
request URLs/IDs, retrieval times, HTTP status and errors in `clinvar_evidence`.
The `clinvar` table remains a display projection. Related-variant searches use the
[documented bare rsID query](https://www.ncbi.nlm.nih.gov/clinvar/docs/help/), capped
at three IDs per search. A shared ClinVar UID can remain linked to multiple searched
rsIDs; batch summary IDs are deduplicated without losing these links.

The [current ESummary example](https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=clinvar&id=65533&retmode=json)
separates germline classification/review status, somatic clinical impact and
oncogenicity. Display fields now preserve these distinctions; complete traits,
locations, submitted gene relationships and accession versions stay in the native
summary. The legacy `clinical_significance` projection prefers germline data and
explicitly labels an older-field fallback. A search result alone is not an exact
allele, candidate-gene or trait match, and contributes no L2G score.

Each lane considers at most 30 distinct rsIDs, spaces requests by at least 350 ms,
reserves time for one summary batch, and records deadline skips. This pacing is
per gather, not a fleet-wide IP quota coordinator; upstream rate-limit failures
remain explicit. Each response is bounded to 4 MiB with a four-second request/body
timeout, without redirects/retries. The request estimate now accounts for individual
searches. Completed ClinVar observations survive other stage-3 lane timeouts.

New gathers record `clinvar_context_version: 1`. Synthesis rechecks native hashes,
request identities and batch membership, then rebuilds `anchors[].clinvar_context`
with references to complete staged observations. Missing summaries/UIDs are explicit
diagnostics; no-match and errors remain distinct. Large cells use bounded SQL
reconstruction. Older gathers remain usable but need re-gathering for this context.
Neither release pinning nor complete coverage is claimed. Provider example fields
were observed through web research; live Worker acceptance remains pending.

## Genebass input and response validation (October 1, 2026)

`genebass_burden_sets` accepts up to four nonempty strings (64 characters each).
Duplicate sets are removed; `[]` disables this context. The existing 25-gene cap
now emits a warning when it truncates candidate coverage. Unexpected response
shapes, upstream error payloads, and invalid SKAT-O p-values produce warnings
instead of no-support or strong-support rows. Numeric p-values must be finite
and in `(0, 1]`; zero cannot establish a representable nonzero probability.

Each request now retains a `genebass_evidence` observation with its requested
gene/burden set, source URL, HTTP status, retrieval time, complete parsed JSON and
canonical JSON hash. Reads have a 1,900,000-byte response ceiling and four-second
request/body deadlines. At most two requests run concurrently, with an 11-second
lane deadline and an 8 MiB native-evidence budget. Requests that cannot fit
are recorded as explicit errors with no HTTP status or native response. Parsed error responses and malformed successful JSON
remain evidence; failed requests do not manufacture no-support rows.

Synthesis checks source context and hashes, recomputes the minimum from native
rows and returns its zero-based `associationIndex`, plus staged table/ID
references. Complete native JSON stays queryable through the existing SQL tools,
including oversized cells. Old gathers remain readable but need regathering to
acquire this evidence. New gathers use `genebass_context_version: 2`; version-1
observations retain their original URL checks and remain readable. Scores are unchanged.

The shared adapter now uses the provider web client's current
`https://main.genebass.org/api/phewas/{ensembl_gene_id}?burdenSet=...` route,
already used by the PheWAS server. Native `phewas[].Pvalue` means SKAT-O;
`Pvalue_Burden` and `Pvalue_SKAT` are different tests. The minimum retains
`associationArray: "phewas"` and a composite `phenotypeId` from trait type,
phenocode, sex, coding and modifier. No human-readable description is invented.
Legacy `associations` observations remain readable.

The captured APOE/pLoF response has 3,589 rows and 1,300,580 decoded bytes.
Replay checks preserve its full JSON/hash through DO staging, chunked SQL and
synthesis. Genebass reconstruction permits up to 2,400 internal SQL reads to
cover the larger evidence budget; other tables keep the existing 300-read cap.
The [provider walkthrough](https://app.genebass.org/walkthrough) describes
cross-phenotype tests, not an exact match to the requested trait. Context still
marks gene/trait matching, release pinning and per-observation contract validation
as unverified and contributes no gene score. Fresh provider and live MCP
acceptance are pending: the current sandbox denies DNS and local Worker binding.
Captured-source/replay receipts are in `scratch/2026-10-01-genebass-route/`.

<!-- license-and-data-terms:start -->
## License and data terms

The code in this repository is licensed under the Apache License 2.0 (see `LICENSE`).
If you use it in your work, please cite it (see `CITATION.cff`). Citation is requested, not required.

The license covers the code only. Data that this server returns stays under the terms of the source it came from.
You are responsible for your use of the data under those terms.
<!-- license-and-data-terms:end -->
