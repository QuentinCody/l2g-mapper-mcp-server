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
