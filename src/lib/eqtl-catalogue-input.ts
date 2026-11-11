import { z } from "zod";

const hash = z.string().regex(/^[a-f0-9]{64}$/);
/** One explicitly selected dataset/anchor pair keeps FTP fan-out bounded. */
export const eqtlCatalogueSchema = z.object({
  release: z.literal("r7"),
  dataset_id: z.string().regex(/^QTD\d{6}$/),
  rsid: z.string().regex(/^rs[1-9]\d*$/),
  metadata_sha256: hash,
  paths_sha256: hash,
  max_rows: z.number().int().min(1).max(500).default(200),
  fine_mapping_sha256: hash.optional().describe("Opt into qualified scoring using this complete r7 credible-set file SHA-256; requires exact nominal/gene/allele joins and native PIP >= 0.95."),
}).strict();
export type EqtlCatalogueInput = z.infer<typeof eqtlCatalogueSchema>;
export const EQTL_CATALOGUE_CONTEXT_VERSION = "2026-10-06-r7-finemap";
