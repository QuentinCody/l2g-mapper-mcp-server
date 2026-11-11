import { z } from "zod";

const hash = z.string().regex(/^[a-f0-9]{64}$/);
export const eqtlR8Schema = z.object({
  release: z.literal("r8_beta"),
  dataset_id: z.string().regex(/^QTD\d{6}$/),
  rsid: z.string().regex(/^rs[1-9]\d*$/),
  metadata_sha256: hash,
  file_sha256: hash,
  max_rows: z.number().int().min(1).max(500).default(200),
  score_fine_mapping: z.boolean().default(false).describe("Explicitly enable qualified tissue support from this beta credible-set snapshot; context only by default."),
}).strict();
export type EqtlR8Input = z.infer<typeof eqtlR8Schema>;
export const EQTL_R8_CONTEXT_VERSION = "2026-10-06-r8-parquet";

export function assertCatalogueSelection(caps: Record<string, unknown>) {
  if (caps.eqtl_catalogue != null && caps.eqtl_catalogue_r8 != null) throw new Error("Select only one of eqtl_catalogue or eqtl_catalogue_r8; dataset IDs are not interchangeable across releases");
}
