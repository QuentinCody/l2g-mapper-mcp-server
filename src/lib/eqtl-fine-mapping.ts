import type { EqtlDataset } from "@bio-mcp/shared/adapters/eqtl/ftp-catalogue";
import { readTsvGz, type WholeFileResult } from "@bio-mcp/shared/adapters/eqtl/sumstats";
import { assertFileHash } from "@bio-mcp/shared/adapters/eqtl/file-evidence";
import type { EqtlCatalogueInput } from "./eqtl-catalogue-input";
import { assertEqtlFineMapSource, type eqtlCatalogueTransport } from "./eqtl-catalogue-transport";
import { assertEqtlTable } from "./eqtl-table-evidence";

export interface EqtlFineMapping {
  requested_variant: string; max_rows: number; expected_sha256: string;
  status: "observed" | "error"; error: string | null; result: WholeFileResult | null;
}

export function assertEqtlFineMapping(observation: EqtlFineMapping | undefined, dataset: EqtlDataset, variant: string, input: EqtlCatalogueInput) {
  if (!input.fine_mapping_sha256) {
    if (observation !== undefined) throw new Error("Unexpected staged eQTL fine mapping");
    return;
  }
  if (!observation || observation.requested_variant !== `chr${variant}` || observation.max_rows !== input.max_rows
    || observation.expected_sha256 !== input.fine_mapping_sha256) throw new Error("Staged eQTL fine-mapping request mismatch");
  if (observation.status === "error" && observation.result === null && typeof observation.error === "string" && observation.error) return;
  const table = observation.result; const evidence = table?.file_evidence;
  if (observation.status !== "observed" || observation.error !== null || !table || !dataset.credible_sets_url
    || table.source_file !== dataset.credible_sets_url || table.rows.length > input.max_rows || typeof table.truncated !== "boolean"
    || table.coverage_complete !== false || !evidence || evidence.source_url !== table.source_file || evidence.http_status !== 200
    || evidence.content_range !== null || evidence.hash_scope !== "complete_response_bytes" || evidence.sha256 !== input.fine_mapping_sha256
    || !Number.isSafeInteger(evidence.byte_length) || evidence.byte_length < 1 || evidence.byte_length > 16 * 1024 * 1024) throw new Error("Staged eQTL credible-set evidence mismatch");
  assertEqtlFineMapSource(table.source_file, dataset.dataset_id, dataset.study_id);
  assertEqtlTable(table);
  if (table.rows.some(row => row.variant !== observation.requested_variant)) throw new Error("Staged eQTL fine-mapping variant filter mismatch");
}

/** One bounded complete compressed file, filtered to the exact anchor variant. */
export async function fetchEqtlFineMapping(dataset: EqtlDataset, variant: string, input: EqtlCatalogueInput,
  io: ReturnType<typeof eqtlCatalogueTransport>, signal: AbortSignal): Promise<EqtlFineMapping | undefined> {
  if (!input.fine_mapping_sha256) return undefined;
  const observation: EqtlFineMapping = { requested_variant: `chr${variant}`, max_rows: input.max_rows,
    expected_sha256: input.fine_mapping_sha256, status: "error", error: null, result: null };
  try {
    if (!dataset.credible_sets_url) throw new Error("Selected dataset has no credible-set file");
    assertEqtlFineMapSource(dataset.credible_sets_url, dataset.dataset_id, dataset.study_id);
    const result = await readTsvGz({ url: dataset.credible_sets_url, maxRows: input.max_rows, expected_sha256: input.fine_mapping_sha256,
      rowFilters: { variant: observation.requested_variant } }, io.fetchFile, signal);
    signal.throwIfAborted();
    assertFileHash(result.file_evidence, input.fine_mapping_sha256);
    observation.result = result; observation.status = "observed";
  } catch (error) { observation.error = error instanceof Error ? error.message.slice(0, 500) : "Credible-set read failed"; }
  return observation;
}
