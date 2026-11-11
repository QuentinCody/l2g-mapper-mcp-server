/// <reference types="node" />
import { gzipSync } from "node:zlib";
import { createHash } from "node:crypto";
import { expect, it, vi } from "vitest";
import { eqtlFineNative, eqtlInput } from "../../test/eqtl-catalogue-fixture";
import { assertEqtlFineMapping, fetchEqtlFineMapping } from "./eqtl-fine-mapping";
import { assertEqtlTable } from "./eqtl-table-evidence";
import { readTsvGz } from "@bio-mcp/shared/adapters/eqtl/sumstats";

it("uses the bounded supplied transport, pins full compressed bytes and keeps exact native rows", async () => {
  const native = eqtlFineNative(); const table = native.fine_mapping!.result!;
  const raw = table.native_header + "\n" + table.raw_rows[0] + "\n" + table.raw_rows[0].replace("chr1_10_A_G", "chr1_11_A_G") + "\n";
  const bytes = gzipSync(raw); const digest = createHash("sha256").update(bytes).digest("hex");
  const io = { resourcesFetch: vi.fn(), fetchFile: vi.fn(async () => new Response(bytes, { headers: { "content-length": String(bytes.length) } })) };
  const result = await fetchEqtlFineMapping(native.dataset, "1_10_A_G", { ...eqtlInput, fine_mapping_sha256: digest }, io, new AbortController().signal);
  expect(result).toMatchObject({ status: "observed", expected_sha256: digest, result: { row_count: 1, raw_rows: table.raw_rows, rows: table.rows, file_evidence: { sha256: digest } } });
  expect(io.fetchFile).toHaveBeenCalledTimes(1);
  assertEqtlFineMapping(result, native.dataset, "1_10_A_G", { ...eqtlInput, fine_mapping_sha256: digest });
});

it.each(["hash", "partial", "missing-file", "cancelled"])("keeps a failed fine-mapping read explicit (%s)", async mode => {
  const native = eqtlFineNative(); const controller = new AbortController();
  if (mode === "cancelled") controller.abort(new Error("cancelled"));
  if (mode === "missing-file") native.dataset.credible_sets_url = null;
  const io = { resourcesFetch: vi.fn(), fetchFile: vi.fn(async () => new Response(gzipSync("variant\nchr1_10_A_G\n"), { status: mode === "partial" ? 206 : 200 })) };
  const result = await fetchEqtlFineMapping(native.dataset, "1_10_A_G", { ...eqtlInput, fine_mapping_sha256: "d".repeat(64) }, io, controller.signal);
  expect(result).toMatchObject({ status: "error", result: null, error: expect.any(String) });
});

it("does not fetch an unrequested file and rejects aborts before decoding", async () => {
  const io = { resourcesFetch: vi.fn(), fetchFile: vi.fn() };
  expect(await fetchEqtlFineMapping(eqtlFineNative().dataset, "1_10_A_G", eqtlInput, io, new AbortController().signal)).toBeUndefined();
  expect(io.fetchFile).not.toHaveBeenCalled();
  const controller = new AbortController();
  await expect(readTsvGz({ url: "https://fixture.invalid" }, async () => { controller.abort(new Error("cancelled")); return new Response(gzipSync("variant\nchr1_10_A_G\n")); }, controller.signal)).rejects.toThrow("cancelled");
});

it.each(["hash", "source", "status", "rows", "header", "raw", "variant", "request", "unrequested"])("rejects corrupted fine-map provenance with an otherwise usable result (%s)", mode => {
  const n = eqtlFineNative(); const fine = n.fine_mapping!; const table = fine.result!;
  if (mode === "hash") table.file_evidence.sha256 = "a".repeat(64);
  if (mode === "source") table.source_file = "https://example.test/file.gz";
  if (mode === "status") table.file_evidence.http_status = 206;
  if (mode === "rows") table.rows[0].pip = 0.1;
  if (mode === "header") table.native_header = "variant";
  if (mode === "raw") table.raw_rows = [];
  if (mode === "variant") { table.rows[0].variant = "chr1_10_A_T"; table.raw_rows = table.rows.map(row => table.columns.map(c => row[c]).join("\t")); }
  if (mode === "request") fine.requested_variant = "chr1_10_A_T";
  expect(() => assertEqtlFineMapping(fine, n.dataset, "1_10_A_G", { ...eqtlInput, ...(mode === "unrequested" ? {} : { fine_mapping_sha256: "d".repeat(64) }) })).toThrow();
});

it("rejects inconsistent nominal projections while retaining numeric underflow semantics", () => {
  const n = eqtlFineNative(); assertEqtlTable(n.region);
  n.region.rows[0].beta = 10;
  expect(() => assertEqtlTable(n.region)).toThrow("differs from native TSV");
});
