import type { EqtlFileFetchOptions } from "@bio-mcp/shared/adapters/eqtl/http";
import { CATALOGUE_RESOURCES_BASE, EBI_FTP_BASE } from "@bio-mcp/shared/adapters/eqtl/http";

let nextFtpSlot = 0;

export function assertEqtlSource(url: string, datasetId?: string, studyId?: string) {
  const parsed = new URL(url);
  const match = /^\/pub\/databases\/spot\/eQTL\/sumstats\/(QTS\d{6})\/(QTD\d{6})\/\2\.(?:all|cc)\.tsv\.gz(?:\.tbi)?$/.exec(parsed.pathname);
  if (!url.startsWith(`${EBI_FTP_BASE}/sumstats/`) || parsed.username || parsed.password || parsed.search || parsed.hash ||
    !match || (datasetId && match[2] !== datasetId) || (studyId && match[1] !== studyId)) throw new Error("Unsupported eQTL source-file URL");
}

export function assertEqtlFineMapSource(url: string, datasetId?: string, studyId?: string) {
  const parsed = new URL(url);
  const match = /^\/pub\/databases\/spot\/eQTL\/susie\/(QTS\d{6})\/(QTD\d{6})\/\2\.credible_sets\.tsv\.gz$/.exec(parsed.pathname);
  if (!url.startsWith(`${EBI_FTP_BASE}/susie/`) || parsed.username || parsed.password || parsed.search || parsed.hash ||
    !match || (datasetId && match[2] !== datasetId) || (studyId && match[1] !== studyId)) throw new Error("Unsupported eQTL credible-set URL");
}

async function ftpSlot(signal: AbortSignal) {
  signal.throwIfAborted();
  const slot = Math.max(Date.now(), nextFtpSlot);
  nextFtpSlot = slot + 2000;
  await new Promise<void>((resolve, reject) => {
    const onAbort = () => { clearTimeout(timer); reject(signal.reason); };
    const timer = setTimeout(() => { signal.removeEventListener("abort", onAbort); resolve(); }, Math.max(0, slot - Date.now()));
    signal.addEventListener("abort", onAbort, { once: true });
  });
  signal.throwIfAborted();
}

async function read(url: string, headers: Record<string, string>, signal: AbortSignal) {
  signal.throwIfAborted();
  const response = await fetch(url, { headers, signal, redirect: "manual" });
  if (!response.ok) {
    await response.body?.cancel().catch(() => undefined);
    throw Object.assign(new Error(`eQTL Catalogue HTTP ${response.status}`), { status: response.status });
  }
  return response;
}

/** Request-scoped cancellation; fixed provider hosts, no retries or redirects. */
export function eqtlCatalogueTransport(signal: AbortSignal) {
  return {
    resourcesFetch: async (path: string) => {
      if (!["/tabix/tabix_ftp_paths.tsv", "/data_tables/dataset_metadata_r7.tsv"].includes(path)) throw new Error("Unsupported r7 metadata path");
      return read(CATALOGUE_RESOURCES_BASE + path, { Accept: "text/plain" }, signal);
    },
    fetchFile: async (url: string, options?: EqtlFileFetchOptions) => {
      if (url.startsWith(`${EBI_FTP_BASE}/susie/`)) assertEqtlFineMapSource(url);
      else assertEqtlSource(url);
      await ftpSlot(signal);
      const range = options?.range;
      return read(url, { Accept: "application/octet-stream", ...(range ? { Range: `bytes=${range.start}-${range.end ?? ""}` } : {}) }, signal);
    },
  };
}
