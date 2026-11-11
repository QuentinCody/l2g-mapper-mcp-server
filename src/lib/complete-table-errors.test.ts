import { afterEach, expect, it, vi } from "vitest";
import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";
import { readCompleteTable } from "./complete-table";
vi.mock("@bio-mcp/shared/staging/utils", () => ({ queryDataFromDo: vi.fn() }));
afterEach(() => vi.resetAllMocks());
const namespace = {} as DurableObjectNamespace;
const result = (rows: unknown[], extra = {}) => ({ rows, row_count: rows.length, sql: "fixture", data_access_id: "saved", executed_at: "fixture", ...extra });

it.each([{ truncated: true }, { count_capped: true }, { total_matching: 2 }])("rejects capped reads rather than scoring partial evidence: %j", async flags => {
  vi.mocked(queryDataFromDo).mockResolvedValue(result([{}], flags));
  await expect(readCompleteTable(namespace, "saved", "opentargets_evidence")).rejects.toThrow("capped");
  expect(queryDataFromDo).toHaveBeenCalledTimes(1);
});

it("does not retry ordinary storage failures as an oversized-row read", async () => {
  vi.mocked(queryDataFromDo).mockRejectedValue(new Error("storage unavailable"));
  await expect(readCompleteTable(namespace, "saved", "opentargets_evidence")).rejects.toThrow("storage unavailable");
  expect(queryDataFromDo).toHaveBeenCalledTimes(1);
});

it.each(["missing", "short", "invalid_json", "budget"])("fails incomplete or excessive reconstruction (%s) without returning partial rows", async mode => {
  vi.mocked(queryDataFromDo).mockImplementation(async (_ns, _id, sql) => {
    if (sql === "SELECT * FROM opentargets_evidence") throw new Error("LOSSLESS_QUERY_RESULT_TOO_LARGE");
    if (sql.startsWith("PRAGMA")) return result([{ name: "response_json" }]);
    if (sql.includes("COUNT(*)")) return result([{ total: 1 }]);
    if (sql.includes("length(")) return result([{ size: mode === "budget" ? 3000000 : 10 }]);
    if (mode === "missing") return result([]);
    return result([{ part: "x".repeat(mode === "short" ? 1 : mode === "budget" ? 8192 : 10) }]);
  });
  await expect(readCompleteTable(namespace, "saved", "opentargets_evidence")).rejects.toThrow();
  expect(vi.mocked(queryDataFromDo).mock.calls.length).toBeLessThanOrEqual(300);
});
