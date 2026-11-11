import { afterEach, expect, it, vi } from "vitest";
import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";
import { loadBurdenSupport } from "./burden-scoring";
vi.mock("@bio-mcp/shared/staging/utils", () => ({ queryDataFromDo: vi.fn() }));
afterEach(() => vi.resetAllMocks());

it("leaves legacy gathers with no burden lane usable without loading their potentially large native pages", async () => {
  vi.mocked(queryDataFromDo).mockResolvedValue({ rows: [], row_count: 0, sql: "fixture", data_access_id: "saved", executed_at: "fixture" });
  const result = await loadBurdenSupport({} as DurableObjectNamespace, "saved", ["opentargets_evidence"], "EFO_1");
  expect(result.support.size).toBe(0);
  expect(queryDataFromDo).toHaveBeenCalledExactlyOnceWith({}, "saved", "SELECT phase FROM opentargets_evidence WHERE phase = 'gene burden' LIMIT 1", 1);
});

it("does not turn a failed evidence probe into a negative association", async () => {
  vi.mocked(queryDataFromDo).mockRejectedValue(new Error("storage unavailable"));
  await expect(loadBurdenSupport({} as DurableObjectNamespace, "saved", ["opentargets_evidence"], "EFO_1")).rejects.toThrow("storage unavailable");
});
