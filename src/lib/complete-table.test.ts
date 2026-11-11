import { expect, it, vi } from "vitest";
vi.mock("cloudflare:workers", () => ({ RpcTarget: class {}, WorkerEntrypoint: class {}, DurableObject: class { constructor(public ctx: unknown, public env: unknown) {} } }));
import { storage } from "../../test/l2g-storage";
import { readCompleteTable } from "./complete-table";

it.each(["opentargets_evidence", "eqtl_catalogue_evidence", "finngen_evidence"])("reconstructs oversized Unicode and escaped-text rows without changing order or values (%s)", async table => {
  const store = storage();
  try {
    const original = [
      { sequence: 2, response_json: JSON.stringify({ text: '😀"\\'.repeat(20000) }), nullable: null, marker: false },
      { sequence: 1, response_json: JSON.stringify({ text: "α".repeat(80000) }), nullable: "null", marker: true },
    ];
    const stub = store.namespace.get(store.namespace.idFromName("large"));
    const response = await stub.fetch(new Request("https://fixture/process", { method: "POST", body: JSON.stringify({ data: { [table]: original } }) }));
    expect(response.ok).toBe(true);
    const rows = await readCompleteTable<Record<string, unknown>>(store.namespace, "large", table, `SELECT * FROM ${table} ORDER BY sequence`);
    expect(rows.map(row => ({ sequence: row.sequence, response_json: row.response_json, nullable: row.nullable, marker: Boolean(row.marker) }))).toEqual([...original].reverse());
  } finally { store.close(); }
});

it("rejects unsupported reconstruction tables before querying", async () => {
  await expect(readCompleteTable({} as DurableObjectNamespace, "saved", "untrusted")).rejects.toThrow("Unsupported");
});
