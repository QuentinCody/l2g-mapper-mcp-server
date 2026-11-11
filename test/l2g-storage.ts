import { DatabaseSync } from "node:sqlite";
import { L2gDataDO } from "../src/do";

export function storage() {
  const databases: DatabaseSync[] = [];
  let failInsert = false;
  const instances = new Map<string, { fetch: (request: Request) => Promise<Response> }>();
  const namespace = { idFromName: (id: string) => id, get: (id: string) => {
    if (instances.has(id)) return instances.get(id)!;
    const db = new DatabaseSync(":memory:"); databases.push(db); let ready = Promise.resolve();
    const exec = (sql: string, ...values: any[]) => {
      if (failInsert && /INSERT INTO ["`]?scored_candidate_genes/.test(sql)) { failInsert = false; throw new Error("fixture score insert failed"); }
      const rows = db.prepare(sql).all(...values);
      const iterator = rows[Symbol.iterator]();
      return { next: () => iterator.next(), toArray: () => rows, one: () => rows[0], [Symbol.iterator]: () => iterator };
    };
    const transactionSync = <T>(run: () => T): T => {
      db.exec("BEGIN"); try { const result = run(); db.exec("COMMIT"); return result; } catch (error) { db.exec("ROLLBACK"); throw error; }
    };
    const ctx = { blockConcurrencyWhile: (run: () => Promise<void>) => { ready = run(); return ready; }, storage: { sql: { exec }, transactionSync } };
    const instance = new L2gDataDO(ctx as unknown as DurableObjectState, {} as Env);
    const stub = { fetch: async (request: Request) => { await ready; return instance.fetch(request); } };
    instances.set(id, stub); return stub;
  } } as unknown as DurableObjectNamespace;
  return { namespace, failScoreInsert: () => { failInsert = true; }, close: () => databases.forEach(db => { db.close(); }) };
}
