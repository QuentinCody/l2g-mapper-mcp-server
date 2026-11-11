import { coerceRow, type TableResult } from "@bio-mcp/shared/adapters/eqtl/sumstats";

/** Compare every parsed scalar with the retained TSV, including underflow and NA rules. */
export function assertEqtlTable(table: TableResult & { native_header: string; raw_rows: string[] }) {
  if (!Array.isArray(table.columns) || !table.columns.length || table.columns.some(c => typeof c !== "string" || !c)
    || new Set(table.columns).size !== table.columns.length || typeof table.native_header !== "string"
    || JSON.stringify(table.native_header.split("\t").map(c => c.replace(/^#/, ""))) !== JSON.stringify(table.columns)
    || !Array.isArray(table.rows) || !Array.isArray(table.raw_rows) || table.raw_rows.length !== table.rows.length
    || table.row_count !== table.rows.length) throw new Error("Staged eQTL Catalogue TSV/header mismatch");
  for (let index = 0; index < table.rows.length; index++) {
    const raw = table.raw_rows[index]; const row = table.rows[index];
    if (typeof raw !== "string" || !row || typeof row !== "object" || Array.isArray(row)) throw new Error("Invalid staged eQTL Catalogue row");
    const cells = raw.split("\t"); const parsed = coerceRow(table.columns, cells);
    if (cells.length !== table.columns.length || Object.keys(row).length !== table.columns.length
      || table.columns.some(c => row[c] !== parsed[c])) throw new Error("Staged eQTL Catalogue parsed row differs from native TSV");
  }
}
