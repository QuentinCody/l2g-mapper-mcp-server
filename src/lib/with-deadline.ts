/** Report genuine phase timeouts; completed phases must not emit later warnings. */
export async function withDeadline<T>(promise: Promise<T>, ms: number, label: string, warnings: string[]): Promise<T | null> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<null>(resolve => {
    timer = setTimeout(() => { warnings.push(`${label} exceeded ${ms}ms budget — skipped`); resolve(null); }, ms);
  });
  try { return await Promise.race([promise, timeout]); }
  catch (error) { warnings.push(`${label} failed: ${error instanceof Error ? error.message : String(error)}`); return null; }
  finally { clearTimeout(timer); }
}
