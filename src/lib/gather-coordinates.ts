import type { VariantCoord } from "@bio-mcp/shared/variants/resolve";
import { resolveAnchor } from "./coordinates";
import { withDeadline } from "./with-deadline";

/** Publish the completed phase only; late responses must not change gathered inputs. */
export async function gatherCoordinates(anchors: Array<{ rsid: string; grch38?: VariantCoord | null; grch37?: VariantCoord | null }>, warnings: string[]) {
  const completed = new Map<number, Awaited<ReturnType<typeof resolveAnchor>>>();
  const pending = Promise.all(anchors.map(async (anchor, index) => { completed.set(index, await resolveAnchor(anchor.rsid)); }));
  await withDeadline(pending, 7000, "coordinate normalization", warnings);
  const results = [...completed.entries()].sort(([a], [b]) => a - b);
  for (const [index, result] of results) {
    anchors[index].grch38 = result.grch38; anchors[index].grch37 = result.grch37;
    warnings.push(...result.warnings);
  }
  return results.flatMap(([, result]) => result.observations);
}
