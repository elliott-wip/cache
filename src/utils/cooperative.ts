import stableStringify from "safe-stable-stringify";
import { scheduler } from "node:timers/promises";

// Share the budget across consecutive bulk phases and concurrent requests.
// A new phase must not reset the clock and extend an uninterrupted turn.
let yieldAt = performance.now() + 8;

export async function forEachCooperatively<T>(
  items: Iterable<T>,
  callback: (item: T, index: number) => void,
): Promise<void> {
  let index = 0;
  for (const item of items) {
    if (index > 0 && performance.now() >= yieldAt) {
      await scheduler.yield();
      yieldAt = performance.now() + 8;
    }
    callback(item, index);
    index += 1;
  }
}

export async function mapCooperatively<T, R>(
  items: Iterable<T>,
  callback: (item: T, index: number) => R,
): Promise<R[]> {
  const results: R[] = [];
  await forEachCooperatively(items, (item, index) => {
    results.push(callback(item, index));
  });
  return results;
}

export async function flatMapCooperatively<T, R>(
  items: readonly T[],
  callback: (item: T, index: number) => readonly R[],
): Promise<R[]> {
  const results: R[] = [];
  for (let index = 0; index < items.length; index += 1) {
    if (index > 0 && performance.now() >= yieldAt) {
      await scheduler.yield();
      yieldAt = performance.now() + 8;
    }
    const mapped = callback(items[index]!, index);
    for (let mappedIndex = 0; mappedIndex < mapped.length; mappedIndex += 1) {
      if (mappedIndex > 0 && performance.now() >= yieldAt) {
        await scheduler.yield();
        yieldAt = performance.now() + 8;
      }
      results.push(mapped[mappedIndex]!);
    }
  }
  return results;
}

/** Internal collapse keys contain completed request objects, never undefined elements. */
export async function stableStringifyArrayCooperatively(
  items: readonly object[],
): Promise<string> {
  const parts = await mapCooperatively(items, (item) => stableStringify(item));
  return `[${parts.join(",")}]`;
}

/** Build a map without a synchronous full-batch Map constructor pass. */
export async function mapFromEntriesCooperatively<Key, Value>(
  entries: Iterable<readonly [Key, Value]>,
): Promise<Map<Key, Value>> {
  const result = new Map<Key, Value>();
  await forEachCooperatively(entries, ([key, value]) => {
    result.set(key, value);
  });
  return result;
}
