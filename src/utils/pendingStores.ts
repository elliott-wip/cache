import type { ReadonlyDeep } from "type-fest";

import type { CacheSpec } from "../types/00_CacheSpec.js";
import type {
  AnyParams,
  AnyValidators,
  ConsumerDirectives,
  Entry,
  NormalizedParams,
} from "../types/index.js";
import {
  birthDate,
  calculateStoreFor,
  classify,
  EntryClassification,
} from "./normalizedProducerResultResourceHelpers.js";
import { variantMatchesRequest } from "./varyHelpers.js";

/**
 * The producer wrappers' overlay of entries whose `cache.store()` is still in
 * flight.
 *
 * ## Why it exists
 *
 * A wrapper fires `cache.store()` after the producer resolves WITHOUT awaiting
 * it, so the caller gets its value as soon as the producer has it rather than
 * after a round trip to the backing store. Request collapsing covers callers
 * that arrive while the producer is running, but nothing covered the window
 * between the producer resolving and the store's write landing: a caller that
 * arrived in that window read the store, found nothing, and ran the producer
 * again. Against an in-memory store the window is a few microtasks; against a
 * worker-backed or networked store it is the store's write latency, which is
 * long enough that a plain sequential repeat (`await f(x); await f(x)`) paid
 * for the producer twice.
 *
 * This overlay closes that window: every entry handed to `cache.store()` is
 * held here until that store call settles, and the wrapper consults the
 * overlay before reading the store. An entry that would be a fresh hit had the
 * write already landed is served from here instead.
 *
 * ## What it does NOT do
 *
 * - It only serves entries that classify as {@link EntryClassification.Usable}
 *   under the caller's directives. A pending entry that is already stale (a
 *   producer with `freshUntilAge: 0`, say) is not served, even if a stale
 *   serve would have been allowed: the wrapper's stale-while-revalidate and
 *   stale-if-error paths need the store's lookup result, so those requests
 *   fall through to the store as before.
 * - It never serves an entry the store would not retain (`storeFor: 0`, or
 *   an entry already past every usefulness bound), so a producer that asked
 *   for its response not to be stored is not silently memoized for one call.
 * - It never outlives the write. An entry leaves the overlay when its store
 *   call settles, on SUCCESS (the store now answers for it) and on FAILURE
 *   alike. A failed write is forgotten, not pinned: the next caller reads the
 *   store, misses, and runs the producer again, exactly as if the overlay
 *   did not exist.
 * - It is per wrapper instance and in-process. Two wrappers over the same
 *   cache do not share it, and neither does another process.
 *
 * Variants are honored: an entry only matches a request whose (normalized)
 * params agree with the entry's `vary`, by the same rule the stores apply.
 *
 * @internal
 */
export class PendingStores<
  Spec extends CacheSpec,
  Validators extends AnyValidators,
  Params extends AnyParams,
> {
  /**
   * Pending entries by resource id. A `Set` (not a single slot) because one
   * id can have several variants in flight, and one variant can be written
   * twice in quick succession (two invocations whose collapse windows did
   * not overlap); {@link findUsable} picks among them the way the cache does.
   */
  readonly #byId = new Map<Spec["id"], Set<Entry<Spec, Validators, Params>>>();

  /**
   * Holds `entries` in the overlay until `stored` settles, then drops them
   * (whether it resolved or rejected -- see the class docs). `stored`'s
   * rejection is observed here but not swallowed for other holders of the
   * promise: the wrapper still attaches its own logging handlers.
   *
   * `entries` must already be normalized, so the same normalization is not
   * repeated per lookup; entries the store would not retain are skipped.
   */
  track(
    entries: readonly Entry<Spec, Validators, Params>[],
    stored: Promise<unknown>,
    now: Date,
  ): void {
    const tracked = entries.filter(
      (entry) => calculateStoreFor(entry, now) > 0,
    );
    if (tracked.length === 0) {
      return;
    }

    tracked.forEach((entry) => {
      const pending = this.#byId.get(entry.id);
      if (pending === undefined) {
        this.#byId.set(entry.id, new Set([entry]));
      } else {
        pending.add(entry);
      }
    });

    const untrack = () => {
      tracked.forEach((entry) => {
        const pending = this.#byId.get(entry.id);
        if (pending === undefined) {
          return;
        }
        pending.delete(entry);
        if (pending.size === 0) {
          this.#byId.delete(entry.id);
        }
      });
    };
    // `.then(f, f)` rather than `.finally(f)`: the latter would re-reject and
    // create an unhandled rejection of its own when the store fails.
    void stored.then(untrack, untrack);
  }

  /**
   * The pending entry that would satisfy the request as a fresh hit (the
   * cache's `usable` slot) had its write already landed, or `undefined` if
   * none would. Among several candidates, the newest by birth date wins --
   * the same rule `Cache.get` applies to stored entries.
   */
  findUsable(
    id: Spec["id"],
    normalizedParams: Readonly<NormalizedParams<Params>>,
    directives: ReadonlyDeep<ConsumerDirectives>,
    now: Date,
  ): Entry<Spec, Validators, Params> | undefined {
    const pending = this.#byId.get(id);
    if (pending === undefined) {
      return undefined;
    }

    let best: Entry<Spec, Validators, Params> | undefined;
    for (const entry of pending) {
      if (
        variantMatchesRequest(entry.vary, normalizedParams) &&
        classify(entry, directives, now) === EntryClassification.Usable &&
        (best === undefined || birthDate(entry) > birthDate(best))
      ) {
        best = entry;
      }
    }
    return best;
  }
}
