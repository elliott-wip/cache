import { chunk } from "es-toolkit";
import type { CacheSpec } from "../types/00_CacheSpec.js";
import type { AnyParams } from "../types/01_Params.js";
import type { AnyValidators } from "../types/02_Validators.js";
import type { EntryForId } from "../types/06_Normalization.js";
import type { Store, StoreGetManyRequest } from "../types/06_Store.js";
import { raceWithSignal } from "./utils.js";

/**
 * The most requests that one flush passes to one `store.getMany()` call. A
 * flush with more requests splits them into several concurrent calls. The cap
 * splits a batch when it flushes; it never makes a batch flush early.
 */
export const MAX_REQUESTS_PER_BATCHED_GET_MANY = 500;

type StoredEntry<
  Spec extends CacheSpec,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = EntryForId<Spec, Validators, Params, Spec["id"]>;

type QueuedRead<
  Spec extends CacheSpec,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = {
  readonly request: StoreGetManyRequest<Spec, Params>;
  readonly signal: AbortSignal | undefined;
  readonly resolve: (entries: StoredEntry<Spec, Validators, Params>[]) => void;
  readonly reject: (error: unknown) => void;
};

/**
 * Gathers the store reads that arrive in one event-loop turn and runs them as
 * one `store.getMany()` call. This is what `Cache`'s `batchReads` option turns
 * on; see that option for the contract that callers see.
 *
 * The first read opens a batch and schedules its flush with `setImmediate`.
 * Every later read in the same turn joins that batch, and a read after the
 * flush opens the next one. There is no timer and no configurable window: a
 * batch holds its reads for one turn, never longer.
 *
 * The batcher only shares the QUERY. Each read gets its own slot of the
 * `getMany` result, which the store fills per request, so batching never makes
 * two requests share one result.
 *
 * The flush runs in the async context (for example, the `AsyncLocalStorage`
 * stores and the active tracing span) of the read that opened the batch,
 * because `setImmediate` captures that context. So the store sees one query in
 * the first caller's context, even though it also answers the other callers.
 */
export class StoreReadBatcher<
  Spec extends CacheSpec,
  Validators extends AnyValidators,
  Params extends AnyParams,
> {
  readonly #store: Store<Spec, Validators, Params>;
  #queue: QueuedRead<Spec, Validators, Params>[] = [];
  #scheduledFlush: NodeJS.Immediate | undefined;

  constructor(store: Store<Spec, Validators, Params>) {
    this.#store = store;
  }

  /**
   * Reads one request's entries through the next batched `getMany` call.
   *
   * The shared `getMany` call runs without a signal, because one caller's
   * abort must not cancel the other callers' reads. So `signal` controls only
   * this caller's view of the read:
   * - an already-aborted signal rejects before the read enters the queue;
   * - an abort while the read waits for its flush, or while its batch's query
   *   runs, rejects at once with the signal's reason. A read that aborts while
   *   it waits is also dropped from the query.
   */
  async read(
    request: StoreGetManyRequest<Spec, Params>,
    signal: AbortSignal | undefined,
  ): Promise<StoredEntry<Spec, Validators, Params>[]> {
    signal?.throwIfAborted();

    const { promise, resolve, reject } =
      Promise.withResolvers<StoredEntry<Spec, Validators, Params>[]>();
    this.#queue.push({ request, signal, resolve, reject });
    this.#scheduledFlush ??= setImmediate(() => {
      this.#flush();
    });

    return raceWithSignal(promise, signal);
  }

  /**
   * Starts the queued reads now, without waiting for their scheduled flush.
   * `Cache.close()` calls this, so that every read that the cache accepted
   * while it was open is in flight when the store closes, as an unbatched read
   * would be.
   *
   * This does not wait for the reads to settle. The store's own `close()`
   * decides what happens to reads in flight, within the caller's timeout, for
   * batched and unbatched reads alike.
   */
  flushNow(): void {
    if (this.#scheduledFlush === undefined) {
      return;
    }
    clearImmediate(this.#scheduledFlush);
    this.#flush();
  }

  #flush(): void {
    this.#scheduledFlush = undefined;
    const queued = this.#queue;
    this.#queue = [];

    const live = queued.filter((read) => {
      if (read.signal?.aborted !== true) {
        return true;
      }
      // `raceWithSignal` already rejected this caller with the abort reason.
      // Settling the inner promise too leaves no read pending forever.
      read.reject(read.signal.reason);
      return false;
    });

    for (const reads of chunk(live, MAX_REQUESTS_PER_BATCHED_GET_MANY)) {
      // `#readBatch` calls `store.getMany` before its first `await`, so the
      // read is in flight when this returns. It never rejects.
      void this.#readBatch(reads);
    }
  }

  /**
   * Runs one `getMany` call and settles each of its reads. Never rejects: a
   * store failure rejects every read in the call with the store's error.
   */
  async #readBatch(
    reads: readonly QueuedRead<Spec, Validators, Params>[],
  ): Promise<void> {
    try {
      const results = await this.#store.getMany(
        reads.map(({ request }) => request),
      );
      if (results.length !== reads.length) {
        throw new Error(
          `store.getMany() returned ${String(results.length)} results for ${String(reads.length)} requests; it must return one result per request`,
        );
      }
      reads.forEach((read, i) => {
        // Non-null assertion is safe: the lengths match (checked above).
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        read.resolve(results[i]!);
      });
    } catch (error) {
      reads.forEach((read) => {
        read.reject(error);
      });
    }
  }
}
