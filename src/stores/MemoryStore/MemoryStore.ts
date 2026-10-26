import {
  mapCooperatively,
  forEachCooperatively,
  mapFromEntriesCooperatively,
} from "../../utils/cooperative.js";
import { isNonEmptyArray } from "type-party/runtime/nonempty.js";
import type { CacheSpec, SpecForId } from "../../types/00_CacheSpec.js";
import type {
  StoreEntryResult,
  StoreGetManyResult,
} from "../../types/06_Store.js";
import {
  type AnyParams,
  type AnyValidators,
  type Entry,
  type EntryForId,
  type NormalizedParams,
  type Store,
  type StoreEntryInput,
  type StoreGetManyRequest,
} from "../../types/index.js";
import {
  birthDate,
  validatorsAsStored,
  validatorsEqual,
} from "../../utils/normalizedProducerResultResourceHelpers.js";
import type { JsonOf } from "../../utils/utils.js";
import {
  jsonStringify,
  keepMaxPerGroup,
  keepMaxPerGroupCooperatively,
} from "../../utils/utils.js";
import {
  requestVariantKeyForVaryKeys,
  resultVariantKey,
  type VariantKey,
  type VaryKeys,
} from "../../utils/varyHelpers.js";
import ExpiringEntryMap from "./ExpiringEntryMap.js";

// Full cache key that includes the variant key.
type FullCacheKey<ResourceId extends string> = JsonOf<
  readonly [ResourceId, VariantKey]
>;

/**
 * This class implements an in-memory store for cache entries. For details on
 * each method, see the interface.
 *
 * Note that this class is implemented to make get() fast, at the expense of
 * making store() slower, under the assumption that reads from the cache happen
 * much more often than new data is stored (which should be the case).
 */
export default class MemoryStore<
  Spec extends CacheSpec = CacheSpec,
  Validators extends AnyValidators = AnyValidators,
  Params extends AnyParams = AnyParams,
> implements Store<Spec, Validators, Params> {
  /**
   * This map stores metadata about each distinct `ResourceId` (i.e., primary
   * cache key) that's stored. Specifically...
   *
   * - When an incoming request comes in, we have to find entries that match on
   *   both the primary cache key (the `ResourceId`) and the secondary cache key
   *   (the `VariantKey`). However, to match on the latter, we can't compute all
   *   possible variant keys for the request, so, instead, we need to store
   *   which sets of `varyKeys` we've seen for producer results for this
   *   `ResourceId`. That's what `varyKeysSets` holds. This is consulted on each
   *   request. See note on {@link requestVariantKeyForVaryKeys} for details.
   *
   * - When all stored entries for a given ResourceId have been evicted/expired,
   *   we want to reclaim a bit of memory (by deleting the whole map entry for
   *   that `ResourceId`, so we use `entryVariantKeys` to track how many entries
   *   we're still storing for this `ResourceId`.
   *
   * - Similarly, during cache invalidation, we want to be able to delete all
   *   the stored entries for a given `ResourceId`, so we use `entryVariantKeys`
   *   to be able to find all of those.
   */
  private readonly resourceMetadataMap = new Map<
    Spec["id"],
    { varyKeysSets: VaryKeys[]; entryVariantKeys: VariantKey[] }
  >();

  /**
   * Meanwhile, this map wholes the actual cached entries, keyed by their full
   * cache key. We use an ExpiringEntryMap to efficiently support time- and
   * size-based expiration of entries. It stores the `ResourceId` in addition to
   * the entry solely so that, on expiration, we can decrement the entry count
   * for that resource id without having to parse the cache key.
   */
  private readonly entriesMap: ExpiringEntryMap<
    FullCacheKey<Spec["id"]>,
    [Entry<Spec, Validators, Params>, Spec["id"]]
  >;

  private readonly fallbackDeleteAfter: number;

  /**
   * @param opts.numItemsLimit If set, the store will limit the number of items
   *   it maintains by evicting least recently used items. Note that, while this
   *   caps the amount of memory the store will use, it also adds marginal
   *   overhead to every lookup, as the store must record that the looked up
   *   item is now the most recently accessed.
   *
   * @param opts.fallbackDeleteAfter When an item is stored, the caller (usually
   *   the Cache class) tells the store how long to retain the item for, based
   *   on how long it's likely to be useful for satisfying future requests.
   *   However, sometimes, the cache will tell the store that an entry can be
   *   stored forever. This usually happens if the producer doesn't limit the
   *   item's `maxStale`, in which case the data is potentially usable forever,
   *   as consumers can request arbitarily data using the maxStale directive.
   *   However, storing this sort of data forever is impractical in terms of
   *   memory usage. So, the fallbackDeleteAfter setting controls the TTL that
   *   should apply in these cases. Like all times in this caching setup, this
   *   is in seconds.
   *
   * @param opts.onItemEviction A callback that's called whenever an item is
   *   removed from the store -- whether because it expired or because it was
   *   evicted to make room for a new item under the `numItemsLimit` setting.
   */
  constructor(opts?: {
    numItemsLimit?: number;
    fallbackDeleteAfter?: number;
    onItemEviction?: (entry: Entry<Spec, Validators, Params>) => void;
  }) {
    const { numItemsLimit, onItemEviction, fallbackDeleteAfter } = opts ?? {};

    this.entriesMap = new ExpiringEntryMap({
      numItemsLimit,

      onItemEviction: async ([entry, resourceId]) => {
        this.onItemEviction(entry, resourceId);
        onItemEviction?.(entry);
      },
    });
    this.fallbackDeleteAfter = fallbackDeleteAfter ?? 60 * 60; /* 60 minutes */
  }

  private onItemEviction(
    entry: Entry<Spec, Validators, Params>,
    resourceId: Spec["id"],
  ) {
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const metadata = this.resourceMetadataMap.get(resourceId)!;
    const evictedItemVariantKey = resultVariantKey(entry.vary);

    metadata.entryVariantKeys = metadata.entryVariantKeys.filter(
      (it) => it !== evictedItemVariantKey,
    );

    if (metadata.entryVariantKeys.length === 0) {
      this.resourceMetadataMap.delete(resourceId);
    }
  }

  public async get<Id extends Spec["id"]>(
    id: Id,
    normalizedParams: NormalizedParams<Params>,
    options?: { signal?: AbortSignal },
  ): Promise<Entry<SpecForId<Spec, Id>, Validators, Params>[]> {
    options?.signal?.throwIfAborted();
    return this.#getOneSync(id, normalizedParams);
  }

  public async getMany<
    const Reqs extends readonly StoreGetManyRequest<Spec, Params>[],
  >(
    requests: Reqs,
    options?: { signal?: AbortSignal },
  ): Promise<StoreGetManyResult<Spec, Reqs, Validators, Params>> {
    options?.signal?.throwIfAborted();
    if (requests.length <= 32) {
      return requests.map((it) =>
        this.#getOneSync(it.id, it.params),
      ) as StoreGetManyResult<Spec, Reqs, Validators, Params>;
    }
    return isNonEmptyArray(requests)
      ? ((await mapCooperatively(requests, (it) =>
          this.#getOneSync(it.id, it.params),
        )) as StoreGetManyResult<Spec, Reqs, Validators, Params>)
      : ([] as StoreGetManyResult<Spec, Reqs, Validators, Params>);
  }

  #getOneSync<Id extends Spec["id"]>(
    id: Id,
    normalizedParams: NormalizedParams<Params>,
  ): EntryForId<Spec, Validators, Params, Id>[] {
    const resourceMetadata = this.resourceMetadataMap.get(id);

    if (!resourceMetadata) {
      return [];
    }

    return resourceMetadata.varyKeysSets.flatMap((varyKeys) => {
      const variantKey = requestVariantKeyForVaryKeys(
        normalizedParams,
        varyKeys,
      );

      const cacheKey = makeCacheKey(id, variantKey);
      const variantResult = this.entriesMap.get(cacheKey);
      return variantResult
        ? ([variantResult[0]] satisfies Entry<
            Spec,
            Validators,
            Params
          >[] as unknown as EntryForId<Spec, Validators, Params, Id>[])
        : [];
    });
  }

  private storeTail: Promise<unknown> = Promise.resolve();
  private pendingStores = 0;

  public async store(
    entriesWithTimes: readonly StoreEntryInput<Spec, Validators, Params>[],
  ): Promise<readonly StoreEntryResult[]> {
    // Keep overlapping writers ordered while their bulk passes yield. Each
    // call still compares every slot to its state before that call writes.
    // Single-entry callers depend on writes happening before store() returns
    // its promise (wrapProducer deliberately does not await that promise).
    if (entriesWithTimes.length <= 32 && this.pendingStores === 0) {
      return Promise.resolve(this.#storeSmall(entriesWithTimes));
    }
    this.pendingStores += 1;
    const pending = this.storeTail.then(() => this.#store(entriesWithTimes));
    this.storeTail = pending
      .finally(() => {
        this.pendingStores -= 1;
      })
      .catch(() => {});
    return pending;
  }

  #storeSmall(
    entries: readonly StoreEntryInput<Spec, Validators, Params>[],
  ): readonly StoreEntryResult[] {
    const prepared = entries.map((input, index) => {
      const variantKey = resultVariantKey(input.entry.vary);
      return {
        input,
        index,
        variantKey,
        cacheKey: makeCacheKey(input.entry.id, variantKey),
      };
    });
    const preCallBySlot = new Map(
      prepared.map(
        ({ cacheKey }) => [cacheKey, this.entriesMap.get(cacheKey)] as const,
      ),
    );
    const winners = keepMaxPerGroup({
      items: prepared,
      groupBy: (it) => it.cacheKey,
      maxBy: (it) => birthDate(it.input.entry).valueOf(),
    });
    winners.forEach((it) => this.#persist(it));
    const winnerIndexBySlot = new Map(
      winners.map(({ cacheKey, index }) => [cacheKey, index] as const),
    );
    return prepared.map(({ input, index, cacheKey }) =>
      winnerIndexBySlot.get(cacheKey) === index
        ? this.#relationshipToExisting(input.entry, preCallBySlot.get(cacheKey))
        : {},
    );
  }

  async #store(
    entriesWithTimes: readonly StoreEntryInput<Spec, Validators, Params>[],
  ): Promise<readonly StoreEntryResult[]> {
    // Derive each input's slot (variant + full cache key) once, up front.
    const prepared = await mapCooperatively(
      entriesWithTimes,
      (input, index) => {
        const variantKey = resultVariantKey(input.entry.vary);
        return {
          input,
          index,
          variantKey,
          cacheKey: makeCacheKey(input.entry.id, variantKey),
        };
      },
    );

    // The relationship is measured against what each slot held BEFORE this
    // call, so snapshot every touched slot's current entry now, before any
    // write. Reading a slot more than once here is harmless: nothing has been
    // written yet, so each read of a slot returns the same pre-call value.
    const preCallBySlot = await mapFromEntriesCooperatively(
      await mapCooperatively(
        prepared,
        ({ cacheKey }) => [cacheKey, this.entriesMap.get(cacheKey)] as const,
      ),
    );

    // Dedupe within the call per the contract's uniform rule: for each slot,
    // only the entry with the newest birth date persists (the same rule the
    // other stores apply).
    const winners = await keepMaxPerGroupCooperatively({
      items: prepared,
      groupBy: (it) => it.cacheKey,
      maxBy: (it) => birthDate(it.input.entry).valueOf(),
    });
    await forEachCooperatively(winners, (it) => this.#persist(it));

    const winnerIndexBySlot = await mapFromEntriesCooperatively(
      await mapCooperatively(
        winners,
        ({ cacheKey, index }) => [cacheKey, index] as const,
      ),
    );

    // Each slot's winner reports how its validators relate to that slot's
    // pre-call entry; every other input (a dropped within-call duplicate) is
    // omitted.
    return await mapCooperatively(prepared, ({ input, index, cacheKey }) =>
      winnerIndexBySlot.get(cacheKey) === index
        ? this.#relationshipToExisting(input.entry, preCallBySlot.get(cacheKey))
        : {},
    );
  }

  /** Writes one prepared entry into the store, updating the resource metadata. */
  #persist(prepared: {
    readonly input: StoreEntryInput<Spec, Validators, Params>;
    readonly variantKey: VariantKey;
    readonly cacheKey: FullCacheKey<Spec["id"]>;
  }): void {
    const { input, variantKey, cacheKey } = prepared;
    const { entry, maxStoreForSeconds: deleteAfter } = input;
    const { id } = entry;

    const resourceMetadata = this.#resourceMetadataFor(id);

    // Record this entry's varyKeys set and variant key (deduped). We use arrays
    // rather than sets because the per-resource counts are tiny and we'd rather
    // slow down store() than get(). The varyKeys array is canonicalized so
    // equal sets are reference-equal (and thus deduped).
    const varyKeys = canonicalSmallStringMultiset(Object.keys(entry.vary));
    if (!resourceMetadata.varyKeysSets.includes(varyKeys)) {
      resourceMetadata.varyKeysSets.push(varyKeys);
    }
    if (!resourceMetadata.entryVariantKeys.includes(variantKey)) {
      resourceMetadata.entryVariantKeys.push(variantKey);
    }

    const deleteAfterSeconds =
      deleteAfter === Infinity ? this.fallbackDeleteAfter : deleteAfter;
    this.entriesMap.set(cacheKey, [entry, id], deleteAfterSeconds * 1000);
  }

  /** Returns the resource's metadata record, creating an empty one if absent. */
  #resourceMetadataFor(id: Spec["id"]): {
    varyKeysSets: VaryKeys[];
    entryVariantKeys: VariantKey[];
  } {
    const existing = this.resourceMetadataMap.get(id);
    if (existing !== undefined) {
      return existing;
    }
    const created = { varyKeysSets: [], entryVariantKeys: [] };
    this.resourceMetadataMap.set(id, created);
    return created;
  }

  #relationshipToExisting(
    entry: Entry<Spec, Validators, Params>,
    existing:
      | readonly [Entry<Spec, Validators, Params>, Spec["id"]]
      | undefined,
  ): StoreEntryResult {
    // This store holds raw in-memory objects, so normalize BOTH sides to their
    // JSON-serialized form before checking emptiness or comparing, to agree
    // with the JSON-backed stores on type-violating values (e.g. `undefined`).
    const newValidators = validatorsAsStored(entry.validators);
    if (Object.keys(newValidators).length === 0) {
      return {};
    }
    if (existing === undefined) {
      return { relationshipToExistingStoredData: "is-new" };
    }
    return {
      relationshipToExistingStoredData: validatorsEqual(
        validatorsAsStored(existing[0].validators),
        newValidators,
      )
        ? "unchanged"
        : "changed",
    };
  }

  public async delete(id: Spec["id"]) {
    // A later invalidation must not be overtaken by a yielded earlier write.
    if (this.pendingStores > 0) await this.storeTail;
    const resourceMetadata = this.resourceMetadataMap.get(id);
    if (!resourceMetadata) {
      return;
    }

    for (const variantKey of resourceMetadata.entryVariantKeys) {
      const cacheKey = makeCacheKey(id, variantKey);
      this.entriesMap.delete(cacheKey);
    }

    this.resourceMetadataMap.delete(id);
  }

  public async close() {
    if (this.pendingStores > 0) await this.storeTail;
    this.entriesMap.close();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }
}

function makeCacheKey<Id extends string>(
  id: Id,
  variantKey: VariantKey,
): FullCacheKey<Id> {
  // Help TS realize that Jsonifying id will result in a string.
  // Without this, we get `Jsonify<NotJsonableToNull<Id>>`, which
  // TS doesn't reduce to string.
  return jsonStringify([id, variantKey]) satisfies JsonOf<
    [unknown, VariantKey]
  > as JsonOf<[Id, VariantKey]>;
}

/**
 * In JS, if you want to use an array/set/etc as a Map key, you have to pass the
 * exact same object that was used as the key when you want to do a lookup,
 * because JS doesn't have value equality. That's a giant pain, as we'd like to
 * have `map.set(new Set(['a','b']), x)`, then `map.get(new Set(['a','b']))`
 * work in our code above, where the set would be the set of keys that the entry
 * varied on. The work around is to stringify the value, but then you're using
 * extra memory and may have to parse it again to actually use it.
 *
 * The helper below makes it simple to instead use the value you want as a key,
 * by returning the same object every time it's given an array with the same
 * strings, including if the strings are given in a different order. I.e.,
 *
 * ```ts
 * canonicalSmallStringMultiset(['a', 'b']) ===
 *  canonicalSmallStringMultiset(['a', 'b']) ===
 *  canonicalSmallStringMultiset(['b', 'a'])
 * ```
 *
 * It's a multiset because it allows duplicate strings, but input arrays with
 * duplicates are not equal to those without, i.e.
 *
 * ```ts
 * canonicalSmallStringMultiset(['a', 'a']) !==
 *  canonicalSmallStringMultiset(['a'])
 * ```
 *
 * The name refers to "small string" because it's optimized for multi-sets with
 * a few number of strings; with more items, it'd be better to use an actual JS
 * Set.
 */
const canonicalSmallStringMultiset = (() => {
  const canonical = new Map<string, readonly string[]>();

  return (arr: readonly string[]) => {
    const key = jsonStringify(arr.slice().sort());
    const canonicalArr = canonical.get(key);
    if (!canonicalArr) {
      canonical.set(key, arr);
      return arr;
    }

    return canonicalArr;
  };
})();
