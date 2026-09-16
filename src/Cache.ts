import { groupBy, sortBy, sumBy } from "es-toolkit";
import { EventEmitter } from "events";
import type { InvariantOf, ReadonlyDeep } from "type-fest";
import { isNonEmptyArray, mapNonEmpty } from "type-party/runtime/nonempty.js";
import {
  cacheStoreEntryChannel,
  publishCacheRead,
  publishCacheStoreEntry,
  type CacheReadOutcome,
} from "./diagnostics.js";
import type { ReadonlyConsumerRequest } from "./types/03_ConsumerRequest.js";
import {
  type Entry,
  type NormalizeParamName,
  type NormalizeParamValue,
  type NormalizedProducerResultResource,
} from "./types/06_Normalization.js";
import {
  type AnyParams,
  type AnyParamValue,
  type AnyValidators,
  type CacheSpec,
  type ConsumerDirectives,
  type Logger,
  type ProducerResultResource,
  type ResourceTypeName,
  type ResourceTypes,
  type ResourceTypeSpec,
  type SpecForId,
  type SpecOf,
  type Store,
  type StoreEntryInput,
  type StoreEntryResult,
  type Vary,
} from "./types/index.js";
import {
  classifyIdAgainst,
  resourceType,
  resourceTypesEntries,
  type ResourceTypesEntries,
} from "./resourceTypeClassification.js";
import { type Bind1 } from "./types/utils.js";
import {
  normalizeParams,
  normalizeProducerResultResource,
  normalizeVary,
} from "./utils/normalization.js";
import * as entryUtils from "./utils/normalizedProducerResultResourceHelpers.js";
import { assertUnreachable, defaultLoggersByComponent } from "./utils/utils.js";

/**
 * Thrown when an id matches zero resource types in the cache's registry.
 * See the classification contract in {@link ResourceTypes}' module docs.
 */
export class UnclassifiableIdError extends Error {
  override readonly name = "UnclassifiableIdError";
  readonly cacheName: string;
  readonly id: string;

  constructor(args: {
    cacheName: string;
    id: string;
    message?: string;
    /**
     * When one or more registry guards THREW on this id (rather than
     * returning false), the error(s) land here (an `AggregateError` when
     * more than one threw) so the underlying parse failure stays
     * debuggable from the log line. See {@link Cache.classify}.
     */
    cause?: unknown;
  }) {
    super(
      args.message ??
        `Cache "${args.cacheName}": id ${JSON.stringify(args.id)} matches no resource type in the registry`,
      args.cause === undefined ? undefined : { cause: args.cause },
    );
    this.cacheName = args.cacheName;
    this.id = args.id;
  }
}

/**
 * Thrown when an id matches more than one resource type in the cache's
 * registry. Registries must partition the id space, so this always indicates
 * a bug in the registry's `matches` guards; classification fails loud (over
 * first-match-wins) so the overlap is caught the first time it occurs rather
 * than silently resolved by object-key order.
 */
export class AmbiguousResourceTypeError extends Error {
  override readonly name = "AmbiguousResourceTypeError";
  readonly cacheName: string;
  readonly id: string;
  readonly matchedResourceTypes: readonly string[];

  constructor(args: {
    cacheName: string;
    id: string;
    matchedResourceTypes: readonly string[];
    message?: string;
  }) {
    super(
      args.message ??
        `Cache "${args.cacheName}": id ${JSON.stringify(args.id)} matches more than one resource type in the registry (${args.matchedResourceTypes.join(", ")})`,
    );
    this.cacheName = args.cacheName;
    this.id = args.id;
    this.matchedResourceTypes = args.matchedResourceTypes;
  }
}

/**
 * The result of a cache lookup. MatchingSpecs is the subset of CacheSpec
 * variants whose id is compatible with the request's id (`Id`).
 */
export type CacheLookupResult<
  MatchingSpecs extends CacheSpec,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = {
  usable?: Entry<MatchingSpecs, Validators, Params> | undefined;
  usableWhileRevalidate?: Entry<MatchingSpecs, Validators, Params> | undefined;
  usableIfError?: Entry<MatchingSpecs, Validators, Params> | undefined;
  validatable: Entry<MatchingSpecs, Validators, Params>[];
};

/**
 * A backing store that supports `StoreSupportedTypes` and must cover
 * `RequiredSpec` -- the spec whose ids the cache will actually ask for.
 *
 * The store only has to support *at least* the cache's own resource types. That
 * matters because `Store` is invariant in its `Spec` -- `Spec` sits in both
 * `store()`/`delete()`'s parameters and `get()`/`getMany()`'s return types --
 * so a `Store<Wide>` is NOT assignable to a `Store<Narrow>` even though, for
 * any id the narrow cache actually asks for, the wide store returns exactly the
 * same thing. Rather than fight that variance, the store's own spec is captured
 * in `StoreSupportedTypes` and merely *checked* for coverage, so a
 * general-purpose store needs no artificially narrowed type arguments.
 *
 * The intersected guard is where coverage is enforced: TS has no lower-bound
 * constraint (`StoreSupportedTypes super RequiredSpec` is not expressible), so a
 * store that does NOT cover picks up an unsatisfiable requirement instead.
 * Inference still comes from the plain `Store<...>` member.
 *
 * Note: the Store interface should _already_ be invariant in its Params, but
 * TS's underlying handling of functions as always bivariant (which the compiler
 * tries to hide/override in some cases under strictFunctionTypes, but this
 * doesn't apply to class methods; see
 * https://www.typescriptlang.org/tsconfig/#strictFunctionTypes) means that we
 * have to use `InvariantOf<Params>` explicitly to get the type errors we want.
 */
type CoveringStore<
  RequiredSpec extends CacheSpec,
  StoreSupportedTypes extends CacheSpec,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = Store<StoreSupportedTypes, Validators, InvariantOf<Params>> &
  // The tuple brackets make this a whole-union comparison rather than a
  // distributive, member-by-member one. Without them, a `RequiredSpec` union
  // distributes and each covered member contributes `unknown`, which absorbs the
  // other members' requirements -- so a store covering ANY one resource type
  // would satisfy a registry of several.
  ([RequiredSpec] extends [StoreSupportedTypes]
    ? unknown
    : {
        readonly __storeMustSupportAtLeast: RequiredSpec;
      });

/**
 * Everything a {@link Cache} needs, as one bag. The store is a field rather
 * than a separate constructor argument because `options` has no useful default
 * (`name` and `resourceTypes` are both required), so there was never a call
 * shape where passing the store alone said anything.
 */
export type CacheOptions<
  RT extends ResourceTypes,
  Validators extends AnyValidators = AnyValidators,
  Params extends AnyParams = AnyParams,
  /**
   * The spec the STORE supports, which may be WIDER than this cache's own
   * `SpecOf<RT>` -- most stores are general-purpose. It is last and defaulted so
   * that `CacheOptions<RT, Validators, Params>` keeps meaning what it always
   * did; inserting it earlier silently shifts `Validators` into this slot at
   * every 3-argument use site.
   */
  StoreSupportedTypes extends CacheSpec = SpecOf<RT>,
> = {
  /**
   * REQUIRED. The backing store that will actually hold cache entries. It must
   * support at least this cache's own resource types, but may support more --
   * see {@link CoveringStore}.
   */
  store: CoveringStore<SpecOf<RT>, StoreSupportedTypes, Validators, Params>;
  /**
   * REQUIRED. Names this cache instance (≈ the backing table) in every
   * diagnostics message. Instance-unique per process by convention;
   * uniqueness is not enforced.
   */
  name: string;
  /**
   * REQUIRED. The cache's resource-type registry; see {@link ResourceTypes}.
   * Must partition the id space. For a cache with exactly one resource type,
   * {@link singleTypeCacheOptions} builds this (and names it) for you.
   */
  resourceTypes: RT;
  logger?: Logger;
  onGetAfterClose?: "throw" | "act-empty";
  onStoreAfterClose?: "throw" | "no-op";
  normalizeParamName?: NormalizeParamName<Params>;
  normalizeParamValue?: NormalizeParamValue<Params>;
};

/**
 * This class implements a cache using a generalized version of HTTP's
 * underlying caching model, but w/o encoding HTTP-specific details (like header
 * parsing), so that it can be useful in more contexts. As part of this
 * generalization, this class talks about a cached value's "id and request
 * params" rather than its "URI and request headers", and cache directives are
 * provided as explicit arguments (not header strings). Similarly, it refers to
 * the "producer and consumer" of cached values, rather than the "server and the
 * client". Beyond renaming, it leaves open the set of available validators for
 * users to define (e.g., db row version numbers), rather than hard-coding HTTP
 * validators like etags and last-modified dates, and it supports a set of
 * directives somewhat more general than their HTTP equivalents.
 *
 * For (critical) background details on the HTTP caching model, see the docs.
 *
 * The cache is parameterized over a {@link ResourceTypes} registry, which
 * names each kind of resource the cache can hold and pairs it with a runtime
 * classifier for its id sub-space plus a (phantom) content type. The cache's
 * {@link CacheSpec} union is derived from the registry via {@link SpecOf}:
 * the get/store/getMany methods narrow the content type based on the id of
 * each request, and reject mismatched (id, content) pairs at compile time.
 * At runtime, every id the cache sees (requests, stored entries -- primary
 * and supplemental -- and deletes) is classified against the registry, and
 * ids that match zero or multiple resource types are loudly rejected.
 *
 * TODO: support the concept of warnings.
 * See https://tools.ietf.org/html/rfc7234#section-5.5
 */
export default class Cache<
  const RT extends ResourceTypes = ResourceTypes,
  Validators extends AnyValidators = AnyValidators,
  in out Params extends AnyParams = AnyParams,
  StoreSupportedTypes extends CacheSpec = SpecOf<RT>,
> {
  readonly #logger: Bind1<Logger, "cache">;
  readonly #dataStore: Store<StoreSupportedTypes, Validators, Params>;
  #closed = false;
  readonly #onGetAfterClose: "throw" | "act-empty";
  readonly #onStoreAfterClose: "throw" | "no-op";
  readonly #resourceTypeEntries: ResourceTypesEntries<RT>;

  /**
   * Names this cache instance (≈ the backing table) in every diagnostics
   * message. Instance-unique per process by convention; uniqueness is not
   * enforced.
   */
  public readonly name: string;

  /**
   * The cache's resource-type registry (constructor `options.resourceTypes`).
   *
   * Note: beyond letting callers introspect the registry, this public
   * property is what lets the producer wrappers *infer* `RT` from a cache
   * value: the other RT-mentioning members (`classify`'s
   * `ResourceTypeName<RT>`, the `SpecOf<RT>`-derived method types) are
   * keyof-/conditional-shaped and unusable as inference sources, so without
   * a bare-`RT` member, wrapping a cache built over a narrowed one-entry
   * registry would collapse `RT` to its constraint (producer `req.id` would
   * become plain `string`).
   */
  public readonly resourceTypes: RT;

  public readonly emitter = new EventEmitter();
  public readonly normalizeParamName: NormalizeParamName<Params>;
  public readonly normalizeParamValue: NormalizeParamValue<Params>;

  constructor(
    options: CacheOptions<RT, Validators, Params, StoreSupportedTypes>,
  ) {
    const unboundLogger = options.logger ?? defaultLoggersByComponent.cache;
    this.#logger = unboundLogger.bind(null, "cache");
    this.#dataStore = options.store;
    this.#onGetAfterClose = options.onGetAfterClose ?? "throw";
    this.#onStoreAfterClose = options.onStoreAfterClose ?? "throw";
    this.name = options.name;
    this.resourceTypes = options.resourceTypes;
    this.#resourceTypeEntries = resourceTypesEntries(options.resourceTypes);
    this.normalizeParamName = options.normalizeParamName ?? ((it) => it);
    this.normalizeParamValue =
      options.normalizeParamValue ??
      (<K extends keyof Params>(_name: K, v: AnyParamValue) =>
        v as Params[K] & AnyParamValue);
  }

  private static bestEntry<
    EntrySpec extends CacheSpec,
    Validators extends AnyValidators,
    Params extends AnyParams,
  >(suitableEntries: readonly Entry<EntrySpec, Validators, Params>[]) {
    // "When more than one suitable response is stored, a cache MUST use
    // the most recent response (as determined by the Date header field)."
    // https://tools.ietf.org/html/rfc7234#section-4
    return sortBy(suitableEntries, [(it) => entryUtils.birthDate(it)]).at(-1);
  }

  // Create this as an instance member to get `this` binding
  private normalizeParams = (params: ReadonlyDeep<Partial<Params>>) =>
    normalizeParams(this.normalizeParamName, this.normalizeParamValue, params);

  // Create this as an instance member to get `this` binding
  private normalizeVary = (vary: Vary<Params>) =>
    normalizeVary(this.normalizeParamName, this.normalizeParamValue, vary);

  /**
   * Total classification of an id against the cache's registry: evaluates
   * EVERY registry entry's `matches` guard, and returns the name of the one
   * resource type that matched.
   *
   * Throws {@link UnclassifiableIdError} (0 matches) or
   * {@link AmbiguousResourceTypeError} (>1 match); every guard is evaluated
   * (rather than first-match-wins) so registry overlaps are caught the first
   * time an id hits them.
   *
   * Classification runs on every get/getMany request id, every stored entry
   * id (primary and supplemental), and every delete id -- classification
   * failures reject the operation BEFORE touching the store.
   *
   * A guard that THROWS is treated as not matching: guards routinely reject
   * foreign ids by failing to parse them (e.g. a `JSON.parse`-based guard
   * fed a non-JSON id), so a throw is a "no" -- and when no type ends up
   * matching, the guard error(s) surface as the
   * {@link UnclassifiableIdError}'s `cause` rather than leaking as a raw
   * parse error with no cache/id attribution.
   */
  public classify(id: string): ResourceTypeName<RT> {
    const classification = classifyIdAgainst(this.#resourceTypeEntries, id);
    switch (classification.matched) {
      case "one": {
        return classification.name;
      }
      case "none": {
        throw new UnclassifiableIdError({
          cacheName: this.name,
          id,
          cause: classification.cause,
        });
      }
      case "many": {
        throw new AmbiguousResourceTypeError({
          cacheName: this.name,
          id,
          matchedResourceTypes: classification.names,
        });
      }
      default: {
        return assertUnreachable(classification);
      }
    }
  }

  /**
   * Gets relevant items from the cache, always returning a promise for an
   * object with four possible keys:
   *
   * - `usable`: this is the cached value (if any) that satisfies the consumer's
   *   request, given its cache directives, without requiring even background
   *   revalidation. **If this key holds a value, all other keys in this object
   *   will be undefined/empty.** This value will almost always be fresh, since
   *   stale values aren't usable by defualt; the exception is if the consumer
   *   allowed stale responses (sans revalidation) through the `maxStale`
   *   directive. If multiple cached values would've have been suitable, this
   *   holds the preferred one (which currently means the newest).
   *
   * - `usableWhileRevalidate`: this holds the preferred response (if any)
   *   that's usable to satisfy the client's request, but that must be
   *   (re-)validated in the background.
   *
   * - `usableIfError`: holds an entry (if any) that's usable only in case of an
   *   error reaching the producer while trying to fetch/revalidate the cached
   *   value. If there's a `usableWhileRevalidate` response, this key will
   *   always be empty [because the usableWhileRevalidate response should be
   *   returned before calling the producer, so there's no chance on an error.]
   *
   * - `validatable`: when validation is necessary (either because no usable
   *    response is held by the cache, or the usable response requires
   *    background re-validation), this array holds all entries in the cache
   *    that have validation info -- including, possibly, responses present in
   *    the other returned keys -- and that would be usable were the producer
   *    to confirm (revalidate) that the resource's current state matches the
   *    state identified by the validation info. Otherwise, this array is empty.
   *    These are returned so that the user can make a conditional request for
   *    the latest content that takes into account the validation info (e.g.,
   *    the etags w/ `If-None-Match`) of these saved responses. These responses
   *    are probably stale, but it's possible they're not (e.g., if consumer
   *    used a maxAge directive shorter than the producer's freshness lifetime).
   *
   * The result's content type is narrowed to the spec variants whose id is
   * compatible with `req.id`. So, e.g., if the cache's `Spec` is a union and
   * `req.id` is a literal that only matches one variant, callers don't have to
   * narrow the returned content themselves.
   *
   * Emits one `read` diagnostics message (see `CACHE_READ_CHANNEL_NAME`) per
   * call: the lookup result on success, or `found: "read-failed"` with the
   * error if the store threw (and then the error still propagates). An
   * *aborted* read emits nothing, matching the `throwIfAborted` fast path.
   */
  public async get<Id extends SpecOf<RT>["id"]>(
    req: ReadonlyConsumerRequest<Params, Id>,
    options?: { signal?: AbortSignal },
  ): Promise<CacheLookupResult<SpecForId<SpecOf<RT>, Id>, Validators, Params>> {
    options?.signal?.throwIfAborted();

    const { id, params, directives } = req;
    const resourceType = this.classify(id);

    if (this.#closed) {
      if (this.#onGetAfterClose === "throw") {
        this.#logger("trace", "received request when closed and throwing");
        throw new Error("Store has been closed...");
      }
      this.#logger(
        "trace",
        "received request when closed, so returning no entries",
      );
      publishCacheRead({
        cache: this.name,
        resourceType,
        resourceId: id,
        found: "none",
      });
      return { validatable: [] };
    }

    const now = new Date();
    const normalizedParams = this.normalizeParams(params);

    this.#logger("trace", "received request", { id, params, normalizedParams });
    this.#logger("trace", "requested entries from the store");

    // Unlike producer invocations, store reads are NOT collapsed: N concurrent
    // identical requests perform N row fetches. A point read is cheap next to
    // the producer calls collapsing exists to protect.
    //
    // Worth knowing if that ever changes: reads must not reuse results across
    // a time window the way producer invocations do. A lookup result is a
    // freshness decision evaluated at a specific `now`, so replaying one
    // serves decisions computed against a stale clock. Only a PENDING-only
    // promise share is sound, which bounds the skew to the read's own
    // duration.
    const cacheEntries = await this.#dataStore
      .get(id, normalizedParams, options)
      .catch((error: unknown) => {
        // A read that FAILED is reported on the channel (one message, same as
        // a successful lookup) before the error propagates, so subscribers can
        // use the read channel as a total denominator -- see
        // CACHE_READ_CHANNEL_NAME. An *aborted* read is not a failed read: the
        // caller cancelled, and aborts already emit nothing here (the
        // `throwIfAborted` at the top of this method rejects before any
        // message), so staying silent keeps that consistent instead of
        // inflating read-failure rates with client cancellations.
        if (options?.signal?.aborted !== true) {
          publishCacheRead({
            cache: this.name,
            resourceType,
            resourceId: id,
            found: "read-failed",
            error,
          });
        }
        throw error;
      });

    const { result, outcome } = this.#processCacheEntries(
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
      cacheEntries as unknown as NormalizedProducerResultResource<
        SpecForId<SpecOf<RT>, Id>,
        Validators,
        Params
      >[],
      directives,
      now,
      { requestIndex: 0 },
    );

    publishCacheRead({
      cache: this.name,
      resourceType,
      resourceId: id,
      ...outcome,
    });

    return result;
  }

  /**
   * Gets relevant items from the cache for multiple requests in a single
   * operation. This method is functionally equivalent to calling `get()` for
   * each individual request and concatenating the results, but is optimized by
   * using the store's `getMany` method to batch the underlying data store
   * operations.
   *
   * The result is a tuple typed per-request: each output slot's content type
   * is narrowed to the spec variants compatible with the corresponding input
   * request's id.
   *
   * Emits one `read` diagnostics message PER request -- the lookup result on
   * success, or `found: "read-failed"` with the error for every request in the
   * batch if the underlying store read throws (and then the error still
   * propagates). An *aborted* read emits nothing. All request ids are
   * classified up front, so a classification failure rejects the whole call
   * before touching the store, emitting nothing.
   *
   * @param requests Array of consumer requests to process
   * @returns Promise that resolves to an array of CacheLookupResult objects in
   * the same order as the input requests
   */
  public async getMany<
    const Reqs extends readonly ReadonlyConsumerRequest<
      Params,
      SpecOf<RT>["id"]
    >[],
  >(
    requests: Reqs,
    options?: { signal?: AbortSignal },
  ): Promise<GetManyResults<Reqs, RT, Validators, Params>> {
    options?.signal?.throwIfAborted();

    if (!isNonEmptyArray(requests)) {
      return [] as GetManyResults<Reqs, RT, Validators, Params>;
    }

    // Classify every request id up front: a classification failure rejects
    // the whole operation before we touch the store.
    const resourceTypes = mapNonEmpty(requests, (req) => this.classify(req.id));

    const publishRead = (requestIndex: number, outcome: CacheReadOutcome) => {
      publishCacheRead({
        cache: this.name,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        resourceType: resourceTypes[requestIndex]!,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        resourceId: requests[requestIndex]!.id,
        ...outcome,
      });
    };

    if (this.#closed) {
      if (this.#onGetAfterClose === "throw") {
        this.#logger(
          "trace",
          "received getMany request when closed and throwing",
        );
        throw new Error("Store has been closed...");
      }
      this.#logger(
        "trace",
        "received getMany request when closed, so returning no entries",
      );

      return mapNonEmpty(requests, (_req, i) => {
        publishRead(i, { found: "none" });
        return { validatable: [] };
      });
    }

    const now = new Date();

    // Prepare requests for the store's getMany method
    this.#logger("trace", "received getMany request", {
      requestCount: requests.length,
      requests: requests.map((r) => ({ id: r.id, params: r.params })),
    });
    this.#logger("trace", "requested entries from the store via getMany");

    // Use the store's optimized getMany method
    const cacheEntriesForRequests = await this.#dataStore
      .getMany(
        mapNonEmpty(requests, (req) => ({
          id: req.id,
          params: this.normalizeParams(req.params),
        })),
        options,
      )
      .catch((error: unknown) => {
        // One `read-failed` message PER REQUEST, not one per call: the channel
        // is one-message-per-lookup, and a batch read is N lookups that all
        // failed together. Emitting one message for the batch would make a
        // subscriber's per-id read counts disagree with the successful path.
        // Aborts stay silent for the reason given in `get`.
        if (options?.signal?.aborted !== true) {
          requests.forEach((req, i) => {
            publishCacheRead({
              cache: this.name,
              // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
              resourceType: resourceTypes[i]!,
              resourceId: req.id,
              found: "read-failed",
              error,
            });
          });
        }
        throw error;
      });

    this.#logger("trace", "received entries from the store via getMany", {
      resultCount: sumBy(cacheEntriesForRequests, (it) => it.length),
    });

    // Process each request and return results in the same order
    return mapNonEmpty(requests, (req, i) => {
      const { result, outcome } = this.#processCacheEntries(
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        cacheEntriesForRequests[
          i
        ]! as unknown as NormalizedProducerResultResource<
          SpecForId<SpecOf<RT>, SpecOf<RT>["id"]>,
          Validators,
          Params
        >[],
        req.directives,
        now,
        { requestIndex: i },
      );
      publishRead(i, outcome);
      return result;
    });
  }

  /**
   * Stores ProducerResultResources that it assumes were _just now_ retrieved
   * from the producer. If the result wasn't retrieved just now, its retrieval
   * time can be specified.
   *
   * The (id, content) pairs in `data` are checked against the cache's derived
   * spec at compile time, so a `Story[]` cannot be stored under a `story:...`
   * id, etc.
   *
   * Every entry's id -- primary and supplemental callers alike pass a flat
   * list here -- is classified against the registry up front; any
   * classification failure rejects the call before persisting anything (so a
   * producer minting a malformed id can't write a permanently unreadable
   * row).
   *
   * Emits one `store-entry` diagnostics message PER entry (see
   * `CACHE_STORE_ENTRY_CHANNEL_NAME`).
   */
  public async store(
    data: readonly ProducerResultResource<SpecOf<RT>, Validators, Params>[],
  ): Promise<readonly StoreEntryResult[]> {
    // Classify all entry ids up front -- any failure rejects before we
    // normalize, emit events, or persist anything.
    const resourceTypes = data.map((it) => this.classify(it.id));

    if (this.#closed) {
      if (this.#onStoreAfterClose === "throw") {
        this.#logger(
          "trace",
          "received store request when closed and throwing",
        );
        throw new Error("Store has been closed...");
      }
      this.#logger(
        "trace",
        "received store request after throwing and doing nothing",
      );
      return [];
    }

    const now = new Date();
    const entriesWithTimes = data.map((it) => {
      const entry = normalizeProducerResultResource(
        this.normalizeVary,
        it,
        now,
      );
      return {
        entry,
        maxStoreForSeconds: entryUtils.calculateStoreFor(entry, now),
      };
    });

    this.#logger(
      "trace",
      "storing the following entries with (possibly inferred) storeFor times",
      entriesWithTimes,
    );

    entriesWithTimes.forEach(({ entry, maxStoreForSeconds }) => {
      this.emitter.emit("store", entry, maxStoreForSeconds);
    });

    const results = await this.#dataStore.store(
      entriesWithTimes as unknown as readonly StoreEntryInput<
        StoreSupportedTypes,
        Validators,
        Params
      >[],
    );

    // The results array is parallel to the input entries (see Store.store).
    // One message PER ENTRY -- primary plus every supplemental -- so this is
    // the package's other O(entries) diagnostics payload, and `publish` would
    // discard each message without preventing its construction. Hence the
    // subscriber test around the whole loop rather than inside it.
    if (cacheStoreEntryChannel.hasSubscribers) {
      results.forEach(({ relationshipToExistingStoredData }, i) => {
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        const { entry } = entriesWithTimes[i]!;
        publishCacheStoreEntry({
          cache: this.name,
          // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
          resourceType: resourceTypes[i]!,
          resourceId: entry.id,
          vary: entry.vary,
          validators: entry.validators,
          relationshipToExistingStoredData,
        });
      });
    }

    return results;
  }

  /**
   * Deletes all stored entries for resources with the given id (regardless of
   * variant). The id is classified against the registry first; a
   * classification failure rejects the call before touching the store.
   *
   * After `close()`, behaves like {@link Cache.store} (deletes are writes):
   * throws or silently no-ops per the `onStoreAfterClose` option.
   */
  public async delete(id: SpecOf<RT>["id"]): Promise<void> {
    this.classify(id);

    if (this.#closed) {
      if (this.#onStoreAfterClose === "throw") {
        this.#logger(
          "trace",
          "received delete request when closed and throwing",
        );
        throw new Error("Store has been closed...");
      }
      this.#logger(
        "trace",
        "received delete request when closed, so doing nothing",
      );
      return;
    }

    return this.#dataStore.delete(id);
  }

  public async close(timeout?: number) {
    this.#closed = true;
    return (
      this.#dataStore.close?.(timeout) ?? this.#dataStore[Symbol.asyncDispose]()
    );
  }

  public async [Symbol.asyncDispose]() {
    return this.close(60_000);
  }

  /**
   * Processes cache entries for a single request, returning both the
   * CacheLookupResult and the {@link CacheReadOutcome} that describes it for
   * the read channel. The outcome is returned rather than re-derived from
   * `result` because this is where it is actually decided -- each branch below
   * knows its own answer, including which entry (if any) it selected and hence
   * which slot to name, so there is nothing to reconstruct. This is the core
   * logic shared between get() and getMany().
   */
  #processCacheEntries<Id extends SpecOf<RT>["id"]>(
    entries: readonly Entry<SpecForId<SpecOf<RT>, Id>, Validators, Params>[],
    directives: ReadonlyDeep<ConsumerDirectives>,
    now: Date,
    context: { requestIndex: number },
  ): {
    result: CacheLookupResult<SpecForId<SpecOf<RT>, Id>, Validators, Params>;
    outcome: CacheReadOutcome;
  } {
    const classifiedEntries = groupBy(entries, (it) =>
      entryUtils.classify(it, directives, now),
    );

    this.#logger("trace", "classified stored entries for request", {
      requestIndex: context.requestIndex,
      classifiedEntries,
    });

    const logAndReturn = (
      result: CacheLookupResult<SpecForId<SpecOf<RT>, Id>, Validators, Params>,
      outcome: CacheReadOutcome,
    ) => {
      this.#logger("trace", "chose/returned this data for request", {
        requestIndex: context.requestIndex,
        res: result,
      });
      return { result, outcome };
    };

    const usableEntries =
      classifiedEntries[entryUtils.EntryClassification.Usable];

    if (usableEntries) {
      // Non-null assertion is safe because of groupBy mechanics.
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      const selected = Cache.bestEntry(usableEntries)!;
      return logAndReturn(
        { usable: selected, validatable: [] },
        { found: "usable", vary: selected.vary },
      );
    }

    const validatableEntries = entries.filter(entryUtils.isValidatable);

    const usableWhileRevalidateEntries =
      classifiedEntries[entryUtils.EntryClassification.UsableWhileRevalidate];

    if (usableWhileRevalidateEntries) {
      // Non-null assertion is safe because of groupBy mechanics.
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      const selected = Cache.bestEntry(usableWhileRevalidateEntries)!;
      return logAndReturn(
        {
          usableWhileRevalidate: selected,
          validatable: validatableEntries,
        },
        { found: "usable-while-revalidate", vary: selected.vary },
      );
    }

    const usableIfErrorEntries =
      classifiedEntries[entryUtils.EntryClassification.UsableIfError];

    if (usableIfErrorEntries) {
      // Non-null assertion is safe because of groupBy mechanics.
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      const selected = Cache.bestEntry(usableIfErrorEntries)!;
      return logAndReturn(
        { usableIfError: selected, validatable: validatableEntries },
        { found: "usable-if-error", vary: selected.vary },
      );
    }

    return logAndReturn(
      { usableIfError: undefined, validatable: validatableEntries },
      { found: "none" },
    );
  }
}

/**
 * {@link Cache.getMany}'s return type: one {@link CacheLookupResult} per
 * request, each narrowed to the spec variants its own request's id selects.
 * Named once because it is spelled both in the signature and in the
 * empty-input early return.
 */
type GetManyResults<
  Reqs extends readonly { id: string }[],
  RT extends ResourceTypes,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = {
  -readonly [K in keyof Reqs]: CacheLookupResult<
    SpecForId<SpecOf<RT>, Extract<Reqs[K]["id"], SpecOf<RT>["id"]>>,
    Validators,
    Params
  >;
};

/**
 * The one-entry registry {@link singleTypeCacheOptions} builds. Keyed by an
 * index signature, because the name is a runtime value the caller asked not to
 * think about. The types carrying the cache's *data* contract stay precise --
 * `SpecOf` of this registry is exactly `CacheSpec<Id, Content>`.
 *
 * What the index signature costs is everything keyed by the type's NAME, since
 * `ResourceTypeName` of this registry is `string`: `classify()` returns `string`
 * rather than the literal, and a {@link producerByIdType} record's keys are no
 * longer checked against the registry (see `singleTypeCacheOptions`' docs).
 * Write the registry out by hand to get both back.
 */
export type SingleTypeRegistry<Id extends string, Content> = {
  readonly [name: string]: ResourceTypeSpec<Id, Content>;
};

/**
 * What {@link singleTypeCacheOptions} accepts. The three RT-dependent keys are
 * omitted from {@link CacheOptions} and re-declared here, so everything else is
 * derived and cannot drift as `CacheOptions` grows.
 *
 * `Id` is inferred from `validateId` and from nothing else. The store
 * deliberately does not mention `Id`: its own spec is captured in
 * `StoreSupportedTypes` and merely *checked* for coverage, exactly as for a
 * hand-written registry (see {@link CacheOptions}' `store`). That buys two
 * things at once -- a general-purpose store can back a sole-type cache, and the
 * store stops competing with the guard as an inference site for `Id`. While it
 * did compete, an untyped `new MemoryStore()` won and silently collapsed `Id`
 * back to `string`, discarding the guard's narrowing.
 *
 * The intersected carrier is what keeps a narrower `Id` honest. `validateId`
 * has to be optional in the base object to serve as an inference site at all;
 * the carrier makes it REQUIRED as soon as `Id` is narrower than `string`. An
 * explicit type argument is fixed before the carrier resolves, so naming a
 * narrow `Id` without a guard is rejected -- and an *inferred* narrow `Id`
 * could only have come from a guard in the first place. So a narrower id space
 * always has a runtime check behind it. (The tuple brackets make this a
 * whole-set comparison rather than a distributive, member-by-member one.)
 *
 * Note that coverage is NOT checked here: it cannot be. The check has to
 * compare `CacheSpec<Id, Content>` against the store's spec, and a conditional
 * spelled over `Id` is resolved while checking the `store` property -- before
 * `validateId` has contributed its inference, since a type predicate's
 * narrowing lands in a later pass. It therefore resolves against `Id`'s default
 * of `string` and rejects every store typed more narrowly than that. (Probed:
 * a store typed for exactly the guard's id space failed its own coverage
 * check.) Coverage is left to `new Cache`, where `Id` is already fixed.
 */
type SingleTypeCacheOptionsInput<
  Id extends string,
  Validators extends AnyValidators,
  Params extends AnyParams,
  StoreSupportedTypes extends CacheSpec,
> = Omit<
  CacheOptions<ResourceTypes, Validators, Params>,
  "store" | "name" | "resourceTypes"
> & {
  /**
   * REQUIRED. Only has to support *at least* this cache's sole resource type;
   * see {@link CacheOptions}' `store`.
   */
  store: Store<StoreSupportedTypes, Validators, InvariantOf<Params>>;
  /**
   * REQUIRED. Names this cache instance (and, by default, its sole resource
   * type) in every diagnostics message.
   */
  name: string;
  /** Defaults to the cache's own `name`. */
  resourceTypeName?: string;
  /**
   * A REAL runtime guard for the sole resource type's id space; the only thing
   * that can narrow `Id` below `string`, and required once it is narrower.
   */
  validateId?: (id: string) => id is Id;
} & ([string] extends [Id]
    ? unknown
    : { validateId: (id: string) => id is Id });

/**
 * The one way to call {@link singleTypeCacheOptions}.
 *
 * The returned `store` is re-declared without {@link CacheOptions}' coverage
 * guard on purpose. Leaving the guard on would have this function's own
 * (asserted) return type claim the guard's phantom property, so `new Cache`
 * would see the requirement already satisfied and wave an under-covering store
 * through. Handing back the plain store type instead leaves the real one for
 * the constructor to check.
 */
export type SingleTypeCacheOptionsBuilder<Content> = <
  Id extends string = string,
  Validators extends AnyValidators = AnyValidators,
  Params extends AnyParams = AnyParams,
  StoreSupportedTypes extends CacheSpec = CacheSpec<Id, Content>,
>(
  options: SingleTypeCacheOptionsInput<
    Id,
    Validators,
    Params,
    StoreSupportedTypes
  >,
) => Omit<
  CacheOptions<
    SingleTypeRegistry<Id, Content>,
    Validators,
    Params,
    StoreSupportedTypes
  >,
  "store"
> & { store: Store<StoreSupportedTypes, Validators, InvariantOf<Params>> };

/**
 * Builds {@link CacheOptions} for a cache with exactly ONE resource type, so the
 * caller does not have to invent a name for that type or nest a one-entry
 * registry literal:
 *
 * ```ts
 * const cache = new Cache(singleTypeCacheOptions<Json>()({ store, name: "xyz-cache" }));
 * ```
 *
 * The resource type is named after the cache unless `resourceTypeName` says
 * otherwise. That name reaches diagnostics only -- it is never part of a store
 * key -- so naming it after the cache cannot invalidate entries, and it keeps
 * `resourceType` meaningful across caches instead of collapsing every
 * sole-type cache into one shared literal. Spread the result to add anything
 * else {@link CacheOptions} accepts.
 *
 * Curried for the same reason {@link resourceType} is: `Content` cannot be
 * inferred from anything, so it must be given explicitly, and TS has no partial
 * type-argument inference.
 *
 * As with a hand-written registry, the store only has to support *at least*
 * this cache's resource type, so a general-purpose store can back a sole-type
 * cache. Coverage is checked by `new Cache`; see {@link CacheOptions}' `store`.
 *
 * ## Narrowing the id space
 *
 * By default the sole type accepts every id, so the id space is `string`. Pass
 * `validateId` to narrow it. It is a REAL runtime guard, not a type assertion,
 * so a nonconforming id throws `UnclassifiableIdError` before the store is
 * touched:
 *
 * ```ts
 * const cache = new Cache(
 *   singleTypeCacheOptions<Schema>()({
 *     store: new MemoryStore(),
 *     name: "tickets",
 *     validateId: idStartsWith("ticket:"),
 *   }),
 * );
 * // cache.get({ id: "ticket:1", ... })  // ok
 * // cache.get({ id: "nope", ... })      // compile error
 * ```
 *
 * The guard is *required* as soon as `Id` is narrower than `string` (see
 * {@link SingleTypeCacheOptionsBuilder}), so a narrower id space always has a
 * runtime check behind it.
 *
 * ## What writing the registry by hand still buys
 *
 * This sugar's registry is keyed by an index signature, because the type's name
 * is a runtime value the caller asked not to think about. Two consequences,
 * both of which the hand-written form avoids:
 *
 * - `classify()` returns `string` rather than the type's literal name.
 * - `ResourceTypeName` of that registry is `string` too, so the compile-time
 *   check that a {@link producerByIdType} record's keys are real registry names
 *   cannot fire. A misspelled key is accepted at compile time and then throws
 *   `NoProducerForResourceTypeError` on every request. On a hand-written
 *   registry the same typo is a compile error.
 *
 * So prefer the hand-written one-entry registry when you use `producerByIdType`
 * or want the literal name; the sugar is for the common case of one resource
 * type and one bare producer function:
 *
 * ```ts
 * new Cache({
 *   store,
 *   name: "tickets",
 *   resourceTypes: {
 *     tickets: resourceType<Schema>()({ matches: idStartsWith("ticket:") }),
 *   },
 * });
 * ```
 */
export function singleTypeCacheOptions<
  Content,
>(): SingleTypeCacheOptionsBuilder<Content> {
  const build = ({
    resourceTypeName,
    validateId,
    ...rest
  }: {
    name: string;
    resourceTypeName?: string;
    validateId?: (id: string) => boolean;
  }) => ({
    ...rest,
    resourceTypes: {
      [resourceTypeName ?? rest.name]:
        validateId === undefined
          ? resourceType<Content>()({
              matches: (id): id is string => typeof id === "string",
            })
          : resourceType<Content>()({
              // SAFETY: re-stating the caller's own guard. It is erased to a
              // plain boolean predicate here because this implementation cannot
              // name the `Id` its call signatures infer; the narrowing restored
              // by the cast below is the one the caller declared.
              matches: validateId as (id: string) => id is string,
            }),
    },
  });

  // SAFETY: the registry's key is a runtime value, so TS types the object
  // literal by its computed-key signature and cannot see that the entry's guard
  // is the caller's `Id` guard. The runtime object is precisely one entry, under
  // the only key the returned options are ever read with, holding the guard the
  // caller supplied (or an accept-everything guard when they supplied none, in
  // which case `Id` really is `string`).
  return build as unknown as SingleTypeCacheOptionsBuilder<Content>;
}
