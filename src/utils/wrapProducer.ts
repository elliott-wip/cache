import stableStringify from "safe-stable-stringify";
import type { ReadonlyDeep } from "type-fest";
import type { PublicInterface } from "type-party";

import type Cache from "../Cache.js";
import type { CacheLookupResult } from "../Cache.js";
import {
  NoProducerForResourceTypeError,
  rethrowUnroutableWithCacheName,
} from "./producer-errors.js";
import {
  cacheProduceChannel,
  publishCacheFetch,
  publishCacheProduce,
  type CacheFetchDisposition,
} from "../diagnostics.js";
import type { SpecForId } from "../types/00_CacheSpec.js";
import {
  type IdOfResourceType,
  type ResourceTypeName,
  type ResourceTypes,
  type SpecOf,
} from "../types/00_ResourceTypes.js";
import type {
  AnyParams,
  AnyValidators,
  ConsumerDirectives,
  ReadonlyConsumerRequest,
  Entry,
  EntryForId,
  Logger,
  RequestPairedProducerResult,
  Vary,
  PartialReadonlyConsumerRequest
} from "../types/index.js";
import {
  collapsedInvocationTaskCreator,
  type CollapsedInvocation,
} from "./collapsedTaskCreator.js";
import {
  normalizeParams,
  normalizeProducerResultResource,
  normalizeVary,
} from "./normalization.js";
import { PendingStores } from "./pendingStores.js";
import {
  completeRequest,
  primaryNormalizedResultResourceFromRequestPairedProducerResult,
  requestPairedProducerResultToResources,
} from "./requestPairedProducerUtils.js";
import {
  assertUnreachable,
  defaultLoggersByComponent,
  raceWithSignal,
} from "./utils.js";

/**
 * Everything the producer wrappers need besides the producer itself, as one
 * bag. The cache is a FIELD rather than a separate positional argument because
 * a wrapper is always "this cache, wrapped this way": the two were never
 * independently meaningful, and splitting them left every call site passing
 * `undefined` in the middle just to reach the producer.
 */
export type WrapProducerOptions<
  RT extends ResourceTypes,
  Validators extends AnyValidators = AnyValidators,
  Params extends AnyParams = AnyParams,
> = {
  /**
   * REQUIRED. An instance of the cache class. This is where values returned by
   * the producer will actually be stored, and it is the inference site for
   * `RT`/`Validators`/`Params` (see {@link Cache.resourceTypes} for why a
   * bare-`RT` member is needed for that at all).
   */
  cache: PublicInterface<Cache<RT, Validators, Params>>;
  /**
   * Controls whether the function returned by `wrapProducer`/`wrapBulkProducer`
   * will fall back to calling the produer if its attempt to read from the cache
   * results in an error, or whether it will throw. Normally, falling back to
   * calling the producer is desirable (so that brief unavailability of the
   * cache doesn't effect the application), and this is the default. However,
   * this must be considered carefully: calling through to the producer on every
   * request can _dramatically_ increase the load it's under -- e.g., if the
   * cache hit rate was even 95% (which is very low for many applications), then
   * calling the producer unconditionally will increase the load its under by
   * 20x! I.e., instead of 1 in 20 requests hitting the producer, all 20 will.
   * If the producer is/uses a shared resource, and it doesn't have good load
   * shedding or autoscaling mechanisms, and the requests to it that are going
   * through this cache aren't its most important work, then sending all the
   * requests to the producer could lead to cascading failures and/or prevent it
   * from serving more important requests. In that case, having the function
   * returned by `wrappedProducer` throw might be more desirable.
   */
  onCacheReadFailure?: "throw" | "call-producer";
  /**
   * If multiple, identical requests (i.e., calls to the function returned by
   * `wrapProducer`/`wrapBulkProducer`) are made that overlap in time (i.e., one
   * has started, but not yet finished, at the time another starts), and
   * multiple of these requests would be forwarded to the wrapped producer
   * [because there's no cached value to satisfy them], these requests can be
   * "deduplicated", so that only one request (which'll still be a bulk request
   * in the case of wrapBulkProducer) is made to the underlying producer, and
   * its response is used for all the overlapping requests. This setting
   * controls the maximum number of seconds that are allowed to have elapsed
   * between the current request and the first of the overlapping requests, if
   * this deduplication is to occur. I.e., if a request occurred greater than
   * `collapseOverlappingRequestsTime` seconds after the earliest, identical,
   * overlapping request, it will not be merged with the prior one, and instead
   * a new request will go to the producer
   */
  collapseOverlappingRequestsTime?: number;
  /**
   * A custom logger to use (optional).
   */
  logger?: Logger;
};

/**
 * A producer over some set of resource types: it sees exactly those types' ids
 * and returns results stamped with them. Used at both scales -- with a single
 * `K` it is a {@link producerByIdType} sub-producer (which sees only its own
 * branch's ids), and with a whole covered set it is the function half of
 * {@link CoveringProducer}, which is what the wrappers take.
 *
 * Note: {@link RequestPairedProducerResult} already allows
 * `supplementalResources` from ANY spec variant, so (e.g.) a `site_day`
 * producer can still attach `business_slice` supplementals.
 */
export type ResourceTypeProducer<
  RT extends ResourceTypes,
  K extends ResourceTypeName<RT>,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = (
  req: ReadonlyConsumerRequest<Params, IdOfResourceType<RT[K]>>,
) => Promise<
  RequestPairedProducerResult<
    SpecOf<RT>,
    Validators,
    Params,
    IdOfResourceType<RT[K]>
  >
>;

/**
 * {@link producerByIdType}'s argument: one entry per covered resource type,
 * any non-empty subset of the registry. `Covered` is inferred from the
 * record's keys. Non-coverage is expressed by omission, so a wrapper never has
 * to make claims about types that other wrappers may produce.
 */
export type ProducersFor<
  RT extends ResourceTypes,
  Covered extends ResourceTypeName<RT>,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = {
  readonly [K in Covered]: ResourceTypeProducer<RT, K, Validators, Params>;
};

/**
 * The key under which a producer function declares WHICH resource types it
 * covers. A real runtime symbol carrying a real runtime value (see
 * {@link CoveringProducer}), not a type-only phantom: the wrappers read it to
 * enforce coverage, and it is exported so declaration emit can name it.
 */
export const coveredTypes: unique symbol = Symbol(
  "@zingage/cache.coveredTypes",
);

/**
 * A producer function that may additionally declare WHICH resource types it
 * covers. The property carries a real runtime value -- the covered type names
 * -- so it serves double duty: `Covered` is inferred from it at compile time,
 * and the wrapper reads it at runtime to enforce coverage before touching the
 * store. A value-less phantom would leave the wrapper with no runtime source for
 * the covered set, since there are no record keys to read.
 *
 * ## Why the property is conditionally required
 *
 * It is optional when `Covered` is the whole registry -- that is what lets a
 * bare function be passed with no ceremony, and no runtime check is needed
 * there because the compiler already made the function prove it accepts every
 * registry id. It is REQUIRED when `Covered` is a strict subset, which makes
 * "narrowed coverage implies runtime proof of that narrowing" an invariant the
 * type system enforces rather than a convention.
 *
 * That requirement is what keeps coverage from having two sources that can
 * disagree. You might expect an explicit `Covered` type argument
 * (`wrapProducer<RT, "story">(...)`) to be enough to narrow coverage; it is
 * rejected precisely because it would narrow the TYPES while leaving the runtime
 * covered set absent, which the wrapper reads as "the whole registry". The types
 * still ban uncovered ids at the call site, so the gap is only reachable by
 * defeating them (a cast or a loosely-typed id -- see
 * {@link NoProducerForResourceTypeError}), but that is exactly the case that
 * error exists to catch: instead of throwing, the wrapper would hand the id to a
 * producer written for a different resource type and then store its content
 * under the incoming id. Routing partial coverage through
 * {@link producerByIdType}, which always supplies the runtime value, makes the
 * declared set and the reachable producers one and the same by construction.
 *
 * `Covered` still infers from the property even though it also appears in the
 * condition (probed, along with the deferred case where `Covered` is an
 * unresolved generic -- how the hashed-input wrappers forward theirs).
 *
 * Narrowing each result's `Id` to `IdOfResourceType<RT[Covered]>` bounds the
 * PRIMARY result to covered types only. Supplementals are unaffected: they are
 * a separate field typed over the full `SpecOf<RT>`, so a covered producer can
 * still attach supplementals for any registry type.
 */
export type CoveringProducer<
  RT extends ResourceTypes,
  Covered extends ResourceTypeName<RT>,
  Validators extends AnyValidators = AnyValidators,
  Params extends AnyParams = AnyParams,
> = ResourceTypeProducer<RT, Covered, Validators, Params> &
  CoveredTypesCarrier<RT, Covered>;


/**
 * The conditionally-required {@link coveredTypes} carrier: optional when
 * `Covered` is the whole registry, REQUIRED when it is a strict subset. Shared
 * by {@link CoveringProducer} and its bulk counterpart so the two cannot drift
 * -- relaxing one to always-optional would reopen the hole described on
 * `CoveringProducer` for that wrapper alone.
 *
 * The tuple brackets make this a whole-set comparison rather than a
 * distributive, member-by-member one.
 */
export type CoveredTypesCarrier<
  RT extends ResourceTypes,
  Covered extends ResourceTypeName<RT>,
> = [ResourceTypeName<RT>] extends [Covered]
  ? { readonly [coveredTypes]?: readonly Covered[] }
  : { readonly [coveredTypes]: readonly Covered[] };

/**
 * A request as the wrappers' id-erased internals see it: every id the wrapper
 * could be handed, with `id` still usable as that type (see
 * {@link ReadonlyConsumerRequest}).
 */
export type LooseRequestFor<
  RT extends ResourceTypes,
  Params extends AnyParams,
> = ReadonlyConsumerRequest<Params, SpecOf<RT>["id"]>;

/** An entry as the wrappers' id-erased internals see it. */
export type LooseEntryFor<
  RT extends ResourceTypes,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = Entry<SpecForId<SpecOf<RT>, SpecOf<RT>["id"]>, Validators, Params>;

/** A cache lookup result as the wrappers' id-erased internals see it. */
export type LooseLookupResultFor<
  RT extends ResourceTypes,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = CacheLookupResult<
  SpecForId<SpecOf<RT>, SpecOf<RT>["id"]>,
  Validators,
  Params
>;

/** A request-paired producer result over the whole registry. */
export type LooseResultFor<
  RT extends ResourceTypes,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = RequestPairedProducerResult<SpecOf<RT>, Validators, Params>;

/**
 * The internal, id-erased shape all wrapper plumbing dispatches through.
 * SAFETY: the producer is only invoked after (a) `cache.classify(req.id)`
 * succeeds and (b) the coverage check confirms the classified type is one the
 * producer declared (or the producer declared none, i.e. it covers the whole
 * registry) -- so the request's id is in exactly the id sub-space the producer
 * accepts.
 */
export type LooseProducer<
  RT extends ResourceTypes,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = (
  req: LooseRequestFor<RT, Params>,
) => Promise<LooseResultFor<RT, Validators, Params>>;

/**
 * Snapshots a producer's declared covered set for the coverage check: a `Set`
 * for membership testing, insertion-ordered so the error message can still
 * list the names in their declared order, and a copy so a post-wrap mutation
 * of the caller's array can't widen (or otherwise change) coverage later.
 * `undefined` means the producer declared nothing, which (see
 * {@link CoveringProducer}) means it covers the whole registry: there is no
 * covered set to enumerate and nothing to check.
 */
export function coveredTypeSet(producer: {
  readonly [coveredTypes]?: readonly string[];
}): ReadonlySet<string> | undefined {
  const declared = producer[coveredTypes];
  return declared === undefined ? undefined : new Set(declared);
}

/**
 * The write side of the coverage contract: what `producerByIdType` and
 * `bulkProducerByIdType` throw when handed an empty record. Shared so the two
 * helpers can't word the same refusal differently, and kept next to the read
 * side ({@link coveredTypeSet}) because both are statements about where a
 * declared covered set comes from.
 */
export function emptyProducersRecordMessage(
  helper: "producerByIdType" | "bulkProducerByIdType",
  wrapper: "wrapProducer" | "wrapBulkProducer",
): string {
  return (
    `${helper}: \`producers\` must be a record with one entry per covered ` +
    `resource type and cannot be empty. (A producer that covers the whole ` +
    `registry needs no helper: pass the function itself to ${wrapper}.)`
  );
}

/**
 * Throws {@link NoProducerForResourceTypeError} if `resourceType` is outside a
 * DECLARED covered set. `covered === undefined` (a whole-registry producer)
 * checks nothing.
 */
export function assertResourceTypeCovered(
  cacheName: string,
  covered: ReadonlySet<string> | undefined,
  resourceType: string,
  id: string,
): void {
  if (covered !== undefined && !covered.has(resourceType)) {
    throw new NoProducerForResourceTypeError({
      cacheName,
      resourceType,
      coveredResourceTypes: [...covered],
      id,
    });
  }
}

/**
 * What {@link wrapProducer} returns. Named because it is spelled both in the
 * signature and in the closing cast, far enough apart to drift, and the cast is
 * the only thing that would notice them disagreeing.
 */
type WrappedProducerFn<
  RT extends ResourceTypes,
  Covered extends ResourceTypeName<RT>,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = <Id extends IdOfResourceType<RT[Covered]>>(
  req: PartialReadonlyConsumerRequest<Params, Id>,
  options?: { signal?: AbortSignal },
) => Promise<EntryForId<SpecOf<RT>, Validators, Params, Id>>;

/**
 * Logged by both wrappers when a producer failure is absorbed by the request's
 * own if-error entry. Shared so a rewording can't land in one wrapper only,
 * leaving operators grepping for two different strings for one event.
 */
export const PRODUCER_ERROR_FALLBACK_WARNING =
  "error calling producer; falling back to a cached value, as permitted";

/** The always-unreachable tail of a wrapper's abort path. */
export function throwUnreachableAbort(signal: AbortSignal | undefined): never {
  signal?.throwIfAborted();
  // Unreachable: only called after observing `signal.aborted`.
  throw new Error("unreachable: throwAborted called without an aborted signal");
}

/**
 * Fundamentally, this function takes a producer that returns values (likely
 * without the help of a cache), and returns a function that's a drop-in
 * replacement for it, except that it tries to lookup and reuse prior
 * results from a cache, before calling the underlying user-provided producer.
 *
 * Exactly ONE producer function is passed, and it is the primitive:
 *
 * - A **bare function covers the whole registry.** `Covered` defaults to
 *   every registry type name, and the compiler makes the function prove it --
 *   its parameter must accept every registry id. Sole-type caches therefore
 *   just pass their producer.
 * - **Partial coverage requires {@link producerByIdType}**, which turns a
 *   per-resource-type record into a single function carrying its covered set
 *   in the optional {@link coveredTypes} property. `Covered` is inferred from
 *   that property (inference beats the default whenever it is present), which
 *   bounds the returned function's request type: it accepts exactly the
 *   covered types' ids, and requests for uncovered types are compile errors.
 *
 * A type with no producer in any wrapper is legal and normal: its entries are
 * written as other producers' supplemental resources (or direct `store()`
 * calls) and read via `Cache.get` -- the serve-if-present contract. Partial
 * coverage is also what makes capability-scoped and split wrappers honest: a
 * second `wrapProducer` call can cover a different subset of the same cache,
 * and adding a registry type grants no existing wrapper-holder fetch authority
 * over it.
 *
 * The wrapper calls `cache.classify(req.id)` once per request; the classify
 * result is what stamps `resourceType` on the `fetch`/`produce` diagnostics
 * messages. If the producer declared a covered set and the classified type is
 * not in it -- reachable only via a cast or loosely-typed id -- the wrapper
 * throws {@link NoProducerForResourceTypeError} before reading the cache. A
 * bare function declares no covered set, so that check is skipped entirely
 * (its coverage is the whole registry by construction).
 *
 * ## Producer purity contract
 *
 * A producer passed to any wrapper must be a side-effect-free read of its
 * resource type's origin: every invocation may be collapsed (shared with
 * other concurrent logical callers) and its result stored, so producer calls
 * are never 1:1 with callers. Requests "made for their side effects" should
 * not route through a wrapper at all; a consumer that must reach the origin
 * sends bypass directives (`maxAge: 0`); a producer whose response must not
 * be stored returns `storeFor: 0` directives.
 *
 * ## Bypass requests skip the cache read
 *
 * When the consumer's directives could never be satisfied by cached data
 * (`maxAge: 0`), the wrapper does not call `Cache.get` at all: `maxAge: 0`
 * structurally guarantees producer contact (closing the age-≤0/clock-skew
 * hole where a same-millisecond or future-dated entry has age ≤ 0 and would
 * satisfy `maxAge: 0` from cache), and bypass requests don't pollute the
 * `read` channel. Bypass requests still collapse (only with
 * identical-directive peers -- the collapse key includes directives) and
 * their results are still stored.
 *
 * Note that any supplemental resources returned by a producer will be
 * cached but not returned to the caller.
 *
 * The wrapped function is generic over the specific id of an incoming request
 * so that the result's content type is narrowed to the covered spec variants
 * compatible with that id.
 *
 * ## AbortSignal support
 *
 * The returned function accepts an optional `{ signal }` parameter. The
 * signal is forwarded to `cache.get()`, so the store read can be aborted. If
 * the signal fires before the cache read completes, the function throws
 * without ever contacting the producer. Once the cache read resolves and the
 * producer must be called, no signal reaches the producer -- because that call
 * may be sharing one underlying producer invocation with other callers who have
 * not aborted. Producers accordingly take **no** `options` parameter at all
 * (see {@link CoveringProducer}): every producer call goes through the
 * collapsed-invocation task, so there is never a single caller's signal that
 * could be forwarded without letting one caller cancel another's work. The
 * caller's wait for the producer result is instead **raced** against the signal,
 * so the caller can bail out immediately without waiting for the producer to
 * finish.
 *
 * Critically, bailing out does NOT prevent the producer's result from being
 * stored: the collapsed invocation always fires a (non-awaited)
 * `cache.store()` after the producer resolves (see the next section), so the
 * work is never wasted. The trade-off is that the producer itself cannot
 * observe the signal to cancel its own in-progress work (e.g., an outgoing
 * HTTP request). Supporting that would require either (a) aborting the shared
 * task only when *all* callers have aborted, which adds significant
 * complexity, or (b) giving up request collapsing for callers that pass a
 * signal, which would defeat its purpose.
 *
 * ## The store is fire-and-forget; in-flight writes still count as hits
 *
 * The returned function resolves as soon as the producer has a value; the
 * `cache.store()` of that value (primary and supplemental resources alike) is
 * fired but NOT awaited, so a caller never waits on a store round trip and a
 * failed write never fails the call (it is logged and dropped). The precise
 * guarantee about repeated calls is then:
 *
 * - Identical calls that overlap the producer invocation share it (request
 *   collapsing, above).
 * - A call that arrives after the producer resolved but before its
 *   `cache.store()` settled is answered from the wrapper's own in-memory
 *   overlay of in-flight writes -- consulted before the cache read, and
 *   honoring the request's params against each entry's `vary` and the
 *   caller's directives exactly as a stored entry would be -- provided the
 *   entry would be a fresh hit had the write landed. So a plain sequential
 *   repeat (`await f(x); await f(x)`) runs the producer once, whatever the
 *   store's write latency. Such a serve reports `served-from-cache` on the
 *   fetch channel and, like a bypass request, emits no `read` message (no
 *   store read happened).
 * - Once the store call settles the overlay forgets the entry, on success
 *   (the store answers for it now) and on failure alike: a failed write is
 *   never pinned, so the next call reads the store, misses, and runs the
 *   producer again.
 * - The overlay is per wrapper instance and in-process. It does not cover a
 *   different wrapper over the same cache, a different process, or the
 *   stale-while-revalidate / stale-if-error paths (a pending entry that is
 *   already stale falls through to the store read, as before).
 *
 * The point at which a value is DURABLE is therefore not the wrapped call's
 * resolution but the settlement of its store call, which the wrapper does not
 * expose per call. `await cache.close()` is the flush: every store call the
 * wrapper has fired has already been handed to the backing store by the time
 * the wrapped call resolves, and the bundled stores' `close()` drains their
 * pending writes before resolving. A short-lived process (a script, a
 * one-shot job) that skips `close()` can lose its last writes; that is a
 * property of the stores, not of the wrappers, but it is the one place a
 * wrapper user has to think about it.
 *
 * @param options - The cache to wrap, plus the wrapping behaviour; see
 *   {@link WrapProducerOptions}. `options.cache` is where values returned by
 *   the producer (see below) will actually be stored.
 *
 * @param producer - The function actually responsible for returning the
 *   results that will be sent to the user and/or stored in the cache. It acts
 *   as the origin or "producer" for every resource type it covers. It is
 *   passed the request (id and params) along with the caller's cache
 *   directives, which may be needed in case the producer is itself backed by a
 *   cache, and it needs to decide whether to contact its origin. Pass a bare
 *   function to cover the whole registry, or {@link producerByIdType}'s result
 *   to cover a subset with one sub-producer per resource type.
 */
export default function wrapProducer<
  RT extends ResourceTypes,
  Covered extends ResourceTypeName<RT> = ResourceTypeName<RT>,
  Validators extends AnyValidators = AnyValidators,
  Params extends AnyParams = AnyParams,
>(
  options: WrapProducerOptions<RT, Validators, Params>,
  producer: CoveringProducer<RT, Covered, Validators, Params>,
): WrappedProducerFn<RT, Covered, Validators, Params> {
  const {
    cache,
    collapseOverlappingRequestsTime = 3,
    onCacheReadFailure = "call-producer",
    logger = defaultLoggersByComponent["wrap-producer"],
  } = options;

  // SAFETY: see LooseProducer.
  const looseProducer = producer as unknown as LooseProducer<
    RT,
    Validators,
    Params
  >;

  const covered = coveredTypeSet(producer);

  const logTrace = logger.bind(null, "wrap-producer", "trace");
  const logWarning = logger.bind(null, "wrap-producer", "warn");

  type LooseRequest = LooseRequestFor<RT, Params>;
  type LooseResult = LooseResultFor<RT, Validators, Params>;
  type LooseEntry = LooseEntryFor<RT, Validators, Params>;
  type LooseLookupResult = LooseLookupResultFor<RT, Validators, Params>;

  const normalizeVaryBound = (vary: Vary<Params>) =>
    normalizeVary(cache.normalizeParamName, cache.normalizeParamValue, vary);
  const normalizeParamsBound = (params: LooseRequest["params"]) =>
    normalizeParams(
      cache.normalizeParamName,
      cache.normalizeParamValue,
      params,
    );

  // Entries whose cache.store() is still in flight, consulted before every
  // cache read; see the function docs and PendingStores.
  const pendingStores = new PendingStores<SpecOf<RT>, Validators, Params>();

  const callProducerAndLog = async (req: LooseRequest) => {
    logTrace("contacting producer", req);
    let resp: LooseResult;
    try {
      resp = await looseProducer(req);
    } catch (error: unknown) {
      // A by-id-type producer routes ids itself and may throw a special
      // UnroutableIdError if the id can't be classified to a covered type.
      // However, the producer has no cache to name in its errors, so this is
      // where the cache's name is attached.
      rethrowUnroutableWithCacheName(cache.name, error);
    }
    logTrace("got response from producer", resp);
    return resp;
  };

  // Suppose the caller is requesting a resource, and we're already in the
  // process of requesting that resource from the producer (or storing the
  // response), and our pending request had the same id and the same params.
  // This can happen in practice, e.g.: a user signs in; that triggers 10 items
  // to load to show on home screen; but each of those 10 depend on some
  // cache-managed resource, so 10 requests hit the cache for that resource
  // essentially at once; one will start first, and the other 9 will be in the
  // situation of asking the cache to request data for which the same request is
  // already pending.
  //
  // In a situation like the above, what are we to do? Technically, it is
  // _posssible_ for the resource at the origin to change after the 10th request
  // came in to the cache, but before the origin's response to the cache's first
  // outbound request arrived. Therefore, there's an argument that the cache
  // technically should issue a new request to the origin for each of the 10
  // requests described in our hypothetical above.
  //
  // In practice, though, such behavior would risk bombarding the origin and
  // surprising users (who, by virtue of using a cache, might only be expecting
  // one request to the origin) for very little gain, as it's highly unlikely
  // that the resource will change at the origin just in the window of time that
  // the origin's response to an identical request is in transit.
  //
  // In other words, its not usually our job to add layers of caching that the
  // caller didn't ask for [which is what not hitting the origin 10 times would
  // be], but this case is special because the fact that the response hasn't
  // finished saving to the store yet means that, even if the user _does_ want
  // caching here (and they probably do), there's no directive they can use to
  // request it.
  //
  // So, here, we decide to keep track of the in-flight requests to the origin
  // (and their pending saves to the store), and, if there is an identical
  // request pending that was issued less than `collapseOverlappingRequestsTime`
  // seconds ago, we wait for and use the response of the already-pending
  // request, rather than issuing a new one. We make
  // `collapseOverlappingRequestsTime` configurable to placate any user worried
  // about the miniscule risk of inconsistency from this caching.
  //
  // We do this using the collapsedInvocationTaskCreator utility (the
  // metadata-carrying sibling of the public collapsedTaskCreator). That
  // utility does track the pending tasks in memory, so this optimzation will
  // be hindered a bit if the cache frontend is horizontally-scaled across
  // more than one server, but that's fine. We _could_ put this data in the
  // backing store, but that seems like it could create more race conditions?
  // And since batching identical requests at all is an optimization, putting
  // this in the store would probably be overkill.
  //
  // Finally, note is that we only collapse requests that target the same id
  // _with the same parameters_. If we didn't require the params to match, we
  // could get back a response to the first/pending request, only to find out
  // that it includes a `varyKeys` value that makes it unsuitable to serve the
  // second request (that we were trying to avoid making). So we'd have to add
  // fallback logic to handle actually issuing the second request in that case,
  // and that would be too much extra complexity to be worth it. We're in a
  // similar situation with directives, which must also match -- which is also
  // what keeps bypass (`maxAge: 0`) invocations from ever being shared with
  // plain-miss callers.
  //
  // No signal reaches the producer (`CoveringProducer` takes no options
  // parameter at all), because the
  // task may be shared with other callers who haven't aborted. It always
  // fires a (non-awaited) cache.store() after the producer resolves. This is
  // critical for the abort-signal design: since collapsed producer calls
  // don't receive a signal and can't be cancelled by individual callers, we
  // rely on the fact that the producer's work always flows into the cache --
  // ensuring it isn't wasted even when the caller that triggered it has
  // aborted.
  //
  // Because the store is not awaited, the collapse map alone leaves a window
  // -- from the producer resolving until the write lands -- in which an
  // identical call would read the store, miss, and run the producer again.
  // The entries handed to cache.store() are therefore also held in
  // `pendingStores` until that store call settles (see PendingStores), and
  // the wrapped function consults that overlay before reading the store.
  //
  // It also publishes the invocation's `produce` diagnostics message when the
  // producer settles (one message per actual invocation; collapsed callers
  // share one).
  const collapsedCallProducerAndStore = collapsedInvocationTaskCreator(
    async (
      invocation: CollapsedInvocation,
      req: LooseRequest,
      resourceType: string,
    ) => {
      const start = performance.now();
      let requestPairedResult: LooseResult;

      const publishProduce = (outcome: "success" | "error") => {
        // `publish` would discard the message, but not before the argument was
        // built -- so the test has to be here, not left to the channel. Same
        // reasoning as the bulk wrapper and `Cache.store`.
        if (!cacheProduceChannel.hasSubscribers) {
          return;
        }
        publishCacheProduce({
          cache: cache.name,
          trigger: invocation.trigger,
          requests: [{ resourceType, resourceId: req.id }],
          collapsedCallerCount: invocation.attachedCallerCount(),
          outcome,
          durationMs: performance.now() - start,
        });
      };

      try {
        requestPairedResult = await callProducerAndLog(req);
      } catch (error) {
        publishProduce("error");
        throw error;
      }
      publishProduce("success");

      logTrace(`attempting to store response.`);
      const resources = requestPairedProducerResultToResources(
        requestPairedResult,
        req.id,
      );
      const stored = cache.store(resources);
      stored
        .then(() => {
          logTrace(`successfully stored producer's response`);
        })
        .catch((e) => {
          logTrace(`error storing producer's response`, e);
        });

      // Hold the just-stored entries in the overlay until the store settles.
      // Normalized here the way `Cache.store` normalizes them (a store-time
      // `now` as the fallback produced-at date). Normalization can throw on a
      // malformed directive (`freshUntilAge: NaN`); `Cache.store` rejects on
      // the same input and that rejection is already logged and dropped
      // above, so a throw here must not turn a successful produce into a
      // failed call -- the entries are simply not overlaid.
      try {
        const now = new Date();
        pendingStores.track(
          resources.map((it) =>
            normalizeProducerResultResource(normalizeVaryBound, it, now),
          ),
          stored,
          now,
        );
      } catch (e) {
        logTrace(
          `not holding producer's response as pending: normalization failed`,
          e,
        );
      }

      return requestPairedResult;
    },
    collapseOverlappingRequestsTime * 1000,
    // The key covers the full request (id, params, directives) but not the
    // resourceType arg, which is derived from the id and so adds nothing.
    ([req]) => stableStringify(req),
  );

  const wrappedProducer = async function (
    req: PartialReadonlyConsumerRequest<Params, SpecOf<RT>["id"]>,
    callOptions?: { signal?: AbortSignal },
  ): Promise<LooseEntry> {
    const signal = callOptions?.signal;

    const finalRequest = completeRequest(req) as LooseRequest;
    const { id, params, directives } = finalRequest;

    // Classify once per request: the result dispatches to the producer AND
    // stamps `resourceType` on the fetch/produce messages, so dispatch and
    // telemetry cannot disagree. Classification errors propagate (they're
    // contract violations, not dispositions -- no fetch message).
    const resourceType = cache.classify(id);

    // A classified type outside this wrapper's DECLARED coverage throws BEFORE
    // any cache read (see NoProducerForResourceTypeError's docs). Skipped
    // entirely for a bare producer, which declares no covered set because it
    // covers the whole registry.
    assertResourceTypeCovered(cache.name, covered, resourceType, id);

    const publishFetch = (
      collapsed: boolean,
      disposition: CacheFetchDisposition,
    ) => {
      publishCacheFetch({
        cache: cache.name,
        resourceType,
        resourceId: id,
        collapsed,
        ...disposition,
      });
    };

    const directivesImpliedBypass = isRequestingCacheBypass(directives);

    // Every abort-caused rejection settles the logical request as `aborted`.
    const throwAborted = (collapsed: boolean): never => {
      publishFetch(collapsed, {
        disposition: "aborted",
        directivesImpliedBypass,
      });
      throwUnreachableAbort(signal);
    };

    if (signal?.aborted) {
      throwAborted(false);
    }

    // Awaits an in-flight producer invocation on behalf of this caller:
    // races the caller's wait against its signal, settles the fetch message,
    // and applies the caller's own usableIfError fallback on producer error.
    const settleOnProducer = async (
      attached: { promise: Promise<LooseResult>; rode: boolean },
      usableIfError: LooseEntry | undefined,
    ): Promise<LooseEntry> => {
      let result: LooseEntry;
      try {
        result = await raceWithSignal(
          attached.promise.then((it) =>
            primaryNormalizedResultResourceFromRequestPairedProducerResult<
              SpecOf<RT>,
              Validators,
              Params,
              SpecOf<RT>["id"]
            >(normalizeVaryBound, it, id),
          ),
          signal,
        );
      } catch (error) {
        // If the error was the signal being aborted, propagate that; don't
        // treat it as a producer failure (or fall back to a cached
        // usable-if-error value).
        if (signal?.aborted) {
          throwAborted(attached.rode);
        }

        if (usableIfError) {
          logWarning(PRODUCER_ERROR_FALLBACK_WARNING, {
            error,
            entry: usableIfError,
          });
          publishFetch(attached.rode, {
            disposition: "served-stale-after-error",
            vary: usableIfError.vary,
          });
          return usableIfError;
        }

        publishFetch(attached.rode, {
          disposition: "producer-error",
          directivesImpliedBypass,
        });
        throw error;
      }

      publishFetch(attached.rode, {
        disposition: "served-from-producer",
        directivesImpliedBypass,
      });
      return result;
    };

    // Bypass requests skip the cache read entirely (see the function docs):
    // no `read` message, guaranteed producer contact, result still stored,
    // still collapsed with identical-directive peers.
    if (directivesImpliedBypass) {
      logTrace(
        `request has cache-bypassing directives; skipping the cache read`,
        { id, params },
      );
      return settleOnProducer(
        collapsedCallProducerAndStore("bypass", finalRequest, resourceType),
        undefined,
      );
    }

    // An entry this wrapper produced whose store() has not settled yet is a
    // hit (see the function docs): it is what the store read would return had
    // the write already landed, so it settles the fetch the same way.
    const pendingEntry = pendingStores.findUsable(
      id,
      normalizeParamsBound(params),
      directives,
      new Date(),
    );
    if (pendingEntry !== undefined) {
      logTrace(`serving an entry whose store is still in flight`, {
        id,
        params,
      });
      publishFetch(false, {
        disposition: "served-from-cache",
        vary: pendingEntry.vary,
      });
      return pendingEntry;
    }

    logTrace(`asking the cache for a response`, { id, params });

    const cacheRes = await cache
      .get<SpecOf<RT>["id"]>(finalRequest, callOptions)
      .catch((e: unknown) => {
        // If the errror was the signal being aborted, propagate that (as an
        // `aborted` settlement); don't assume a read failure.
        if (signal?.aborted) {
          throwAborted(false);
        }

        switch (onCacheReadFailure) {
          case "throw":
            // The request never reached a disposition: no fetch message.
            throw e;
          case "call-producer":
            // Pretend the cache returned no results so that we'll fall through to
            // the producer
            return {
              validatable: [],
            } satisfies LooseLookupResult as LooseLookupResult;
          default:
            assertUnreachable(onCacheReadFailure);
        }
      });

    const { usable, usableIfError, usableWhileRevalidate } = cacheRes;

    // We have ready-to-go content from the cache, w/ no refresh required.
    if (usable) {
      publishFetch(false, {
        disposition: "served-from-cache",
        vary: usable.vary,
      });
      return usable;
    }

    if (signal?.aborted) {
      throwAborted(false);
    }

    // If we're here, we either don't have usable content at all, or we have
    // content that's only usable in the event of an origin error, or if we make
    // a background request to revalidate it. In any case, we're gonna need to
    // contact the origin for the result.
    //
    // TODO: Support validation requests and invalidation. How to do this is
    // actually tricker than it seems. There are questions like does the
    // validation response create a new Entry [potentially leaving the old entry
    // still there to match on other requests], or should it update an existing
    // one? [Does this mean entries need some notion of an id? Who generates
    // that?] What if the existing entry has since been deleted or aged out of
    // the store? Can the response update more than one entry? Can it update
    // things about it beyond reseting age to zero (e.g., changing the producer
    // directives)? Etc. For some HTTP context, see
    // https://tools.ietf.org/html/rfc7234#section-4.3.4 For invalidation, the
    // idea would be to somehow let request A passing through the producer
    // trigger the invalidation of other cached results [a la a POST
    // invalidating a GET in HTTP], but how? Call a user-provided invalidate
    // function and pass it the just-made request, the promise for its response,
    // and the entry store, and can delete entries made invalid by the request
    // that just passed through?
    //
    // The collapsed call doesn't receive the signal directly (since the
    // underlying task may be shared with other callers who haven't aborted).
    // However, we race the caller's observation of the result against the
    // signal so they can bail out immediately. The underlying producer task
    // keeps running and its result is stored via the collapsed invocation.
    //
    // The trigger labels the invocation's INITIATING cause; if this call
    // instead rides an invocation already in flight, that invocation's
    // original trigger stands.
    const attached = collapsedCallProducerAndStore(
      usableWhileRevalidate ? "revalidation" : "miss",
      finalRequest,
      resourceType,
    );

    if (usableWhileRevalidate) {
      // swallow error rather than crash.
      attached.promise.catch(() => {
        logWarning(
          "error asynchronously requesting refreshed content from producer",
          { id, params, directives },
        );
      });
      publishFetch(false, {
        disposition: "served-stale-while-revalidating",
        vary: usableWhileRevalidate.vary,
      });
      return usableWhileRevalidate;
    }

    return settleOnProducer(attached, usableIfError);
  };

  // SAFETY: the runtime function is id-erased internally (see LooseProducer),
  // but every path upholds the per-id contract the signature promises: the
  // cache read narrows by id, and the producer's request-paired result is
  // stamped with the request's own id.
  return wrappedProducer as unknown as WrappedProducerFn<
    RT,
    Covered,
    Validators,
    Params
  >;
}

export function isRequestingCacheBypass(
  dirs: ReadonlyDeep<ConsumerDirectives>,
) {
  return dirs.maxAge === 0;
}
