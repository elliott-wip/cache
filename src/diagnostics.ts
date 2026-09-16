import type { Channel } from "node:diagnostics_channel";
import * as diagnosticsChannel from "node:diagnostics_channel";
import type { AnyParams } from "./types/01_Params.js";
import type { AnyValidators } from "./types/02_Validators.js";
import type { Vary } from "./types/04_ProducerResult.js";
import type { StoreEntryRelationship } from "./types/06_Store.js";

/**
 * @fileoverview The package's diagnostics channels. There are four:
 *
 * - `@zingage/cache:read` -- what did a cache lookup find? Published by
 *   `Cache.get`/`Cache.getMany` (including direct callers that never touch a
 *   wrapper).
 * - `@zingage/cache:fetch` -- how was a logical wrapped-producer request
 *   answered? Published by the producer wrappers, once per call of the
 *   wrapped function (per request element, for bulk), at settlement.
 * - `@zingage/cache:produce` -- one message per actual producer invocation
 *   (foreground misses AND background revalidations). Producer latency and
 *   error rate live here.
 * - `@zingage/cache:store-entry` -- one message per entry passed to
 *   `Cache.store()`.
 *
 * Every message carries the same two attribution fields ({@link Attribution};
 * `produce` carries them per element of `requests[]` instead): the cache
 * instance's `name` and the resource-type name from the cache's registry.
 *
 * ## `vary`: the variant key shared by `read`, `fetch`, and `store-entry`
 *
 * A cache's unit of storage is the `(id, vary)` PAIR, where `vary` is the
 * normalized paramName->value map that defines a VARIANT: a producer that
 * varies on request params writes one entry per variant under the same id. So
 * `resourceId` alone is not enough to line a serve up with the store that
 * produced the value it served -- on a varying producer every variant's
 * serves would be credited to whichever variant stored first. The three
 * channels that can identify a variant therefore all report the same
 * normalized `vary` object: `store-entry` for the entry written, and
 * `read`/`fetch` for the entry a lookup SELECTED. Where no entry was selected
 * the key is OMITTED, never published as `{}` -- `{}` identifies a real
 * variant (the only one a non-varying producer ever writes), so conflating it
 * with "unknown" would silently merge those two populations. `produce` has no
 * `vary`: an invocation is a request to the origin, and which variant(s) its
 * result occupies isn't known until it returns.
 *
 * ## Why `fetch` and `produce` are two channels, not one
 *
 * They are the two spans of the same story with different subjects and
 * cardinalities: a `fetch` is the consumer-side span (one per logical
 * request); a `produce` is the origin-side span (one per producer
 * invocation). They relate many-to-one in two ways -- request collapsing (N
 * callers ride 1 invocation; inlining invocation stats into fetch messages
 * would republish them once per rider, weighting producer error/latency
 * metrics by rider count) and bulk batching (one invocation covers
 * `requests[]`) -- and are temporally decoupled in two ways: a
 * stale-while-revalidate revalidation settles *after* its triggering fetch
 * event already shipped, and an `aborted` fetch settles *before* its
 * invocation does (the collapsed producer call keeps running and stores in
 * the background). The durations also mean different things:
 * `produce.durationMs` is invocation time, while any fetch-level duration
 * would be caller wait (riders wait less than the invocation took).
 */

/**
 * The attribution fields shared by every diagnostics message.
 */
type Attribution = {
  /** Cache instance name (constructor `options.name`). */
  cache: string;
  /** Resource-type name from the cache's registry (`cache.classify(id)`). */
  resourceType: string;
};

/**
 * A `diagnostics_channel` {@link Channel} whose publish/subscribe sides are
 * typed to a specific message shape.
 */
export type TypedChannel<T, Name extends string> = Omit<
  Channel,
  "publish" | "subscribe" | "unsubscribe"
> & {
  publish(message: T): void;
  subscribe(callback: (message: T, name: Name) => void): void;
  unsubscribe(callback: (message: T, name: Name) => void): boolean;
};

/**
 * Opens a channel under a typed view of its messages. Curried so the message
 * type is explicit while `Name` is inferred from the argument -- the same shape
 * as `resourceType<Content>()({ … })` elsewhere in the package.
 *
 * The one place the package asserts a message type onto `diagnostics_channel`'s
 * untyped {@link Channel}: `channel()` is the only way to obtain one, so the
 * assertion is unavoidable, and having it here once means adding a channel
 * cannot add another.
 */
const typedChannel =
  <T>() =>
  <Name extends string>(name: Name): TypedChannel<T, Name> =>
    diagnosticsChannel.channel(name) as TypedChannel<T, Name>;

/**
 * Name of the diagnostics channel that fires once per cache lookup --
 * i.e., once per `Cache.get()` call and once per request in a
 * `Cache.getMany()` call -- reporting what the lookup found.
 *
 * Wrapper requests with bypass directives (`maxAge: 0`) never appear here:
 * they skip the cache read entirely.
 *
 * A read that itself **fails** (the store threw) publishes `found:
 * "read-failed"` carrying the `error`, one message per request in the failed
 * call, and *then* the error propagates (`Cache.get`/`getMany` still reject).
 * This keeps the channel's one-message-per-lookup invariant total, so a
 * subscriber can use it as a complete denominator. It matters most under the
 * wrappers' default `onCacheReadFailure: "call-producer"`, which absorbs the
 * store error and substitutes an empty lookup result: without this message a
 * store failing every read is indistinguishable on the channels from a pure
 * cache-miss workload -- same producer traffic, same `fetch` dispositions, no
 * signal naming the cause.
 */
export const CACHE_READ_CHANNEL_NAME = "@zingage/cache:read";

/**
 * The lookup results that SELECTED a stored entry:
 * - "usable":                  satisfiable from cache alone
 * - "usable-while-revalidate": only usable if paired with a background refresh
 * - "usable-if-error":         only usable as a producer-failure fallback
 *
 * Split out of {@link CacheReadFound} because these are exactly the results
 * that can identify a variant: a selected entry has a `vary`, and "none" has
 * no entry to take one from.
 */
export type CacheReadFoundWithEntry =
  | "usable"
  | "usable-while-revalidate"
  | "usable-if-error";

/**
 * What a *completed* lookup found, evaluated against the request's directives:
 * either one of the entry-selecting results ({@link CacheReadFoundWithEntry})
 * or "none" -- nothing this request could use.
 *
 * (Reserved for future conditional revalidation: entries that are merely
 * `validatable` report "none" today.)
 *
 * Deliberately excludes `"read-failed"`, which is not a lookup *result* -- see
 * {@link CacheReadMessage}. Keeping the two apart lets the mapping from a
 * lookup result to a `found` value stay total.
 */
export type CacheReadFound = CacheReadFoundWithEntry | "none";

/**
 * What a *completed* lookup contributes to its read message: the `found`
 * value paired with the variant it identifies, if any.
 *
 * Exported because `Cache`'s lookup evaluation returns one and spreads it into
 * the message -- the same reason {@link CacheFetchDisposition} is exported for
 * the wrappers. Pairing the two fields in one value is what keeps "which
 * `found` values carry a `vary`" stated once instead of re-derived at each of
 * the four publish sites.
 */
export type CacheReadOutcome =
  | {
      found: CacheReadFoundWithEntry;
      /**
       * The SELECTED entry's (normalized) vary -- identifying the
       * `(resourceId, vary)` variant this lookup matched, which is the same
       * key `store-entry` reports for the write that put the value there (see
       * {@link CacheStoreEntryMessage.vary}). Reads and stores can therefore
       * be joined per variant rather than per id, which is the only way to
       * get correct per-variant counts out of a varying producer.
       *
       * When more than one entry qualified, this is the one the lookup
       * actually chose (the freshest), matching the entry handed back in the
       * lookup result.
       */
      vary: Vary<AnyParams>;
    }
  | {
      found: "none";
      /**
       * No entry was selected, so there is no variant to identify. Absent
       * rather than `{}`, which identifies a real variant (see this file's
       * overview).
       */
      vary?: undefined;
    };

/**
 * The message type published to the read diagnostics channel.
 *
 * A discriminated union rather than a flat object with an optional `error`:
 * `error` is present exactly when the read failed, so a subscriber that
 * narrows on `found === "read-failed"` gets the error without a non-null
 * assertion, and one that handles the result cases can't reach for an `error`
 * that isn't there. `vary` rides the same discriminant -- present exactly on
 * the entry-selecting results.
 */
export type CacheReadMessage = Attribution & { resourceId: string } & (
    | CacheReadOutcome
    | {
        /**
         * The store threw; no lookup result exists. The error still propagates
         * to the `Cache.get`/`getMany` caller after this message is published.
         */
        found: "read-failed";
        /** Whatever the store threw. Unknown by design -- stores may reject with anything. */
        error: unknown;
        /** No lookup result, so no variant. Absent, never `{}`. */
        vary?: undefined;
      }
  );

/**
 * The diagnostics channel for read events. Subscribe with
 * `cacheReadChannel.subscribe((message) => …)` for an inferred
 * `CacheReadMessage`, or by name via `CACHE_READ_CHANNEL_NAME`.
 */
export const cacheReadChannel = typedChannel<CacheReadMessage>()(
  CACHE_READ_CHANNEL_NAME,
);

/**
 * Publishes a read event to the diagnostics channel.
 * @internal
 */
export function publishCacheRead(message: CacheReadMessage): void {
  cacheReadChannel.publish(message);
}

/**
 * Name of the diagnostics channel that fires once per call of a wrapped
 * producer function (per request element, for the bulk wrappers), at
 * settlement, reporting how the logical request was answered.
 *
 * Sole exception: a cache-read failure under `onCacheReadFailure: "throw"`
 * rethrows before any disposition exists and emits nothing.
 */
export const CACHE_FETCH_CHANNEL_NAME = "@zingage/cache:fetch";

/**
 * How a logical request was answered, and -- on the producer-path branch --
 * whether the consumer's directives forced the producer trip.
 *
 * Exported because the producer wrappers publish these: they take a value of
 * this type and spread it into the message, so the disposition vocabulary and
 * the "which branch carries the bypass flag" rule live here only. A wrapper
 * re-spelling the union locally would stay assignable while silently lacking
 * any disposition added here.
 *
 * Note that publishers OMIT `directivesImpliedBypass` on the cache-read branch
 * rather than publishing it as `false`; it is typed `?: false` so subscribers
 * can still read the property uniformly across the union. `vary` is the
 * mirror image: present on the cache-read branch, omitted on the other, and
 * typed `?: undefined` there so it too reads uniformly.
 */
export type CacheFetchDisposition =
  | {
      // Dispositions reachable only via a cache read -- which bypass
      // requests skip entirely, making this branch sound by construction
      // rather than statistically. (Without skip-read, `maxAge: 0` can be
      // satisfied: age is a ms-resolution float compared with strict `>`,
      // so a same-millisecond entry has age 0 ≤ 0, and cross-pod clock skew
      // makes age *negative* -- the producer-stamped `date` is the birth
      // basis.)
      //
      // One `served-from-cache` source is not a STORE read: a wrapper serves
      // an entry it produced itself whose `cache.store()` is still in flight
      // (see `wrapProducer`'s docs). That is a hit from the consumer's side
      // -- the value the store read would have returned had the write landed
      // -- so it is reported as one, but it emits no `read` message, since
      // the read channel counts store reads.
      disposition:
        | "served-from-cache"
        | "served-stale-while-revalidating"
        | "served-stale-after-error";
      directivesImpliedBypass?: false;
      /**
       * The SERVED entry's (normalized) vary: identifying the
       * `(resourceId, vary)` variant the value came from. It is the same key
       * `store-entry` reports for the write that put it there (see
       * {@link CacheStoreEntryMessage.vary}), so a subscriber can attribute
       * serves to the exact stored variant -- e.g. count how many times one
       * version was served before a newer one replaced it. Keying on
       * `resourceId` alone gets that wrong the moment a producer varies:
       * every variant's serves land on whichever variant stored first.
       *
       * Reported per the branch, not the request: these entries come back
       * from the cache read already normalized, so publishing costs a property
       * read and nothing else.
       */
      vary: Vary<AnyParams>;
    }
  | {
      disposition: "served-from-producer" | "producer-error" | "aborted";
      /**
       * True iff the consumer's directives forced producer contact
       * regardless of cache contents (`isRequestingCacheBypass`:
       * `maxAge: 0`). A pure request property -- and any of these three
       * dispositions can be bypass-triggered (the producer trip can fail
       * or be abandoned). Lets hit-rate dashboards separate bypass traffic
       * from real misses.
       */
      directivesImpliedBypass: boolean;
      /**
       * Absent on the whole producer branch, including `served-from-producer`
       * -- which DID hand the caller an entry. What this field identifies is
       * the stored variant a value was served OUT OF, and a produced value
       * came out of the origin: the variant it lands in belongs to the write,
       * which is reported on `store-entry` when (and only if) the
       * fire-and-forget store actually succeeds, and once per invocation
       * rather than once per collapsed rider. Publishing a vary here would
       * identify a variant this request never read, for a write that may
       * still fail.
       *
       * `producer-error` and `aborted` have no entry at all. In every case
       * the key is omitted rather than set to `{}`, which identifies a real
       * variant (see this file's overview).
       */
      vary?: undefined;
    };

/**
 * The message type published to the fetch diagnostics channel.
 */
export type CacheFetchMessage = Attribution & {
  resourceId: string;
  /**
   * True if this request's own SETTLEMENT rode an already-in-flight producer
   * call -- it was answered (or errored/aborted) via an invocation some
   * other caller initiated. Cache-served settlements report false even when
   * the request attached a background revalidation to an in-flight
   * invocation: a `served-stale-while-revalidating` rider counts in the
   * produce channel's `collapsedCallerCount` but not here, because its
   * settlement was the cached entry, not the invocation.
   *
   * In the base (not the union) because it doesn't co-vary with the bypass
   * flag: e.g. `served-stale-after-error` can be collapsed (a rider on a
   * failed shared invocation falling back to its own if-error entry) but
   * never bypassed.
   */
  collapsed: boolean;
} & CacheFetchDisposition;

/**
 * The diagnostics channel for fetch events. Subscribe with
 * `cacheFetchChannel.subscribe((message) => …)` for an inferred
 * `CacheFetchMessage`, or by name via `CACHE_FETCH_CHANNEL_NAME`.
 */
export const cacheFetchChannel = typedChannel<CacheFetchMessage>()(
  CACHE_FETCH_CHANNEL_NAME,
);

/**
 * Publishes a fetch event to the diagnostics channel.
 * @internal
 */
export function publishCacheFetch(message: CacheFetchMessage): void {
  cacheFetchChannel.publish(message);
}

/**
 * Name of the diagnostics channel that fires once per actual producer
 * invocation, when the invocation settles -- foreground misses AND
 * background revalidations (whose outcome is invisible on the fetch channel,
 * arriving after the triggering fetch event already shipped).
 */
export const CACHE_PRODUCE_CHANNEL_NAME = "@zingage/cache:produce";

/**
 * The message type published to the produce diagnostics channel.
 */
export type CacheProduceMessage = {
  cache: string;
  /**
   * Why the producer was contacted -- the invocation's INITIATING cause.
   * Bypass never mixes with the other two (the collapse key includes the
   * request's directives, so a `maxAge: 0` caller and a plain-miss caller
   * never share an invocation), but miss and revalidation callers use
   * identical directives and CAN share one: a same-key miss arriving while a
   * revalidation is in flight rides it (and vice versa) without re-labeling
   * the trigger.
   */
  trigger: "miss" | "revalidation" | "bypass";
  /**
   * The requests this invocation covered, in the order the producer received
   * them. Length 1 except for bulk producers, whose batches MAY span resource
   * types -- so read each element's own `resourceType`; do not treat
   * `requests[0].resourceType` as the invocation's type.
   */
  requests: readonly { resourceType: string; resourceId: string }[];
  /**
   * Total logical callers this invocation served: the initiator plus every
   * rider that collapsed onto it (≥ 1; a background revalidation with no
   * waiting foreground caller reports 1). Counts ATTACHMENT, not settlement
   * dependence: an SWR caller whose background revalidation rode this
   * invocation is counted here while its own fetch reports
   * `collapsed: false` -- so Σ(collapsedCallerCount − 1) ≥ the number of
   * `collapsed: true` fetches. May also undercount by riders that attach in
   * the settlement's microtask window (they're served by the just-settled
   * invocation, but this count was already read).
   */
  collapsedCallerCount: number;
  outcome: "success" | "error";
  durationMs: number;
};

/**
 * The diagnostics channel for produce events. Subscribe with
 * `cacheProduceChannel.subscribe((message) => …)` for an inferred
 * `CacheProduceMessage`, or by name via `CACHE_PRODUCE_CHANNEL_NAME`.
 */
export const cacheProduceChannel = typedChannel<CacheProduceMessage>()(
  CACHE_PRODUCE_CHANNEL_NAME,
);

/**
 * Publishes a produce event to the diagnostics channel.
 * @internal
 */
export function publishCacheProduce(message: CacheProduceMessage): void {
  cacheProduceChannel.publish(message);
}

/**
 * Name of the diagnostics channel that fires once per entry passed to
 * `Cache.store()`, reporting how the entry's value relates to what was
 * already stored for the same `(id, vary)` variant (see
 * {@link StoreEntryRelationship}) -- or `undefined` when the store didn't
 * report a relationship for the entry.
 *
 * Attribution comes from classifying the entry's own id -- which is what
 * makes supplemental writes and direct `Cache.store()` calls correctly
 * attributed with no name threading.
 *
 * Subscribers can use these events to, e.g., observe how often cached data is
 * actually changing at the origin.
 */
export const CACHE_STORE_ENTRY_CHANNEL_NAME = "@zingage/cache:store-entry";

/**
 * The message type published to the store-entry diagnostics channel.
 */
export type CacheStoreEntryMessage = Attribution & {
  /** The stored entry's resource id */
  resourceId: string;
  /**
   * The stored entry's (normalized) vary object -- the second half of the
   * `(resourceId, vary)` pair identifying the variant this write targeted,
   * and the join key `read`/`fetch` report for the serves out of that
   * variant (see this file's overview).
   */
  vary: Vary<AnyParams>;
  /** The stored entry's validators, on which the comparison was keyed */
  validators: Partial<AnyValidators>;
  /**
   * How the entry's value relates to what the variant previously held (see
   * {@link StoreEntryRelationship}), or `undefined` when the store
   * didn't report a relationship for this entry (it didn't perform the
   * check, the entry had empty validators so there was nothing to compare
   * on, or the entry was an in-call duplicate that lost to a newer entry for
   * the same variant and so was never persisted).
   */
  relationshipToExistingStoredData: StoreEntryRelationship | undefined;
};

/**
 * The diagnostics channel for store-entry events. Subscribe with
 * `cacheStoreEntryChannel.subscribe((message) => …)` for an inferred
 * `CacheStoreEntryMessage`, or by name via `CACHE_STORE_ENTRY_CHANNEL_NAME`.
 */
export const cacheStoreEntryChannel = typedChannel<CacheStoreEntryMessage>()(
  CACHE_STORE_ENTRY_CHANNEL_NAME,
);

/**
 * Publishes a store-entry event to the diagnostics channel.
 * @internal
 */
export function publishCacheStoreEntry(message: CacheStoreEntryMessage): void {
  cacheStoreEntryChannel.publish(message);
}
