import { isEqual } from "es-toolkit";
import type { ReadonlyDeep } from "type-fest";

import { instantiateTaggedType } from "type-party/runtime/tagged-types.js";
import {
  type AnyParams,
  type AnyValidators,
  type CacheSpec,
  type ConsumerDirectives,
} from "../index.js";
import type {
  NormalizedProducerMaxStale,
  NormalizedProducerResultResource,
} from "../types/06_Normalization.js";
import { normalizeConsumerMaxStale } from "./normalization.js";

type AnyNormalizedProducerResultResource = NormalizedProducerResultResource<
  CacheSpec,
  AnyValidators,
  AnyParams
>;

/**
 * Returns the moment when the resource's value was generated _by the origin_.
 * This may be different from the date that the NormalizedProducerResultResource
 * was created, if the NormalizedProducerResultResource was created by a cache
 * that had already been holding the origin's result for some time.
 */
export function birthDate(it: AnyNormalizedProducerResultResource) {
  return new Date(it.date.valueOf() - it.initialAge * 1000);
}

/**
 * Returns the amount of time between the time when the resource was generated
 * _at the origin_ and the provided date.
 */
export function age(it: AnyNormalizedProducerResultResource, at: Date) {
  return (at.valueOf() - birthDate(it).valueOf()) / 1000;
}

/**
 * How many seconds remain until the entry could not even potentially satisfy
 * an incoming request. This will often be infinite, because the consumer can
 * request arbitrarily stale entries (via maxStale).
 */
export function potentiallyUsefulFor(
  it: AnyNormalizedProducerResultResource,
  at: Date,
) {
  return it.directives.maxStale && !isValidatable(it)
    ? it.directives.freshUntilAge + it.directives.maxStale.ifError - age(it, at)
    : Infinity;
}

/**
 * Calculates the maximum amount of time -- in seconds! -- that the backing
 * store may store the entry. `Cache.store` hands the result to the store as
 * `maxStoreForSeconds`; a result of 0 means the store will not retain the entry
 * at all (which is what a producer's `storeFor: 0` asks for). It considers the producer's requested storeFor
 * time, and when the data will become definitively useless.
 *
 * @param entry The entry who's time-to-store should be calculated
 * @param at The date when the entry will be stored. This effects how long it
 *   should be stored for because, as entries get closer to the end of their
 *   freshness lifetime, the suggested storeFor time may go down (when it isn't
 *   dictated by the producer's directives).
 */
export function calculateStoreFor(
  entry: AnyNormalizedProducerResultResource,
  at: Date,
) {
  const producerStoreFor = entry.directives.storeFor;
  const requestedStoreFor =
    producerStoreFor !== undefined
      ? producerStoreFor - entry.initialAge
      : Infinity;

  return Math.max(
    0,
    Math.min(requestedStoreFor, potentiallyUsefulFor(entry, at)),
  );
}

/**
 * Returns whether the entry has data that can be used for revalidation.
 */
export function isValidatable(it: AnyNormalizedProducerResultResource) {
  return Object.keys(it.validators).length > 0;
}

/**
 * Structural, order-independent deep-equality over two opaque `validators`
 * objects, used to decide how a newly-stored entry relates to what was already
 * stored for its slot. Validators are user-defined opaque JSON, so we do not
 * normalize anything (e.g. weak/strong etag `W/` prefixes).
 *
 * Callers must pass values in their JSON-serialized form (see
 * {@link validatorsAsStored}); values read back from a JSON-backed store
 * already are.
 */
export function validatorsEqual(
  a: Partial<AnyValidators>,
  b: Partial<AnyValidators>,
): boolean {
  return isEqual(a, b);
}

/**
 * Returns the validators as they survive JSON serialization. Validator values
 * are JSON by contract, but `undefined` values are expressible from JS and
 * are silently dropped when an entry is persisted. Change detection operates
 * on this serialized form -- for both the emptiness check and the comparison
 * -- so that stores that see the raw in-memory object (MemoryStore, and the
 * incoming side in PostgresStore) agree with stores that compare persisted
 * JSON (SqliteStore).
 */
export function validatorsAsStored(
  validators: Partial<AnyValidators>,
): Partial<AnyValidators> {
  // The round-trip is exactly the transform persistence applies, and
  // serializing a validators object yields a validators object (minus any
  // dropped keys), so the cast back is sound.
  return JSON.parse(JSON.stringify(validators)) as Partial<AnyValidators>;
}

export function isFresh(it: AnyNormalizedProducerResultResource, at: Date) {
  const ageAt = age(it, at);
  return ageAt >= 0 && ageAt <= it.directives.freshUntilAge;
}

// Note: Unusable entries may still have validation info; in that way, they
// could be helpful in fetching an updated ProducerResult that is usable.
// This was originally a numeric enum, but strings made for way better logs.
export const EntryClassification = {
  Usable: "Usable",
  UsableWhileRevalidate: "UsableWhileRevalidate",
  UsableIfError: "UsableIfError",
  Unusable: "Unusable",
} as const;

/**
 * Returns a results applicability/usability, for a set of consumer directives,
 * and at a given date, based on its stored age etc.
 *
 * Note: this does **not** factor in the entry's `id` or `vary` value; its
 * assumed that the entry is a valid candidate for the request's params.
 */
export function classify(
  entry: AnyNormalizedProducerResultResource,
  consumerDirs: ReadonlyDeep<ConsumerDirectives>,
  at: Date,
) {
  const producerDirs = entry.directives;

  // Exact logic here may change if I get more clarity on HTTP directive
  // interactions and those interactons can't be simulated with these rules
  // (after some transformation of the input producer + consumer directives).
  // Context: https://twitter.com/ethanresnick/status/1200154215756312580
  const ageAtDate = age(entry, at);

  // An entry exceeding the consumer's maxAge can _never_ be usable,
  // even when the origin is unreachable. maxAge is a hard ceiling.
  if (consumerDirs.maxAge !== undefined && ageAtDate > consumerDirs.maxAge) {
    return EntryClassification.Unusable;
  }

  // Normalize the consumer's maxStale directive, if provided, before we do
  // anything that might use it.
  const givenConsumerMaxStaleNormalized = consumerDirs.maxStale
    ? normalizeConsumerMaxStale(consumerDirs.maxStale)
    : undefined;

  // Compute effective freshness lifetime: the minimum of the consumer's
  // freshUntilAge override (if provided) and the producer's freshUntilAge.
  const effectiveFreshUntilAge = Math.min(
    givenConsumerMaxStaleNormalized?.freshUntilAge ?? Infinity,
    producerDirs.freshUntilAge,
  );

  // A fresh entry (by the effective freshness), and which we've already checked
  // satisifies the consumer's maxAge, is always usable.
  if (ageAtDate <= effectiveFreshUntilAge) {
    return EntryClassification.Usable;
  }

  // For stale entries, it gets a bit more complicated. There are 4 cases,
  // corresponding to whether or not each of (consumer, producer) did or didn't
  // provide a maxStale directive...

  // The simplest case is when no maxStale is given by either party, which is
  // common enough that we short-circuit it here (even though removing this
  // check shouldn't change the result).
  if (!producerDirs.maxStale && !givenConsumerMaxStaleNormalized) {
    return EntryClassification.Unusable;
  }

  // If we do have at least one maxStale, apply the logic below for figuring out
  // (from the normalized versions of the explicitly given `maxStale` directives
  // of each party) what final maxStale value to apply.
  const finalProducerMaxStale =
    producerDirs.maxStale ??
    instantiateTaggedType<NormalizedProducerMaxStale>({
      withoutRevalidation: Infinity,
      whileRevalidate: Infinity,
      ifError: Infinity,
    });

  const finalConsumerMaxStale =
    givenConsumerMaxStaleNormalized ??
    (!producerDirs.maxStale
      ? instantiateTaggedType<NormalizedProducerMaxStale>({
          withoutRevalidation: 0,
          whileRevalidate: 0,
          ifError: 0,
        })
      : instantiateTaggedType<NormalizedProducerMaxStale>({
          withoutRevalidation: 0,
          whileRevalidate: producerDirs.maxStale.whileRevalidate,
          ifError: producerDirs.maxStale.ifError,
        }));

  const staleness = ageAtDate - effectiveFreshUntilAge;

  // Use maxStale value that are the minimums of the consumer and producer's
  // respective maxStale entries, to make sure we're satisfying both parties'
  // requirements.
  if (
    staleness <=
    Math.min(
      finalConsumerMaxStale.withoutRevalidation,
      finalProducerMaxStale.withoutRevalidation,
    )
  ) {
    return EntryClassification.Usable;
  }

  if (
    staleness <=
    Math.min(
      finalConsumerMaxStale.whileRevalidate,
      finalProducerMaxStale.whileRevalidate,
    )
  ) {
    return EntryClassification.UsableWhileRevalidate;
  }

  if (
    staleness <=
    Math.min(finalConsumerMaxStale.ifError, finalProducerMaxStale.ifError)
  ) {
    return EntryClassification.UsableIfError;
  }

  return EntryClassification.Unusable;
}
