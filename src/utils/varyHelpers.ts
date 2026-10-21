import type { Tagged } from "type-fest";

import type {
  AnyParams,
  AnyParamValue,
  NormalizedParams,
  NormalizedVary,
} from "../types/index.js";
import { type JsonOf, jsonStringify } from "./utils.js";

// Not the secondary cache key, but a canonical list of the _names_ of the
// params that are used to to generate the secondary cache key. (I.e., the
// request params that the cached response was indicated to vary on.)
export type VaryKeys = readonly string[];

// NB: Looks like JsonOf<[k, v, k, v, k, v, ...]>, and this format is relied on
// by some of the stores (i.e., is part of the public contract).
export type VariantKey = Tagged<
  JsonOf<(string | null | AnyParamValue)[]>,
  "VariantKey"
>;

// The only difference between VaryEntry and NormalizedVaryEntry is that the
// latter excludes undefined. We need to write these types out here and cast
// below because TS can't follow this. See note in the definition of AnyParams.
type VaryEntry<V extends AnyParams> = readonly [
  string,
  V[keyof V] | null | undefined,
];

type NormalizedVaryEntry<V extends AnyParams> = readonly [
  string,
  Exclude<V[keyof V], undefined> | null,
];

/**
 * This function produces a unique/canonical string for a NormalizedVary value.
 * In other words, it's a string that represents the exact param names + values
 * that the producer said its result varies on. This is the secondary cache key.
 */
export function resultVariantKey<V extends AnyParams>(vary: NormalizedVary<V>) {
  return resultVariantKeyFromVaryEntries(
    Object.entries(vary) satisfies VaryEntry<V>[] as NormalizedVaryEntry<V>[],
  );
}

export function resultVariantKeyFromVaryEntries<V extends AnyParams>(
  entries: readonly NormalizedVaryEntry<V>[],
) {
  const sortedEntries = entries.toSorted(varyEntriesSorter);
  return jsonStringify(sortedEntries.flat(1)) as VariantKey;
}

/**
 * Returns whether a given `vary` object is compatible with the request's params.
 *
 * Recall that a `null` vary value means "this variant applies only when the
 * request omits that param" (see the note on `AnyParamValue`), so an absent
 * param must compare equal to a `null` vary value.
 */
export function variantMatchesRequest<V extends AnyParams>(
  vary: Readonly<NormalizedVary<V>>,
  normalizedParams: Readonly<NormalizedParams<V>>,
) {
  return Object.entries(vary).every(
    ([key, value]) =>
      ownParamValue(normalizedParams, key) ===
      (value === null ? undefined : value),
  );
}

/**
 * Picks each request's variants for a store whose `getMany` fetches every
 * stored variant of the requested ids in one query. A request's variants are
 * the ones stored under its id whose `vary` its params satisfy (see
 * {@link variantMatchesRequest}). Returns one array per request, in request
 * order.
 *
 * Variants are looked up by `id.toWellFormed()`. A SQL driver sends text as
 * UTF-8, which has no lone UTF-16 surrogates, so the database stores and
 * returns a lone surrogate as U+FFFD. `get` binds the raw id and so finds
 * those rows. This lookup makes the same replacement, so `getMany` finds
 * them too.
 */
export function variantsMatchingRequests<
  V extends AnyParams,
  Variant extends {
    readonly id: string;
    readonly vary: Readonly<NormalizedVary<V>>;
  },
>(
  variants: readonly Variant[],
  requests: readonly {
    readonly id: string;
    readonly params: Readonly<NormalizedParams<V>>;
  }[],
): Variant[][] {
  const variantsById = Map.groupBy(variants, ({ id }) => id);
  return requests.map(({ id, params }) =>
    (variantsById.get(id.toWellFormed()) ?? []).filter(({ vary }) =>
      variantMatchesRequest(vary, params),
    ),
  );
}

/**
 * Read a request param by name with *own-property* semantics, returning
 * `undefined` when the param is absent.
 *
 * A plain `normalizedParams[key]` resolves up the prototype chain, so a param
 * name that collides with an inherited `Object.prototype` member -- e.g.
 * `"constructor"`, `"toString"`, `"__proto__"` -- would read back the inherited
 * function/prototype instead of `undefined`. That breaks matching for vary
 * keys with those names, because a `null` vary value (meaning "param absent")
 * would never compare equal to the inherited value. `Object.hasOwn` restricts
 * the lookup to the request's actual params.
 */
function ownParamValue<V extends AnyParams>(
  normalizedParams: Readonly<NormalizedParams<V>>,
  key: string,
): AnyParamValue | undefined {
  return Object.hasOwn(normalizedParams, key)
    ? normalizedParams[key]
    : undefined;
}

/**
 * When a consumer request comes in, we need to look at its params to figure out
 * whether any cached entries match. However, a consumer request can match
 * producer results with many, many different variant keys (as returned by
 * {@see {@link resultVariantKey}}).
 *
 * For example, imagine a consumer request with params { "a": 1, "b": 2 }. This
 * could match a producer result with `vary: {}` or `vary: { "a": 1 }` or
 * `vary: { "b": 2 }` or `vary: { "a": 1, "b": 2 }`. So, each consumer request
 * with n params generates 2^n potentially-matching variant keys.
 *
 * Enumerating all these is impractical, especially because consumer requests
 * will tend to have way more params than producer results will have vary on.
 *
 * Therefore, this function takes a set of param names for which it's known that
 * at least some producer results vary on that set -- these are the `varyKeys` --
 * and then computes the single variant key with exactly those params that the
 * request would match.
 */
export function requestVariantKeyForVaryKeys<V extends AnyParams>(
  normalizedParams: NormalizedParams<V>,
  varyKeys: VaryKeys,
) {
  return resultVariantKeyFromVaryEntries(
    varyKeys.map(
      // Own-property read (see `ownParamValue`): a missing param -- including a
      // vary key that collides with an inherited `Object.prototype` member like
      // `"__proto__"` -- becomes `null`, matching the "param absent" sentinel a
      // producer stores as a `null` vary value.
      (it) => [it, ownParamValue(normalizedParams, it) ?? null] as const,
    ) satisfies NormalizedVaryEntry<AnyParams>[] as NormalizedVaryEntry<V>[],
  );
}

function varyEntriesSorter(
  a: readonly [string, unknown],
  b: readonly [string, unknown],
) {
  const strA = a[0];
  const strB = b[0];

  return strA > strB ? 1 : strA < strB ? -1 : 0;
}
