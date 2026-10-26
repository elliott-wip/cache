import { mapCooperatively, forEachCooperatively } from "./cooperative.js";
import { default as debug } from "debug";
import { maxBy } from "es-toolkit";

import pLimit from "p-limit";
import type { CacheSpec, SpecForId } from "../types/00_CacheSpec.js";
import type { Entry, NormalizedParams } from "../types/06_Normalization.js";
import type { Store, StoreGetManyResult } from "../types/06_Store.js";
import type { AnyParams, AnyValidators } from "../types/index.js";
import { components, type Logger } from "../types/index.js";
export type { JsonOf } from "type-party";
export { jsonParse, jsonStringify } from "type-party/runtime/json.js";

export const defaultLoggersByComponent = Object.fromEntries(
  components.map(
    (name) =>
      [
        name,
        (() => {
          const debugLogger = debug(`@zingage/cache:${name}`);
          return (_, level, message, data) => {
            debugLogger(`(${level}) ${message} %O`, data);
          };
        })(),
      ] as const,
  ),
) satisfies Record<string, Logger> as {
  [K in (typeof components)[number]]: Logger;
};

/**
 * Preserves a type's tupleness during a map operation.
 */
export function mapTuple<T extends readonly unknown[], U>(
  arr: T,
  fn: (it: T[number]) => U,
) {
  return arr.map(fn) as { [K in keyof T]: U };
}

export function assertUnreachable(_it: never): never {
  throw new Error("Expected this code to never be reached.");
}

/**
 * Races a promise against an AbortSignal. If the signal is already aborted or
 * becomes aborted before the promise settles, the returned promise rejects with
 * the signal's reason. The original promise is NOT cancelled (it continues
 * running); this only controls the caller's observation of the result.
 */
export function raceWithSignal<T>(
  promise: Promise<T>,
  signal: AbortSignal | undefined,
): Promise<T> {
  if (!signal) return promise;

  if (signal.aborted) {
    // Prevent unhandled rejection on the underlying promise, as callers are
    // supposed to only have to handle rejection of the returned promise from
    // this fn.
    promise.catch(() => {});
    throw signal.reason;
  }

  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      reject(signal.reason);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

/**
 * Faster, more-type-safe version of lodash's zip, just for pairs.
 * If the arrays are not the same length, the longer one will be truncated!
 * (That's what allows us to not have `undefined` in the result type.)
 */
export const zip2 = <T, U>(a: readonly T[], b: readonly U[]): [T, U][] => {
  const res = [];
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    res.push([a[i], b[i]] as [T, U]);
  }

  return res;
};

/**
 * Groups the items by `groupBy` and keeps, per group, the (first) item that
 * maximizes `maxBy`. Group order follows first appearance in `items` (i.e.,
 * `Map.groupBy` insertion order).
 *
 * All stores use this to dedupe a `store()` call's entries per (id, vary)
 * slot, keeping the entry with the newest birth date, so that the in-call
 * "winner" is the same across every store implementation.
 */
export function keepMaxPerGroup<T>(opts: {
  items: readonly T[];
  groupBy: (item: T) => string;
  maxBy: (item: T) => number;
}): T[] {
  return Map.groupBy(opts.items, opts.groupBy)
    .values()
    .map((group) =>
      // Non-null assertions are safe because the group cannot be empty,
      // or it wouldn't have an entry in the Map.
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      group.length > 1 ? maxBy(group, opts.maxBy)! : group[0]!,
    )
    .toArray();
}

/** Cooperative equivalent of keepMaxPerGroup for bulk store paths. */
export async function keepMaxPerGroupCooperatively<T>(opts: {
  items: readonly T[];
  groupBy: (item: T) => string;
  maxBy: (item: T) => number;
}): Promise<T[]> {
  const winners = new Map<string, { item: T; max: number }>();
  await forEachCooperatively(opts.items, (item) => {
    const key = opts.groupBy(item);
    const max = opts.maxBy(item);
    const previous = winners.get(key);
    if (!previous || max > previous.max) winners.set(key, { item, max });
  });
  return mapCooperatively(winners.values(), ({ item }) => item);
}

/**
 * A naive implementation of the `getMany` method that uses the store's `get`
 * method to fulfill all requests with limited parallelism.
 *
 * This implementation is provided as a default fallback for stores that don't
 * implement their own optimized `getMany` method. It calls `store.get` for each
 * request with up to 10 concurrent operations to improve performance.
 *
 * @param store The store instance to use for retrieving entries
 * @param requests Array of requests, each containing an id and params
 * @param maxConcurrency Maximum number of concurrent requests (default: 10)
 * @returns A promise that resolves to a tuple-typed array, where each output
 *  slot holds the matching entries for the corresponding input request, with
 *  content narrowed to the spec variants compatible with that request's id.
 */
export async function naiveGetMany<
  Spec extends CacheSpec,
  Validators extends AnyValidators,
  Params extends AnyParams,
  const Reqs extends readonly {
    readonly id: Spec["id"];
    readonly params: Readonly<NormalizedParams<Params>>;
  }[],
>(
  store: Store<Spec, Validators, Params>,
  requests: Reqs,
  maxConcurrency = 10,
  options?: { signal?: AbortSignal },
): Promise<StoreGetManyResult<Spec, Reqs, Validators, Params>> {
  options?.signal?.throwIfAborted();

  const limit = pLimit(maxConcurrency);

  // Process all requests with controlled concurrency
  const promises = await mapCooperatively(requests, (request) => {
    const pending = limit(async () =>
      store.get(request.id, request.params, options),
    );
    // A yielded enrollment pass may outlive an early store rejection.
    void pending.catch(() => {});
    return pending;
  });

  const res = await Promise.all(promises);
  return res satisfies Entry<
    SpecForId<Spec, Spec["id"]>,
    Validators,
    Params
  >[][] as {
    -readonly [K in keyof Reqs]: Entry<
      SpecForId<Spec, Reqs[K]["id"]>,
      Validators,
      Params
    >[];
  };
}

// Highly incomplete code for interoperating with raw HTTP responses,
// by doing all the necessary header parsing and age inference.
// import { FullResponse } from 'request-promise-native'
// import { ProducerResult } from './types'
//
// function httpProducerResult<T extends FullResponse>(
//   resp: T,
//   fetchTimeMs: number,
// ): ProducerResult<T, { etag: string }, string> {
//   const { vary } = resp.headers.vary;
//   const varyHeaders = Array.isArray(vary) ? vary : vary ? [vary] : [];
//   const varyHeadersNormalized = varyHeaders.map((it) => it.trim());

//   return {
//     id: resp.request.uri.href,
//     // In proper HTTP, method (not just uri) is part of the cache key;
//     // we simulate that by treating method as a param name that's always varied on
//     vary: {
//       method: resp.request.method,
//       ...Object.fromEntries(
//         varyHeadersNormalized.map((header) => [
//           header,
//           resp.request.hasHeader(header)
//             ? resp.request.getHeader(header)
//             : null,
//         ]),
//       ),
//     },
//     content: resp,
//     // TODO, parse cache-control or expires w/ Date; use heuristics if missing.
//     maxAge: 100000000000,
//     validate: resp.headers.etag ? { etag: String(resp.headers.etag) } : {},
//     initialAge: Math.round((resp.headers.age || 0) + fetchTimeMs * 1000),
//   };
// }
