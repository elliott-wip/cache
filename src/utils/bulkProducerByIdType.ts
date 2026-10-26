import { mapCooperatively, forEachCooperatively } from "./cooperative.js";
/**
 * @fileoverview Per-resource-type dispatch for BULK producers: the bulk
 * counterpart of `producerByIdType`, and the sub-producer vocabulary that only
 * this helper's callers speak.
 *
 * Split from the wrapper for the same reason the single helper is: routing a
 * batch to sub-producers is a function of the resource-type registry's `matches`
 * guards and nothing else, so a by-id-type bulk producer is a value in its own
 * right -- buildable, drivable and testable before any cache exists.
 *
 * Routing itself is imported from `producerByIdType` rather than reimplemented,
 * which is what keeps the two helpers' answers to "is this id covered" from
 * drifting apart. The edge back to `wrapBulkProducer` is **type-only** (its
 * `CoveringBulkProducer` return shape and its id-erased `LooseBulkProducer`), as
 * is `wrapBulkProducer`'s edge to the sub-producer type here, so the runtime
 * import graph stays acyclic in both directions.
 *
 * @module
 */
import type {
  IdOfResourceType,
  ResourceTypeName,
  ResourceTypes,
  SpecOf,
} from "../types/00_ResourceTypes.js";
import { resourceTypesEntries } from "../resourceTypeClassification.js";
import type {
  AnyParams,
  AnyValidators,
  ReadonlyConsumerRequest,
  RequestPairedProducerResult,
} from "../types/index.js";
import { resolveCoveredSubProducer } from "./producerByIdType.js";
import type {
  CoveringBulkProducer,
  LooseBulkProducer,
} from "./wrapBulkProducer.js";
import {
  coveredTypes,
  emptyProducersRecordMessage,
  type LooseRequestFor,
  type LooseResultFor,
} from "./wrapProducer.js";

/**
 * A bulk producer for ONE resource type: a {@link bulkProducerByIdType}
 * sub-producer. That helper splits the wrapper's batch by classified resource
 * type and calls each sub-producer once with its own type's slice, so a
 * sub-producer's batch never mixes types. (The wrapper itself takes a single
 * {@link CoveringBulkProducer}, which by default sees the WHOLE mixed batch --
 * that is the point of the single-function form.) Results are request-paired;
 * Error elements mark per-request failures.
 *
 * The result type is a flat array of (RequestPairedProducerResult |
 * ErrorType): each element is a discriminated union over the type's spec, so
 * each (id, content) pair must internally agree, but the type system does
 * not require the i'th result element to align with the i'th request's id at
 * the call site (which would require gnarly mapped-tuple typing). When
 * `wrapBulkProducer` returns its results to the caller, they ARE narrowed
 * per-request via the wrapper's own generic.
 */
export type BulkResourceTypeProducer<
  RT extends ResourceTypes,
  K extends ResourceTypeName<RT>,
  Validators extends AnyValidators,
  Params extends AnyParams,
  ErrorType extends Error,
> = (
  reqs: readonly ReadonlyConsumerRequest<Params, IdOfResourceType<RT[K]>>[],
) => Promise<
  (
    | RequestPairedProducerResult<
        SpecOf<RT>,
        Validators,
        Params,
        IdOfResourceType<RT[K]>
      >
    | ErrorType
  )[]
>;

/**
 * {@link bulkProducerByIdType}'s argument: one entry per covered resource
 * type, any non-empty subset of the registry. `Covered` is inferred from the
 * record's keys, exactly as in `producerByIdType`.
 */
export type BulkProducersFor<
  RT extends ResourceTypes,
  Covered extends ResourceTypeName<RT>,
  Validators extends AnyValidators,
  Params extends AnyParams,
  ErrorType extends Error,
> = {
  readonly [K in Covered]: BulkResourceTypeProducer<
    RT,
    K,
    Validators,
    Params,
    ErrorType
  >;
};

/**
 * The bulk counterpart of `producerByIdType`: sugar over `wrapBulkProducer`'s
 * single-producer primitive that turns a record with one bulk producer per
 * covered resource type into ONE function, and declares its covered set in
 * {@link coveredTypes} so the wrapper can both infer `Covered` from it and
 * enforce it at runtime.
 *
 * Use it when each resource type has its own origin, or to cover a strict
 * subset of the registry. Skip it -- pass a bare function -- when the producer
 * wants the full mixed batch, which is the capability the single-function form
 * exists for.
 *
 * Behaviour on each invocation:
 *
 * - The incoming batch is split by classifying each `req.id` against the
 *   registry, remembering every request's ORIGINAL index, and each sub-producer
 *   is invoked once, concurrently, with its own slice.
 * - Results are reassembled **positionally**: slice position `j` maps back to
 *   that request's original index. Positional is forced, not chosen: a batch
 *   can legitimately contain the same id twice with different `params`, so the
 *   id is not a routing key.
 * - A sub-producer's **failure** -- a rejection, or a synchronous throw from a
 *   non-async sub-producer -- is caught and written into that type's slots as
 *   `Error` elements, so per-request error isolation survives the merge, and one
 *   type's failure never discards a sibling type's results. It lives in the
 *   sugar rather than in the wrapper.
 * - A sub-producer whose result count does not match the slice it was given --
 *   in EITHER direction -- fails the whole invocation, naming that resource type
 *   and both counts. Both directions mean the sub-producer disagrees with the
 *   slice it was handed, so its positional pairing is no longer trustworthy, and
 *   catching it here is what makes the error name the offending sub-producer;
 *   the wrapper makes the same check in both directions for a bare producer, but
 *   sees only the merged batch and so can report a total at best. Padding the
 *   missing slots with `Error`s instead would turn a contract violation into a
 *   per-request failure and hide the bug.
 *
 * Requests routed through this helper are classified twice, for the reason
 * given on `producerByIdType`.
 *
 * Throws at construction on an empty record.
 *
 * @param resourceTypes - The registry the producer's ids will be classified
 *   against: `cache.resourceTypes` for the cache it will be wrapped against.
 *   See `producerByIdType` for why the registry and not the cache.
 * @param producers - One {@link BulkResourceTypeProducer} per covered resource
 *   type; `Covered` is inferred from the keys.
 */
export function bulkProducerByIdType<
  RT extends ResourceTypes,
  Covered extends ResourceTypeName<RT>,
  Validators extends AnyValidators = AnyValidators,
  Params extends AnyParams = AnyParams,
  ErrorType extends Error = Error,
>(
  resourceTypes: RT,
  producers: BulkProducersFor<RT, Covered, Validators, Params, ErrorType>,
): CoveringBulkProducer<RT, Covered, Validators, Params, ErrorType> {
  // SAFETY: see LooseProducer in wrapProducer.ts. A value is only read out of
  // this record after classification succeeds and the key is confirmed to be
  // the record's own, so each sub-producer's slice holds exactly the ids it
  // declared. Snapshotted for the reason given on `coveredTypeSet`.
  const looseProducers = { ...producers } as unknown as Readonly<
    Record<string, LooseBulkProducer<RT, Validators, Params, ErrorType>>
  >;
  const coveredResourceTypes = Object.keys(looseProducers);

  if (coveredResourceTypes.length === 0) {
    throw new Error(
      emptyProducersRecordMessage("bulkProducerByIdType", "wrapBulkProducer"),
    );
  }

  // Computed once, here, rather than per classified id. Named for the registry
  // it comes from: `entries` in this function's scope means a request group's.
  const classifierEntries = resourceTypesEntries(resourceTypes);

  type LooseRequest = LooseRequestFor<RT, Params>;
  type LooseResult = LooseResultFor<RT, Validators, Params>;

  const dispatchingProducer = async (reqs: readonly LooseRequest[]) => {
    // Throws UnroutableIdError on the first id that doesn't classify to exactly
    // one covered type -- before any sub-producer runs, so a batch with an
    // unroutable element contacts no origin at all. Unreachable through the
    // wrapper unless the registry here disagrees with the cache's; reachable
    // whenever the returned producer is driven directly.
    const classified = await mapCooperatively(reqs, (req, index) => ({
      index,
      req,
      ...resolveCoveredSubProducer(
        classifierEntries,
        looseProducers,
        coveredResourceTypes,
        req.id,
      ),
    }));

    // Preallocated so each group can write its own results back at their
    // original indices. Every request classifies into exactly one group and
    // every group fills its whole slice (count-checked below), so this is dense
    // by the time it is returned.
    // oxlint-disable-next-line unicorn/no-new-array -- intentional preallocation; filled by index below
    const results: (LooseResult | Error)[] = new Array(reqs.length);

    await Promise.all(
      [...Map.groupBy(classified, (it) => it.resourceType).entries()].map(
        async ([resourceType, entries]) => {
          // Every member of a group resolved to the same resource type, hence
          // to the same sub-producer. Non-null assertion is safe: Map.groupBy
          // never makes an empty group.
          const { subProducer } = entries[0]!;

          // try/catch rather than a `.catch()` on the returned promise: a
          // sub-producer that fails SYNCHRONOUSLY -- a non-async function with
          // an argument-validation `throw`, or one whose first synchronous step
          // throws (the hashed-input wrappers' internal producers read their
          // input registry synchronously) -- never reaches a handler attached
          // to its return value. That would take the whole MIXED batch down
          // with it, discarding the other resource types' already-computed
          // results and their store, for exactly the failure this helper
          // promises to isolate to one type's slots.
          let subResults: readonly (LooseResult | Error)[];
          try {
            subResults = await subProducer(
              await mapCooperatively(entries, (it) => it.req),
            );
          } catch (error: unknown) {
            // Per-request error isolation survives the merge: this type's
            // slots settle as Error elements instead of failing the whole
            // invocation. A non-Error failure is wrapped rather than stored
            // raw, since a non-Error in a result slot would be read as a
            // successful producer result; the original is kept as `cause`.
            const asError =
              error instanceof Error
                ? error
                : new Error(
                    `bulkProducerByIdType: the "${resourceType}" producer rejected with a non-Error value`,
                    { cause: error },
                  );
            subResults = await mapCooperatively(entries, () => asError);
          }

          // Any count disagreement fails the whole invocation, rather than
          // filling what came back and leaving the rest as holes for the
          // wrapper's own under-return check to find: this names the offending
          // resource type and both counts, where the wrapper -- which sees only
          // the merged batch -- can report a total ("results for only 7 of 10
          // requests") but never which sub-producer broke it. It also stops the
          // merge resting on `undefined` as the "no result here" sentinel; that
          // is sound (see the wrapper's check) but a count comparison needs no
          // such premise. The failure path above is length-correct by
          // construction, so this only fires for a sub-producer that returned.
          if (subResults.length !== entries.length) {
            throw new Error(
              `bulkProducerByIdType: the "${resourceType}" producer returned a ` +
                `result count (${String(subResults.length)}) that does not match ` +
                `the number of requests in its slice (${String(entries.length)}); ` +
                `each sub-producer must return exactly one result or Error per ` +
                `request it was given`,
            );
          }

          await forEachCooperatively(entries, (entry, j) => {
            // SAFETY: the counts were just checked to match, so slot `j` of a
            // sub-producer's results exists for every entry in its slice.
            results[entry.index] = subResults[j]!;
          });
        },
      ),
    );

    return results;
  };

  // SAFETY: the dispatching function is id-erased internally (see
  // LooseProducer in wrapProducer.ts), and the covered names are the record's
  // own keys, so the declared set and the reachable sub-producers cannot
  // disagree.
  return Object.assign(dispatchingProducer, {
    [coveredTypes]: coveredResourceTypes,
  }) as unknown as CoveringBulkProducer<
    RT,
    Covered,
    Validators,
    Params,
    ErrorType
  >;
}
