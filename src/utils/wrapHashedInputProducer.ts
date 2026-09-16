import pLimit from "p-limit";
import type { ReadonlyDeep } from "type-fest";

import { AmbiguousResourceTypeError, UnclassifiableIdError } from "../Cache.js";
import type { CacheSpec, SpecForId } from "../types/00_CacheSpec.js";
import type {
  ContentOfResourceType,
  IdOfResourceType,
  ResourceTypeName,
  ResourceTypes,
  SpecOf,
} from "../types/00_ResourceTypes.js";
import type {
  AnyParams,
  AnyValidators,
  ConsumerDirectives,
  Entry,
  ProducerResultResource,
  RequestPairedProducerResult,
} from "../types/index.js";
import {
  builtBranches,
  type HashedInputProducer,
  type HashedInputProducerMeta,
  type LooseBranch,
} from "./hashedInputProducerByInputType.js";
import {
  bulkProducerByIdType,
  type BulkProducersFor,
} from "./bulkProducerByIdType.js";
import { producerByIdType } from "./producerByIdType.js";
import { zip2 } from "./utils.js";
import wrapProducer, {
  type ProducersFor,
  type WrapProducerOptions,
} from "./wrapProducer.js";
import { wrapBulkProducer } from "./wrapBulkProducer.js";

/**
 * ## Hashed-input producers vs. (plain) producers
 *
 * `wrapProducer`/`wrapBulkProducer` model a cache as a **lookup of a mutable
 * entity by its id**: the caller already has the id, and the cached value is a
 * function of that id and time (e.g. "the current `User` for `user:123`"). The
 * id is the natural cache key, so the producer receives it directly.
 *
 * `wrapHashedInputProducer`/`wrapBulkHashedInputProducer` model the other common
 * case: the cached value is an **expensive-to-compute function of some input**,
 * reused whenever the same input recurs (e.g. an LLM extraction over a chunk of
 * text, a rendered template, a compiled artifact). Here the *input* is the
 * identity of the work, so a hash of the input is the natural cache key — but
 * the producer wants the original, un-hashed input to actually do the
 * computation. These wrappers encapsulate exactly that: a `hashInput` derives
 * the cache id from an input and a `produce` takes the full input, and the
 * wrapper keeps the input around just long enough to hand it over on a miss,
 * otherwise behaving like `wrapProducer` / `wrapBulkProducer` (same caching,
 * request-collapsing, fire-and-forget store with in-flight writes counting as
 * hits, stale-while-revalidate, abort, and diagnostics behavior; see
 * {@link WrapProducerOptions} and `wrapProducer`'s docs).
 *
 * ## One resource type, or several
 *
 * A cache with one resource type needs nothing but the two functions:
 *
 * ```ts
 * const compute = wrapHashedInputProducer({ cache, hashInput, produce });
 * ```
 *
 * For several, {@link hashedInputProducerByInputType} builds a *hashed-input producer* —
 * one `.when` per covered resource type, dispatching on the input — and the
 * wrapper takes that instead:
 *
 * ```ts
 * const compute = wrapHashedInputProducer({ cache, hashedInputProducer });
 * ```
 *
 * A hashed-input producer is built with **no cache**: it is a value in its own right,
 * constructible (and reusable) before any cache exists, and checked against a
 * cache's registry only where the two are wired together. Coverage is whatever
 * the chain added — any non-empty subset of the registry.
 *
 * ## Minted ids
 *
 * Because hashed-input ids are hashes, the registry's in-band-discriminator
 * requirement falls on `hashInput`: a branch's `hashInput` must mint ids that
 * its resource type's `matches` guard accepts. That is checked twice. At compile
 * time, wiring a hashed-input producer to a cache compares each branch's minted-id
 * type against its variant's `IdOfResourceType`. At runtime, each hashed id is
 * classified before it is used for anything — the backstop for a mint that
 * reached the wrapper through a cast or an untyped boundary. Each way that can
 * fail gets its own error, all naming the branch: the mint matched no resource
 * type (`UnclassifiableIdError`), or several (`AmbiguousResourceTypeError`), or
 * exactly one that isn't the branch's own
 * ({@link MintedIdResourceTypeMismatchError} — a *successful* classification
 * with the wrong answer, so it indicts the branch's `hashInput` rather than the
 * registry's guards, and carries both the expected and the actual type as
 * fields). For accept-everything registries the runtime check is vacuous (the
 * guard accepts every id), and the compile-time one carries the weight.
 *
 * The single-producer form declares no resource type, so there is no name to
 * check a mint against; classifying the id at all is the whole runtime check
 * there, and the mismatch case is unreachable.
 *
 * ## Supplemental resources: input-keyed or id-keyed
 *
 * Like plain producers, a hashed-input producer can return `supplementalResources`
 * — values it produced as a byproduct that are worth caching. They come in
 * two forms, distinguished by which key is present:
 *
 * - **Input-keyed** (`{ input, content, … }`): identified by the input the
 *   value would be computed from. The wrapper routes the input through the
 *   same `matchesInput` branch selection it applies to call-time inputs,
 *   hashes it with the routed branch's `hashInput` (so any COVERED branch
 *   can be the target, not just the producing one), and mint-checks the
 *   result against that branch's type — a bad mint rejects the invocation
 *   loudly, naming the branch. A later `compute(thatInput)` finds the entry
 *   as a cache hit. In a hashed-input producer these are correlated per variant:
 *   the `input` and `content` must come from the SAME variant, so "computing a
 *   collection also caches its stories" is checked rather than merely allowed.
 * - **Id-keyed** (`{ id, content, … }`, a plain {@link ProducerResultResource}):
 *   for ANY registry type, covered or not — exactly what plain producers'
 *   supplementals are. Classified by their own id at store time. This is what
 *   makes these wrappers full peers of `wrapProducer` when the hash exists for
 *   **key privacy** rather than for expensive computation: derive the primary
 *   key by hashing, and still supplementally store other resources under their
 *   natural ids. Typing these needs the registry's id space, which
 *   the single-producer form has; a (cache-free) hashed-input producer must be
 *   declared with the registry's `SpecOf` to return them — see
 *   {@link hashedInputProducerByInputType}.
 *
 * @module
 */

/**
 * Cap on concurrent `hashInput` calls — for both bulk input hashing and
 * supplemental hashing — so a large batch with a possibly-async `hashInput`
 * doesn't flood the event loop.
 */
const HASH_CONCURRENCY = 10;

/**
 * What a hashed-input producer returns: like a plain
 * {@link RequestPairedProducerResult}, but the primary carries no `id` (it's
 * stamped on from the derived hash), and each `supplementalResources` entry
 * is either **input-keyed** (`{ input, … }` — hashed and mint-checked via
 * the `matchesInput`-routed covered branch) or **id-keyed** (`{ id, … }` —
 * a plain {@link ProducerResultResource} for any registry type, classified
 * at store time). See the module docs.
 */
export type HashedInputProducerResult<
  Input,
  Spec extends CacheSpec,
  Validators extends AnyValidators,
  Params extends AnyParams,
  /**
   * The union of specs input-keyed supplementals may target. The wrappers
   * pass every COVERED branch's spec (inputs route by `matchesInput`, so
   * any covered branch can produce a supplemental); defaults to `Spec` so a
   * bare 4-arg instantiation keeps the producing-branch-confined meaning.
   */
  CoveredSpec extends CacheSpec = Spec,
  /**
   * The union of specs id-keyed supplementals may target. The wrappers pass
   * the FULL registry union (id-keyed supplementals are classified by their
   * own id at store time, like plain producers').
   */
  RegistrySpec extends CacheSpec = Spec,
> = Omit<
  RequestPairedProducerResult<Spec, Validators, Params>,
  "id" | "supplementalResources"
> & {
  supplementalResources?: (
    | (CoveredSpec extends unknown
        ? Omit<
            ProducerResultResource<CoveredSpec, Validators, Params>,
            "id"
          > & {
            input: Input;
            id?: never;
          }
        : never)
    | (RegistrySpec extends unknown
        ? ProducerResultResource<RegistrySpec, Validators, Params> & {
            input?: never;
          }
        : never)
  )[];
};

/**
 * Picks the branch for an input: the first (in `.when` order) whose
 * `matchesInput` accepts it. Throws if none does.
 *
 * A branch with NO guard matches unconditionally, which is exactly -- and only
 * -- the single-producer form's one nameless entry (see
 * {@link branchEntriesFor}; every `.when` branch has a guard). Selecting on the
 * guard's ABSENCE rather than on `branchEntries.length === 1` is load-bearing:
 * a hashed-input producer built from a single `.when` is also one entry, and its
 * guard must still be consulted, or an input that guard rejects would be
 * produced and stored anyway under that branch's minted id.
 *
 * A guard that THROWS counts as a non-match, exactly as a registry guard does
 * in {@link Cache.classify}: guards routinely reject foreign inputs by failing
 * to read them (a property access on the wrong shape, a parse that throws), so
 * one branch's failure to recognize an input must not stop a later branch from
 * claiming it. When nothing matches, the guard error(s) surface as the routing
 * error's `cause` rather than leaking with no wrapper/input attribution. Unlike
 * `classify` this stays first-match-wins -- `.when` order is the documented
 * tie-break, since two variants may accept the same input type -- so guards
 * after the match are never evaluated and cannot contribute an error.
 */
function findBranch<B extends { matchesInput?: (input: unknown) => boolean }>(
  wrapperName: string,
  branchEntries: readonly (readonly [string | undefined, B])[],
  input: unknown,
): readonly [string | undefined, B] {
  // Allocates nothing unless a guard actually throws (as in `classify`).
  let guardErrors: unknown[] | undefined;

  const entry = branchEntries.find(([, branch]) => {
    if (branch.matchesInput === undefined) {
      return true;
    }
    try {
      return branch.matchesInput(input);
    } catch (error) {
      (guardErrors ??= []).push(error);
      return false;
    }
  });

  if (entry === undefined) {
    throw new Error(
      `${wrapperName}: no branch matched the input ${JSON.stringify(input)}`,
      guardErrors === undefined
        ? undefined
        : {
            cause:
              guardErrors.length === 1
                ? guardErrors[0]
                : new AggregateError(
                    guardErrors,
                    "one or more branch guards threw while routing an input",
                  ),
          },
    );
  }
  return entry;
}

/**
 * Every way a built {@link HashedInputProducer} can fail to fit a cache's registry,
 * as a union of named problem objects. Surfaced through
 * {@link wrapHashedInputProducer}'s RETURN type rather than its parameter: a
 * conditional over a type parameter placed on the parameter is resolved before
 * that parameter is inferred, which reports every branch as broken. In the
 * return position everything is already inferred.
 */
type HashedInputProducerProblems<
  RT extends ResourceTypes,
  Meta extends HashedInputProducerMeta,
> =
  | ([Exclude<Meta["covered"], ResourceTypeName<RT>>] extends [never]
      ? never
      : {
          ERROR: "hashedInputProducer covers resource types that are not in this cache's registry";
          got: Exclude<Meta["covered"], ResourceTypeName<RT>>;
          expected: ResourceTypeName<RT>;
        })
  | ([Meta["idKeyedSpec"]] extends [SpecOf<RT>]
      ? never
      : {
          ERROR: "hashedInputProducer declares id-keyed supplementals outside this cache's registry";
          got: Meta["idKeyedSpec"];
          expected: SpecOf<RT>;
        })
  | {
      [K in Meta["covered"] & ResourceTypeName<RT>]: [
        Meta["mintedIds"][K],
      ] extends [IdOfResourceType<RT[K]>]
        ? [Meta["outputs"][K]] extends [ContentOfResourceType<RT[K]>]
          ? never
          : {
              ERROR: "a variant's declared output does not match its resource type's content";
              variant: K;
              expected: ContentOfResourceType<RT[K]>;
              got: Meta["outputs"][K];
            }
        : {
            ERROR: "a branch's hashInput mints ids outside its variant's resource type";
            variant: K;
            expected: IdOfResourceType<RT[K]>;
            got: Meta["mintedIds"][K];
          };
    }[Meta["covered"] & ResourceTypeName<RT>];

/**
 * The function a hashed-input wrapper returns. `Extract` rather than `&` when
 * satisfying `SpecForId`'s id constraint: an intersection with a union does not
 * reduce, which silently widens the content back to the whole registry's union
 * on a partially-covering producer.
 */
type WrappedHashedInputProducer<
  RT extends ResourceTypes,
  Input,
  MintedId,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = (
  input: Input,
  options?: { directives?: ConsumerDirectives; signal?: AbortSignal },
) => Promise<
  Entry<
    SpecForId<SpecOf<RT>, Extract<MintedId, SpecOf<RT>["id"]>>,
    Validators,
    Params
  >
>;

/** The bulk analogue of {@link WrappedHashedInputProducer}. */
type WrappedBulkHashedInputProducer<
  RT extends ResourceTypes,
  Input,
  MintedId,
  Validators extends AnyValidators,
  Params extends AnyParams,
  ErrorType extends Error,
> = (
  inputs: readonly Input[],
  options?: { directives?: ConsumerDirectives; signal?: AbortSignal },
) => Promise<
  (
    | Entry<
        SpecForId<SpecOf<RT>, Extract<MintedId, SpecOf<RT>["id"]>>,
        Validators,
        Params
      >
    | ErrorType
  )[]
>;

/**
 * Thrown when a branch's `hashInput` minted an id that classifies to exactly
 * one resource type -- just not the branch's own.
 *
 * Its own error rather than a re-worded {@link UnclassifiableIdError}, because
 * it is a different fact with a different culprit. Classification did not fail
 * here: it succeeded and returned a type the branch does not own, which points
 * at that branch's `hashInput` rather than at the registry's `matches` guards.
 * Both facts a handler needs to tell those apart -- the type the branch expected
 * and the type the id actually classified to -- are therefore fields and not
 * merely message text, so "a producer is wired to the wrong branch" can be
 * routed separately from "this registry does not cover this id space".
 *
 * Only the multi-branch form raises it. The single-producer form declares no
 * resource type, so there is no name to check a mint against and an
 * unclassifiable mint is the only reachable mint failure (see
 * {@link checkMintedId}).
 */
export class MintedIdResourceTypeMismatchError extends Error {
  override readonly name = "MintedIdResourceTypeMismatchError";
  readonly cacheName: string;
  readonly id: string;
  /** The branch -- i.e. the resource-type name -- whose `hashInput` minted `id`. */
  readonly branch: string;
  /** The resource type `id` actually classified to. Never equals `branch`. */
  readonly classifiedResourceType: string;

  constructor(args: {
    cacheName: string;
    id: string;
    branch: string;
    classifiedResourceType: string;
  }) {
    super(
      `Cache "${args.cacheName}": \`hashInput\` for branch "${args.branch}" minted id ${JSON.stringify(args.id)}, which classifies to resource type "${args.classifiedResourceType}" instead of "${args.branch}"`,
    );
    this.cacheName = args.cacheName;
    this.id = args.id;
    this.branch = args.branch;
    this.classifiedResourceType = args.classifiedResourceType;
  }
}

/**
 * Checks that a branch's `hashInput` minted an id that classifies to that
 * branch's own resource type. Runs before the id is used for anything (in
 * particular, before any cache read). Vacuous for accept-everything
 * registries, whose guard accepts every id.
 *
 * Three distinguishable failures, one per way the check can go wrong: the mint
 * matched NO resource type ({@link UnclassifiableIdError}) or SEVERAL
 * ({@link AmbiguousResourceTypeError}) -- both rethrown from `classify` with the
 * offending branch named, since the fact is unchanged and only the message
 * improves -- or it matched exactly one that isn't the branch's
 * ({@link MintedIdResourceTypeMismatchError}).
 *
 * `branchName` is absent for the single-producer form, which declares no
 * resource type to check against; there, classifying the id at all is the whole
 * check (an unclassifiable mint still fails loud, just without a name, and the
 * mismatch case is unreachable).
 */
function checkMintedId(
  cache: { readonly name: string; classify: (id: string) => string },
  branchName: string | undefined,
  id: string,
): void {
  const forBranch =
    branchName === undefined ? "" : ` for branch "${branchName}"`;

  let classified: string;
  try {
    classified = cache.classify(id);
  } catch (e) {
    // `cause` is carried through deliberately: `classify` puts a throwing
    // registry guard's own error there, and that parse failure is the only
    // debuggable account of WHY the mint didn't classify. Rebuilding the error
    // without it would strip it at exactly the boundary that names the branch.
    if (e instanceof UnclassifiableIdError) {
      throw new UnclassifiableIdError({
        cacheName: cache.name,
        id,
        message: `Cache "${cache.name}": \`hashInput\`${forBranch} minted id ${JSON.stringify(id)}, which matches no resource type in the registry`,
        cause: e.cause,
      });
    }
    if (e instanceof AmbiguousResourceTypeError) {
      throw new AmbiguousResourceTypeError({
        cacheName: cache.name,
        id,
        matchedResourceTypes: e.matchedResourceTypes,
        message: `Cache "${cache.name}": \`hashInput\`${forBranch} minted id ${JSON.stringify(id)}, which matches more than one resource type in the registry (${e.matchedResourceTypes.join(", ")})`,
      });
    }
    throw e;
  }

  if (branchName !== undefined && classified !== branchName) {
    throw new MintedIdResourceTypeMismatchError({
      cacheName: cache.name,
      id,
      branch: branchName,
      classifiedResourceType: classified,
    });
  }
}

/**
 * Resolves either call form to the branch table the plumbing dispatches over.
 * The single-producer form has no resource-type name to check mints against, so
 * its one entry is nameless (see {@link checkMintedId}).
 */
function branchEntriesFor(
  wrapperName: string,
  hashedInputProducer: HashedInputProducer<HashedInputProducerMeta> | undefined,
  hashInput: ((input: never) => string | Promise<string>) | undefined,
  produce: ((input: never) => Promise<unknown>) | undefined,
): readonly (readonly [string | undefined, LooseBranch<never>])[] {
  if (hashedInputProducer !== undefined) {
    if (hashInput !== undefined || produce !== undefined) {
      throw new Error(
        `${wrapperName}: pass EITHER \`hashedInputProducer\`, or \`hashInput\` and ` +
          "`produce`; passing both leaves it ambiguous which produces a value.",
      );
    }
    return hashedInputProducer[builtBranches].entries;
  }
  if (hashInput === undefined || produce === undefined) {
    throw new Error(
      `${wrapperName}: pass either \`hashedInputProducer\` (built by ` +
        "`hashedInputProducerByInputType`), or both `hashInput` and `produce`.",
    );
  }
  return [[undefined, { hashInput, produce } as unknown as LooseBranch<never>]];
}

/**
 * Like {@link wrapProducer}, but for "hashed-input producers" whose values are a
 * function of an `Input` rather than a lookup by id: `hashInput` derives the
 * cache id from an input, and `produce` receives the full input. The returned
 * function is called with the input directly.
 *
 * Two forms, one options bag:
 *
 * ```ts
 * // one resource type: no builder, no variant map, no type arguments
 * const compute = wrapHashedInputProducer({ cache, hashInput, produce });
 *
 * // several: a hashedInputProducer built (cache-free) by hashedInputProducerByInputType
 * const compute = wrapHashedInputProducer({ cache, hashedInputProducer });
 * ```
 *
 * See the module docs for when to use this vs. {@link wrapProducer}, and for the
 * branch/coverage/minted-id contracts. Any {@link WrapProducerOptions} field may
 * be set alongside.
 */
export function wrapHashedInputProducer<
  RT extends ResourceTypes,
  Meta extends HashedInputProducerMeta & { kind: "single" },
>(
  options: WrapProducerOptions<RT, Meta["validators"], Meta["params"]> & {
    hashedInputProducer: HashedInputProducer<Meta>;
  },
): [HashedInputProducerProblems<RT, Meta>] extends [never]
  ? WrappedHashedInputProducer<
      RT,
      Meta["inputs"][Meta["covered"]],
      Meta["mintedIds"][Meta["covered"]],
      Meta["validators"],
      Meta["params"]
    >
  : HashedInputProducerProblems<RT, Meta>;
export function wrapHashedInputProducer<
  RT extends ResourceTypes,
  Input,
  MintedId extends SpecOf<RT>["id"],
  Validators extends AnyValidators = AnyValidators,
  Params extends AnyParams = AnyParams,
>(
  options: WrapProducerOptions<RT, Validators, Params> & {
    hashInput: (input: Input) => MintedId | Promise<MintedId>;
    produce: (
      input: ReadonlyDeep<Input>,
    ) => Promise<
      HashedInputProducerResult<
        Input,
        SpecForId<SpecOf<RT>, MintedId>,
        Validators,
        Params,
        SpecForId<SpecOf<RT>, MintedId>,
        SpecOf<RT>
      >
    >;
  },
): WrappedHashedInputProducer<RT, Input, MintedId, Validators, Params>;
export function wrapHashedInputProducer(
  options: WrapProducerOptions<ResourceTypes, AnyValidators, AnyParams> & {
    hashedInputProducer?: HashedInputProducer<HashedInputProducerMeta>;
    hashInput?: (input: never) => string | Promise<string>;
    produce?: (input: never) => Promise<unknown>;
  },
): unknown {
  // `cache` is read directly AND left in `producerOptions`: this wrapper needs
  // it (mint checks, supplemental hashing, the registry it routes against) and
  // `wrapProducer` now takes it as a field of the same bag.
  const { cache } = options;
  const { hashedInputProducer, hashInput, produce, ...producerOptions } =
    options;
  const branchEntries = branchEntriesFor(
    "wrapHashedInputProducer",
    hashedInputProducer,
    hashInput,
    produce,
  );
  const registry = new InputRegistry<never>();
  const hashSupplementals = makeSupplementalHasher<never>(
    "wrapHashedInputProducer",
    branchEntries,
    cache,
  );

  // The internal producer for a branch recovers that branch's input from the
  // registry and hands it to the branch's `produce`. `registry.get` runs
  // synchronously before `produce` is invoked, so the input is read while
  // still registered (see InputRegistry docs).
  const internalProducerFor =
    (branch: LooseBranch<never>) => async (req: { readonly id: string }) => {
      const input = registry.get(req.id);
      return hashSupplementals(await branch.produce(input as never));
    };

  // With named branches the per-type record goes through `producerByIdType`,
  // which turns it into the single producer function `wrapProducer` takes and
  // which carries this wrapper's coverage, so the branch keys still bound the
  // delegated requests. The single-producer form needs none of that: there is
  // one producer for every id it mints.
  //
  // SAFETY: the casts bridge the erased internal producers to the helper's
  // per-type record type (and to `wrapProducer`'s producer type), which are
  // opaque while `RT` is an unresolved generic. Runtime dispatch upholds the
  // contract: each producer only ever receives ids its own branch's `hashInput`
  // minted, checked below to classify to that branch's type before any request.
  const wrapped =
    hashedInputProducer === undefined
      ? wrapProducer(
          producerOptions,
          // Non-null: branchEntriesFor guarantees exactly one entry here.
          // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
          internalProducerFor(branchEntries[0]![1]) as unknown as Parameters<
            typeof wrapProducer
          >[1],
        )
      : wrapProducer(
          producerOptions,
          producerByIdType(
            cache.resourceTypes,
            Object.fromEntries(
              branchEntries.map(([name, branch]) => [
                name,
                internalProducerFor(branch),
              ]),
            ) as unknown as ProducersFor<
              ResourceTypes,
              string,
              AnyValidators,
              AnyParams
            >,
          ),
        );

  return async (
    input: never,
    callOptions?: { directives?: ConsumerDirectives; signal?: AbortSignal },
  ) => {
    const signal = callOptions?.signal;
    signal?.throwIfAborted();

    const [branchName, branch] = findBranch(
      "wrapHashedInputProducer",
      branchEntries,
      input,
    );
    const id = await branch.hashInput(input);
    signal?.throwIfAborted();

    checkMintedId(cache, branchName, id);

    registry.acquire(id, input);
    try {
      return await wrapped(
        // SAFETY: bridges to `PartialConsumerRequest`, whose id/directives
        // are `ReadonlyDeep`-wrapped and so opaque against the plain types
        // here while `RT` is generic; the id was just checked to classify to
        // this branch's covered type. The conditional spread avoids
        // `directives: undefined` (rejected under
        // `exactOptionalPropertyTypes`).
        {
          id,
          ...(callOptions?.directives === undefined
            ? {}
            : { directives: callOptions.directives }),
        } as unknown as Parameters<typeof wrapped>[0],
        signal ? { signal } : undefined,
      );
    } finally {
      registry.release(id);
    }
  };
}

/**
 * The bulk analogue of {@link wrapHashedInputProducer}, layered over
 * {@link wrapBulkProducer}: looks each input's derived id up in the cache and
 * computes only the inputs that missed, as a single batch per branch. Results
 * are returned per input, aligned by index.
 *
 * Same two forms as {@link wrapHashedInputProducer}, with `hashedInputProducer` built
 * by {@link bulkHashedInputProducerByInputType}.
 */
export function wrapBulkHashedInputProducer<
  RT extends ResourceTypes,
  Meta extends HashedInputProducerMeta & { kind: "bulk" },
>(
  options: WrapProducerOptions<RT, Meta["validators"], Meta["params"]> & {
    hashedInputProducer: HashedInputProducer<Meta>;
  },
): [HashedInputProducerProblems<RT, Meta>] extends [never]
  ? WrappedBulkHashedInputProducer<
      RT,
      Meta["inputs"][Meta["covered"]],
      Meta["mintedIds"][Meta["covered"]],
      Meta["validators"],
      Meta["params"],
      Meta["errorType"]
    >
  : HashedInputProducerProblems<RT, Meta>;
export function wrapBulkHashedInputProducer<
  RT extends ResourceTypes,
  Input,
  MintedId extends SpecOf<RT>["id"],
  Validators extends AnyValidators = AnyValidators,
  Params extends AnyParams = AnyParams,
  ErrorType extends Error = Error,
>(
  options: WrapProducerOptions<RT, Validators, Params> & {
    hashInput: (input: Input) => MintedId | Promise<MintedId>;
    produce: (
      inputs: readonly ReadonlyDeep<Input>[],
    ) => Promise<
      readonly (
        | HashedInputProducerResult<
            Input,
            SpecForId<SpecOf<RT>, MintedId>,
            Validators,
            Params,
            SpecForId<SpecOf<RT>, MintedId>,
            SpecOf<RT>
          >
        | ErrorType
      )[]
    >;
  },
): WrappedBulkHashedInputProducer<
  RT,
  Input,
  MintedId,
  Validators,
  Params,
  ErrorType
>;
export function wrapBulkHashedInputProducer(
  options: WrapProducerOptions<ResourceTypes, AnyValidators, AnyParams> & {
    hashedInputProducer?: HashedInputProducer<HashedInputProducerMeta>;
    hashInput?: (input: never) => string | Promise<string>;
    produce?: (inputs: readonly never[]) => Promise<unknown>;
  },
): unknown {
  // `cache` both read directly and left in `producerOptions`; see the single
  // wrapper's note.
  const { cache } = options;
  const { hashedInputProducer, hashInput, produce, ...producerOptions } =
    options;
  type LooseBulkBranch = Omit<LooseBranch<never>, "produce"> & {
    produce: (
      inputs: readonly ReadonlyDeep<never>[],
    ) => Promise<
      (
        | HashedInputProducerResult<never, CacheSpec, AnyValidators, AnyParams>
        | Error
      )[]
    >;
  };

  const branchEntries = branchEntriesFor(
    "wrapBulkHashedInputProducer",
    hashedInputProducer,
    hashInput,
    produce as ((input: never) => Promise<unknown>) | undefined,
  ) as unknown as readonly (readonly [string | undefined, LooseBulkBranch])[];
  const registry = new InputRegistry<never>();
  const hashSupplementals = makeSupplementalHasher<never>(
    "wrapBulkHashedInputProducer",
    branchEntries,
    cache,
  );
  // Bound concurrent input hashing (see HASH_CONCURRENCY); shared across calls.
  const hashLimit = pLimit(HASH_CONCURRENCY);

  // Each branch's internal bulk producer recovers its inputs from the registry
  // (synchronously, while still registered) and hands the batch to `produce`.
  const internalProducerFor =
    (branch: LooseBulkBranch) => (reqs: readonly { readonly id: string }[]) => {
      const inputs = reqs.map((req) =>
        registry.get(req.id),
      ) as readonly ReadonlyDeep<never>[];
      return branch
        .produce(inputs)
        .then(async (results) =>
          Promise.all(
            results.map(async (result) =>
              result instanceof Error ? result : hashSupplementals(result),
            ),
          ),
        );
    };

  // Same routing choice and bridging casts as wrapHashedInputProducer's; see there.
  const wrapped =
    hashedInputProducer === undefined
      ? wrapBulkProducer(
          producerOptions,
          // Non-null: branchEntriesFor guarantees exactly one entry here.
          // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
          internalProducerFor(branchEntries[0]![1]) as unknown as Parameters<
            typeof wrapBulkProducer
          >[1],
        )
      : wrapBulkProducer(
          producerOptions,
          bulkProducerByIdType(
            cache.resourceTypes,
            Object.fromEntries(
              branchEntries.map(([name, branch]) => [
                name,
                internalProducerFor(branch),
              ]),
            ) as unknown as BulkProducersFor<
              ResourceTypes,
              string,
              AnyValidators,
              AnyParams,
              Error
            >,
          ),
        );

  return async (
    inputs: readonly never[],
    callOptions?: { directives?: ConsumerDirectives; signal?: AbortSignal },
  ) => {
    const signal = callOptions?.signal;
    signal?.throwIfAborted();

    // Route each input to its branch, keeping the branch itself (not just its
    // name) so hashing doesn't have to look it up again, then hash with that
    // branch's `hashInput` (bounded by `hashLimit`; `hashInput` may be sync --
    // p-limit handles that).
    const routed = zip2(
      inputs,
      inputs.map((input) =>
        findBranch("wrapBulkHashedInputProducer", branchEntries, input),
      ),
    );
    const ids = await Promise.all(
      routed.map(async ([input, [, branch]]) =>
        hashLimit(async () => branch.hashInput(input)),
      ),
    );
    signal?.throwIfAborted();

    const mintedIds = zip2(ids, routed);

    mintedIds.forEach(([id, [, [branchName]]]) => {
      checkMintedId(cache, branchName, id);
    });

    // A separate pass from the mint check, deliberately: `acquire` is only
    // balanced by the `finally` below, so a `checkMintedId` throw partway
    // through a combined loop would leave earlier ids acquired and never
    // released -- retaining their (potentially large) inputs for the process
    // lifetime.
    mintedIds.forEach(([id, [input]]) => {
      registry.acquire(id, input);
    });
    try {
      return await wrapped(
        // SAFETY: same bridge as wrapHashedInputProducer's (see there). The
        // call-level directives apply to every element (conditional spread
        // for `exactOptionalPropertyTypes`, as in the single variant).
        ids.map((id) => ({
          id,
          ...(callOptions?.directives === undefined
            ? {}
            : { directives: callOptions.directives }),
        })) as unknown as Parameters<typeof wrapped>[0],
        signal ? { signal } : undefined,
      );
    } finally {
      ids.forEach((id) => registry.release(id));
    }
  };
}

/**
 * Builds the function that turns a {@link HashedInputProducerResult} into the
 * plain {@link RequestPairedProducerResult} the underlying producer machinery
 * expects, by hashing each supplemental resource's `input` -- with the
 * producing branch's own `hashInput` -- into its storage id.
 *
 * The casts reconstruct the canonical result shape from the hashed-input result
 * (which differs only by how supplementals are keyed); TS can't track the
 * distributive transform across the `Omit`/re-add.
 */
function makeSupplementalHasher<Input>(
  wrapperName: string,
  branchEntries: readonly (readonly [
    string | undefined,
    Pick<LooseBranch<Input>, "matchesInput" | "hashInput">,
  ])[],
  cache: { readonly name: string; classify: (id: string) => string },
) {
  // One limiter shared across every call of the returned resolver.
  const limit = pLimit(HASH_CONCURRENCY);
  return async (
    result: HashedInputProducerResult<
      Input,
      CacheSpec,
      AnyValidators,
      AnyParams
    >,
  ): Promise<
    RequestPairedProducerResult<CacheSpec, AnyValidators, AnyParams>
  > => {
    const { supplementalResources, ...primary } = result;
    if (!supplementalResources || supplementalResources.length === 0) {
      return primary as unknown as RequestPairedProducerResult<
        CacheSpec,
        AnyValidators,
        AnyParams
      >;
    }

    return {
      ...primary,
      supplementalResources: await Promise.all(
        supplementalResources.map((resource) => {
          if (!("input" in resource)) {
            // Id-keyed: a plain ProducerResultResource for any registry type,
            // stored as-is (classified by its own id at store time, exactly
            // like plain producers' supplementals). Resolved OUTSIDE the
            // limiter -- there is nothing to hash, so queueing it would buy a
            // p-limit job and a tick to hand back the same object, and would
            // make id-keyed entries wait behind hashing ones.
            return Promise.resolve(resource);
          }
          // Input-keyed: route to a covered branch with the same
          // `matchesInput` selection applied to call-time inputs, hash
          // with THAT branch's `hashInput`, and mint-check eagerly -- so
          // a producer minting a bad supplemental id fails the invocation
          // loudly (named branch) instead of a silent store-time
          // rejection behind the wrappers' fire-and-forget store.
          const { input, ...rest } = resource;
          return limit(async () => {
            const [branchName, branch] = findBranch(
              wrapperName,
              branchEntries,
              input,
            );
            const id = await branch.hashInput(input as Input);
            checkMintedId(cache, branchName, id);
            return { ...rest, id };
          });
        }),
      ),
    } as unknown as RequestPairedProducerResult<
      CacheSpec,
      AnyValidators,
      AnyParams
    >;
  };
}

/**
 * A reference-counted registry mapping a derived cache id back to the input it
 * was hashed from, so the internal producer can recover the input on a miss.
 *
 * Reference counting (rather than a plain set/delete) is required because of
 * request collapsing: several concurrent calls for the same input derive the
 * same id and may share a single producer call. If the first caller to settle
 * deleted the entry, a still-in-flight caller (or the shared producer call)
 * could find it missing. Each call `acquire`s on the way in and `release`s in a
 * `finally`; the entry is dropped only when the last holder releases it. This
 * keeps the map bounded (unlike a process-lifetime map), since nothing is
 * retained past the calls that need it.
 *
 * Soundness of "release after the wrapped call settles": the underlying
 * `wrapProducer`/`wrapBulkProducer` invoke the producer *synchronously* while
 * the wrapped call is still running (including the fire-and-forget
 * stale-while-revalidate refresh), and our internal producer reads the input as
 * its first, synchronous step — so the read always happens before the release.
 */
class InputRegistry<Input> {
  private readonly entries = new Map<
    string,
    { input: Input; refCount: number }
  >();

  acquire(id: string, input: Input): void {
    const existing = this.entries.get(id);
    if (existing) {
      existing.refCount += 1;
      // Keep the latest input. For a given id the inputs are equal (barring an
      // astronomically unlikely hash collision), so this is a no-op in practice.
      existing.input = input;
    } else {
      this.entries.set(id, { input, refCount: 1 });
    }
  }

  get(id: string): Input {
    const entry = this.entries.get(id);
    if (entry === undefined) {
      throw new Error(
        `wrapHashedInputProducer: no input is registered for cache id "${id}". ` +
          "The producer was invoked for a key this wrapper did not produce, " +
          "which should be impossible (it would indicate a hash collision or a bug).",
      );
    }
    return entry.input;
  }

  release(id: string): void {
    const entry = this.entries.get(id);
    if (entry === undefined) {
      return;
    }
    entry.refCount -= 1;
    if (entry.refCount <= 0) {
      this.entries.delete(id);
    }
  }
}
