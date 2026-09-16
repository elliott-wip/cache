# cache

## Package Contents

This package provides a class, [`Cache`](./src/Cache.ts), that implements caching using a model inspired by [HTTP's](https://datatracker.ietf.org/doc/html/rfc9111). The HTTP model is already incredibly powerful, and this class generalizes and extends it further in some ways.

Even though this package borrows ideas from HTTP, it can be used to cache any kind of data from any source, not just HTTP responses. Its implementation assumes no HTTP particulars (like specific header formats).

Still, you must understand a number of concepts from HTTP's caching model in order to use this package effectively. Those are explained in ["Caching Model"](./docs/caching-model.md). Please read that, or many of the names and APIs in this package won't make sense.

### Backing stores

The `Cache` class can only function with a "backing store" that actually holds the cache's entries. There is a common `Store` interface (see the [types file](./src/types/06_Store.ts)) that all stores must implement. We currently have three backing stores, one that [holds items in memory](./src/stores/MemoryStore/MemoryStore.ts); one that [stores items in Postgres](./src/stores/PostgresStore/PostgresStore.ts); and one that [uses Sqlite](./src/stores/SqliteStore/SqliteStore.ts).

Note that not all backing stores will be able to store all kinds of data, although it's recommended that general-purpose stores be able to store any data that's JSON-serializable. Store implementations can communicate the type of data they support by adding a constraint on their first type parameter, e.g., a store with the signature `class MyStore<Spec extends CacheSpec<string, string[]>, ...>` is indicating that it can only store string arrays. Trying to use a store with a `Cache` instance parameterized for entries of different types will yield a type error.

Stores never see resource-type names or classification (below); those are entirely a `Cache` concern, so any `Store` implementation works unchanged.

### The resource-type registry

Every `Cache` is built over a **resource-type registry** ([`ResourceTypes`](./src/types/00_ResourceTypes.ts)): a record naming each *kind* of resource the cache can hold, pairing a runtime classifier for that kind's id sub-space (`matches`) with its content type (a type-level phantom). `Cache` takes a single options bag; `store`, `name` and `resourceTypes` are all required:

```ts
const storiesResourceTypes = {
  story: resourceType<Story>()({ matches: idStartsWith("story:") }),
  collection: resourceType<Story[]>()({ matches: idStartsWith("collection:") }),
} satisfies ResourceTypes;

const cache = new Cache({
  store: store,
  name: "stories", // names this cache instance in every diagnostics message
  resourceTypes: storiesResourceTypes,
});
```

The registry's `matches` guards must **partition the id space**: for every id the cache will ever see (requests, primary results, supplemental results, deletes), exactly one entry must match. `cache.classify(id)` evaluates *every* guard: zero matches throws `UnclassifiableIdError`, two or more throws `AmbiguousResourceTypeError` — fail loud over first-match-wins, so a registry overlap is caught the first time it occurs. Classification runs on every `get`/`getMany` request id, every stored entry id (primary *and* supplemental, all validated before anything persists — a producer minting a malformed id can't write a permanently unreadable row), and every `delete` id. Guards should be cheap (prefix checks preferred); ids must carry their type in-band: **an id must be classifiable by inspection**.

A cache with exactly **one** resource type doesn't have to name it — `singleTypeCacheOptions` builds the whole options bag, naming the type after the cache (override with `resourceTypeName`):

```ts
const cache = new Cache(
  singleTypeCacheOptions<TicketSchema>()({ store, name: "zendesk_ticket_schemas" }),
);
```

By default its sole type accepts every id, so classification never fails and the id space is `string`. Spread the result to set anything else `CacheOptions` accepts. The one thing it gives up is precision of the resource-type *name*: `classify()` returns `string` rather than a literal, which is the point of not having to pick one.

To narrow the id space below `string` — template-literal or branded ids flowing through to every request, producer, and entry type — pass a **real guard** as `validateId`:

```ts
const cache = new Cache(
  singleTypeCacheOptions<TicketSchema>()({
    store,
    name: "zendesk_ticket_schemas",
    validateId: idStartsWith("zendesk-ticket-schema:"),
  }),
);
```

`validateId` is *required* as soon as the id space is narrower than `string`, so a narrowed id type always has a runtime check behind it — naming a narrow `Id` by explicit type argument without supplying the guard is a compile error. The store needs no type argument to earn the narrowing: `Id` comes from the guard alone, and the store is only checked for coverage (see below).

Writing the one-entry registry out by hand stays equivalent, and is the better choice when you also want the literal resource-type name:

```ts
const cache = new Cache({
  store,
  name: "zendesk_ticket_schemas",
  resourceTypes: {
    ticket_schema: resourceType<TicketSchema>()({
      matches: idStartsWith("zendesk-ticket-schema:"),
    }),
  },
});
```

### The store may support more types than the cache

`Store` is invariant in its `Spec` — `Spec` appears both in `store()`/`delete()`'s parameters and in `get()`/`getMany()`'s return types — so a `Store<Wide>` is not assignable to a `Store<Narrow>`, even though for any id the narrow cache actually asks for, the wide store returns exactly the same thing. Since most stores are general-purpose, requiring an exact match would mean re-instantiating them with artificially narrowed type arguments.

So the cache doesn't require a match: it captures the store's own spec and only checks that it **covers** the registry. A general-purpose store just works, with no type arguments and no narrowing:

```ts
const store = new MemoryStore<SpecOf<typeof everyResourceTypeWeHave>>();
const cache = new Cache({ store, name: "stories", resourceTypes: storiesResourceTypes });
```

A store that does *not* cover the registry is still rejected, so this convenience can't silently accept a store that would fail at runtime on an id it doesn't handle. The same rule applies to `singleTypeCacheOptions`, so one general-purpose store can back several single-type caches:

```ts
const store = new MemoryStore<SpecOf<typeof everyResourceTypeWeHave>>();
const tickets = new Cache(
  singleTypeCacheOptions<TicketSchema>()({
    store,
    name: "zendesk_ticket_schemas",
    validateId: idStartsWith("zendesk-ticket-schema:"),
  }),
);
```

Nothing about the wider store leaks into the narrow cache: `tickets` still accepts only `zendesk-ticket-schema:` ids and still returns `TicketSchema`, not the store's content union.

### Per-id content typing (heterogeneous caches)

The cache's [`CacheSpec`](./src/types/00_CacheSpec.ts) union — which pairs each `id` type with the corresponding `content` type — is **derived from the registry** via `SpecOf`, so the two can never drift:

```ts
type StoriesCacheSpec = SpecOf<typeof storiesResourceTypes>;
// = CacheSpec<`story:${string}`, Story> | CacheSpec<`collection:${string}`, Story[]>

// Per-id content narrowing on read:
const storyRes = await cache.get({ id: "story:1", params: {}, directives: {} });
//    storyRes.usable?.content has type `Story | undefined`
const collRes = await cache.get({ id: "collection:top", params: {}, directives: {} });
//    collRes.usable?.content has type `Story[] | undefined`

// The (id, content) pair is also enforced on write:
await cache.store([
  { id: "story:1", content: aStory, directives: { freshUntilAge: 60 } },           // ok
  { id: "collection:top", content: [story1, story2], directives: { freshUntilAge: 60 } }, // ok
  // @ts-expect-error -- can't store a Story[] under a story:* id
  { id: "story:bad", content: [story1], directives: { freshUntilAge: 60 } },
]);
```

This is particularly useful when a producer that fetches a collection wants to additionally cache each individual entry (via `supplementalResources`), so that point lookups for each entry by id can also be served by the same cache.

## Key Files

- [`Cache.ts`](./src/Cache.ts): this defines the basic cache class. Note that the class's job is just to return whether/which previously-stored responses are usable to satisfy an incoming request. It does not handle things like making requests to the producer for new responses when no cached response is usable.

- [`MemoryStore.ts`](./src/stores/MemoryStore/MemoryStore.ts): a store for retaining cached data in memory, with a TTL and optional LRU eviction to cap memory usage.

- [`PostgresStore.ts`](./src/stores/PostgresStore/PostgresStore.ts): a store for retaining cached data in Postgres.

The package provides **four** functions for wrapping producers with a cache. They split along two axes — single vs. bulk, and "lookup" vs. "compute". All four take **exactly one producer**, and in every case per-resource-type dispatch is opt-in sugar that builds that one producer from several: `producerByIdType`/`bulkProducerByIdType` route by the request's id, while the compute wrappers' `hashedInputProducerByInputType`/`bulkHashedInputProducerByInputType` route by *input*. None of those four sugar helpers needs a cache:

- [`wrapProducer.ts`](./src/utils/wrapProducer.ts) — **`wrapProducer`**: the package's most important export, arguably. It takes an options bag holding the `Cache` instance, plus a producer (a function that returns data to cache), and returns a function that will use a cached value when a suitable one is available, but otherwise call through to the producer and store its return value for future requests.

  ```ts
  // A bare function covers the WHOLE registry. Sole-type caches stop here:
  const getVisits = wrapProducer({ cache }, async (req) => ({
    content: await fetchVisits(req.id),
    directives: { freshUntilAge: 60 },
  }));

  // Per-resource-type dispatch (and/or partial coverage) is opt-in sugar.
  // Note it takes the REGISTRY, not the cache -- routing by id type needs
  // nothing else, so this is a value you can build and test on its own:
  const getStories = wrapProducer({ cache }, producerByIdType(storiesResourceTypes, {
    story: async (req) => ({ content: await fetchStory(req.id), directives: { freshUntilAge: 60 } }),
    collection: async (req) => {
      const collection = await fetchCollection(req.id);
      return {
        content: collection,
        directives: { freshUntilAge: 60 },
        // supplementals may target any registry type, covered by this wrapper or not
        supplementalResources: collection.stories.map((s) => ({ id: `story:${s.id}`, content: s, directives: { freshUntilAge: 60 } })),
      };
    },
  }));
  ```

  A **bare function covers the whole registry**, and the compiler makes it prove that: its parameter must accept every registry id. **Partial coverage requires `producerByIdType`** (or `bulkProducerByIdType`), which turns a per-resource-type record into one function carrying its covered set in an optional symbol-keyed property (`coveredTypes`). That set is the wrapper's **coverage**, and it bounds the returned function's request type: requests for uncovered types are compile errors (and, if reached via casts, throw `NoProducerForResourceTypeError` *before any cache read*). A bare producer can't under-cover, so that error is unreachable for it. `producerByIdType` throws at construction on an empty record.

  Reaching for `producerByIdType` also buys per-branch type checking: each key narrows its sub-producer's `req.id` **and** its result to that resource type's content, so TypeScript checks the (id, content) correlation per branch. A single function's result type is the union over its covered ids, so it is free to return one variant's content for every id.

  Both helpers take the **resource-type registry** (`cache.resourceTypes`, or the literal you declared it from) rather than a cache: classifying an id to pick a sub-producer is all they do, and that needs the registry alone. So a by-id-type producer is a value in its own right — buildable, drivable and unit-testable before any cache exists, and reusable across caches over the same registry. It is also the inference site for the registry type, which is why it is a parameter rather than a type argument.

  When such a producer is driven **directly** and an id doesn't classify to exactly one covered type, it throws `UnroutableIdError` — cache-free, carrying `id`, `coveredResourceTypes`, and a `detail` discriminated on `reason` (`"uncovered"` / `"unclassifiable"` / `"ambiguous"`). Driven through a wrapper, the wrapper catches it and re-throws the equivalent cache-named error (`NoProducerForResourceTypeError`, `UnclassifiableIdError`, `AmbiguousResourceTypeError`), so a wrapped producer's observable errors are unchanged. Reaching it through a wrapper at all means the registry the producer was built from disagrees with the cache's own — the wrapper classifies first, against the cache's registry — so passing `cache.resourceTypes` (rather than another object of the same shape) is what keeps the two in step.

  Writing a multi-resource-type producer **by hand** instead? `classifyIdAgainst(resourceTypesEntries(resourceTypes), id)` is the same total classification the cache and these helpers use, exported for exactly that, and it returns an outcome rather than throwing so you can render your own errors.

  A type with no producer in any wrapper is legal and normal: its entries are written as other producers' supplemental resources (or direct `store()` calls) and read via `Cache.get` — the serve-if-present contract. Partial coverage also makes capability-scoped and split wrappers honest: a second `wrapProducer` call can cover a different subset of the same cache.

  Producers must be **side-effect-free reads of their resource type's origin**: invocations may be collapsed (shared with concurrent logical callers) and their results stored, so producer calls are never 1:1 with callers. Consumers that must reach the origin send bypass directives (`maxAge: 0`) — which skip the cache read entirely, guaranteeing producer contact (the result is still stored, and identical bypass requests still collapse). Producers whose response must not be stored return `storeFor: 0`.

  **When a repeat call pays for the producer, precisely.** The wrapped function resolves as soon as the producer has a value; the `cache.store()` of that value is fired but **not awaited**, so a call never waits on a store round trip and a failed write never fails the call (it is logged and dropped). Two mechanisms keep an identical repeat from running the producer again: calls that **overlap the producer invocation** share it (request collapsing, bounded by `collapseOverlappingRequestsTime`), and a call that arrives **after the producer resolved but before its store settled** is answered from the wrapper's own in-memory overlay of in-flight writes — consulted before the cache read, and honoring the request's params against the entry's `vary` and the caller's directives exactly as a stored entry would be, provided the entry would be a fresh hit had the write landed. So a plain sequential repeat (`await f(x); await f(x)`) runs the producer once, whatever the store's write latency (a worker-backed `SqliteStore` or a networked `PostgresStore` take milliseconds to land a write, which used to be long enough for the second call to miss). The overlay forgets an entry when its store call settles — on success (the store answers for it now) and on **failure** alike, so a failed write is never pinned: the next call reads the store, misses, and produces again. It is per wrapper instance and in-process (not shared across wrappers over the same cache, or across processes), and it does not cover the stale-while-revalidate / stale-if-error paths (an already-stale pending entry falls through to the store read). Such a serve reports `served-from-cache` on the fetch channel and, like a bypass request, emits no `read` message. `wrapHashedInputProducer` inherits all of this; the bulk wrappers do not yet have the overlay.

  **Durability is at `cache.close()`, not at the call's resolution.** Every store call a wrapper fires has been handed to the backing store by the time the wrapped call resolves, and the bundled stores' `close()` drains pending writes before resolving. A short-lived process (a script, a one-shot job) must therefore `await cache.close()` before exiting, or its last writes can be lost. This is a property of the stores rather than of the wrappers, but the wrappers are where it is easy to forget: nothing in the call's return tells you the write has landed.

- [`wrapBulkProducer.ts`](./src/utils/wrapBulkProducer.ts) — **`wrapBulkProducer`**: the same idea for producers that resolve many requests at once. It looks each request up in the cache and calls the underlying producer only for the ones that missed (or need background revalidation).

  Because the producer is a single function, a bare one receives the **full** set of requests to produce — mixed resource types and all — in ONE call, so it can optimize across them (a single upstream call covering several types, cross-type dedup, a join). That means one producer invocation per collapse window, not one per resource type. `bulkProducerByIdType` opts into per-type dispatch instead: it splits the batch by classified resource type, calls each sub-producer once with its own type-pure slice, reassembles the results positionally into the caller's order, and isolates a sub-producer's failure — a rejection or a synchronous throw — into that type's result slots as `Error` elements, so it never discards a sibling type's results.

  `wrapProducer` and `wrapBulkProducer` both treat the cache **`id` as a reference to a mutable entity**: the caller already has the id, and the cached value is whatever that entity currently is — a function of the `id` and time (e.g. "the current `User` for `user:123`"). The id is the natural cache key, so the producer receives it directly.

  Both per-resource-type helpers live in [`producerByIdType.ts`](./src/utils/producerByIdType.ts) — they need neither wrapper at runtime, and no `Cache`. The errors both wrappers raise are in [`producer-errors.ts`](./src/utils/producer-errors.ts).

  Like the two "compute" wrappers below, both take **one options bag** followed by the producer: the cache is the bag's required `cache` field (`wrapProducer({ cache, ...options }, producer)`), not a separate positional argument, so all four wrappers are called the same way and no call site has to pass `undefined` in the middle to reach the producer.

  Upgrading from 1.6.0: the wrapped function returned by either wrapper is now a **plain function**. 1.6.0 attached the `Cache` instance to it as a `cache` property ("for convenience, e.g. in closing it"); 2.0 does not, since the cache is a field of the options bag the caller already holds. Hold on to that instance yourself if you were reaching for `wrapped.cache` (e.g. `wrapped.cache.close()` → `cache.close()`).

- [`wrapHashedInputProducer.ts`](./src/utils/wrapHashedInputProducer.ts) — **`wrapHashedInputProducer`** and **`wrapBulkHashedInputProducer`**: the "compute" counterparts to the two above, for when the cached value is not an entity looked up by id but an expensive-to-compute **function of some input** — value = `f(input)`, reused whenever the same input recurs (e.g. an LLM extraction over a chunk of text). Here a hash of the input is the natural cache key, but the producer wants the original, un-hashed input to do the work.

  You pass **one options bag**. For a cache with one resource type, the two functions are the whole contract — no record, no map, no type arguments:

  ```ts
  const extract = wrapHashedInputProducer({
    cache,
    hashInput: (input: Chunk) => `extract:${sha256(canonicalize(input))}` as const,
    produce: async (input) => ({ content: await runLlm(input), directives: { freshUntilAge: Infinity } }),
  });
  ```

  For several resource types, [`hashedInputProducerByInputType`](./src/utils/hashedInputProducerByInputType.ts) builds a **hashed-input producer** — one `.when` per covered type, dispatching on the input. It takes **no cache**: a hashed-input producer is a value in its own right, buildable (and reusable) before any cache exists, and checked against a cache's registry only where the two are wired together.

  ```ts
  const hashedInputProducer = hashedInputProducerByInputType<{
    story: HashedInputVariant<StoryInput, Story>;
    collection: HashedInputVariant<CollInput, Story[]>;
  }>()
    .when((i): i is StoryInput => i.kind === "story", {
      name: "story",
      hashInput: (input) => `extract:story:${input.id}`,        // input: StoryInput
      produce: async (input) => ({ content: await extractStory(input), directives }),
    })
    .when((i): i is CollInput => i.kind === "collection", {
      name: "collection",
      hashInput: (input) => `extract:collection:${input.ids.join(",")}`,
      produce: async (input) => ({ content: await extractAll(input), directives }),
    })
    .build();

  const extract = wrapHashedInputProducer({ cache, hashedInputProducer });
  ```

  The variant map declares each resource type's input and the content computed from it, so a branch is validated where it is written: `produce` must return that variant's output, `input` is narrowed to that variant's input (no casts), and a second `.when` for an already-covered variant is rejected rather than silently shadowed. `name` selects the variant and the guard is only the runtime dispatcher — which is why two variants may be computed from the *same* input type (a summary and a translation of one story) and why a guard may prove a *subtype* of the declared input.

  Wiring a producer to a cache is what brings the registry in: each branch's minted-id type is checked against its resource type's id space, each variant's declared output against that type's content, and every variant name against the registry — reported as named, per-branch problems. `hashInput` must mint ids the branch's own registry guard accepts, and that is *also* checked at runtime by classifying each minted id (naming the branch), which is the backstop for a mint arriving through a cast or an untyped boundary. Each way it can fail gets its own error: the mint matched no resource type (`UnclassifiableIdError`) or several (`AmbiguousResourceTypeError`), or exactly one that isn't the branch's own (`MintedIdResourceTypeMismatchError`, carrying `branch` and `classifiedResourceType` — classification succeeded there, so the mismatch indicts the branch's `hashInput` rather than the registry's guards). For accept-everything registries — a `singleTypeCacheOptions` cache with no `validateId` — the runtime check is vacuous and the compile-time one carries the weight.

  The wrapper keeps each input around (reference-counted, so it survives request collapsing without leaking) just long enough to hand it to `produce` on a miss, and otherwise behaves like `wrapProducer`/`wrapBulkProducer` — including call-time consumer `directives` (e.g. `compute(input, { directives: { maxAge: 0 } })` forces a recompute). A hashed-input producer may also return `supplementalResources` in two forms: **input-keyed** (`{ input, … }` — routed to a covered branch via the same guard selection as call-time inputs, hashed with that branch's `hashInput`, and mint-checked eagerly, so a later `compute(thatInput)` is a hit; in a hashed-input producer the `input` and `content` must come from the SAME variant) or **id-keyed** (`{ id, … }` — a plain resource for *any* registry type, classified by its own id at store time, exactly like plain producers' supplementals; typing these needs the registry's id space, so a cache-free hashed-input producer must be declared with its `SpecOf` to return them).

  `wrapBulkHashedInputProducer` is the same, with `bulkHashedInputProducerByInputType` for the multi-type form: each branch's `produce` computes a batch of that branch's missed inputs and returns a result (or an `Error`) per input, aligned by index.

## Diagnostics channels

The package publishes telemetry on four [`diagnostics_channel`](https://nodejs.org/api/diagnostics_channel.html) channels (see [`diagnostics.ts`](./src/diagnostics.ts)). Every message carries `{ cache, resourceType }` attribution — the cache instance's `name` and the classified resource-type name — so subscribers can build per-cache, per-resource-type metrics with no name threading. (`produce` covers a whole batch, so it carries `cache` at the top level and each element's `resourceType` inside `requests[]`.) Each channel exports its name constant, its message type, and a typed channel object (`TypedChannel`).

| Channel                       | Cardinality                                                                             | Message highlights                                                                                                                                                                                                                                                        |
| ----------------------------- | --------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `@zingage/cache:read`        | One per cache lookup (`Cache.get`; per request for `getMany`) — including direct callers | `found`: `"usable"` \| `"usable-while-revalidate"` \| `"usable-if-error"` \| `"none"`, evaluated against the request's directives; plus `vary` — identifying the selected entry's variant — on the three entry-selecting results, and omitted on the others. Bypass requests never appear (they skip the read). A read the store *failed* publishes `found: "read-failed"` with the `error` — one message per request in the failed call — and *then* the error propagates, which keeps the channel a total denominator; only an **aborted** read emits nothing. |
| `@zingage/cache:fetch`       | One per call of a wrapped producer (per request element, for bulk), at settlement        | `disposition`: `served-from-cache`, `served-stale-while-revalidating`, `served-stale-after-error`, `served-from-producer`, `producer-error`, or `aborted`; `collapsed` (the settlement rode an in-flight invocation; cache-served settlements report `false` even when they attached a background revalidation as a rider); cache-served dispositions carry `vary` (identifying the served entry's variant) and producer-path dispositions carry `directivesImpliedBypass`.       |
| `@zingage/cache:produce`     | One per actual producer invocation (foreground misses AND background revalidations)      | `trigger`: `"miss"` \| `"revalidation"` \| `"bypass"` (the invocation's initiating cause; riders never re-label); `requests[]` (`{resourceType, resourceId}`, in the order the producer received them — a bulk batch MAY span resource types, so read each element's own `resourceType`); `collapsedCallerCount`; `outcome`; `durationMs`. Producer latency and error rate live here. |
| `@zingage/cache:store-entry` | One per entry passed to `Cache.store()` (supplementals attributed to their own type)     | `resourceId`, `vary`, `validators`, `relationshipToExistingStoredData` (`"is-new"` \| `"unchanged"` \| `"changed"` \| `undefined`).                                                                                                                                       |

A cache's unit of storage is the `(id, vary)` **pair**, where `vary` is the normalized paramName→value map that defines a **variant**: a producer that varies on request params writes one entry per variant under one id. So the three channels that can identify a variant all report the same normalized `vary` object — `store-entry` for the entry written, `read`/`fetch` for the entry a lookup *selected* — which is what lets a subscriber attribute serves to the exact stored variant (e.g. how many times one version was served before a newer one replaced it) instead of smearing every variant's serves onto whichever variant stored first. Where no variant was served the key is **omitted**, never published as `{}`, because `{}` identifies a real variant — the only one a non-varying producer ever writes. That covers `found: "none"`/`"read-failed"` on `read`, and `producer-error`/`aborted` on `fetch` — and also `served-from-producer`, which did hand the caller a value: that value came from the origin, not a stored variant, and the variant it lands in is reported on `store-entry` if and when the write succeeds.

Upgrading from 1.6.0, whose single channel reported a `status` string: `hit` → `served-from-cache`, `stale_while_revalidate` → `served-stale-while-revalidating`, and `miss`/`bypass` → `served-from-producer` (now distinguished by `directivesImpliedBypass`). `served-stale-after-error`, `producer-error`, and `aborted` are new dispositions that 1.6.0 could not report at all.

`fetch` and `produce` are the two spans of one story with different subjects and cardinalities: a `fetch` is the consumer-side span (one per logical request); a `produce` is the origin-side span (one per invocation). N collapsed callers ride one invocation, one bulk invocation covers many requests, a stale-while-revalidate refresh settles *after* its triggering fetch already shipped, and an `aborted` fetch settles *before* its invocation does (the collapsed producer call keeps running and stores in the background).
