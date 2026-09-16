import { expect } from "chai";
import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, it, mock } from "node:test";
import { setTimeout as delay } from "timers/promises";
import type { ReadonlyDeep } from "type-fest";

import { expectRejection } from "../../test/v2AcceptanceHelpers.js";
import Cache from "../Cache.js";
import {
  idStartsWith,
  MemoryStore,
  resourceType,
  type ResourceTypes,
  type SpecOf,
} from "../index.js";
import type { AnyParams, AnyValidators, Entry } from "../types/index.js";
import {
  bulkHashedInputProducerByInputType,
  hashedInputProducerByInputType,
  type HashedInputVariant,
} from "./hashedInputProducerByInputType.js";
import {
  MintedIdResourceTypeMismatchError,
  wrapBulkHashedInputProducer,
  wrapHashedInputProducer,
} from "./wrapHashedInputProducer.js";

// Hashed-input wrappers take ONE options bag: `{ cache, hashInput, produce }` for a
// single resource type, or `{ cache, hashedInputProducer }` for several. A hashing
// producer is built by `hashedInputProducerByInputType` with no cache at all.
const testRegistry = {
  computed: resourceType<string>()({
    matches: (id): id is string => typeof id === "string",
  }),
} satisfies ResourceTypes;
const testCacheOptions = {
  name: "wrap-computing-producer-test",
  resourceTypes: testRegistry,
};

type Input = { text: string };

const hashInput = (input: Input): string => `computed:${input.text}`;

const result = (content: string) => ({
  content,
  directives: { freshUntilAge: 100 },
});

const contentOf = (
  entry: Entry<SpecOf<typeof testRegistry>, AnyValidators, AnyParams> | Error,
): string => {
  if (entry instanceof Error) {
    throw entry;
  }
  return entry.content;
};

describe("wrapHashedInputProducer", () => {
  let cache: Cache<typeof testRegistry>;

  beforeEach(() => {
    cache = new Cache({
      store: new MemoryStore<SpecOf<typeof testRegistry>>(),
      ...testCacheOptions,
    });
  });

  afterEach(async () => cache.close());

  it("calls the producer with the full input (not an id) and caches by input hash", async () => {
    const producer = mock.fn(async (input: Input) =>
      result(input.text.toUpperCase()),
    );
    const compute = wrapHashedInputProducer({
      cache,
      hashInput,
      produce: producer,
    });

    const first = await compute({ text: "hello" });
    expect(first.content).to.eq("HELLO");
    expect(producer.mock.calls[0]?.arguments[0]).to.deep.eq({ text: "hello" });

    const second = await compute({ text: "hello" });
    expect(second.content).to.eq("HELLO");
    expect(producer.mock.callCount()).to.eq(1);
  });

  it("an immediate sequential repeat is a hit while the store is in flight: computes once", async () => {
    // A MemoryStore whose writes park until released, holding open the window
    // between `produce` resolving and its store landing. No timers.
    class GatedStore extends MemoryStore<SpecOf<typeof testRegistry>> {
      readonly gates: (() => void)[] = [];
      override async store(
        ...args: Parameters<MemoryStore<SpecOf<typeof testRegistry>>["store"]>
      ) {
        await new Promise<void>((resolve) => this.gates.push(resolve));
        return super.store(...args);
      }
    }
    const store = new GatedStore();
    const gatedCache = new Cache({ store, ...testCacheOptions });
    try {
      const produce = mock.fn(async (input: ReadonlyDeep<Input>) =>
        result(`computed:${input.text}`),
      );
      const compute = wrapHashedInputProducer({
        cache: gatedCache,
        collapseOverlappingRequestsTime: 0,
        hashInput,
        produce,
      });

      const first = await compute({ text: "a" });
      expect(store.gates).to.have.lengthOf(1); // the write is parked
      const second = await compute({ text: "a" });
      expect(second.content).to.eq(first.content);
      expect(produce.mock.callCount()).to.eq(1);
    } finally {
      store.gates.splice(0).forEach((release) => release());
      await gatedCache.close();
    }
  });

  it("recomputes for a different input", async () => {
    const producer = mock.fn(async (input: Input) =>
      result(input.text.toUpperCase()),
    );
    const compute = wrapHashedInputProducer({
      cache,
      hashInput,
      produce: producer,
    });

    await compute({ text: "a" });
    await compute({ text: "b" });
    expect(producer.mock.callCount()).to.eq(2);
  });

  it("supports an async hashInput", async () => {
    const producer = mock.fn(async (input: Input) => result(input.text));
    const compute = wrapHashedInputProducer({
      cache,
      hashInput: async (input: Input) => {
        await delay(1);
        return `computed:${input.text}`;
      },
      produce: producer,
    });

    await compute({ text: "x" });
    await compute({ text: "x" });
    expect(producer.mock.callCount()).to.eq(1);
  });

  it("keeps the input registered for concurrent un-collapsed calls (reference counting)", async () => {
    const producer = mock.fn(async (input: Input) => {
      await delay(20);
      return result(input.text);
    });
    const compute = wrapHashedInputProducer({
      cache,
      collapseOverlappingRequestsTime: 0,
      hashInput,
      produce: producer,
    });

    const results = await Promise.all([
      compute({ text: "q" }),
      compute({ text: "q" }),
    ]);
    expect(results.map((r) => r.content)).to.deep.eq(["q", "q"]);
  });

  it("stores supplementals under their input's hash, so a later compute() hits", async () => {
    const producer = mock.fn(async (input: Input) => ({
      content: input.text.toUpperCase(),
      directives: { freshUntilAge: 100 },
      supplementalResources:
        input.text === "primary"
          ? [
              {
                input: { text: "side" },
                content: "SIDE",
                directives: { freshUntilAge: 100 },
              },
            ]
          : [],
    }));
    const compute = wrapHashedInputProducer({
      cache,
      hashInput,
      produce: producer,
    });

    await compute({ text: "primary" });
    expect(producer.mock.callCount()).to.eq(1);

    const side = await compute({ text: "side" });
    expect(side.content).to.eq("SIDE");
    expect(producer.mock.callCount()).to.eq(1);
  });

  it("call-time directives: maxAge 0 forces recomputation of a memoized input", async () => {
    const producer = mock.fn(async (input: Input) =>
      result(input.text.toUpperCase()),
    );
    const compute = wrapHashedInputProducer({
      cache,
      hashInput,
      produce: producer,
    });

    expect((await compute({ text: "hello" })).content).to.eq("HELLO");
    expect((await compute({ text: "hello" })).content).to.eq("HELLO");
    expect(producer.mock.callCount()).to.eq(1);

    // Bypass directives skip the cache read and force producer contact,
    // exactly as on a plain wrapped producer.
    const forced = await compute(
      { text: "hello" },
      { directives: { maxAge: 0 } },
    );
    expect(forced.content).to.eq("HELLO");
    expect(producer.mock.callCount()).to.eq(2);

    // Plain calls still serve from cache (the memoized entry stands).
    expect((await compute({ text: "hello" })).content).to.eq("HELLO");
    expect(producer.mock.callCount()).to.eq(2);
  });
});

describe("wrapBulkHashedInputProducer", () => {
  let cache: Cache<typeof testRegistry>;

  beforeEach(() => {
    cache = new Cache({
      store: new MemoryStore<SpecOf<typeof testRegistry>>(),
      ...testCacheOptions,
    });
  });

  afterEach(async () => cache.close());

  it("computes only the missing inputs and aligns results to the input order", async () => {
    const producer = mock.fn(async (inputs: readonly Input[]) =>
      inputs.map((input) => result(input.text.toUpperCase())),
    );
    const compute = wrapBulkHashedInputProducer({
      cache,
      hashInput,
      produce: producer,
    });

    await compute([{ text: "b" }]);
    expect(producer.mock.callCount()).to.eq(1);
    expect(producer.mock.calls[0]?.arguments[0]).to.deep.eq([{ text: "b" }]);

    const results = await compute([
      { text: "a" },
      { text: "b" },
      { text: "c" },
    ]);
    expect(results.map(contentOf)).to.deep.eq(["A", "B", "C"]);
    expect(producer.mock.callCount()).to.eq(2);
    expect(producer.mock.calls[1]?.arguments[0]).to.deep.eq([
      { text: "a" },
      { text: "c" },
    ]);
  });

  it("returns an empty array for no inputs without calling the producer", async () => {
    const producer = mock.fn(async (inputs: readonly Input[]) =>
      inputs.map((input) => result(input.text)),
    );
    const compute = wrapBulkHashedInputProducer({
      cache,
      hashInput,
      produce: producer,
    });

    const results = await compute([]);
    expect(results).to.deep.eq([]);
    expect(producer.mock.callCount()).to.eq(0);
  });

  it("serves a fully-cached batch without calling the producer", async () => {
    const producer = mock.fn(async (inputs: readonly Input[]) =>
      inputs.map((input) => result(input.text.toUpperCase())),
    );
    const compute = wrapBulkHashedInputProducer({
      cache,
      hashInput,
      produce: producer,
    });

    await compute([{ text: "a" }, { text: "b" }]);
    const results = await compute([{ text: "a" }, { text: "b" }]);
    expect(results.map(contentOf)).to.deep.eq(["A", "B"]);
    expect(producer.mock.callCount()).to.eq(1);
  });

  it("call-time directives apply to every element: maxAge 0 recomputes a memoized batch", async () => {
    const producer = mock.fn(async (inputs: readonly Input[]) =>
      inputs.map((input) => result(input.text.toUpperCase())),
    );
    const compute = wrapBulkHashedInputProducer({
      cache,
      hashInput,
      produce: producer,
    });

    expect(
      (await compute([{ text: "a" }, { text: "b" }])).map(contentOf),
    ).to.deep.eq(["A", "B"]);
    expect(
      (await compute([{ text: "a" }, { text: "b" }])).map(contentOf),
    ).to.deep.eq(["A", "B"]);
    expect(producer.mock.callCount()).to.eq(1);

    const forced = await compute([{ text: "a" }, { text: "b" }], {
      directives: { maxAge: 0 },
    });
    expect(forced.map(contentOf)).to.deep.eq(["A", "B"]);
    expect(producer.mock.callCount()).to.eq(2);
  });
});

// --- heterogeneous branches: correlated (input kind, id space, content) ---
//
// Includes cross-variant supplemental coverage: supplementals may be
// input-keyed for ANY covered branch — routed by `matchesInput`, hashed by the
// routed branch's `hashInput` — or id-keyed for any registry type.

type Story = { id: string; title: string };
type StoryInput = { kind: "story"; id: string };
type CollInput = { kind: "collection"; ids: string[] };
type VInput = StoryInput | CollInput;

const storiesRegistry = {
  story: resourceType<Story>()({ matches: idStartsWith("extract:story:") }),
  collection: resourceType<Story[]>()({
    matches: idStartsWith("extract:collection:"),
  }),
} satisfies ResourceTypes;

const makeStory = (id: string): Story => ({ id, title: `Story ${id}` });
// Each guard proves only its OWN branch's input. Under the old record-keyed
// wrapper both had to claim the whole `VInput` union, which is why every
// `produce` body needed an `input as StoryInput` cast to get anywhere.
const isStory = (input: VInput): input is StoryInput => input.kind === "story";
const isCollection = (input: VInput): input is CollInput =>
  input.kind === "collection";
type StoryVariants = {
  story: HashedInputVariant<StoryInput, Story>;
  collection: HashedInputVariant<CollInput, Story[]>;
};

/** `contentOf` for the two-variant registry, whose content types differ. */
const storyContentOf = (
  entry:
    | Entry<SpecOf<typeof storiesRegistry>, AnyValidators, AnyParams>
    | Error,
): Story | Story[] => {
  if (entry instanceof Error) {
    throw entry;
  }
  return entry.content;
};

describe("hashed-input wrappers with heterogeneous branches", () => {
  let cache: Cache<typeof storiesRegistry>;

  beforeEach(() => {
    cache = new Cache({
      store: new MemoryStore<SpecOf<typeof storiesRegistry>>(),
      name: "computing-branches-test",
      resourceTypes: storiesRegistry,
    });
  });

  afterEach(async () => cache.close());

  it("dispatches by matchesInput and returns the right (typed) content per branch", async () => {
    const compute = wrapHashedInputProducer({
      cache,
      hashedInputProducer: hashedInputProducerByInputType<StoryVariants>()
        .when(isStory, {
          name: "story",
          hashInput: (input) => `extract:story:${input.id}` as const,
          produce: async (input) => ({
            content: makeStory(input.id),
            directives: { freshUntilAge: 100 },
          }),
        })
        .when(isCollection, {
          name: "collection",
          hashInput: (input) =>
            `extract:collection:${input.ids.join(",")}` as const,
          produce: async (input) => ({
            content: input.ids.map(makeStory),
            directives: { freshUntilAge: 100 },
          }),
        })
        .build(),
    });

    const story = await compute({ kind: "story", id: "1" });
    expect(story.content).to.deep.eq(makeStory("1"));

    const collection = await compute({ kind: "collection", ids: ["1", "2"] });
    expect(collection.content).to.deep.eq([makeStory("1"), makeStory("2")]);
  });

  it("caches same-branch supplementals under their input's hash across branch dispatch", async () => {
    const storyProduce = mock.fn(async (input: ReadonlyDeep<StoryInput>) => ({
      content: makeStory(input.id),
      directives: { freshUntilAge: 100 },
      supplementalResources: [
        // A related story this computation produced as a byproduct: same
        // branch (story), keyed by ITS input, so compute({...related}) hits.
        // No widening to the union: a supplemental's input names exactly one
        // variant, and carries that variant's content.
        {
          input: {
            kind: "story",
            id: `${input.id}-related`,
          } satisfies StoryInput,
          content: makeStory(`${input.id}-related`),
          directives: { freshUntilAge: 100 },
        },
      ],
    }));
    const collectionProduce = mock.fn(
      async (input: ReadonlyDeep<CollInput>) => ({
        content: input.ids.map(makeStory),
        directives: { freshUntilAge: 100 },
      }),
    );

    const compute = wrapHashedInputProducer({
      cache,
      hashedInputProducer: hashedInputProducerByInputType<StoryVariants>()
        .when(isStory, {
          name: "story",
          hashInput: (input) => `extract:story:${input.id}` as const,
          produce: storyProduce,
        })
        .when(isCollection, {
          name: "collection",
          hashInput: (input) =>
            `extract:collection:${input.ids.join(",")}` as const,
          produce: collectionProduce,
        })
        .build(),
    });

    const s1 = await compute({ kind: "story", id: "1" });
    expect(s1.content).to.deep.eq(makeStory("1"));
    expect(storyProduce.mock.callCount()).to.eq(1);

    // The supplemental was cached under its own (story) input's hash, so
    // computing it directly never invokes the producer again...
    const related = await compute({ kind: "story", id: "1-related" });
    expect(related.content).to.deep.eq(makeStory("1-related"));
    expect(storyProduce.mock.callCount()).to.eq(1);

    // ...and collection dispatch still works independently.
    const coll = await compute({ kind: "collection", ids: ["9"] });
    expect(coll.content).to.deep.eq([makeStory("9")]);
    expect(collectionProduce.mock.callCount()).to.eq(1);
  });

  it("input-keyed supplementals may target OTHER covered branches: routed by matchesInput, hashed by the routed branch's hashInput", async () => {
    const collectionProduce = mock.fn(async (input: ReadonlyDeep<VInput>) => ({
      content: (input as CollInput).ids.map(makeStory),
      directives: { freshUntilAge: 100 },
    }));
    const compute = wrapHashedInputProducer({
      cache,
      hashedInputProducer: hashedInputProducerByInputType<StoryVariants>()
        .when(isStory, {
          name: "story",
          hashInput: (input) => `extract:story:${input.id}` as const,
          produce: async (input) => ({
            content: makeStory(input.id),
            directives: { freshUntilAge: 100 },
            supplementalResources: [
              // A byproduct belonging to the OTHER branch: keyed by a
              // collection input, so the wrapper must route it via
              // matchesInput to the collection branch and hash it with ITS
              // hashInput. The (input, content) pair is checked against that
              // branch's variant, so a Story here would not compile.
              {
                input: {
                  kind: "collection",
                  ids: [input.id],
                } satisfies CollInput,
                content: [makeStory(input.id)],
                directives: { freshUntilAge: 100 },
              },
            ],
          }),
        })
        .when(isCollection, {
          name: "collection",
          hashInput: (input) =>
            `extract:collection:${input.ids.join(",")}` as const,
          produce: collectionProduce,
        })
        .build(),
    });

    const s7 = await compute({ kind: "story", id: "7" });
    expect(s7.content).to.deep.eq(makeStory("7"));

    // The cross-branch supplemental is a hit for the collection branch: its
    // producer is never invoked.
    const coll = await compute({ kind: "collection", ids: ["7"] });
    expect(coll.content).to.deep.eq([makeStory("7")]);
    expect(collectionProduce.mock.callCount()).to.eq(0);
  });

  it("id-keyed supplementals may target ANY registry type -- even one no wrapper covers (restored plain-producer parity)", async () => {
    const snapshotRegistry = {
      story: resourceType<Story>()({ matches: idStartsWith("extract:story:") }),
      collection: resourceType<Story[]>()({
        matches: idStartsWith("extract:collection:"),
      }),
      // Never covered by any wrapper: written only as a supplemental,
      // read via serve-if-present Cache.get.
      site_snapshot: resourceType<string>()({
        matches: idStartsWith("snapshot:"),
      }),
    } satisfies ResourceTypes;
    const snapshotCache = new Cache({
      store: new MemoryStore<SpecOf<typeof snapshotRegistry>>(),
      name: "computing-id-keyed-suppl-test",
      resourceTypes: snapshotRegistry,
    });
    try {
      // Id-keyed supplementals need the registry's id space, which a cache-free
      // builder only has if it is declared -- hence the SpecOf argument.
      const compute = wrapHashedInputProducer({
        cache: snapshotCache,
        hashedInputProducer: hashedInputProducerByInputType<
          StoryVariants,
          AnyValidators,
          AnyParams,
          SpecOf<typeof snapshotRegistry>
        >()
          .when(isStory, {
            name: "story",
            hashInput: (input) => `extract:story:${input.id}` as const,
            produce: async (input) => ({
              content: makeStory(input.id),
              directives: { freshUntilAge: 100 },
              supplementalResources: [
                // Id-keyed: a plain ProducerResultResource, stored under its
                // own natural id and classified at store time.
                {
                  id: `snapshot:${input.id}` as `snapshot:${string}`,
                  content: `raw-html-${input.id}`,
                  directives: { freshUntilAge: 100 },
                },
              ],
            }),
          })
          .when(isCollection, {
            name: "collection",
            hashInput: (input) =>
              `extract:collection:${input.ids.join(",")}` as const,
            produce: async (input) => ({
              content: input.ids.map(makeStory),
              directives: { freshUntilAge: 100 },
            }),
          })
          .build(),
      });

      const s3 = await compute({ kind: "story", id: "3" });
      expect(s3.content).to.deep.eq(makeStory("3"));

      // The supplemental store is fire-and-forget behind the wrapper, so
      // poll via direct Cache.get (which never triggers producers).
      const readSnapshot = async () =>
        (
          await snapshotCache.get({
            id: "snapshot:3",
            params: {},
            directives: {},
          })
        ).usable;
      let entry = await readSnapshot();
      let attempts = 0;
      while (entry === undefined && attempts < 200) {
        await delay(5);
        attempts += 1;
        entry = await readSnapshot();
      }
      expect(entry?.content).to.eq("raw-html-3");
    } finally {
      await snapshotCache.close();
    }
  });

  it("a supplemental input matching no covered branch rejects the invocation loudly", async () => {
    const compute = wrapHashedInputProducer({
      cache,
      hashedInputProducer: hashedInputProducerByInputType<StoryVariants>()
        .when(isStory, {
          name: "story",
          hashInput: (input) => `extract:story:${input.id}` as const,
          produce: async (input) => ({
            content: makeStory(input.id),
            directives: { freshUntilAge: 100 },
            supplementalResources: [
              {
                // Matches neither branch's guard. The type system rejects this
                // honestly, so it is cast through: the runtime routing failure
                // is what is under test.
                input: { kind: "neither" } as unknown as StoryInput,
                content: makeStory("x"),
                directives: { freshUntilAge: 100 },
              },
            ],
          }),
        })
        .when(isCollection, {
          name: "collection",
          hashInput: (input) =>
            `extract:collection:${input.ids.join(",")}` as const,
          produce: async (input) => ({
            content: input.ids.map(makeStory),
            directives: { freshUntilAge: 100 },
          }),
        })
        .build(),
    });

    await assert.rejects(() => compute({ kind: "story", id: "9" }), {
      message: /no branch matched/,
    });
  });

  // --- the bulk wrapper over several branches, end to end ---
  //
  // The bulk path routes and hashes per ITEM and then delegates to one
  // per-resource-type bulk producer each, so batch partitioning, result
  // alignment, per-item errors, and cross-branch supplementals are all its own
  // wiring rather than the single wrapper's.

  it("bulk: partitions a mixed batch by matchesInput, aligns results to input order, and recomputes only misses", async () => {
    const storyProduce = mock.fn(
      async (inputs: readonly ReadonlyDeep<StoryInput>[]) =>
        inputs.map((input) => ({
          content: makeStory(input.id),
          directives: { freshUntilAge: 100 },
        })),
    );
    const collectionProduce = mock.fn(
      async (inputs: readonly ReadonlyDeep<CollInput>[]) =>
        inputs.map((input) => ({
          content: input.ids.map(makeStory),
          directives: { freshUntilAge: 100 },
        })),
    );
    const computeAll = wrapBulkHashedInputProducer({
      cache,
      hashedInputProducer: bulkHashedInputProducerByInputType<StoryVariants>()
        .when(isStory, {
          name: "story",
          hashInput: (input) => `extract:story:${input.id}` as const,
          produce: storyProduce,
        })
        .when(isCollection, {
          name: "collection",
          hashInput: (input) =>
            `extract:collection:${input.ids.join(",")}` as const,
          produce: collectionProduce,
        })
        .build(),
    });

    const first = await computeAll([
      { kind: "story", id: "1" },
      { kind: "collection", ids: ["1", "2"] },
      { kind: "story", id: "2" },
    ]);
    expect(first.map(storyContentOf)).to.deep.eq([
      makeStory("1"),
      [makeStory("1"), makeStory("2")],
      makeStory("2"),
    ]);
    // Each branch's producer saw ONLY its own inputs, in batch.
    expect(storyProduce.mock.calls[0]?.arguments[0]).to.deep.eq([
      { kind: "story", id: "1" },
      { kind: "story", id: "2" },
    ]);
    expect(collectionProduce.mock.calls[0]?.arguments[0]).to.deep.eq([
      { kind: "collection", ids: ["1", "2"] },
    ]);

    // Only the new story is a miss: the story branch is re-invoked with just
    // it, and the collection branch is not invoked at all.
    const second = await computeAll([
      { kind: "collection", ids: ["1", "2"] },
      { kind: "story", id: "3" },
      { kind: "story", id: "1" },
    ]);
    expect(second.map(storyContentOf)).to.deep.eq([
      [makeStory("1"), makeStory("2")],
      makeStory("3"),
      makeStory("1"),
    ]);
    expect(storyProduce.mock.calls[1]?.arguments[0]).to.deep.eq([
      { kind: "story", id: "3" },
    ]);
    expect(storyProduce.mock.callCount()).to.eq(2);
    expect(collectionProduce.mock.callCount()).to.eq(1);
  });

  it("bulk: one branch's per-input error is returned in place, leaving the batch's other items served", async () => {
    const boom = new Error("story 2 could not be extracted");
    const computeAll = wrapBulkHashedInputProducer({
      cache,
      hashedInputProducer: bulkHashedInputProducerByInputType<StoryVariants>()
        .when(isStory, {
          name: "story",
          hashInput: (input) => `extract:story:${input.id}` as const,
          produce: async (inputs) =>
            inputs.map((input) =>
              input.id === "2"
                ? boom
                : {
                    content: makeStory(input.id),
                    directives: { freshUntilAge: 100 },
                  },
            ),
        })
        .when(isCollection, {
          name: "collection",
          hashInput: (input) =>
            `extract:collection:${input.ids.join(",")}` as const,
          produce: async (inputs) =>
            inputs.map((input) => ({
              content: input.ids.map(makeStory),
              directives: { freshUntilAge: 100 },
            })),
        })
        .build(),
    });

    const results = await computeAll([
      { kind: "story", id: "1" },
      { kind: "story", id: "2" },
      { kind: "collection", ids: ["5"] },
    ]);
    expect(results[1]).to.eq(boom);
    expect(storyContentOf(results[0]!)).to.deep.eq(makeStory("1"));
    expect(storyContentOf(results[2]!)).to.deep.eq([makeStory("5")]);
  });

  it("bulk: input-keyed supplementals route across branches and hash with the routed branch's hashInput", async () => {
    const collectionProduce = mock.fn(
      async (inputs: readonly ReadonlyDeep<CollInput>[]) =>
        inputs.map((input) => ({
          content: input.ids.map(makeStory),
          directives: { freshUntilAge: 100 },
        })),
    );
    const computeAll = wrapBulkHashedInputProducer({
      cache,
      hashedInputProducer: bulkHashedInputProducerByInputType<StoryVariants>()
        .when(isStory, {
          name: "story",
          hashInput: (input) => `extract:story:${input.id}` as const,
          produce: async (inputs) =>
            inputs.map((input) => ({
              content: makeStory(input.id),
              directives: { freshUntilAge: 100 },
              supplementalResources: [
                {
                  input: {
                    kind: "collection",
                    ids: [input.id],
                  } satisfies CollInput,
                  content: [makeStory(input.id)],
                  directives: { freshUntilAge: 100 },
                },
              ],
            })),
        })
        .when(isCollection, {
          name: "collection",
          hashInput: (input) =>
            `extract:collection:${input.ids.join(",")}` as const,
          produce: collectionProduce,
        })
        .build(),
    });

    await computeAll([{ kind: "story", id: "7" }]);

    const coll = await computeAll([{ kind: "collection", ids: ["7"] }]);
    expect(coll.map(storyContentOf)).to.deep.eq([[makeStory("7")]]);
    expect(collectionProduce.mock.callCount()).to.eq(0);
  });

  // --- a SOLE `.when` branch is still guarded ---
  //
  // A guard may prove a subtype of its variant's declared input, so `compute`
  // accepts every `StoryInput` while this guard accepts only some of them --
  // no cast needed to reach an input the sole branch rejects.
  const isIdentifiedStory = (input: VInput): input is StoryInput =>
    input.kind === "story" && input.id !== "";

  const soleStoryBranch = (
    produce: (input: ReadonlyDeep<StoryInput>) => Promise<{
      content: Story;
      directives: { freshUntilAge: number };
    }>,
  ) =>
    hashedInputProducerByInputType<StoryVariants>()
      .when(isIdentifiedStory, {
        name: "story",
        hashInput: (input) => `extract:story:${input.id}` as const,
        produce,
      })
      .build();

  it("a sole `.when` branch still consults its guard: a rejected input is never produced", async () => {
    const produce = mock.fn(async (input: ReadonlyDeep<StoryInput>) => ({
      content: makeStory(input.id),
      directives: { freshUntilAge: 100 },
    }));
    const compute = wrapHashedInputProducer({
      cache,
      hashedInputProducer: soleStoryBranch(produce),
    });

    expect((await compute({ kind: "story", id: "1" })).content).to.deep.eq(
      makeStory("1"),
    );

    await assert.rejects(() => compute({ kind: "story", id: "" }), {
      message: /no branch matched/,
    });
    // Still 1: the rejected input was not produced, so nothing was stored
    // under the id its `hashInput` would have minted.
    expect(produce.mock.callCount()).to.eq(1);
  });

  it("a sole `.when` branch is guarded in the bulk wrapper too", async () => {
    const produce = mock.fn(
      async (inputs: readonly ReadonlyDeep<StoryInput>[]) =>
        inputs.map((input) => ({
          content: makeStory(input.id),
          directives: { freshUntilAge: 100 },
        })),
    );
    const computeAll = wrapBulkHashedInputProducer({
      cache,
      hashedInputProducer: bulkHashedInputProducerByInputType<StoryVariants>()
        .when(isIdentifiedStory, {
          name: "story",
          hashInput: (input) => `extract:story:${input.id}` as const,
          produce,
        })
        .build(),
    });

    await assert.rejects(
      () =>
        computeAll([
          { kind: "story", id: "1" },
          { kind: "story", id: "" },
        ]),
      { message: /no branch matched/ },
    );
    // The whole batch is rejected during routing, before any production.
    expect(produce.mock.callCount()).to.eq(0);
  });

  it("a supplemental the sole `.when` branch's guard rejects also rejects the invocation", async () => {
    const compute = wrapHashedInputProducer({
      cache,
      hashedInputProducer: soleStoryBranch(async (input) => ({
        content: makeStory(input.id),
        directives: { freshUntilAge: 100 },
        supplementalResources: [
          {
            // Its own branch's guard rejects this, so there is no branch whose
            // `hashInput` may mint its id -- the sole branch's must not be
            // borrowed for it.
            input: { kind: "story", id: "" } satisfies StoryInput,
            content: makeStory("x"),
            directives: { freshUntilAge: 100 },
          },
        ],
      })),
    });

    await assert.rejects(() => compute({ kind: "story", id: "9" }), {
      message: /no branch matched/,
    });
  });

  it("a guard that THROWS counts as a non-match, so a later branch still claims the input", async () => {
    const collectionProduce = mock.fn(
      async (input: ReadonlyDeep<CollInput>) => ({
        content: input.ids.map(makeStory),
        directives: { freshUntilAge: 100 },
      }),
    );
    const compute = wrapHashedInputProducer({
      cache,
      hashedInputProducer: hashedInputProducerByInputType<StoryVariants>()
        // Reads a field only story inputs have, the way a guard that parses or
        // dereferences its input does; on a collection input that throws.
        .when(
          (input): input is StoryInput =>
            (input as StoryInput).id.startsWith("s"),
          {
            name: "story",
            hashInput: (input) => `extract:story:${input.id}` as const,
            produce: async (input) => ({
              content: makeStory(input.id),
              directives: { freshUntilAge: 100 },
            }),
          },
        )
        .when(isCollection, {
          name: "collection",
          hashInput: (input) =>
            `extract:collection:${input.ids.join(",")}` as const,
          produce: collectionProduce,
        })
        .build(),
    });

    const coll = await compute({ kind: "collection", ids: ["1"] });
    expect(coll.content).to.deep.eq([makeStory("1")]);
    expect(collectionProduce.mock.callCount()).to.eq(1);
  });

  it("when NO branch matches, a guard's throw surfaces as the routing error's cause", async () => {
    const guardError = new Error("could not read the input");
    const compute = wrapHashedInputProducer({
      cache,
      hashedInputProducer: hashedInputProducerByInputType<StoryVariants>()
        .when(
          (input): input is StoryInput => {
            void input;
            throw guardError;
          },
          {
            name: "story",
            hashInput: (input) => `extract:story:${input.id}` as const,
            produce: async (input) => ({
              content: makeStory(input.id),
              directives: { freshUntilAge: 100 },
            }),
          },
        )
        .build(),
    });

    const thrown = await expectRejection(() =>
      compute({ kind: "story", id: "1" }),
    );
    expect((thrown as Error).message).to.match(/no branch matched/);
    // Without this the guard's own failure is the one thing a caller cannot
    // recover from the routing error.
    expect((thrown as Error).cause).to.eq(guardError);
  });

  it("several throwing guards aggregate into the routing error's cause", async () => {
    const storyGuardError = new Error("story guard could not read the input");
    const collectionGuardError = new Error("collection guard likewise");
    const compute = wrapHashedInputProducer({
      cache,
      hashedInputProducer: hashedInputProducerByInputType<StoryVariants>()
        .when(
          (input): input is StoryInput => {
            void input;
            throw storyGuardError;
          },
          {
            name: "story",
            hashInput: (input) => `extract:story:${input.id}` as const,
            produce: async (input) => ({
              content: makeStory(input.id),
              directives: { freshUntilAge: 100 },
            }),
          },
        )
        .when(
          (input): input is CollInput => {
            void input;
            throw collectionGuardError;
          },
          {
            name: "collection",
            hashInput: (input) =>
              `extract:collection:${input.ids.join(",")}` as const,
            produce: async (input) => ({
              content: input.ids.map(makeStory),
              directives: { freshUntilAge: 100 },
            }),
          },
        )
        .build(),
    });

    const thrown = await expectRejection(() =>
      compute({ kind: "story", id: "1" }),
    );
    expect((thrown as Error).cause).to.be.instanceOf(AggregateError);
    expect(((thrown as Error).cause as AggregateError).errors).to.deep.eq([
      storyGuardError,
      collectionGuardError,
    ]);
  });

  it("a supplemental whose routed branch mints a misclassified id rejects loudly, naming that branch", async () => {
    const compute = wrapHashedInputProducer({
      cache,
      hashedInputProducer: hashedInputProducerByInputType<StoryVariants>()
        .when(isStory, {
          name: "story",
          hashInput: (input) => `extract:story:${input.id}` as const,
          produce: async (input) => ({
            content: makeStory(input.id),
            directives: { freshUntilAge: 100 },
            supplementalResources: [
              // Routed to the collection branch, whose buggy hashInput below
              // mints a story-prefixed id.
              {
                input: {
                  kind: "collection",
                  ids: ["9"],
                } satisfies CollInput,
                content: [makeStory("9")],
                directives: { freshUntilAge: 100 },
              },
            ],
          }),
        })
        .when(isCollection, {
          name: "collection",
          // BUG under test: mints an id in the story branch's id space. Both
          // the builder and the wrapper reject this honestly, so the buggy
          // value is cast through -- the runtime mint-check is the net for
          // exactly these type-level bypasses.
          hashInput: (input) =>
            `extract:story:${input.ids.join(",")}` as unknown as `extract:collection:${string}`,
          produce: async (input) => ({
            content: input.ids.map(makeStory),
            directives: { freshUntilAge: 100 },
          }),
        })
        .build(),
    });

    const thrown = await expectRejection(() =>
      compute({ kind: "story", id: "9" }),
    );
    // Its own error, not a re-worded UnclassifiableIdError: the mint DID
    // classify, to exactly one type -- just not this branch's. So the culprit is
    // the branch's `hashInput`, not the registry's guards, and both facts needed
    // to say so are fields rather than message text, which is what lets a
    // handler route "a producer is wired to the wrong branch" apart from "this
    // registry doesn't cover this id space".
    assert.ok(thrown instanceof MintedIdResourceTypeMismatchError);
    expect(thrown.branch).to.equal("collection");
    expect(thrown.classifiedResourceType).to.equal("story");
    expect(thrown.id).to.equal("extract:story:9");
    expect(thrown.cacheName).to.equal(cache.name);
    expect(thrown.message).to.include('branch "collection"');
  });
});
