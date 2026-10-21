import { isEqual } from "es-toolkit";
import fc from "fast-check";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { AnyValidatorsArb } from "../../test/arbitraries/02_Validators.js";
import {
  postgresEnvironmentIsConfigured,
  postgresStoreFixture,
} from "../../test/fixtures.js";
import type {
  AnyValidators,
  CacheSpec,
  Entry,
  NormalizedParams,
  NormalizedProducerDirectives,
  NormalizedVary,
  Store,
} from "../types/index.js";
// The change-detection result types are the source of truth in 06_Store.ts per
// the shared contract; import them from there directly so this suite compiles
// against the contract regardless of whether they're also re-exported from the
// public barrel.
import type {
  StoreEntryRelationship,
  StoreEntryResult,
} from "../types/06_Store.js";
import MemoryStore from "./MemoryStore/MemoryStore.js";
import type { PostgresStoreSupportedParams } from "./PostgresStore/PostgresStore.js";
import SqliteStore from "./SqliteStore/SqliteStore.js";

// ---------------------------------------------------------------------------
// Test spec + shared literals. Copied (not imported) from
// Store.conformance.test.ts so this suite stays disjoint from the file the
// implementer edits. NormalizedVary/NormalizedParams/NormalizedProducerDirectives
// are `Tagged` types with no public constructor, so building literals requires
// the same minimal `as`-bridging the sibling conformance file uses.
// ---------------------------------------------------------------------------
type TestContent = {
  value: string;
  nested?: { count: number; labels: string[] };
};
type TestSpec = CacheSpec<string, TestContent>;
type TestParams = PostgresStoreSupportedParams & {
  format?: string;
  lang?: string;
  audience?: string;
};
type TestStore = Store<TestSpec, AnyValidators, TestParams>;

type StoreFixture = {
  store: TestStore;
  cleanup: () => Promise<void>;
};

const matchingParams = {
  format: "json",
  lang: "en",
  audience: "public",
} as NormalizedParams<TestParams>;

const varyOnFormat = { format: "json" } as NormalizedVary<TestParams>;
const varyOnLanguage = { lang: "en" } as NormalizedVary<TestParams>;
const directives = { freshUntilAge: 60 } as NormalizedProducerDirectives;

// Convenience so tests can read the reported relationship out of the parallel
// result array without repeating the optional-chain everywhere.
function relationshipsOf(
  results: readonly StoreEntryResult[],
): (StoreEntryRelationship | undefined)[] {
  return results.map((r) => r?.relationshipToExistingStoredData);
}

describe("Store change-detection conformance", () => {
  describe("MemoryStore", () => {
    defineChangeDetectionConformance(async () => {
      const store = new MemoryStore<TestSpec, AnyValidators, TestParams>();
      return { store, cleanup: async () => store.close() };
    }, { entriesExpire: true });
  });

  describe("SqliteStore", () => {
    defineChangeDetectionConformance(async () => {
      const directory = await mkdtemp(join(tmpdir(), "cache-sqlite-cd-"));
      const store = new SqliteStore<TestSpec, AnyValidators, TestParams>({
        databasePath: join(directory, "cache.sqlite"),
        readWorkerCount: 2,
      });
      return {
        store,
        cleanup: async () => {
          await store.close();
          await rm(directory, { recursive: true, force: true });
        },
      };
    }, { entriesExpire: true });
  });

  describe(
    "PostgresStore",
    postgresEnvironmentIsConfigured()
      ? {}
      : { skip: "Postgres environment variables are not configured" },
    () => {
      defineChangeDetectionConformance(async () => {
        const fixture = postgresStoreFixture();
        return {
          store: fixture.postgresStore as unknown as TestStore,
          cleanup: async () => fixture.cleanup(),
        };
      });
    },
  );

  // Generator self-audit. This is store-independent -- it only exercises the
  // shared arbitrary + oracle -- so it lives at the top level rather than
  // per-store. It proves the property-test generator actually produces every
  // relationship outcome (the "dimension-accounting" guard: a future edit that
  // silently stops generating, say, `unchanged` becomes a red test instead of
  // an invisible coverage hole).
  it("generator exercises is-new, unchanged, changed, and omitted outcomes", () => {
    const samples = fc.sample(operationsArb, { numRuns: 3000, seed: 0xc0ffee });
    const seen = new Set<string>();
    for (const calls of samples) {
      const oracle = makeOracle();
      for (const call of calls) {
        for (const rel of oracle.expectAndApply(call)) {
          seen.add(String(rel));
        }
      }
    }
    assert.ok(seen.has("is-new"), "generator never produced an is-new case");
    assert.ok(seen.has("unchanged"), "generator never produced an unchanged case");
    assert.ok(seen.has("changed"), "generator never produced a changed case");
    assert.ok(
      seen.has("undefined"),
      "generator never produced an empty-validators (omitted) case",
    );
  });
});

function defineChangeDetectionConformance(
  createFixture: () => Promise<StoreFixture>,
  opts?: {
    /**
     * Whether this store expires entries per `maxStoreForSeconds`. Stores that
     * do must not treat an expired (even if still physically present) record
     * as existing stored data. PostgresStore never expires records -- every
     * record it holds is returned by `get` -- so the expiry test doesn't apply
     * to it.
     */
    entriesExpire?: boolean;
  },
) {
  // -- Empty validators => field omitted, regardless of what's stored --------

  it("omits the relationship when incoming validators are empty (fresh slot)", async () => {
    await withStore(createFixture, async (store) => {
      const results = await store.store([
        {
          entry: makeEntry("id", "a", varyOnFormat, { validators: {} }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(results)[0], undefined);
    });
  });

  it("omits the relationship for empty validators even when the slot already holds data", async () => {
    await withStore(createFixture, async (store) => {
      // Seed a non-empty entry, then overwrite with empty validators. The
      // empty-validators rule wins over the fact that the slot changed.
      await store.store([
        {
          entry: makeEntry("id", "a", varyOnFormat, {
            validators: { etag: "seed" },
          }),
          maxStoreForSeconds: 60,
        },
      ]);

      const overwriteEmpty = await store.store([
        {
          entry: makeEntry("id", "b", varyOnFormat, { validators: {} }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(overwriteEmpty)[0], undefined);

      // And again, now that the slot holds an empty-validators entry.
      const overwriteEmptyAgain = await store.store([
        {
          entry: makeEntry("id", "c", varyOnFormat, { validators: {} }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(overwriteEmptyAgain)[0], undefined);
    });
  });

  // -- Validators are interpreted as their JSON-serialized form --------------
  //
  // `undefined` isn't a legal validator value (AnyValidators values are JSON),
  // but it's expressible from JS, and JSON serialization silently drops
  // `undefined`-valued keys. Every store must interpret validators as what
  // survives serialization -- including stores that hold or receive the raw
  // in-memory object -- or the same input classifies differently per store.

  it("ignores undefined-valued keys when deciding whether validators are empty", async () => {
    await withStore(createFixture, async (store) => {
      // Deliberately type-violating input; see the section comment above.
      const effectivelyEmpty = {
        etag: undefined,
      } as unknown as AnyValidators;

      const results = await store.store([
        {
          entry: makeEntry("id", "a", varyOnFormat, {
            validators: effectivelyEmpty,
          }),
          maxStoreForSeconds: 60,
        },
      ]);

      // Nothing survives serialization, so there's nothing to compare on: the
      // relationship must be omitted (not "is-new").
      assert.equal(relationshipsOf(results)[0], undefined);
    });
  });

  it("ignores undefined-valued keys when comparing validators, on both sides", async () => {
    await withStore(createFixture, async (store) => {
      // Deliberately type-violating input; see the section comment above.
      const dirtyOne = {
        etag: "1",
        extra: undefined,
      } as unknown as AnyValidators;
      const dirtyTwo = {
        etag: "2",
        extra: undefined,
      } as unknown as AnyValidators;

      // Incoming side: a clean seed, then a re-store whose only difference is
      // an undefined-valued key. Serialization-wise they're identical.
      await store.store([
        {
          entry: makeEntry("incoming", "a", varyOnFormat, {
            validators: { etag: "1" },
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      const incomingSide = await store.store([
        {
          entry: makeEntry("incoming", "b", varyOnFormat, {
            validators: dirtyOne,
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(incomingSide)[0], "unchanged");

      // Stored side: seed with the undefined-valued key, then re-store clean.
      const dirtySeed = await store.store([
        {
          entry: makeEntry("stored", "a", varyOnFormat, {
            validators: dirtyTwo,
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(dirtySeed)[0], "is-new");

      const storedSide = await store.store([
        {
          entry: makeEntry("stored", "b", varyOnFormat, {
            validators: { etag: "2" },
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(storedSide)[0], "unchanged");
    });
  });

  // -- is-new granularity is per (id, vary) slot, not per resource id --------

  it("reports is-new for a slot that held no live entry", async () => {
    await withStore(createFixture, async (store) => {
      const results = await store.store([
        {
          entry: makeEntry("fresh", "a", varyOnFormat, {
            validators: { etag: "x" },
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(results)[0], "is-new");
    });
  });

  it("reports is-new for a brand-new variant of an already-stored resource id", async () => {
    await withStore(createFixture, async (store) => {
      // First variant of the resource is is-new...
      const first = await store.store([
        {
          entry: makeEntry("id", "fmt", varyOnFormat, {
            validators: { etag: "fmt" },
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(first)[0], "is-new");

      // ...and so is a *different* variant of the SAME id: change detection is
      // keyed per (id, vary) slot, not per resource id.
      const secondVariant = await store.store([
        {
          entry: makeEntry("id", "lang", varyOnLanguage, {
            validators: { etag: "lang" },
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(secondVariant)[0], "is-new");

      // Re-storing the original variant now sees a live entry in its slot.
      const restoreOriginal = await store.store([
        {
          entry: makeEntry("id", "fmt2", varyOnFormat, {
            validators: { etag: "fmt-changed" },
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(restoreOriginal)[0], "changed");
    });
  });

  // -- Expired records are not "live", so they're not the comparison target --

  if (opts?.entriesExpire === true) {
    it("reports is-new when the slot's stored record has expired, even if it's still physically present", async () => {
      await withStore(createFixture, async (store) => {
        // 0.125s is exactly representable in binary, so `* 1000` stays an
        // integer and can't trip SQLite's STRICT INTEGER expires_at column.
        const validators = { etag: "same" };
        const first = await store.store([
          {
            entry: makeEntry("id", "a", varyOnFormat, { validators }),
            maxStoreForSeconds: 0.125,
          },
        ]);
        assert.equal(relationshipsOf(first)[0], "is-new");

        // Wait until the record is expired. Nothing has forced a cleanup pass,
        // so a store may well still hold the record physically -- but `get`
        // would no longer return it, so per the contract the slot holds no
        // LIVE entry and change detection must not compare against it: storing
        // the SAME validators again must be "is-new", not "unchanged".
        await sleep(500);

        const second = await store.store([
          {
            entry: makeEntry("id", "b", varyOnFormat, { validators }),
            maxStoreForSeconds: 60,
          },
        ]);
        assert.equal(relationshipsOf(second)[0], "is-new");
      });
    });
  }

  // -- unchanged: order-independent, structural deep-equality ----------------

  it("reports unchanged for validators that deep-equal the stored ones (key order + nesting independent)", async () => {
    await withStore(createFixture, async (store) => {
      await store.store([
        {
          entry: makeEntry("id", "a", varyOnFormat, {
            validators: { a: "1", b: "2", meta: { x: 1, y: [1, 2] } },
          }),
          maxStoreForSeconds: 60,
        },
      ]);

      // Same content, top-level keys and nested keys reordered.
      const reordered = await store.store([
        {
          entry: makeEntry("id", "b", varyOnFormat, {
            validators: { meta: { y: [1, 2], x: 1 }, b: "2", a: "1" },
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(reordered)[0], "unchanged");
    });
  });

  // -- changed ---------------------------------------------------------------

  it("reports changed when validators differ from the stored ones", async () => {
    await withStore(createFixture, async (store) => {
      await store.store([
        {
          entry: makeEntry("id", "a", varyOnFormat, {
            validators: { etag: "1" },
          }),
          maxStoreForSeconds: 60,
        },
      ]);

      const changedValue = await store.store([
        {
          entry: makeEntry("id", "b", varyOnFormat, {
            validators: { etag: "2" },
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(changedValue)[0], "changed");

      // A nested-value difference (same shape) is also "changed".
      await store.store([
        {
          entry: makeEntry("id", "c", varyOnFormat, {
            validators: { meta: { x: 1, y: [1, 2] } },
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      const nestedChanged = await store.store([
        {
          entry: makeEntry("id", "d", varyOnFormat, {
            validators: { meta: { x: 2, y: [1, 2] } },
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(nestedChanged)[0], "changed");
    });
  });

  // -- A -> B -> A birth-date scenario ---------------------------------------

  it("compares against the newest-birth-date stored entry, not set membership (A -> B -> A)", async () => {
    await withStore(createFixture, async (store) => {
      const validatorsA = { etag: "A" };
      const validatorsB = { etag: "B" };

      // Birth date = date - initialAge*1000. We drive birth dates purely
      // through the entries' date/initialAge fields -- never by mocking the
      // clock -- so B is born strictly after A.
      const storeA1 = await store.store([
        {
          entry: makeEntry("id", "A1", varyOnFormat, {
            validators: validatorsA,
            date: new Date("2024-01-01T00:00:00.000Z"),
            initialAge: 0,
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(storeA1)[0], "is-new");

      const storeB = await store.store([
        {
          entry: makeEntry("id", "B", varyOnFormat, {
            validators: validatorsB,
            date: new Date("2024-06-01T00:00:00.000Z"),
            initialAge: 0,
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(storeB)[0], "changed");

      // Store A again. Even though validators A were previously a member of
      // this slot's history, the comparison is against the currently-stored
      // (newest-birth-date) entry, which is B -- so A vs B is "changed".
      const storeA2 = await store.store([
        {
          entry: makeEntry("id", "A2", varyOnFormat, {
            validators: validatorsA,
            date: new Date("2024-03-01T00:00:00.000Z"),
            initialAge: 0,
          }),
          maxStoreForSeconds: 60,
        },
      ]);
      assert.equal(relationshipsOf(storeA2)[0], "changed");

      const stored = await store.get("id", matchingParams);
      assert.equal(stored.length, 1);
      assert.deepEqual(stored[0]?.validators, validatorsA);
    });
  });

  // -- Result array is parallel to input (length + order), mixed batch -------

  it("returns a result array parallel to the input entries for a mixed batch", async () => {
    await withStore(createFixture, async (store) => {
      // Seed two distinct slots.
      await store.store([
        {
          entry: makeEntry("seeded-unchanged", "s", varyOnFormat, {
            validators: { etag: "s0" },
          }),
          maxStoreForSeconds: 60,
        },
        {
          entry: makeEntry("seeded-changed", "s", varyOnFormat, {
            validators: { etag: "s1" },
          }),
          maxStoreForSeconds: 60,
        },
      ]);

      // One store() call whose entries each target a DISTINCT slot, so there's
      // no in-call dedup ambiguity -- each entry's relationship is its own.
      const results = await store.store([
        {
          entry: makeEntry("brand-new", "n", varyOnFormat, {
            validators: { etag: "n" },
          }),
          maxStoreForSeconds: 60,
        },
        {
          entry: makeEntry("seeded-unchanged", "u", varyOnFormat, {
            validators: { etag: "s0" },
          }),
          maxStoreForSeconds: 60,
        },
        {
          entry: makeEntry("seeded-changed", "c", varyOnFormat, {
            validators: { etag: "s1-different" },
          }),
          maxStoreForSeconds: 60,
        },
        {
          entry: makeEntry("empty-one", "e", varyOnFormat, { validators: {} }),
          maxStoreForSeconds: 60,
        },
      ]);

      assert.equal(results.length, 4);
      assert.deepEqual(relationshipsOf(results), [
        "is-new",
        "unchanged",
        "changed",
        undefined,
      ]);
    });
  });

  // -- Within-a-single-store()-call duplicates -------------------------------

  it("dedupes in-call duplicates by newest birth date; the winner reports against the pre-call snapshot and the dropped duplicate is omitted", async () => {
    await withStore(createFixture, async (store) => {
      const validatorsA = { etag: "A" };
      const validatorsB = { etag: "B" };

      // Pre-call stored value for the slot.
      await store.store([
        {
          entry: makeEntry("id", "seed", varyOnFormat, {
            validators: validatorsA,
          }),
          maxStoreForSeconds: 60,
        },
      ]);

      // A single call with two entries for the SAME slot. The FIRST entry has
      // the newest birth date, so it must win under the contract's uniform
      // newest-birth-date dedup rule -- a last-write-wins implementation would
      // persist (and report for) the second entry instead. Crucially the
      // winner's validators equal the *pre-call* stored value A, so the winner
      // must report "unchanged", proving the comparison is against the
      // pre-call snapshot and NOT against the other in-call entry B (which
      // would give "changed").
      const results = await store.store([
        {
          entry: makeEntry("id", "winner", varyOnFormat, {
            validators: validatorsA,
            date: new Date("2024-05-01T00:00:00.000Z"),
            initialAge: 0,
          }),
          maxStoreForSeconds: 60,
        },
        {
          entry: makeEntry("id", "loser", varyOnFormat, {
            validators: validatorsB,
            date: new Date("2024-02-01T00:00:00.000Z"),
            initialAge: 0,
          }),
          maxStoreForSeconds: 60,
        },
      ]);

      assert.equal(results.length, 2);
      const rels = relationshipsOf(results);

      // Winner (index 0, newest birth date) reports against the pre-call
      // snapshot A; the dropped duplicate persists nothing and is omitted.
      assert.deepEqual(rels, ["unchanged", undefined]);

      // The winner is what actually persisted, even though it came FIRST.
      const stored = await store.get("id", matchingParams);
      assert.equal(stored.length, 1);
      assert.deepEqual(stored[0]?.validators, validatorsA);
      assert.equal(stored[0]?.content.value, "winner");
    });
  });

  // -- Property: random store sequences vs a naive newest-per-slot oracle ----

  it("matches a naive newest-validators-per-slot oracle over random store sequences", async () => {
    await withStore(createFixture, async (store) => {
      await fc.assert(
        fc.asyncProperty(
          fc.record({ runId: fc.uuid(), calls: operationsArb }),
          async ({ runId, calls }) => {
            const oracle = makeOracle();
            for (const call of calls) {
              const expected = oracle.expectAndApply(call);
              const inputs = call.map((op) => {
                const { id, vary } = slotSpec(runId, op.slotIdx);
                return {
                  entry: makeEntry(id, "v", vary, {
                    validators: op.validators,
                    date: op.date,
                    initialAge: op.initialAge,
                  }),
                  maxStoreForSeconds: 60,
                };
              });

              const results = await store.store(inputs);
              assert.equal(results.length, inputs.length);
              assert.deepEqual(relationshipsOf(results), expected);
            }

            // Each iteration namespaces its slots under a unique runId; delete
            // them so long-lived stores don't accumulate across iterations.
            await store.delete(`${runId}:a`);
            await store.delete(`${runId}:b`);
          },
        ),
        { numRuns: 100 },
      );
    });
  });
}

// ===========================================================================
// Property-test generator + oracle (shared by the property and the audit test)
// ===========================================================================

// A single input entry within one store() call, addressed by an abstract slot
// index (see `slotSpec` for the id/vary mapping).
type OracleOp = {
  slotIdx: number;
  validators: AnyValidators;
  date: Date;
  initialAge: number;
};
// One store() call is a list of ops.
type OracleCall = OracleOp[];

// Four abstract slots: two resource ids x two variants. This exercises the
// per-(id, vary) slot granularity (same id + different vary is a different
// slot) while keeping the slot space small enough that `unchanged` collisions
// actually occur.
const SLOT_COUNT = 4;

function slotSpec(
  runId: string,
  slotIdx: number,
): { id: string; vary: NormalizedVary<TestParams> } {
  return {
    id: `${runId}:${slotIdx < 2 ? "a" : "b"}`,
    vary: slotIdx % 2 === 0 ? varyOnFormat : varyOnLanguage,
  };
}

// Small pool of validator objects, drawn from heavily so that repeats (and
// therefore `unchanged`) are frequent. Deliberately includes:
//   - `{}` (empty) so the omit rule is exercised inside the property;
//   - a superset pair ({etag:v1} vs {etag:v1, weak:true}) => "changed";
//   - key-reordered duplicates (top-level and nested) that are isEqual-equal,
//     so an order-SENSITIVE implementation bug would diverge from the
//     order-independent oracle and be caught.
const validatorsPool = [
  {},
  { etag: "v1" },
  { etag: "v2" },
  { etag: "v1", weak: true },
  { weak: true, etag: "v1" },
  { lastModified: "2020-01-01T00:00:00Z", meta: { rev: 1, tags: ["a", "b"] } },
  { meta: { tags: ["a", "b"], rev: 1 }, lastModified: "2020-01-01T00:00:00Z" },
] satisfies AnyValidators[];

// Values that don't survive a JSON round-trip stably would make the oracle
// (which holds the in-memory object) and the store (which round-trips through
// JSON) legitimately disagree; exclude them. In practice `AnyValidatorsArb`'s
// `fc.jsonValue()` never emits Infinity/NaN/undefined and isEqual treats -0/0
// as equal, so this filter almost never fires -- it's a documented safety net.
const roundTrips = (v: AnyValidators): boolean =>
  isEqual(JSON.parse(JSON.stringify(v)), v);

const poolArb: fc.Arbitrary<AnyValidators> = fc.constantFrom(...validatorsPool);
// ~80% from the collision-friendly pool, ~20% from the broad reused arbitrary
// for structural breadth.
const validatorsArb: fc.Arbitrary<AnyValidators> = fc.oneof(
  poolArb,
  poolArb,
  poolArb,
  poolArb,
  AnyValidatorsArb.filter(roundTrips),
);

const dateArb = fc.date({ noInvalidDate: true });

// One store() call: for each of the four slots, optionally include an op. This
// guarantees DISTINCT slots within a call (each slot appears at most once), so
// there is no in-call dedup ambiguity to reconcile across the three stores'
// differing dedup rules -- see the by-construction exclusions note below.
const callArb: fc.Arbitrary<OracleCall> = fc
  .tuple(
    ...Array.from({ length: SLOT_COUNT }, (_unused, slotIdx) =>
      fc
        .option(
          fc.record({
            validators: validatorsArb,
            date: dateArb,
            initialAge: fc.nat({ max: 1000 }),
          }),
          { nil: undefined },
        )
        .map((v) => (v === undefined ? undefined : { slotIdx, ...v })),
    ),
  )
  .map((slots) => slots.filter((s): s is OracleOp => s !== undefined))
  .filter((ops) => ops.length > 0);

const operationsArb: fc.Arbitrary<OracleCall[]> = fc.array(callArb, {
  minLength: 1,
  maxLength: 8,
});

// Naive oracle: for each slot, remember the validators of the most recently
// stored entry (all current stores keep <= 1 entry per slot, and distinct
// slots per call means the sole writer of a slot always wins). Compute each
// input's relationship against the PRE-call state, then apply the call.
function makeOracle() {
  const stored = new Map<number, AnyValidators>();
  return {
    expectAndApply(call: OracleCall): (StoreEntryRelationship | undefined)[] {
      const expected = call.map(
        (op): StoreEntryRelationship | undefined => {
          if (Object.keys(op.validators).length === 0) return undefined;
          if (!stored.has(op.slotIdx)) return "is-new";
          return isEqual(stored.get(op.slotIdx), op.validators)
            ? "unchanged"
            : "changed";
        },
      );
      // Storing any entry (even empty-validators) occupies the slot, so the
      // next store to that slot compares against it rather than seeing is-new.
      for (const op of call) stored.set(op.slotIdx, op.validators);
      return expected;
    },
  };
}

// === By-construction exclusions of the property generator (coverage holes) ===
//
// Per the writing-tests skill, every constraint on the generator is a
// zero-coverage region; each is enumerated here and covered by a dedicated
// example test above:
//
//  1. NO in-call duplicate slots. `callArb` emits each slot at most once per
//     call, so the "multiple inputs for one slot in one store() call" case is
//     unreachable by the property. Covered by
//     "dedupes in-call duplicates by newest birth date...", which pins the
//     contract's uniform rule: every store keeps the entry with the newest
//     birth date. (Rationale for still excluding duplicates here: randomly
//     generated dates can tie, and the winner among birth-date ties is
//     implementation-defined, so a store-agnostic oracle can't predict it.)
//
//  2. Newest-birth-date dedup among MULTIPLE same-call entries is never
//     exercised: with <= 1 entry per slot and distinct slots per call, each
//     slot's stored value is simply the last write, so `date`/`initialAge`
//     never influence the property's outcome. Covered by the "A -> B -> A"
//     test (cross-call: comparison target is the currently-stored value) and
//     the in-call duplicate test (newest-birth-date entry wins even when it
//     is not last).
//
//  3. Slots come from a fixed 2-ids x 2-varies set, not arbitrary id/vary
//     values -- a deliberate bound so `unchanged` collisions are frequent.
//     Arbitrary id/vary storage/matching is already covered by
//     Store.conformance.test.ts.
//
//  4. Validator values are restricted to JSON-round-trippable data (see
//     `roundTrips`); Infinity/NaN/undefined are never generated because they
//     are not representable in the JSON-backed stores. The runtime meaning of
//     type-violating `undefined` values (interpreted as their JSON-serialized
//     form) is covered by the dedicated "ignores undefined-valued keys" tests.
//
//  5. `maxStoreForSeconds` is fixed at 60, so no entry ever expires within a
//     property run and the expired-slot rule (expired records are not live =>
//     "is-new") is unreachable here. Covered by the dedicated "expired ...
//     still physically present" test (for the stores where entries expire).
//
// NOT excluded (exercised inside the property): empty-validators entries (the
// omit rule), is-new on fresh slots, and per-(id,vary) slot granularity.

// ===========================================================================
// Local helpers (copied from Store.conformance.test.ts to keep this file
// self-contained and disjoint from the file the implementer edits).
// ===========================================================================

async function withStore(
  createFixture: () => Promise<StoreFixture>,
  test: (store: TestStore) => Promise<void>,
) {
  const { store, cleanup } = await createFixture();
  try {
    await test(store);
  } finally {
    await cleanup();
  }
}

function makeEntry(
  id: string,
  value: string,
  vary: NormalizedVary<TestParams> = varyOnFormat,
  opts?: {
    content?: TestContent;
    date?: Date;
    initialAge?: number;
    validators?: AnyValidators;
  },
): Entry<TestSpec, AnyValidators, TestParams> {
  return {
    id,
    content: opts?.content ?? { value },
    vary,
    validators: opts?.validators ?? {},
    directives,
    initialAge: opts?.initialAge ?? 0,
    date: opts?.date ?? new Date("2024-01-01T00:00:00.000Z"),
  };
}
