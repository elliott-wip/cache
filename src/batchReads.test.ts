import assert from "node:assert/strict";
import { describe, it, mock } from "node:test";
import { setImmediate as nextTurn } from "node:timers/promises";

import {
  postgresEnvironmentIsConfigured,
  postgresStoreFixture,
} from "../test/fixtures.js";
import {
  ACCEPT_ANY_REGISTRY,
  captureChannels,
  freshFor100,
  uniqueCacheName,
  waitUntil,
} from "../test/v2AcceptanceHelpers.js";
import Cache, { singleTypeCacheOptions } from "./Cache.js";
import MemoryStore from "./stores/MemoryStore/MemoryStore.js";
import type { CacheSpec } from "./types/00_CacheSpec.js";
import type { AnyParams } from "./types/01_Params.js";
import type { AnyValidators } from "./types/02_Validators.js";
import { MAX_REQUESTS_PER_BATCHED_GET_MANY } from "./utils/storeReadBatcher.js";
import wrapProducer from "./utils/wrapProducer.js";

/**
 * The `batchReads` contract: the `get()` calls of one event-loop turn share
 * one `store.getMany()` call, and nothing else about a `get()` changes. Each
 * request still gets its own lookup result and its own `read` message.
 */

type Spec = CacheSpec<string, string>;

/**
 * A MemoryStore whose reads are spied on. `beforeGetMany` runs inside every
 * `getMany` call before the store reads, so a test can hold a batch's query
 * open.
 */
function spiedStore(options?: { beforeGetMany?: () => Promise<void> }) {
  const store = new MemoryStore<Spec, AnyValidators, AnyParams>();
  const getMany = store.getMany.bind(store);
  const getSpy = mock.method(store, "get");
  const getManySpy = mock.method(
    store,
    "getMany",
    async (...args: Parameters<typeof getMany>) => {
      await options?.beforeGetMany?.();
      return getMany(...args);
    },
  );
  return {
    store,
    getCallCount: () => getSpy.mock.callCount(),
    /** The number of requests in each `getMany` call, in call order. */
    getManyRequestCounts: () =>
      getManySpy.mock.calls.map(({ arguments: [requests] }) => requests.length),
  };
}

/** A gate that holds a store read open until the test releases it. */
function gate() {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { wait: async () => promise, release: () => resolve() };
}

const batchedCacheOver = (
  store: MemoryStore<Spec, AnyValidators, AnyParams>,
  name = uniqueCacheName("batch-reads"),
) =>
  new Cache({
    store,
    name,
    resourceTypes: ACCEPT_ANY_REGISTRY,
    batchReads: true,
  });

const lookup = (id: string, params: Record<string, string> = {}) => ({
  id,
  params,
  directives: {},
});

// A read that never settles would otherwise hang the run (node:test waits for
// a pending test forever), so each suite fails after a bound instead.
const suiteOptions = { timeout: 20_000 };

describe("Cache with batchReads", suiteOptions, () => {
  it("serves concurrent gets from one getMany call, with each request's own result", async () => {
    const { store, getCallCount, getManyRequestCounts } = spiedStore();
    const name = uniqueCacheName("batch-reads");
    const batched = batchedCacheOver(store, name);
    const unbatched = new Cache({
      store,
      name: uniqueCacheName("unbatched"),
      resourceTypes: ACCEPT_ANY_REGISTRY,
    });
    await batched.store([
      { id: "plain", content: "plain", directives: freshFor100 },
      {
        id: "by-format",
        vary: { format: "json" },
        content: "json",
        directives: freshFor100,
      },
    ]);

    // Ten requests: hits, misses, and two requests for one id whose params
    // select different variants.
    const requests = [
      lookup("plain"),
      lookup("by-format", { format: "json" }),
      lookup("by-format", { format: "html" }),
      lookup("plain", { format: "html" }),
      ...Array.from({ length: 6 }, (_, i) => lookup(`missing:${String(i)}`)),
    ];

    const capture = captureChannels(name);
    try {
      const batchedResults = await Promise.all(
        requests.map(async (req) => batched.get(req)),
      );

      assert.deepEqual(getManyRequestCounts(), [10]);
      assert.equal(getCallCount(), 0);
      assert.deepEqual(
        batchedResults.map((it) => it.usable?.content),
        ["plain", "json", undefined, "plain", ...Array(6).fill(undefined)],
      );

      // One `read` message per request, each naming its own request's outcome.
      assert.deepEqual(
        capture.read.map(({ resourceId, found }) => [resourceId, found]),
        requests.map(({ id }, i) => [
          id,
          batchedResults[i]?.usable === undefined ? "none" : "usable",
        ]),
      );

      // The same lookups without batchReads: one store.get each, and the same
      // results.
      const unbatchedResults = await Promise.all(
        requests.map(async (req) => unbatched.get(req)),
      );
      assert.equal(getCallCount(), 10);
      assert.deepEqual(getManyRequestCounts(), [10]);
      assert.deepEqual(batchedResults, unbatchedResults);
    } finally {
      capture.stop();
    }
  });

  it("gathers every get of one event-loop turn, including gets after microtask hops", async () => {
    const { store, getManyRequestCounts } = spiedStore();
    const cache = batchedCacheOver(store);

    const first = cache.get(lookup("a"));
    // Promise continuations run in the same turn, so a get that a few awaits
    // delay still joins the batch. A flush at the end of the microtask queue
    // would have sent "a" alone.
    for (const _ of Array.from({ length: 5 })) {
      await Promise.resolve();
    }
    const second = cache.get(lookup("b"));
    await Promise.all([first, second]);

    assert.deepEqual(getManyRequestCounts(), [2]);
  });

  it("flushes at the next setImmediate, and a later get starts the next batch", async () => {
    const { store, getManyRequestCounts } = spiedStore();
    const cache = batchedCacheOver(store);

    const first = cache.get(lookup("a"));
    // setImmediate callbacks run in order, so the batch's flush, which the
    // get scheduled first, has run by the time this one resolves. A flush on
    // a timer would not have.
    await nextTurn();
    assert.deepEqual(getManyRequestCounts(), [1]);

    await first;
    await cache.get(lookup("b"));
    assert.deepEqual(getManyRequestCounts(), [1, 1]);
  });

  it("splits a flush of 1,200 requests into getMany calls of at most 500", async () => {
    const { store, getManyRequestCounts } = spiedStore();
    const cache = batchedCacheOver(store);
    const ids = Array.from({ length: 1200 }, (_, i) => `id:${String(i)}`);
    await cache.store(
      ids.map((id) => ({ id, content: id, directives: freshFor100 })),
    );

    const results = await Promise.all(
      ids.map(async (id) => cache.get(lookup(id))),
    );

    assert.equal(MAX_REQUESTS_PER_BATCHED_GET_MANY, 500);
    assert.deepEqual(getManyRequestCounts(), [500, 500, 200]);
    assert.deepEqual(
      results.map((it) => it.usable?.content),
      ids,
    );
  });

  it("rejects only the request whose signal aborts while it waits, and drops it from the query", async () => {
    const { store, getManyRequestCounts } = spiedStore();
    const name = uniqueCacheName("batch-reads");
    const cache = batchedCacheOver(store, name);
    const controller = new AbortController();
    const reason = new Error("caller gave up");

    const capture = captureChannels(name);
    try {
      const pending = [
        cache.get(lookup("a")),
        cache.get(lookup("b"), { signal: controller.signal }),
        cache.get(lookup("c")),
      ];
      controller.abort(reason);
      const [a, b, c] = await Promise.allSettled(pending);

      assert.equal(a?.status, "fulfilled");
      assert.deepEqual(b, { status: "rejected", reason });
      assert.equal(c?.status, "fulfilled");
      assert.deepEqual(getManyRequestCounts(), [2]);
      assert.deepEqual(
        capture.read.map(({ resourceId }) => resourceId),
        ["a", "c"],
      );
    } finally {
      capture.stop();
    }
  });

  it("rejects a request at once when its signal aborts while its batch's query runs", async () => {
    const query = gate();
    const { store, getManyRequestCounts } = spiedStore({
      beforeGetMany: query.wait,
    });
    const cache = batchedCacheOver(store);
    const controller = new AbortController();
    const reason = new Error("caller gave up");

    const aborted = cache.get(lookup("a"), { signal: controller.signal });
    const other = cache.get(lookup("b"));
    await waitUntil(
      () => getManyRequestCounts().length === 1,
      "the batch's getMany call to start",
    );
    controller.abort(reason);

    // The query is still held open, so only an abort listener can settle the
    // aborted request this soon.
    const settledWhileQueryRuns = await Promise.race([
      aborted.then(
        () => "resolved",
        (error: unknown) => error,
      ),
      nextTurn().then(() => "still pending"),
    ]);
    assert.equal(settledWhileQueryRuns, reason);

    query.release();
    assert.equal((await other).usable, undefined);
    assert.deepEqual(getManyRequestCounts(), [2]);
  });

  it("rejects every request in a failed getMany call, with one read-failed message each", async () => {
    const store = new MemoryStore<Spec, AnyValidators, AnyParams>();
    const failure = new Error("store is down");
    mock.method(store, "getMany", async () => {
      throw failure;
    });
    const name = uniqueCacheName("batch-reads");
    const cache = batchedCacheOver(store, name);
    const ids = Array.from({ length: 10 }, (_, i) => `id:${String(i)}`);

    const capture = captureChannels(name);
    try {
      const settled = await Promise.allSettled(
        ids.map(async (id) => cache.get(lookup(id))),
      );

      assert.deepEqual(
        settled,
        ids.map(() => ({ status: "rejected", reason: failure })),
      );
      assert.deepEqual(
        capture.read.map(({ resourceId, found }) => [resourceId, found]),
        ids.map((id) => [id, "read-failed"]),
      );
      assert.ok(
        capture.read.every(
          (message) =>
            message.found === "read-failed" && message.error === failure,
        ),
      );
    } finally {
      capture.stop();
    }
  });

  it("starts queued reads before close() closes the store", async () => {
    const store = new MemoryStore<Spec, AnyValidators, AnyParams>();
    const events: string[] = [];
    const getMany = store.getMany.bind(store);
    mock.method(
      store,
      "getMany",
      async (...args: Parameters<typeof getMany>) => {
        events.push("getMany");
        return getMany(...args);
      },
    );
    const close = store.close.bind(store);
    mock.method(store, "close", async () => {
      events.push("close");
      return close();
    });
    const cache = batchedCacheOver(store);
    await cache.store([{ id: "a", content: "a", directives: freshFor100 }]);

    // Both reads are queued, not yet flushed, when close() starts.
    const reads = [cache.get(lookup("a")), cache.get(lookup("b"))];
    await cache.close();
    const [a, b] = await Promise.all(reads);

    assert.deepEqual(events, ["getMany", "close"]);
    assert.equal(a?.usable?.content, "a");
    assert.equal(b?.usable, undefined);
  });

  it("settles close(timeout) even when a batch's query never settles", async () => {
    // The query never settles. close() must still reach the store's close,
    // as it does for an unbatched read in flight.
    const { store, getManyRequestCounts } = spiedStore({
      beforeGetMany: async () => new Promise<void>(() => {}),
    });
    const cache = batchedCacheOver(store);

    void cache.get(lookup("a"));
    await cache.close(100);

    assert.deepEqual(getManyRequestCounts(), [1]);
  });

  it("rejects a get that arrives after close() starts", async () => {
    const { store, getManyRequestCounts } = spiedStore();
    const cache = batchedCacheOver(store);

    const closing = cache.close();
    await assert.rejects(cache.get(lookup("a")), /Store has been closed/);
    await closing;

    assert.deepEqual(getManyRequestCounts(), []);
  });

  it("is forwarded by singleTypeCacheOptions", async () => {
    const { store, getCallCount, getManyRequestCounts } = spiedStore();
    const cache = new Cache(
      singleTypeCacheOptions<string>()({
        store,
        name: uniqueCacheName("batch-reads-single-type"),
        batchReads: true,
      }),
    );

    await Promise.all(["a", "b", "c"].map(async (id) => cache.get(lookup(id))));

    assert.deepEqual(getManyRequestCounts(), [3]);
    assert.equal(getCallCount(), 0);
  });
});

describe("wrapProducer over a batchReads cache", suiteOptions, () => {
  it("reads the cache for 10 concurrent wrapped calls with one store read", async () => {
    const { store, getCallCount, getManyRequestCounts } = spiedStore();
    const cache = batchedCacheOver(store);
    const ids = Array.from({ length: 10 }, (_, i) => `id:${String(i)}`);
    // Half the ids are cached; the other half miss and reach the producer.
    await cache.store(
      ids.slice(0, 5).map((id) => ({
        id,
        content: `cached:${id}`,
        directives: freshFor100,
      })),
    );
    const load = wrapProducer({ cache }, async (req) => ({
      content: `produced:${req.id}`,
      directives: freshFor100,
    }));

    // The shape of `EmrVisitService.getByIds`: one wrapped call per id, all
    // started in one turn.
    const results = await Promise.all(
      ids.map(async (id) => load({ id, directives: {} })),
    );

    assert.deepEqual(getManyRequestCounts(), [10]);
    assert.equal(getCallCount(), 0);
    assert.deepEqual(
      results.map(({ content }) => content),
      ids.map((id, i) => (i < 5 ? `cached:${id}` : `produced:${id}`)),
    );
  });
});

describe(
  "Cache with batchReads over PostgresStore",
  postgresEnvironmentIsConfigured()
    ? suiteOptions
    : { skip: "Postgres environment variables are not configured" },
  () => {
    it("reads 10 concurrent gets with one SQL statement", async () => {
      const statements: string[] = [];
      const fixture = postgresStoreFixture({
        onQuery: (text) => {
          statements.push(text);
        },
      });
      try {
        const cache = new Cache({
          store: fixture.postgresStore,
          name: uniqueCacheName("batch-reads-postgres"),
          resourceTypes: ACCEPT_ANY_REGISTRY,
          batchReads: true,
        });
        const ids = Array.from({ length: 10 }, (_, i) => `id:${String(i)}`);
        await cache.store(
          ids.map((id) => ({ id, content: id, directives: freshFor100 })),
        );

        const before = statements.length;
        const results = await Promise.all(
          ids.map(async (id) => cache.get(lookup(id))),
        );

        assert.equal(statements.slice(before).length, 1);
        assert.deepEqual(
          results.map((it) => it.usable?.content),
          ids,
        );
      } finally {
        await fixture.cleanup();
      }
    });
  },
);
