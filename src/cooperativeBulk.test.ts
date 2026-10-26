import assert from "node:assert/strict";
import { setImmediate } from "node:timers/promises";
import { it, mock } from "node:test";
import stableStringify from "safe-stable-stringify";
import Cache from "./Cache.js";
import {
  MemoryStore,
  resourceType,
  type AnyParams,
  type NormalizedParams,
  type SpecOf,
} from "./index.js";
import { stableStringifyArrayCooperatively } from "./utils/cooperative.js";
import { wrapBulkProducer } from "./utils/wrapBulkProducer.js";

const batchSize = 10_000;
function makeCache() {
  let classifications = 0;
  const registry = {
    resources: resourceType<number>()({
      matches: (id): id is string => {
        classifications += 1;
        // Deterministically exceed the cooperative budget, independent of CPU
        // speed, while keeping the classification contract realistic and pure.
        const until = performance.now() + 0.002;
        while (performance.now() < until) {
          /* classification work */
        }
        return id.startsWith("resource:");
      },
    }),
  };
  const store = new MemoryStore<SpecOf<typeof registry>>();
  const cache = new Cache({
    name: "cooperative-bulk",
    resourceTypes: registry,
    store,
  });
  return { cache, store, classifications: () => classifications };
}
const requests = Array.from({ length: batchSize }, (_, i) => ({
  id: `resource:${i}`,
}));

it("yields during a 10k cold batch without splitting its producer cohort", async () => {
  const { cache, classifications } = makeCache();
  const producer = mock.fn(async (reqs: readonly { id: string }[]) =>
    reqs.map((req) => ({
      content: Number(req.id.slice("resource:".length)),
      directives: { freshUntilAge: 60 },
    })),
  );
  const wrapped = wrapBulkProducer(
    { cache, collapseOverlappingRequestsTime: 0 },
    producer,
  );
  let seenAtMacrotask = 0;
  const tick = setImmediate().then(() => {
    seenAtMacrotask = classifications();
  });
  try {
    const result = await wrapped(requests);
    await tick;
    assert.ok(seenAtMacrotask > 0 && seenAtMacrotask < batchSize);
    assert.equal(producer.mock.callCount(), 1);
    assert.equal(producer.mock.calls[0]!.arguments[0].length, batchSize);
    assert.deepEqual(
      result.map((entry) => (entry instanceof Error ? entry : entry.content)),
      requests.map((_, i) => i),
    );
  } finally {
    await cache.close();
  }
});

it("yields during a 10k warm batch and preserves ordered cache hits", async () => {
  const { cache, classifications } = makeCache();
  const producer = mock.fn(async () => {
    throw new Error("warm batch must not produce");
  });
  const wrapped = wrapBulkProducer({ cache }, producer);
  try {
    await cache.store(
      requests.map((req, i) => ({
        id: req.id,
        content: i,
        directives: { freshUntilAge: 60 },
      })),
    );
    const before = classifications();
    let seenAtMacrotask = 0;
    const tick = setImmediate().then(() => {
      seenAtMacrotask = classifications() - before;
    });
    const result = await wrapped(requests);
    await tick;
    assert.ok(seenAtMacrotask > 0 && seenAtMacrotask < batchSize);
    assert.equal(producer.mock.callCount(), 0);
    assert.deepEqual(
      result.map((entry) => (entry instanceof Error ? entry : entry.content)),
      requests.map((_, i) => i),
    );
  } finally {
    await cache.close();
  }
});

it("classifies an invalid final ID before any cache or producer side effect despite yielding", async () => {
  const { cache, store, classifications } = makeCache();
  const read = mock.method(store, "getMany");
  const write = mock.method(store, "store");
  const producer = mock.fn(async () => []);
  const wrapped = wrapBulkProducer({ cache }, producer);
  let seenAtMacrotask = 0;
  const tick = setImmediate().then(() => {
    seenAtMacrotask = classifications();
  });
  try {
    await assert.rejects(wrapped([...requests, { id: "invalid" }]));
    await tick;
    assert.ok(seenAtMacrotask > 0 && seenAtMacrotask < batchSize);
    assert.equal(read.mock.callCount(), 0);
    assert.equal(write.mock.callCount(), 0);
    assert.equal(producer.mock.callCount(), 0);
  } finally {
    await cache.close();
  }
});

it("an abort during yielded preflight never starts cache reads or a producer", async () => {
  const { cache, store } = makeCache();
  const read = mock.method(store, "getMany");
  const producer = mock.fn(async () => []);
  const wrapped = wrapBulkProducer({ cache }, producer);
  const controller = new AbortController();
  const reason = new Error("abort during preflight");
  const tick = setImmediate().then(() => {
    controller.abort(reason);
  });
  try {
    await assert.rejects(
      wrapped(requests, { signal: controller.signal }),
      (error) => error === reason,
    );
    await tick;
    assert.equal(read.mock.callCount(), 0);
    assert.equal(producer.mock.callCount(), 0);
  } finally {
    await cache.close();
  }
});

it("cooperative collapse keys preserve stable array serialization", async () => {
  const shared = { z: [true, null, { b: "b", a: "a" }], a: 1 };
  const values = [
    { id: "resource:1", params: { shared, absent: undefined }, directives: {} },
    {
      id: "resource:2",
      directives: { maxAge: 0 },
      params: { shared, nested: [undefined, Infinity] },
    },
    ...requests,
  ];
  assert.equal(
    await stableStringifyArrayCooperatively(values),
    stableStringify(values),
  );
});

it("single MemoryStore writes remain immediately visible without awaiting their promise", async () => {
  const { cache, store } = makeCache();
  try {
    await cache.store([
      {
        id: "resource:template",
        content: 0,
        directives: { freshUntilAge: 60 },
      },
    ]);
    const { usable } = await cache.get({
      id: "resource:template",
      params: {},
      directives: {},
    });
    assert.ok(usable);
    const pending = store.store([
      {
        entry: { ...usable, id: "resource:immediate" },
        maxStoreForSeconds: 60,
      },
    ]);
    const read = await store.get(
      "resource:immediate",
      {} as NormalizedParams<AnyParams>,
    );
    assert.equal(read.length, 1);
    await pending;
  } finally {
    await cache.close();
  }
});

it("yielded MemoryStore writers retain ordered comparisons and later invalidation", async () => {
  const { cache, store } = makeCache();
  try {
    await cache.store([
      {
        id: "resource:template",
        content: 0,
        directives: { freshUntilAge: 60 },
      },
    ]);
    const { usable } = await cache.get({
      id: "resource:template",
      params: {},
      directives: {},
    });
    assert.ok(usable);
    const firstInputs = requests.map((req) => ({
      entry: { ...usable, id: req.id, validators: { version: "one" } },
      maxStoreForSeconds: 60,
    }));
    const secondInputs = firstInputs.map((input) => ({
      ...input,
      entry: { ...input.entry, validators: { version: "two" } },
    }));
    const first = store.store(firstInputs);
    const second = store.store(secondInputs);
    const deletion = store.delete(requests[batchSize - 1]!.id);
    const [firstResults, secondResults] = await Promise.all([first, second]);
    await deletion;
    assert.ok(
      firstResults.every(
        (result) => result.relationshipToExistingStoredData === "is-new",
      ),
    );
    assert.ok(
      secondResults.every(
        (result) => result.relationshipToExistingStoredData === "changed",
      ),
    );
    assert.deepEqual(
      await store.get(
        requests[batchSize - 1]!.id,
        {} as NormalizedParams<AnyParams>,
      ),
      [],
    );
  } finally {
    await cache.close();
  }
});

it("a bypass settled before a sibling read abort keeps its producer disposition", async () => {
  const { cache, store } = makeCache();
  const { cacheFetchChannel, cacheProduceChannel } =
    await import("./diagnostics.js");
  const produced = Promise.withResolvers<void>();
  const readStarted = Promise.withResolvers<void>();
  const fetches: { resourceId: string; disposition: string }[] = [];
  const onFetch = (message: { resourceId: string; disposition: string }) => {
    fetches.push(message);
  };
  const onProduce = () => {
    produced.resolve();
  };
  cacheFetchChannel.subscribe(onFetch);
  cacheProduceChannel.subscribe(onProduce);
  mock.method(
    store,
    "getMany",
    async (_requests: unknown, options?: { signal?: AbortSignal }) => {
      readStarted.resolve();
      return new Promise((_resolve, reject) => {
        options!.signal!.addEventListener(
          "abort",
          () => {
            reject(options!.signal!.reason);
          },
          { once: true },
        );
      });
    },
  );
  const controller = new AbortController();
  const reason = new Error("abort sibling read after bypass finishes");
  const wrapped = wrapBulkProducer({ cache }, async (reqs) =>
    reqs.map(() => ({ content: 1, directives: { freshUntilAge: 60 } })),
  );
  try {
    const pending = wrapped(
      [
        { id: "resource:bypass", directives: { maxAge: 0 } },
        { id: "resource:read" },
      ],
      { signal: controller.signal },
    );
    const rejection = assert.rejects(pending, (error) => error === reason);
    await Promise.all([produced.promise, readStarted.promise]);
    await setImmediate();
    controller.abort(reason);
    await rejection;
    assert.deepEqual(
      fetches
        .map(({ resourceId, disposition }) => ({ resourceId, disposition }))
        .sort((a, b) => a.resourceId.localeCompare(b.resourceId)),
      [
        { resourceId: "resource:bypass", disposition: "served-from-producer" },
        { resourceId: "resource:read", disposition: "aborted" },
      ],
    );
  } finally {
    controller.abort(reason);
    cacheFetchChannel.unsubscribe(onFetch);
    cacheProduceChannel.unsubscribe(onProduce);
    await cache.close();
  }
});

function makeLifecycleCache(duringNormalization: (cache: Cache) => void) {
  let scheduled = false;
  const store = new MemoryStore();
  const cache = new Cache({
    name: "cooperative-lifecycle",
    resourceTypes: {
      resources: resourceType<unknown>()({
        matches: (_id): _id is string => true,
      }),
    },
    store,
    normalizeParamValue: (_name, value) => {
      if (!scheduled) {
        scheduled = true;
        void setImmediate().then(() => {
          duringNormalization(cache);
        });
      }
      const until = performance.now() + 0.02;
      while (performance.now() < until) {
        /* force a preparation yield */
      }
      return value;
    },
  });
  const inputs = requests.slice(0, 1_000).map((req, i) => ({
    id: req.id,
    content: i,
    vary: { x: "v" },
    directives: { freshUntilAge: 60 },
  }));
  return { cache, store, inputs };
}

it("a public delete cannot overtake a bulk store yielding in normalization", async () => {
  let deletion: Promise<void> | undefined;
  const { cache, store, inputs } = makeLifecycleCache((active) => {
    deletion = active.delete("resource:0");
  });
  try {
    await cache.store(inputs);
    assert.ok(deletion);
    await deletion;
    assert.deepEqual(
      await store.get("resource:0", {
        x: "v",
      } as unknown as NormalizedParams<AnyParams>),
      [],
    );
  } finally {
    await cache.close();
  }
});

it("public close waits for previously accepted bulk write preparation", async () => {
  let closing: Promise<void> | undefined;
  const { cache, store, inputs } = makeLifecycleCache((active) => {
    closing = active.close();
  });
  const order: string[] = [];
  const originalStore = store.store.bind(store);
  const originalClose = store.close.bind(store);
  mock.method(store, "store", (entries: Parameters<typeof store.store>[0]) => {
    order.push("store");
    return originalStore(entries);
  });
  mock.method(store, "close", () => {
    order.push("close");
    return originalClose();
  });
  await cache.store(inputs);
  assert.ok(closing);
  await closing;
  assert.deepEqual(order, ["store", "close"]);
});

it("a fast miss rejection remains observed while background refresh enrollment yields", async () => {
  const { cache } = makeCache();
  try {
    await cache.store([
      {
        id: "resource:template",
        content: 0,
        directives: { freshUntilAge: 60 },
      },
    ]);
    const { usable } = await cache.get({
      id: "resource:template",
      params: {},
      directives: {},
    });
    assert.ok(usable);
    mock.method(cache, "getMany", async (reqs: readonly { id: string }[]) =>
      reqs.map((req, index) =>
        index === 0
          ? { validatable: [] }
          : {
              validatable: [],
              usableWhileRevalidate: { ...usable, id: req.id },
            },
      ),
    );
    const failure = new Error("fast foreground miss failure");
    const wrapped = wrapBulkProducer({ cache, logger: () => {} }, async () => {
      throw failure;
    });
    const payload = "x".repeat(2_048);
    // Stable-key preparation for the refresh cohort must yield, leaving enough
    // time for a fast foreground rejection to become unhandled without the
    // immediate observer installed by the wrapper.
    await assert.rejects(
      wrapped(requests.map((request) => ({ ...request, params: { payload } }))),
      (error) => error === failure,
    );
    await setImmediate();
  } finally {
    await cache.close();
  }
});

it("an invalid final bulk store ID prevents writes and events after yielded preflight", async () => {
  const { cache, store } = makeCache();
  const write = mock.method(store, "store");
  let events = 0;
  cache.emitter.on("store", () => {
    events += 1;
  });
  try {
    const inputs = [...requests, { id: "invalid" }].map((req) => ({
      id: req.id,
      content: 1,
      directives: { freshUntilAge: 60 },
    }));
    await assert.rejects(cache.store(inputs));
    assert.equal(write.mock.callCount(), 0);
    assert.equal(events, 0);
  } finally {
    await cache.close();
  }
});

it("a pre-aborted mixed batch publishes every aborted fetch before any read starts", async () => {
  const { cache, store } = makeCache();
  const { cacheFetchChannel } = await import("./diagnostics.js");
  const fetches: { resourceId: string; disposition: string }[] = [];
  const onFetch = (message: { resourceId: string; disposition: string }) => {
    fetches.push(message);
  };
  cacheFetchChannel.subscribe(onFetch);
  const read = mock.method(store, "getMany");
  const producer = mock.fn(async () => []);
  const wrapped = wrapBulkProducer({ cache }, producer);
  const reason = new Error("already aborted");
  try {
    await assert.rejects(
      wrapped(
        [
          { id: "resource:bypass", directives: { maxAge: 0 } },
          { id: "resource:read" },
        ],
        { signal: AbortSignal.abort(reason) },
      ),
      (error) => error === reason,
    );
    assert.deepEqual(
      fetches.map(({ resourceId, disposition }) => ({
        resourceId,
        disposition,
      })),
      [
        { resourceId: "resource:bypass", disposition: "aborted" },
        { resourceId: "resource:read", disposition: "aborted" },
      ],
    );
    assert.equal(read.mock.callCount(), 0);
    assert.equal(producer.mock.callCount(), 0);
  } finally {
    cacheFetchChannel.unsubscribe(onFetch);
    await cache.close();
  }
});
