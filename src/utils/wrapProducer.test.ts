import { expect } from "chai";
import { afterEach, beforeEach, describe, it, mock } from "node:test";

import { setTimeout as delay } from "timers/promises";
import Cache from "../Cache.js";
import {
  MemoryStore,
  resourceType,
  type ResourceTypes,
  type SpecOf,
} from "../index.js";
import type {
  AnyParams,
  AnyValidators,
  ConsumerDirectives,
  Entry,
  RequestPairedProducerResult,
} from "../types/index.js";
import wrapProducer from "./wrapProducer.js";

// The wrapper takes ONE producer function over a required registry, and a bare
// function covers the whole registry. This suite's behavior is
// id-structure-agnostic, so it uses a sole-type registry and passes its
// producers bare; per-type dispatch (`producerByIdType`) and
// coverage/classification behavior live in coverageRuntime.test.ts and
// resourceTypeClassification.test.ts.
const testRegistry = {
  resources: resourceType<unknown>()({
    matches: (id): id is string => typeof id === "string",
  }),
} satisfies ResourceTypes;
const testCacheOptions = {
  name: "wrap-producer-test",
  resourceTypes: testRegistry,
};
const makeTestCache = () =>
  new Cache({
    store: new MemoryStore<SpecOf<typeof testRegistry>>(),
    ...testCacheOptions,
  });
type TestCache = ReturnType<typeof makeTestCache>;
type WrappedFn = (
  req: {
    id: string;
    params?: Partial<AnyParams>;
    directives?: ConsumerDirectives;
  },
  options?: { signal?: AbortSignal },
) => Promise<Entry<SpecOf<typeof testRegistry>, AnyValidators, AnyParams>>;

/* eslint-disable @typescript-eslint/no-explicit-any */
describe("wrapProducer", () => {
  let fetcher: ReturnType<
      typeof mock.fn<
        (it: {
          id: string;
        }) => Promise<RequestPairedProducerResult<any, any, any>>
      >
    >,
    cache: TestCache,
    sut: WrappedFn;

  beforeEach(() => {
    cache = makeTestCache();
    fetcher = mock.fn(
      async (_req) =>
        ({
          content: new Date().toISOString(),
          directives: {
            freshUntilAge: 0.1,
            maxStale: {
              withoutRevalidation: 0,
              whileRevalidate: 0.4,
              ifError: 0.4,
            },
          },
          supplementalResources: [
            {
              id: "s",
              vary: { dummy: true },
              content: "supplemental",
              directives: { freshUntilAge: 1 },
            },
          ],
        }) satisfies RequestPairedProducerResult<any, any, any>,
    );

    sut = wrapProducer({ cache, collapseOverlappingRequestsTime: 0 }, fetcher);
  });

  afterEach(async () => {
    return cache.close();
  });

  it("should call the fetcher at first", async () => {
    await sut({ id: "myUrl" });
    expect(fetcher.mock.callCount()).to.eq(1);
    expect(fetcher.mock.calls[0]?.arguments).to.deep.eq([
      { id: "myUrl", params: {}, directives: {} },
    ]);
  });

  it("should add supplemental resources to the cache", async () => {
    await sut({ id: "myUrl" });
    expect(fetcher.mock.callCount()).to.eq(1);
    expect(fetcher.mock.calls[0]?.arguments).to.deep.eq([
      { id: "myUrl", params: {}, directives: {} },
    ]);

    const res = await sut({ id: "s", params: { dummy: true } });
    expect(fetcher.mock.callCount()).to.eq(1);
    expect(res).to.deep.include({
      id: "s",
      vary: { dummy: true },
      content: "supplemental",
      directives: { freshUntilAge: 1 },
    });
  });

  it("should not call the fetcher again during the freshness window", async () => {
    // A wide self-owned freshness window: the shared fixture's 100ms window
    // is routinely exceeded by (first call + 30ms delay) under full-suite
    // load, flaking this test. The subject — a second request within the
    // freshness window must not refetch — is unchanged.
    const freshFetcher = mock.fn(
      async (_req: { id: string }) =>
        ({
          content: "fresh",
          directives: { freshUntilAge: 30 },
        }) satisfies RequestPairedProducerResult<any, any, any>,
    );
    const freshSut = wrapProducer(
      { cache, collapseOverlappingRequestsTime: 0 },
      freshFetcher,
    );

    await freshSut({ id: "myUrl" });
    await delay(30);
    await freshSut({ id: "myUrl" });

    expect(freshFetcher.mock.callCount()).to.eq(1);
  });

  it("should call but not block on the fetcher during the staleWhileRefresh window, if any", async () => {
    // Per-test wrapper with wide windows: the fixture's 100ms/400ms windows
    // require the second request to land inside a 400ms wall-clock band,
    // which full-suite load stalls (>1s observed) can miss. First result:
    // 1s fresh + 30s SWR band; refreshed results are hour-fresh so the
    // settle-polling below can never trigger further revalidations.
    let producerCalls = 0;
    const swrFetcher = mock.fn(async () => {
      producerCalls += 1;
      return {
        content: `produced-${producerCalls}`,
        directives:
          producerCalls === 1
            ? {
                freshUntilAge: 1,
                maxStale: {
                  withoutRevalidation: 0,
                  whileRevalidate: 30,
                  ifError: 30,
                },
              }
            : { freshUntilAge: 3600 },
      } satisfies RequestPairedProducerResult<any, any, any>;
    });
    const swrSut = wrapProducer(
      { cache, collapseOverlappingRequestsTime: 0 },
      swrFetcher,
    );

    // Load content into the cache.
    const res1 = await swrSut({ id: "myUrl", params: {} });

    // Get past the 1s freshness window (structurally guaranteed — the timer
    // can only fire later than its deadline) into the 30s-wide SWR band.
    await delay(1100);

    // Request cached data, which should come back to us immediately w/ the
    // old result (faster than the fetcher loads), while a second load is
    // triggered.
    const res2 = await swrSut({ id: "myUrl" });
    expect(res1.content).to.deep.eq(res2.content);

    // Wait for the refresh's fire-and-forget store to become visible using
    // DIRECT cache reads, which never trigger producers. Polling through the
    // wrapper instead would serve-stale again on every pre-visibility poll
    // and fire an extra revalidation each time, breaking the exact
    // call-count below. The old entry is past its 1s freshness, so `usable`
    // is populated if and only if the refreshed (hour-fresh) entry landed.
    const waitForRefreshedEntry = async (attempt: number): Promise<void> => {
      const direct = await cache.get({
        id: "myUrl",
        params: {},
        directives: {},
      });
      if (direct.usable !== undefined || attempt >= 200) return;
      await delay(25);
      return waitForRefreshedEntry(attempt + 1);
    };
    await waitForRefreshedEntry(0);

    const res3 = await swrSut({ id: "myUrl" });
    expect(res2.content).to.not.deep.eq(res3.content);
    expect(swrFetcher.mock.callCount()).to.eq(2);
  });

  it("should call the fetcher again and block after the expiration window", async () => {
    const res1 = await sut({ id: "myUrl" });
    await delay(600);
    const res2 = await sut({ id: "myUrl" });

    expect(res1.content).not.to.deep.eq(res2.content);
    expect(fetcher.mock.callCount()).to.eq(2);
  });

  it("should return the error if the fetcher rejects", async () => {
    const testError = new Error("test");
    const rejectingFetcher = mock.fn(async () => Promise.reject(testError));
    const sut2 = wrapProducer({ cache }, rejectingFetcher);

    return sut2({ id: "someUrl" }).then(
      () => {
        throw new Error("should have rejected");
      },
      (e: unknown) => {
        expect(e).to.eql(testError);
      },
    );
  });

  it("should use the cached response if fetcher rejects during the staleIfError window", async () => {
    const testError = new Error("test");
    // Windows sized so each phase keeps seconds of margin: the original
    // 50ms/100ms windows required the stale-if-error request to land inside
    // a 100ms-wide wall-clock band, which full-suite load stalls (>1s
    // observed) routinely miss. Phase boundaries: fresh until 1s, if-error
    // usable until 5s.
    const testResult = {
      content: { body: { test: true }, headers: {} },
      directives: {
        freshUntilAge: 1,
        maxStale: {
          withoutRevalidation: 0,
          whileRevalidate: 0,
          ifError: 4,
        },
      },
    } satisfies RequestPairedProducerResult<any, any, any>;

    let customFetcherCallCount = 0;
    const customTestFetcher = mock.fn(async () => {
      const currentCall = customFetcherCallCount;
      customFetcherCallCount++;
      switch (currentCall) {
        case 0:
          return testResult;
        case 1:
        case 2:
          throw testError;
        case 3:
          return testResult;
        default:
          throw new Error("unexpected number of calls");
      }
    });

    const sut2 = wrapProducer({ cache }, customTestFetcher);

    const firstRes = await sut2({ id: "someUrl" });
    expect(firstRes).to.deep.include(testResult);

    await delay(1100); // past 1s freshness, well inside the 5s if-error bound

    // first res is expired, and the fetcher errored, but we should
    // be able to reuse the first res anyway because of staleIfError.
    const secondRes = await sut2({ id: "someUrl" });
    expect(secondRes).to.deep.include(testResult);
    expect(customTestFetcher.mock.callCount()).to.eq(2);

    await delay(4600); // now past the 5s if-error bound

    // now, the staleIfError window should be up, so we have to go back
    // to the fetcher, but it errors again, so we should get that error.
    try {
      await sut2({ id: "someUrl" });
    } catch (e) {
      expect(e).to.deep.eq(testError);
      expect(customTestFetcher.mock.callCount()).to.eq(3);
    }

    // finally, the server comes back online for the next request
    const fourthRes = await sut2({ id: "someUrl" });
    expect(fourthRes).to.deep.include({ content: testResult.content });
  });

  it("should treat resolution values with Error-instance content as success", async () => {
    const test404 = new Error("test");
    const resolveWithErrorFetcher = mock.fn(
      async () =>
        ({
          content: test404 as unknown,
          directives: { freshUntilAge: 0 },
        }) satisfies RequestPairedProducerResult<any, any, any>,
    );
    const sut2 = wrapProducer({ cache }, resolveWithErrorFetcher);

    return sut2({ id: "someUrl2" }).then((it) => {
      expect(it).to.include({ content: test404 });
    });
  });

  it("should respect consumer directives too", async () => {
    const randomId = String(Math.random());

    // Even though producer says the data's good for 100ms,
    // it should get called twice if the client sets its maxAge to 0
    await sut({ id: randomId, directives: { maxAge: 0 } });
    await delay(5);
    await sut({ id: randomId, directives: { maxAge: 0 } });
    expect(fetcher.mock.callCount()).to.eq(2);

    // but this third call should use the cache, because no consumer directive
    await sut({ id: randomId });
    expect(fetcher.mock.callCount()).to.eq(2);
  });

  describe("the onCacheReadFailure setting", async () => {
    const err = new Error("Cache get error 2");
    let mockCache: TestCache;

    beforeEach(() => {
      mockCache = makeTestCache();
      mockCache.get = async () => {
        throw err;
      };
    });

    afterEach(async () => {
      await mockCache.close();
    });

    it("should throw if configured and cache's get method rejects", async () => {
      const wrappedProducer = wrapProducer(
        { cache: mockCache, onCacheReadFailure: "throw" },
        async ({ id }) => ({
          content: id,
          directives: { freshUntilAge: 1 },
        }),
      );

      await wrappedProducer({ id: "test" }).then(
        () => {
          throw new Error("should've rejected");
        },
        (e) => {
          expect(e).to.eq(err);
        },
      );
    });

    it("should call the producer if configured and cache's get method rejects", async () => {
      const mockProducer = mock.fn(async ({ id }: { id: string }) => ({
        content: id,
        directives: { freshUntilAge: 1 },
      }));

      const wrappedProducer = wrapProducer(
        { cache: mockCache, onCacheReadFailure: "call-producer" },
        mockProducer,
      );

      const res = await wrappedProducer({ id: "test" });
      expect(res.content).to.eq("test");
    });

    it("should call the producer by default if cache's get method rejects", async () => {
      const mockProducer = mock.fn(async ({ id }: { id: string }) => ({
        content: id,
        directives: { freshUntilAge: 1 },
      }));

      const wrappedProducer = wrapProducer({ cache: mockCache }, mockProducer);

      const res = await wrappedProducer({ id: "test" });
      expect(res.content).to.eq("test");
    });
  });

  describe("AbortSignal support", () => {
    it("should reject immediately with an already-aborted signal", async () => {
      const controller = new AbortController();
      controller.abort(new Error("pre-aborted"));

      try {
        await sut({ id: "myUrl" }, { signal: controller.signal });
        throw new Error("should have rejected");
      } catch (e) {
        expect(e).to.be.instanceOf(Error);
        expect((e as Error).message).to.eq("pre-aborted");
      }

      expect(fetcher.mock.callCount()).to.eq(0);
    });

    it("should reject if signal is aborted before the cache read completes (i.e., before producer is called)", async () => {
      const controller = new AbortController();
      let cacheGetResolve: (v: any) => void;

      const mockCache = makeTestCache();

      // Make cache.get hang until we resolve it manually, simulating a slow
      // cache read during which the signal fires.
      mockCache.get = () =>
        new Promise((resolve) => {
          cacheGetResolve = resolve;
        });

      const producerFn = mock.fn(async () => ({
        content: "test",
        directives: { freshUntilAge: 1 },
      }));

      const sut2 = wrapProducer(
        { cache: mockCache, collapseOverlappingRequestsTime: 0 },
        producerFn,
      );

      const resultPromise = sut2(
        { id: "abort-during-read" },
        { signal: controller.signal },
      );

      await delay(5);
      controller.abort(new Error("cancelled-during-read"));

      // Resolve the cache read after the abort — the throwIfAborted checkpoint
      // after the cache read should fire.
      cacheGetResolve!({ validatable: [] });

      try {
        await resultPromise;
        throw new Error("should have rejected");
      } catch (e) {
        expect(e).to.be.instanceOf(Error);
        expect((e as Error).message).to.eq("cancelled-during-read");
      }

      // Producer should never have been called
      expect(producerFn.mock.callCount()).to.eq(0);

      await mockCache.close();
    });

    it("should reject when signal is aborted mid-producer, but still store the producer's result", async () => {
      const controller = new AbortController();

      const slowFetcher = mock.fn(
        async () =>
          new Promise<RequestPairedProducerResult<any, any, any>>((resolve) => {
            setTimeout(
              () =>
                resolve({
                  content: "slow-but-stored",
                  directives: { freshUntilAge: 10 },
                }),
              100,
            );
          }),
      );

      const slowSut = wrapProducer(
        { cache, collapseOverlappingRequestsTime: 0 },
        slowFetcher,
      );

      const resultPromise = slowSut(
        { id: "mid-producer-abort" },
        { signal: controller.signal },
      );

      // Abort mid-producer-call — caller should reject immediately.
      await delay(10);
      controller.abort(new Error("cancelled-mid-producer"));

      try {
        await resultPromise;
        throw new Error("should have rejected");
      } catch (e) {
        expect((e as Error).message).to.eq("cancelled-mid-producer");
      }

      // Wait for the producer to finish and store its result.
      await delay(150);

      // Verify the result was still stored: a subsequent request should hit.
      const result = await slowSut({ id: "mid-producer-abort" });
      expect(result.content).to.eq("slow-but-stored");
      expect(slowFetcher.mock.callCount()).to.eq(1);
    });

    it("should reject the aborting caller but not the other, and still store the collapsed result", async () => {
      const controller = new AbortController();

      const slowFetcher = mock.fn(
        async () =>
          new Promise<RequestPairedProducerResult<any, any, any>>((resolve) => {
            setTimeout(
              () =>
                resolve({
                  content: "shared-result",
                  directives: { freshUntilAge: 10 },
                }),
              80,
            );
          }),
      );

      const collapsingSut = wrapProducer(
        { cache, collapseOverlappingRequestsTime: 10 },
        slowFetcher,
      );

      const req = { id: "collapse-abort-store" };

      const abortablePromise = collapsingSut(req, {
        signal: controller.signal,
      });
      const normalPromise = collapsingSut(req);

      await delay(5);
      controller.abort(new Error("caller1-aborted"));

      // The aborting caller should reject immediately.
      try {
        await abortablePromise;
        throw new Error("should have rejected");
      } catch (e) {
        expect((e as Error).message).to.eq("caller1-aborted");
      }

      // The non-aborting caller should still get the result.
      const normalResult = await normalPromise;
      expect(normalResult.content).to.eq("shared-result");

      // Only one producer call should have been made (collapsed)
      expect(slowFetcher.mock.callCount()).to.eq(1);

      // And the result should be stored — a third call should hit the cache.
      await delay(10);
      const cachedResult = await collapsingSut(req);
      expect(cachedResult.content).to.eq("shared-result");
      expect(slowFetcher.mock.callCount()).to.eq(1);
    });

    it("should still return a cache hit even when signal is provided", async () => {
      const controller = new AbortController();

      // First, populate the cache
      await sut({ id: "cached-signal-test" });
      expect(fetcher.mock.callCount()).to.eq(1);

      // Second call with signal should still get a cache hit
      const result = await sut(
        { id: "cached-signal-test" },
        { signal: controller.signal },
      );
      expect(result.content).to.be.a("string");
      expect(fetcher.mock.callCount()).to.eq(1);
    });

    it("should not abort usableIfError fallback when signal is aborted", async () => {
      const controller = new AbortController();
      controller.abort(new Error("abort-before-call"));

      const errorProducer = mock.fn(async () => {
        throw new Error("producer-error");
      });
      const sut2 = wrapProducer({ cache }, errorProducer);

      // With an already-aborted signal, the call should reject immediately
      // (before even checking the cache or calling the producer)
      try {
        await sut2({ id: "if-error-abort" }, { signal: controller.signal });
        throw new Error("should have rejected");
      } catch (e) {
        expect((e as Error).message).to.eq("abort-before-call");
      }

      expect(errorProducer.mock.callCount()).to.eq(0);
    });

    it("should not treat abort errors as cache read failures eligible for fallback", async () => {
      const controller = new AbortController();
      const mockCache = makeTestCache();

      // Make cache.get throw an abort error
      mockCache.get = async (_req, options) => {
        options?.signal?.throwIfAborted();
        return { validatable: [] };
      };

      const producerFn = mock.fn(async () => ({
        content: null,
        directives: { freshUntilAge: 1 },
      }));

      const sut2 = wrapProducer(
        { cache: mockCache, onCacheReadFailure: "call-producer" },
        producerFn,
      );

      controller.abort(new Error("aborted-during-cache-read"));

      try {
        await sut2({ id: "test" }, { signal: controller.signal });
        throw new Error("should have rejected");
      } catch (e) {
        // Should propagate the abort error, NOT fall back to the producer
        expect((e as Error).message).to.eq("aborted-during-cache-read");
      }

      expect(producerFn.mock.callCount()).to.eq(0);

      await mockCache.close();
    });
  });

  describe("in-flight stores", () => {
    /**
     * A MemoryStore whose `store()` does not complete until the test says so:
     * each call parks on a gate the test can release (the write then lands in
     * memory as usual) or fail (the write is lost and `store()` rejects). No
     * timers anywhere -- the window under test is held open exactly as long
     * as the test needs it.
     */
    class GatedStore extends MemoryStore<SpecOf<typeof testRegistry>> {
      readonly gates: { release: () => void; fail: (e: unknown) => void }[] =
        [];

      override async store(
        ...args: Parameters<MemoryStore<SpecOf<typeof testRegistry>>["store"]>
      ) {
        await new Promise<void>((resolve, reject) => {
          this.gates.push({ release: resolve, fail: reject });
        });
        return super.store(...args);
      }

      releaseAll() {
        this.gates.splice(0).forEach((gate) => gate.release());
      }

      failAll(error: unknown) {
        this.gates.splice(0).forEach((gate) => gate.fail(error));
      }
    }

    /**
     * Resolves once the wrapper's own `cache.store()` promise for the i-th
     * store call has settled AND the wrapper's settlement handlers have run.
     * The wrapper attaches those handlers synchronously when it fires the
     * store, so a handler attached here (later) runs after them.
     */
    const storeSettled = async (
      storeSpy: { mock: { calls: { result?: unknown }[] } },
      i: number,
    ) => {
      const result = storeSpy.mock.calls[i]?.result;
      expect(result).to.be.instanceOf(Promise);
      await (result as Promise<unknown>).then(
        () => undefined,
        () => undefined,
      );
    };

    let store: GatedStore;
    let gatedCache: TestCache;
    let producer: ReturnType<
      typeof mock.fn<
        (it: {
          id: string;
          params: AnyParams;
        }) => Promise<RequestPairedProducerResult<any, any, any>>
      >
    >;
    let gatedSut: WrappedFn;

    beforeEach(() => {
      store = new GatedStore();
      gatedCache = new Cache({ store, ...testCacheOptions });
      producer = mock.fn(async (req) => ({
        content: `produced-${producer.mock.callCount()}`,
        directives: { freshUntilAge: 100 },
        ...(req.id === "with-supplemental"
          ? {
              supplementalResources: [
                {
                  id: "supplemental",
                  content: "supplemental-content",
                  directives: { freshUntilAge: 100 },
                },
              ],
            }
          : {}),
      }));
      // Collapse window 0: no call rides another's invocation unless they
      // start in the same millisecond, so the tests below exercise the
      // overlay, not request collapsing (which has its own test here).
      gatedSut = wrapProducer(
        { cache: gatedCache, collapseOverlappingRequestsTime: 0 },
        producer,
      );
    });

    afterEach(async () => {
      store.releaseAll();
      await gatedCache.close();
    });

    it("an immediate sequential repeat is a hit while the store is in flight: the producer runs once", async () => {
      const storeGet = mock.method(store, "get");
      const storeSpy = mock.method(gatedCache, "store");

      const first = await gatedSut({ id: "x" });
      expect(producer.mock.callCount()).to.eq(1);
      expect(store.gates).to.have.lengthOf(1); // the write is parked
      expect(storeGet.mock.callCount()).to.eq(1); // the miss that led to it

      const second = await gatedSut({ id: "x" });
      expect(producer.mock.callCount()).to.eq(1);
      expect(second.content).to.eq(first.content);
      // Served from the overlay, so the store was not even read.
      expect(storeGet.mock.callCount()).to.eq(1);

      // Once the write lands, the store answers and the overlay is empty:
      // the next call is a store read that hits.
      store.releaseAll();
      await storeSettled(storeSpy, 0);
      const third = await gatedSut({ id: "x" });
      expect(producer.mock.callCount()).to.eq(1);
      expect(third.content).to.eq(first.content);
      expect(storeGet.mock.callCount()).to.eq(2);
    });

    it("a failed store does not pin the entry: the next call goes back to the producer", async () => {
      const storeSpy = mock.method(gatedCache, "store");

      await gatedSut({ id: "x" });
      expect(producer.mock.callCount()).to.eq(1);

      // Still a hit while the (doomed) write is in flight...
      await gatedSut({ id: "x" });
      expect(producer.mock.callCount()).to.eq(1);

      // ...but once it has failed, the overlay forgets the entry and the
      // store never had it, so the producer runs again.
      store.failAll(new Error("disk full"));
      await storeSettled(storeSpy, 0);

      const afterFailure = await gatedSut({ id: "x" });
      expect(producer.mock.callCount()).to.eq(2);
      expect(afterFailure.content).to.eq("produced-1");

      // And that second write, once it lands, is what later calls hit.
      store.releaseAll();
      await storeSettled(storeSpy, 1);
      const afterSuccess = await gatedSut({ id: "x" });
      expect(producer.mock.callCount()).to.eq(2);
      expect(afterSuccess.content).to.eq("produced-1");
    });

    it("concurrent duplicates still collapse onto one producer call, and the collapsed result is a hit while its store is in flight", async () => {
      let resolveProducer!: (
        it: RequestPairedProducerResult<any, any, any>,
      ) => void;
      let onProducerCalled!: () => void;
      const producerCalled = new Promise<void>((resolve) => {
        onProducerCalled = resolve;
      });
      // The FIRST invocation parks until the test resolves it (so both
      // callers can attach to it); any later one resolves at once with
      // distinguishable content, so a regression here fails rather than
      // hangs.
      let invocations = 0;
      const slowProducer = mock.fn(
        async (): Promise<RequestPairedProducerResult<any, any, any>> => {
          invocations += 1;
          return invocations === 1
            ? new Promise((resolve) => {
                resolveProducer = resolve;
                onProducerCalled();
              })
            : { content: "not-shared", directives: { freshUntilAge: 100 } };
        },
      );
      const collapsingSut: WrappedFn = wrapProducer(
        { cache: gatedCache, collapseOverlappingRequestsTime: 10 },
        slowProducer,
      );

      const a = collapsingSut({ id: "x" });
      const b = collapsingSut({ id: "x" });
      await producerCalled;

      resolveProducer({
        content: "shared",
        directives: { freshUntilAge: 100 },
      });
      const [resA, resB] = await Promise.all([a, b]);
      expect(resA.content).to.eq("shared");
      expect(resB.content).to.eq("shared");
      expect(slowProducer.mock.callCount()).to.eq(1);

      // The invocation is over (so nothing to collapse onto) and its write is
      // parked: a third, sequential call is answered from the overlay.
      expect(store.gates).to.have.lengthOf(1);
      const resC = await collapsingSut({ id: "x" });
      expect(resC.content).to.eq("shared");
      expect(slowProducer.mock.callCount()).to.eq(1);
    });

    it("honors variants: a pending entry is only a hit for requests its vary matches", async () => {
      const varyingProducer = mock.fn(
        async (req: { id: string; params: AnyParams }) => ({
          content: `lang=${req.params["lang"] as string}`,
          vary: { lang: req.params["lang"] },
          directives: { freshUntilAge: 100 },
        }),
      );
      const varyingSut: WrappedFn = wrapProducer(
        { cache: gatedCache, collapseOverlappingRequestsTime: 0 },
        varyingProducer,
      );

      const fr = await varyingSut({ id: "x", params: { lang: "fr" } });
      expect(fr.content).to.eq("lang=fr");
      expect(varyingProducer.mock.callCount()).to.eq(1);

      // Same id, different variant: the pending `fr` entry must not answer.
      const en = await varyingSut({ id: "x", params: { lang: "en" } });
      expect(en.content).to.eq("lang=en");
      expect(varyingProducer.mock.callCount()).to.eq(2);

      // Both writes are still parked; each variant is a hit for its own params.
      expect(store.gates).to.have.lengthOf(2);
      expect(
        (await varyingSut({ id: "x", params: { lang: "fr" } })).content,
      ).to.eq("lang=fr");
      expect(
        (await varyingSut({ id: "x", params: { lang: "en" } })).content,
      ).to.eq("lang=en");
      expect(varyingProducer.mock.callCount()).to.eq(2);
    });

    it("honors consumer directives: a bypass request (maxAge: 0) still reaches the producer", async () => {
      await gatedSut({ id: "x" });
      expect(producer.mock.callCount()).to.eq(1);

      await gatedSut({ id: "x", directives: { maxAge: 0 } });
      expect(producer.mock.callCount()).to.eq(2);
    });

    it("does not serve an entry the store would not retain (storeFor: 0)", async () => {
      const unstorableProducer = mock.fn(async () => ({
        content: "do-not-store",
        directives: { freshUntilAge: 100, storeFor: 0 },
      }));
      const unstorableSut: WrappedFn = wrapProducer(
        { cache: gatedCache, collapseOverlappingRequestsTime: 0 },
        unstorableProducer,
      );

      await unstorableSut({ id: "x" });
      await unstorableSut({ id: "x" });
      expect(unstorableProducer.mock.callCount()).to.eq(2);
    });

    it("a supplemental resource whose store is in flight is a hit for its own id", async () => {
      await gatedSut({ id: "with-supplemental" });
      expect(producer.mock.callCount()).to.eq(1);
      expect(store.gates).to.have.lengthOf(1);

      const supplemental = await gatedSut({ id: "supplemental" });
      expect(supplemental.content).to.eq("supplemental-content");
      expect(producer.mock.callCount()).to.eq(1);
    });

    it("the overlay is per wrapper: a second wrapper over the same cache reads the store", async () => {
      const otherProducer = mock.fn(async () => ({
        content: "other",
        directives: { freshUntilAge: 100 },
      }));
      const otherSut: WrappedFn = wrapProducer(
        { cache: gatedCache, collapseOverlappingRequestsTime: 0 },
        otherProducer,
      );

      await gatedSut({ id: "x" });
      expect(producer.mock.callCount()).to.eq(1);

      // The first wrapper's write is still parked, and the second wrapper
      // has no view of it: it misses and produces.
      await otherSut({ id: "x" });
      expect(otherProducer.mock.callCount()).to.eq(1);
    });
  });
});
/* eslint-enable @typescript-eslint/no-explicit-any */
