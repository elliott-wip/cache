import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  postgresEnvironmentIsConfigured,
  postgresStoreFixture,
} from "../../../test/fixtures.js";
import type {
  AnyValidators,
  CacheSpec,
  Entry,
  NormalizedParams,
  NormalizedProducerDirectives,
  NormalizedVary,
} from "../../types/index.js";
import type { PostgresStoreSupportedParams } from "./PostgresStore.js";

type StringSpec = CacheSpec<string, string>;

const noParams = {} as NormalizedParams<PostgresStoreSupportedParams>;

function makeEntry(
  id: string,
): Entry<StringSpec, AnyValidators, PostgresStoreSupportedParams> {
  return {
    id,
    content: id,
    vary: {} as NormalizedVary<PostgresStoreSupportedParams>,
    validators: {},
    directives: { freshUntilAge: 60 } as NormalizedProducerDirectives,
    initialAge: 0,
    date: new Date("2024-01-01T00:00:00.000Z"),
  };
}

/**
 * Stores one entry per id, then runs `getMany` for those ids and returns its
 * result together with every statement that the `getMany` call sent.
 */
async function getManyStatements(ids: readonly string[]) {
  const statements: { text: string; parameterCount: number }[] = [];
  const fixture = postgresStoreFixture({
    onQuery: (text, parameters) => {
      statements.push({ text, parameterCount: parameters.length });
    },
  });
  try {
    const store = fixture.postgresStore;
    await store.store(
      ids.map((id) => ({ entry: makeEntry(id), maxStoreForSeconds: 60 })),
    );

    const before = statements.length;
    const results = await store.getMany(
      ids.map((id) => ({ id, params: noParams })),
    );
    return { results, statements: statements.slice(before) };
  } finally {
    await fixture.cleanup();
  }
}

describe(
  "PostgresStore.getMany",
  postgresEnvironmentIsConfigured()
    ? { timeout: 60_000 }
    : { skip: "Postgres environment variables are not configured" },
  () => {
    it("reads 25 ids with one select statement", async () => {
      const ids = Array.from({ length: 25 }, (_, i) => `id:${String(i)}`);

      const { results, statements } = await getManyStatements(ids);

      assert.equal(statements.length, 1);
      assert.match(statements[0]?.text ?? "", /^select .* in \(/);
      assert.deepEqual(
        results.map((entries) => entries.map(({ content }) => content)),
        ids.map((id) => [id]),
      );
    });

    it("splits more than 1,000 unique ids into statements of at most 1,000 ids", async () => {
      const ids = Array.from({ length: 1001 }, (_, i) => `id:${String(i)}`);

      const { results, statements } = await getManyStatements(ids);

      assert.deepEqual(
        statements.map(({ parameterCount }) => parameterCount),
        [1000, 1],
      );
      assert.deepEqual(
        results.map((entries) => entries.map(({ content }) => content)),
        ids.map((id) => [id]),
      );
    });

    it("queries each id once, and still answers every duplicate request", async () => {
      const { results, statements } = await getManyStatements([
        "a",
        "b",
        "a",
        "a",
      ]);

      assert.deepEqual(
        statements.map(({ parameterCount }) => parameterCount),
        [2],
      );
      assert.deepEqual(
        results.map((entries) => entries.map(({ content }) => content)),
        [["a"], ["b"], ["a"], ["a"]],
      );
    });

    it("rejects with the abort reason when the signal aborts while the query runs", async () => {
      const controller = new AbortController();
      const reason = new Error("caller gave up");
      // Armed only after the store() call below, whose statement also has an
      // `in (...)` list.
      const abortOnSelect = { armed: false };
      const fixture = postgresStoreFixture({
        onQuery: (text) => {
          // Abort as the getMany statement goes out, so the query itself
          // still completes and only the check after it can reject.
          if (abortOnSelect.armed && /^select .* in \(/.test(text)) {
            controller.abort(reason);
          }
        },
      });
      try {
        const store = fixture.postgresStore;
        await store.store([{ entry: makeEntry("a"), maxStoreForSeconds: 60 }]);
        abortOnSelect.armed = true;

        await assert.rejects(
          store.getMany([{ id: "a", params: noParams }], {
            signal: controller.signal,
          }),
          (error) => error === reason,
        );
      } finally {
        await fixture.cleanup();
      }
    });
  },
);
