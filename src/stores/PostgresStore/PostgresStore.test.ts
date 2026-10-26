import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe, it, mock } from "node:test";
import pg from "pg";
import type {
  AnyValidators,
  CacheSpec,
  Entry,
  NormalizedParams,
  NormalizedProducerDirectives,
  NormalizedVary,
  StoreEntryInput,
} from "../../types/index.js";
import PostgresStore, {
  type PostgresStoreSupportedParams,
} from "./PostgresStore.js";

type Spec = CacheSpec<string, { value: number }>;
type Params = PostgresStoreSupportedParams;
type Input = StoreEntryInput<Spec, AnyValidators, Params>;
type Statement = { sql: string; parameters: readonly unknown[] };
const emptyVary = {} as NormalizedVary<Params>;
const directives = { freshUntilAge: 60 } as NormalizedProducerDirectives;
function input(id: string, value: number, date = new Date(1_000)): Input {
  const entry: Entry<Spec, AnyValidators, Params> = {
    id,
    content: { value },
    vary: emptyVary,
    directives,
    validators: { etag: String(value) },
    initialAge: 0,
    date,
  };
  return { entry, maxStoreForSeconds: 60 };
}
async function fixture() {
  const schema = `cache_bulk_${randomUUID().replaceAll("-", "")}`;
  const qualified = `"${schema}"."entries"`;
  const pool = new pg.Pool({
    host: process.env["DATABASE_HOST"],
    port: Number(process.env["DATABASE_PORT"]),
    user: process.env["DATABASE_USER"],
    password: process.env["DATABASE_PASSWORD"],
    database: process.env["DATABASE_NAME"],
    statement_timeout: 15_000,
  });
  const store = new PostgresStore<Spec, AnyValidators, Params>(pool, {
    schemaName: schema,
    tableName: "entries",
  });
  await store.get("initialize", {} as NormalizedParams<Params>);
  return {
    store,
    pool,
    qualified,
    cleanup: async () => {
      await pool.query(`drop schema "${schema}" cascade`);
      await pool.end();
    },
  };
}
function observeQueries(
  beforeQuery?: (statement: Statement) => Promise<void> | undefined,
) {
  const statements: Statement[] = [];
  // oxlint-disable-next-line typescript-eslint/unbound-method -- Reflect.apply below restores each actual client as the receiver.
  const original = pg.Client.prototype.query;
  const query = mock.method(pg.Client.prototype, "query", function (
    this: pg.Client,
    ...args: unknown[]
  ) {
    const statement = {
      sql: String(args[0]),
      parameters: Array.isArray(args[1]) ? args[1] : [],
    };
    statements.push(statement);
    const pending = beforeQuery?.(statement);
    return pending === undefined
      ? Reflect.apply(original, this, args)
      : pending.then(() => Reflect.apply(original, this, args));
  } as typeof pg.Client.prototype.query);
  return { statements, restore: () => query.mock.restore() };
}
const postgresConfigured = [
  "DATABASE_HOST",
  "DATABASE_PORT",
  "DATABASE_USER",
  "DATABASE_PASSWORD",
  "DATABASE_NAME",
].every((key) => process.env[key] !== undefined);

describe(
  "PostgresStore bulk persistence",
  postgresConfigured ? {} : { skip: "Postgres is not configured" },
  () => {
    it("keeps the one-statement fast path for a small write", async () => {
      const f = await fixture();
      const observed = observeQueries();
      try {
        assert.deepEqual(await f.store.store([input("one", 1)]), [
          { relationshipToExistingStoredData: "is-new" },
        ]);
        assert.equal(observed.statements.length, 1);
        assert.match(observed.statements[0]!.sql, /^with "old"/i);
      } finally {
        observed.restore();
        await f.cleanup();
      }
    });

    it(
      "stores 23000 unique slots beyond the former bind limit with ordered duplicate metadata",
      { timeout: 30_000 },
      async () => {
        const f = await fixture();
        const observed = observeQueries();
        const entries = Array.from({ length: 23_000 }, (_, index) =>
          input(`id:${String(index).padStart(5, "0")}`, 1),
        );
        entries[0] = input(
          JSON.stringify(["visit", 'quote" and slash\\ and newline\n']),
          1,
        );
        entries.push(
          input("id:01000", 2, new Date(2_000)),
          input("id:01000", 0, new Date(0)),
        );
        try {
          const results = await f.store.store(entries.toReversed());
          assert.equal(results.length, entries.length);
          assert.deepEqual(results[0], {});
          assert.deepEqual(results[1], {
            relationshipToExistingStoredData: "is-new",
          });
          assert.deepEqual(results[entries.length - 1 - 1_000], {});
          assert.equal(
            results.filter(
              (result) => result.relationshipToExistingStoredData === "is-new",
            ).length,
            23_000,
          );
          const inserts = observed.statements.filter((statement) =>
            /^insert into /i.test(statement.sql),
          );
          assert.equal(inserts.length, 23);
          assert.ok(
            inserts.every((statement) => statement.parameters.length <= 3_000),
          );
          const metadata = observed.statements.filter(
            (statement) =>
              /^select /i.test(statement.sql) &&
              statement.sql.includes("'validators'"),
          );
          assert.equal(metadata.length, 1);
          assert.equal(metadata[0]!.parameters.length, 1);
          const rows = await f.pool.query<{ count: number }>(
            `select count(*)::int as count from ${f.qualified}`,
          );
          assert.equal(rows.rows[0]!.count, 23_000);
          const winner = await f.store.get(
            "id:01000",
            {} as NormalizedParams<Params>,
          );
          assert.equal(winner[0]!.content.value, 2);
          const repeated = await f.store.store(entries.toReversed());
          assert.equal(
            repeated.filter(
              (result) =>
                result.relationshipToExistingStoredData === "unchanged",
            ).length,
            23_000,
          );
        } finally {
          observed.restore();
          await f.cleanup();
        }
      },
    );

    it("rolls every batch back if a later batch fails", async () => {
      const f = await fixture();
      await f.store.store([input("id:00000", 1)]);
      await f.pool.query(
        `alter table ${f.qualified} add constraint reject_last check (resource_id <> 'zz:reject')`,
      );
      const observed = observeQueries();
      try {
        const entries = Array.from({ length: 1_500 }, (_, index) =>
          input(`id:${String(index).padStart(5, "0")}`, 2),
        );
        entries.push(input("zz:reject", 2));
        await assert.rejects(
          f.store.store(entries),
          (error: unknown) =>
            typeof error === "object" &&
            error !== null &&
            "code" in error &&
            error.code === "23514",
        );
        assert.equal(
          observed.statements.filter((statement) =>
            /^insert into /i.test(statement.sql),
          ).length,
          2,
        );
        assert.ok(
          observed.statements.some((statement) =>
            /^rollback/i.test(statement.sql),
          ),
        );
        const rows = await f.pool.query<{
          resource_id: string;
          entry: { content: { value: number } };
        }>(`select resource_id, entry from ${f.qualified}`);
        assert.equal(rows.rows.length, 1);
        assert.equal(rows.rows[0]!.resource_id, "id:00000");
        assert.equal(rows.rows[0]!.entry.content.value, 1);
      } finally {
        observed.restore();
        await f.cleanup();
      }
    });

    it("compares every batch against one pre-write metadata snapshot", async () => {
      const f = await fixture();
      const keys = Array.from(
        { length: 2_001 },
        (_, index) => `id:${String(index).padStart(5, "0")}`,
      );
      await f.store.store(keys.map((id) => input(id, 1)));
      const lateId = keys[1_500]!;
      const progress = { writes: 0, interfered: false };
      const observed = observeQueries((statement) => {
        if (/^insert into /i.test(statement.sql)) {
          progress.writes += 1;
          if (progress.writes === 2) {
            return f.pool
              .query(
                `update ${f.qualified} set entry = $1::jsonb where resource_id = $2`,
                [JSON.stringify(input(lateId, 9).entry), lateId],
              )
              .then(() => {
                progress.interfered = true;
              });
          }
        }
        return undefined;
      });
      try {
        const results = await f.store.store(
          keys.map((id) => input(id, id === lateId ? 1 : 2)),
        );
        assert.equal(progress.interfered, true);
        assert.deepEqual(results[1_500], {
          relationshipToExistingStoredData: "unchanged",
        });
        assert.deepEqual(results[0], {
          relationshipToExistingStoredData: "changed",
        });
        const stored = await f.store.get(
          lateId,
          {} as NormalizedParams<Params>,
        );
        assert.equal(stored[0]!.content.value, 1);
      } finally {
        observed.restore();
        await f.cleanup();
      }
    });

    it(
      "uses the legacy global UTF-16 lock order across concurrent old and batched writers",
      { timeout: 30_000 },
      async () => {
        const f = await fixture();
        const keys = [
          ...Array.from(
            { length: 2_001 },
            (_, index) => `id:${String(index).padStart(5, "0")}`,
          ),
          "id:\u{10000}",
          "id:\ue000",
        ];
        await f.store.store(keys.map((id) => input(id, 1)));
        const sortedKeys = keys.toSorted((left, right) =>
          left < right ? -1 : left > right ? 1 : 0,
        );
        const legacyParameters = sortedKeys.flatMap((id) => [
          id,
          "{}",
          JSON.stringify(input(id, 3).entry),
        ]);
        const legacySql = `insert into ${f.qualified} (resource_id, vary, entry) values ${sortedKeys.map((_id, index) => `($${3 * index + 1}, $${3 * index + 2}, $${3 * index + 3})`).join(",")} on conflict (resource_id, vary) do update set entry = excluded.entry`;
        const bothReady = Promise.withResolvers<void>();
        const state = { firstWrites: 0 };
        const observed = observeQueries((statement) => {
          if (
            /^insert into /i.test(statement.sql) &&
            statement.sql.includes(f.qualified) &&
            state.firstWrites < 2
          ) {
            state.firstWrites += 1;
            if (state.firstWrites === 2) bothReady.resolve();
            return bothReady.promise;
          }
          return undefined;
        });
        try {
          await Promise.all([
            f.store.store(keys.toReversed().map((id) => input(id, 2))),
            f.pool.query(legacySql, legacyParameters),
          ]);
          const newWrites = observed.statements.filter(
            (statement) =>
              /^insert into /i.test(statement.sql) &&
              statement.parameters.length > 0 &&
              JSON.parse(String(statement.parameters[2])).content.value === 2,
          );
          assert.deepEqual(
            newWrites.flatMap((statement) =>
              statement.parameters.filter(
                (_parameter, index) => index % 3 === 0,
              ),
            ),
            sortedKeys,
          );
          const values = await f.pool.query<{ value: number }>(
            `select distinct (entry->'content'->>'value')::int as value from ${f.qualified}`,
          );
          assert.equal(values.rows.length, 1);
          assert.ok(values.rows[0]!.value === 2 || values.rows[0]!.value === 3);
        } finally {
          bothReady.resolve();
          observed.restore();
          await f.cleanup();
        }
      },
    );
  },
);
