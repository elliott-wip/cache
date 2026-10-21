import { chunk } from "es-toolkit";
import type {
  ColumnType,
  PostgresPool,
  RawBuilder,
  SqlBool,
} from "kysely";
import { Kysely, PostgresDialect, sql } from "kysely";
import type { Tagged } from "type-fest";
import type {
  DateString,
  Jsonify,
  JsonOf,
  JSONWithUndefined,
} from "type-party";
import { parseDateString } from "type-party/runtime/dates.js";
import { entryUtils } from "../../index.js";
import type { CacheSpec, SpecForId } from "../../types/00_CacheSpec.js";
import type { AnyParams } from "../../types/01_Params.js";
import type { AnyValidators } from "../../types/02_Validators.js";
import type {
  Entry,
  JsonifiedEntry,
  NormalizedParams,
  NormalizedVary,
} from "../../types/06_Normalization.js";
import type {
  StoreEntryResult,
  StoreGetManyResult,
} from "../../types/06_Store.js";
import type {
  EntryForId,
  Logger,
  Store,
  StoreEntryInput,
  StoreGetManyRequest,
  Vary,
} from "../../types/index.js";
import type { Bind2 } from "../../types/utils.js";
import { restoreInfinityInDirectives } from "../../utils/normalization.js";
import {
  validatorsAsStored,
  validatorsEqual,
} from "../../utils/normalizedProducerResultResourceHelpers.js";
import {
  defaultLoggersByComponent,
  jsonStringify,
  keepMaxPerGroup,
} from "../../utils/utils.js";
import { variantsMatchingRequests } from "../../utils/varyHelpers.js";

/**
 * The most unique ids that one `getMany` statement binds. Postgres allows
 * 65,535 bind parameters in one statement, so this is far below the limit. It
 * keeps each statement's `in` list and result set to a bounded size.
 */
const MAX_IDS_PER_GET_MANY_QUERY = 1_000;

/**
 * Type representing the qualified name of the cache table.
 */
type CacheTableName = Tagged<string, "CacheTableName">;

/**
 * Type representing the entry in the cache table. It's basically the same as
 * the Entry type, but the date is a string since that's what it ends up as
 * after roundtripping through JSON.
 */
type TableEntry<
  Spec extends PostgresStoreCompatibleSpec,
  Validators extends AnyValidators,
  Params extends AnyParams,
  Id extends Spec["id"] = Spec["id"],
> = JsonifiedEntry<SpecForId<Spec, Id>, Validators, Params>;

type CacheTables<
  Spec extends PostgresStoreCompatibleSpec,
  Validators extends AnyValidators,
  Params extends AnyParams,
> = {
  [key in CacheTableName]: {
    // TODO: should be <Id, Id, never>, but kysely looses the tag at some point.
    resource_id: ColumnType<string, string, never>;
    // Select type is `NormalizedVary` (not `NormalizedParams`) because the
    // stored value can carry `null` values -- a `null` vary value means "this
    // variant applies only when the request omits that param". `getMany` reads
    // this column back to match variants in JS via `variantMatchesRequest`.
    vary: ColumnType<
      Readonly<NormalizedVary<Params>>,
      JsonOf<NormalizedVary<Params>>,
      never
    >;
    entry: ColumnType<
      TableEntry<Spec, Validators, Params>,
      JsonOf<TableEntry<Spec, Validators, Params>>,
      JsonOf<TableEntry<Spec, Validators, Params>>
    >;
  };
};

/**
 * We restrict the params this store supports to _primitive_ values.
 *
 * `get` matches variants in the database (see `varyMatchesParamsSql`): a
 * non-null vary value must `jsonb`-equal the corresponding param, and a `null`
 * vary value requires the param to be *absent*. `jsonb` equality is exact at
 * every level, so object/array values would match correctly in `get`. The
 * param type is still limited to primitives, because that's all real callers
 * use, and because `getMany` matches in JS with `variantMatchesRequest`, which
 * compares with `===` and so cannot deep-compare objects. For primitives the
 * two matchers agree, which keeps `get` and `getMany` (and the Memory/Sqlite
 * stores) on one contract.
 *
 * A previous implementation matched via the JSONB containment operator
 * (`vary <@ $params`). It was replaced because containment cannot express the
 * `null`-vary rule -- `<@` requires the key to be *present* with an equal
 * value, so `{"format": null}` never matched a request that omitted `format` --
 * and because it matched partially-contained nested objects rather than
 * requiring exact equality.
 */
export type PostgresStoreSupportedParams = {
  [paramName: string]: string | number | boolean | undefined;
};

/**
 * The constraint that PostgresStore places on a `Spec`'s `content`: must be
 * JSON-serializable (potentially with undefined values present, since those
 * are stripped on serialization).
 */
export type PostgresStoreCompatibleSpec = CacheSpec<string, JSONWithUndefined>;

/**
 * Builds the SQL predicate that keeps only the stored variants (rows) whose
 * `vary` requirements are satisfied by a request's `params`, implementing the
 * full matching contract in the database.
 *
 * A row matches iff *no* vary entry is violated. For each `(key, value)` in the
 * row's `vary`:
 *   - a `null` value means "the param must be *absent*", so it's violated when
 *     `params` contains that key; and
 *   - a non-null value must `jsonb`-equal `params -> key` (which is SQL `NULL`,
 *     hence `IS DISTINCT FROM`, when the param is missing).
 *
 * `resource_id` (indexed) has already narrowed the candidate rows to the few
 * variants stored for one id, so the per-row `jsonb_each` scan is cheap. We use
 * the `jsonb_exists` function rather than the `?` operator because `?` is
 * ambiguous with driver parameter placeholders.
 */
function varyMatchesParamsSql<Params extends PostgresStoreSupportedParams>(
  params: Readonly<NormalizedParams<Params>>,
): RawBuilder<SqlBool> {
  const paramsJson = jsonStringify(params);
  return sql<SqlBool>`not exists (
    select 1
    from jsonb_each(${sql.ref("vary")}) as v(key, value)
    where case
      when jsonb_typeof(v.value) = 'null' then jsonb_exists(${paramsJson}::jsonb, v.key)
      else (${paramsJson}::jsonb -> v.key) is distinct from v.value
    end
  )`;
}

/**
 * This class implements a store for cache entries, backed by Postgres. For
 * details on each method, see the Store interface.
 *
 * Each row in the table is a separate cache entry, uniquely identified by
 * (resourceId, vary) pair. Each of these rows stores the id (string), vary
 * (jsonb), and a full entry (jsonb).
 *
 * We do not use any mechanisms to speed up the entry lookup aside from an index
 * on the vary column. We rely on Postgres to be fast enough for our needs.
 *
 * `getMany` reads up to {@link MAX_IDS_PER_GET_MANY_QUERY} unique ids in one
 * round trip (see its docs), so
 * a caller with many ids should use it, or a `Cache` with `batchReads`, rather
 * than many concurrent `get` calls: each `get` is its own query and holds its
 * own pool connection.
 */
export default class PostgresStore<
  Spec extends PostgresStoreCompatibleSpec = PostgresStoreCompatibleSpec,
  Validators extends AnyValidators = AnyValidators,
  Params extends PostgresStoreSupportedParams = PostgresStoreSupportedParams,
> implements Store<Spec, Validators, Params> {
  /** Object containing info about the schema and table name */
  private readonly tableNameData: {
    schemaName: string;
    tableName: string;
    qualifiedName: CacheTableName;
  };
  private readonly db: Kysely<CacheTables<Spec, Validators, Params>>;
  /** Promise that resolves when the required tables are initialized */
  private ensureInitializedPromise: Promise<void>;

  private readonly logInfo: Bind2<Logger, "postgres-store", "info">;
  private readonly logTrace: Bind2<Logger, "postgres-store", "trace">;
  private readonly logError: Bind2<Logger, "postgres-store", "error">;
  private readonly logWarn: Bind2<Logger, "postgres-store", "warn">;

  /**
   * @param pool - The postgres pool to use. Typed as Kysely's structural
   *  `PostgresPool` rather than `pg`'s `Pool`, so that this package's
   *  published declarations do not force consumers to install `@types/pg`.
   *  A real `pg.Pool` satisfies it.
   * @param opts.schemaName - The name of the schema to use
   * @param opts.tableName - The name of the table to use
   * @param opts.logger - Optional custom logger to use. Defaults to using
   *  the debug module with the @zingage/cache:postgres-store namespace
   */
  constructor(
    pool: PostgresPool,
    opts: {
      schemaName: string;
      tableName: string;
      logger?: Logger;
      assumeIsInitialized?: boolean;
    },
  ) {
    const unboundLogger =
      opts.logger ?? defaultLoggersByComponent["postgres-store"];

    this.logInfo = unboundLogger.bind(null, "postgres-store", "info");
    this.logTrace = unboundLogger.bind(null, "postgres-store", "trace");
    this.logError = unboundLogger.bind(null, "postgres-store", "error");
    this.logWarn = unboundLogger.bind(null, "postgres-store", "warn");

    this.db = new Kysely({ dialect: new PostgresDialect({ pool }) });
    this.tableNameData = this.getTableNameData(opts.schemaName, opts.tableName);
    this.ensureInitializedPromise = opts.assumeIsInitialized
      ? Promise.resolve()
      : this.ensureInitialized();
  }

  async get<Id extends Spec["id"]>(
    id: Id,
    params: Readonly<NormalizedParams<Params>>,
    options?: { signal?: AbortSignal },
  ): Promise<EntryForId<Spec, Validators, Params, Id>[]> {
    const signal = options?.signal;
    signal?.throwIfAborted();

    this.logTrace("querying for entries with id and params", {
      id,
      params,
    });
    await this.ensureInitializedPromise;

    signal?.throwIfAborted();

    const result = await this.db
      .selectFrom(this.tableName)
      .where("resource_id", "=", id)
      .where(varyMatchesParamsSql(params))
      .selectAll()
      .execute();

    signal?.throwIfAborted();

    const entries = result.map((it) =>
      this.deserializeEntry(
        it.entry satisfies TableEntry<
          Spec,
          Validators,
          Params,
          Spec["id"]
        > as unknown as TableEntry<Spec, Validators, Params, Id>,
      ),
    );

    this.logTrace("returning entries from postgres query", entries);
    return entries;
  }

  /**
   * Reads the entries for all requests with one `resource_id in (...)`
   * statement per {@link MAX_IDS_PER_GET_MANY_QUERY} unique ids. It then
   * filters each request's rows in JS with `variantMatchesRequest`, as
   * `SqliteStore` does, rather than in SQL as `get` does.
   *
   * The two matchers agree for every param value that both can evaluate:
   * `get`'s `jsonb` equality and `variantMatchesRequest`'s `===` give the same
   * answer for strings, numbers, and booleans, and both read a `null` vary
   * value as "the param must be absent". The store-conformance suite checks
   * this agreement with a property test. One difference: Postgres cannot read
   * a `jsonb` string that holds U+0000 or a lone surrogate, so `get` rejects a
   * request with such a param value, and `getMany` matches it in JS. No stored
   * variant can hold such a value, because `store` rejects it too.
   *
   * Unlike `get`, this loads EVERY stored variant of each requested id into
   * JS. That is cheap while each id holds a few variants, which is the usual
   * case. A cache whose producers vary on a high-cardinality param (one
   * variant per user, say) should not read through `getMany`; see
   * `docs/implementing-a-store.md`.
   *
   * The statements for a batch of more than one chunk are not in one
   * transaction, so a concurrent write can land between them. That is the same
   * consistency that separate `get` calls give, and it is enough for a cache.
   */
  async getMany<
    const Reqs extends readonly StoreGetManyRequest<Spec, Params>[],
  >(
    requests: Reqs,
    options?: { signal?: AbortSignal },
  ): Promise<StoreGetManyResult<Spec, Reqs, Validators, Params>> {
    const signal = options?.signal;
    signal?.throwIfAborted();

    this.logTrace("querying for multiple entries", {
      requestCount: requests.length,
    });

    if (requests.length === 0) {
      return [] as StoreGetManyResult<Spec, Reqs, Validators, Params>;
    }

    await this.ensureInitializedPromise;

    signal?.throwIfAborted();

    const uniqueIds = [...new Set(requests.map(({ id }) => id))];

    // Chunks run one after another, so a large batch holds one pool connection
    // at a time instead of one per chunk.
    const rows = [];
    for (const ids of chunk(uniqueIds, MAX_IDS_PER_GET_MANY_QUERY)) {
      rows.push(
        ...(await this.db
          .selectFrom(this.tableName)
          .where("resource_id", "in", ids)
          .selectAll()
          .execute()),
      );
      signal?.throwIfAborted();
    }

    // Each row is deserialized once, so duplicate requests for one id share
    // entry objects, as concurrent `MemoryStore` reads do. That is sound while
    // no reader mutates a returned entry, and `Cache` and the producer
    // wrappers do not.
    const variants = rows.map((row) => ({
      id: row.resource_id,
      vary: row.vary,
      entry: this.deserializeEntry(row.entry),
    }));
    const results = variantsMatchingRequests(variants, requests).map(
      (matches) => matches.map(({ entry }) => entry),
    );

    this.logTrace("returning entries from postgres getMany query", {
      requestCount: requests.length,
      uniqueIdCount: uniqueIds.length,
      rowCount: rows.length,
    });

    return results as StoreGetManyResult<Spec, Reqs, Validators, Params>;
  }

  async store(
    entries: readonly StoreEntryInput<Spec, Validators, Params>[],
  ): Promise<readonly StoreEntryResult[]> {
    this.logTrace("storing entries", entries);
    await this.ensureInitializedPromise;

    // Early return if there are no entries to store
    if (entries.length === 0) {
      this.logTrace("no entries to store, returning early");
      return [];
    }

    // Postgres only allows an ON CONFLICT to affect the same key once per
    // query, so collapse entries that share a slot (id + vary), keeping the
    // newest by birth date. Dropped duplicates report `{}` (they weren't the
    // value compared/stored).
    const deduped = keepMaxPerGroup({
      items: entries.map((it, originalIndex) => ({ it, originalIndex })),
      groupBy: ({ it }) => this.slotKey(it.entry),
      maxBy: ({ it }) => entryUtils.birthDate(it.entry).getTime(),
    });

    // Sort the rows by a deterministic key so that every store() call acquires
    // its row locks in the same global order. Postgres processes a multi-row
    // INSERT's rows in order, locking each conflicting row as it goes, so two
    // concurrent calls whose batches share >= 2 slots in DIFFERENT orders
    // could otherwise deadlock (each holding a row the other is waiting on).
    // With a consistent order, the later call just waits for the earlier one.
    const rows = deduped
      .map(({ it }) => ({
        resource_id: it.entry.id,
        vary: this.serializeVary(it.entry.vary),
        entry: this.serializeEntry(it.entry),
      }))
      .toSorted((a, b) =>
        a.resource_id < b.resource_id
          ? -1
          : a.resource_id > b.resource_id
            ? 1
            : a.vary < b.vary
              ? -1
              : a.vary > b.vary
                ? 1
                : 0,
      );
    const resourceIds = [...new Set(deduped.map(({ it }) => it.entry.id))];

    // One statement with two sibling CTEs sharing a single snapshot: `old` reads
    // each affected slot's validators as they were BEFORE this call, while
    // `upsert` overwrites the rows. A data-modifying CTE runs even though the
    // final query doesn't reference `upsert`, so the write still happens; and
    // because both CTEs see the pre-statement snapshot, `old` reflects the
    // state before the upsert. We select `old` back to compare in JS (the same
    // `validatorsEqual` the other stores use), rather than comparing in SQL.
    //
    // Concurrency caveat: this store plays a little loose with the contract's
    // "as it was before this store() call" under a same-slot race. The
    // classification comes from this statement's snapshot, but ON CONFLICT DO
    // UPDATE can still see -- and overwrite -- a row a concurrent writer
    // committed after that snapshot was taken. In that window we may report
    // "is-new" (or a stale changed/unchanged) even though the upsert actually
    // replaced the concurrent write. We accept that approximation for cache
    // workloads rather than pay for row locks (SELECT ... FOR UPDATE) on
    // every store; MemoryStore (synchronous) and SqliteStore (one write
    // transaction) are exact.
    // NB: kysely can't keep the row type for columns whose type depends on this
    // class's `Params`/`Validators` generics, so -- as in `get` -- we cast the
    // read-back rows to their known shape. The query *construction* above is
    // still fully checked by kysely (table, columns, insert shape, on-conflict).
    const oldRows = (await this.db
      .with("old", (c) =>
        c
          .selectFrom(this.tableName)
          .where("resource_id", "in", resourceIds)
          .select((eb) => [
            "resource_id",
            "vary",
            sql<Partial<Validators>>`${eb.ref("entry")} -> 'validators'`.as(
              "validators",
            ),
          ]),
      )
      .with("upsert", (c) =>
        c
          .insertInto(this.tableName)
          .values(rows)
          .onConflict((oc) =>
            oc
              .columns(["resource_id", "vary"])
              .doUpdateSet((eb) => ({ entry: eb.ref("excluded.entry") })),
          ),
      )
      .selectFrom("old")
      .selectAll("old")
      .execute()
      .catch((error: unknown) => {
        this.logError("failed to store entries", error);
        throw error;
      })) as unknown as readonly {
      resource_id: string;
      vary: Readonly<NormalizedVary<Params>>;
      validators: Partial<Validators>;
    }[];
    this.logTrace("stored entries successfully");

    // Index each affected slot's pre-call validators, so a slot with no row here
    // is one that didn't exist before the call (i.e. "is-new").
    const oldValidatorsBySlot = new Map(
      oldRows.map((row) => [
        this.slotKey({ id: row.resource_id, vary: row.vary }),
        row.validators,
      ]),
    );

    // Each deduped ("winner") entry reports its relationship, keyed back to its
    // original input index.
    const resultByInputIndex = new Map<number, StoreEntryResult>(
      deduped.map(({ it, originalIndex }) => [
        originalIndex,
        this.#relationshipToExisting(it.entry, oldValidatorsBySlot),
      ]),
    );

    // Map back onto the full input order; every input that isn't a winner (a
    // dropped within-call duplicate) isn't in the map and so is omitted.
    return entries.map((_entry, index) => resultByInputIndex.get(index) ?? {});
  }

  /**
   * A canonical key for an entry's slot (resource id + vary). `jsonStringify` is
   * a stable stringify, so the same logical slot always yields the same key
   * regardless of key order or how Postgres returns the `vary` jsonb -- which is
   * what lets us match the read-back `old` rows to inputs in JS.
   */
  private slotKey(entry: {
    id: string;
    vary: Readonly<NormalizedVary<Params>>;
  }): string {
    return jsonStringify([entry.id, entry.vary]);
  }

  /**
   * How an incoming entry's validators relate to the slot's pre-call validators.
   * Empty incoming validators are omitted; a slot absent from the map is is-new.
   */
  #relationshipToExisting(
    entry: Entry<Spec, Validators, Params>,
    oldValidatorsBySlot: ReadonlyMap<string, Partial<AnyValidators>>,
  ): StoreEntryResult {
    // The old side comes back from jsonb and so is already in JSON-serialized
    // form; normalize the raw in-memory incoming side to match, so both the
    // emptiness check and the comparison agree with the other stores on
    // type-violating values (e.g. `undefined`).
    const newValidators = validatorsAsStored(entry.validators);
    if (Object.keys(newValidators).length === 0) {
      return {};
    }
    const old = oldValidatorsBySlot.get(this.slotKey(entry));
    if (old === undefined) {
      return { relationshipToExistingStoredData: "is-new" };
    }
    return {
      relationshipToExistingStoredData: validatorsEqual(old, newValidators)
        ? "unchanged"
        : "changed",
    };
  }

  async delete(id: Spec["id"]): Promise<void> {
    this.logTrace("deleting entries for id", id);
    await this.ensureInitializedPromise;

    try {
      await this.db
        .deleteFrom(this.tableName)
        .where("resource_id", "=", id)
        .execute();
      this.logTrace("deleted entries for id successfully", id);
    } catch (error) {
      this.logError("failed to delete entries for id", {
        id,
        error,
      });
      throw error;
    }
  }

  async [Symbol.asyncDispose]() {
    // we don't need to do anything here, the caller should handle closing db connection
    this.logInfo(
      "[Symbol.asyncDispose] called, but no action needed for postgres store",
    );
  }

  private getTableNameData(schemaName: string, tableName: string) {
    if (schemaName.includes(".") || tableName.includes(".")) {
      // kysely gets really confused if we allow dots to be there, it doesn't know how to properly escape them
      // we can use sql.id to bypass that, but it's not accepted by insertInto and createTable,
      // so it forces us to use raw queries for these operations
      // I decided to stick to a simpler solution, since dots in schema/table names are very rare
      throw new Error("schema name and table name cannot include dots");
    }
    return {
      schemaName,
      tableName,
      qualifiedName: `${schemaName}.${tableName}` as CacheTableName,
    };
  }

  // just a convenience getter for qualified cache table name
  private get tableName() {
    return this.tableNameData.qualifiedName;
  }

  /**
   * Initialize the database schema and table if they don't exist.
   * This is called automatically in the constructor.
   */
  private async ensureInitialized() {
    this.logTrace("initializing database schema and table");
    try {
      await this.db.transaction().execute(async (tx) => {
        await tx.schema
          .createSchema(this.tableNameData.schemaName)
          .ifNotExists()
          .execute();

        await tx.schema
          .createTable(this.tableName)
          .ifNotExists()
          .addColumn("resource_id", "text", (col) => col.notNull())
          .addColumn("vary", "jsonb", (col) => col.notNull())
          .addColumn("entry", "jsonb", (col) => col.notNull())
          .addPrimaryKeyConstraint(`${this.tableNameData.tableName}_pkey`, [
            "resource_id",
            "vary",
          ])
          .execute();

        // I was wondering whether to use the default jsonb_ops index or jsonb_path_ops index.
        // The second one should be faster for containment queries, but the documentation says:
        //    "A disadvantage of the jsonb_path_ops approach is that it produces no index entries
        //    for JSON structures not containing any values, such as {"a": {}}.
        //    If a search for documents containing such a structure is requested, it will require a full-index scan,
        //    which is quite slow. jsonb_path_ops is therefore ill-suited for applications that often perform such searches."
        // Seeing as we'll have empty vary values very often, I decided to use the default jsonb_ops index.
        // We can come back to this and add a special handling for empty vary values if we need the performance boost.
        await tx.schema
          .createIndex(`${this.tableName}_vary_idx`)
          .ifNotExists()
          .on(this.tableName)
          .using("gin")
          .column("vary")
          .execute();
      });
      this.logInfo("database schema and table initialized successfully");
    } catch (error) {
      this.logError("failed to initialize database schema and table", error);
      throw error;
    }
  }

  private serializeEntry(entry: Entry<Spec, Validators, Params>) {
    return jsonStringify(entry) satisfies
      | JsonOf<Jsonify<Entry<Spec, Validators, Params>>>
      | undefined as JsonOf<
      JsonifiedEntry<SpecForId<Spec, Spec["id"]>, Validators, Params>
    >;
  }

  private serializeVary(
    vary: NormalizedVary<Params>,
  ): JsonOf<NormalizedVary<Params>> {
    // NormalizedVary is a legal Json
    return jsonStringify(vary) satisfies
      | JsonOf<Jsonify<NormalizedVary<Params>>>
      | undefined as JsonOf<NormalizedVary<Params>>;
  }

  private deserializeEntry<Id extends Spec["id"]>(
    entry: TableEntry<Spec, Validators, Params, Id>,
  ): EntryForId<Spec, Validators, Params, Id> {
    const _ = entry satisfies JsonifiedEntry<
      SpecForId<Spec, Id>,
      Validators,
      Params
    > as unknown as JsonifiedEntry<
      PostgresStoreCompatibleSpec,
      AnyValidators,
      AnyParams
    >;

    return {
      id: _.id,
      content: _.content,
      vary: _.vary satisfies Vary<AnyParams> as NormalizedVary<Params>,
      validators: _.validators satisfies AnyValidators as Partial<Validators>,
      directives: restoreInfinityInDirectives(_.directives),
      initialAge: _.initialAge,
      date: parseDateString(_.date satisfies string as unknown as DateString),
    } satisfies Entry<
      PostgresStoreCompatibleSpec,
      Validators,
      Params
    > as EntryForId<Spec, Validators, Params, Id>;
  }
}
