import {
  mapCooperatively,
  forEachCooperatively,
  mapFromEntriesCooperatively,
  sortCooperatively,
} from "../../utils/cooperative.js";
import type { ColumnType, PostgresPool, RawBuilder, SqlBool } from "kysely";
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
  keepMaxPerGroupCooperatively,
  naiveGetMany,
} from "../../utils/utils.js";

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
    // variant applies only when the request omits that param". `get` reads this
    // column back to match variants in JS via `variantMatchesRequest`.
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
 * every level, so object/array values would in fact match correctly -- but the
 * param type is still limited to primitives, because that's all real callers
 * use and it keeps the contract identical to the Memory/Sqlite stores, whose JS
 * matcher (`variantMatchesRequest`) compares with `===` and so cannot
 * deep-compare objects.
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

  async getMany<
    const Reqs extends readonly StoreGetManyRequest<Spec, Params>[],
  >(
    requests: Reqs,
    options?: { signal?: AbortSignal },
  ): Promise<StoreGetManyResult<Spec, Reqs, Validators, Params>> {
    this.logTrace("querying for multiple entries", {
      requestCount: requests.length,
    });

    // For PostgresStore, we'll use the naive implementation until we have time
    // to optimize it. Technically, all these calls should probably be wrapped
    // in a transaction, so that concurrent deletes can't lead us to returning
    // partial/incosistent state, but that doesn't really matter for this cache.
    return naiveGetMany<Spec, Validators, Params, Reqs>(
      this,
      requests,
      undefined,
      options,
    );
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
    const deduped = await keepMaxPerGroupCooperatively({
      items: await mapCooperatively(entries, (it, originalIndex) => ({
        it,
        originalIndex,
      })),
      groupBy: ({ it }) => this.slotKey(it.entry),
      maxBy: ({ it }) => entryUtils.birthDate(it.entry).getTime(),
    });

    // Sort the rows by a deterministic key so that every store() call acquires
    // its row locks in the same global order. Postgres processes a multi-row
    // INSERT's rows in order, locking each conflicting row as it goes, so two
    // concurrent calls whose batches share >= 2 slots in DIFFERENT orders
    // could otherwise deadlock (each holding a row the other is waiting on).
    // With a consistent order, the later call just waits for the earlier one.
    const rows = await sortCooperatively(
      await mapCooperatively(deduped, ({ it }) => ({
        resource_id: it.entry.id,
        vary: this.serializeVary(it.entry.vary),
        entry: this.serializeEntry(it.entry),
      })),
      (a, b) =>
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
    const resourceIds = new Set<string>();
    await forEachCooperatively(deduped, ({ it }) => {
      resourceIds.add(it.entry.id);
    });
    type OldRow = {
      resource_id: string;
      vary: Readonly<NormalizedVary<Params>>;
      validators: Partial<Validators>;
    };
    const oldQuery = this.db
      .selectFrom(this.tableName)
      .select((eb) => [
        "resource_id",
        "vary",
        sql<Partial<Validators>>`${eb.ref("entry")} -> 'validators'`.as(
          "validators",
        ),
      ]);
    const batchSize = 1_000;
    let oldRows: readonly OldRow[];
    try {
      if (rows.length <= batchSize) {
        // Keep the one-statement fast path for ordinary small cache writes.
        oldRows = (await this.db
          .with("old", () =>
            oldQuery.where("resource_id", "in", [...resourceIds]),
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
          .execute()) as unknown as readonly OldRow[];
      } else {
        // One encoded parameter avoids both expanded IN parameters and a
        // full-cohort synchronous array-encoding pass in the Postgres driver.
        const resourceIdsJson = `[${(await mapCooperatively(resourceIds, (id) => JSON.stringify(id))).join(",")}]`;
        const previousQuery = oldQuery.where(
          sql<SqlBool>`${sql.ref("resource_id")} IN (SELECT jsonb_array_elements_text(${resourceIdsJson}::jsonb))`,
        );
        oldRows = await this.db.transaction().execute(async (trx) => {
          // Capture ONE pre-write statement snapshot for every input slot.
          // Repeating this SELECT per batch would change comparison metadata.
          const previous = await trx.executeQuery<OldRow>(
            previousQuery.compile(),
          );
          // Sort the whole call before batching. The identical JS comparator
          // preserves global lock order with older one-statement writers.
          for (let start = 0; start < rows.length; start += batchSize) {
            await trx
              .insertInto(this.tableName)
              .values(rows.slice(start, start + batchSize))
              .onConflict((oc) =>
                oc
                  .columns(["resource_id", "vary"])
                  .doUpdateSet((eb) => ({ entry: eb.ref("excluded.entry") })),
              )
              .execute();
          }
          return previous.rows;
        });
      }
    } catch (error: unknown) {
      this.logError("failed to store entries", error);
      throw error;
    }
    this.logTrace("stored entries successfully");

    // Like the former one-statement path, comparisons use the pre-write
    // snapshot. A concurrent writer may still commit between that snapshot
    // and an upsert, so this metadata remains approximate under same-slot races.
    // Build comparison metadata after the transaction releases its connection.
    const oldValidatorsBySlot = await mapFromEntriesCooperatively(
      await mapCooperatively(
        oldRows,
        (row) =>
          [
            this.slotKey({ id: row.resource_id, vary: row.vary }),
            row.validators,
          ] as const,
      ),
    );

    // Each deduped ("winner") entry reports its relationship, keyed back to its
    // original input index.
    const resultByInputIndex = await mapFromEntriesCooperatively(
      await mapCooperatively(
        deduped,
        ({ it, originalIndex }) =>
          [
            originalIndex,
            this.#relationshipToExisting(it.entry, oldValidatorsBySlot),
          ] as const,
      ),
    );

    // Map back onto the full input order; every input that isn't a winner (a
    // dropped within-call duplicate) isn't in the map and so is omitted.
    return mapCooperatively(
      entries,
      (_entry, index) => resultByInputIndex.get(index) ?? {},
    );
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
