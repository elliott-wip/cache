import sqlite from "node:sqlite";
import { setTimeout as sleep } from "node:timers/promises";
import {
  isWorkerThread as isPiscinaWorkerThread,
  workerData as piscinaWorkerData,
} from "piscina";
import type { JSONWithUndefined, JsonOf } from "type-party";
import { jsonParse, jsonStringify } from "type-party/runtime/json.js";

import type {
  AnyParams,
  AnyValidators,
  CacheSpec,
  JsonifiedEntry,
  NormalizedParams,
  NormalizedVary,
} from "../../types/index.js";
import type { StoreEntryResult } from "../../types/06_Store.js";
import { validatorsEqual } from "../../utils/normalizedProducerResultResourceHelpers.js";
import {
  variantMatchesRequest,
  type VariantKey,
} from "../../utils/varyHelpers.js";

export type WorkerInitData = {
  databasePath: string;
  busyTimeoutMs: number;
  role: "reader" | "writer";
};

type WorkerEntry = JsonifiedEntry<
  CacheSpec<string, JSONWithUndefined>,
  AnyValidators,
  AnyParams
>;

export type WorkerEntryJson = JsonOf<WorkerEntry>;

type GetRequest = {
  id: string;
  params: NormalizedParams<AnyParams>;
};

export type StoreEntryInput = {
  resourceId: string;
  variantKey: VariantKey;
  vary: NormalizedVary<AnyParams>;
  entryJson: WorkerEntryJson;
  expiresAt: number | null;
};

export type WorkerOperations = {
  get: {
    request: GetRequest;
    result: WorkerEntryJson[];
  };
  getMany: {
    request: { requests: readonly GetRequest[] };
    result: WorkerEntryJson[][];
  };
  store: {
    request: { entries: readonly StoreEntryInput[] };
    result: readonly StoreEntryResult[];
  };
  delete: {
    request: { id: string };
    result: void;
  };
  deleteExpired: {
    request: { ids?: readonly string[] };
    result: void;
  };
  close: {
    request: { checkpoint: boolean };
    result: void;
  };
};

type WorkerOperationType = keyof WorkerOperations;

export type WorkerRequestInput<
  Type extends WorkerOperationType = WorkerOperationType,
> = { [Key in Type]: { type: Key } & WorkerOperations[Key]["request"] }[Type];

export type ReadWorkerRequestInput = WorkerRequestInput<"get" | "getMany">;
export type ReadWorkerCloseInput = WorkerRequestInput<"close">;
export type WriteWorkerRequestInput = WorkerRequestInput<
  "store" | "delete" | "deleteExpired" | "close"
>;

export type WorkerResultForRequest<
  Request extends { type: WorkerOperationType },
> = WorkerOperations[Request["type"]]["result"];

type CacheRow = {
  resource_id: string;
  vary: JsonOf<NormalizedVary<AnyParams>>;
  entry: WorkerEntryJson;
};

type SqliteStoreWorkerContext = {
  selectById(id: string): CacheRow[];
  selectByIds(ids: readonly string[]): CacheRow[];
  selectForSlot(
    resourceId: string,
    variantKey: VariantKey,
  ): { entry: WorkerEntryJson } | undefined;
  upsert(entry: StoreEntryInput): void;
  deleteById(id: string): void;
  deleteExpiredByIds(ids?: readonly string[]): void;
  transaction<T>(
    opts: { scope: "read" | "write" },
    fn: (context: SqliteStoreWorkerContext) => T,
  ): T;
  close(opts: { checkpoint: boolean }): void;
};

const SQLITE_MAX_BOUND_IDS_PER_QUERY = 900;

const initPromise = Promise.resolve().then(() =>
  initializeWithRetry(piscinaWorkerData as WorkerInitData),
);

// Initialization starts eagerly when the worker loads. Keep the original
// promise rejected for request handlers, but observe the rejection so a store
// that is constructed and never used does not produce an unhandled rejection.
initPromise.catch(() => undefined);

if (!isPiscinaWorkerThread) {
  throw new Error("SqliteStore worker must be started as a Piscina worker");
}

export default async function runTask(
  message: WorkerRequestInput,
): Promise<WorkerResultForRequest<WorkerRequestInput>> {
  if (message.type === "close") {
    const context = await initPromise.catch(() => undefined);
    context?.close({ checkpoint: message.checkpoint });
    return undefined;
  }

  const context = await initPromise;
  switch (message.type) {
    case "get":
      return context.transaction({ scope: "read" }, (ctx) =>
        getMatchingEntryJson(ctx, message),
      );
    case "getMany":
      return context.transaction({ scope: "read" }, (ctx) =>
        getManyMatchingEntryJson(ctx, message.requests),
      );
    case "store":
      return context.transaction({ scope: "write" }, (ctx) =>
        message.entries.map((entry) => {
          // Read the pre-upsert row for this slot to classify the incoming
          // entry, THEN upsert. This all happens within the write transaction,
          // so the read reflects the state before any of this call's writes.
          const result = relationshipForStoredEntry(ctx, entry);
          ctx.upsert(entry);
          return result;
        }),
      );
    case "delete":
      context.deleteById(message.id);
      return undefined;
    case "deleteExpired":
      context.transaction({ scope: "write" }, (ctx) => {
        ctx.deleteExpiredByIds(message.ids);
      });
      return undefined;
  }
}

async function initializeWithRetry(
  data: WorkerInitData,
): Promise<SqliteStoreWorkerContext> {
  const deadline = Date.now() + data.busyTimeoutMs;
  let attempt = 0;

  while (true) {
    try {
      return initialize(data);
    } catch (error) {
      if (
        (!isDatabaseLockedError(error) && !isNoSuchTableError(error)) ||
        Date.now() >= deadline
      ) {
        throw error;
      }
      attempt += 1;
      await sleep(Math.min(25 * attempt, 100));
    }
  }
}

function initialize(data: WorkerInitData): SqliteStoreWorkerContext {
  const db = new sqlite.DatabaseSync(data.databasePath, {
    timeout: data.busyTimeoutMs,
  });

  try {
    db.exec(`PRAGMA busy_timeout = ${data.busyTimeoutMs};`);

    if (data.role === "writer") {
      db.exec(`
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
      `);

      db.exec(`
        CREATE TABLE IF NOT EXISTS cache_entries (
          resource_id TEXT NOT NULL,
          variant_key TEXT NOT NULL,
          vary TEXT NOT NULL,
          entry TEXT NOT NULL,
          expires_at INTEGER,
          PRIMARY KEY (resource_id, variant_key)
        ) STRICT;

        CREATE INDEX IF NOT EXISTS cache_entries_resource_id_idx
          ON cache_entries(resource_id);

        CREATE INDEX IF NOT EXISTS cache_entries_expires_at_idx
          ON cache_entries(expires_at)
          WHERE expires_at IS NOT NULL;
      `);
    }
  } catch (error) {
    db.close();
    throw error;
  }

  const prepared = {
    selectById: db.prepare(`
      SELECT resource_id, vary, entry
      FROM cache_entries
      WHERE resource_id = ?
        AND (expires_at IS NULL OR expires_at > ?)
    `),
    selectForSlot: db.prepare(`
      SELECT entry
      FROM cache_entries
      WHERE resource_id = ? AND variant_key = ?
        AND (expires_at IS NULL OR expires_at > ?)
    `),
    upsert: db.prepare(`
      INSERT INTO cache_entries (resource_id, variant_key, vary, entry, expires_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(resource_id, variant_key) DO UPDATE SET
        vary = excluded.vary,
        entry = excluded.entry,
        expires_at = excluded.expires_at
    `),
    deleteById: db.prepare(`
      DELETE FROM cache_entries
      WHERE resource_id = ?
    `),
    deleteExpired: db.prepare(`
      DELETE FROM cache_entries
      WHERE expires_at IS NOT NULL AND expires_at <= ?
    `),
    deleteExpiredById: db.prepare(`
      DELETE FROM cache_entries
      WHERE resource_id = ? AND expires_at IS NOT NULL AND expires_at <= ?
    `),
  };

  // Each query computes its own expiry cutoff at execution time, so reads may
  // differ slightly from cleanup or other reads if clocks skew. That only affects
  // which cache entries this backend chooses to keep returning; Store correctness
  // requires returned entries to match the request id and variant, not every
  // stored entry to remain visible until a particular observer's clock expires it.
  const expiryCutoff = () => Date.now();

  let closed = false;
  return {
    selectById: (id) =>
      prepared.selectById.all(id, expiryCutoff()) as CacheRow[],
    // Filtering out expired rows matters for correctness, not just hygiene:
    // change detection must only consider records that a `get` would still
    // return, so an expired-but-not-yet-vacuumed row must read as "no live
    // entry" (=> is-new), matching the expiry filter on the select* reads.
    selectForSlot: (resourceId, variantKey) =>
      prepared.selectForSlot.get(resourceId, variantKey, expiryCutoff()) as
        | { entry: WorkerEntryJson }
        | undefined,
    selectByIds: (ids) => {
      const uniqueIds = [...new Set(ids)];
      if (uniqueIds.length === 0) return [];

      const rows: CacheRow[] = [];
      for (const chunk of chunks(uniqueIds, SQLITE_MAX_BOUND_IDS_PER_QUERY)) {
        rows.push(
          ...(db
            .prepare(
              `
          SELECT resource_id, vary, entry
          FROM cache_entries
          WHERE resource_id IN (${chunk.map(() => "?").join(", ")})
            AND (expires_at IS NULL OR expires_at > ?)
        `,
            )
            .all(...chunk, expiryCutoff()) as CacheRow[]),
        );
      }
      return rows;
    },
    upsert: (entry) => {
      prepared.upsert.run(
        entry.resourceId,
        entry.variantKey,
        jsonStringify(entry.vary),
        entry.entryJson,
        entry.expiresAt,
      );
    },
    deleteById: (id) => {
      prepared.deleteById.run(id);
    },
    deleteExpiredByIds: (ids) => {
      const now = expiryCutoff();
      if (ids === undefined) {
        prepared.deleteExpired.run(now);
        return;
      }

      const uniqueIds = [...new Set(ids)];
      if (uniqueIds.length === 0) return;

      if (uniqueIds.length === 1) {
        const id = uniqueIds[0]!;
        prepared.deleteExpiredById.run(id, now);
        return;
      }

      for (const chunk of chunks(uniqueIds, SQLITE_MAX_BOUND_IDS_PER_QUERY)) {
        db.prepare(
          `
          DELETE FROM cache_entries
          WHERE resource_id IN (${chunk.map(() => "?").join(", ")})
            AND expires_at IS NOT NULL
            AND expires_at <= ?
        `,
        ).run(...chunk, now);
      }
    },
    transaction({ scope }, fn) {
      db.exec(scope === "write" ? "BEGIN IMMEDIATE" : "BEGIN");
      try {
        const result = fn(this);
        db.exec("COMMIT");
        return result;
      } catch (error) {
        try {
          db.exec("ROLLBACK");
        } catch {
          // Preserve the original failure.
        }
        throw error;
      }
    },
    close({ checkpoint }) {
      if (closed) return;
      closed = true;
      if (checkpoint) {
        db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
      }
      db.close();
    },
  };
}

function getMatchingEntryJson(
  context: SqliteStoreWorkerContext,
  { id, params }: GetRequest,
): WorkerEntryJson[] {
  const rows = context.selectById(id);
  return rows
    .filter((it) => variantMatchesRequest(jsonParse(it.vary), params))
    .map((row) => row.entry);
}

function getManyMatchingEntryJson(
  context: SqliteStoreWorkerContext,
  requests: readonly GetRequest[],
): WorkerEntryJson[][] {
  if (requests.length === 0) return [];

  const rowsById = Map.groupBy(
    context.selectByIds(requests.map(({ id }) => id)),
    ({ resource_id }) => resource_id,
  );

  // Look rows up by the id as SQLite stores it. SQLite receives each bound id
  // as UTF-8, so a lone UTF-16 surrogate in a request id reaches the table, and
  // comes back, as U+FFFD. `toWellFormed` makes the same replacement, so a
  // request still finds the rows that `get` would find for it.
  return requests.map(({ id, params }) =>
    (rowsById.get(id.toWellFormed()) ?? [])
      .filter((row) => variantMatchesRequest(jsonParse(row.vary), params))
      .map((row) => row.entry),
  );
}

/**
 * Classifies an incoming entry against the row currently stored for its slot
 * (read from within the write transaction, i.e. before this call's upserts).
 * The field is omitted when the incoming entry has empty validators.
 */
function relationshipForStoredEntry(
  context: SqliteStoreWorkerContext,
  entry: StoreEntryInput,
): StoreEntryResult {
  const newValidators = jsonParse(entry.entryJson).validators;
  if (Object.keys(newValidators).length === 0) {
    return {};
  }

  const existing = context.selectForSlot(entry.resourceId, entry.variantKey);
  if (existing === undefined) {
    return { relationshipToExistingStoredData: "is-new" };
  }

  const existingValidators = jsonParse(existing.entry).validators;
  return {
    relationshipToExistingStoredData: validatorsEqual(
      existingValidators,
      newValidators,
    )
      ? "unchanged"
      : "changed",
  };
}

function isDatabaseLockedError(error: unknown): boolean {
  return error instanceof Error && /database is locked/i.test(error.message);
}

function isNoSuchTableError(error: unknown): boolean {
  return error instanceof Error && /no such table/i.test(error.message);
}

function chunks<T>(items: readonly T[], size: number): T[][] {
  const result: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    result.push(items.slice(i, i + size));
  }
  return result;
}
