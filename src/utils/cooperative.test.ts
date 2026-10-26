import assert from "node:assert/strict";
import { setImmediate } from "node:timers/promises";
import { it } from "node:test";
import { sortCooperatively } from "./cooperative.js";

it("cooperative sorting matches stable native UTF-16 ordering without mutating input", async () => {
  const keys = ["a", "A", "\u{10000}", "\ue000", "é", "same", "", "z"];
  const input = Array.from({ length: 4_096 }, (_, index) => ({
    key: keys[(index * 17) % keys.length]!,
    index,
  }));
  const compare = (
    left: (typeof input)[number],
    right: (typeof input)[number],
  ) => (left.key < right.key ? -1 : left.key > right.key ? 1 : 0);
  const original = [...input];
  assert.deepEqual(
    await sortCooperatively(input, compare),
    input.toSorted(compare),
  );
  assert.deepEqual(input, original);
  assert.deepEqual(await sortCooperatively(input, () => Number.NaN), input);
});

it("cooperative sorting yields between bounded runs and merged output", async () => {
  const input = Array.from(
    { length: 4_096 },
    (_, index) => (index * 3571) % 4096,
  );
  const progress = { comparisons: 0, comparisonsAtTick: 0 };
  const tick = setImmediate().then(() => {
    progress.comparisonsAtTick = progress.comparisons;
  });
  const actual = await sortCooperatively(input, (left, right) => {
    progress.comparisons += 1;
    const until = performance.now() + 0.002;
    while (performance.now() < until) {
      /* comparator work */
    }
    return left - right;
  });
  await tick;
  assert.ok(
    progress.comparisonsAtTick > 0 &&
      progress.comparisonsAtTick < progress.comparisons,
  );
  assert.deepEqual(
    actual,
    input.toSorted((left, right) => left - right),
  );
});
