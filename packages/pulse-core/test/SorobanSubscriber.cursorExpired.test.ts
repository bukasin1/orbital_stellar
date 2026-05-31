/**
 * packages/pulse-core/test/SorobanSubscriber.cursorExpired.test.ts
 *
 * Verifies that SorobanSubscriber correctly handles a cursor-expired error
 * from the Stellar RPC server:
 *   1. Emits an `engine.cursor_expired` notification on the Watcher.
 *   2. Resumes polling from the latest ledger (drops the history gap).
 *   3. Does NOT re-throw or loop on the error.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { SorobanSubscriberCursorExpired as SorobanSubscriber } from "../src/SorobanSubscriber.js";
import { Watcher } from "../src/Watcher.js";
import type { SorobanCursorExpiredNotification } from "../src/index.js";

// ─── helpers ──────────────────────────────────────────────────────────────────

/** Builds the JSON-RPC -32600 error that Stellar RPC throws for an expired cursor. */
function makeCursorExpiredError(message = "cursor is before the oldest ledger retained") {
  const err = Object.assign(new Error(message), { code: -32600 });
  return err;
}

/** Minimal stub matching the parts of rpc.Server we use. */
function makeServerStub(getEventsImpl: () => Promise<unknown>) {
  return {
    getEvents: vi.fn(getEventsImpl),
    getLatestLedger: vi.fn(async () => ({
      sequence: 999_999,
      id: "abc",
      protocolVersion: "21",
    })),
  };
}

/** Empty successful response from getEvents. */
const emptyEventsResponse = {
  events: [],
  cursor: "1000-0",
  latestLedger: 999_999,
  latestLedgerCloseTime: 0,
  oldestLedger: 900_000,
  oldestLedgerCloseTime: 0,
  ledgerRetentionWindow: 17_280,
};

/**
 * Advance fake timers by 0 ms and flush all resulting microtasks/promises.
 * This fires the initial setTimeout(fn, 0) that scheduleNext() registers on
 * start(), waits for poll() + fetchAndForward()/recoverFromExpiredCursor() to
 * complete, then stops — without triggering the subsequent pollIntervalMs timer.
 */
async function flushFirstPoll() {
  await vi.advanceTimersByTimeAsync(0);
}

// ─── tests ────────────────────────────────────────────────────────────────────

describe("SorobanSubscriber — cursor expired recovery", () => {
  let watcher: Watcher;
  let warnings: string[];
  let mockLogger: {
    info: ReturnType<typeof vi.fn<(msg: string, ...args: unknown[]) => void>>;
    warn: ReturnType<typeof vi.fn<(msg: string, ...args: unknown[]) => void>>;
    error: ReturnType<typeof vi.fn<(msg: string, ...args: unknown[]) => void>>;
  };

  beforeEach(() => {
    vi.useFakeTimers();
    watcher = new Watcher("GTEST_ADDRESS");
    warnings = [];
    mockLogger = {
      info: vi.fn<(msg: string, ...args: unknown[]) => void>(),
      warn: vi.fn<(msg: string, ...args: unknown[]) => void>((msg: string) => {
        warnings.push(msg);
      }),
      error: vi.fn<(msg: string, ...args: unknown[]) => void>(),
    };
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    watcher.stop();
  });

  // ── 1. emits engine.cursor_expired ──────────────────────────────────────

  it("emits engine.cursor_expired with source and lostCursor when getEvents throws a cursor-expired error", async () => {
    const expiredCursor = "500000-1";

    const server = makeServerStub(async () => {
      throw makeCursorExpiredError();
    });

    const subscriber = new SorobanSubscriber({
      rpcUrl: "https://soroban-testnet.stellar.org",
      source: "CONTRACT_ABC",
      cursor: expiredCursor,
      pollIntervalMs: 60_000,
      logger: mockLogger,
    });

    (subscriber as unknown as Record<string, unknown>)["server"] = server;

    const notifications: SorobanCursorExpiredNotification[] = [];
    watcher.on("engine.cursor_expired", (n) => {
      notifications.push(n as unknown as SorobanCursorExpiredNotification);
    });

    subscriber.start(watcher);

    // Flush only the first poll (delay = 0). The next timer is pollIntervalMs
    // (60 s) away, so advancing by 0 ms stops after exactly one poll cycle.
    await flushFirstPoll();

    subscriber.stop();

    expect(notifications).toHaveLength(1);
    const n = notifications[0]!;
    expect(n.type).toBe("engine.cursor_expired");
    expect(n.source).toBe("CONTRACT_ABC");
    expect(n.lostCursor).toBe(expiredCursor);
    expect(typeof n.emittedAt).toBe("string");
  });

  // ── 2. recovers by resuming from startLedger mode ────────────────────────

  it("resumes polling from startLedger mode (no cursor) after cursor expiry without rethrowing", async () => {
    let callCount = 0;

    const server = makeServerStub(async () => {
      callCount += 1;
      if (callCount === 1) throw makeCursorExpiredError("startLedger is out of range");
      return emptyEventsResponse;
    });

    const subscriber = new SorobanSubscriber({
      rpcUrl: "https://soroban-testnet.stellar.org",
      source: "MY_CONTRACT",
      cursor: "100-0",
      pollIntervalMs: 50,
      logger: mockLogger,
    });

    (subscriber as unknown as Record<string, unknown>)["server"] = server;

    subscriber.start(watcher);

    // First poll (delay = 0) — throws cursor-expired, triggers recovery.
    await flushFirstPoll();

    // Internal cursor must be cleared so next request uses startLedger mode.
    const internalCursor = (subscriber as unknown as Record<string, unknown>)["cursor"];
    expect(internalCursor).toBeUndefined();

    // No error path taken — cursor-expired is handled, not propagated.
    expect(mockLogger.error).not.toHaveBeenCalled();

    // Advance past pollIntervalMs (50 ms) to trigger the second poll.
    await vi.advanceTimersByTimeAsync(50);

    subscriber.stop();

    // Second getEvents call must have fired (subscriber kept running after recovery).
    expect(callCount).toBeGreaterThanOrEqual(2);
  });

  // ── 3. logs a warn with data-loss implication ────────────────────────────

  it("logs a warning that describes the data-loss implication", async () => {
    const server = makeServerStub(async () => {
      throw makeCursorExpiredError("cursor before oldest ledger");
    });

    const subscriber = new SorobanSubscriber({
      rpcUrl: "https://soroban-testnet.stellar.org",
      source: "LOSS_SOURCE",
      cursor: "42-0",
      pollIntervalMs: 60_000,
      logger: mockLogger,
    });

    (subscriber as unknown as Record<string, unknown>)["server"] = server;

    subscriber.start(watcher);
    await flushFirstPoll();
    subscriber.stop();

    expect(warnings.length).toBeGreaterThanOrEqual(1);
    const warnMsg = warnings[0]!.toLowerCase();
    expect(warnMsg).toMatch(/lost|permanently|data/);
  });

  // ── 4. non-cursor errors are NOT treated as cursor-expired ───────────────

  it("does not treat a generic network error as a cursor-expired error", async () => {
    const server = makeServerStub(async () => {
      throw new Error("Network timeout");
    });

    const subscriber = new SorobanSubscriber({
      rpcUrl: "https://soroban-testnet.stellar.org",
      source: "NET_ERR_SOURCE",
      pollIntervalMs: 60_000,
      logger: mockLogger,
    });

    (subscriber as unknown as Record<string, unknown>)["server"] = server;

    const expiredNotifications: unknown[] = [];
    watcher.on("engine.cursor_expired", (n) => expiredNotifications.push(n));

    subscriber.start(watcher);
    await flushFirstPoll();
    subscriber.stop();

    // Must NOT have emitted cursor_expired for a plain Error (no .code).
    expect(expiredNotifications).toHaveLength(0);

    // Must have logged an error instead.
    expect(mockLogger.error).toHaveBeenCalled();
  });

  // ── 5. lostCursor is undefined when no cursor was configured ─────────────

  it("sets lostCursor to undefined in the notification when no cursor was stored", async () => {
    const server = makeServerStub(async () => {
      throw makeCursorExpiredError("startLedger must be after oldest ledger");
    });

    const subscriber = new SorobanSubscriber({
      rpcUrl: "https://soroban-testnet.stellar.org",
      source: "NO_CURSOR_SOURCE",
      // No cursor — subscriber uses startLedger mode from the start.
      pollIntervalMs: 60_000,
      logger: mockLogger,
    });

    (subscriber as unknown as Record<string, unknown>)["server"] = server;

    const notifications: SorobanCursorExpiredNotification[] = [];
    watcher.on("engine.cursor_expired", (n) => {
      notifications.push(n as unknown as SorobanCursorExpiredNotification);
    });

    subscriber.start(watcher);
    await flushFirstPoll();
    subscriber.stop();

    expect(notifications).toHaveLength(1);
    expect(notifications[0]!.lostCursor).toBeUndefined();
  });

  // ── 6. stop() halts polling after recovery ───────────────────────────────

  it("stops emitting after stop() is called even during recovery", async () => {
    let getEventsCallCount = 0;

    const server = makeServerStub(async () => {
      getEventsCallCount += 1;
      if (getEventsCallCount === 1) throw makeCursorExpiredError();
      return emptyEventsResponse;
    });

    const subscriber = new SorobanSubscriber({
      rpcUrl: "https://soroban-testnet.stellar.org",
      source: "STOP_TEST",
      cursor: "1-0",
      pollIntervalMs: 100,
      logger: mockLogger,
    });

    (subscriber as unknown as Record<string, unknown>)["server"] = server;

    subscriber.start(watcher);

    // First poll — throws cursor-expired, triggers recovery, schedules next
    // poll at pollIntervalMs (100 ms).
    await flushFirstPoll();

    // Stop before the second poll fires.
    subscriber.stop();

    const countAfterStop = getEventsCallCount;

    // Advance well past pollIntervalMs — no further polls should occur.
    await vi.advanceTimersByTimeAsync(500);

    expect(getEventsCallCount).toBe(countAfterStop);
  });
});
