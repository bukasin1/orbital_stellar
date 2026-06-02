/**
 * packages/pulse-core/test/SorobanSubscriber.cursorExpired.test.ts
 *
 * Verifies that SorobanSubscriber correctly handles a cursor-expired error
 * from the Stellar RPC server:
 *   1. Emits an `engine.cursor_expired` notification on the Watcher.
 *   2. Logs a warn message documenting the data-loss implication.
 *   3. Does NOT re-throw or loop on the error.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { SorobanSubscriber } from "../src/SorobanSubscriber.js";
import { Watcher } from "../src/Watcher.js";
import type {
  SorobanCursorExpiredNotification,
} from "../src/index.js";
import type {
  SorobanRpc,
  SorobanEvent,
  CursorStore,
} from "../src/SorobanSubscriber.js";

// ─── helpers ──────────────────────────────────────────────────────────────────

/** Builds the JSON-RPC -32600 error that Stellar RPC throws for an expired cursor. */
function makeCursorExpiredError(
  message = "cursor is before the oldest ledger retained",
): Error & { code: number } {
  return Object.assign(new Error(message), { code: -32600 });
}

/** Creates a minimal in-memory CursorStore seeded with an optional initial cursor. */
function makeMemoryCursorStore(initial?: string): CursorStore {
  let stored: string | undefined = initial;
  return {
    getCursor: vi.fn(async () => stored),
    saveCursor: vi.fn(async (cursor: string | undefined) => {
      stored = cursor;
    }),
  };
}

/** Creates a mock SorobanRpc that delegates getEvents to the provided implementation. */
function makeMockRpc(
  getEventsImpl: (
    startCursor: string | undefined,
    limit: number,
    signal?: AbortSignal,
  ) => Promise<{ events: SorobanEvent[]; cursor?: string }>,
): SorobanRpc {
  return {
    getEvents: vi.fn(getEventsImpl),
  };
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
    vi.restoreAllMocks();
    watcher.stop();
  });

  // ── 1. emits engine.cursor_expired ──────────────────────────────────────

  it("emits engine.cursor_expired with source and lostCursor when getEvents throws a cursor-expired error", async () => {
    const expiredCursor = "500000-1";
    const cursorStore = makeMemoryCursorStore(expiredCursor);

    const rpc = makeMockRpc(async () => {
      throw makeCursorExpiredError();
    });

    const subscriber = new SorobanSubscriber({
      rpc,
      cursorStore,
      onEvent: vi.fn(async () => {}),
      source: "CONTRACT_ABC",
      logger: mockLogger,
      watcher,
    });

    const notifications: SorobanCursorExpiredNotification[] = [];
    watcher.on("engine.cursor_expired", (n) => {
      notifications.push(n as unknown as SorobanCursorExpiredNotification);
    });

    await subscriber.pollOnce();

    expect(notifications).toHaveLength(1);
    const n = notifications[0]!;
    expect(n.type).toBe("engine.cursor_expired");
    expect(n.source).toBe("CONTRACT_ABC");
    expect(n.lostCursor).toBe(expiredCursor);
    expect(typeof n.emittedAt).toBe("string");
  });

  // ── 2. does not re-throw on cursor-expired ──────────────────────────────

  it("does not re-throw on cursor-expired — pollOnce resolves cleanly", async () => {
    const cursorStore = makeMemoryCursorStore("100-0");

    const rpc = makeMockRpc(async () => {
      throw makeCursorExpiredError("startLedger is out of range");
    });

    const subscriber = new SorobanSubscriber({
      rpc,
      cursorStore,
      onEvent: vi.fn(async () => {}),
      source: "MY_CONTRACT",
      logger: mockLogger,
      watcher,
    });

    // Must not throw — the cursor-expired error is handled internally.
    await expect(subscriber.pollOnce()).resolves.toBeUndefined();

    // No error-level log for cursor-expired (only a warn).
    expect(mockLogger.error).not.toHaveBeenCalled();
  });

  // ── 3. clears cursor after recovery ─────────────────────────────────────

  it("clears the cursor after recovery so the next poll starts fresh", async () => {
    const cursorStore = makeMemoryCursorStore("100-0");

    const rpc = makeMockRpc(async () => {
      throw makeCursorExpiredError("startLedger is out of range");
    });

    const subscriber = new SorobanSubscriber({
      rpc,
      cursorStore,
      onEvent: vi.fn(async () => {}),
      source: "MY_CONTRACT",
      logger: mockLogger,
      watcher,
    });

    await subscriber.pollOnce();

    // The cursor must have been cleared via saveCursor(undefined).
    expect(cursorStore.saveCursor).toHaveBeenCalledWith(undefined);
  });

  // ── 4. logs a warn with data-loss implication ────────────────────────────

  it("logs a warning that describes the data-loss implication", async () => {
    const cursorStore = makeMemoryCursorStore("42-0");

    const rpc = makeMockRpc(async () => {
      throw makeCursorExpiredError("cursor before oldest ledger");
    });

    const subscriber = new SorobanSubscriber({
      rpc,
      cursorStore,
      onEvent: vi.fn(async () => {}),
      source: "LOSS_SOURCE",
      logger: mockLogger,
      watcher,
    });

    await subscriber.pollOnce();

    expect(warnings.length).toBeGreaterThanOrEqual(1);
    const warnMsg = warnings[0]!.toLowerCase();
    expect(warnMsg).toMatch(/lost|permanently|data/);
  });

  // ── 5. non-cursor errors are NOT treated as cursor-expired ───────────────

  it("does not treat a generic network error as a cursor-expired error", async () => {
    const cursorStore = makeMemoryCursorStore();

    const rpc = makeMockRpc(async () => {
      throw new Error("Network timeout");
    });

    const subscriber = new SorobanSubscriber({
      rpc,
      cursorStore,
      onEvent: vi.fn(async () => {}),
      source: "NET_ERR_SOURCE",
      logger: mockLogger,
      watcher,
    });

    const expiredNotifications: unknown[] = [];
    watcher.on("engine.cursor_expired", (n) => expiredNotifications.push(n));

    // A non-cursor-expired error should propagate (re-throw).
    await expect(subscriber.pollOnce()).rejects.toThrow("Network timeout");

    // Must NOT have emitted cursor_expired for a plain Error (no .code).
    expect(expiredNotifications).toHaveLength(0);
  });

  // ── 6. lostCursor is undefined when no cursor was stored ─────────────────

  it("sets lostCursor to undefined in the notification when no cursor was stored", async () => {
    const cursorStore = makeMemoryCursorStore(undefined);

    const rpc = makeMockRpc(async () => {
      throw makeCursorExpiredError("startLedger must be after oldest ledger");
    });

    const subscriber = new SorobanSubscriber({
      rpc,
      cursorStore,
      onEvent: vi.fn(async () => {}),
      source: "NO_CURSOR_SOURCE",
      logger: mockLogger,
      watcher,
    });

    const notifications: SorobanCursorExpiredNotification[] = [];
    watcher.on("engine.cursor_expired", (n) => {
      notifications.push(n as unknown as SorobanCursorExpiredNotification);
    });

    await subscriber.pollOnce();

    expect(notifications).toHaveLength(1);
    expect(notifications[0]!.lostCursor).toBeUndefined();
  });

  // ── 7. source defaults to "unknown" when not configured ──────────────────

  it('defaults source to "unknown" when not configured', async () => {
    const cursorStore = makeMemoryCursorStore("1-0");

    const rpc = makeMockRpc(async () => {
      throw makeCursorExpiredError();
    });

    const subscriber = new SorobanSubscriber({
      rpc,
      cursorStore,
      onEvent: vi.fn(async () => {}),
      // No source provided — should default to "unknown".
      logger: mockLogger,
      watcher,
    });

    const notifications: SorobanCursorExpiredNotification[] = [];
    watcher.on("engine.cursor_expired", (n) => {
      notifications.push(n as unknown as SorobanCursorExpiredNotification);
    });

    await subscriber.pollOnce();

    expect(notifications).toHaveLength(1);
    expect(notifications[0]!.source).toBe("unknown");
  });

  // ── 8. a JSON-RPC error with wrong code is not treated as cursor-expired ─

  it("does not treat a JSON-RPC error with a non -32600 code as cursor-expired", async () => {
    const cursorStore = makeMemoryCursorStore("1-0");

    const rpc = makeMockRpc(async () => {
      throw Object.assign(
        new Error("cursor is before the oldest ledger retained"),
        { code: -32601 }, // Wrong code — method not found, not invalid request.
      );
    });

    const subscriber = new SorobanSubscriber({
      rpc,
      cursorStore,
      onEvent: vi.fn(async () => {}),
      source: "WRONG_CODE_SRC",
      logger: mockLogger,
      watcher,
    });

    const expiredNotifications: unknown[] = [];
    watcher.on("engine.cursor_expired", (n) => expiredNotifications.push(n));

    // Should propagate as an unknown error, not be handled as cursor-expired.
    await expect(subscriber.pollOnce()).rejects.toThrow();
    expect(expiredNotifications).toHaveLength(0);
  });
});
