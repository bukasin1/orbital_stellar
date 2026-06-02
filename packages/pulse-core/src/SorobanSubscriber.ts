import type { ContractSubscriptionFilter, ContractAddress } from "./index.js";
import { SorobanRpcError } from "./errors.js";

/**
 * SorobanSubscriber — polls a Soroban RPC for contract events and forwards
 * them to a caller-supplied handler or Watcher.
 *
 * Graceful shutdown guarantee
 * ---------------------------
 * When `stop()` is called the subscriber:
 *   1. Marks itself stopped so no new polls are started.
 *   2. Aborts the in-flight `getEvents` request via an `AbortController`.
 *   3. Awaits the in-flight poll Promise so the caller can `await stop()` and
 *      be certain no further events will be emitted once the Promise resolves.
 *   4. Silently drops any events that arrive from an aborted poll.
 *
 * ## Deduplication
 * An in-memory LRU set (default cap: 1024 event IDs) suppresses events that
 * have already been emitted. This is best-effort: events outside the window
 * may be re-emitted after a restart.
 *
 * ## Cursor-expiry recovery
 * Stellar RPC retains at most 7 days of events (24 h by default). If the
 * stored cursor falls outside that window, `getEvents` will throw a
 * JSON-RPC -32600 error. SorobanSubscriber catches that error, emits an
 * `engine.cursor_expired` notification on the Watcher, logs a warning about
 * the data-loss implication, and resumes polling from the current
 * `latestLedger` — dropping the gap in history.
 *
 * **Data-loss notice**: Events that occurred between the expired cursor
 * and the recovery point are permanently lost. Consumers that require
 * guaranteed event delivery should persist a durable replay store and compare
 * the recovered `latestLedger` against the last successfully processed ledger
 * to detect any gap.
 */

import * as StellarSdk from "@stellar/stellar-sdk";
import type { rpc } from "@stellar/stellar-sdk";
import type { Watcher } from "./Watcher.js";
import type { SorobanCursorExpiredNotification } from "./index.js";

// ---------------------------------------------------------------------------
// Minimal LRU set (Map-backed, insertion-order eviction).
// ---------------------------------------------------------------------------

class LruSet {
  private readonly map = new Map<string, 1>();

  constructor(private readonly maxSize: number) {}

  has(id: string): boolean {
    return this.map.has(id);
  }

  add(id: string): void {
    if (this.map.has(id)) this.map.delete(id);
    this.map.set(id, 1);
    if (this.map.size > this.maxSize) {
      this.map.delete(this.map.keys().next().value as string);
    }
  }

  get size(): number {
    return this.map.size;
  }
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/** Minimal interface for a cursor persistence layer. */
export interface CursorStore {
  getCursor(): Promise<string | undefined>;
  saveCursor(cursor: string | undefined): Promise<void>;
}

/** Alias for {@link CursorStore}. */
export type CursorStoreLike = CursorStore;

/** A single event returned by the Soroban RPC. */
export interface SorobanEvent {
  id: string;
  pagingToken: string;
  topic: string[];
  value: unknown;
  contractId?: string;
  type?: string;
}

/** Minimal interface for a Soroban RPC client. */
export interface SorobanRpc {
  getEvents(
    startCursor: string | undefined,
    limit: number,
    signal?: AbortSignal,
    filters?: ContractSubscriptionFilter[],
  ): Promise<{ events: SorobanEvent[]; cursor?: string }>;
}

/** Alias for {@link SorobanRpc}; the name used by EventEngine's replay API. */
export type SorobanRpcLike = SorobanRpc;

export interface SorobanSubscription {
  id: string;
  filters: ContractSubscriptionFilter[];
  onEvent?: (event: SorobanEvent) => Promise<void>;
}

export interface ReconnectingPayload {
  attempt: number;
  delayMs: number;
  cursor?: string;
  source: "soroban";
}

export interface SorobanSubscriberOptions {
  rpc: SorobanRpc;
  cursorStore: CursorStore;
  /**
   * Default handler invoked for every event. Optional when per-subscription
   * handlers (see {@link SorobanSubscription.onEvent}) are used instead.
   */
  onEvent?: (event: SorobanEvent) => Promise<void>;
  /**
   * When set, the subscriber operates in bounded-replay mode: polling stops
   * (and `onDone` is called) once every event whose ledger is strictly less
   * than `endLedger` has been delivered.  The cursor store is **not** updated
   * during replay — progress is ephemeral and intentionally discarded.
   */
  endLedger?: number;
  /** Called once when a bounded replay run has delivered all events up to endLedger. */
  onDone?: () => void;
  /** Max events to request per `getEvents` call. Must be 1–10,000. Defaults to 100. */
  pageLimit?: number;
  /** @deprecated Alias for {@link SorobanSubscriberOptions.pageLimit}. */
  pageSize?: number;
  /** Max distinct event IDs remembered for cross-poll de-duplication. Defaults to 10,000. */
  dedupCacheSize?: number;
  /** Interval for the self-driving {@link SorobanSubscriber.start} poll loop. Defaults to 2000ms. */
  pollIntervalMs?: number;
  /** Delay before retrying after a retryable RPC error. Defaults to 1000ms. */
  retryDelayMs?: number;
  /** Injectable timer scheduler (for testing). Defaults to `globalThis.setTimeout`. */
  setTimeoutFn?: typeof setTimeout;
  /** Injectable timer canceller (for testing). Defaults to `globalThis.clearTimeout`. */
  clearTimeoutFn?: typeof clearTimeout;
  /** Notified when a retryable {@link SorobanRpcError} is caught and a retry is scheduled. */
  onRetryableError?: (error: SorobanRpcError) => void;
  /** Notified when a terminal (non-retryable) {@link SorobanRpcError} is caught. */
  onTerminalError?: (error: unknown) => void;

  // Continuous polling & cursor-expired options (rpcUrl-based construction)
  rpcUrl?: string;
  source?: string;
  cursor?: string;
  filters?: rpc.Api.GetEventsRequest["filters"];
  logger?: {
    info(msg: string, ...args: unknown[]): void;
    warn(msg: string, ...args: unknown[]): void;
    error(msg: string, ...args: unknown[]): void;
  };
  watcher?: Watcher;
}

/** JSON-RPC error shape thrown by @stellar/stellar-sdk when the RPC server
 *  returns an error response. The `code` field is the JSON-RPC error code. */
type JsonRpcError = {
  code: number;
  message: string;
  data?: unknown;
};

/**
 * Detects whether the error thrown by rpc.Server.getEvents indicates
 * that the supplied cursor (or startLedger) is outside the server's retention
 * window.
 *
 * Stellar RPC returns JSON-RPC error code -32600 ("Invalid Request") with a
 * message containing "startLedger" or "cursor" and a phrase like "before the
 * oldest ledger" / "out of range" when the requested ledger is no longer
 * retained.
 */
function isCursorExpiredError(err: unknown): boolean {
  if (typeof err !== "object" || err === null) return false;

  const e = err as JsonRpcError;

  // JSON-RPC -32600 = Invalid Request — the code Stellar RPC uses for
  // out-of-retention-window cursor/startLedger errors.
  if (e.code !== -32600) return false;

  const msg = typeof e.message === "string" ? e.message.toLowerCase() : "";
  return (
    msg.includes("cursor") ||
    msg.includes("startledger") ||
    msg.includes("start_ledger") ||
    msg.includes("before the oldest") ||
    msg.includes("out of range")
  );
}

const MIN_PAGE_LIMIT = 1;
const MAX_PAGE_LIMIT = 10_000;
const DEFAULT_PAGE_LIMIT = 100;
const DEFAULT_DEDUP_CACHE_SIZE = 10_000;

const noop = { info: () => {}, warn: () => {}, error: () => {} };

// ---------------------------------------------------------------------------
// SorobanSubscriber
// ---------------------------------------------------------------------------

export class SorobanSubscriber {
  private readonly rpc: SorobanRpc;
  private readonly cursorStore: CursorStore;
  private readonly onEvent?: (event: SorobanEvent) => Promise<void>;
  private readonly pageLimit: number;

  private isStopped = false;

  /** AbortController for the currently in-flight `getEvents` call. */
  private inflightAbort: AbortController | null = null;

  /** Promise for the currently in-flight `pollOnce` call, used by `stop()`. */
  private inflightPoll: Promise<void> | null = null;

  /**
   * True while `_doPoll` is executing.  Used by `stop()` to avoid a deadlock
   * when `stop()` is called from within an `onEvent` handler — in that case
   * we must not await `inflightPoll` because we are already inside it.
   */
  private isPolling = false;

  /** Active multi-filter subscriptions. Empty means single legacy `onEvent` mode. */
  subscriptions: SorobanSubscription[] = [];

  // --- Cross-poll de-duplication state ---
  /** Insertion-ordered set of recently-delivered event IDs (bounded FIFO window). */
  private readonly seen: LruSet;
  private readonly dedupCacheSize: number;

  // --- Bounded-replay mode state (set when `endLedger` is provided) ---
  /** Exclusive upper-bound ledger; replay stops once an event reaches it. */
  private readonly endLedger?: number;
  /** Called once when a bounded replay run completes. */
  private readonly onDone?: () => void;
  /** Ephemeral cursor used during replay so the durable store is never written. */
  private replayCursor: string | undefined;
  /** True once a replay run has finished (endLedger reached or stream exhausted). */
  private replayDone = false;

  // --- Self-driving poll loop state (used by start()/stop()) ---
  private _isRunning = false;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private readonly pollIntervalMs: number;
  /** ISO timestamp of the most recently delivered event, or null. */
  lastEventAt: string | null = null;

  // --- Retry state ---
  private readonly retryDelayMs: number;
  private readonly setTimeoutFn: typeof setTimeout;
  private readonly clearTimeoutFn: typeof clearTimeout;
  private readonly onRetryableError?: (error: SorobanRpcError) => void;
  private readonly onTerminalError?: (error: unknown) => void;
  private retryTimer: ReturnType<typeof setTimeout> | null = null;

  // --- Cursor-expired recovery state ---
  private readonly source?: string;
  private readonly log: { info(msg: string, ...args: unknown[]): void; warn(msg: string, ...args: unknown[]): void; error(msg: string, ...args: unknown[]): void };
  private watcher: Watcher | null = null;

  constructor(options: SorobanSubscriberOptions) {
    const pageLimit = options.pageLimit ?? options.pageSize ?? DEFAULT_PAGE_LIMIT;
    if (!Number.isFinite(pageLimit) || pageLimit < MIN_PAGE_LIMIT || pageLimit > MAX_PAGE_LIMIT) {
      throw new RangeError(`pageLimit must be between 1 and 10,000 (received ${pageLimit})`);
    }

    this.pageLimit = pageLimit;
    this.dedupCacheSize = options.dedupCacheSize ?? DEFAULT_DEDUP_CACHE_SIZE;
    this.seen = new LruSet(this.dedupCacheSize);
    this.endLedger = options.endLedger;
    this.onDone = options.onDone;
    this.pollIntervalMs = options.pollIntervalMs ?? 2000;
    this.retryDelayMs = options.retryDelayMs ?? 1000;
    this.setTimeoutFn = options.setTimeoutFn ?? globalThis.setTimeout;
    this.clearTimeoutFn = options.clearTimeoutFn ?? globalThis.clearTimeout;
    this.onRetryableError = options.onRetryableError;
    this.onTerminalError = options.onTerminalError;
    this.source = options.source;
    this.log = options.logger ?? noop;

    if (options.watcher) {
      this.watcher = options.watcher;
    }

    // rpcUrl-based construction: build an rpc adapter and an in-memory cursor store
    if (options.rpcUrl) {
      const server = new StellarSdk.rpc.Server(options.rpcUrl);
      let cursor: string | undefined = options.cursor ?? undefined;
      const filters = options.filters ?? [];

      this.rpc = {
        getEvents: async (startCursor: string | undefined, limit: number, signal?: AbortSignal) => {
          const req: any = {
            filters,
            limit,
          };
          if (startCursor) {
            req.cursor = startCursor;
          } else {
            req.startLedger = 0;
          }
          const res = await server.getEvents(req);
          return {
            events: res.events as any as SorobanEvent[],
            cursor: res.cursor,
          };
        },
      };

      this.cursorStore = {
        getCursor: async () => cursor,
        saveCursor: async (c: string | undefined) => {
          cursor = c;
        },
      };
    } else {
      this.rpc = options.rpc!;
      this.cursorStore = options.cursorStore!;
    }

    this.onEvent = options.onEvent;
  }

  /** True when operating in bounded-replay mode (an `endLedger` was supplied). */
  private get isReplayMode(): boolean {
    return this.endLedger !== undefined;
  }

  /** Whether the self-driving poll loop is active. */
  get isRunning(): boolean {
    return this._isRunning;
  }

  /**
   * Begins a self-driving poll loop, invoking {@link pollOnce} immediately and
   * then every `pollIntervalMs`. Idempotent while already running.
   */
  start(): void {
    if (this._isRunning) return;
    this._isRunning = true;
    this.isStopped = false;
    const tick = () => {
      this.inflightPoll = (this.inflightPoll ?? Promise.resolve()).then(() => this.pollOnce());
    };
    tick();
    this.pollTimer = setInterval(tick, this.pollIntervalMs);
    // Allow the Node.js process to exit even if the timer is still active.
    if (
      typeof this.pollTimer === "object" &&
      this.pollTimer !== null &&
      "unref" in this.pollTimer
    ) {
      (this.pollTimer as { unref(): void }).unref();
    }
  }

  /** Marks the run complete and fires `onDone` exactly once. */
  private finishReplay(): void {
    if (this.replayDone) return;
    this.replayDone = true;
    this.onDone?.();
  }

  /**
   * Executes a single poll cycle:
   *   1. Reads the current cursor from the store.
   *   2. Fetches the next page of events from the RPC.
   *   3. Forwards each event to its handler(s) and advances the cursor.
   *
   * If the subscriber is stopped before or during the poll the method returns
   * early without emitting any further events.
   */
  async pollOnce(): Promise<void> {
    if (this.isStopped) return;

    const abort = new AbortController();
    this.inflightAbort = abort;

    const poll = this._doPoll(abort.signal);
    this.inflightPoll = poll;

    try {
      await poll;
    } finally {
      // Clear references once this poll is done (whether it succeeded,
      // was aborted, or threw for another reason).
      if (this.inflightPoll === poll) {
        this.inflightPoll = null;
      }
      if (this.inflightAbort === abort) {
        this.inflightAbort = null;
      }
    }
  }

  /**
   * Gracefully stops the subscriber.
   *
   * - Marks the subscriber as stopped so no new polls begin.
   * - Aborts any in-flight `getEvents` request.
   * - Cancels any pending retry timer.
   * - Awaits the in-flight poll so that, once this Promise resolves, the
   *   caller is guaranteed no further events will be emitted.
   *
   * When called from within an `onEvent` handler (i.e. from inside the poll
   * itself) the await is skipped to avoid a deadlock — the poll will naturally
   * terminate on the next `isStopped` check after `onEvent` returns.
   */
  async stop(): Promise<void> {
    this.isStopped = true;
    this._isRunning = false;
    if (this.pollTimer !== null) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
    if (this.retryTimer !== null) {
      this.clearTimeoutFn(this.retryTimer);
      this.retryTimer = null;
    }
    this.inflightAbort?.abort();
    // Only await the in-flight poll when we are NOT already inside it.
    // Awaiting from within onEvent would deadlock because the poll is waiting
    // for onEvent to return before it can settle.
    if (this.inflightPoll && !this.isPolling) {
      await this.inflightPoll;
    }
  }

  /** Alias for {@link stop}; mirrors the lifecycle vocabulary of EventEngine. */
  async shutdown(): Promise<void> {
    await this.stop();
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private async _doPoll(signal: AbortSignal): Promise<void> {
    // In replay mode, bail immediately if we've already reached endLedger.
    if (this.isReplayMode && this.replayDone) return;

    let activeSubs = [...this.subscriptions];
    if (activeSubs.length === 0) {
      activeSubs = [{ id: "__legacy__", filters: [] }];
    }

    let rpcCalls: ContractSubscriptionFilter[][] = [];
    const hasMatchAll = activeSubs.some((sub) => sub.filters.length === 0);

    if (hasMatchAll) {
      rpcCalls = [[]];
    } else {
      const flatFilters: ContractSubscriptionFilter[] = [];
      for (const sub of activeSubs) {
        flatFilters.push(...sub.filters);
      }

      if (flatFilters.length === 0) {
        rpcCalls = [[]];
      } else {
        for (let i = 0; i < flatFilters.length; i += 5) {
          rpcCalls.push(flatFilters.slice(i, i + 5));
        }
      }
    }

    // In replay mode use the ephemeral replayCursor; otherwise read from store.
    const currentCursor = this.isReplayMode
      ? this.replayCursor
      : await this.cursorStore.getCursor();

    const promises = rpcCalls.map((filters) =>
      this.rpc.getEvents(
        currentCursor,
        this.pageLimit,
        signal,
        filters.length > 0 ? filters : undefined,
      ),
    );

    let results: { events: SorobanEvent[]; cursor?: string }[];
    try {
      results = await Promise.all(promises);
    } catch (err) {
      // An aborted request is expected during shutdown — swallow it silently.
      if (this.isAbortError(err)) return;
      // Cursor-expired errors are recoverable — emit notification & reset.
      if (isCursorExpiredError(err)) {
        await this.recoverFromExpiredCursor(err);
        return;
      }
      // Route classified RPC errors to the retry/terminal handlers when present.
      if (err instanceof SorobanRpcError) {
        if (err.retryable) {
          if (this.onRetryableError) {
            this.onRetryableError(err);
            this.scheduleRetry();
            return;
          }
        } else if (this.onTerminalError) {
          this.onTerminalError(err);
          return;
        }
      }
      throw err;
    }

    const allEventsMap = new Map<string, SorobanEvent>();
    for (const res of results) {
      if (res && res.events) {
        for (const event of res.events) {
          allEventsMap.set(event.id, event);
        }
      }
    }

    const uniqueEvents = Array.from(allEventsMap.values());

    if (rpcCalls.length > 1) {
      uniqueEvents.sort((a, b) => a.pagingToken.localeCompare(b.pagingToken));
    }

    // A bounded replay that fetched no further events has exhausted the stream
    // before reaching endLedger — finish so onDone fires exactly once.
    if (this.isReplayMode && uniqueEvents.length === 0) {
      this.finishReplay();
      return;
    }

    this.isPolling = true;
    try {
      for (const event of uniqueEvents) {
        // Re-check after every event delivery in case stop() was called
        // concurrently (e.g. from within the onEvent handler).
        if (this.isStopped) return;

        // In replay mode, stop (exclusive) once an event reaches endLedger.
        if (this.isReplayMode && this.endLedger !== undefined) {
          const ledger = this.extractLedger(event);
          if (ledger !== undefined && ledger >= this.endLedger) {
            this.finishReplay();
            return;
          }
        }

        // Cross-poll de-duplication: suppress IDs we've already delivered.
        if (this.seen.has(event.id)) continue;

        // Deliver before recording so a throwing handler leaves the ID
        // un-recorded (and therefore re-deliverable on a later poll).
        await this.dispatch(event);
        this.lastEventAt = new Date().toISOString();
        this.seen.add(event.id);

        if (this.isReplayMode) {
          // Replay progress is ephemeral and must never touch the durable store.
          this.replayCursor = event.pagingToken;
        } else {
          await this.cursorStore.saveCursor(event.pagingToken);
        }
      }
    } finally {
      this.isPolling = false;
    }
  }

  /**
   * Routes a single event to its handler(s).
   *
   * - Legacy mode (no subscriptions): invokes the constructor `onEvent`.
   * - Subscription mode: invokes the `onEvent` of every subscription whose
   *   filters match the event, falling back to the constructor `onEvent`.
   */
  private async dispatch(event: SorobanEvent): Promise<void> {
    if (this.subscriptions.length === 0) {
      if (this.onEvent) await this.onEvent(event);
      if (this.watcher) {
        this.watcher.emit("soroban.event", event as never);
        this.watcher.emit("*", event as never);
      }
      return;
    }

    for (const sub of this.subscriptions) {
      if (this.eventMatchesSubscription(event, sub)) {
        const handler = sub.onEvent ?? this.onEvent;
        if (handler) await handler(event);
      }
    }
  }

  /** True when any of the subscription's filters matches the event. */
  private eventMatchesSubscription(event: SorobanEvent, sub: SorobanSubscription): boolean {
    // A subscription with no filters matches every event.
    if (sub.filters.length === 0) return true;
    return sub.filters.some((filter) => this.eventMatchesFilter(event, filter));
  }

  /** True when the event satisfies a single filter (currently contractId scoped). */
  private eventMatchesFilter(event: SorobanEvent, filter: ContractSubscriptionFilter): boolean {
    const contractIds = filter.contractIds as ContractAddress[] | undefined;
    if (contractIds && contractIds.length > 0) {
      return (
        event.contractId !== undefined && contractIds.includes(event.contractId as ContractAddress)
      );
    }
    // No contractId constraint → matches.
    return true;
  }

  /** Schedules a single deferred re-poll using the injectable timer. */
  private scheduleRetry(): void {
    if (this.isStopped) return;
    this.retryTimer = this.setTimeoutFn(() => {
      this.retryTimer = null;
      if (this.isStopped) return;
      this.inflightPoll = (this.inflightPoll ?? Promise.resolve()).then(() => this.pollOnce());
    }, this.retryDelayMs);
  }

  /**
   * Recovers from a cursor-expired error by:
   *   1. Logging a warning with data-loss implications.
   *   2. Emitting an `engine.cursor_expired` notification on the Watcher.
   *   3. Clearing the cursor so the next poll uses startLedger mode.
   */
  private async recoverFromExpiredCursor(err: unknown): Promise<void> {
    const lostCursor = await this.cursorStore.getCursor();

    this.log.warn(
      `[pulse-core] SorobanSubscriber(${this.source ?? "unknown"}): cursor "${lostCursor ?? "<none>"}" ` +
        `is outside the RPC server's retention window. ` +
        `Events between the expired cursor and the recovery point are PERMANENTLY LOST. ` +
        `Resuming from latestLedger. ` +
        `Consider persisting a durable replay store to detect such gaps in future.`,
      err,
    );

    if (this.watcher) {
      const notification: SorobanCursorExpiredNotification = {
        type: "engine.cursor_expired",
        source: this.source ?? "unknown",
        lostCursor,
        emittedAt: new Date().toISOString(),
      };
      this.watcher.emit("engine.cursor_expired", notification as never);
    }

    await this.cursorStore.saveCursor(undefined);
  }

  /**
   * Extracts the ledger sequence number from a SorobanEvent.
   * The Soroban RPC embeds the ledger in the event `id` field as
   * `<ledger>-<index>` (e.g. "1234-0").  Falls back to a `ledger` field if
   * present on the raw event object.
   */
  private extractLedger(event: SorobanEvent): number | undefined {
    // Prefer explicit ledger field (available in some RPC responses).
    const raw = event as unknown as Record<string, unknown>;
    if (typeof raw.ledger === "number") return raw.ledger;

    // Parse from paging token / id encoded as "<ledger>-<index>".
    const match = event.id.match(/^(\d+)-/);
    if (match && match[1] !== undefined) {
      const n = parseInt(match[1], 10);
      if (!isNaN(n)) return n;
    }
    return undefined;
  }

  private isAbortError(err: unknown): boolean {
    if (err instanceof Error) {
      // DOMException name set by the Fetch API / AbortController
      if ((err as { name?: string }).name === "AbortError") return true;
      // Node.js / undici uses this code
      if ((err as NodeJS.ErrnoException).code === "ABORT_ERR") return true;
    }
    return false;
  }
}
