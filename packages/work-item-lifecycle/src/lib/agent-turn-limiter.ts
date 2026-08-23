import { Context, Deferred, Duration, Effect, Ref } from "effect"
import type { SqlClient } from "effect/unstable/sql/SqlClient"
import type {
  AgentBackendError,
  AgentTurnResult,
  ContinueTurnInput,
  InspectInput,
  InspectResult,
  StartTurnInput,
} from "@ready-for-agent/agent-backend"
import { AgentBackend } from "@ready-for-agent/agent-backend"
import type { DbServiceShape } from "@ready-for-agent/db-service"
import { STEP_RUN_REASON, WAITING_FOR_AGENT_TURN_MESSAGE } from "./types.js"

const DEFAULT_MAX_CONCURRENT_AGENT_TURNS = 2
const CONFIG_RECHECK_INTERVAL = Duration.millis(200)

/**
 * Bucket key for Agent Turns with no ambient Step Run / Repository identity
 * (for example a call made outside a Lifecycle Step handler). All such calls
 * share one bucket, so behavior is unchanged from a single shared limiter
 * when Repository identity is unavailable.
 */
const NO_REPOSITORY_BUCKET = "__no_current_step_run__"

export interface AgentBackendService {
  readonly startTurn: (
    input: StartTurnInput,
  ) => Effect.Effect<AgentTurnResult, AgentBackendError>
  readonly continueTurn: (
    input: ContinueTurnInput,
  ) => Effect.Effect<AgentTurnResult, AgentBackendError>
  readonly inspect: (
    input: InspectInput,
  ) => Effect.Effect<InspectResult, AgentBackendError>
}

/**
 * Ambient Step Run identity for the fiber executing a Lifecycle Step handler.
 * Used by the Agent Turn limiter to project mid-run wait state and to admit
 * Agent Turns fairly per Repository.
 */
export type CurrentStepRunValue = {
  readonly stepRunId: string
  readonly repositoryId: string
} | null

export const CurrentStepRun = Context.Reference<CurrentStepRunValue>(
  "@ready-for-agent/work-item-lifecycle/CurrentStepRun",
  { defaultValue: () => null },
)

/**
 * Captured Agent Backend id for the Work Item whose Step Run is executing.
 * Lifecycle sets this for the handler fiber so Agent Turns and Session ops
 * route to the captured adapter instead of re-resolving settings each turn.
 */
export type CurrentCapturedAgentBackendIdValue = string | null

export const CurrentCapturedAgentBackendId =
  Context.Reference<CurrentCapturedAgentBackendIdValue>(
    "@ready-for-agent/work-item-lifecycle/CurrentCapturedAgentBackendId",
    { defaultValue: () => null },
  )

type SavedStepRunReason = {
  readonly reasonCode: string | null
  readonly reasonMessage: string | null
}

/** Per-Repository Agent Turn admission bucket. */
interface RepoTurnBucket {
  /** Currently held permits for this Repository. */
  readonly inFlight: number
  /**
   * Global sequence number recorded the last time this Repository was
   * admitted. A never-admitted Repository keeps the default `0`, which
   * sorts as least-recently-serviced ahead of any Repository that has ever
   * been admitted.
   */
  readonly lastAdmittedSeq: number
  /** FIFO queue of waiters currently blocked for this Repository. */
  readonly queue: ReadonlyArray<Deferred.Deferred<void>>
}

const emptyRepoTurnBucket: RepoTurnBucket = {
  inFlight: 0,
  lastAdmittedSeq: 0,
  queue: [],
}

interface TurnSchedulerState {
  readonly buckets: Readonly<Record<string, RepoTurnBucket>>
  readonly totalInFlight: number
  readonly seq: number
}

const emptySchedulerState: TurnSchedulerState = {
  buckets: {},
  totalInFlight: 0,
  seq: 0,
}

/**
 * Drop a Repository's bucket once it is fully idle (no in-flight permits,
 * no waiters), so a long-lived harness process does not accumulate one
 * permanent bucket entry per Repository ID it has ever seen. A Repository
 * that becomes active again later simply gets a fresh bucket via `enqueue`.
 */
const withUpdatedBucket = (
  buckets: Readonly<Record<string, RepoTurnBucket>>,
  repositoryId: string,
  bucket: RepoTurnBucket,
): Readonly<Record<string, RepoTurnBucket>> => {
  if (bucket.inFlight > 0 || bucket.queue.length > 0) {
    return { ...buckets, [repositoryId]: bucket }
  }
  const next = { ...buckets }
  delete next[repositoryId]
  return next
}

/**
 * Pick the Repository that should be admitted next among those with a
 * non-empty waiter queue. A Repository with an unmet guaranteed minimum
 * (in-flight permits below its configured floor) is a priority claim ahead
 * of ordinary fair-share rotation; within the priority pool (or across all
 * pending Repositories when no unmet guarantee exists) the
 * least-recently-serviced Repository is picked, so no Repository can
 * indefinitely starve another simply by having steadier demand.
 */
const pickNextRepository = (
  state: TurnSchedulerState,
  guaranteedMins: Readonly<Record<string, number>>,
): string | null => {
  const pending = Object.entries(state.buckets).filter(
    ([, bucket]) => bucket.queue.length > 0,
  )
  if (pending.length === 0) {
    return null
  }
  const unmet = pending.filter(
    ([repositoryId, bucket]) =>
      (guaranteedMins[repositoryId] ?? 0) > bucket.inFlight,
  )
  const pool = unmet.length > 0 ? unmet : pending
  let best: readonly [string, RepoTurnBucket] | null = null
  for (const entry of pool) {
    if (best === null || entry[1].lastAdmittedSeq < best[1].lastAdmittedSeq) {
      best = entry
    }
  }
  return best === null ? null : best[0]
}

/**
 * Admit as many queued waiters as global capacity allows. Pure state
 * transition; the caller is responsible for resolving the returned
 * `Deferred`s once this has been committed to the `Ref`.
 */
const admitPending = (
  state: TurnSchedulerState,
  maxConcurrentAgentTurns: number,
  guaranteedMins: Readonly<Record<string, number>>,
): {
  readonly next: TurnSchedulerState
  readonly admitted: ReadonlyArray<Deferred.Deferred<void>>
} => {
  let current = state
  const admitted: Deferred.Deferred<void>[] = []
  for (;;) {
    if (current.totalInFlight >= maxConcurrentAgentTurns) {
      break
    }
    const repositoryId = pickNextRepository(current, guaranteedMins)
    if (repositoryId === null) {
      break
    }
    const bucket = current.buckets[repositoryId] ?? emptyRepoTurnBucket
    const [deferred, ...rest] = bucket.queue
    if (deferred === undefined) {
      break
    }
    admitted.push(deferred)
    const seq = current.seq + 1
    current = {
      seq,
      totalInFlight: current.totalInFlight + 1,
      buckets: {
        ...current.buckets,
        [repositoryId]: {
          inFlight: bucket.inFlight + 1,
          lastAdmittedSeq: seq,
          queue: rest,
        },
      },
    }
  }
  return { next: current, admitted }
}

/**
 * Cap concurrent lifecycle Agent Turn processes using the current harness
 * Config value, admitted fairly per Repository. `inspect` is not wrapped.
 *
 * Re-reads Config on each scheduling attempt so raising the limit frees
 * capacity promptly and lowering it does not interrupt in-flight processes
 * (they finish; new acquires wait until taken drops). When more than one
 * Repository has pending demand for a freed permit, the Repository least
 * recently granted one is admitted next; a Repository with no pending demand
 * does not affect ordering for the Repositories that do. A Repository's
 * optional guaranteed-minimum concurrent Agent Turns (Repository setting
 * `guaranteedMinConcurrentAgentTurns`) is honored as a priority claim ahead
 * of fair-share rotation while that Repository has an unmet guarantee and
 * pending demand — an idle, unmet guarantee never withholds capacity from
 * other Repositories.
 *
 * While waiting for a permit, marks the ambient Step Run with
 * `waiting_for_agent_turn` so GraphQL can show **Queued** instead of
 * **Running**, and records `session_wait_started_at` so max-duration and
 * visibility-lease clocks freeze for the wait. When the slot is acquired,
 * accumulates the wait into `session_wait_ms` and restores any prior mid-run
 * phase (for example Review: pre-commit) instead of clearing the reason.
 */
export const limitAgentTurns = (
  backend: AgentBackendService,
  db: Pick<DbServiceShape, "getConfig" | "notifyWorkItemsChanged">,
  sql: Pick<SqlClient, "unsafe">,
): Effect.Effect<AgentBackendService> =>
  Effect.gen(function* () {
    const stateRef = yield* Ref.make<TurnSchedulerState>(emptySchedulerState)

    const currentMax = db.getConfig.pipe(
      Effect.map((config) => Math.max(1, config.maxConcurrentAgentTurns)),
      Effect.orElseSucceed(() => DEFAULT_MAX_CONCURRENT_AGENT_TURNS),
    )

    /** Live Repository guaranteed-minimum concurrent Agent Turns. Missing/null → 0 (fully fair-share). */
    const loadGuaranteedMin = (repositoryId: string): Effect.Effect<number> =>
      repositoryId === NO_REPOSITORY_BUCKET
        ? Effect.succeed(0)
        : sql
            .unsafe(
              `SELECT guaranteed_min_concurrent_agent_turns AS guaranteedMin
               FROM repository WHERE id = ?`,
              [repositoryId],
            )
            .pipe(
              Effect.map((rows) => {
                const row = (
                  rows as readonly { readonly guaranteedMin: number | null }[]
                )[0]
                const raw = row?.guaranteedMin
                return typeof raw === "number" &&
                  Number.isFinite(raw) &&
                  raw > 0
                  ? raw
                  : 0
              }),
              Effect.orElseSucceed(() => 0),
            )

    /**
     * Re-evaluate admission: read the current global cap and the guaranteed
     * minimum of every Repository with pending demand, then admit as many
     * queued waiters as capacity allows. Safe to call concurrently and
     * redundantly — the state transition is atomic and a `Deferred` can only
     * be completed once.
     */
    const attemptSchedule = (): Effect.Effect<void> =>
      Effect.gen(function* () {
        const state = yield* Ref.get(stateRef)
        const pendingRepositoryIds = Object.entries(state.buckets)
          .filter(([, bucket]) => bucket.queue.length > 0)
          .map(([repositoryId]) => repositoryId)
        if (pendingRepositoryIds.length === 0) {
          return
        }
        const max = yield* currentMax
        const guaranteedEntries = yield* Effect.forEach(
          pendingRepositoryIds,
          (repositoryId) =>
            loadGuaranteedMin(repositoryId).pipe(
              Effect.map((min) => [repositoryId, min] as const),
            ),
          { concurrency: "unbounded" },
        )
        const guaranteedMins = Object.fromEntries(guaranteedEntries)
        // Uninterruptible: commit admission to the Ref and notify every
        // admitted waiter's `Deferred` as one unit, so a batch is never left
        // half-notified if this call is interrupted partway through. This is
        // a liveness/tidiness guarantee, not the mechanism that prevents a
        // leak on interruption — `abandonWaiter` establishes that guarantee
        // independently by consulting the Ref's own queue membership (see
        // below), which flips atomically with this same commit regardless of
        // when (or by which fiber) each `Deferred` is actually resolved.
        yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const admitted = yield* Ref.modify(stateRef, (current) => {
              const result = admitPending(current, max, guaranteedMins)
              return [result.admitted, result.next]
            })
            yield* Effect.forEach(
              admitted,
              (deferred) => Deferred.succeed(deferred, undefined),
              { discard: true },
            )
          }),
        )
      })

    const enqueue = (
      repositoryId: string,
      deferred: Deferred.Deferred<void>,
    ): Effect.Effect<void> =>
      Ref.update(stateRef, (state) => {
        const bucket = state.buckets[repositoryId] ?? emptyRepoTurnBucket
        return {
          ...state,
          buckets: {
            ...state.buckets,
            [repositoryId]: {
              ...bucket,
              queue: [...bucket.queue, deferred],
            },
          },
        }
      })

    const release = (repositoryId: string): Effect.Effect<void> =>
      Ref.update(stateRef, (state) => {
        const bucket = state.buckets[repositoryId] ?? emptyRepoTurnBucket
        const updated = {
          ...bucket,
          inFlight: Math.max(0, bucket.inFlight - 1),
        }
        return {
          ...state,
          totalInFlight: Math.max(0, state.totalInFlight - 1),
          buckets: withUpdatedBucket(state.buckets, repositoryId, updated),
        }
      })

    /**
     * Cleanup for a waiter abandoned (interrupted) before `withTurnSlot`
     * finished waiting. Atomically checks the Ref's own queue membership —
     * the single source of truth for whether a permit was committed to this
     * waiter — rather than that waiter's `Deferred` completion, which can
     * momentarily lag a cross-fiber commit (another waiter's `attemptSchedule`
     * call can commit this waiter's admission to the Ref before it gets to
     * resolving this specific `Deferred` in its own notify loop). Still
     * queued: dequeue it, no permit was ever granted. Already removed from
     * the queue (whether or not its `Deferred` has been resolved yet): the
     * scheduler already committed a permit to it, so release that permit
     * back rather than leaking it.
     */
    const abandonWaiter = (
      repositoryId: string,
      deferred: Deferred.Deferred<void>,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        const wasStillQueued = yield* Ref.modify(stateRef, (state) => {
          const bucket = state.buckets[repositoryId]
          if (bucket === undefined || !bucket.queue.includes(deferred)) {
            return [false, state]
          }
          const updated = {
            ...bucket,
            queue: bucket.queue.filter((waiter) => waiter !== deferred),
          }
          return [
            true,
            {
              ...state,
              buckets: withUpdatedBucket(state.buckets, repositoryId, updated),
            },
          ]
        })
        if (!wasStillQueued) {
          yield* release(repositoryId)
          yield* attemptSchedule()
        }
      })

    const markWaiting = (): Effect.Effect<SavedStepRunReason | null> =>
      Effect.gen(function* () {
        const current = yield* CurrentStepRun
        if (current === null) {
          return null
        }
        const rows = (yield* sql.unsafe(
          `SELECT reason_code, reason_message, session_wait_started_at
           FROM step_run
           WHERE id = ? AND status = 'running'`,
          [current.stepRunId],
        )) as readonly {
          readonly reason_code: string | null
          readonly reason_message: string | null
          readonly session_wait_started_at: number | null
        }[]
        const row = rows[0]
        if (row === undefined) {
          return null
        }
        const saved: SavedStepRunReason = {
          reasonCode:
            row.reason_code === STEP_RUN_REASON.waitingForAgentTurn
              ? null
              : row.reason_code,
          reasonMessage:
            row.reason_code === STEP_RUN_REASON.waitingForAgentTurn
              ? null
              : row.reason_message,
        }
        const now = Date.now()
        // Keep the original wait start if already waiting (idempotent re-mark).
        const waitStartedAt = row.session_wait_started_at ?? now
        yield* sql.unsafe(
          `UPDATE step_run
           SET reason_code = ?,
               reason_message = ?,
               session_wait_started_at = ?,
               updated_at = ?
           WHERE id = ?
             AND status = 'running'`,
          [
            STEP_RUN_REASON.waitingForAgentTurn,
            WAITING_FOR_AGENT_TURN_MESSAGE,
            waitStartedAt,
            now,
            current.stepRunId,
          ],
        )
        yield* db.notifyWorkItemsChanged(current.repositoryId)
        return saved
      }).pipe(
        Effect.catch((error) =>
          Effect.logWarning(
            "Failed to update Agent Turn wait state on Step Run",
            { waiting: true, error },
          ).pipe(Effect.as(null)),
        ),
      )

    const clearWaiting = (
      saved: SavedStepRunReason | null,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        const current = yield* CurrentStepRun
        if (current === null) {
          return
        }
        const now = Date.now()
        const rows = (yield* sql.unsafe(
          `SELECT session_wait_started_at, session_wait_ms
           FROM step_run
           WHERE id = ?
             AND status = 'running'
             AND reason_code = ?`,
          [current.stepRunId, STEP_RUN_REASON.waitingForAgentTurn],
        )) as readonly {
          readonly session_wait_started_at: number | null
          readonly session_wait_ms: number | null
        }[]
        const row = rows[0]
        if (row === undefined) {
          return
        }
        const openWaitMs =
          row.session_wait_started_at === null
            ? 0
            : Math.max(0, now - row.session_wait_started_at)
        const sessionWaitMs = Math.max(0, row.session_wait_ms ?? 0) + openWaitMs
        yield* sql.unsafe(
          `UPDATE step_run
           SET reason_code = ?,
               reason_message = ?,
               session_wait_ms = ?,
               session_wait_started_at = NULL,
               updated_at = ?
           WHERE id = ?
             AND status = 'running'
             AND reason_code = ?`,
          [
            saved?.reasonCode ?? null,
            saved?.reasonMessage ?? null,
            sessionWaitMs,
            now,
            current.stepRunId,
            STEP_RUN_REASON.waitingForAgentTurn,
          ],
        )
        yield* db.notifyWorkItemsChanged(current.repositoryId)
      }).pipe(
        Effect.catch((error) =>
          Effect.logWarning(
            "Failed to update Agent Turn wait state on Step Run",
            { waiting: false, error },
          ),
        ),
        Effect.asVoid,
      )

    const withTurnSlot = <A, E, R>(
      effect: Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E, R> =>
      Effect.gen(function* () {
        const current = yield* CurrentStepRun
        const repositoryId = current?.repositoryId ?? NO_REPOSITORY_BUCKET
        const deferred = yield* Deferred.make<void>()
        yield* enqueue(repositoryId, deferred)

        let markedWaiting = false
        let savedReason: SavedStepRunReason | null = null

        const waitForSlot = Effect.gen(function* () {
          for (;;) {
            if (yield* Deferred.isDone(deferred)) {
              return
            }
            yield* attemptSchedule()
            if (yield* Deferred.isDone(deferred)) {
              return
            }
            if (!markedWaiting) {
              savedReason = yield* markWaiting()
              markedWaiting = true
            }
            yield* Effect.race(
              Deferred.await(deferred),
              Effect.sleep(CONFIG_RECHECK_INTERVAL),
            )
          }
        }).pipe(Effect.onInterrupt(() => abandonWaiter(repositoryId, deferred)))

        yield* waitForSlot

        // From here on the permit is granted (the Ref already reflects it as
        // in-flight). Guard the *entire* remaining span — restoring any prior
        // mid-run phase and then running the actual turn — with the release
        // finalizer, so an interrupt landing anywhere in this span (including
        // while clearing the waiting mark, before the wrapped Agent Backend
        // call even starts) still frees the permit instead of leaking it.
        return yield* Effect.gen(function* () {
          if (markedWaiting) {
            yield* clearWaiting(savedReason)
          }
          return yield* effect
        }).pipe(
          Effect.ensuring(
            Effect.gen(function* () {
              yield* release(repositoryId)
              yield* attemptSchedule()
            }),
          ),
        )
      })

    return AgentBackend.of({
      startTurn: (input) => withTurnSlot(backend.startTurn(input)),
      continueTurn: (input) => withTurnSlot(backend.continueTurn(input)),
      inspect: (input) => backend.inspect(input),
    })
  })
