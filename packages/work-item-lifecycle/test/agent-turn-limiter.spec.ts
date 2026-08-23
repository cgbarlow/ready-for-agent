import { Deferred, Effect, Fiber, Layer, Ref } from "effect"
import { SqlClient } from "effect/unstable/sql"
import { AgentBackend } from "@ready-for-agent/agent-backend"
import { DatabaseTest } from "@ready-for-agent/db/test"
import {
  DbService,
  DbServiceLive,
  type DbServiceShape,
} from "@ready-for-agent/db-service"
import {
  CurrentStepRun,
  limitAgentTurns,
} from "../src/lib/agent-turn-limiter.js"
import {
  REVIEW_PRE_COMMIT_MESSAGE,
  STEP_RUN_REASON,
  WAITING_FOR_AGENT_TURN_MESSAGE,
} from "../src/lib/types.js"
import { describe, expect, it } from "bun:test"

const TestLayer = DbServiceLive.pipe(Layer.provideMerge(DatabaseTest))

const runTest = <A, E>(
  effect: Effect.Effect<A, E, Layer.Layer.Success<typeof TestLayer>>,
) => Effect.runPromise(Effect.provide(effect, TestLayer))

const startInput = {
  prompt: "implement",
  cwd: "/tmp/worktree",
  model: "test/model",
  thinkingLevel: "low",
}

const seedRunningStepRun = (input: {
  readonly stepRunId: string
  readonly workItemId: string
  readonly repositoryId: string
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const now = Date.now()
    yield* sql.unsafe(
      `INSERT INTO repository (
         id, forge, forge_host, project_path, local_path, is_bare, paused,
         issues_reconciled_at, created_at, updated_at
       ) VALUES (?, 'github', 'github.com', 'o/r', ?, 1, 0, NULL, ?, ?)`,
      [input.repositoryId, `/tmp/${input.repositoryId}`, now, now],
    )
    yield* sql.unsafe(
      `INSERT INTO work_item (
         id, repository_id, issue_number, state, state_ready_at, worktree_path,
         session_id, failure_code, failure_message, created_at, updated_at
       ) VALUES (?, ?, 1, 'implement', ?,
         '/tmp/worktree', NULL, NULL, NULL, ?, ?)`,
      [input.workItemId, input.repositoryId, now, now, now],
    )
    yield* sql.unsafe(
      `INSERT INTO step_run (
         id, work_item_id, step, status, queue_job_id, queued_at,
         started_at, finished_at, reason_code, reason_message,
         created_at, updated_at
       ) VALUES (?, ?, 'implement', 'running', NULL, ?, ?, NULL, NULL, NULL, ?, ?)`,
      [input.stepRunId, input.workItemId, now, now, now, now],
    )
  })

/** Seed a bare Repository row with an optional guaranteed-minimum floor. */
const seedRepositoryGuarantee = (input: {
  readonly repositoryId: string
  readonly guaranteedMin: number | null
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const now = Date.now()
    yield* sql.unsafe(
      `INSERT INTO repository (
         id, forge, forge_host, project_path, local_path, is_bare, paused,
         guaranteed_min_concurrent_agent_turns,
         issues_reconciled_at, created_at, updated_at
       ) VALUES (?, 'github', 'github.com', 'o/r', ?, 1, 0, ?, NULL, ?, ?)`,
      [
        input.repositoryId,
        `/tmp/${input.repositoryId}`,
        input.guaranteedMin,
        now,
        now,
      ],
    )
  })

describe("limitAgentTurns", () => {
  it("caps concurrent start/continue to Config max and queues the rest", () =>
    runTest(
      Effect.gen(function* () {
        const db = yield* DbService
        const sql = yield* SqlClient.SqlClient
        yield* db.updateConfig({
          selectedAgentBackend: "opencode",
          defaultModel: "opencode/deepseek-v4-flash-free",
          defaultThinkingLevel: "low",
          reviewModel: null,
          reviewThinkingLevel: null,
          maxConcurrentAgentTurns: 2,
          maxConcurrentWorkItems: 5,
        })

        const release = yield* Deferred.make<void>()
        const twoRunning = yield* Deferred.make<void>()
        const started = yield* Ref.make(0)
        const maximumActive = yield* Ref.make(0)
        const active = yield* Ref.make(0)

        const gatedRun = () =>
          Effect.gen(function* () {
            yield* Ref.update(active, (n) => n + 1)
            const current = yield* Ref.get(active)
            yield* Ref.update(maximumActive, (max) => Math.max(max, current))
            const count = yield* Ref.updateAndGet(started, (n) => n + 1)
            if (count === 2) {
              yield* Deferred.succeed(twoRunning, undefined)
            }
            yield* Deferred.await(release)
            yield* Ref.update(active, (n) => n - 1)
            return { sessionId: "ses_test", assistantText: "" }
          })

        const inner = AgentBackend.of({
          startTurn: () => gatedRun(),
          continueTurn: () => gatedRun(),
          inspect: () =>
            Effect.succeed({
              backend: { id: "opencode" as const, label: "OpenCode" },
              models: [],
            }),
        })
        const limited = yield* limitAgentTurns(inner, db, sql)

        const first = yield* limited
          .startTurn(startInput)
          .pipe(Effect.forkChild)
        const second = yield* limited
          .startTurn(startInput)
          .pipe(Effect.forkChild)
        const third = yield* limited
          .continueTurn({
            ...startInput,
            sessionId: "ses_existing",
          })
          .pipe(Effect.forkChild)

        yield* Deferred.await(twoRunning)
        expect(yield* Ref.get(started)).toBe(2)
        expect(yield* Ref.get(maximumActive)).toBe(2)
        expect(yield* Ref.get(active)).toBe(2)

        yield* Effect.sleep("50 millis")
        expect(yield* Ref.get(started)).toBe(2)

        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(first)
        yield* Fiber.join(second)
        yield* Fiber.join(third)

        expect(yield* Ref.get(started)).toBe(3)
        expect(yield* Ref.get(maximumActive)).toBe(2)
        expect(yield* Ref.get(active)).toBe(0)
      }),
    ))

  it("does not count inspect toward the Agent Turn limit", () =>
    runTest(
      Effect.gen(function* () {
        const db = yield* DbService
        const sql = yield* SqlClient.SqlClient
        yield* db.updateConfig({
          selectedAgentBackend: "opencode",
          defaultModel: "opencode/deepseek-v4-flash-free",
          defaultThinkingLevel: "low",
          reviewModel: null,
          reviewThinkingLevel: null,
          maxConcurrentAgentTurns: 1,
          maxConcurrentWorkItems: 5,
        })

        const releaseStart = yield* Deferred.make<void>()
        const inspectStarted = yield* Deferred.make<void>()
        let startActive = false
        let inspectWhileStartActive = false

        const inner = AgentBackend.of({
          startTurn: () =>
            Effect.gen(function* () {
              startActive = true
              yield* Deferred.await(releaseStart)
              startActive = false
              return { sessionId: "ses_test", assistantText: "" }
            }),
          continueTurn: () =>
            Effect.succeed({ sessionId: "ses_test", assistantText: "" }),
          inspect: () =>
            Effect.gen(function* () {
              inspectWhileStartActive = startActive
              yield* Deferred.succeed(inspectStarted, undefined)
              return {
                backend: { id: "opencode" as const, label: "OpenCode" },
                models: [{ id: "model-a", thinkingLevels: ["low"] }],
              }
            }),
        })
        const limited = yield* limitAgentTurns(inner, db, sql)

        const startFiber = yield* limited
          .startTurn(startInput)
          .pipe(Effect.forkChild)
        yield* Effect.sleep("20 millis")
        const models = yield* limited.inspect({ cwd: "/tmp" })
        yield* Deferred.await(inspectStarted)
        yield* Deferred.succeed(releaseStart, undefined)
        yield* Fiber.join(startFiber)

        expect(models.models).toEqual([
          { id: "model-a", thinkingLevels: ["low"] },
        ])
        expect(inspectWhileStartActive).toBe(true)
      }),
    ))

  it("admits a waiter when Config max is raised while a run is in flight", () =>
    runTest(
      Effect.gen(function* () {
        const db = yield* DbService
        const sql = yield* SqlClient.SqlClient
        yield* db.updateConfig({
          selectedAgentBackend: "opencode",
          defaultModel: "opencode/deepseek-v4-flash-free",
          defaultThinkingLevel: "low",
          reviewModel: null,
          reviewThinkingLevel: null,
          maxConcurrentAgentTurns: 1,
          maxConcurrentWorkItems: 5,
        })

        const releaseFirst = yield* Deferred.make<void>()
        const firstStarted = yield* Deferred.make<void>()
        const secondStarted = yield* Deferred.make<void>()
        let starts = 0

        const inner = AgentBackend.of({
          startTurn: () =>
            Effect.gen(function* () {
              starts += 1
              if (starts === 1) {
                yield* Deferred.succeed(firstStarted, undefined)
                yield* Deferred.await(releaseFirst)
              } else {
                yield* Deferred.succeed(secondStarted, undefined)
              }
              return { sessionId: `ses_${starts}`, assistantText: "" }
            }),
          continueTurn: () =>
            Effect.succeed({ sessionId: "ses_x", assistantText: "" }),
          inspect: () =>
            Effect.succeed({
              backend: { id: "opencode" as const, label: "OpenCode" },
              models: [],
            }),
        })
        const limited = yield* limitAgentTurns(inner, db, sql)

        const first = yield* limited
          .startTurn(startInput)
          .pipe(Effect.forkChild)
        yield* Deferred.await(firstStarted)

        const second = yield* limited
          .startTurn(startInput)
          .pipe(Effect.forkChild)
        yield* Effect.sleep("50 millis")
        expect(starts).toBe(1)

        yield* db.updateConfig({
          selectedAgentBackend: "opencode",
          defaultModel: "opencode/deepseek-v4-flash-free",
          defaultThinkingLevel: "low",
          reviewModel: null,
          reviewThinkingLevel: null,
          maxConcurrentAgentTurns: 2,
          maxConcurrentWorkItems: 5,
        })
        yield* Deferred.await(secondStarted)
        expect(starts).toBe(2)

        yield* Deferred.succeed(releaseFirst, undefined)
        yield* Fiber.join(first)
        yield* Fiber.join(second)
      }),
    ))

  it("marks the ambient Step Run waiting while blocked on a session slot", () =>
    runTest(
      Effect.gen(function* () {
        const db = yield* DbService
        const sql = yield* SqlClient.SqlClient
        yield* db.updateConfig({
          selectedAgentBackend: "opencode",
          defaultModel: "opencode/deepseek-v4-flash-free",
          defaultThinkingLevel: "low",
          reviewModel: null,
          reviewThinkingLevel: null,
          maxConcurrentAgentTurns: 1,
          maxConcurrentWorkItems: 5,
        })

        const repositoryId = "repo-limiter-wait"
        const workItemId = "wi-01JLIMITERWAIT000000000001"
        const stepRunId = "srun-01JLIMITERWAIT00000000001"
        yield* seedRunningStepRun({
          stepRunId,
          workItemId,
          repositoryId,
        })

        const releaseFirst = yield* Deferred.make<void>()
        const firstStarted = yield* Deferred.make<void>()
        const secondStarted = yield* Deferred.make<void>()
        let starts = 0

        const inner = AgentBackend.of({
          startTurn: () =>
            Effect.gen(function* () {
              starts += 1
              if (starts === 1) {
                yield* Deferred.succeed(firstStarted, undefined)
                yield* Deferred.await(releaseFirst)
              } else {
                yield* Deferred.succeed(secondStarted, undefined)
              }
              return { sessionId: `ses_${starts}`, assistantText: "" }
            }),
          continueTurn: () =>
            Effect.succeed({ sessionId: "ses_x", assistantText: "" }),
          inspect: () =>
            Effect.succeed({
              backend: { id: "opencode" as const, label: "OpenCode" },
              models: [],
            }),
        })
        const limited = yield* limitAgentTurns(inner, db, sql)

        const first = yield* limited
          .startTurn(startInput)
          .pipe(Effect.forkChild)
        yield* Deferred.await(firstStarted)

        const second = yield* limited.startTurn(startInput).pipe(
          Effect.provideService(CurrentStepRun, {
            stepRunId,
            repositoryId,
          }),
          Effect.forkChild,
        )

        yield* Effect.sleep("50 millis")
        expect(starts).toBe(1)

        const waitingRows = (yield* sql.unsafe(
          `SELECT status, reason_code, reason_message, session_wait_started_at,
                  session_wait_ms
           FROM step_run WHERE id = ?`,
          [stepRunId],
        )) as readonly {
          readonly status: string
          readonly reason_code: string | null
          readonly reason_message: string | null
          readonly session_wait_started_at: number | null
          readonly session_wait_ms: number | null
        }[]
        expect(waitingRows[0]).toMatchObject({
          status: "running",
          reason_code: STEP_RUN_REASON.waitingForAgentTurn,
          reason_message: WAITING_FOR_AGENT_TURN_MESSAGE,
          session_wait_ms: 0,
        })
        expect(waitingRows[0]!.session_wait_started_at).toBeTypeOf("number")

        yield* Deferred.succeed(releaseFirst, undefined)
        yield* Deferred.await(secondStarted)
        yield* Fiber.join(first)
        yield* Fiber.join(second)

        const afterRows = (yield* sql.unsafe(
          `SELECT status, reason_code, reason_message, session_wait_started_at,
                  session_wait_ms
           FROM step_run WHERE id = ?`,
          [stepRunId],
        )) as readonly {
          readonly status: string
          readonly reason_code: string | null
          readonly reason_message: string | null
          readonly session_wait_started_at: number | null
          readonly session_wait_ms: number | null
        }[]
        expect(afterRows[0]).toMatchObject({
          status: "running",
          reason_code: null,
          reason_message: null,
          session_wait_started_at: null,
        })
        expect(afterRows[0]!.session_wait_ms ?? 0).toBeGreaterThanOrEqual(40)
        expect(starts).toBe(2)
      }),
    ))

  it("restores a prior Review phase after waiting for a session slot", () =>
    runTest(
      Effect.gen(function* () {
        const db = yield* DbService
        const sql = yield* SqlClient.SqlClient
        yield* db.updateConfig({
          selectedAgentBackend: "opencode",
          defaultModel: "opencode/deepseek-v4-flash-free",
          defaultThinkingLevel: "low",
          reviewModel: null,
          reviewThinkingLevel: null,
          maxConcurrentAgentTurns: 1,
          maxConcurrentWorkItems: 5,
        })

        const repositoryId = "repo-limiter-restore-phase"
        const workItemId = "wi-01JLIMITERREST000000000001"
        const stepRunId = "srun-01JLIMITERREST00000000001"
        yield* seedRunningStepRun({
          stepRunId,
          workItemId,
          repositoryId,
        })
        yield* sql.unsafe(
          `UPDATE step_run
           SET reason_code = ?, reason_message = ?, updated_at = ?
           WHERE id = ?`,
          [
            STEP_RUN_REASON.reviewPreCommit,
            REVIEW_PRE_COMMIT_MESSAGE,
            Date.now(),
            stepRunId,
          ],
        )

        const releaseFirst = yield* Deferred.make<void>()
        const firstStarted = yield* Deferred.make<void>()
        const secondStarted = yield* Deferred.make<void>()
        let starts = 0

        const inner = AgentBackend.of({
          startTurn: () =>
            Effect.gen(function* () {
              starts += 1
              if (starts === 1) {
                yield* Deferred.succeed(firstStarted, undefined)
                yield* Deferred.await(releaseFirst)
              } else {
                yield* Deferred.succeed(secondStarted, undefined)
              }
              return { sessionId: `ses_${starts}`, assistantText: "" }
            }),
          continueTurn: () =>
            Effect.succeed({ sessionId: "ses_x", assistantText: "" }),
          inspect: () =>
            Effect.succeed({
              backend: { id: "opencode" as const, label: "OpenCode" },
              models: [],
            }),
        })
        const limited = yield* limitAgentTurns(inner, db, sql)

        const first = yield* limited
          .startTurn(startInput)
          .pipe(Effect.forkChild)
        yield* Deferred.await(firstStarted)

        const second = yield* limited.startTurn(startInput).pipe(
          Effect.provideService(CurrentStepRun, {
            stepRunId,
            repositoryId,
          }),
          Effect.forkChild,
        )

        yield* Effect.sleep("50 millis")
        expect(starts).toBe(1)

        const waitingRows = (yield* sql.unsafe(
          `SELECT reason_code, reason_message FROM step_run WHERE id = ?`,
          [stepRunId],
        )) as readonly {
          readonly reason_code: string | null
          readonly reason_message: string | null
        }[]
        expect(waitingRows[0]).toEqual({
          reason_code: STEP_RUN_REASON.waitingForAgentTurn,
          reason_message: WAITING_FOR_AGENT_TURN_MESSAGE,
        })

        yield* Deferred.succeed(releaseFirst, undefined)
        yield* Deferred.await(secondStarted)
        yield* Fiber.join(first)
        yield* Fiber.join(second)

        const afterRows = (yield* sql.unsafe(
          `SELECT reason_code, reason_message FROM step_run WHERE id = ?`,
          [stepRunId],
        )) as readonly {
          readonly reason_code: string | null
          readonly reason_message: string | null
        }[]
        expect(afterRows[0]).toEqual({
          reason_code: STEP_RUN_REASON.reviewPreCommit,
          reason_message: REVIEW_PRE_COMMIT_MESSAGE,
        })
        expect(starts).toBe(2)
      }),
    ))

  it("releases the Agent Turn permit when an admitted waiter is interrupted before its turn starts", () =>
    runTest(
      Effect.gen(function* () {
        const db = yield* DbService
        const sql = yield* SqlClient.SqlClient
        yield* db.updateConfig({
          selectedAgentBackend: "opencode",
          defaultModel: "opencode/deepseek-v4-flash-free",
          defaultThinkingLevel: "low",
          reviewModel: null,
          reviewThinkingLevel: null,
          maxConcurrentAgentTurns: 1,
          maxConcurrentWorkItems: 5,
        })

        const repositoryId = "repo-interrupt-after-admit"
        const workItemId = "wi-01JINTERRUPTAFTER00000001"
        const stepRunId = "srun-01JINTERRUPTAFTER0000001"
        yield* seedRunningStepRun({ stepRunId, workItemId, repositoryId })

        // A waiter admitted while it had to wait must clear its
        // waiting_for_agent_turn mark (`clearWaiting`) before it starts
        // running its turn — a real, unavoidable DB round trip. This proxy
        // stalls exactly those two queries (uniquely identified by
        // referencing session_wait_ms, which markWaiting's queries never
        // do) so the test can deterministically land an interrupt inside
        // that window, rather than relying on timing luck.
        const clearWaitingGate = yield* Deferred.make<void>()
        const clearWaitingStarted = yield* Deferred.make<void>()
        const slowSql: Pick<SqlClient.SqlClient, "unsafe"> = {
          unsafe: ((query: string, params?: unknown) =>
            Effect.gen(function* () {
              if (query.includes("session_wait_ms")) {
                yield* Deferred.succeed(clearWaitingStarted, undefined)
                yield* Deferred.await(clearWaitingGate)
              }
              return yield* sql.unsafe(query, params as never)
            })) as SqlClient.SqlClient["unsafe"],
        }

        const releaseFirst = yield* Deferred.make<void>()
        const firstStarted = yield* Deferred.make<void>()
        let starts = 0

        const inner = AgentBackend.of({
          startTurn: () =>
            Effect.gen(function* () {
              starts += 1
              if (starts === 1) {
                yield* Deferred.succeed(firstStarted, undefined)
                yield* Deferred.await(releaseFirst)
              }
              return { sessionId: `ses_${starts}`, assistantText: "" }
            }),
          continueTurn: () =>
            Effect.succeed({ sessionId: "ses_x", assistantText: "" }),
          inspect: () =>
            Effect.succeed({
              backend: { id: "opencode" as const, label: "OpenCode" },
              models: [],
            }),
        })
        const limited = yield* limitAgentTurns(inner, db, slowSql)

        // First turn takes the only permit and holds it.
        const first = yield* limited
          .startTurn(startInput)
          .pipe(Effect.forkChild)
        yield* Deferred.await(firstStarted)

        // Second turn (same Repository) queues behind it and is marked
        // waiting for the Agent Turn.
        const second = yield* limited
          .startTurn(startInput)
          .pipe(
            Effect.provideService(CurrentStepRun, { stepRunId, repositoryId }),
            Effect.forkChild,
          )
        yield* Effect.sleep("30 millis")

        // Freeing the permit admits the second waiter (its Deferred
        // resolves and the scheduler counts it in-flight); it is now
        // stalled inside clearWaiting(), which is exactly the window an
        // operator's Interrupt Work Item can land in for a Step Run that
        // was waiting for an Agent Turn.
        yield* Deferred.succeed(releaseFirst, undefined)
        yield* Fiber.join(first)
        yield* Deferred.await(clearWaitingStarted)
        yield* Fiber.interrupt(second)
        yield* Deferred.succeed(clearWaitingGate, undefined)

        // A third turn on the same Repository must still be admissible:
        // the interrupted second waiter's permit must have been released
        // rather than leaked.
        const thirdStarted = yield* Deferred.make<void>()
        const third = yield* limited.startTurn(startInput).pipe(
          Effect.tap(() => Deferred.succeed(thirdStarted, undefined)),
          Effect.forkChild,
        )
        const admitted = yield* Effect.race(
          Deferred.await(thirdStarted).pipe(Effect.as(true)),
          Effect.sleep("1 second").pipe(Effect.as(false)),
        )
        expect(admitted).toBe(true)
        yield* Fiber.interrupt(third)
      }),
    ))

  describe("repository-aware fair-share admission", () => {
    /**
     * Shared fixture for the fair-share/guaranteed-minimum tests: a fake
     * Agent Backend that records each admitted call (by its `model` field,
     * used here purely as a per-call identifier) into `order` and blocks
     * until the test resolves that call's `Deferred` gate, plus a `runTurn`
     * helper that starts a gated call under a given Repository's ambient
     * `CurrentStepRun` context.
     */
    const makeFairShareFixture = (
      db: Pick<DbServiceShape, "getConfig" | "notifyWorkItemsChanged">,
      sql: SqlClient.SqlClient,
    ) =>
      Effect.gen(function* () {
        const order: string[] = []
        const gates = new Map<string, Deferred.Deferred<void>>()

        const inner = AgentBackend.of({
          startTurn: (input) =>
            Effect.gen(function* () {
              order.push(input.model)
              const gate = gates.get(input.model)
              if (gate) {
                yield* Deferred.await(gate)
              }
              return { sessionId: `ses_${input.model}`, assistantText: "" }
            }),
          continueTurn: () =>
            Effect.succeed({ sessionId: "ses_x", assistantText: "" }),
          inspect: () =>
            Effect.succeed({
              backend: { id: "opencode" as const, label: "OpenCode" },
              models: [],
            }),
        })
        const limited = yield* limitAgentTurns(inner, db, sql)

        const runTurn = (repositoryId: string, turnId: string) =>
          Effect.gen(function* () {
            const gate = yield* Deferred.make<void>()
            gates.set(turnId, gate)
            const fiber = yield* limited
              .startTurn({ ...startInput, model: turnId })
              .pipe(
                Effect.provideService(CurrentStepRun, {
                  stepRunId: `srun-${turnId}`,
                  repositoryId,
                }),
                Effect.forkChild,
              )
            return { fiber, gate }
          })

        const release = (turn: {
          readonly fiber: Fiber.Fiber<unknown, unknown>
          readonly gate: Deferred.Deferred<void>
        }) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(turn.gate, undefined)
            yield* Fiber.join(turn.fiber)
          })

        return { order, runTurn, release }
      })

    it("behaves like a single shared limiter when only one Repository has demand", () =>
      runTest(
        Effect.gen(function* () {
          const db = yield* DbService
          const sql = yield* SqlClient.SqlClient
          yield* db.updateConfig({
            selectedAgentBackend: "opencode",
            defaultModel: "opencode/deepseek-v4-flash-free",
            defaultThinkingLevel: "low",
            reviewModel: null,
            reviewThinkingLevel: null,
            maxConcurrentAgentTurns: 2,
            maxConcurrentWorkItems: 5,
          })
          const { order, runTurn, release } = yield* makeFairShareFixture(
            db,
            sql,
          )
          const repositoryId = "repo-single"

          const t1 = yield* runTurn(repositoryId, "t1")
          const t2 = yield* runTurn(repositoryId, "t2")
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["t1", "t2"])

          const t3 = yield* runTurn(repositoryId, "t3")
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["t1", "t2"])

          yield* release(t1)
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["t1", "t2", "t3"])

          yield* release(t2)
          yield* release(t3)
        }),
      ))

    it("grants a freed Agent Turn to whichever pending Repository was least recently granted one", () =>
      runTest(
        Effect.gen(function* () {
          const db = yield* DbService
          const sql = yield* SqlClient.SqlClient
          yield* db.updateConfig({
            selectedAgentBackend: "opencode",
            defaultModel: "opencode/deepseek-v4-flash-free",
            defaultThinkingLevel: "low",
            reviewModel: null,
            reviewThinkingLevel: null,
            maxConcurrentAgentTurns: 1,
            maxConcurrentWorkItems: 5,
          })
          const { order, runTurn, release } = yield* makeFairShareFixture(
            db,
            sql,
          )
          const repoA = "repo-fair-a"
          const repoB = "repo-fair-b"

          const a1 = yield* runTurn(repoA, "a1")
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["a1"])

          const a2 = yield* runTurn(repoA, "a2")
          const b1 = yield* runTurn(repoB, "b1")
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["a1"])

          // Repo B has never been serviced; it is admitted ahead of Repo A's
          // second waiter even though A asked to run again first.
          yield* release(a1)
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["a1", "b1"])

          yield* release(b1)
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["a1", "b1", "a2"])

          yield* release(a2)
        }),
      ))

    it("does not let a high-volume Repository's demand starve a low-volume Repository", () =>
      runTest(
        Effect.gen(function* () {
          const db = yield* DbService
          const sql = yield* SqlClient.SqlClient
          yield* db.updateConfig({
            selectedAgentBackend: "opencode",
            defaultModel: "opencode/deepseek-v4-flash-free",
            defaultThinkingLevel: "low",
            reviewModel: null,
            reviewThinkingLevel: null,
            maxConcurrentAgentTurns: 1,
            maxConcurrentWorkItems: 5,
          })
          const { order, runTurn, release } = yield* makeFairShareFixture(
            db,
            sql,
          )
          const repoHigh = "repo-high-volume"
          const repoLow = "repo-low-volume"

          const h1 = yield* runTurn(repoHigh, "h1")
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["h1"])

          // High keeps three requests queued; Low has just one.
          const h2 = yield* runTurn(repoHigh, "h2")
          const h3 = yield* runTurn(repoHigh, "h3")
          const l1 = yield* runTurn(repoLow, "l1")
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["h1"])

          yield* release(h1)
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["h1", "l1"])

          yield* release(l1)
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["h1", "l1", "h2"])

          yield* release(h2)
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["h1", "l1", "h2", "h3"])

          yield* release(h3)
        }),
      ))

    it("honors a Repository's guaranteed-minimum ahead of fair-share, even against heavier demand from another Repository", () =>
      runTest(
        Effect.gen(function* () {
          const db = yield* DbService
          const sql = yield* SqlClient.SqlClient
          yield* db.updateConfig({
            selectedAgentBackend: "opencode",
            defaultModel: "opencode/deepseek-v4-flash-free",
            defaultThinkingLevel: "low",
            reviewModel: null,
            reviewThinkingLevel: null,
            maxConcurrentAgentTurns: 2,
            maxConcurrentWorkItems: 5,
          })
          const repoGuaranteed = "repo-guaranteed-a"
          const repoOrdinary = "repo-ordinary-a"
          yield* seedRepositoryGuarantee({
            repositoryId: repoGuaranteed,
            guaranteedMin: 1,
          })
          const { order, runTurn, release } = yield* makeFairShareFixture(
            db,
            sql,
          )

          const o1 = yield* runTurn(repoOrdinary, "o1")
          const o2 = yield* runTurn(repoOrdinary, "o2")
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["o1", "o2"])

          const o3 = yield* runTurn(repoOrdinary, "o3")
          const g1 = yield* runTurn(repoGuaranteed, "g1")
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["o1", "o2"])

          // The guaranteed Repository's unmet floor wins the freed permit
          // over Ordinary's own third (older) waiter.
          yield* release(o1)
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["o1", "o2", "g1"])

          yield* release(o2)
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["o1", "o2", "g1", "o3"])

          yield* release(g1)
          yield* release(o3)
        }),
      ))

    it("does not let an idle guaranteed-minimum withhold capacity, and reclaims it once demand returns", () =>
      runTest(
        Effect.gen(function* () {
          const db = yield* DbService
          const sql = yield* SqlClient.SqlClient
          yield* db.updateConfig({
            selectedAgentBackend: "opencode",
            defaultModel: "opencode/deepseek-v4-flash-free",
            defaultThinkingLevel: "low",
            reviewModel: null,
            reviewThinkingLevel: null,
            maxConcurrentAgentTurns: 1,
            maxConcurrentWorkItems: 5,
          })
          const repoGuaranteed = "repo-guaranteed-idle"
          const repoOrdinary = "repo-ordinary-idle"
          yield* seedRepositoryGuarantee({
            repositoryId: repoGuaranteed,
            guaranteedMin: 1,
          })
          const { order, runTurn, release } = yield* makeFairShareFixture(
            db,
            sql,
          )

          // repoGuaranteed has a configured floor but no pending demand yet:
          // Ordinary is admitted immediately, unblocked by the idle guarantee.
          const o1 = yield* runTurn(repoOrdinary, "o1")
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["o1"])

          const o2 = yield* runTurn(repoOrdinary, "o2")
          const g1 = yield* runTurn(repoGuaranteed, "g1")
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["o1"])

          // repoGuaranteed now has demand again and reclaims its floor ahead
          // of Ordinary's next waiter.
          yield* release(o1)
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["o1", "g1"])

          yield* release(g1)
          yield* Effect.sleep("30 millis")
          expect(order).toEqual(["o1", "g1", "o2"])

          yield* release(o2)
        }),
      ))
  })
})
