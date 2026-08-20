import { Config, Duration, Effect, Layer, Redacted, Schema } from "effect"
import { extractErrorCode } from "@ready-for-agent/github-service"
import {
  AzureDevOpsService,
  type AzureDevOpsServiceError,
  type AzureDevOpsServiceShape,
} from "./azure-devops-service.js"
import {
  AzureDevOpsNotImplementedError,
  AzureDevOpsProjectUnavailableError,
  AzureDevOpsRequestError,
} from "./errors.js"
import {
  AZURE_DEVOPS_FORGE_HOST,
  AZURE_DEVOPS_PAT_ENV_VAR,
  type AzureDevOpsProjectIdentity,
  type AzureDevOpsReadyLabeledIssue,
  type AzureDevOpsRepository,
  splitAzureDevOpsProjectPath,
} from "./types.js"

const REQUEST_TIMEOUT = Duration.seconds(30)
/** Azure DevOps REST API version pinned for stable response shapes. */
const API_VERSION = "7.1"

type AzureDevOpsFetch = typeof fetch

class AzureDevOpsHttpError extends Error {
  constructor(
    readonly statusCode: number,
    message: string,
  ) {
    super(message)
  }
}

const RequiredString = Schema.String.pipe(
  Schema.check(
    Schema.makeFilter((value: string) =>
      value.trim() === "" ? "Expected a non-empty string" : undefined,
    ),
  ),
)

const ProjectSchema = Schema.Struct({
  id: Schema.optional(RequiredString),
  name: RequiredString,
  url: Schema.optional(Schema.NullOr(Schema.String)),
})

const ConnectionDataSchema = Schema.Struct({
  authenticatedUser: Schema.Struct({
    id: Schema.optional(Schema.String),
    providerDisplayName: Schema.optional(Schema.NullOr(Schema.String)),
  }),
})

const PositiveInt = Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0)))

const PullRequestSchema = Schema.Struct({
  pullRequestId: PositiveInt,
  status: Schema.optional(Schema.String),
  isDraft: Schema.optional(Schema.Boolean),
  title: Schema.optional(Schema.NullOr(Schema.String)),
  description: Schema.optional(Schema.NullOr(Schema.String)),
  sourceRefName: Schema.optional(Schema.NullOr(Schema.String)),
})
type AzureDevOpsPullRequest = typeof PullRequestSchema.Type

const PullRequestListSchema = Schema.Struct({
  value: Schema.Array(PullRequestSchema),
})

const RepositoryMetaSchema = Schema.Struct({
  defaultBranch: Schema.optional(Schema.NullOr(Schema.String)),
})

/** Semaphore for Ready Work Items, mirrors GitHub/GitLab's `ready-for-agent` label. */
const READY_FOR_AGENT_TAG = "ready-for-agent"

/**
 * `System.LinkTypes.Dependency-Reverse` is the "Predecessor" end of a
 * Successor-Predecessor link: the linked work item must complete before this
 * one, i.e. it blocks this one. The forward end
 * (`System.LinkTypes.Dependency-Forward`, "Successor") is the opposite
 * direction and is never surfaced as `blockedBy`.
 */
const PREDECESSOR_LINK_TYPE = "System.LinkTypes.Dependency-Reverse"

/**
 * Work item states treated as closed for Ready Issue / blocking purposes.
 * Azure DevOps has no fixed closed-state name across process templates
 * (Agile: Closed, Scrum: Done/Removed, CMMI: Closed), so this is a
 * heuristic over the common default templates rather than a per-project
 * `workitemtypes/{type}/states` lookup (out of scope for this ticket).
 */
const CLOSED_STATE_NAMES = new Set([
  "closed",
  "done",
  "removed",
  "resolved",
  "completed",
])

const isOpenState = (state: string | undefined): boolean =>
  state === undefined
    ? true
    : !CLOSED_STATE_NAMES.has(state.trim().toLowerCase())

const toIssueState = (state: string | undefined): "OPEN" | "CLOSED" =>
  isOpenState(state) ? "OPEN" : "CLOSED"

const IdentityRefSchema = Schema.Struct({
  displayName: Schema.optional(Schema.NullOr(Schema.String)),
  uniqueName: Schema.optional(Schema.NullOr(Schema.String)),
})

const identityDisplayName = (
  identity: typeof IdentityRefSchema.Type | undefined,
): string | null => {
  if (identity === undefined) return null
  const displayName = identity.displayName?.trim()
  if (displayName !== undefined && displayName !== "") return displayName
  const uniqueName = identity.uniqueName?.trim()
  if (uniqueName !== undefined && uniqueName !== "") return uniqueName
  return null
}

const WorkItemRelationSchema = Schema.Struct({
  rel: RequiredString,
  url: RequiredString,
})

const WorkItemFieldsSchema = Schema.Struct({
  "System.Title": Schema.optional(Schema.String),
  "System.Description": Schema.optional(Schema.NullOr(Schema.String)),
  "System.State": Schema.optional(Schema.String),
  "System.CreatedDate": Schema.optional(Schema.String),
  "System.CreatedBy": Schema.optional(IdentityRefSchema),
})

const WorkItemSchema = Schema.Struct({
  id: Schema.Int,
  fields: WorkItemFieldsSchema,
  relations: Schema.optional(Schema.Array(WorkItemRelationSchema)),
})
type AzureDevOpsWorkItem = typeof WorkItemSchema.Type

const WorkItemBatchSchema = Schema.Struct({
  value: Schema.Array(WorkItemSchema),
})

const WiqlWorkItemRefSchema = Schema.Struct({ id: Schema.Int })
const WiqlResultSchema = Schema.Struct({
  workItems: Schema.optional(Schema.Array(WiqlWorkItemRefSchema)),
})

/** Max work item ids per `_apis/wit/workitems` batch GET (API limit is 200). */
const WORK_ITEM_BATCH_SIZE = 200

const chunk = <T>(items: readonly T[], size: number): T[][] => {
  const chunks: T[][] = []
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size))
  }
  return chunks
}

const readyForAgentWiqlQuery = (): string =>
  `SELECT [System.Id] FROM WorkItems WHERE [System.TeamProject] = @project AND [System.Tags] CONTAINS '${READY_FOR_AGENT_TAG}' ORDER BY [System.Id]`

const workItemUrl = (
  identity: AzureDevOpsProjectIdentity,
  id: number,
): string =>
  `https://${AZURE_DEVOPS_FORGE_HOST}/${encodeURIComponent(identity.organization)}/${encodeURIComponent(identity.project)}/_workitems/edit/${id}`

/** Extract the numeric work item id from a relation's `.../workItems/{id}` URL. */
const workItemIdFromRelationUrl = (url: string): number | null => {
  const match = /\/workItems\/(\d+)$/i.exec(url)
  if (match === null) return null
  const id = Number(match[1])
  return Number.isSafeInteger(id) ? id : null
}

const toReadyLabeledIssue = (
  identity: AzureDevOpsProjectIdentity,
  item: AzureDevOpsWorkItem,
  stateById: ReadonlyMap<number, string | undefined>,
): AzureDevOpsReadyLabeledIssue => {
  const fields = item.fields
  const createdAt = new Date(fields["System.CreatedDate"] ?? "")
  if (Number.isNaN(createdAt.getTime())) {
    throw new Error(
      `Invalid Azure DevOps work item creation time: ${String(fields["System.CreatedDate"])}`,
    )
  }
  const blockerIds = new Set(
    (item.relations ?? [])
      .filter((relation) => relation.rel === PREDECESSOR_LINK_TYPE)
      .map((relation) => workItemIdFromRelationUrl(relation.url))
      .filter((id): id is number => id !== null),
  )
  const blockedBy = [...blockerIds]
    .filter((id) => isOpenState(stateById.get(id)))
    .sort((left, right) => left - right)
    .map((id) => ({ number: id, url: workItemUrl(identity, id) }))
  return {
    number: item.id,
    title: fields["System.Title"] ?? "",
    body: fields["System.Description"] ?? "",
    url: workItemUrl(identity, item.id),
    createdAt,
    state: toIssueState(fields["System.State"]),
    author: identityDisplayName(fields["System.CreatedBy"]),
    parent: null,
    parentPosition: null,
    hasChildren: false,
    hierarchySupported: false,
    blockedBy,
    closingPullRequests: [],
  } satisfies AzureDevOpsReadyLabeledIssue
}

const decode = <S extends { readonly Type: unknown }>(
  schema: S & Parameters<typeof Schema.decodeUnknownSync>[0],
  value: unknown,
): S["Type"] => Schema.decodeUnknownSync(schema)(value)

const organizationApiBase = (organization: string): string =>
  `https://${AZURE_DEVOPS_FORGE_HOST}/${encodeURIComponent(organization)}`

const REFS_HEADS_PREFIX = "refs/heads/"

/** Azure DevOps requires fully-qualified refs on PR create/list bodies. */
const toFullRefName = (branch: string): string =>
  branch.startsWith(REFS_HEADS_PREFIX)
    ? branch
    : `${REFS_HEADS_PREFIX}${branch}`

/** Inverse of {@link toFullRefName}; strips the `refs/heads/` prefix if present. */
const branchFromRefName = (ref: string): string =>
  ref.startsWith(REFS_HEADS_PREFIX) ? ref.slice(REFS_HEADS_PREFIX.length) : ref

/**
 * Azure DevOps Git Pull Requests are scoped by repository, not project. This
 * codebase assumes one Git repository per project sharing the project's name
 * (see {@link splitAzureDevOpsProjectPath} doc), so the repositoryId path
 * segment is always the project name.
 */
const pullRequestsPath = (
  identity: AzureDevOpsProjectIdentity,
  suffix = "",
): string =>
  `/${encodeURIComponent(identity.project)}/_apis/git/repositories/${encodeURIComponent(identity.project)}/pullrequests${suffix}`

const repositoryMetaPath = (identity: AzureDevOpsProjectIdentity): string =>
  `/${encodeURIComponent(identity.project)}/_apis/git/repositories/${encodeURIComponent(identity.project)}`

const requestError = (
  message: string,
  cause: unknown,
): AzureDevOpsRequestError => {
  const code = extractErrorCode(cause)
  return new AzureDevOpsRequestError({
    message,
    cause,
    ...(code !== undefined ? { code } : {}),
    ...(cause instanceof AzureDevOpsHttpError
      ? { statusCode: cause.statusCode }
      : {}),
  })
}

const invalidProjectPath = (
  repository: AzureDevOpsRepository,
): AzureDevOpsRequestError =>
  new AzureDevOpsRequestError({
    message: `Invalid Azure DevOps Project Path (expected <organization>/<project>): ${repository.projectPath}`,
  })

const notImplemented = (
  method: string,
): Effect.Effect<never, AzureDevOpsNotImplementedError> =>
  Effect.fail(new AzureDevOpsNotImplementedError({ method }))

export const makeAzureDevOpsService = (options: {
  readonly token?: string
  readonly fetch?: AzureDevOpsFetch
}): AzureDevOpsServiceShape => {
  const fetchImpl = options.fetch ?? fetch
  const headers: Record<string, string> =
    options.token === undefined || options.token.trim() === ""
      ? { Accept: "application/json" }
      : {
          Accept: "application/json",
          // Azure DevOps PAT auth: HTTP Basic with an empty username.
          Authorization: `Basic ${Buffer.from(`:${options.token}`).toString("base64")}`,
        }

  const requestUnknown = (
    organization: string,
    path: string,
    message: string,
    init: { readonly method?: string; readonly body?: unknown } = {},
  ): Effect.Effect<unknown, AzureDevOpsRequestError> =>
    Effect.tryPromise({
      try: async () => {
        const separator = path.includes("?") ? "&" : "?"
        const response = await fetchImpl(
          `${organizationApiBase(organization)}${path}${separator}api-version=${API_VERSION}`,
          {
            method: init.method ?? "GET",
            headers: {
              ...headers,
              ...(init.body === undefined
                ? {}
                : { "Content-Type": "application/json" }),
            },
            body:
              init.body === undefined ? undefined : JSON.stringify(init.body),
          },
        )
        if (!response.ok) {
          throw new AzureDevOpsHttpError(
            response.status,
            `${message}: Azure DevOps returned HTTP ${response.status}`,
          )
        }
        return await response.json()
      },
      catch: (cause) => requestError(message, cause),
    }).pipe(
      Effect.timeout(REQUEST_TIMEOUT),
      Effect.catchTag("TimeoutError", (cause) =>
        Effect.fail(requestError(`${message} timed out`, cause)),
      ),
    )

  /**
   * Batch-fetch work items with all fields and relations
   * (`$expand=all`) for the given ids, chunked to the API's per-request
   * limit. `errorPolicy=Omit` so a deleted/inaccessible id (e.g. a stale
   * Predecessor link target) drops that item instead of failing the whole
   * batch.
   */
  const fetchWorkItemsWithRelations = (
    organization: string,
    ids: readonly number[],
  ): Effect.Effect<readonly AzureDevOpsWorkItem[], AzureDevOpsRequestError> =>
    Effect.gen(function* () {
      const results: AzureDevOpsWorkItem[] = []
      for (const batch of chunk(ids, WORK_ITEM_BATCH_SIZE)) {
        const value = yield* requestUnknown(
          organization,
          `/_apis/wit/workitems?ids=${batch.join(",")}&$expand=all&errorPolicy=Omit`,
          `Failed to load Azure DevOps work items ${batch.join(",")}`,
        )
        const decoded = yield* Effect.try({
          try: () => decode(WorkItemBatchSchema, value),
          catch: (cause) =>
            requestError("Azure DevOps returned invalid work item data", cause),
        })
        results.push(...decoded.value)
      }
      return results
    })

  const unavailableOn404 = <A>(
    repository: AzureDevOpsRepository,
    effect: Effect.Effect<A, AzureDevOpsRequestError>,
  ): Effect.Effect<
    A,
    AzureDevOpsProjectUnavailableError | AzureDevOpsRequestError
  > =>
    effect.pipe(
      Effect.catch(
        (
          error,
        ): Effect.Effect<
          never,
          AzureDevOpsProjectUnavailableError | AzureDevOpsRequestError
        > =>
          error.statusCode === 404
            ? Effect.fail(new AzureDevOpsProjectUnavailableError(repository))
            : Effect.fail(error),
      ),
    )

  const decodePullRequest = (
    value: unknown,
    message: string,
  ): Effect.Effect<AzureDevOpsPullRequest, AzureDevOpsRequestError> =>
    Effect.try({
      try: () => decode(PullRequestSchema, value),
      catch: (cause) => requestError(message, cause),
    })

  /**
   * Active pull requests for the exact source branch, newest first per
   * Azure DevOps default ordering. Empty when none exist (does not fail).
   */
  const listPullRequestsForBranch = (
    repository: AzureDevOpsRepository,
    identity: AzureDevOpsProjectIdentity,
    headRefName: string,
  ): Effect.Effect<
    readonly AzureDevOpsPullRequest[],
    AzureDevOpsServiceError
  > =>
    unavailableOn404(
      repository,
      requestUnknown(
        identity.organization,
        `${pullRequestsPath(identity)}?searchCriteria.status=active&searchCriteria.sourceRefName=${encodeURIComponent(toFullRefName(headRefName))}`,
        `Failed to list pull requests for ${repository.projectPath}:${headRefName}`,
      ).pipe(
        Effect.flatMap((value) =>
          Effect.try({
            try: () => decode(PullRequestListSchema, value).value,
            catch: (cause) =>
              requestError(
                `Azure DevOps returned invalid pull request data for ${repository.projectPath}:${headRefName}`,
                cause,
              ),
          }),
        ),
      ),
    )

  /**
   * Resolve the base branch for a new draft pull request: an explicit
   * `baseRefName` when provided, otherwise the Git repository's default
   * branch (Azure DevOps project metadata has no default-branch field —
   * that lives on the repository resource, unlike GitLab's project).
   */
  const resolveBaseRefName = (
    repository: AzureDevOpsRepository,
    identity: AzureDevOpsProjectIdentity,
    explicitBaseRefName: string | undefined,
  ): Effect.Effect<string, AzureDevOpsServiceError> =>
    Effect.gen(function* () {
      const trimmed = explicitBaseRefName?.trim() ?? ""
      if (trimmed !== "") return trimmed
      const meta = yield* unavailableOn404(
        repository,
        requestUnknown(
          identity.organization,
          repositoryMetaPath(identity),
          `Failed to resolve the default branch for ${repository.projectPath}`,
        ).pipe(
          Effect.map((value) => decode(RepositoryMetaSchema, value)),
          Effect.mapError((error) =>
            error instanceof AzureDevOpsRequestError
              ? error
              : requestError(
                  `Azure DevOps returned invalid repository metadata for ${repository.projectPath}`,
                  error,
                ),
          ),
        ),
      )
      const defaultBranch = meta.defaultBranch?.trim() ?? ""
      if (defaultBranch === "") {
        return yield* new AzureDevOpsRequestError({
          message: `Repository ${repository.projectPath} has no default base branch`,
        })
      }
      return branchFromRefName(defaultBranch)
    })

  return {
    verifyProject: Effect.fn("AzureDevOpsService.verifyProject")(
      function* (repository) {
        const identity = splitAzureDevOpsProjectPath(repository.projectPath)
        if (identity === null) {
          return yield* invalidProjectPath(repository)
        }
        const value = yield* unavailableOn404(
          repository,
          requestUnknown(
            identity.organization,
            `/_apis/projects/${encodeURIComponent(identity.project)}`,
            `Failed to verify Azure DevOps project ${repository.projectPath}`,
          ),
        )
        const project = yield* Effect.try({
          try: () => decode(ProjectSchema, value),
          catch: (cause) =>
            requestError("Azure DevOps returned an invalid project", cause),
        })
        return {
          forge: repository.forge,
          forgeHost: repository.forgeHost,
          projectPath: `${identity.organization}/${project.name}`,
        } satisfies AzureDevOpsRepository
      },
    ),
    getAuthenticatedUserLogin: Effect.fn(
      "AzureDevOpsService.getAuthenticatedUserLogin",
    )(function* (repository) {
      const identity = splitAzureDevOpsProjectPath(repository.projectPath)
      if (identity === null) {
        return yield* invalidProjectPath(repository)
      }
      const value = yield* unavailableOn404(
        repository,
        requestUnknown(
          identity.organization,
          "/_apis/connectionData",
          `Failed to resolve authenticated Azure DevOps user for ${identity.organization}`,
        ),
      )
      const connectionData = yield* Effect.try({
        try: () => decode(ConnectionDataSchema, value),
        catch: (cause) =>
          requestError("Azure DevOps returned invalid connection data", cause),
      })
      const displayName = connectionData.authenticatedUser.providerDisplayName
      if (typeof displayName === "string" && displayName.trim() !== "") {
        return displayName
      }
      const id = connectionData.authenticatedUser.id
      if (typeof id === "string" && id.trim() !== "") {
        return id
      }
      return yield* requestError(
        "Azure DevOps did not return an authenticated user identity",
        connectionData,
      )
    }),
    listReadyIssues: Effect.fn("AzureDevOpsService.listReadyIssues")(
      function* (repository) {
        const identity = splitAzureDevOpsProjectPath(repository.projectPath)
        if (identity === null) {
          return yield* invalidProjectPath(repository)
        }
        const wiqlValue = yield* unavailableOn404(
          repository,
          requestUnknown(
            identity.organization,
            `/${encodeURIComponent(identity.project)}/_apis/wit/wiql`,
            `Failed to list Ready Issues for ${repository.projectPath}`,
            { method: "POST", body: { query: readyForAgentWiqlQuery() } },
          ),
        )
        const wiql = yield* Effect.try({
          try: () => decode(WiqlResultSchema, wiqlValue),
          catch: (cause) =>
            requestError("Azure DevOps returned an invalid WIQL result", cause),
        })
        const ids = (wiql.workItems ?? []).map((item) => item.id)
        if (ids.length === 0) return []

        const items = yield* unavailableOn404(
          repository,
          fetchWorkItemsWithRelations(identity.organization, ids),
        )
        const knownIds = new Set(items.map((item) => item.id))
        const blockerIds = new Set<number>()
        for (const item of items) {
          for (const relation of item.relations ?? []) {
            if (relation.rel !== PREDECESSOR_LINK_TYPE) continue
            const blockerId = workItemIdFromRelationUrl(relation.url)
            if (blockerId !== null) blockerIds.add(blockerId)
          }
        }
        const missingBlockerIds = [...blockerIds].filter(
          (id) => !knownIds.has(id),
        )
        const blockerItems =
          missingBlockerIds.length === 0
            ? []
            : yield* unavailableOn404(
                repository,
                fetchWorkItemsWithRelations(
                  identity.organization,
                  missingBlockerIds,
                ),
              )
        const stateById = new Map<number, string | undefined>()
        for (const item of [...items, ...blockerItems]) {
          stateById.set(item.id, item.fields["System.State"])
        }

        return yield* Effect.try({
          try: () =>
            items
              .map((item) => toReadyLabeledIssue(identity, item, stateById))
              .sort((left, right) => left.number - right.number),
          catch: (cause) =>
            requestError("Azure DevOps returned invalid work item data", cause),
        })
      },
    ),
    hasCredentials: () => Effect.succeed(options.token !== undefined),
    hasAmbientCredentials: () => Effect.succeed(options.token !== undefined),
    getOpenPullRequestNumber: Effect.fn(
      "AzureDevOpsService.getOpenPullRequestNumber",
    )(function* (repository, headRefName) {
      const identity = splitAzureDevOpsProjectPath(repository.projectPath)
      if (identity === null) {
        return yield* invalidProjectPath(repository)
      }
      const list = yield* listPullRequestsForBranch(
        repository,
        identity,
        headRefName,
      )
      const first = list[0]
      if (first === undefined) {
        return yield* new AzureDevOpsRequestError({
          message: `No open pull request found for ${repository.projectPath}:${headRefName}`,
        })
      }
      return first.pullRequestId
    }),
    findOpenPullRequestNumber: Effect.fn(
      "AzureDevOpsService.findOpenPullRequestNumber",
    )(function* (repository, headRefName) {
      const identity = splitAzureDevOpsProjectPath(repository.projectPath)
      if (identity === null) {
        return yield* invalidProjectPath(repository)
      }
      const list = yield* listPullRequestsForBranch(
        repository,
        identity,
        headRefName,
      )
      const first = list[0]
      return first === undefined ? null : first.pullRequestId
    }),
    createDraftPullRequest: Effect.fn(
      "AzureDevOpsService.createDraftPullRequest",
    )(function* (repository, input) {
      const identity = splitAzureDevOpsProjectPath(repository.projectPath)
      if (identity === null) {
        return yield* invalidProjectPath(repository)
      }
      const baseRefName = yield* resolveBaseRefName(
        repository,
        identity,
        input.baseRefName,
      )
      const created = yield* unavailableOn404(
        repository,
        requestUnknown(
          identity.organization,
          pullRequestsPath(identity),
          `Failed to create draft pull request for ${repository.projectPath}:${input.headRefName}`,
          {
            method: "POST",
            body: {
              sourceRefName: toFullRefName(input.headRefName),
              targetRefName: toFullRefName(baseRefName),
              title: input.title,
              description: input.body,
              isDraft: true,
            },
          },
        ).pipe(
          Effect.flatMap((value) =>
            decodePullRequest(
              value,
              `Azure DevOps returned an invalid pull request after create for ${repository.projectPath}:${input.headRefName}`,
            ),
          ),
        ),
      )
      if (created.isDraft !== true) {
        return yield* new AzureDevOpsRequestError({
          message: `Azure DevOps did not create a draft pull request for ${repository.projectPath}:${input.headRefName}`,
        })
      }
      return created.pullRequestId
    }),
    updateOpenDraftPullRequestCopy: Effect.fn(
      "AzureDevOpsService.updateOpenDraftPullRequestCopy",
    )(function* (repository, headRefName, input) {
      const identity = splitAzureDevOpsProjectPath(repository.projectPath)
      if (identity === null) {
        return yield* invalidProjectPath(repository)
      }
      const list = yield* listPullRequestsForBranch(
        repository,
        identity,
        headRefName,
      )
      const details = list[0]
      if (details === undefined) {
        return null
      }
      // Non-draft open PRs (ready for review / human-edited): do not overwrite.
      if (details.isDraft !== true) {
        return details.pullRequestId
      }
      const currentTitle = details.title ?? ""
      const currentBody = details.description ?? ""
      if (currentTitle === input.title && currentBody === input.body) {
        return details.pullRequestId
      }
      // Copy update is best-effort: open draft identity remains valid.
      yield* requestUnknown(
        identity.organization,
        pullRequestsPath(identity, `/${details.pullRequestId}`),
        `Failed to update draft pull request ${details.pullRequestId} for ${repository.projectPath}`,
        {
          method: "PATCH",
          body: { title: input.title, description: input.body },
        },
      ).pipe(Effect.asVoid, Effect.ignore)
      return details.pullRequestId
    }),
    countOpenNonDraftPullRequests: () =>
      notImplemented("countOpenNonDraftPullRequests"),
    getPullRequestCheckStatus: () =>
      notImplemented("getPullRequestCheckStatus"),
    getPrStatusCheckDiagnostics: () =>
      notImplemented("getPrStatusCheckDiagnostics"),
    markPullRequestReadyForReview: Effect.fn(
      "AzureDevOpsService.markPullRequestReadyForReview",
    )(function* (repository, headRefName) {
      const identity = splitAzureDevOpsProjectPath(repository.projectPath)
      if (identity === null) {
        return yield* invalidProjectPath(repository)
      }
      const list = yield* listPullRequestsForBranch(
        repository,
        identity,
        headRefName,
      )
      const listed = list[0]
      if (listed === undefined) {
        return yield* new AzureDevOpsRequestError({
          message: `No open pull request found for ${repository.projectPath}:${headRefName}`,
        })
      }
      // Already ready for review: idempotent success.
      if (listed.isDraft !== true) {
        return
      }
      const updated = yield* requestUnknown(
        identity.organization,
        pullRequestsPath(identity, `/${listed.pullRequestId}`),
        `Failed to mark pull request ${listed.pullRequestId} ready for review for ${repository.projectPath}`,
        {
          method: "PATCH",
          body: { isDraft: false },
        },
      ).pipe(
        Effect.flatMap((value) =>
          decodePullRequest(
            value,
            `Azure DevOps returned an invalid pull request after mark ready for ${repository.projectPath}:${headRefName}`,
          ),
        ),
      )
      if (updated.isDraft === true) {
        return yield* new AzureDevOpsRequestError({
          message: `Azure DevOps did not clear the draft flag for pull request ${listed.pullRequestId} for ${repository.projectPath}:${headRefName}`,
        })
      }
    }),
    getPullRequestLifecycleStatus: () =>
      notImplemented("getPullRequestLifecycleStatus"),
    mergePullRequest: () => notImplemented("mergePullRequest"),
    ensureIssueCompletedWithSummary: () =>
      notImplemented("ensureIssueCompletedWithSummary"),
    closeOpenPullRequestsForBranch: () =>
      notImplemented("closeOpenPullRequestsForBranch"),
    deleteBranch: () => notImplemented("deleteBranch"),
  } satisfies AzureDevOpsServiceShape
}

export const makeAzureDevOpsServiceFromToken = (
  token: string,
  fetchImpl: AzureDevOpsFetch = fetch,
): AzureDevOpsServiceShape =>
  makeAzureDevOpsService({ token, fetch: fetchImpl })

/**
 * Helper-process Live layer: reads `AZURE_DEVOPS_EXT_PAT` from the
 * environment. Keymaxxer injects the named vault secret aliased as
 * `AZURE_DEVOPS_EXT_PAT` so the raw token never enters the Harness process.
 */
export const AzureDevOpsServiceLive = Layer.effect(
  AzureDevOpsService,
  Effect.gen(function* () {
    const token = yield* Config.redacted(AZURE_DEVOPS_PAT_ENV_VAR)
    return makeAzureDevOpsServiceFromToken(Redacted.value(token))
  }),
)
