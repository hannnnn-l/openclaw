import { sessionChanges } from "../../sessions/session-row-changes.js";
import {
  openOpenClawAgentDatabase,
  runOpenClawAgentWriteTransaction,
} from "../../state/openclaw-agent-db.js";
import type { SessionAccessScope } from "./session-accessor.sqlite-contract.js";
import { publishSessionEntryCacheCategoryUpdate } from "./session-accessor.sqlite-entry-cache.js";
import { resolveSqliteScope, toDatabaseOptions } from "./session-accessor.sqlite-scope.js";
import {
  applySessionGroupCategoryMutation,
  prepareSessionGroupCategoryMutation,
} from "./session-group-categories.kernel.js";
import { runSessionCollaborationWrite } from "./session-sharing-store.async.js";

/** Prepared rows stay with the broker; only target identities cross the admission boundary. */
export function updateSessionGroupCategoriesInWorker(params: {
  scope: SessionAccessScope & { agentId: string };
  from: string;
  to?: string;
  assertTargetCurrent?: (target: { agentId: string; sessionKey: string }) => void;
}): Promise<number> {
  const { scope, from, to, assertTargetCurrent } = params;
  const agentId = scope.agentId;
  let keys: string[] = [];
  const assertCurrent = () => {
    for (const sessionKey of keys) {
      assertTargetCurrent?.({ agentId, sessionKey });
    }
  };
  return runSessionCollaborationWrite(
    scope,
    { type: "category.apply", input: { scope, from, to } },
    (capturedScope) => {
      const options = toDatabaseOptions(resolveSqliteScope(capturedScope));
      const database = openOpenClawAgentDatabase(options);
      const planned = prepareSessionGroupCategoryMutation(database, from);
      keys = [...planned.keys()];
      assertCurrent();
      return runOpenClawAgentWriteTransaction((current) => {
        assertCurrent();
        return applySessionGroupCategoryMutation(
          current,
          planned,
          to,
          capturedScope.env ?? process.env,
        ).length;
      }, options);
    },
    (changed, location, database) => {
      publishSessionEntryCacheCategoryUpdate(
        database,
        changed.map(({ sessionKey }) => sessionKey),
        to,
      );
      sessionChanges.emitBatch(
        changed.map(({ sessionKey, sessionId }) => ({
          agentId: location.agentId,
          storePath: location.storePath,
          sessionKey,
          facts: { kind: "category" as const, sessionId, category: to?.trim() || null },
        })),
      );
      return changed.length;
    },
    assertCurrent,
    async (operation, preparedScope) => {
      keys = await operation.execute({
        type: "category.prepare",
        input: { scope: preparedScope, from },
      });
      assertCurrent();
    },
  );
}
