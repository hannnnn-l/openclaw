import { addTimerTimeoutGraceMs } from "@openclaw/normalization-core/number-coercion";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { logVerbose } from "../../globals.js";
import { racePromiseWithAbortSignal } from "../../infra/abort-signal.js";
import {
  exceedsApprovalTextLimit,
  sanitizeExecApprovalWarningTextWithStatus,
} from "../../infra/exec-approval-text-sanitize.js";
import { evaluateShellAllowlistWithAuthorization } from "../../infra/exec-approvals-allowlist.js";
import {
  loadExecApprovals,
  recordAllowlistMatchesUse,
  resolveExecApprovalsFromFile,
  type ExecAsk,
  type ExecSecurity,
} from "../../infra/exec-approvals.js";
import { buildAuthorizedShellCommandFromPlan } from "../../infra/exec-authorization-render.js";
import {
  EXEC_AUTO_REVIEW_DENIAL_GUIDANCE,
  formatExecAutoReviewAssessment,
} from "../../infra/exec-auto-review.js";
import {
  DEFAULT_PLUGIN_APPROVAL_TIMEOUT_MS,
  PLUGIN_APPROVAL_DESCRIPTION_MAX_LENGTH,
  PLUGIN_APPROVAL_TITLE_MAX_LENGTH,
  truncatePluginApprovalDetail,
} from "../../infra/plugin-approvals.js";
import {
  prepareSystemRunMutableFileBinding,
  revalidateSystemRunMutableFileBinding,
  type SystemRunMutableFileBinding,
} from "../../infra/system-run-approval-binding.js";
import { sliceUtf16Safe, truncateUtf16Safe } from "../../utils.js";
import type { ExecReviewerConfig } from "../exec-auto-reviewer.js";
import { callGatewayTool } from "../tools/gateway.js";

type CliNativeToolApprovalOutcome =
  | { kind: "allow"; grantAlways: boolean; updatedInput?: Record<string, unknown> }
  | {
      kind: "deny";
      reason: "operand-binding" | "policy-oversized" | "user" | "unavailable" | "auto-review";
      message?: string;
    };

const CLI_NATIVE_TOOL_DESCRIPTION_HEAD_CHARS = 300;
const CLI_NATIVE_TOOL_DESCRIPTION_TAIL_CHARS = 80;
const CLI_NATIVE_TOOL_DESCRIPTION_MAX_CHARS =
  CLI_NATIVE_TOOL_DESCRIPTION_HEAD_CHARS + CLI_NATIVE_TOOL_DESCRIPTION_TAIL_CHARS;
const CLI_NATIVE_TOOL_APPROVAL_GATEWAY_GRACE_MS = 10_000;
// Bash is arbitrary shell execution, so a name-wide grant is unrestricted.
// Bash fails closed when even the reviewer-only detail cannot show the complete input.
const CLI_NATIVE_TOOL_ARBITRARY_EXECUTION_TOOL = "Bash";

export function resolveCliNativeToolApprovalPlan(execPermission: {
  security: ExecSecurity;
  ask: ExecAsk;
}): "allow" | "deny" | "prompt" {
  if (execPermission.security === "deny") {
    return "deny";
  }
  // ask "off" means never prompt (exec mode "allowlist" relies on this): full
  // security auto-allows, anything stricter denies without an approval request.
  if (execPermission.ask === "off") {
    return execPermission.security === "full" ? "allow" : "deny";
  }
  return "prompt";
}

type CliNativeToolDescription = { compact: string; text: string; truncated: boolean };

/**
 * The gateway caps approval descriptions (PLUGIN_APPROVAL_DESCRIPTION_MAX_LENGTH),
 * so full inputs cannot ride this channel. Head+tail display defeats padded
 * prefixes hiding an executable tail, and the quantified marker makes a partial
 * view an explicit operator decision. Accepted tradeoff: the middle stays
 * unreviewable; oversized inputs therefore never earn allow-always.
 */
function formatCliNativeToolDescription(
  toolInput: Record<string, unknown>,
): CliNativeToolDescription {
  const compact = JSON.stringify(toolInput) ?? "{}";
  if (compact.length <= CLI_NATIVE_TOOL_DESCRIPTION_MAX_CHARS) {
    return { compact, text: compact, truncated: false };
  }
  const head = truncateUtf16Safe(compact, CLI_NATIVE_TOOL_DESCRIPTION_HEAD_CHARS);
  const tail = sliceUtf16Safe(compact, compact.length - CLI_NATIVE_TOOL_DESCRIPTION_TAIL_CHARS);
  const hiddenChars = compact.length - head.length - tail.length;
  return {
    compact,
    text: `${head} …[+${hiddenChars} chars hidden]… ${tail}`,
    truncated: true,
  };
}

/**
 * Review one allowlist-missing native Bash command with the configured model.
 * Returns `undefined` when this command cannot be reviewed, so the caller keeps
 * its existing human approval request. The reviewer itself maps provider
 * failures, timeouts, and invalid responses to `ask`.
 */
async function reviewCliNativeBashCommand(params: {
  command: string;
  cwd?: string;
  env?: NodeJS.ProcessEnv;
  cfg?: OpenClawConfig;
  agentId?: string;
  sessionKey?: string;
  reviewer?: ExecReviewerConfig;
  abortSignal?: AbortSignal;
}): Promise<import("../../infra/exec-auto-review.js").ExecAutoReviewDecision | undefined> {
  const { buildExecAutoReviewInputForShellCommand, reviewExecRequestWithConfiguredModel } =
    await import("../../plugin-sdk/agent-harness-exec-review-runtime.js");
  // The builder refuses multi-segment commands, policy-blocked carriers, blocked
  // shell wrappers, and audit-suppression or exec-control commands, so the
  // reviewer never sees a shape the approval binder would reject anyway.
  const input = await buildExecAutoReviewInputForShellCommand({
    command: params.command,
    cwd: params.cwd ?? null,
    host: "claude-cli",
    envKeys: params.env ? Object.keys(params.env) : undefined,
    agent: { id: params.agentId, sessionKey: params.sessionKey },
  });
  if (!input) {
    return undefined;
  }
  return racePromiseWithAbortSignal(
    reviewExecRequestWithConfiguredModel({
      cfg: params.cfg,
      agentId: params.agentId,
      reviewer: params.reviewer,
      input,
    }),
    params.abortSignal,
  );
}

export async function requestCliNativeToolApproval(params: {
  toolName: string;
  toolInput: Record<string, unknown>;
  pluginId: string;
  sessionKey?: string;
  agentId?: string;
  toolCallId?: string;
  cwd?: string;
  fallbackCwd?: string;
  env?: NodeJS.ProcessEnv;
  bindingEnv?: NodeJS.ProcessEnv;
  assertActive?: () => void;
  abortSignal?: AbortSignal;
  ask: ExecAsk;
  /** Exec mode "auto": review eligible Bash allowlist misses before prompting. */
  autoReview?: boolean;
  cfg?: OpenClawConfig;
  reviewer?: ExecReviewerConfig;
}): Promise<CliNativeToolApprovalOutcome> {
  try {
    const timeoutMs = DEFAULT_PLUGIN_APPROVAL_TIMEOUT_MS;
    const gatewayTimeoutMs =
      addTimerTimeoutGraceMs(timeoutMs, CLI_NATIVE_TOOL_APPROVAL_GATEWAY_GRACE_MS) ??
      timeoutMs + CLI_NATIVE_TOOL_APPROVAL_GATEWAY_GRACE_MS;
    const description = formatCliNativeToolDescription(params.toolInput);
    // Sanitization escapes control/bidi characters into longer visible
    // sequences, so a short raw command can still overflow the 512-char
    // description bound after sanitization and get truncated at render time.
    const summarySanitization =
      params.toolName === CLI_NATIVE_TOOL_ARBITRARY_EXECUTION_TOOL
        ? sanitizeExecApprovalWarningTextWithStatus(description.text)
        : null;
    // Approvals resolve from summary-only surfaces (channel text, push), which
    // never carry the reviewer detail. Bash therefore fails closed whenever any
    // resolving surface could see less than the complete command: a truncated
    // description, sanitization-altered display, or post-sanitization overflow.
    if (
      params.toolName === CLI_NATIVE_TOOL_ARBITRARY_EXECUTION_TOOL &&
      (description.truncated ||
        summarySanitization?.truncated === true ||
        summarySanitization?.oversized === true ||
        (summarySanitization &&
          exceedsApprovalTextLimit(
            summarySanitization.text,
            PLUGIN_APPROVAL_DESCRIPTION_MAX_LENGTH,
          )))
    ) {
      return { kind: "deny", reason: "policy-oversized" };
    }
    const bashCommand =
      params.toolName === CLI_NATIVE_TOOL_ARBITRARY_EXECUTION_TOOL &&
      typeof params.toolInput.command === "string"
        ? params.toolInput.command
        : undefined;
    let autoAllow: (() => CliNativeToolApprovalOutcome) | undefined;
    if (
      params.ask === "on-miss" &&
      params.pluginId === "claude-cli" &&
      bashCommand &&
      params.agentId
    ) {
      const file = loadExecApprovals();
      const { allowlist } = resolveExecApprovalsFromFile({ file, agentId: params.agentId });
      const analysis = await evaluateShellAllowlistWithAuthorization({
        command: bashCommand,
        allowlist,
        safeBins: new Set(),
        cwd: params.cwd,
        env: params.env,
      });
      const plan = analysis.authorizationPlan;
      const candidates = plan?.groups.flatMap((group) => group.candidates) ?? [];
      const miss = candidates.findIndex(
        (candidate, index) =>
          candidate.transport.kind !== "direct" ||
          candidate.trustMode !== "executable" ||
          !candidate.sourceSegment.resolution?.execution.resolvedPath ||
          candidate.sourceSegment.resolution.wrapperChain?.length ||
          analysis.segmentSatisfiedBy[index] !== "allowlist",
      );
      const rendered =
        plan?.ok && params.cwd && candidates.length > 0 && miss === -1
          ? buildAuthorizedShellCommandFromPlan({
              plan,
              mode: "enforced",
              segmentSatisfiedBy: analysis.segmentSatisfiedBy,
            })
          : {
              ok: false as const,
              reason:
                plan && !plan.ok
                  ? plan.reason
                  : (candidates[miss]?.sourceStep.text ?? "no classified executable"),
            };
      if (rendered.ok) {
        autoAllow = () => {
          params.abortSignal?.throwIfAborted();
          params.assertActive?.();
          // Require current grants even when the caller policy is full with prompting.
          recordAllowlistMatchesUse({
            approvals: file,
            agentId: params.agentId,
            matches: analysis.allowlistMatches,
            command: bashCommand,
            resolvedPath:
              candidates[0]?.sourceSegment.resolution?.execution.resolvedPath ?? undefined,
            authorization: {
              source: "current-policy",
              security: "allowlist",
              ask: params.ask,
              allowlistSatisfied: true,
            },
          });
          logVerbose("Claude CLI native Bash auto-allowed via exec allowlist");
          return {
            kind: "allow",
            grantAlways: false,
            updatedInput: { ...params.toolInput, command: rendered.command },
          };
        };
      } else {
        const reason = sanitizeExecApprovalWarningTextWithStatus(rendered.reason).text;
        description.text += `\nExec allowlist miss: ${truncateUtf16Safe(reason, 100)}`;
      }
    }
    let mutableFileBinding: SystemRunMutableFileBinding | undefined;
    if (params.toolName === CLI_NATIVE_TOOL_ARBITRARY_EXECUTION_TOOL) {
      // Bind script bytes before the out-of-band approval wait. Text-identical
      // Bash input can otherwise execute a rewritten file after approval.
      const prepared = await prepareSystemRunMutableFileBinding({
        command: { kind: "shell", text: bashCommand ?? "" },
        cwd: params.cwd ?? params.fallbackCwd,
        env: autoAllow ? params.env : (params.bindingEnv ?? params.env),
      });
      if (!prepared.ok) {
        const message =
          prepared.reason === "unsupported-command-shape"
            ? `${prepared.message}\nNo approval request was created for this attempt; this is not a user denial. Retry a supported direct executable/script command with explicit paths through the normal approval flow.`
            : prepared.message;
        return {
          kind: "deny",
          reason: "operand-binding",
          message: sanitizeExecApprovalWarningTextWithStatus(`${message}\n${description.text}`)
            .text,
        };
      }
      mutableFileBinding = prepared.binding.operands.length > 0 ? prepared.binding : undefined;
    }
    if (autoAllow) {
      return autoAllow();
    }
    // Exec mode "auto" reviews a bindable Bash allowlist miss with the
    // configured model before spending a human approval. "always" keeps
    // prompting, and an unreviewable command falls through unchanged.
    if (params.autoReview === true && bashCommand && params.ask !== "always") {
      const decision = await reviewCliNativeBashCommand({
        command: bashCommand,
        cwd: params.cwd ?? params.fallbackCwd,
        env: params.env,
        cfg: params.cfg,
        agentId: params.agentId,
        sessionKey: params.sessionKey,
        reviewer: params.reviewer,
        abortSignal: params.abortSignal,
      }).catch(() => undefined);
      if (params.abortSignal?.aborted) {
        return { kind: "deny", reason: "unavailable" };
      }
      if (decision?.decision === "deny") {
        // A reviewer denial is terminal: it must not become a human prompt.
        return {
          kind: "deny",
          reason: "auto-review",
          message: sanitizeExecApprovalWarningTextWithStatus(
            `Exec denied by auto-review (${formatExecAutoReviewAssessment(decision)}): ${decision.rationale}\n${EXEC_AUTO_REVIEW_DENIAL_GUIDANCE}`,
          ).text,
        };
      }
      if (decision?.decision === "allow-once") {
        // The model call is an out-of-band wait like a human approval, so the
        // bound script bytes must still match before the CLI owns the spawn.
        if (mutableFileBinding) {
          const binding = await revalidateSystemRunMutableFileBinding({
            binding: mutableFileBinding,
            cwd: params.cwd ?? params.fallbackCwd,
          });
          if (!binding.ok) {
            return { kind: "deny", reason: "operand-binding", message: binding.message };
          }
        }
        try {
          params.abortSignal?.throwIfAborted();
          params.assertActive?.();
        } catch {
          return { kind: "deny", reason: "unavailable" };
        }
        logVerbose(
          `Claude CLI native Bash auto-review allowed once (${formatExecAutoReviewAssessment(decision)})`,
        );
        return { kind: "allow", grantAlways: false };
      }
      if (decision) {
        description.text += `\nExec auto-review deferred to human approval (${formatExecAutoReviewAssessment(decision)}): ${truncateUtf16Safe(decision.rationale, 100)}`;
      }
    }
    if (
      bashCommand &&
      exceedsApprovalTextLimit(
        sanitizeExecApprovalWarningTextWithStatus(description.text).text,
        PLUGIN_APPROVAL_DESCRIPTION_MAX_LENGTH,
      )
    ) {
      return { kind: "deny", reason: "policy-oversized" };
    }
    // Standing grants require a complete description and never cover arbitrary Bash.
    const allowedDecisions =
      params.ask === "always" ||
      params.toolName === CLI_NATIVE_TOOL_ARBITRARY_EXECUTION_TOOL ||
      description.truncated
        ? ["allow-once", "deny"]
        : ["allow-once", "allow-always", "deny"];
    const requestResult = await racePromiseWithAbortSignal(
      callGatewayTool<{ id?: string; decision?: unknown }>(
        "plugin.approval.request",
        { timeoutMs: gatewayTimeoutMs },
        {
          pluginId: params.pluginId,
          toolName: params.toolName,
          toolCallId: params.toolCallId,
          agentId: params.agentId,
          sessionKey: params.sessionKey,
          title: truncateUtf16Safe(
            `${params.pluginId} native tool: ${params.toolName}`,
            PLUGIN_APPROVAL_TITLE_MAX_LENGTH,
          ),
          description: description.text,
          detail: truncatePluginApprovalDetail(description.compact),
          severity: "warning",
          allowedDecisions,
          timeoutMs,
          twoPhase: true,
        },
        { expectFinal: false, signal: params.abortSignal },
      ),
      params.abortSignal,
    );
    const id = typeof requestResult?.id === "string" ? requestResult.id : "";
    if (!id) {
      return { kind: "deny", reason: "unavailable" };
    }
    let decision: unknown;
    if (Object.hasOwn(requestResult ?? {}, "decision")) {
      decision = requestResult.decision;
    } else {
      // Abort must cancel the RPC so the Gateway removes the pending prompt.
      const waitResult = await racePromiseWithAbortSignal(
        callGatewayTool<{ id?: string; decision?: unknown }>(
          "plugin.approval.waitDecision",
          { timeoutMs: gatewayTimeoutMs },
          { id },
          { signal: params.abortSignal },
        ),
        params.abortSignal,
      );
      decision = waitResult?.id === id ? waitResult.decision : undefined;
    }
    if (params.abortSignal?.aborted) {
      return { kind: "deny", reason: "unavailable" };
    }
    if ((decision === "allow-once" || decision === "allow-always") && mutableFileBinding) {
      // This control response is OpenClaw's last boundary before the CLI owns
      // spawn, so reject bytes that changed during the approval wait.
      const binding = await revalidateSystemRunMutableFileBinding({
        binding: mutableFileBinding,
        cwd: params.cwd ?? params.fallbackCwd,
      });
      if (!binding.ok) {
        return { kind: "deny", reason: "operand-binding", message: binding.message };
      }
    }
    if (
      decision === "allow-once" ||
      (decision === "allow-always" && allowedDecisions.includes(decision))
    ) {
      return { kind: "allow", grantAlways: decision === "allow-always" };
    }
    return { kind: "deny", reason: decision === "deny" ? "user" : "unavailable" };
  } catch {
    return { kind: "deny", reason: "unavailable" };
  }
}
