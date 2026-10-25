import { resolveChannelGroupRequireMention } from "openclaw/plugin-sdk/channel-policy";
import { createPluginRuntimeMock } from "openclaw/plugin-sdk/channel-test-helpers";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { setMattermostRuntime } from "../runtime.js";
import type { MattermostConfig } from "../types.js";
import { resolveMattermostAccount } from "./accounts.js";
import type { MattermostIngressPost } from "./monitor-ingress.js";
import { createMattermostPostHandler } from "./monitor-posts.js";
import type { MattermostMonitorContext } from "./monitor-types.js";
import type { OpenClawConfig } from "./runtime-api.js";

const dispatch = vi.hoisted(() => vi.fn());
const hasParticipation = vi.hoisted(() => vi.fn());

vi.mock("./monitor-turn.js", () => ({ dispatchMattermostInboundTurn: dispatch }));
vi.mock("./thread-participation.js", () => ({
  hasMattermostThreadParticipationWithPersistence: hasParticipation,
}));
vi.mock("openclaw/plugin-sdk/channel-inbound", async (importOriginal) => ({
  ...(await importOriginal<typeof import("openclaw/plugin-sdk/channel-inbound")>()),
  resolveInboundSessionEnvelopeContextAsync: async () => ({ envelopeOptions: {} }),
}));

const botProps = { from_bot: "true" };

describe("Mattermost posts from bot accounts", () => {
  beforeEach(() => {
    dispatch.mockReset();
    hasParticipation.mockReset().mockResolvedValue(false);
  });

  function setup(config: Partial<MattermostConfig> = {}, cfgExtra: OpenClawConfig = {}) {
    const mattermost: MattermostConfig = {
      enabled: true,
      requireMention: false,
      groupPolicy: "open",
      historyLimit: 0,
      ...config,
    };
    const cfg: OpenClawConfig = {
      ...cfgExtra,
      channels: { ...cfgExtra.channels, mattermost },
    };
    const account = resolveMattermostAccount({ cfg, accountId: "default" });
    const core = createPluginRuntimeMock();
    core.channel.mentions.buildMentionRegexes = () => [];
    core.channel.groups.resolveRequireMention = resolveChannelGroupRequireMention;
    core.channel.routing.resolveAgentRoute = () => ({
      agentId: "main",
      channel: "mattermost",
      accountId: "default",
      sessionKey: "agent:main:mattermost:channel:room",
      mainSessionKey: "agent:main:main",
      lastRoutePolicy: "session",
      matchedBy: "default",
    });
    core.channel.commands.shouldHandleTextCommands = () => false;
    setMattermostRuntime(core);
    const monitor = {
      cfg,
      account,
      core,
      client: { request: vi.fn() },
      botUserId: "bot",
      botUsername: "bot",
      groupPolicy: account.config.groupPolicy ?? "open",
      pairing: { readAllowFromStore: async () => [] },
      resources: {
        resolveChannelInfo: async () => ({ id: "room", type: "O" }),
        resolveUserInfo: async (id: string) => ({ id, username: id }),
        resolveMattermostMedia: async () => [],
      },
      runtime: { log: vi.fn(), error: vi.fn() },
      logVerboseMessage: vi.fn(),
      logDebugMessage: vi.fn(),
    } as unknown as MattermostMonitorContext;
    const handler = createMattermostPostHandler(monitor);
    return (post: Partial<MattermostIngressPost> = {}) =>
      handler(
        {
          id: "post-1",
          channel_id: "room",
          user_id: "other-bot",
          message: "status update",
          create_at: 1_714_000_000_000,
          props: botProps,
          ...post,
        },
        { data: { sender_name: "other-bot" } },
      );
  }

  const human = { user_id: "alice", props: {} };
  const mentioned = { message: "@bot status update" };
  it.each([
    ["default admits bot posts", {}, {}, true],
    ["allowBots=false ignores bot posts", { allowBots: false }, {}, false],
    ["allowBots=false keeps human posts", { allowBots: false }, human, true],
    ["mentions drops an unmentioned bot post", { allowBots: "mentions" }, {}, false],
    ["mentions admits an explicit mention", { allowBots: "mentions" }, mentioned, true],
    ["mentions keeps unmentioned human posts", { allowBots: "mentions" }, human, true],
    [
      "channel override beats wildcard and account",
      { allowBots: false, groups: { "*": { allowBots: false }, room: { allowBots: true } } },
      {},
      true,
    ],
    ["wildcard override beats account", { groups: { "*": { allowBots: false } } }, {}, false],
  ] satisfies Array<[string, Partial<MattermostConfig>, Partial<MattermostIngressPost>, boolean]>)(
    "%s",
    async (_name, config, post, admitted) => {
      await setup(config)(post);
      expect(dispatch).toHaveBeenCalledTimes(admitted ? 1 : 0);
    },
  );

  it("does not count thread participation as a mention for allowBots=mentions", async () => {
    const receive = setup({ allowBots: "mentions", requireMention: true });
    hasParticipation.mockResolvedValue(true);

    await receive({ root_id: "root" });
    expect(dispatch).not.toHaveBeenCalled();

    await receive({ ...human, id: "post-2", root_id: "root" });
    expect(dispatch).toHaveBeenCalledOnce();
  });

  it("reports bot-pair facts with merged loop protection config", async () => {
    const receive = setup(
      {
        botLoopProtection: { windowSeconds: 600 },
        groups: {
          "*": { botLoopProtection: { cooldownSeconds: 1800, maxEventsPerWindow: 10 } },
          room: { botLoopProtection: { maxEventsPerWindow: 6 } },
        },
      },
      { channels: { defaults: { botLoopProtection: { enabled: true } } } },
    );

    await receive();
    await receive({ ...human, id: "post-2" });

    expect(dispatch.mock.calls.map(([, params]) => params.botLoopProtection)).toEqual([
      {
        scopeId: "default",
        conversationId: "room",
        senderId: "other-bot",
        receiverId: "bot",
        eventId: "post-1",
        config: { windowSeconds: 600, cooldownSeconds: 1800, maxEventsPerWindow: 6 },
        defaultsConfig: { enabled: true },
        defaultEnabled: true,
        nowMs: 1_714_000_000_000,
      },
      undefined,
    ]);
  });
});
