import 'dotenv/config';

import { spawn } from 'node:child_process';
import { promises as fs, rmSync } from 'node:fs';
import path from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';

import {
  ChannelType,
  Client,
  GatewayIntentBits,
  MessageFlags,
  PermissionFlagsBits,
  SlashCommandBuilder,
} from 'discord.js';
import {
  AudioPlayerStatus,
  EndBehaviorType,
  NoSubscriberBehavior,
  StreamType,
  VoiceConnectionStatus,
  createAudioPlayer,
  createAudioResource,
  entersState,
  getVoiceConnection,
  joinVoiceChannel,
} from '@discordjs/voice';
import Groq, { toFile } from 'groq-sdk';
import prism from 'prism-media';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const tmpDir = path.join(__dirname, 'tmp');
await fs.mkdir(tmpDir, { recursive: true });
const lockPath = path.join(tmpDir, 'voice-bot.pid');
const dataDir = path.resolve(process.env.DATA_DIR || path.join(__dirname, 'data'));
await fs.mkdir(dataDir, { recursive: true });
const statePath = path.join(dataDir, 'state.json');
const runtimeConfigPath = path.join(dataDir, 'runtime-config.json');
const statusPath = path.join(dataDir, 'status.json');
const eventLogPath = path.join(dataDir, 'events.jsonl');

async function ensureSingleInstance() {
  const existingPidText = await fs.readFile(lockPath, 'utf8').catch(() => null);
  const existingPid = Number(existingPidText);
  if (Number.isInteger(existingPid) && existingPid > 0 && existingPid !== process.pid) {
    try {
      process.kill(existingPid, 0);
      throw new Error(`Another voice-bot.mjs process is already running with pid ${existingPid}`);
    } catch (error) {
      if (error.code !== 'ESRCH') throw error;
    }
  }

  await fs.writeFile(lockPath, String(process.pid));
  const cleanup = () => rmSync(lockPath, { force: true });
  process.once('exit', cleanup);
  for (const signal of ['SIGINT', 'SIGTERM']) {
    process.once(signal, () => {
      cleanup();
      process.exit(0);
    });
  }
}

await ensureSingleInstance();

const DISCORD_TOKEN = process.env.DISCORD_TOKEN?.trim();
const DISCORD_GUILD_ID = process.env.DISCORD_GUILD_ID?.trim();
const GROQ_API_KEY = process.env.GROQ_API_KEY?.trim();
const AUTO_JOIN_ENABLED = (process.env.AUTO_JOIN_ENABLED || 'false') === 'true';
const AUTO_JOIN_GUILD_ID = process.env.AUTO_JOIN_GUILD_ID?.trim() || '';
const AUTO_JOIN_VOICE_CHANNEL_ID = process.env.AUTO_JOIN_VOICE_CHANNEL_ID?.trim() || '';
const AUTO_JOIN_TEXT_CHANNEL_ID = process.env.AUTO_JOIN_TEXT_CHANNEL_ID?.trim() || '';

const DEFAULT_GROQ_CHAT_MODEL = process.env.GROQ_CHAT_MODEL?.trim() || 'groq/compound';
const DEFAULT_GROQ_STT_MODEL = process.env.GROQ_STT_MODEL?.trim() || 'whisper-large-v3-turbo';
const DEFAULT_WEB_SEARCH_ENABLED = (process.env.WEB_SEARCH_ENABLED || 'true') === 'true';
const DEFAULT_WEB_SEARCH_MODEL = process.env.WEB_SEARCH_MODEL?.trim() || 'groq/compound';
const DEFAULT_IDLE_CHATTER_ENABLED = (process.env.IDLE_CHATTER_ENABLED || 'false') === 'true';
const DEFAULT_IDLE_CHATTER_MINUTES = Math.max(1, Math.min(180, Number(process.env.IDLE_CHATTER_MINUTES || 5)));
const DEFAULT_IDLE_CHATTER_USE_WEB = (process.env.IDLE_CHATTER_USE_WEB || 'true') === 'true';
const DEFAULT_IDLE_CHATTER_STYLE = process.env.IDLE_CHATTER_STYLE?.trim() || 'mixed';
const DEFAULT_IDLE_LEAVE_ENABLED = (process.env.IDLE_LEAVE_ENABLED || 'true') === 'true';
const DEFAULT_IDLE_LEAVE_MINUTES = Math.max(1, Math.min(1440, Number(process.env.IDLE_LEAVE_MINUTES || 60)));
const DEFAULT_IDLE_LEAVE_PHRASE = process.env.IDLE_LEAVE_PHRASE?.trim() || '';
const DEFAULT_ACTIVE_DIALOGUE_ENABLED = (process.env.ACTIVE_DIALOGUE_ENABLED || 'false') === 'true';
const DEFAULT_ACTIVE_DIALOGUE_SECONDS = Math.max(10, Math.min(300, Number(process.env.ACTIVE_DIALOGUE_SECONDS || 45)));
const DEFAULT_CONFIRM_DANGEROUS_ACTIONS = (process.env.CONFIRM_DANGEROUS_ACTIONS || 'true') === 'true';
const DEFAULT_ASSISTANT_PERSONA = process.env.ASSISTANT_PERSONA?.trim() || 'default';
const DEFAULT_ASSISTANT_NAME = process.env.ASSISTANT_NAME?.trim() || 'Бот';
const DEFAULT_HEALTHCHECK_ENABLED = (process.env.HEALTHCHECK_ENABLED || 'true') === 'true';
const DEFAULT_STT_LANGUAGE = process.env.STT_LANGUAGE?.trim() ?? '';
const DEFAULT_TTS_PROVIDER = (process.env.TTS_PROVIDER?.trim() || (process.platform === 'darwin' ? 'macos' : 'espeak')).toLowerCase();
const DEFAULT_MACOS_TTS_VOICE = process.env.MACOS_TTS_VOICE?.trim() || 'Milena';
const DEFAULT_ESPEAK_TTS_VOICE = process.env.ESPEAK_TTS_VOICE?.trim() || 'ru';
const DEFAULT_ESPEAK_TTS_SPEED = Math.max(80, Math.min(260, Number(process.env.ESPEAK_TTS_SPEED || 165)));
const DEFAULT_EDGE_TTS_VOICE = process.env.EDGE_TTS_VOICE?.trim() || 'ru-RU-SvetlanaNeural';
const DEFAULT_EDGE_TTS_ENGLISH_VOICE = process.env.EDGE_TTS_ENGLISH_VOICE?.trim() || 'en-US-AvaMultilingualNeural';
const DEFAULT_EDGE_TTS_RATE = process.env.EDGE_TTS_RATE?.trim() || '+0%';
const DEFAULT_EDGE_TTS_PITCH = process.env.EDGE_TTS_PITCH?.trim() || '+0Hz';
const EDGE_TTS_COMMAND = process.env.EDGE_TTS_COMMAND?.trim() || '';

const LISTEN_WITHOUT_WAKE_WORD = (process.env.LISTEN_WITHOUT_WAKE_WORD || 'false') === 'true';
const ENV_BOT_WAKE_WORD = (process.env.BOT_WAKE_WORD || DEFAULT_ASSISTANT_NAME || 'бот').trim().toLowerCase();
const DEFAULT_BOT_WAKE_ALIASES = ENV_BOT_WAKE_WORD === 'бот'
  ? 'вот,от,робот,роботик,ботик,бота,боту,боте,боты,ботом,бод,бат,борт,вод,бо,ботт'
  : '';
const ENV_BOT_WAKE_ALIASES = process.env.BOT_WAKE_ALIASES || DEFAULT_BOT_WAKE_ALIASES;
const ENV_BOT_WAKE_FUZZY = (process.env.BOT_WAKE_FUZZY || 'true') === 'true';
const MAX_REPLY_CHARS = Math.max(120, Number(process.env.MAX_REPLY_CHARS || 500));
const SILENT_MESSAGES = (process.env.SILENT_MESSAGES || 'true') === 'true';
const SILENCE_MS = Math.max(450, Number(process.env.SILENCE_MS || 1500));
const MAX_UTTERANCE_MS = Math.max(3000, Number(process.env.MAX_UTTERANCE_MS || 12000));
const STALE_CAPTURE_MS = MAX_UTTERANCE_MS + SILENCE_MS + 5000;
const MIN_AUDIO_MS = Math.max(250, Number(process.env.MIN_AUDIO_MS || 350));
const MIN_RMS = Math.max(1, Number(process.env.MIN_RMS || 60));
const REPLY_COOLDOWN_MS = Math.max(0, Number(process.env.REPLY_COOLDOWN_MS || 3500));
const IGNORE_AFTER_JOIN_MS = Math.max(0, Number(process.env.IGNORE_AFTER_JOIN_MS || 500));
const PRESENCE_ANNOUNCEMENTS_ENABLED = (process.env.PRESENCE_ANNOUNCEMENTS_ENABLED || 'true') === 'true';
const PRESENCE_ANNOUNCEMENT_DELAY_MS = Math.max(0, Number(process.env.PRESENCE_ANNOUNCEMENT_DELAY_MS || 900));
const PRESENCE_ANNOUNCEMENT_COOLDOWN_MS = Math.max(0, Number(process.env.PRESENCE_ANNOUNCEMENT_COOLDOWN_MS || 25_000));
const PRESENCE_ANNOUNCEMENT_QUIET_WAIT_MS = Math.max(0, Number(process.env.PRESENCE_ANNOUNCEMENT_QUIET_WAIT_MS || 8_000));
const VOICE_DEBUG = (process.env.VOICE_DEBUG || 'false') === 'true';
const API_LIMIT_ALERT_THRESHOLDS = [50, 20, 10];
const MAX_MEMORY_ITEMS = Math.max(10, Number(process.env.MAX_MEMORY_ITEMS || 200));
const MEMORY_CONTEXT_LIMIT = Math.max(0, Number(process.env.MEMORY_CONTEXT_LIMIT || 8));
const MAX_REMINDER_ITEMS = Math.max(10, Number(process.env.MAX_REMINDER_ITEMS || 200));
const MAX_REMINDER_TIMEOUT_MS = 2_147_000_000;
const IDLE_CHATTER_CHECK_MS = 30_000;
const IDLE_LEAVE_CHECK_MS = 30_000;
const HEALTHCHECK_INTERVAL_MS = 60_000;
const EVENT_LOG_MAX_PAYLOAD_CHARS = 2500;
const STT_PROMPT_BASE = process.env.STT_PROMPT?.trim()
  || 'Русская и английская речь в Discord, часто mixed language. Частые слова: Бот, bot, what, вот, от, робот, роботик, ботик, бота, боду, бод, bat, board, борт, войс, voice, channel, disconnect, mute, move, запомни, remember, remind, stop, хватит, остановись, харош, хорош.';

if (!DISCORD_TOKEN) throw new Error('DISCORD_TOKEN is missing in .env');
if (!GROQ_API_KEY) console.warn('GROQ_API_KEY is missing. Chat/STT will fail until it is set in .env or runtime config.');

function logVoiceDebug(message) {
  if (VOICE_DEBUG) console.log(message);
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createVoiceDiagnostics() {
  return {
    voiceEvents: 0,
    captures: 0,
    ignored: 0,
    lastVoiceEventAt: null,
    lastCaptureAt: null,
    lastCaptureStats: null,
    lastTranscript: null,
    lastIgnoredReason: null,
    lastIgnoredAt: null,
    lastError: null,
    lastAnswerAt: null,
    lastTimingsMs: null,
  };
}

function markIgnored(session, reason, extra = {}) {
  if (!session?.diagnostics) return;
  session.diagnostics.ignored += 1;
  session.diagnostics.lastIgnoredReason = reason;
  session.diagnostics.lastIgnoredAt = Date.now();
  Object.assign(session.diagnostics, extra);
  logVoiceDebug(`capture ignored reason=${reason}`);
}

function captureTimeoutError() {
  const error = new Error('capture timeout');
  error.code = 'CAPTURE_TIMEOUT';
  return error;
}

function isExpectedReceiveClose(error) {
  return error?.code === 'ERR_STREAM_PREMATURE_CLOSE' || error?.code === 'CAPTURE_TIMEOUT';
}

function cleanupStaleActiveCaptures(session) {
  const activeUserIds = [...(session?.activeUsers || [])];
  if (!activeUserIds.length) {
    session?.activeUserStartedAt?.clear?.();
    return;
  }
  session.activeUserStartedAt ||= new Map();
  const now = Date.now();
  for (const userId of activeUserIds) {
    const startedAt = session.activeUserStartedAt.get(userId) || 0;
    if (startedAt && now - startedAt <= STALE_CAPTURE_MS) continue;
    const reason = startedAt ? 'stale_capture_cleared' : 'orphan_capture_cleared';
    session.activeUserStartedAt.delete(userId);
    session.activeUsers?.delete(userId);
    if (session.diagnostics) {
      session.diagnostics.staleCaptures = (session.diagnostics.staleCaptures || 0) + 1;
      session.diagnostics.lastIgnoredReason = reason;
      session.diagnostics.lastIgnoredAt = now;
    }
    appendEvent(reason, {
      guildId: session.guild?.id,
      voiceChannelId: session.voiceChannel?.id,
      userId,
      ageMs: startedAt ? now - startedAt : null,
    });
  }
  for (const userId of [...session.activeUserStartedAt.keys()]) {
    if (!session.activeUsers.has(userId)) session.activeUserStartedAt.delete(userId);
  }
}

function safeEventValue(value) {
  if (value === null || value === undefined) return value;
  if (typeof value === 'string') {
    return value.length > EVENT_LOG_MAX_PAYLOAD_CHARS
      ? `${value.slice(0, EVENT_LOG_MAX_PAYLOAD_CHARS)}...`
      : value;
  }
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (Array.isArray(value)) return value.slice(0, 40).map(safeEventValue);
  if (typeof value === 'object') {
    const result = {};
    for (const [key, item] of Object.entries(value).slice(0, 80)) {
      if (/token|secret|password|apiKey|authorization/i.test(key)) {
        result[key] = '[redacted]';
      } else {
        result[key] = safeEventValue(item);
      }
    }
    return result;
  }
  return String(value);
}

function appendEvent(type, payload = {}) {
  const row = {
    ts: new Date().toISOString(),
    type,
    payload: safeEventValue(payload),
  };
  void fs.appendFile(eventLogPath, `${JSON.stringify(row)}\n`).catch((error) => {
    console.error('event log write failed:', error);
  });
}

const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates],
});

const sessions = new Map();
const groqLimitAlertState = new Map();
const groqLastLimits = new Map();
const reminderTimers = new Map();
const stateStore = await loadStateStore();
let runtimeConfig = await loadRuntimeConfig();
let runtimeConfigMtime = 0;
let saveStoreQueue = Promise.resolve();
let saveRuntimeConfigQueue = Promise.resolve();
let groqClient = null;
let groqClientKey = '';
let monitorChannel = null;
let lastBotEnabled = runtimeConfig.botEnabled !== false;
let autoJoinInProgress = false;
let autoJoinSuppressedUntilManualJoin = false;
let healthcheckInProgress = false;
const startedAt = Date.now();

function hasConfiguredAutoJoin() {
  return Boolean(AUTO_JOIN_ENABLED && AUTO_JOIN_GUILD_ID && AUTO_JOIN_VOICE_CHANNEL_ID && AUTO_JOIN_TEXT_CHANNEL_ID);
}

function createEmptyStateStore() {
  return { version: 1, guilds: {} };
}

async function loadStateStore() {
  const raw = await fs.readFile(statePath, 'utf8').catch((error) => {
    if (error.code === 'ENOENT') return null;
    throw error;
  });
  if (!raw) return createEmptyStateStore();

  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return createEmptyStateStore();
    if (!parsed.guilds || typeof parsed.guilds !== 'object') parsed.guilds = {};
    parsed.version = 1;
    return parsed;
  } catch (error) {
    const brokenPath = `${statePath}.broken-${Date.now()}`;
    await fs.rename(statePath, brokenPath).catch(() => {});
    console.error(`state store is corrupted, moved to ${brokenPath}:`, error);
    return createEmptyStateStore();
  }
}

function getGuildState(guildId) {
  const key = String(guildId || 'global');
  if (!stateStore.guilds[key]) {
    stateStore.guilds[key] = { memories: [], userMemories: {}, reminders: [] };
  }
  const guildState = stateStore.guilds[key];
  if (!Array.isArray(guildState.memories)) guildState.memories = [];
  if (!guildState.userMemories || typeof guildState.userMemories !== 'object') guildState.userMemories = {};
  if (!Array.isArray(guildState.reminders)) guildState.reminders = [];
  return guildState;
}

function saveStateStore() {
  const payload = JSON.stringify(stateStore, null, 2);
  const tmpPath = `${statePath}.tmp`;
  saveStoreQueue = saveStoreQueue
    .catch(() => {})
    .then(async () => {
      await fs.writeFile(tmpPath, payload);
      await fs.rename(tmpPath, statePath);
    })
    .catch((error) => console.error('state store save failed:', error));
  return saveStoreQueue;
}

function createId(prefix) {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function trimStoredItems(guildState) {
  guildState.memories.splice(0, Math.max(0, guildState.memories.length - MAX_MEMORY_ITEMS));
  for (const [userId, memories] of Object.entries(guildState.userMemories || {})) {
    if (!Array.isArray(memories)) {
      delete guildState.userMemories[userId];
      continue;
    }
    memories.splice(0, Math.max(0, memories.length - MAX_MEMORY_ITEMS));
  }
  guildState.reminders.sort((a, b) => a.dueAt - b.dueAt);
  guildState.reminders.splice(MAX_REMINDER_ITEMS);
}

function defaultWakeAliasesFor(wakeWord) {
  const normalizedWake = normalizeCommandText(wakeWord);
  if (normalizedWake === 'бот') {
    return 'вот,от,робот,роботик,ботик,бота,боту,боте,боты,ботом,бод,бат,борт,вод,бо,ботт';
  }
  if (normalizedWake === 'железяка') {
    return 'железка,железяко,железяку,железяке,железякой,железяки,железякин';
  }
  return '';
}

function normalizeAssistantName(value, fallback = DEFAULT_ASSISTANT_NAME) {
  const name = String(value ?? fallback ?? 'Бот').replace(/\s+/g, ' ').trim().slice(0, 40);
  return name || 'Бот';
}

function normalizeWakeWordValue(value, fallback = ENV_BOT_WAKE_WORD) {
  const raw = String(value ?? fallback ?? 'бот').replace(/\s+/g, ' ').trim().slice(0, 40);
  return normalizeCommandText(raw) || 'бот';
}

function normalizeWakeAliasesValue(value, wakeWord) {
  const fallback = ENV_BOT_WAKE_ALIASES || defaultWakeAliasesFor(wakeWord);
  const raw = Array.isArray(value) ? value.join(',') : String(value ?? fallback);
  return raw
    .split(',')
    .map((item) => normalizeCommandText(item))
    .filter((item, index, list) => item && item !== wakeWord && list.indexOf(item) === index)
    .join(', ');
}

function defaultRuntimeConfig() {
  const wakeWord = normalizeWakeWordValue(ENV_BOT_WAKE_WORD);
  return {
    botEnabled: true,
    listeningPaused: false,
    assistantName: normalizeAssistantName(DEFAULT_ASSISTANT_NAME),
    wakeWord,
    wakeAliases: normalizeWakeAliasesValue(ENV_BOT_WAKE_ALIASES, wakeWord),
    wakeFuzzy: ENV_BOT_WAKE_FUZZY,
    groqApiKey: '',
    groqChatModel: DEFAULT_GROQ_CHAT_MODEL,
    groqSttModel: DEFAULT_GROQ_STT_MODEL,
    webSearchEnabled: DEFAULT_WEB_SEARCH_ENABLED,
    webSearchModel: DEFAULT_WEB_SEARCH_MODEL,
    idleChatterEnabled: DEFAULT_IDLE_CHATTER_ENABLED,
    idleChatterMinutes: DEFAULT_IDLE_CHATTER_MINUTES,
    idleChatterUseWeb: DEFAULT_IDLE_CHATTER_USE_WEB,
    idleChatterStyle: DEFAULT_IDLE_CHATTER_STYLE,
    idleLeaveEnabled: DEFAULT_IDLE_LEAVE_ENABLED,
    idleLeaveMinutes: DEFAULT_IDLE_LEAVE_MINUTES,
    idleLeavePhrase: DEFAULT_IDLE_LEAVE_PHRASE,
    activeDialogueEnabled: DEFAULT_ACTIVE_DIALOGUE_ENABLED,
    activeDialogueSeconds: DEFAULT_ACTIVE_DIALOGUE_SECONDS,
    confirmDangerousActions: DEFAULT_CONFIRM_DANGEROUS_ACTIONS,
    assistantPersona: DEFAULT_ASSISTANT_PERSONA,
    healthcheckEnabled: DEFAULT_HEALTHCHECK_ENABLED,
    sttLanguage: DEFAULT_STT_LANGUAGE,
    ttsProvider: DEFAULT_TTS_PROVIDER,
    macosVoice: DEFAULT_MACOS_TTS_VOICE,
    espeakVoice: DEFAULT_ESPEAK_TTS_VOICE,
    espeakSpeed: DEFAULT_ESPEAK_TTS_SPEED,
    edgeVoice: DEFAULT_EDGE_TTS_VOICE,
    edgeEnglishVoice: DEFAULT_EDGE_TTS_ENGLISH_VOICE,
    edgeRate: DEFAULT_EDGE_TTS_RATE,
    edgePitch: DEFAULT_EDGE_TTS_PITCH,
    updatedAt: Date.now(),
  };
}

function normalizeRuntimeConfig(value = {}) {
  const defaults = defaultRuntimeConfig();
  const wakeWord = normalizeWakeWordValue(value.wakeWord, defaults.wakeWord);
  return {
    ...defaults,
    ...value,
    botEnabled: value.botEnabled !== false,
    listeningPaused: value.listeningPaused === true,
    assistantName: normalizeAssistantName(value.assistantName, defaults.assistantName),
    wakeWord,
    wakeAliases: normalizeWakeAliasesValue(value.wakeAliases, wakeWord),
    wakeFuzzy: value.wakeFuzzy === undefined ? defaults.wakeFuzzy : value.wakeFuzzy !== false,
    groqApiKey: String(value.groqApiKey || ''),
    groqChatModel: String(value.groqChatModel || defaults.groqChatModel),
    groqSttModel: String(value.groqSttModel || defaults.groqSttModel),
    webSearchEnabled: value.webSearchEnabled === undefined ? defaults.webSearchEnabled : value.webSearchEnabled !== false,
    webSearchModel: String(value.webSearchModel || defaults.webSearchModel),
    idleChatterEnabled: value.idleChatterEnabled === undefined ? defaults.idleChatterEnabled : value.idleChatterEnabled === true,
    idleChatterMinutes: Math.max(1, Math.min(180, Number(value.idleChatterMinutes || defaults.idleChatterMinutes))),
    idleChatterUseWeb: value.idleChatterUseWeb === undefined ? defaults.idleChatterUseWeb : value.idleChatterUseWeb !== false,
    idleChatterStyle: String(value.idleChatterStyle || defaults.idleChatterStyle),
    idleLeaveEnabled: value.idleLeaveEnabled === undefined ? defaults.idleLeaveEnabled : value.idleLeaveEnabled === true,
    idleLeaveMinutes: Math.max(1, Math.min(1440, Number(value.idleLeaveMinutes || defaults.idleLeaveMinutes))),
    idleLeavePhrase: String(value.idleLeavePhrase ?? defaults.idleLeavePhrase).replace(/\s+/g, ' ').trim().slice(0, 240),
    activeDialogueEnabled: value.activeDialogueEnabled === undefined ? defaults.activeDialogueEnabled : value.activeDialogueEnabled === true,
    activeDialogueSeconds: Math.max(10, Math.min(300, Number(value.activeDialogueSeconds || defaults.activeDialogueSeconds))),
    confirmDangerousActions: value.confirmDangerousActions === undefined ? defaults.confirmDangerousActions : value.confirmDangerousActions !== false,
    assistantPersona: String(value.assistantPersona || defaults.assistantPersona),
    healthcheckEnabled: value.healthcheckEnabled === undefined ? defaults.healthcheckEnabled : value.healthcheckEnabled !== false,
    sttLanguage: normalizeSttLanguage(value.sttLanguage, defaults.sttLanguage),
    ttsProvider: String(value.ttsProvider || defaults.ttsProvider).toLowerCase(),
    macosVoice: String(value.macosVoice || defaults.macosVoice),
    espeakVoice: String(value.espeakVoice || defaults.espeakVoice),
    espeakSpeed: Math.max(80, Math.min(260, Number(value.espeakSpeed || defaults.espeakSpeed))),
    edgeVoice: String(value.edgeVoice || defaults.edgeVoice),
    edgeEnglishVoice: String(value.edgeEnglishVoice || defaults.edgeEnglishVoice),
    edgeRate: String(value.edgeRate || defaults.edgeRate),
    edgePitch: String(value.edgePitch || defaults.edgePitch),
  };
}

async function loadRuntimeConfig() {
  const raw = await fs.readFile(runtimeConfigPath, 'utf8').catch((error) => {
    if (error.code === 'ENOENT') return null;
    throw error;
  });
  if (!raw) return defaultRuntimeConfig();
  try {
    return normalizeRuntimeConfig(JSON.parse(raw));
  } catch (error) {
    console.error('runtime config parse failed:', error);
    return defaultRuntimeConfig();
  }
}

function saveRuntimeConfig() {
  runtimeConfig.updatedAt = Date.now();
  const payload = JSON.stringify(runtimeConfig, null, 2);
  const tmpPath = `${runtimeConfigPath}.tmp`;
  saveRuntimeConfigQueue = saveRuntimeConfigQueue
    .catch(() => {})
    .then(async () => {
      await fs.writeFile(tmpPath, payload);
      await fs.rename(tmpPath, runtimeConfigPath);
      const stat = await fs.stat(runtimeConfigPath).catch(() => null);
      runtimeConfigMtime = stat?.mtimeMs || Date.now();
    })
    .catch((error) => console.error('runtime config save failed:', error));
  return saveRuntimeConfigQueue;
}

function updateRuntimeConfig(patch) {
  runtimeConfig = normalizeRuntimeConfig({ ...runtimeConfig, ...patch });
  void saveRuntimeConfig();
  return runtimeConfig;
}

async function reloadRuntimeConfigIfChanged() {
  const stat = await fs.stat(runtimeConfigPath).catch(() => null);
  if (!stat) return;
  if (stat.mtimeMs <= runtimeConfigMtime) return;
  runtimeConfigMtime = stat.mtimeMs;
  runtimeConfig = await loadRuntimeConfig();
}

function effectiveGroqApiKey() {
  return runtimeConfig.groqApiKey?.trim() || GROQ_API_KEY;
}

function getGroqClient() {
  const apiKey = effectiveGroqApiKey();
  if (!apiKey) throw new Error('Groq API key is missing.');
  if (!groqClient || groqClientKey !== apiKey) {
    groqClient = new Groq({ apiKey });
    groqClientKey = apiKey;
  }
  return groqClient;
}

function getChatModel() {
  return runtimeConfig.groqChatModel || DEFAULT_GROQ_CHAT_MODEL;
}

function getSttModel() {
  return runtimeConfig.groqSttModel || DEFAULT_GROQ_STT_MODEL;
}

function isWebSearchEnabled() {
  return runtimeConfig.webSearchEnabled !== false;
}

function getWebSearchModel() {
  return runtimeConfig.webSearchModel || DEFAULT_WEB_SEARCH_MODEL;
}

function isIdleChatterEnabled() {
  return runtimeConfig.idleChatterEnabled === true;
}

function getIdleChatterMinutes() {
  return Math.max(1, Math.min(180, Number(runtimeConfig.idleChatterMinutes || DEFAULT_IDLE_CHATTER_MINUTES)));
}

function isIdleChatterWebEnabled() {
  return runtimeConfig.idleChatterUseWeb !== false;
}

function getIdleChatterStyle() {
  const style = String(runtimeConfig.idleChatterStyle || DEFAULT_IDLE_CHATTER_STYLE).toLowerCase();
  return ['mixed', 'roast', 'facts', 'news', 'context'].includes(style) ? style : 'mixed';
}

function isIdleLeaveEnabled() {
  return runtimeConfig.idleLeaveEnabled === true;
}

function getIdleLeaveMinutes() {
  return Math.max(1, Math.min(1440, Number(runtimeConfig.idleLeaveMinutes || DEFAULT_IDLE_LEAVE_MINUTES)));
}

function getIdleLeavePhrase() {
  return String(runtimeConfig.idleLeavePhrase || '').replace(/\s+/g, ' ').trim().slice(0, 240);
}

function isActiveDialogueEnabled() {
  return runtimeConfig.activeDialogueEnabled === true;
}

function getActiveDialogueSeconds() {
  return Math.max(10, Math.min(300, Number(runtimeConfig.activeDialogueSeconds || DEFAULT_ACTIVE_DIALOGUE_SECONDS)));
}

function shouldConfirmDangerousActions() {
  return runtimeConfig.confirmDangerousActions !== false;
}

function getAssistantName() {
  return normalizeAssistantName(runtimeConfig.assistantName, DEFAULT_ASSISTANT_NAME);
}

function getWakeWord() {
  return normalizeWakeWordValue(runtimeConfig.wakeWord, ENV_BOT_WAKE_WORD);
}

function getWakeAliases() {
  return normalizeWakeAliasesValue(runtimeConfig.wakeAliases, getWakeWord())
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function isWakeFuzzyEnabled() {
  return runtimeConfig.wakeFuzzy !== false;
}

function wakeWordPattern() {
  const wakeWord = getWakeWord();
  return wakeWord
    ? new RegExp(`(^|[^\\p{L}\\p{N}_])${escapeRegExp(wakeWord)}(?=$|[^\\p{L}\\p{N}_])`, 'iu')
    : null;
}

function getAssistantPersona() {
  const persona = String(runtimeConfig.assistantPersona || DEFAULT_ASSISTANT_PERSONA).toLowerCase();
  return ['default', 'friendly', 'sharp', 'admin', 'quiet', 'english'].includes(persona) ? persona : 'default';
}

function isHealthcheckEnabled() {
  return runtimeConfig.healthcheckEnabled !== false;
}

function normalizeSttLanguage(value, fallback = '') {
  const raw = value === undefined || value === null ? fallback : value;
  const language = String(raw ?? '').trim();
  return language.toLowerCase() === 'auto' ? '' : language;
}

function getSttLanguage() {
  return normalizeSttLanguage(runtimeConfig.sttLanguage, DEFAULT_STT_LANGUAGE);
}

function getTtsProvider() {
  return (runtimeConfig.ttsProvider || DEFAULT_TTS_PROVIDER).toLowerCase();
}

function getMacosVoice() {
  return runtimeConfig.macosVoice || DEFAULT_MACOS_TTS_VOICE;
}

function getEspeakVoice() {
  return runtimeConfig.espeakVoice || DEFAULT_ESPEAK_TTS_VOICE;
}

function getEspeakSpeed() {
  return Math.max(80, Math.min(260, Number(runtimeConfig.espeakSpeed || DEFAULT_ESPEAK_TTS_SPEED)));
}

function getEdgeVoice() {
  return runtimeConfig.edgeVoice || DEFAULT_EDGE_TTS_VOICE;
}

function getEdgeEnglishVoice() {
  return runtimeConfig.edgeEnglishVoice || DEFAULT_EDGE_TTS_ENGLISH_VOICE;
}

function getEdgeRate() {
  return runtimeConfig.edgeRate || DEFAULT_EDGE_TTS_RATE;
}

function getEdgePitch() {
  return runtimeConfig.edgePitch || DEFAULT_EDGE_TTS_PITCH;
}

function getEdgeVoiceForText(text) {
  return isMostlyEnglishText(text) ? getEdgeEnglishVoice() : getEdgeVoice();
}

function isBotEnabled() {
  return runtimeConfig.botEnabled !== false;
}

function isListeningPaused(session = null) {
  return runtimeConfig.listeningPaused === true || session?.paused === true;
}

function silentOptions(content, extra = {}) {
  return {
    content,
    allowedMentions: { parse: [] },
    flags: SILENT_MESSAGES ? MessageFlags.SuppressNotifications : undefined,
    ...extra,
  };
}

async function sendText(channel, content) {
  try {
    return await channel.send(silentOptions(content));
  } catch (error) {
    console.error('channel.send failed:', error);
  }
}

function setMonitorChannel(channel) {
  if (channel?.send) monitorChannel = channel;
}

async function sendMonitorNotice(content, channel = monitorChannel) {
  if (channel?.send) {
    await sendText(channel, content);
  } else {
    console.warn(content);
  }
}

async function reply(interaction, content, extra = {}) {
  const payload = silentOptions(content, extra);
  try {
    if (interaction.deferred || interaction.replied) {
      return await interaction.editReply(payload);
    }
    return await interaction.reply(payload);
  } catch (error) {
    console.error('interaction reply failed:', error);
    if (interaction.channel) return sendText(interaction.channel, content);
  }
}

function getHeader(headers, name) {
  if (!headers) return null;
  if (typeof headers.get === 'function') return headers.get(name);
  return headers[name] ?? headers[name.toLowerCase()] ?? null;
}

function parseHeaderNumber(value) {
  if (value === null || value === undefined) return null;
  const parsed = Number(String(value).split(',')[0].trim());
  return Number.isFinite(parsed) ? parsed : null;
}

function getRateLimitHeaders(source) {
  return source?.headers || source;
}

function formatPercent(value) {
  if (!Number.isFinite(value)) return 'unknown';
  return value < 10 ? value.toFixed(1) : value.toFixed(0);
}

async function maybeAlertGroqLimit(channel, label, metric, limit, remaining, reset) {
  if (!Number.isFinite(limit) || !Number.isFinite(remaining) || limit <= 0) return;

  const percent = remaining / limit * 100;
  const threshold = API_LIMIT_ALERT_THRESHOLDS
    .filter((item) => percent <= item)
    .at(-1);
  const key = `${metric}`;
  const current = groqLimitAlertState.get(key) || { threshold: null, reset: null };

  if (current.reset !== reset || percent > 55) {
    current.threshold = null;
    current.reset = reset;
  }

  if (threshold && (current.threshold === null || threshold < current.threshold)) {
    current.threshold = threshold;
    current.reset = reset;
    groqLimitAlertState.set(key, current);
    await sendMonitorNotice(
      `⚠️ Groq API: лимит ${metric} для ${label} ниже ${threshold}%. Осталось ${remaining}/${limit} (${formatPercent(percent)}%). Сброс: ${reset || 'неизвестно'}.`,
      channel,
    );
  } else {
    groqLimitAlertState.set(key, current);
  }
}

function trackGroqRateLimits(channel, label, source, model = 'unknown') {
  const headers = getRateLimitHeaders(source);
  if (!headers) return;

  const metrics = [
    {
      name: 'requests',
      limit: parseHeaderNumber(getHeader(headers, 'x-ratelimit-limit-requests')),
      remaining: parseHeaderNumber(getHeader(headers, 'x-ratelimit-remaining-requests')),
      reset: getHeader(headers, 'x-ratelimit-reset-requests'),
    },
    {
      name: 'tokens',
      limit: parseHeaderNumber(getHeader(headers, 'x-ratelimit-limit-tokens')),
      remaining: parseHeaderNumber(getHeader(headers, 'x-ratelimit-remaining-tokens')),
      reset: getHeader(headers, 'x-ratelimit-reset-tokens'),
    },
  ];

  for (const metric of metrics) {
    if (!Number.isFinite(metric.limit) || !Number.isFinite(metric.remaining)) continue;
    const key = `${model}:${metric.name}`;
    groqLastLimits.set(key, { ...metric, label, model, checkedAt: Date.now() });
    void maybeAlertGroqLimit(channel || monitorChannel, `${model} / ${label}`, metric.name, metric.limit, metric.remaining, metric.reset)
      .catch((error) => console.error('Groq limit alert failed:', error));
  }
}

function formatGroqLimits() {
  if (!groqLastLimits.size) {
    return 'Пока нет данных по лимитам Groq. Они появятся после первого запроса к STT или chat model.';
  }

  return [...groqLastLimits.values()]
    .map((metric) => {
      const percent = metric.limit > 0 ? metric.remaining / metric.limit * 100 : NaN;
      const checked = new Date(metric.checkedAt).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
      return `${metric.model || 'unknown'} ${metric.name}: ${metric.remaining}/${metric.limit} (${formatPercent(percent)}%), reset=${metric.reset || 'unknown'}, source=${metric.label}, checked=${checked}`;
    })
    .join('\n');
}

function formatSessionStatus(session) {
  if (!session?.connection) return 'Не подключен к voice channel.';
  const diag = session.diagnostics || createVoiceDiagnostics();
  const idleSeconds = session.lastHumanActivityAt ? Math.round((Date.now() - session.lastHumanActivityAt) / 1000) : 0;
  const assistantIdleSeconds = Math.round((Date.now() - (session.lastAssistantInteractionAt || session.joinedAt || Date.now())) / 1000);
  const activeLeft = session.activeDialogueUntil ? Math.max(0, Math.round((session.activeDialogueUntil - Date.now()) / 1000)) : 0;
  return `Voice: ${session.voiceChannel?.name || 'unknown'}, state=${session.connection.state.status}, assistant=${getAssistantName()}, trigger="${getWakeWord() || 'off'}", enabled=${isBotEnabled()}, paused=${isListeningPaused(session)}, persona=${getAssistantPersona()}, activeDialogue=${activeLeft}s, webSearch=${isWebSearchEnabled()}, idleChatter=${isIdleChatterEnabled()} every ${getIdleChatterMinutes()}m style=${getIdleChatterStyle()} web=${isIdleChatterWebEnabled()}, idleLeave=${isIdleLeaveEnabled()} after ${getIdleLeaveMinutes()}m, humanIdle=${idleSeconds}s, assistantIdle=${assistantIdleSeconds}s, busy=${Boolean(session.busy)}, activeCaptures=${session.activeUsers?.size || 0}, history=${session.history?.length || 0}, voiceEvents=${diag.voiceEvents}, captures=${diag.captures}, ignored=${diag.ignored}, lastIgnored=${diag.lastIgnoredReason || 'none'}, lastTranscript=${diag.lastTranscript || 'none'}.`;
}

function escapeRegExp(text) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function leadingToken(text) {
  const match = /^(\s*)([\p{L}\p{N}_-]{1,20})/iu.exec(text || '');
  if (!match) return null;
  return {
    index: match[1].length,
    length: match[2].length,
    raw: match[2],
    normalized: normalizeCommandText(match[2]),
  };
}

function isWakeLikeToken(token) {
  const normalizedWake = normalizeCommandText(getWakeWord());
  if (!token || !normalizedWake) return false;
  if (token === normalizedWake) return true;
  const aliases = getWakeAliases().map((alias) => normalizeCommandText(alias)).filter(Boolean);
  if (aliases.some((alias) => alias === token)) return true;
  if (!isWakeFuzzyEnabled()) return false;

  if (normalizedWake === 'бот') {
    const knownBotVariants = new Set([
      'бот', 'вот', 'от', 'робот', 'роботик', 'ботик',
      'бота', 'боту', 'боте', 'боты', 'ботом', 'ботам',
      'бод', 'бат', 'борт', 'вод', 'бо', 'ботт',
    ]);
    if (knownBotVariants.has(token)) return true;
    if (/^бот[\p{L}]{0,3}$/u.test(token)) return true;
    if (/^робот[\p{L}]{0,3}$/u.test(token)) return true;
  }

  const compactToken = compactText(token);
  if (compactToken.length < 2 || compactToken.length > 18) return false;

  const candidates = [normalizedWake, ...aliases]
    .map((item) => compactText(item))
    .filter((item, index, list) => item && list.indexOf(item) === index);
  for (const candidate of candidates) {
    const distance = levenshteinDistance(compactToken, candidate);
    const maxDistance = candidate.length <= 4 ? 1 : candidate.length <= 8 ? 2 : 3;
    const similarEnough = similarity(compactToken, candidate) >= (candidate.length <= 4 ? 0.58 : 0.68);
    const firstLetterClose = compactToken[0] === candidate[0] || distance <= 1;
    if (distance <= maxDistance && similarEnough && firstLetterClose) return true;
  }
  return false;
}

function findWakeWord(text) {
  const wakeWord = getWakeWord();
  const pattern = wakeWordPattern();
  if (!pattern || !wakeWord) return null;
  const match = pattern.exec(text);
  if (match) {
    return {
      index: match.index + (match[1]?.length || 0),
      length: wakeWord.length,
    };
  }

  const firstToken = leadingToken(text);
  if (firstToken && isWakeLikeToken(firstToken.normalized)) {
    return {
      index: firstToken.index,
      length: firstToken.length,
    };
  }

  for (const alias of getWakeAliases()) {
    const aliasMatch = new RegExp(`^(\\s*)${escapeRegExp(alias)}(?=$|[^\\p{L}\\p{N}_])`, 'iu').exec(text);
    if (aliasMatch) {
      return {
        index: aliasMatch[1]?.length || 0,
        length: alias.length,
      };
    }
  }

  return null;
}

function hasWakeWord(text) {
  if (!getWakeWord()) return true;
  return Boolean(findWakeWord(text));
}

function isActiveDialogue(session) {
  return Boolean(
    isActiveDialogueEnabled()
      && session?.activeDialogueUntil
      && Date.now() < session.activeDialogueUntil,
  );
}

function markActiveDialogue(session) {
  if (!session || !isActiveDialogueEnabled()) return;
  session.activeDialogueUntil = Date.now() + getActiveDialogueSeconds() * 1000;
}

function markAssistantInteraction(session, source = 'voice') {
  if (!session) return;
  session.lastAssistantInteractionAt = Date.now();
  session.lastAssistantInteractionSource = source;
  autoJoinSuppressedUntilManualJoin = false;
}

function shouldAnswer(text, session = null) {
  if (LISTEN_WITHOUT_WAKE_WORD || !getWakeWord()) return true;
  return hasWakeWord(text) || isActiveDialogue(session);
}

function stripWakeWord(text) {
  if (!getWakeWord()) return text.trim();
  const wake = findWakeWord(text);
  if (!wake) return text.trim();
  return text.slice(wake.index + wake.length).trim().replace(/^[,!.?:;\s-]+/, '');
}

function promptFromTranscript(session, transcript) {
  return hasWakeWord(transcript) ? stripWakeWord(transcript) : String(transcript || '').trim();
}

function normalizeCommandText(text) {
  return text
    .toLowerCase()
    .replaceAll('ё', 'е')
    .replace(/[^\p{L}\p{N}\s_-]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function canMoveMembers(member) {
  return Boolean(
    member?.permissions?.has(PermissionFlagsBits.Administrator)
      || member?.permissions?.has(PermissionFlagsBits.MoveMembers),
  );
}

function canUsePermission(member, permission) {
  return Boolean(
    member?.permissions?.has(PermissionFlagsBits.Administrator)
      || member?.permissions?.has(permission),
  );
}

function candidateMemberNames(member) {
  return [
    member.displayName,
    member.nickname,
    member.user?.globalName,
    member.user?.username,
    member.user?.tag,
    member.id,
  ]
    .filter(Boolean)
    .map((name) => normalizeCommandText(name));
}

function compactText(text) {
  return normalizeCommandText(text).replace(/\s+/g, '');
}

function levenshteinDistance(a, b) {
  if (a === b) return 0;
  if (!a.length) return b.length;
  if (!b.length) return a.length;

  let previous = Array.from({ length: b.length + 1 }, (_, index) => index);
  let current = new Array(b.length + 1);

  for (let i = 1; i <= a.length; i += 1) {
    current[0] = i;
    for (let j = 1; j <= b.length; j += 1) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      current[j] = Math.min(
        current[j - 1] + 1,
        previous[j] + 1,
        previous[j - 1] + cost,
      );
    }
    [previous, current] = [current, previous];
  }

  return previous[b.length];
}

function similarity(a, b) {
  const left = compactText(a);
  const right = compactText(b);
  if (!left || !right) return 0;
  if (left === right) return 1;
  if (left.includes(right) || right.includes(left)) {
    return Math.min(left.length, right.length) / Math.max(left.length, right.length) * 0.92;
  }

  const distance = levenshteinDistance(left, right);
  const base = 1 - distance / Math.max(left.length, right.length);
  const leftTokens = normalizeCommandText(a).split(' ').filter(Boolean);
  const rightTokens = normalizeCommandText(b).split(' ').filter(Boolean);
  const tokenHits = rightTokens.filter((token) => leftTokens.some((item) => item.includes(token) || token.includes(item))).length;
  const tokenScore = rightTokens.length ? tokenHits / rightTokens.length : 0;

  return Math.max(base, tokenScore * 0.85);
}

function searchTokens(text) {
  return normalizeCommandText(text)
    .split(' ')
    .map((token) => token.trim())
    .filter((token) => token.length >= 3);
}

function scoreTextRelevance(text, prompt) {
  const promptTokens = new Set(searchTokens(prompt));
  if (!promptTokens.size) return 0;
  const textTokens = new Set(searchTokens(text));
  const textTokenList = [...textTokens];
  let hits = 0;
  for (const token of promptTokens) {
    if (textTokens.has(token) || textTokenList.some((item) => item.includes(token) || token.includes(item))) {
      hits += 1;
    }
  }
  return hits / promptTokens.size;
}

function addMemoryItem(guildId, actorMember, text) {
  const guildState = getGuildState(guildId);
  const item = {
    id: createId('mem'),
    text: text.trim().slice(0, 1200),
    userId: actorMember?.id || null,
    userName: actorMember?.displayName || actorMember?.user?.username || null,
    createdAt: Date.now(),
  };
  guildState.memories.push(item);
  trimStoredItems(guildState);
  void saveStateStore();
  return item;
}

function addUserMemoryItem(guildId, actorMember, text) {
  const guildState = getGuildState(guildId);
  const userId = actorMember?.id || 'unknown';
  if (!Array.isArray(guildState.userMemories[userId])) guildState.userMemories[userId] = [];
  const item = {
    id: createId('umem'),
    text: text.trim().slice(0, 1200),
    userId,
    userName: actorMember?.displayName || actorMember?.user?.username || null,
    createdAt: Date.now(),
  };
  guildState.userMemories[userId].push(item);
  trimStoredItems(guildState);
  void saveStateStore();
  return item;
}

function clearMemoryItems(guildId) {
  const guildState = getGuildState(guildId);
  const userCount = Object.values(guildState.userMemories || {})
    .reduce((sum, memories) => sum + (Array.isArray(memories) ? memories.length : 0), 0);
  const count = guildState.memories.length + userCount;
  guildState.memories = [];
  guildState.userMemories = {};
  void saveStateStore();
  return count;
}

function relevantMemories(guildId, prompt, limit = MEMORY_CONTEXT_LIMIT) {
  if (!limit) return [];
  const memories = [...getGuildState(guildId).memories];
  if (!memories.length) return [];

  const scored = memories.map((memory, index) => ({
    memory,
    score: scoreTextRelevance(memory.text, prompt) + index / Math.max(1, memories.length) * 0.05,
  }));
  scored.sort((a, b) => b.score - a.score);

  const relevant = scored.filter((item) => item.score > 0.05).slice(0, limit);
  if (relevant.length) return relevant.map((item) => item.memory);
  return memories.slice(-Math.min(limit, 5));
}

function relevantUserMemories(guildId, userId, prompt, limit = MEMORY_CONTEXT_LIMIT) {
  if (!limit || !userId) return [];
  const memories = [...(getGuildState(guildId).userMemories?.[userId] || [])];
  if (!memories.length) return [];

  const scored = memories.map((memory, index) => ({
    memory,
    score: scoreTextRelevance(memory.text, prompt) + index / Math.max(1, memories.length) * 0.05,
  }));
  scored.sort((a, b) => b.score - a.score);

  const relevant = scored.filter((item) => item.score > 0.05).slice(0, limit);
  if (relevant.length) return relevant.map((item) => item.memory);
  return memories.slice(-Math.min(limit, 5));
}

function formatMemoryContext(guildId, prompt, userId = null) {
  const userMemories = relevantUserMemories(guildId, userId, prompt, Math.ceil(MEMORY_CONTEXT_LIMIT / 2));
  const memories = relevantMemories(guildId, prompt);
  if (!userMemories.length && !memories.length) return '';
  const lines = [];
  if (userMemories.length) {
    lines.push('Персональная память текущего пользователя:');
    lines.push(...userMemories.map((memory, index) => `${index + 1}. ${memory.text}`));
  }
  if (memories.length) {
    lines.push('Общая память сервера:');
    lines.push(...memories
    .map((memory, index) => {
      const author = memory.userName ? `${memory.userName}: ` : '';
      return `${index + 1}. ${author}${memory.text}`;
    }));
  }
  return lines.join('\n');
}

function formatMemoryList(guildId, userId = null) {
  const memories = getGuildState(guildId).memories.slice(-10);
  const userMemories = userId ? (getGuildState(guildId).userMemories?.[userId] || []).slice(-10) : [];
  if (!memories.length && !userMemories.length) return 'Память пока пустая.';
  const sections = [];
  if (userMemories.length) {
    sections.push('Персонально о тебе:');
    sections.push(...userMemories.map((memory, index) => {
      const date = new Date(memory.createdAt).toLocaleString('ru-RU', { dateStyle: 'short', timeStyle: 'short' });
      return `${index + 1}. ${memory.text} (${date})`;
    }));
  }
  if (memories.length) {
    sections.push('Общая память сервера:');
    sections.push(...memories
    .map((memory, index) => {
      const date = new Date(memory.createdAt).toLocaleString('ru-RU', { dateStyle: 'short', timeStyle: 'short' });
      const author = memory.userName ? `${memory.userName}: ` : '';
      return `${index + 1}. ${author}${memory.text} (${date})`;
    }));
  }
  return sections.join('\n');
}

function parseAmount(value) {
  const normalized = normalizeCommandText(value);
  const direct = Number(normalized.replace(',', '.'));
  if (Number.isFinite(direct) && direct > 0) return direct;

  const words = new Map([
    ['один', 1], ['одну', 1], ['одна', 1], ['раз', 1],
    ['два', 2], ['две', 2],
    ['три', 3], ['четыре', 4], ['пять', 5], ['шесть', 6], ['семь', 7],
    ['восемь', 8], ['девять', 9], ['десять', 10], ['пятнадцать', 15],
    ['двадцать', 20], ['тридцать', 30], ['сорок', 40], ['пятьдесят', 50],
    ['шестьдесят', 60],
  ]);
  return words.get(normalized) || null;
}

function unitToMs(unit) {
  const normalized = normalizeCommandText(unit);
  if (/^сек/.test(normalized)) return 1000;
  if (/^мин/.test(normalized)) return 60 * 1000;
  if (/^час/.test(normalized)) return 60 * 60 * 1000;
  if (/^(день|дня|днеи|сут)/.test(normalized)) return 24 * 60 * 60 * 1000;
  return null;
}

function recurringUnitToMs(unit) {
  const normalized = normalizeCommandText(unit);
  if (/^час/.test(normalized)) return 60 * 60 * 1000;
  if (/^(день|дня|днеи|сут)/.test(normalized)) return 24 * 60 * 60 * 1000;
  if (/^недел/.test(normalized)) return 7 * 24 * 60 * 60 * 1000;
  if (/^месяц/.test(normalized)) return 30 * 24 * 60 * 60 * 1000;
  return unitToMs(unit);
}

function cleanReminderText(text) {
  return String(text || '')
    .replace(/^(?:что\s+|о том что\s+|про\s+|[:,-]\s*)/iu, '')
    .trim();
}

function parseReminderCommand(prompt) {
  const text = String(prompt || '').trim();
  const recurringInterval = text.match(/(?:^|\s)(?:напоминай|напомни)(?:\s+мне)?\s+кажд(?:ые|ый|ую|ое)\s+(\d+(?:[.,]\d+)?|[а-яё]+)?\s*(секунд[уы]?|сек|минут[уы]?|мин|час(?:а|ов)?|день|дня|дней|сут(?:ки|ок)?|недел[юияь]*|месяц(?:а|ев)?)\s*(.*)$/iu);
  if (recurringInterval) {
    const amount = recurringInterval[1] ? parseAmount(recurringInterval[1]) : 1;
    const unit = recurringInterval[2];
    const intervalMs = amount ? Math.round(amount * recurringUnitToMs(unit)) : 0;
    const reminderText = cleanReminderText(recurringInterval[3]);
    if (!intervalMs) return { error: 'Не понял период. Пример: “бот напоминай каждые 2 часа размяться”.' };
    if (!reminderText) return { error: 'Что именно повторять?' };
    return {
      dueAt: Date.now() + intervalMs,
      text: reminderText.slice(0, 1000),
      repeatIntervalMs: intervalMs,
      repeatLabel: `каждые ${amount || 1} ${unit}`,
    };
  }

  const recurringDay = text.match(/(?:^|\s)(?:напоминай|напомни)(?:\s+мне)?\s+кажд(?:ый|ое)\s+день\s*(.*)$/iu);
  if (recurringDay) {
    const reminderText = cleanReminderText(recurringDay[1]);
    if (!reminderText) return { error: 'Что именно повторять каждый день?' };
    return {
      dueAt: Date.now() + 24 * 60 * 60 * 1000,
      text: reminderText.slice(0, 1000),
      repeatIntervalMs: 24 * 60 * 60 * 1000,
      repeatLabel: 'каждый день',
    };
  }

  const match = text.match(/(?:^|\s)напомни(?:\s+мне)?\s+через\s+(.+)$/iu);
  if (!match) return null;

  const tail = match[1].trim();
  const withAmount = tail.match(/^(\d+(?:[.,]\d+)?|[а-яё]+)\s*(секунд[уы]?|сек|минут[уы]?|мин|час(?:а|ов)?|день|дня|дней|сут(?:ки|ок)?)\s*(.*)$/iu);
  const withoutAmount = tail.match(/^(секунду|минуту|час|день|сутки)\s*(.*)$/iu);

  let amount = null;
  let unit = '';
  let reminderText = '';
  if (withAmount) {
    amount = parseAmount(withAmount[1]);
    unit = withAmount[2];
    reminderText = withAmount[3] || '';
  } else if (withoutAmount) {
    amount = 1;
    unit = withoutAmount[1];
    reminderText = withoutAmount[2] || '';
  } else {
    return { error: 'Не понял время. Пример: “бот напомни через 5 минут проверить чай”.' };
  }

  const unitMs = unitToMs(unit);
  if (!amount || !unitMs) {
    return { error: 'Не понял время. Пример: “бот напомни через 5 минут проверить чай”.' };
  }

  reminderText = cleanReminderText(reminderText);
  if (!reminderText) return { error: 'Что именно напомнить?' };

  return {
    dueAt: Date.now() + Math.round(amount * unitMs),
    text: reminderText.slice(0, 1000),
  };
}

function formatDueTime(dueAt) {
  const delayMs = Math.max(0, dueAt - Date.now());
  const minutes = Math.round(delayMs / 60000);
  if (minutes < 1) return 'меньше чем через минуту';
  if (minutes < 60) return `через ${minutes} мин.`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `через ${hours} ч.`;
  return new Date(dueAt).toLocaleString('ru-RU', { dateStyle: 'short', timeStyle: 'short' });
}

function addReminderItem(session, actorMember, text, dueAt, options = {}) {
  const guildState = getGuildState(session.guild.id);
  const reminderVoiceChannel = session.voiceChannel || actorMember?.voice?.channel || null;
  const item = {
    id: createId('rem'),
    guildId: session.guild.id,
    channelId: session.textChannel.id,
    voiceChannelId: reminderVoiceChannel?.id || actorMember?.voice?.channelId || null,
    voiceChannelName: reminderVoiceChannel?.name || null,
    userId: actorMember?.id || null,
    userName: actorMember?.displayName || actorMember?.user?.username || null,
    text: text.trim().slice(0, 1000),
    dueAt,
    repeatIntervalMs: Number(options.repeatIntervalMs || 0) || null,
    repeatLabel: options.repeatLabel || null,
    createdAt: Date.now(),
  };
  guildState.reminders.push(item);
  trimStoredItems(guildState);
  void saveStateStore();
  scheduleReminder(item);
  return item;
}

function removeReminderItem(reminder) {
  const guildState = getGuildState(reminder.guildId);
  guildState.reminders = guildState.reminders.filter((item) => item.id !== reminder.id);
  const timer = reminderTimers.get(reminder.id);
  if (timer) clearTimeout(timer);
  reminderTimers.delete(reminder.id);
  void saveStateStore();
}

function scheduleReminder(reminder) {
  const existing = reminderTimers.get(reminder.id);
  if (existing) clearTimeout(existing);

  const delay = Math.max(0, reminder.dueAt - Date.now());
  const timer = setTimeout(() => {
    reminderTimers.delete(reminder.id);
    if (delay > MAX_REMINDER_TIMEOUT_MS) {
      scheduleReminder(reminder);
    } else {
      void deliverReminder(reminder);
    }
  }, Math.min(delay, MAX_REMINDER_TIMEOUT_MS));
  reminderTimers.set(reminder.id, timer);
}

async function deliverReminder(reminder) {
  try {
    const channel = await client.channels.fetch(reminder.channelId).catch(() => null);
    const mention = reminder.userId ? `<@${reminder.userId}>` : (reminder.userName || '');
    const content = `⏰ ${mention ? `${mention}, ` : ''}напоминание: ${reminder.text}`;
    if (channel?.send) await sendText(channel, content);
    appendEvent('reminder_delivered', {
      guildId: reminder.guildId,
      voiceChannelId: reminder.voiceChannelId,
      userId: reminder.userId,
      text: reminder.text,
      repeatLabel: reminder.repeatLabel,
    });

    const session = findReminderSession(reminder);
    const canSpeakInCurrentVoice = session?.connection
      && session.connection.state.status !== VoiceConnectionStatus.Destroyed
      && (!reminder.voiceChannelId || session.voiceChannel?.id === reminder.voiceChannelId);
    if (canSpeakInCurrentVoice) {
      await speak(session, `Напоминание: ${reminder.text}`).catch((error) => console.error('reminder speak failed:', error));
    } else if (reminder.voiceChannelId && session?.voiceChannel?.id !== reminder.voiceChannelId) {
      console.log(`reminder voice skipped: reminder channel=${reminder.voiceChannelId}, current=${session?.voiceChannel?.id || 'none'}`);
    }
  } catch (error) {
    console.error('deliver reminder failed:', reminder, error);
    appendEvent('reminder_error', { id: reminder.id, message: error.message || String(error) });
  } finally {
    if (!rescheduleRecurringReminder(reminder)) removeReminderItem(reminder);
  }
}

function rescheduleRecurringReminder(reminder) {
  const guildState = getGuildState(reminder.guildId);
  const stored = guildState.reminders.find((item) => item.id === reminder.id);
  const intervalMs = Number(stored?.repeatIntervalMs || reminder.repeatIntervalMs || 0);
  if (!stored || !intervalMs) return false;

  let nextDueAt = Number(stored.dueAt || reminder.dueAt || Date.now()) + intervalMs;
  while (nextDueAt <= Date.now()) nextDueAt += intervalMs;
  stored.dueAt = nextDueAt;
  stored.lastDeliveredAt = Date.now();
  void saveStateStore();
  scheduleReminder(stored);
  appendEvent('reminder_rescheduled', {
    guildId: stored.guildId,
    voiceChannelId: stored.voiceChannelId,
    text: stored.text,
    dueAt: stored.dueAt,
    repeatLabel: stored.repeatLabel,
  });
  return true;
}

function schedulePendingReminders() {
  for (const [guildId, guildState] of Object.entries(stateStore.guilds)) {
    if (!Array.isArray(guildState.reminders)) continue;
    for (const reminder of guildState.reminders) {
      reminder.guildId ||= guildId;
      scheduleReminder(reminder);
    }
  }
}

function formatReminderList(guildId) {
  const reminders = getGuildState(guildId).reminders
    .slice()
    .sort((a, b) => a.dueAt - b.dueAt)
    .slice(0, 10);
  if (!reminders.length) return 'Активных напоминаний нет.';
  return reminders
    .map((reminder, index) => formatReminderChoice(reminder, index))
    .join('\n');
}

function formatReminderChoice(reminder, index = 0) {
  const created = reminder.createdAt
    ? new Date(reminder.createdAt).toLocaleString('ru-RU', { dateStyle: 'short', timeStyle: 'short' })
    : 'дата неизвестна';
  const author = reminder.userName ? `, записал ${reminder.userName}` : '';
  const repeat = reminder.repeatIntervalMs ? `, повтор: ${reminder.repeatLabel || 'включен'}` : '';
  return `${index + 1}. ${formatDueTime(reminder.dueAt)}: ${reminder.text} (создано ${created}${author}${repeat})`;
}

function parseSelectionNumber(prompt) {
  const normalized = normalizeCommandText(prompt);
  const direct = normalized.match(/(?:^|\s)(\d{1,2})(?:\s|$)/u);
  if (direct) return Number(direct[1]);

  const ordinals = [
    ['перв', 1],
    ['втор', 2],
    ['трет', 3],
    ['четверт', 4],
    ['пят', 5],
    ['шест', 6],
    ['седьм', 7],
    ['восьм', 8],
    ['девят', 9],
    ['десят', 10],
  ];
  for (const [prefix, value] of ordinals) {
    if (normalized.split(' ').some((token) => token.startsWith(prefix))) return value;
  }
  return null;
}

function isPositiveConfirmation(prompt) {
  const normalized = normalizeCommandText(prompt);
  return /^(да|ага|угу|ок|окей|yes|yep|yeah|подтверждаю|удаляй|удали|можно|верно|правильно)$/u.test(normalized)
    || normalized.includes('да удал')
    || normalized.includes('подтверждаю');
}

function isNegativeConfirmation(prompt) {
  const normalized = normalizeCommandText(prompt);
  return /^(нет|неа|no|nope|отмена|cancel|стой|не надо|не удаляй)$/u.test(normalized)
    || normalized.includes('не удал')
    || normalized.includes('отмени');
}

function isSameLocalDay(timestamp, offsetDays = 0) {
  const target = new Date();
  target.setDate(target.getDate() + offsetDays);
  const date = new Date(timestamp);
  return date.getFullYear() === target.getFullYear()
    && date.getMonth() === target.getMonth()
    && date.getDate() === target.getDate();
}

function reminderDateScore(reminder, normalizedQuery) {
  let score = 0;
  const asksCreatedAt = normalizedQuery.includes('запис')
    || normalizedQuery.includes('созда')
    || normalizedQuery.includes('добав');
  const checkTimestamp = asksCreatedAt ? reminder.createdAt : reminder.dueAt;

  if (normalizedQuery.includes('сегодня') || normalizedQuery.includes('сегодняш')) {
    if (isSameLocalDay(checkTimestamp, 0)) score += 0.55;
  }
  if (normalizedQuery.includes('завтра') || normalizedQuery.includes('завтраш')) {
    if (isSameLocalDay(checkTimestamp, 1)) score += 0.55;
  }
  if (normalizedQuery.includes('вчера') || normalizedQuery.includes('вчераш')) {
    if (isSameLocalDay(reminder.createdAt, -1) || isSameLocalDay(reminder.dueAt, -1)) score += 0.55;
  }
  if (normalizedQuery.includes('позавчера')) {
    if (isSameLocalDay(reminder.createdAt, -2)) score += 0.55;
  }

  return score;
}

function reminderSearchText(reminder) {
  const dueDate = new Date(reminder.dueAt).toLocaleString('ru-RU', { dateStyle: 'full', timeStyle: 'short' });
  const createdDate = reminder.createdAt
    ? new Date(reminder.createdAt).toLocaleString('ru-RU', { dateStyle: 'full', timeStyle: 'short' })
    : '';
  return [
    reminder.text,
    reminder.userName,
    formatDueTime(reminder.dueAt),
    dueDate,
    createdDate,
    'напоминание',
  ].filter(Boolean).join(' ');
}

function findReminderMatches(guildId, query) {
  const reminders = getGuildState(guildId).reminders
    .slice()
    .sort((a, b) => a.dueAt - b.dueAt);
  const normalizedQuery = normalizeCommandText(query);
  if (!reminders.length) return [];
  if (!normalizedQuery) return reminders.map((reminder, index) => ({ reminder, score: 0.1, index }));

  const scored = reminders.map((reminder, index) => {
    const textScore = scoreTextRelevance(reminderSearchText(reminder), normalizedQuery);
    const directTextScore = scoreTextRelevance(reminder.text, normalizedQuery) * 0.8;
    const fuzzyTextScore = similarity(reminder.text, normalizedQuery) * 0.35;
    const dateScore = reminderDateScore(reminder, normalizedQuery);
    return {
      reminder,
      index,
      score: Math.max(textScore, directTextScore, fuzzyTextScore) + dateScore,
    };
  });

  return scored
    .filter((item) => item.score >= 0.18)
    .sort((a, b) => b.score - a.score);
}

function removeReminderItemsByIds(guildId, ids) {
  const idSet = new Set(ids);
  const guildState = getGuildState(guildId);
  const removed = [];
  guildState.reminders = guildState.reminders.filter((reminder) => {
    if (!idSet.has(reminder.id)) return true;
    removed.push(reminder);
    const timer = reminderTimers.get(reminder.id);
    if (timer) clearTimeout(timer);
    reminderTimers.delete(reminder.id);
    return false;
  });
  if (removed.length) void saveStateStore();
  return removed;
}

function parseDeleteReminderCommand(prompt) {
  const raw = String(prompt || '').trim();
  const normalized = normalizeCommandText(raw);
  if (!normalized.includes('напомин')) return null;
  if (!/(удал|убер|убери|отмен|отмени|сотри|стери|забудь|delete|remove|cancel)/u.test(normalized)) {
    return null;
  }

  if (
    normalized.includes('все напомин')
    || normalized.includes('все мои напомин')
    || normalized.includes('очисти напомин')
    || normalized.includes('сбрось напомин')
    || normalized === 'отмени напоминания'
    || normalized === 'удали напоминания'
  ) {
    return { action: 'clear_reminders' };
  }

  let query = raw
    .replace(/^(?:пожалуйста\s+)?(?:удали|убери|отмени|сотри|стереть|стери|забудь|delete|remove|cancel)\s+(?:мне\s+|мое\s+|моё\s+|мои\s+)?(?:напоминани[еяй]|напоминалк[ауи]?)/iu, '')
    .replace(/^(?:напоминани[еяй]|напоминалк[ауи]?)\s+(?:удали|убери|отмени|сотри|стери|забудь|delete|remove|cancel)/iu, '')
    .replace(/^(?:о|об|про|по|за|там|то|котор(?:ое|ые|ый|ую)|которые|что|где|я|мне)\s+/iu, '')
    .trim();

  if (!query) {
    const number = parseSelectionNumber(raw);
    if (number) query = String(number);
  }

  return { action: 'delete_reminder', text: query.slice(0, 500) };
}

function clearReminderItems(guildId) {
  const guildState = getGuildState(guildId);
  const count = guildState.reminders.length;
  for (const reminder of guildState.reminders) {
    const timer = reminderTimers.get(reminder.id);
    if (timer) clearTimeout(timer);
    reminderTimers.delete(reminder.id);
  }
  guildState.reminders = [];
  void saveStateStore();
  return count;
}

function publicRuntimeConfig() {
  return {
    botEnabled: isBotEnabled(),
    listeningPaused: runtimeConfig.listeningPaused === true,
    assistantName: getAssistantName(),
    wakeWord: getWakeWord(),
    wakeAliases: getWakeAliases().join(', '),
    wakeAliasList: getWakeAliases(),
    wakeFuzzy: isWakeFuzzyEnabled(),
    groqApiKeySet: Boolean(effectiveGroqApiKey()),
    groqChatModel: getChatModel(),
    groqSttModel: getSttModel(),
    webSearchEnabled: isWebSearchEnabled(),
    webSearchModel: getWebSearchModel(),
    idleChatterEnabled: isIdleChatterEnabled(),
    idleChatterMinutes: getIdleChatterMinutes(),
    idleChatterUseWeb: isIdleChatterWebEnabled(),
    idleChatterStyle: getIdleChatterStyle(),
    idleLeaveEnabled: isIdleLeaveEnabled(),
    idleLeaveMinutes: getIdleLeaveMinutes(),
    idleLeavePhrase: getIdleLeavePhrase(),
    activeDialogueEnabled: isActiveDialogueEnabled(),
    activeDialogueSeconds: getActiveDialogueSeconds(),
    confirmDangerousActions: shouldConfirmDangerousActions(),
    assistantPersona: getAssistantPersona(),
    healthcheckEnabled: isHealthcheckEnabled(),
    presenceAnnouncementsEnabled: PRESENCE_ANNOUNCEMENTS_ENABLED,
    presenceAnnouncementCooldownMs: PRESENCE_ANNOUNCEMENT_COOLDOWN_MS,
    sttLanguage: getSttLanguage(),
    ttsProvider: getTtsProvider(),
    macosVoice: getMacosVoice(),
    espeakVoice: getEspeakVoice(),
    espeakSpeed: getEspeakSpeed(),
    edgeVoice: getEdgeVoice(),
    edgeEnglishVoice: getEdgeEnglishVoice(),
    edgeRate: getEdgeRate(),
    edgePitch: getEdgePitch(),
    updatedAt: runtimeConfig.updatedAt || null,
  };
}

function summarizeSessions() {
  return [...sessions.entries()].map(([guildId, session]) => {
    cleanupStaleActiveCaptures(session);
    const voiceMembers = getCurrentVoiceMembers(session);
    return {
      guildId,
      sessionKey: session.sessionKey || guildId,
      guildName: session.guild?.name || null,
      textChannelId: session.textChannel?.id || null,
      textChannelName: session.textChannel?.name || null,
      voiceChannelId: session.voiceChannel?.id || null,
      voiceChannelName: session.voiceChannel?.name || null,
      connectionState: session.connection?.state?.status || 'none',
      paused: isListeningPaused(session),
      busy: Boolean(session.busy),
      activeCaptures: session.activeUsers?.size || 0,
      voiceMembers: voiceMembers.length,
      humanVoiceMembers: voiceMembers.filter((member) => !member.user.bot).length,
      historyItems: session.history?.length || 0,
      activeDialogueUntil: session.activeDialogueUntil || null,
      lastHumanActivityAt: session.lastHumanActivityAt || null,
      lastAssistantInteractionAt: session.lastAssistantInteractionAt || null,
      lastAssistantInteractionSource: session.lastAssistantInteractionSource || null,
      idleLeaveDueAt: isIdleLeaveEnabled()
        ? (session.lastAssistantInteractionAt || session.joinedAt || Date.now()) + getIdleLeaveMinutes() * 60_000
        : null,
      lastIdleChatterAt: session.lastIdleChatterAt || null,
      diagnostics: session.diagnostics || createVoiceDiagnostics(),
    };
  });
}

function memoryStats() {
  const guilds = Object.entries(stateStore.guilds || {});
  const userMemories = (guildState) => Object.values(guildState.userMemories || {})
    .reduce((sum, memories) => sum + (Array.isArray(memories) ? memories.length : 0), 0);
  return {
    guilds: guilds.length,
    memories: guilds.reduce((sum, [, guildState]) => sum + (guildState.memories?.length || 0) + userMemories(guildState), 0),
    reminders: guilds.reduce((sum, [, guildState]) => sum + (guildState.reminders?.length || 0), 0),
  };
}

async function writeStatusSnapshot() {
  const payload = {
    ok: true,
    pid: process.pid,
    startedAt,
    updatedAt: Date.now(),
    uptimeSec: Math.round(process.uptime()),
    enabled: isBotEnabled(),
    listeningPaused: runtimeConfig.listeningPaused === true,
    runtime: publicRuntimeConfig(),
    sessions: summarizeSessions(),
    groqLimits: Object.fromEntries(groqLastLimits.entries()),
    memory: memoryStats(),
    process: {
      memory: process.memoryUsage(),
      cpu: process.cpuUsage(),
    },
  };
  await fs.writeFile(statusPath, JSON.stringify(payload, null, 2)).catch((error) => {
    console.error('status write failed:', error);
  });
}

async function applyRuntimeConfigEffects() {
  const wasEnabled = lastBotEnabled;
  await reloadRuntimeConfigIfChanged().catch((error) => console.error('runtime config reload failed:', error));
  const enabled = isBotEnabled();
  if (!enabled) {
    autoJoinSuppressedUntilManualJoin = false;
    for (const [guildId, session] of sessions.entries()) {
      if (session.connection && session.connection.state.status !== VoiceConnectionStatus.Destroyed) {
        session.connection.destroy();
      }
      sessions.delete(guildId);
    }
  } else if (!wasEnabled && !sessions.size && hasConfiguredAutoJoin() && !autoJoinInProgress && !autoJoinSuppressedUntilManualJoin) {
    autoJoinInProgress = true;
    await autoJoinConfiguredVoice().catch((error) => console.error('auto join after enable failed:', error));
    autoJoinInProgress = false;
  }
  lastBotEnabled = enabled;
  await writeStatusSnapshot();
}

function findBestFuzzy(items, targetText, {
  getNames,
  getLabel,
  minScore = 0.48,
  confidentScore = 0.72,
  margin = 0.18,
  emptyError = 'Не понял цель команды.',
  notFoundError = (target) => `Не нашел “${target}”.`,
  ambiguousError = (labels) => `Нашел несколько похожих вариантов: ${labels}. Скажи точнее.`,
} = {}) {
  const scored = [];

  const normalizedTarget = normalizeCommandText(targetText || '');
  if (!normalizedTarget) return { error: emptyError };

  for (const item of items) {
    const names = getNames(item)
      .filter(Boolean)
      .map((name) => normalizeCommandText(name));
    if (!names.length) continue;

    let bestScore = 0;
    let bestName = names[0];
    for (const name of names) {
      const score = similarity(name, normalizedTarget);
      if (score > bestScore) {
        bestScore = score;
        bestName = name;
      }
    }

    if (bestScore >= minScore) scored.push({ item, score: bestScore, bestName });
  }

  scored.sort((a, b) => b.score - a.score);

  if (!scored.length) {
    return { error: notFoundError(targetText) };
  }

  const [best, second] = scored;
  const confident = best.score >= confidentScore || !second || best.score - second.score >= margin;
  if (!confident) {
    const labels = scored.map(({ item }) => getLabel(item)).slice(0, 5).join(', ');
    return { error: ambiguousError(labels) };
  }

  console.log(`fuzzy match "${targetText}" -> "${getLabel(best.item)}" score=${best.score.toFixed(2)} matched="${best.bestName}"`);
  return { item: best.item };
}

function candidateChannelNames(channel) {
  return [channel.name, channel.id].filter(Boolean);
}

function candidateRoleNames(role) {
  return [role.name, role.id].filter(Boolean);
}

function findVoiceTarget(session, targetText) {
  const voiceMembers = getCurrentVoiceMembers(session);
  if (!voiceMembers.length) {
    return { error: 'Я не вижу участников в текущем голосовом канале.' };
  }

  const result = findBestFuzzy(
    voiceMembers.filter((member) => !member.user.bot),
    targetText,
    {
      getNames: candidateMemberNames,
      getLabel: (member) => member.displayName,
      emptyError: 'Кого выбрать? Скажи имя или похожий ник после команды.',
      notFoundError: (target) => `Не нашел в голосовом канале участника “${target}”.`,
      ambiguousError: (labels) => `Нашел несколько похожих участников: ${labels}. Скажи имя точнее.`,
    },
  );

  return result.error ? result : { member: result.item };
}

function getCurrentVoiceMembers(session) {
  const byId = new Map();
  for (const member of session.voiceChannel?.members?.values?.() || []) {
    if (member?.id) byId.set(member.id, member);
  }
  for (const voiceState of session.guild?.voiceStates?.cache?.values?.() || []) {
    if (voiceState.channelId !== session.voiceChannel?.id) continue;
    const member = voiceState.member || session.guild.members.cache.get(voiceState.id);
    if (member?.id) byId.set(member.id, member);
  }
  return [...byId.values()];
}

function getGuildSessions(guildId) {
  return [...sessions.values()].filter((session) => session.guild?.id === guildId);
}

function getPrimarySession(guildId, voiceChannelId = null) {
  const guildSessions = getGuildSessions(guildId);
  if (!guildSessions.length) return null;
  if (voiceChannelId) {
    const exact = guildSessions.find((session) => session.voiceChannel?.id === voiceChannelId);
    if (exact) return exact;
  }
  return guildSessions[0];
}

function getInteractionSession(interaction) {
  return getPrimarySession(interaction.guildId, interaction.member?.voice?.channelId || null);
}

function findReminderSession(reminder) {
  return getPrimarySession(reminder.guildId, reminder.voiceChannelId || null);
}

async function findMemberTarget(session, targetText) {
  const voiceTarget = findVoiceTarget(session, targetText);
  if (!voiceTarget.error) return voiceTarget;

  const cachedMembers = [...session.guild.members.cache.values()]
    .filter((member) => member.id !== client.user.id);
  const cachedResult = findBestFuzzy(cachedMembers, targetText, {
    getNames: candidateMemberNames,
    getLabel: (member) => member.displayName,
    emptyError: 'Кого выбрать? Скажи имя, ник, тег или ID после команды.',
    notFoundError: () => voiceTarget.error,
    ambiguousError: (labels) => `Нашел несколько похожих участников сервера: ${labels}. Скажи имя точнее.`,
  });
  if (!cachedResult.error) return { member: cachedResult.item };

  const rawQuery = String(targetText || '').trim();
  const searchQuery = normalizeCommandText(rawQuery).split(' ')[0] || rawQuery;
  if (!searchQuery) return cachedResult;

  const searched = await session.guild.members.search({ query: searchQuery, limit: 20 }).catch((error) => {
    console.error('member search failed:', error);
    return null;
  });
  if (!searched?.size) return cachedResult;

  const searchResult = findBestFuzzy([...searched.values()].filter((member) => member.id !== client.user.id), targetText, {
    getNames: candidateMemberNames,
    getLabel: (member) => member.displayName,
    emptyError: 'Кого выбрать? Скажи имя, ник, тег или ID после команды.',
    notFoundError: () => cachedResult.error,
    ambiguousError: (labels) => `Нашел несколько похожих участников сервера: ${labels}. Скажи имя точнее.`,
  });

  return searchResult.error ? searchResult : { member: searchResult.item };
}

async function findVoiceChannel(session, channelText) {
  const channels = await session.guild.channels.fetch();
  const voiceChannels = [...channels.values()].filter(
    (channel) => channel && [ChannelType.GuildVoice, ChannelType.GuildStageVoice].includes(channel.type),
  );

  const result = findBestFuzzy(voiceChannels, channelText, {
    getNames: candidateChannelNames,
    getLabel: (channel) => channel.name,
    emptyError: 'Какой voice channel нужен?',
    notFoundError: () => 'Не нашел такой voice channel.',
    ambiguousError: (labels) => `Нашел несколько похожих voice channel: ${labels}. Скажи точнее.`,
  });
  return result.error ? null : result.item;
}

async function findTextChannel(session, channelText) {
  const channels = await session.guild.channels.fetch();
  const textChannels = [...channels.values()].filter(
    (channel) => channel && [ChannelType.GuildText, ChannelType.GuildAnnouncement].includes(channel.type),
  );

  const result = findBestFuzzy(textChannels, channelText, {
    getNames: candidateChannelNames,
    getLabel: (channel) => channel.name,
    emptyError: 'Какой текстовый канал нужен?',
    notFoundError: () => 'Не нашел такой текстовый канал.',
    ambiguousError: (labels) => `Нашел несколько похожих текстовых каналов: ${labels}. Скажи точнее.`,
  });
  return result.error ? null : result.item;
}

async function findAnyChannel(session, channelText) {
  const channels = await session.guild.channels.fetch();
  const managedChannels = [...channels.values()].filter((channel) => channel && channel.type !== ChannelType.DM);

  const result = findBestFuzzy(managedChannels, channelText, {
    getNames: candidateChannelNames,
    getLabel: (channel) => channel.name,
    emptyError: 'Какой канал нужен?',
    notFoundError: () => 'Не нашел такой канал.',
    ambiguousError: (labels) => `Нашел несколько похожих каналов: ${labels}. Скажи точнее.`,
  });
  return result.error ? null : result.item;
}

async function findRole(session, roleText) {
  await session.guild.roles.fetch().catch(() => null);
  const roles = [...session.guild.roles.cache.values()]
    .filter((role) => role.id !== session.guild.id && !role.managed);

  const result = findBestFuzzy(roles, roleText, {
    getNames: candidateRoleNames,
    getLabel: (role) => role.name,
    minScore: 0.5,
    emptyError: 'Какую роль использовать?',
    notFoundError: (target) => `Не нашел роль “${target}”.`,
    ambiguousError: (labels) => `Нашел несколько похожих ролей: ${labels}. Скажи роль точнее.`,
  });
  return result.error ? result : { role: result.item };
}

function normalizeTextChannelName(name) {
  return normalizeCommandText(name || '')
    .replace(/\s+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 100) || 'new-channel';
}

function normalizeVoiceChannelName(name) {
  const cleaned = String(name || '').trim().replace(/\s+/g, ' ').slice(0, 100);
  return cleaned || 'Новый voice';
}

const ACTION_KEYWORDS = [
  'отключ', 'выкин', 'выкини', 'дискон',
  'кикни', 'кик', 'исключ', 'удали участника',
  'бан', 'забань', 'разбан',
  'таймаут', 'timeout', 'мут на', 'накажи', 'сними таймаут',
  'перемест', 'перекин', 'перетащи',
  'мут', 'замуть', 'размут', 'размуть', 'заглуш', 'разглуш',
  'деаф', 'оглуш',
  'роль', 'выдай роль', 'дай роль', 'забери роль', 'убери роль',
  'ник', 'никнейм', 'переименуй участника',
  'закрой', 'открой', 'залочь', 'разлочь', 'заблок', 'разблок',
  'переимен', 'назови', 'имя канала',
  'создай канал', 'создай чат', 'создай войс',
  'удали канал', 'снеси канал',
  'лимит', 'слоумод', 'slowmode', 'медленный режим',
  'очист', 'удали сообщения', 'почист',
  'напиши', 'отправь в чат', 'скажи в чат',
  'стоп', 'замолчи', 'перестань говорить', 'хватит', 'остановись', 'останови', 'харош', 'хорош',
  'сбрось память', 'забудь память', 'очисти память', 'запомни', 'запиши в память',
  'напомни', 'напоминания', 'отмени напоминания', 'удали напоминание', 'убери напоминание',
  'забудь диалог', 'сбрось диалог', 'новый диалог',
  'статус', 'лимиты', 'limits',
  'пауза', 'не слушай', 'продолжай', 'слушай дальше',
  'замуть всех', 'размуть всех', 'отключи всех', 'перемести всех',
  'создай роль', 'удали роль',
  'тема чата', 'описание чата', 'закрепи',
];

const ACTION_HELP = [
  'отключи Иван',
  'кикни Иван',
  'забань Иван',
  'дай Иван роль Модератор',
  'забери у Иван роль Модератор',
  'дай Иван таймаут 5 минут',
  'сними таймаут с Иван',
  'переименуй Иван в Тестер',
  'перемести Иван в Общий',
  'замуть Иван',
  'размуть Иван',
  'замуть всех',
  'размуть всех',
  'отключи всех от войса',
  'перемести всех в Общий',
  'заглуши Иван',
  'разглуши Иван',
  'закрой войс',
  'открой войс',
  'переименуй войс в Комната тестов',
  'поставь лимит 5',
  'закрой чат',
  'открой чат',
  'создай текстовый канал тест',
  'создай голосовой канал рейд',
  'удали канал старый-тест',
  'переименуй чат в тестовый-чат',
  'создай роль Тестер',
  'удали роль Тестер',
  'поставь тему чата Тестовая тема',
  'закрепи последнее сообщение',
  'включи слоумод 10 секунд',
  'очисти 20 сообщений',
  'напиши в чат тестовое сообщение',
  'покажи статус',
  'покажи лимиты',
  'запомни что серверный пароль лежит у администратора',
  'что ты помнишь',
  'забудь память',
  'напомни через 5 минут проверить чай',
  'покажи напоминания',
  'удали напоминание про чай',
  'удали второе напоминание',
  'пауза',
  'продолжай',
  'стоп',
  'хватит',
  'остановись',
  'харош',
];

function looksLikeAction(prompt) {
  const normalized = normalizeCommandText(prompt);
  return ACTION_KEYWORDS.some((keyword) => normalized.includes(keyword));
}

function extractJsonObject(text) {
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}');
  if (start === -1 || end === -1 || end <= start) return null;
  return text.slice(start, end + 1);
}

function parseSimpleAction(prompt) {
  const normalized = normalizeCommandText(prompt);
  if (!normalized) return null;

  const reminder = parseReminderCommand(prompt);
  if (reminder?.error) return { action: 'action_error', text: reminder.error };
  if (reminder) {
    return {
      action: 'add_reminder',
      text: reminder.text,
      dueAt: reminder.dueAt,
      repeatIntervalMs: reminder.repeatIntervalMs,
      repeatLabel: reminder.repeatLabel,
    };
  }

  const deleteReminder = parseDeleteReminderCommand(prompt);
  if (deleteReminder) return deleteReminder;

  const rememberUserMatch = String(prompt || '').trim().match(/^(?:запомни|запиши в память)\s+(?:обо мне|про меня|для меня|мне)\s*(?:что|:)?\s+(.+)$/iu);
  if (rememberUserMatch?.[1]?.trim()) {
    return { action: 'remember_user_memory', text: rememberUserMatch[1].trim() };
  }
  const rememberMatch = String(prompt || '').trim().match(/^(?:запомни|запиши в память)\s*(?:что|:)?\s+(.+)$/iu);
  if (rememberMatch?.[1]?.trim()) {
    return { action: 'remember_memory', text: rememberMatch[1].trim() };
  }
  const noteMatch = String(prompt || '').trim().match(/^(?:запиши\s+заметку|добавь\s+заметку|сделай\s+заметку)\s*(?:что|:)?\s+(.+)$/iu);
  if (noteMatch?.[1]?.trim()) {
    return { action: 'remember_memory', text: noteMatch[1].trim() };
  }
  if (normalized.includes('что ты помнишь обо мне') || normalized.includes('что помнишь обо мне') || normalized.includes('покажи память обо мне')) {
    return { action: 'show_user_memory' };
  }
  if (normalized.includes('что ты помнишь') || normalized.includes('покажи память') || normalized === 'память') {
    return { action: 'show_memory' };
  }
  if (normalized.includes('забудь память') || normalized.includes('очисти память') || normalized.includes('сбрось память') || normalized.includes('забудь все')) {
    return { action: 'clear_memory' };
  }
  if (normalized.includes('покажи напомин') || normalized === 'напоминания') {
    return { action: 'list_reminders' };
  }
  if (normalized.includes('отмени все напомин') || normalized.includes('очисти напомин') || normalized.includes('сбрось напомин')) {
    return { action: 'clear_reminders' };
  }
  if ((normalized.includes('отключ') || normalized.includes('выкин') || normalized.includes('дискон')) && normalized.includes('всех')) {
    return { action: 'disconnect_all' };
  }
  if ((normalized.includes('замуть') || normalized.includes('замут') || normalized.includes('мут')) && normalized.includes('всех')) {
    return { action: 'mute_all' };
  }
  if ((normalized.includes('размуть') || normalized.includes('размут')) && normalized.includes('всех')) {
    return { action: 'unmute_all' };
  }
  const moveAllMatch = normalized.match(/(?:перемести|перекинь|перетащи)\s+всех\s+(?:в|на)\s+(.+)$/u);
  if (moveAllMatch?.[1]?.trim()) {
    return { action: 'move_all_members', channel: moveAllMatch[1].trim() };
  }
  if (
    /(^|\s)(стоп|замолчи|хватит|остановись|останови|харош|хорош|тихо|заткнись)(\s|$)/u.test(normalized)
    || normalized.includes('перестань говорить')
    || normalized.includes('не говори')
    || normalized.includes('останови речь')
    || normalized.includes('останови спич')
  ) {
    return { action: 'stop_speaking' };
  }
  if (normalized.includes('сбрось диалог') || normalized.includes('забудь диалог') || normalized.includes('новый диалог')) {
    return { action: 'reset_memory' };
  }
  if (normalized.includes('покажи лимит') || normalized === 'лимиты' || normalized === 'limits' || normalized.includes(' limits')) {
    return { action: 'show_limits' };
  }
  if (normalized.includes('покажи статус') || normalized === 'статус') {
    return { action: 'show_status' };
  }
  if (normalized === 'пауза' || normalized.includes('не слушай')) {
    return { action: 'pause_listening' };
  }
  if (normalized === 'продолжай' || normalized.includes('слушай дальше') || normalized.includes('сними паузу')) {
    return { action: 'resume_listening' };
  }

  return null;
}

async function parseAction(prompt, channel = monitorChannel) {
  const simpleAction = parseSimpleAction(prompt);
  if (simpleAction) return simpleAction;
  if (!looksLikeAction(prompt)) return { action: 'none' };

  let completion;
  const model = getChatModel();
  try {
    const result = await getGroqClient().chat.completions.create({
      model,
      temperature: 0,
      max_completion_tokens: 260,
      messages: [
        {
          role: 'system',
          content:
            'Ты строгий JSON-парсер голосовых команд Discord. Верни только JSON без markdown. '
            + 'Схема: {"action":"...","target":"...","channel":"...","value":0,"text":"..."}. '
            + 'Доступные action: disconnect_member, disconnect_all, kick_member, ban_member, move_member, move_all_members, mute_member, unmute_member, mute_all, unmute_all, deafen_member, undeafen_member, timeout_member, untimeout_member, add_role, remove_role, create_role, delete_role, set_nickname, lock_voice, unlock_voice, rename_voice, set_voice_limit, lock_text, unlock_text, rename_text, set_text_topic, pin_last_message, set_slowmode, clear_messages, send_message, create_text_channel, create_voice_channel, delete_channel, show_status, show_limits, reset_memory, pause_listening, resume_listening, stop_speaking, delete_reminder, none. '
            + 'target это имя участника. channel это имя канала назначения или канала для действия. value это число: секунды для timeout/slowmode, лимит voice или количество сообщений. text это имя роли, новый ник, новое имя канала или текст сообщения. '
            + 'Если говорят "отключи/выкинь из войса" это disconnect_member, а "отключи всех" это disconnect_all. Если говорят "кикни/исключи с сервера" это kick_member. Если говорят "замуть" без длительности это mute_member, "замуть всех" это mute_all, а "таймаут на N" это timeout_member. '
            + 'Если говорят "перемести всех в канал" это move_all_members. "стоп/замолчи/хватит/остановись/харош" это stop_speaking. "удали напоминание про X" это delete_reminder и text=X. "сбрось диалог/новый диалог" это reset_memory. "покажи статус" это show_status. "покажи лимиты" это show_limits. '
            + 'Если команда не является действием Discord, action=none.',
        },
        { role: 'user', content: prompt },
      ],
    }).withResponse();
    completion = result.data;
    trackGroqRateLimits(channel, 'action-parser', result.response, model);
  } catch (error) {
    trackGroqRateLimits(channel, 'action-parser', error, model);
    throw error;
  }

  const raw = completion.choices[0]?.message?.content || '{}';
  const json = extractJsonObject(raw) || raw;
  try {
    const parsed = JSON.parse(json);
    if (!parsed || typeof parsed !== 'object') return { action: 'none' };
    return {
      action: String(parsed.action || 'none'),
      target: parsed.target ? String(parsed.target) : '',
      channel: parsed.channel ? String(parsed.channel) : '',
      value: Number.isFinite(Number(parsed.value)) ? Number(parsed.value) : 0,
      text: parsed.text ? String(parsed.text) : '',
    };
  } catch (error) {
    console.error('action parse failed:', raw, error);
    return { action: 'none' };
  }
}

async function editEveryoneOverwrite(channel, overwrites, reason) {
  await channel.permissionOverwrites.edit(channel.guild.roles.everyone, overwrites, { reason });
}

async function disconnectMember(targetMember, actorMember, reason) {
  if (!canMoveMembers(actorMember)) {
    return 'У тебя нет права Move Members или Administrator для этой команды.';
  }
  if (!targetMember?.voice?.channel) {
    return `${targetMember?.displayName || 'Этот участник'} сейчас не в голосовом канале.`;
  }
  if (targetMember.id === client.user.id) {
    return 'Я не буду отключать самого себя этой командой.';
  }

  try {
    await targetMember.voice.disconnect(reason);
    return `Отключил ${targetMember.displayName} от голосового канала.`;
  } catch (error) {
    console.error('disconnect failed:', error);
    return `Не смог отключить ${targetMember.displayName}: ${error.message || error}`;
  }
}

function getManagedVoiceMembers(session, actorMember, { includeActor = true } = {}) {
  return getCurrentVoiceMembers(session)
    .filter((member) => !member.user.bot && member.id !== client.user.id && (includeActor || member.id !== actorMember?.id));
}

function getHumanVoiceMembers(session) {
  return getCurrentVoiceMembers(session)
    .filter((member) => !member.user.bot && member.id !== client.user.id);
}

function displayMemberNames(members) {
  return [...new Set(
    members
      .map((member) => member.displayName || member.user?.globalName || member.user?.username || '')
      .map((name) => String(name).replace(/\s+/g, ' ').trim())
      .filter(Boolean),
  )].slice(0, 12);
}

function displayMemberName(member) {
  return displayMemberNames([member])[0] || 'друг';
}

function pickRandom(items) {
  return items[Math.floor(Math.random() * items.length)];
}

function dayPartGreeting(date = new Date()) {
  const hour = date.getHours();
  if (hour >= 5 && hour < 12) return 'доброе утро';
  if (hour >= 12 && hour < 18) return 'добрый день';
  if (hour >= 18 && hour < 23) return 'добрый вечер';
  return 'доброй ночи';
}

function formatNameListForSpeech(names, limit = 5) {
  const shown = names.slice(0, limit);
  const tail = names.length > limit ? ` и еще ${names.length - limit}` : '';
  return `${shown.join(', ')}${tail}`;
}

function buildMemberJoinAnnouncement(member) {
  const name = displayMemberName(member);
  const greeting = dayPartGreeting();
  return pickRandom([
    `${name}, ${greeting}! Рад тебя слышать.`,
    `${name}, ${greeting}! Заходи, тут как раз стало уютнее.`,
    `${name}, ${greeting}! Отлично, голосовой канал получил усиление.`,
    `${name}, ${greeting}! Хорошо, что заглянул.`,
  ]);
}

function buildMemberLeaveAnnouncement(member) {
  const name = displayMemberName(member);
  return pickRandom([
    `${name} вышел. Канал стал на один голос тише.`,
    `${name} покинул войс. Записываем как стратегическое отступление.`,
    `${name} ушел. Надеюсь, не за хлебом на три дня.`,
    `${name} исчез из войса. Красиво, но подозрительно.`,
  ]);
}

function buildBotJoinAnnouncement(session) {
  const names = displayMemberNames(getHumanVoiceMembers(session));
  if (!names.length) return '';
  return names.length === 1
    ? `Всем привет. ${names[0]}, я на месте.`
    : `Всем привет, я на месте. ${formatNameListForSpeech(names)}, рад вас слышать.`;
}

function isSessionVoiceReady(session) {
  return Boolean(
    session?.connection
      && session.connection.state.status !== VoiceConnectionStatus.Destroyed
      && session.voiceChannel?.id
      && session.player,
  );
}

function rememberPresenceEvent(session, key) {
  if (!PRESENCE_ANNOUNCEMENT_COOLDOWN_MS) return true;
  session.presenceEventTimes ||= new Map();
  const now = Date.now();
  for (const [eventKey, timestamp] of session.presenceEventTimes.entries()) {
    if (now - timestamp > PRESENCE_ANNOUNCEMENT_COOLDOWN_MS * 4) {
      session.presenceEventTimes.delete(eventKey);
    }
  }
  const last = session.presenceEventTimes.get(key) || 0;
  if (now - last < PRESENCE_ANNOUNCEMENT_COOLDOWN_MS) return false;
  session.presenceEventTimes.set(key, now);
  return true;
}

async function waitForPresenceSpeechSlot(session) {
  const startedAt = Date.now();
  while (Date.now() - startedAt <= PRESENCE_ANNOUNCEMENT_QUIET_WAIT_MS) {
    if (!isBotEnabled() || !isSessionVoiceReady(session) || isListeningPaused(session)) return false;
    const playerIdle = session.player?.state?.status !== AudioPlayerStatus.Playing;
    const noActiveSpeech = !session.busy && !session.interruptBusy && !(session.activeUsers?.size);
    if (playerIdle && noActiveSpeech) return true;
    await delay(500);
  }
  return false;
}

function enqueuePresenceAnnouncement(session, text, key) {
  if (!PRESENCE_ANNOUNCEMENTS_ENABLED || !text || !isSessionVoiceReady(session)) return;
  if (!rememberPresenceEvent(session, key)) return;

  session.presenceQueue = (session.presenceQueue || Promise.resolve())
    .catch(() => {})
    .then(async () => {
      if (PRESENCE_ANNOUNCEMENT_DELAY_MS) await delay(PRESENCE_ANNOUNCEMENT_DELAY_MS);
      if (!(await waitForPresenceSpeechSlot(session))) return;
      console.log(`presence announcement: ${text}`);
      await speak(session, text);
      session.lastReplyAt = Date.now();
      session.lastHumanActivityAt = session.lastReplyAt;
    })
    .catch((error) => {
      if (session.diagnostics) session.diagnostics.lastError = error.message || String(error);
      console.error('presence announcement failed:', error);
    });
}

function beginCancellableTurn(session) {
  session.currentTurnId = (session.currentTurnId || 0) + 1;
  session.cancelCurrentTurn = false;
  session.stopSpeechRequested = false;
  return session.currentTurnId;
}

function isTurnCancelled(session, turnId) {
  return session.cancelCurrentTurn === true || session.currentTurnId !== turnId;
}

function beginSpeech(session) {
  session.speechVersion = (session.speechVersion || 0) + 1;
  session.stopSpeechRequested = false;
  return session.speechVersion;
}

function isSpeechCancelled(session, speechVersion) {
  return session.stopSpeechRequested === true
    || session.cancelCurrentTurn === true
    || session.speechVersion !== speechVersion;
}

function stopPlayback(session) {
  if (!session) return false;
  session.cancelCurrentTurn = true;
  session.stopSpeechRequested = true;
  session.speechVersion = (session.speechVersion || 0) + 1;
  const stopped = session.player?.stop(true) || false;
  return stopped || Boolean(session.busy || session.interruptBusy);
}

function setPendingReminderDeletion(session, pending) {
  session.pendingAction = {
    type: 'delete_reminders',
    createdAt: Date.now(),
    ...pending,
  };
}

function clearPendingAction(session) {
  session.pendingAction = null;
}

function activePendingReminderDeletion(session) {
  const pending = session.pendingAction;
  if (!pending || pending.type !== 'delete_reminders') return null;
  if (Date.now() - pending.createdAt > 120_000) {
    clearPendingAction(session);
    return null;
  }
  return pending;
}

function deleteReminderIds(session, ids) {
  const removed = removeReminderItemsByIds(session.guild.id, ids);
  clearPendingAction(session);
  if (!removed.length) return 'Эти напоминания уже не активны.';
  const list = removed.map((reminder, index) => `${index + 1}. ${reminder.text}`).join('\n');
  return removed.length === 1
    ? `Удалил напоминание: ${removed[0].text}`
    : `Удалил напоминаний: ${removed.length}.\n${list}`;
}

function askReminderSelection(session, matches, query, { allowDeleteAll = true } = {}) {
  const shown = matches.slice(0, 6);
  const list = shown.map((item, index) => formatReminderChoice(item.reminder, index)).join('\n');
  setPendingReminderDeletion(session, {
    mode: allowDeleteAll ? 'confirm_or_select' : 'select',
    ids: shown.map((item) => item.reminder.id),
    query,
  });
  const suffix = allowDeleteAll
    ? 'Скажи “бот да”, чтобы удалить все эти, “бот номер 2”, чтобы удалить одно, или “бот нет”.'
    : 'Скажи номер, часть текста или “бот нет”.';
  return `Нашел несколько подходящих напоминаний:\n${list}\n${suffix}`;
}

function handlePendingReminderDeletion(session, prompt) {
  const pending = activePendingReminderDeletion(session);
  if (!pending) return null;

  if (isNegativeConfirmation(prompt)) {
    clearPendingAction(session);
    return { text: 'Ок, ничего не удаляю.', speak: false };
  }

  const activeById = new Map(getGuildState(session.guild.id).reminders.map((reminder) => [reminder.id, reminder]));
  const candidates = pending.ids.map((id) => activeById.get(id)).filter(Boolean);
  if (!candidates.length) {
    clearPendingAction(session);
    return 'Эти напоминания уже не активны.';
  }

  const selectedNumber = parseSelectionNumber(prompt);
  if (selectedNumber && selectedNumber >= 1 && selectedNumber <= candidates.length) {
    return deleteReminderIds(session, [candidates[selectedNumber - 1].id]);
  }

  if (isPositiveConfirmation(prompt)) {
    if (pending.mode === 'select' && candidates.length > 1) {
      return { text: 'Скажи номер напоминания или часть текста. “Да” тут слишком широко.', speak: false };
    }
    return deleteReminderIds(session, candidates.map((reminder) => reminder.id));
  }

  const matches = findReminderMatches(session.guild.id, prompt)
    .filter((item) => candidates.some((reminder) => reminder.id === item.reminder.id));
  if (matches.length === 1) return deleteReminderIds(session, [matches[0].reminder.id]);
  if (matches.length > 1) return askReminderSelection(session, matches, prompt, { allowDeleteAll: pending.mode !== 'select' });

  return null;
}

function handleDeleteReminderCommand(session, parsed) {
  const reminders = getGuildState(session.guild.id).reminders
    .slice()
    .sort((a, b) => a.dueAt - b.dueAt);
  if (!reminders.length) return 'Активных напоминаний нет.';

  const query = String(parsed.text || '').trim();
  const selectedNumber = parseSelectionNumber(query);
  if (selectedNumber && selectedNumber >= 1 && selectedNumber <= reminders.length) {
    return deleteReminderIds(session, [reminders[selectedNumber - 1].id]);
  }

  if (!query) {
    if (reminders.length === 1) {
      setPendingReminderDeletion(session, { mode: 'confirm', ids: [reminders[0].id], query: '' });
      return `Удалить это напоминание?\n${formatReminderChoice(reminders[0], 0)}\nСкажи “бот да” или “бот нет”.`;
    }
    return askReminderSelection(
      session,
      reminders.map((reminder, index) => ({ reminder, index, score: 0.1 })),
      '',
      { allowDeleteAll: false },
    );
  }

  const matches = findReminderMatches(session.guild.id, query);
  if (!matches.length) {
    return `Не нашел активное напоминание по запросу “${query}”. Скажи “бот покажи напоминания”, если нужно увидеть список.`;
  }

  const [best, second] = matches;
  const confident = best.score >= 0.65 || !second || best.score - second.score >= 0.28;
  if (confident) return deleteReminderIds(session, [best.reminder.id]);
  return askReminderSelection(session, matches, query, { allowDeleteAll: true });
}

const DANGEROUS_ACTIONS = new Set([
  'disconnect_member',
  'disconnect_all',
  'kick_member',
  'ban_member',
  'move_member',
  'move_all_members',
  'mute_member',
  'mute_all',
  'deafen_member',
  'timeout_member',
  'remove_role',
  'delete_role',
  'set_nickname',
  'lock_voice',
  'unlock_voice',
  'rename_voice',
  'set_voice_limit',
  'lock_text',
  'unlock_text',
  'rename_text',
  'set_text_topic',
  'pin_last_message',
  'set_slowmode',
  'clear_messages',
  'delete_channel',
  'clear_memory',
  'clear_reminders',
]);

function isDangerousAction(parsed) {
  return shouldConfirmDangerousActions() && DANGEROUS_ACTIONS.has(parsed?.action);
}

function describeParsedAction(parsed) {
  const parts = [parsed.action];
  if (parsed.target) parts.push(`цель: ${parsed.target}`);
  if (parsed.channel) parts.push(`канал: ${parsed.channel}`);
  if (parsed.text) parts.push(`текст: ${parsed.text}`);
  if (parsed.value) parts.push(`значение: ${parsed.value}`);
  return parts.join(', ');
}

function setPendingDangerousAction(session, actorMember, parsed) {
  session.pendingAction = {
    type: 'dangerous_action',
    actorId: actorMember?.id || null,
    parsed,
    createdAt: Date.now(),
  };
}

function activePendingDangerousAction(session) {
  const pending = session.pendingAction;
  if (!pending || pending.type !== 'dangerous_action') return null;
  if (Date.now() - pending.createdAt > 120_000) {
    clearPendingAction(session);
    return null;
  }
  return pending;
}

async function handlePendingDangerousAction(session, actorMember, prompt) {
  const pending = activePendingDangerousAction(session);
  if (!pending) return null;

  if (pending.actorId && actorMember?.id && pending.actorId !== actorMember.id) {
    return { text: 'Жду подтверждение от того, кто дал опасную команду.', speak: false };
  }
  if (isNegativeConfirmation(prompt)) {
    clearPendingAction(session);
    return { text: 'Ок, отменил опасное действие.', speak: false };
  }
  if (!isPositiveConfirmation(prompt)) return null;

  const parsed = pending.parsed;
  clearPendingAction(session);
  appendEvent('dangerous_action_confirmed', {
    guildId: session.guild?.id,
    voiceChannelId: session.voiceChannel?.id,
    actorId: actorMember?.id,
    action: parsed.action,
  });
  return executeParsedAction(session, actorMember, parsed);
}

async function tryHandleVoiceAction(session, actorMember, prompt) {
  const pendingResult = handlePendingReminderDeletion(session, prompt);
  if (pendingResult) return pendingResult;

  const pendingDangerousAction = activePendingDangerousAction(session);
  if (pendingDangerousAction) {
    const pendingDangerous = await handlePendingDangerousAction(session, actorMember, prompt);
    return pendingDangerous || {
      text: `Жду подтверждение опасного действия: ${describeParsedAction(pendingDangerousAction.parsed)}. Скажи “${getWakeWord() || 'бот'} да” или “${getWakeWord() || 'бот'} нет”.`,
      speak: false,
    };
  }

  const parsed = await parseAction(prompt, session.textChannel);
  if (!parsed || parsed.action === 'none') return null;
  if (parsed.action !== 'delete_reminder' && session.pendingAction) clearPendingAction(session);

  if (isDangerousAction(parsed)) {
    setPendingDangerousAction(session, actorMember, parsed);
    appendEvent('dangerous_action_pending', {
      guildId: session.guild?.id,
      voiceChannelId: session.voiceChannel?.id,
      actorId: actorMember?.id,
      action: parsed.action,
    });
    return `Опасное действие требует подтверждения: ${describeParsedAction(parsed)}. Скажи “${getWakeWord() || 'бот'} да” или “${getWakeWord() || 'бот'} нет”.`;
  }

  return executeParsedAction(session, actorMember, parsed);
}

async function executeParsedAction(session, actorMember, parsed) {
  const reason = `Voice command by ${actorMember?.user?.tag || actorMember?.id || 'unknown user'}`;
  const requirePermission = (permission, label) => {
    if (canUsePermission(actorMember, permission)) return null;
    return `У тебя нет права ${label} или Administrator для этой команды.`;
  };
  const getTarget = async () => {
    const target = await findMemberTarget(session, parsed.target);
    return target.error ? target : target.member;
  };
  const roleText = () => (parsed.text || parsed.channel || '').trim();
  const channelText = () => (parsed.channel || parsed.text || '').trim();

  try {
    switch (parsed.action) {
      case 'action_error':
        return parsed.text || 'Не понял команду.';
      case 'remember_memory': {
        const text = parsed.text.trim();
        if (!text) return 'Что запомнить?';
        addMemoryItem(session.guild.id, actorMember, text);
        appendEvent('memory_added', { guildId: session.guild.id, userId: actorMember?.id, scope: 'guild', text });
        return 'Запомнил.';
      }
      case 'remember_user_memory': {
        const text = parsed.text.trim();
        if (!text) return 'Что запомнить о тебе?';
        addUserMemoryItem(session.guild.id, actorMember, text);
        appendEvent('memory_added', { guildId: session.guild.id, userId: actorMember?.id, scope: 'user', text });
        return 'Запомнил персонально о тебе.';
      }
      case 'show_memory': {
        await sendText(session.textChannel, `Память:\n${formatMemoryList(session.guild.id, actorMember?.id)}`);
        return { text: 'Отправил память в чат.', speak: false };
      }
      case 'show_user_memory': {
        await sendText(session.textChannel, `Память о тебе:\n${formatMemoryList(session.guild.id, actorMember?.id)}`);
        return { text: 'Отправил твою память в чат.', speak: false };
      }
      case 'clear_memory': {
        const count = clearMemoryItems(session.guild.id);
        return `Очистил локальную память. Удалено записей: ${count}.`;
      }
      case 'add_reminder': {
        if (!parsed.dueAt || !parsed.text?.trim()) return 'Не понял напоминание. Пример: “бот напомни через 5 минут проверить чай”.';
        const reminder = addReminderItem(session, actorMember, parsed.text, parsed.dueAt, {
          repeatIntervalMs: parsed.repeatIntervalMs,
          repeatLabel: parsed.repeatLabel,
        });
        appendEvent('reminder_added', {
          guildId: session.guild.id,
          userId: actorMember?.id,
          text: reminder.text,
          dueAt: reminder.dueAt,
          repeatLabel: reminder.repeatLabel,
        });
        return reminder.repeatIntervalMs
          ? `Хорошо, буду повторять: ${reminder.repeatLabel || 'периодически'}. Первый раз ${formatDueTime(reminder.dueAt)}.`
          : `Хорошо, напомню ${formatDueTime(reminder.dueAt)}.`;
      }
      case 'list_reminders': {
        await sendText(session.textChannel, `Напоминания:\n${formatReminderList(session.guild.id)}`);
        return { text: 'Отправил напоминания в чат.', speak: false };
      }
      case 'delete_reminder': {
        return handleDeleteReminderCommand(session, parsed);
      }
      case 'clear_reminders': {
        const count = clearReminderItems(session.guild.id);
        clearPendingAction(session);
        return `Отменил активные напоминания. Удалено: ${count}.`;
      }
      case 'disconnect_member': {
        const target = await getTarget();
        if (target.error) return target.error;
        return disconnectMember(target, actorMember, reason);
      }
      case 'disconnect_all': {
        const denied = requirePermission(PermissionFlagsBits.MoveMembers, 'Move Members');
        if (denied) return denied;
        const members = getManagedVoiceMembers(session, actorMember);
        if (!members.length) return 'Некого отключать в текущем voice channel.';
        const results = await Promise.allSettled(members.map((member) => member.voice.disconnect(reason)));
        const ok = results.filter((result) => result.status === 'fulfilled').length;
        return `Отключил участников от voice channel: ${ok}/${members.length}.`;
      }
      case 'kick_member': {
        const denied = requirePermission(PermissionFlagsBits.KickMembers, 'Kick Members');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        if (target.id === client.user.id) return 'Я не буду кикать самого себя.';
        await target.kick(reason);
        return `Кикнул ${target.displayName} с сервера.`;
      }
      case 'ban_member': {
        const denied = requirePermission(PermissionFlagsBits.BanMembers, 'Ban Members');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        if (target.id === client.user.id) return 'Я не буду банить самого себя.';
        await target.ban({ reason });
        return `Забанил ${target.displayName}.`;
      }
      case 'move_member': {
        const denied = requirePermission(PermissionFlagsBits.MoveMembers, 'Move Members');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        if (!target.voice?.channel) return `${target.displayName} сейчас не в голосовом канале.`;
        const destination = await findVoiceChannel(session, parsed.channel);
        if (!destination) return `Не нашел голосовой канал “${parsed.channel}”.`;
        await target.voice.setChannel(destination, reason);
        return `Переместил ${target.displayName} в ${destination.name}.`;
      }
      case 'move_all_members': {
        const denied = requirePermission(PermissionFlagsBits.MoveMembers, 'Move Members');
        if (denied) return denied;
        const destination = await findVoiceChannel(session, parsed.channel || parsed.text);
        if (!destination) return `Не нашел голосовой канал “${parsed.channel || parsed.text}”.`;
        const members = getManagedVoiceMembers(session, actorMember)
          .filter((member) => member.voice?.channelId !== destination.id);
        if (!members.length) return `Некого перемещать в ${destination.name}.`;
        const results = await Promise.allSettled(members.map((member) => member.voice.setChannel(destination, reason)));
        const ok = results.filter((result) => result.status === 'fulfilled').length;
        return `Переместил в ${destination.name}: ${ok}/${members.length}.`;
      }
      case 'mute_member':
      case 'unmute_member': {
        const denied = requirePermission(PermissionFlagsBits.MuteMembers, 'Mute Members');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        await target.voice.setMute(parsed.action === 'mute_member', reason);
        return parsed.action === 'mute_member'
          ? `Замьютил ${target.displayName}.`
          : `Размьютил ${target.displayName}.`;
      }
      case 'mute_all':
      case 'unmute_all': {
        const denied = requirePermission(PermissionFlagsBits.MuteMembers, 'Mute Members');
        if (denied) return denied;
        const members = getManagedVoiceMembers(session, actorMember);
        if (!members.length) return 'Некого менять в текущем voice channel.';
        const muted = parsed.action === 'mute_all';
        const results = await Promise.allSettled(members.map((member) => member.voice.setMute(muted, reason)));
        const ok = results.filter((result) => result.status === 'fulfilled').length;
        return muted ? `Замьютил участников: ${ok}/${members.length}.` : `Размьютил участников: ${ok}/${members.length}.`;
      }
      case 'deafen_member':
      case 'undeafen_member': {
        const denied = requirePermission(PermissionFlagsBits.DeafenMembers, 'Deafen Members');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        await target.voice.setDeaf(parsed.action === 'deafen_member', reason);
        return parsed.action === 'deafen_member'
          ? `Заглушил звук для ${target.displayName}.`
          : `Вернул звук для ${target.displayName}.`;
      }
      case 'timeout_member':
      case 'untimeout_member': {
        const denied = requirePermission(PermissionFlagsBits.ModerateMembers, 'Moderate Members');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        if (parsed.action === 'untimeout_member') {
          await target.timeout(null, reason);
          return `Снял таймаут с ${target.displayName}.`;
        }
        const seconds = Math.max(1, Math.min(28 * 24 * 60 * 60, Math.round(parsed.value || 300)));
        await target.timeout(seconds * 1000, reason);
        return `Выдал таймаут ${target.displayName} на ${seconds} секунд.`;
      }
      case 'add_role':
      case 'remove_role': {
        const denied = requirePermission(PermissionFlagsBits.ManageRoles, 'Manage Roles');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        const roleResult = await findRole(session, roleText());
        if (roleResult.error) return roleResult.error;
        if (parsed.action === 'add_role') {
          await target.roles.add(roleResult.role, reason);
          return `Выдал ${target.displayName} роль ${roleResult.role.name}.`;
        }
        await target.roles.remove(roleResult.role, reason);
        return `Забрал у ${target.displayName} роль ${roleResult.role.name}.`;
      }
      case 'create_role': {
        const denied = requirePermission(PermissionFlagsBits.ManageRoles, 'Manage Roles');
        if (denied) return denied;
        const name = roleText();
        if (!name) return 'Какую роль создать?';
        const role = await session.guild.roles.create({ name: name.slice(0, 100), reason });
        return `Создал роль ${role.name}.`;
      }
      case 'delete_role': {
        const denied = requirePermission(PermissionFlagsBits.ManageRoles, 'Manage Roles');
        if (denied) return denied;
        const roleResult = await findRole(session, roleText());
        if (roleResult.error) return roleResult.error;
        const roleName = roleResult.role.name;
        await roleResult.role.delete(reason);
        return `Удалил роль ${roleName}.`;
      }
      case 'set_nickname': {
        const denied = requirePermission(PermissionFlagsBits.ManageNicknames, 'Manage Nicknames');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        const nickname = parsed.text.trim();
        if (!nickname) return 'Какой ник поставить?';
        await target.setNickname(nickname.slice(0, 32), reason);
        return `Переименовал ${target.displayName} в ${nickname.slice(0, 32)}.`;
      }
      case 'lock_voice':
      case 'unlock_voice': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        if (!session.voiceChannel) return 'Я не подключен к голосовому каналу.';
        await editEveryoneOverwrite(
          session.voiceChannel,
          { Connect: parsed.action === 'lock_voice' ? false : null },
          reason,
        );
        return parsed.action === 'lock_voice' ? 'Закрыл вход в голосовой канал.' : 'Открыл вход в голосовой канал.';
      }
      case 'rename_voice': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        if (!session.voiceChannel) return 'Я не подключен к голосовому каналу.';
        const name = parsed.text.trim();
        if (!name) return 'Как назвать voice channel?';
        await session.voiceChannel.setName(name.slice(0, 100), reason);
        return `Переименовал voice channel в ${name.slice(0, 100)}.`;
      }
      case 'set_voice_limit': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        if (!session.voiceChannel) return 'Я не подключен к голосовому каналу.';
        const limit = Math.max(0, Math.min(99, Math.round(parsed.value)));
        await session.voiceChannel.setUserLimit(limit, reason);
        return limit ? `Поставил лимит voice channel: ${limit}.` : 'Убрал лимит voice channel.';
      }
      case 'lock_text':
      case 'unlock_text': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        await editEveryoneOverwrite(
          session.textChannel,
          { SendMessages: parsed.action === 'lock_text' ? false : null },
          reason,
        );
        return parsed.action === 'lock_text' ? 'Закрыл отправку сообщений в этом чате.' : 'Открыл отправку сообщений в этом чате.';
      }
      case 'rename_text': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        const name = normalizeTextChannelName(parsed.text);
        await session.textChannel.setName(name, reason);
        return `Переименовал текстовый канал в ${name}.`;
      }
      case 'set_text_topic': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        if (!session.textChannel.setTopic) return 'Этот канал не поддерживает тему.';
        const topic = parsed.text.trim();
        await session.textChannel.setTopic(topic.slice(0, 1024), reason);
        return topic ? 'Обновил тему чата.' : 'Очистил тему чата.';
      }
      case 'pin_last_message': {
        const denied = requirePermission(PermissionFlagsBits.PinMessages, 'Pin Messages');
        if (denied) return denied;
        if (!session.textChannel.messages?.fetch) return 'Этот канал не поддерживает закрепление сообщений.';
        const messages = await session.textChannel.messages.fetch({ limit: 1 });
        const message = messages.first();
        if (!message) return 'Не нашел последнее сообщение для закрепления.';
        await message.pin(reason);
        return 'Закрепил последнее сообщение.';
      }
      case 'set_slowmode': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        const seconds = Math.max(0, Math.min(21600, Math.round(parsed.value)));
        const targetChannel = channelText() ? await findTextChannel(session, channelText()) : session.textChannel;
        if (!targetChannel?.setRateLimitPerUser) return 'Этот канал не поддерживает slowmode.';
        await targetChannel.setRateLimitPerUser(seconds, reason);
        return seconds ? `Поставил slowmode ${seconds} секунд.` : 'Выключил slowmode.';
      }
      case 'clear_messages': {
        const denied = requirePermission(PermissionFlagsBits.ManageMessages, 'Manage Messages');
        if (denied) return denied;
        const count = Math.max(1, Math.min(100, Math.round(parsed.value || 10)));
        const targetChannel = channelText() ? await findTextChannel(session, channelText()) : session.textChannel;
        if (!targetChannel?.bulkDelete) return 'Этот канал не поддерживает очистку сообщений.';
        const deleted = await targetChannel.bulkDelete(count, true);
        return `Удалил сообщений: ${deleted.size}.`;
      }
      case 'send_message': {
        const denied = requirePermission(PermissionFlagsBits.SendMessages, 'Send Messages');
        if (denied) return denied;
        const text = parsed.text.trim();
        if (!text) return 'Что написать в чат?';
        const targetChannel = parsed.channel ? await findTextChannel(session, parsed.channel) : session.textChannel;
        if (!targetChannel) return `Не нашел текстовый канал “${parsed.channel}”.`;
        await sendText(targetChannel, text.slice(0, 1800));
        return targetChannel.id === session.textChannel.id ? 'Написал в чат.' : `Написал в #${targetChannel.name}.`;
      }
      case 'create_text_channel': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        const name = normalizeTextChannelName(parsed.text || parsed.channel);
        const created = await session.guild.channels.create({ name, type: ChannelType.GuildText, reason });
        return `Создал текстовый канал #${created.name}.`;
      }
      case 'create_voice_channel': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        const name = normalizeVoiceChannelName(parsed.text || parsed.channel);
        const created = await session.guild.channels.create({ name, type: ChannelType.GuildVoice, reason });
        return `Создал голосовой канал ${created.name}.`;
      }
      case 'delete_channel': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        const name = channelText();
        if (!name) return 'Какой канал удалить? Назови канал явно.';
        const targetChannel = await findAnyChannel(session, name);
        if (!targetChannel) return `Не нашел канал “${name}”.`;
        const deletingCurrentTextChannel = targetChannel.id === session.textChannel?.id;
        const targetName = targetChannel.name;
        await targetChannel.delete(reason);
        if (deletingCurrentTextChannel) {
          return { text: `Удалил канал ${targetName}.`, send: false };
        }
        return `Удалил канал ${targetName}.`;
      }
      case 'show_status': {
        const status = formatSessionStatus(session);
        await sendText(session.textChannel, `Status:\n${status}`);
        return { text: 'Отправил статус в чат.', speak: false };
      }
      case 'show_limits': {
        await sendText(session.textChannel, `Groq API limits:\n${formatGroqLimits()}`);
        return { text: 'Отправил лимиты Groq в чат.', speak: false };
      }
      case 'reset_memory': {
        session.history.splice(0);
        return 'Сбросил память текущего диалога.';
      }
      case 'pause_listening': {
        session.paused = true;
        updateRuntimeConfig({ listeningPaused: true });
        return 'Поставил голосовую обработку на паузу. Чтобы вернуть, скажи: бот продолжай.';
      }
      case 'resume_listening': {
        session.paused = false;
        updateRuntimeConfig({ listeningPaused: false });
        return 'Продолжаю слушать голосовые команды.';
      }
      case 'stop_speaking': {
        const stopped = stopPlayback(session);
        return { text: stopped ? 'Остановил текущую речь.' : 'Сейчас ничего не говорю.', speak: false };
      }
      default:
        return null;
    }
  } catch (error) {
    console.error('action failed:', parsed, error);
    return `Не смог выполнить действие ${parsed.action}: ${error.message || error}`;
  }
}

function wavFromPcm(pcm, { sampleRate = 48000, channels = 2, bitsPerSample = 16 } = {}) {
  const header = Buffer.alloc(44);
  const blockAlign = channels * bitsPerSample / 8;
  const byteRate = sampleRate * blockAlign;
  header.write('RIFF', 0);
  header.writeUInt32LE(36 + pcm.length, 4);
  header.write('WAVE', 8);
  header.write('fmt ', 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(channels, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(blockAlign, 32);
  header.writeUInt16LE(bitsPerSample, 34);
  header.write('data', 36);
  header.writeUInt32LE(pcm.length, 40);
  return Buffer.concat([header, pcm]);
}

function parseWav(wav) {
  if (wav.toString('ascii', 0, 4) !== 'RIFF' || wav.toString('ascii', 8, 12) !== 'WAVE') {
    throw new Error('TTS output is not a WAV file');
  }

  let offset = 12;
  let fmt = null;
  let data = null;

  while (offset + 8 <= wav.length) {
    const id = wav.toString('ascii', offset, offset + 4);
    const size = wav.readUInt32LE(offset + 4);
    const start = offset + 8;
    if (id === 'fmt ') {
      fmt = {
        audioFormat: wav.readUInt16LE(start),
        channels: wav.readUInt16LE(start + 2),
        sampleRate: wav.readUInt32LE(start + 4),
        bitsPerSample: wav.readUInt16LE(start + 14),
      };
    } else if (id === 'data') {
      data = wav.subarray(start, start + size);
    }
    offset = start + size + (size % 2);
  }

  if (!fmt || !data) throw new Error('TTS WAV is missing fmt/data chunks');
  if (fmt.audioFormat !== 1 || fmt.bitsPerSample !== 16 || fmt.sampleRate !== 48000) {
    throw new Error(`Unsupported TTS WAV format: ${JSON.stringify(fmt)}`);
  }

  if (fmt.channels === 2) return data;
  if (fmt.channels !== 1) throw new Error(`Unsupported channel count: ${fmt.channels}`);

  const stereo = Buffer.alloc(data.length * 2);
  for (let inOffset = 0, outOffset = 0; inOffset < data.length; inOffset += 2, outOffset += 4) {
    data.copy(stereo, outOffset, inOffset, inOffset + 2);
    data.copy(stereo, outOffset + 2, inOffset, inOffset + 2);
  }
  return stereo;
}

function pcmStats(pcm) {
  if (!pcm.length) return { durationMs: 0, rms: 0 };
  let sumSquares = 0;
  const samples = pcm.length / 2;
  for (let i = 0; i + 1 < pcm.length; i += 2) {
    const sample = pcm.readInt16LE(i);
    sumSquares += sample * sample;
  }
  return {
    durationMs: pcm.length / (48000 * 2 * 2) * 1000,
    rms: Math.sqrt(sumSquares / samples),
  };
}

function buildSttPrompt(session) {
  const wakeTerms = [
    getAssistantName(),
    getWakeWord(),
    ...getWakeAliases(),
  ].filter(Boolean);
  const names = session
    ? [...new Set(
      getCurrentVoiceMembers(session)
        .flatMap((member) => candidateMemberNames(member))
        .filter((name) => name && name.length <= 32),
    )].slice(0, 60)
    : [];
  return names.length
    ? `${STT_PROMPT_BASE} Текущее имя ассистента: ${getAssistantName()}. Триггерные слова: ${wakeTerms.join(', ')}. Имена и ники на сервере: ${names.join(', ')}.`
    : `${STT_PROMPT_BASE} Текущее имя ассистента: ${getAssistantName()}. Триггерные слова: ${wakeTerms.join(', ')}.`;
}

async function transcribePcm(pcm, userId, sessionOrChannel = monitorChannel) {
  const session = sessionOrChannel?.voiceChannel ? sessionOrChannel : null;
  const channel = session?.textChannel || sessionOrChannel || monitorChannel;
  const wav = wavFromPcm(pcm);
  const model = getSttModel();
  const prompt = buildSttPrompt(session);
  const transcribe = async (language, label) => {
    const file = await toFile(wav, `${userId}.wav`, { type: 'audio/wav' });
    const result = await getGroqClient().audio.transcriptions.create({
      file,
      model,
      ...(language ? { language } : {}),
      ...(prompt ? { prompt } : {}),
      temperature: 0,
      response_format: 'json',
    }).withResponse();
    trackGroqRateLimits(channel, label, result.response, model);
    return (result.data?.text || '').trim();
  };
  const transcribeWithRetry = async (language, label) => {
    let lastError = null;
    for (let attempt = 1; attempt <= 2; attempt += 1) {
      try {
        return await transcribe(language, label);
      } catch (error) {
        lastError = error;
        if (!isTransientGroqConnectionError(error) || attempt >= 2) throw error;
        console.warn(`${label} transient connection error (${error?.cause?.code || error?.code || error?.message}), retrying`);
        await delay(350 * attempt);
      }
    }
    throw lastError;
  };

  try {
    const first = await transcribeWithRetry(getSttLanguage(), 'speech-to-text');
    if (first) return first;
    if (getSttLanguage()) {
      const retry = await transcribeWithRetry('', 'speech-to-text-retry');
      if (retry) return retry;
    }
  } catch (error) {
    trackGroqRateLimits(channel, 'speech-to-text', error, model);
    throw error;
  }
  return '';
}

function shouldUseWebSearch(prompt) {
  if (!isWebSearchEnabled()) return false;
  const normalized = normalizeCommandText(prompt);
  if (!normalized) return false;

  const webPhrases = [
    'найди', 'поищи', 'загугли', 'гугл', 'поиск', 'посмотри в интернете', 'в интернете',
    'интернет', 'сайт', 'ссылк', 'источник', 'новост', 'сейчас', 'сегодня', 'вчера',
    'актуаль', 'последн', 'свеж', 'курс', 'цена', 'стоимость', 'погода', 'расписание',
    'прогноз', 'температура', 'кто такой', 'что известно', 'что происходит', 'что случилось',
    'правда ли', 'проверь', 'обновлен', 'обновление', 'релиз', 'дата выхода', 'версия',
    'статус', 'работает ли', 'график', 'адрес', 'телефон', 'отзывы', 'рейтинг',
    'купить', 'билет', 'матч', 'счет', 'результат', 'доллар', 'евро', 'bitcoin', 'btc',
    'крипто', 'акции', 'latest', 'current', 'news', 'weather', 'forecast', 'price',
    'schedule', 'status', 'release',
  ];
  return webPhrases.some((phrase) => normalized.includes(phrase));
}

function isWeatherQuery(prompt) {
  const normalized = normalizeCommandText(prompt);
  return /погод|weather|forecast|температур|temperature/.test(normalized);
}

function cleanupWeatherLocation(value) {
  return String(value || '')
    .replace(/[?.!,;:]+$/g, '')
    .replace(/(^|\s)(сейчас|сегодня|завтра|пожалуйста|please|now|today|tomorrow)(?=\s|$)/giu, ' ')
    .replace(/(^|\s)(какая|какой|какую|что|там|погода|погоду|weather|forecast|температура)(?=\s|$)/giu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractWeatherLocation(prompt) {
  const text = String(prompt || '').trim();
  const patterns = [
    /(?:погод\p{L}*|weather|forecast|температур\p{L}*)[\s\S]{0,60}?(?:в|во|на|для|in|for)\s+([\p{L}\p{N} .'-]{2,80})/iu,
    /(?:в|во|на|для|in|for)\s+([\p{L}\p{N} .'-]{2,80})[\s\S]{0,40}?(?:погод\p{L}*|weather|forecast|температур\p{L}*)/iu,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    const location = cleanupWeatherLocation(match?.[1]);
    if (location) return location;
  }
  return '';
}

function weatherSearchNames(location) {
  const raw = cleanupWeatherLocation(location);
  if (!raw) return [];
  const lower = raw.toLocaleLowerCase('ru');
  const names = [raw];
  if (/черниг|chernihiv|chernigov/.test(lower)) names.unshift('Чернигов', 'Chernihiv');
  if (/киев|київ|kyiv|kiev/.test(lower)) names.unshift('Киев', 'Kyiv');
  if (/львов|львів|lviv|lvov/.test(lower)) names.unshift('Львов', 'Lviv');
  if (/одесс|одес|odesa|odessa/.test(lower)) names.unshift('Одесса', 'Odesa');
  if (/хар(ь|к)ов|kharkiv|kharkov/.test(lower)) names.unshift('Харьков', 'Kharkiv');
  if (/днепр|дніпр|dnipro|dnepr/.test(lower)) names.unshift('Днепр', 'Dnipro');
  if (/^[\p{Script=Cyrillic} -]+$/u.test(raw) && raw.length > 4) {
    names.push(raw.replace(/[еуіыа]$/iu, ''));
    names.push(raw.replace(/(ом|ем|ой|ий|ый)$/iu, ''));
  }
  return [...new Set(names.map((name) => cleanupWeatherLocation(name)).filter(Boolean))];
}

async function fetchJson(url, timeoutMs = 7000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: { 'user-agent': 'discord-ai-assistant/0.1' },
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.json();
  } finally {
    clearTimeout(timeout);
  }
}

async function geocodeWeatherLocation(location) {
  for (const name of weatherSearchNames(location)) {
    const url = `https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(name)}&count=5&language=ru&format=json`;
    const data = await fetchJson(url).catch(() => null);
    const result = data?.results?.find((item) => item.latitude && item.longitude);
    if (result) return result;
  }
  return null;
}

function weatherCodeLabel(code, english = false) {
  const labels = {
    0: ['ясно', 'clear sky'],
    1: ['почти ясно', 'mainly clear'],
    2: ['переменная облачность', 'partly cloudy'],
    3: ['пасмурно', 'overcast'],
    45: ['туман', 'fog'],
    48: ['изморозь и туман', 'rime fog'],
    51: ['слабая морось', 'light drizzle'],
    53: ['морось', 'drizzle'],
    55: ['сильная морось', 'dense drizzle'],
    61: ['слабый дождь', 'light rain'],
    63: ['дождь', 'rain'],
    65: ['сильный дождь', 'heavy rain'],
    71: ['слабый снег', 'light snow'],
    73: ['снег', 'snow'],
    75: ['сильный снег', 'heavy snow'],
    80: ['небольшие ливни', 'light showers'],
    81: ['ливни', 'showers'],
    82: ['сильные ливни', 'heavy showers'],
    95: ['гроза', 'thunderstorm'],
  };
  return labels[code]?.[english ? 1 : 0] || (english ? 'weather data' : 'погодные данные');
}

async function tryAnswerWeatherQuery(prompt) {
  if (!isWeatherQuery(prompt)) return '';
  const location = extractWeatherLocation(prompt);
  if (!location) return '';
  const place = await geocodeWeatherLocation(location);
  if (!place) return '';

  const params = new URLSearchParams({
    latitude: String(place.latitude),
    longitude: String(place.longitude),
    current: 'temperature_2m,apparent_temperature,relative_humidity_2m,precipitation,weather_code,wind_speed_10m',
    timezone: 'auto',
  });
  const data = await fetchJson(`https://api.open-meteo.com/v1/forecast?${params}`).catch(() => null);
  const current = data?.current;
  if (!current) return '';

  const english = isMostlyEnglishText(prompt);
  const temp = Math.round(current.temperature_2m);
  const feels = Math.round(current.apparent_temperature);
  const wind = Math.round(current.wind_speed_10m);
  const humidity = Math.round(current.relative_humidity_2m);
  const label = weatherCodeLabel(current.weather_code, english);
  const placeName = [place.name, place.admin1, place.country].filter(Boolean).slice(0, 3).join(', ');
  if (english) {
    return `Current weather in ${placeName}: ${temp} C, feels like ${feels} C, ${label}, wind ${wind} km/h, humidity ${humidity}%. Source: Open-Meteo.`;
  }
  return `Сейчас в ${placeName}: ${temp} градусов, ощущается как ${feels}, ${label}, ветер ${wind} км/ч, влажность ${humidity}%. Источник: Open-Meteo.`;
}

function isRequestTooLargeError(error) {
  const code = error?.error?.error?.code || error?.error?.code || error?.code;
  return error?.status === 413 || code === 'request_too_large' || /request entity too large/i.test(error?.message || '');
}

function isTransientGroqConnectionError(error) {
  const code = error?.cause?.code || error?.cause?.errno || error?.code;
  if (error?.status || error?.response?.status) return false;
  return [
    'ECONNRESET',
    'ETIMEDOUT',
    'EAI_AGAIN',
    'ECONNREFUSED',
    'UND_ERR_CONNECT_TIMEOUT',
    'UND_ERR_SOCKET',
  ].includes(code) || /connection error|network|fetch failed|socket|timeout/i.test(error?.message || '');
}

function webSearchModelsToTry(preferredModel) {
  return [...new Set([preferredModel || DEFAULT_WEB_SEARCH_MODEL, 'groq/compound'].filter(Boolean))];
}

function trimAssistantReply(text, limit = MAX_REPLY_CHARS) {
  let replyText = String(text || '').trim();
  if (replyText.length > limit) {
    replyText = `${replyText.slice(0, limit).replace(/\s+\S*$/, '').replace(/[,\s;:]+$/, '')}.`;
  }
  return replyText;
}

function textScriptStats(text) {
  const latin = (String(text || '').match(/[A-Za-z]/g) || []).length;
  const cyrillic = (String(text || '').match(/[А-Яа-яЁё]/g) || []).length;
  return { latin, cyrillic };
}

function isMostlyEnglishText(text) {
  const { latin, cyrillic } = textScriptStats(text);
  return latin >= 18 && latin > cyrillic * 1.4;
}

function personaInstruction() {
  switch (getAssistantPersona()) {
    case 'friendly':
      return 'Тон теплый, спокойный, поддерживающий, без канцелярита.';
    case 'sharp':
      return 'Тон живой, дерзкий и быстрый, можно дружески подкалывать, но без ненависти к защищенным группам.';
    case 'admin':
      return 'Тон как у практичного администратора Discord: четко, по делу, с приоритетом на действия и безопасность.';
    case 'quiet':
      return 'Отвечай максимально коротко, тихим стилем: одно предложение, без лишних деталей.';
    case 'english':
      return 'По умолчанию отвечай на English, но понимай Russian и mixed language.';
    default:
      return 'Тон естественный, как голосовой собеседник для Discord-сервера друзей.';
  }
}

async function askGroq(session, userName, prompt, actorMember = null) {
  const useWebSearch = shouldUseWebSearch(prompt);
  if (useWebSearch && isWeatherQuery(prompt)) {
    try {
      const weatherReply = await tryAnswerWeatherQuery(prompt);
      if (weatherReply) {
        const replyText = trimAssistantReply(weatherReply);
        session.history.push({ role: 'user', content: `${userName}: ${prompt}` });
        session.history.push({ role: 'assistant', content: replyText });
        session.history.splice(0, Math.max(0, session.history.length - 12));
        return replyText;
      }
    } catch (error) {
      console.warn(`weather fallback failed: ${error.message || error}`);
    }
  }
  const memoryContext = useWebSearch ? '' : formatMemoryContext(session.guild?.id, prompt, actorMember?.id || null);
  const messages = [
    {
      role: 'system',
      content:
        `Ты голосовой собеседник в Discord-канале. Твое имя: ${getAssistantName()}. `
        + 'Понимай русский, английский и смешанную речь. '
        + 'Если пользователь говорит в основном по-русски, отвечай по-русски, но нормально вставляй English words/terms. '
        + 'Если пользователь говорит в основном на English или просит answer in English, answer in English. '
        + 'Если вопрос смешанный, отвечай смешанно в том же стиле. Не используй markdown, списки и длинные ссылки, если пользователь явно не попросил. Ответ удобен для произнесения голосом. Максимум 1-3 коротких предложения. '
        + personaInstruction(),
    },
    ...(useWebSearch ? [{
      role: 'system',
      content:
        'Этот вопрос требует актуальной информации из интернета. Используй только web_search и visit_website. '
        + 'Ответь кратко на языке пользователя: Russian, English или mixed. Если точной информации нет, прямо скажи, что не нашел надежного подтверждения. '
        + 'В конце добавь короткую строку "Источники:" с 1-3 названиями сайтов или доменами, без длинных URL.',
    }] : []),
    ...(memoryContext ? [{
      role: 'system',
      content: `Локальная память этого Discord-сервера. Используй ее только если она помогает ответить, и не выдумывай факты вне памяти:\n${memoryContext}`,
    }] : []),
    ...(useWebSearch ? [] : session.history.slice(-8)),
    { role: 'user', content: `${userName}: ${prompt}` },
  ];

  let completion;
  const preferredModel = useWebSearch ? getWebSearchModel() : getChatModel();
  const modelsToTry = useWebSearch ? webSearchModelsToTry(preferredModel) : [preferredModel];
  let usedModel = preferredModel;
  let lastError = null;
  let webSearchRequestTooLarge = false;
  for (const model of modelsToTry) {
    usedModel = model;
    const request = {
      model,
      messages,
      temperature: useWebSearch ? 0.25 : 0.55,
      max_completion_tokens: useWebSearch ? 320 : 180,
    };
    if (useWebSearch) {
      request.compound_custom = {
        tools: {
          enabled_tools: ['web_search', 'visit_website'],
        },
      };
    }
    try {
      if (useWebSearch) console.log(`web search request model=${model} prompt=${prompt.slice(0, 160)}`);
      const result = await getGroqClient().chat.completions.create(request).withResponse();
      completion = result.data;
      trackGroqRateLimits(session.textChannel, useWebSearch ? 'web-search' : 'chat', result.response, model);
      break;
    } catch (error) {
      lastError = error;
      trackGroqRateLimits(session.textChannel, useWebSearch ? 'web-search' : 'chat', error, model);
      if (useWebSearch && isRequestTooLargeError(error)) {
        if (model !== 'groq/compound') {
          console.warn(`web search model ${model} failed with request_too_large, retrying groq/compound`);
          continue;
        }
        webSearchRequestTooLarge = true;
        console.warn('web search failed with request_too_large, falling back to regular chat model');
        break;
      }
      throw error;
    }
  }
  if (!completion && useWebSearch && webSearchRequestTooLarge) {
    usedModel = getChatModel();
    const result = await getGroqClient().chat.completions.create({
      model: usedModel,
      messages: [
        messages[0],
        {
          role: 'system',
          content:
            'Интернет-поиск у провайдера сейчас не прошел из-за ограничения размера запроса. '
            + 'Ответь кратко по общим знаниям и прямо скажи, если для точного ответа нужны актуальные данные.',
        },
        { role: 'user', content: `${userName}: ${prompt}` },
      ],
      temperature: 0.35,
      max_completion_tokens: 180,
    }).withResponse();
    completion = result.data;
    trackGroqRateLimits(session.textChannel, 'chat-fallback', result.response, usedModel);
  }
  if (!completion) throw lastError || new Error(`No completion returned from ${usedModel}`);

  const replyText = trimAssistantReply(completion.choices[0]?.message?.content || '');

  session.history.push({ role: 'user', content: `${userName}: ${prompt}` });
  session.history.push({ role: 'assistant', content: replyText });
  session.history.splice(0, Math.max(0, session.history.length - 12));
  return replyText;
}

async function generateIdleChatter(session) {
  const humanMembers = getHumanVoiceMembers(session);
  const names = displayMemberNames(humanMembers);
  if (!names.length) return '';

  const style = getIdleChatterStyle();
  const canUseWeb = isWebSearchEnabled() && isIdleChatterWebEnabled();
  const mode = (() => {
    if (style === 'news') return canUseWeb ? 'news' : 'facts';
    if (style === 'facts') return 'facts';
    if (style === 'roast') return 'roast';
    if (style === 'context') return 'context';
    const variants = canUseWeb ? ['roast', 'context', 'facts', 'news'] : ['roast', 'context', 'facts'];
    return variants[Math.floor(Math.random() * variants.length)];
  })();
  const memoryContext = formatMemoryContext(session.guild?.id, names.join(' '));
  const recentContext = session.history
    .slice(-6)
    .map((item) => `${item.role}: ${item.content}`)
    .join('\n');
  const isWebMode = mode === 'news' && canUseWeb;
  const model = isWebMode ? getWebSearchModel() : getChatModel();
  const modeInstruction = {
    roast: 'Сделай дерзкий дружеский подкол по никам участников или ситуации в войсе.',
    context: 'Зацепись за локальную память или недавний контекст беседы и кинь смешной комментарий.',
    facts: 'Расскажи неожиданный интересный факт или короткую абсурдную мысль, можно не про участников.',
    news: 'Найди свежую интересную новость из мира и перескажи ее одной живой фразой.',
  }[mode] || 'Скажи живую фразу для продолжения беседы.';
  const prompt = [
    'Сервер закрытый, люди свои. Стиль можно делать острее: сарказм, дружеский roast, черный юмор без занудства.',
    modeInstruction,
    'Можно шутить не только о пользователях, а вообще о чем угодно. Можно использовать видимые ники, локальную память и недавний контекст.',
    'Можно говорить по-русски, English или mixed, если так звучит смешнее или естественнее.',
    'Не произноси токены, API-ключи, пароли и длинные секретные строки целиком. Не скатывайся в угрозы и ненависть по национальности, расе, религии, полу, ориентации, инвалидности или болезням.',
    'Без markdown. Максимум 1-2 коротких предложения, чтобы это нормально звучало голосом.',
    `Участники в voice: ${names.join(', ')}.`,
    memoryContext ? `Локальная память:\n${memoryContext}` : '',
    recentContext ? `Недавний контекст:\n${recentContext}` : '',
  ].filter(Boolean).join('\n');

  try {
    const request = {
      model,
      messages: [
        {
          role: 'system',
          content: 'Ты голосовой собеседник для закрытого Discord-сервера друзей. Говори живо, дерзко, коротко и смешно, как свой человек.',
        },
        { role: 'user', content: prompt },
      ],
      temperature: mode === 'news' ? 0.35 : 0.9,
      max_completion_tokens: mode === 'news' ? 170 : 130,
    };
    if (isWebMode) {
      request.compound_custom = {
        tools: {
          enabled_tools: ['web_search', 'visit_website'],
        },
      };
    }
    const result = await getGroqClient().chat.completions.create(request).withResponse();
    trackGroqRateLimits(session.textChannel, `idle-chatter-${mode}`, result.response, model);
    return trimAssistantReply(result.data?.choices?.[0]?.message?.content || '', 320);
  } catch (error) {
    trackGroqRateLimits(session.textChannel, `idle-chatter-${mode}`, error, model);
    throw error;
  }
}

async function maybeRunIdleChatter() {
  if (!isBotEnabled() || !isIdleChatterEnabled()) return;
  const idleMs = getIdleChatterMinutes() * 60_000;

  for (const session of sessions.values()) {
    if (!session?.connection || session.connection.state.status === VoiceConnectionStatus.Destroyed) continue;
    if (isListeningPaused(session) || session.busy || session.interruptBusy || session.activeUsers?.size) continue;
    if (session.player?.state?.status === AudioPlayerStatus.Playing) continue;
    if (!getHumanVoiceMembers(session).length) continue;

    const now = Date.now();
    const lastHumanActivityAt = session.lastHumanActivityAt || session.joinedAt || now;
    const lastIdleChatterAt = session.lastIdleChatterAt || 0;
    if (now - lastHumanActivityAt < idleMs) continue;
    if (now - lastIdleChatterAt < idleMs) continue;

    session.busy = true;
    const turnId = beginCancellableTurn(session);
    session.lastIdleChatterAt = now;
    try {
      const text = await generateIdleChatter(session);
      if (isTurnCancelled(session, turnId)) continue;
      if (!text) continue;
      console.log(`idle chatter: ${text}`);
      await sendText(session.textChannel, `🤖 ${text}`);
      if (isTurnCancelled(session, turnId)) continue;
      await speak(session, text);
      session.lastReplyAt = Date.now();
      session.lastHumanActivityAt = session.lastReplyAt;
      if (session.diagnostics) {
        session.diagnostics.lastAnswerAt = session.lastReplyAt;
      }
    } catch (error) {
      if (session.diagnostics) session.diagnostics.lastError = error.message || String(error);
      console.error('idle chatter failed:', error);
    } finally {
      session.busy = false;
    }
  }
}

function buildIdleLeavePhrase() {
  const custom = getIdleLeavePhrase();
  if (custom) return custom;

  const variants = [
    'Ну всё, я понял, меня тут держат как мебель. Обиделся и ухожу.',
    'Час меня никто не трогал. Ладно, буду страдать в цифровом одиночестве. Ушел.',
    'Я тут час ждал внимания, но вы сильные и независимые. Покидаю комнату.',
    'Понял намек. Если что, я не плачу, это просто нейросеть перегрелась. Ушел.',
    'Раз я никому не нужен, красиво исчезаю из войса.',
  ];
  return variants[Math.floor(Math.random() * variants.length)];
}

async function maybeRunIdleLeave() {
  if (!isBotEnabled() || !isIdleLeaveEnabled()) return;
  const idleMs = getIdleLeaveMinutes() * 60_000;
  const now = Date.now();

  for (const [guildId, session] of sessions.entries()) {
    if (!session?.connection || session.connection.state.status === VoiceConnectionStatus.Destroyed) continue;
    if (session.idleLeaveInProgress || session.busy || session.interruptBusy || session.activeUsers?.size) continue;
    if (session.player?.state?.status === AudioPlayerStatus.Playing) continue;

    const lastAssistantInteractionAt = session.lastAssistantInteractionAt || session.joinedAt || now;
    if (now - lastAssistantInteractionAt < idleMs) continue;

    session.idleLeaveInProgress = true;
    session.busy = true;
    const turnId = beginCancellableTurn(session);
    const phrase = buildIdleLeavePhrase();
    try {
      console.log(`idle leave triggered guild=${guildId} voice=${session.voiceChannel?.id || 'unknown'}`);
      appendEvent('idle_leave_triggered', {
        guildId,
        voiceChannelId: session.voiceChannel?.id,
        minutes: getIdleLeaveMinutes(),
        lastAssistantInteractionAt,
      });
      await sendText(session.textChannel, `🤖 ${phrase}`);
      if (!isTurnCancelled(session, turnId)) {
        await speak(session, phrase).catch((error) => {
          console.error('idle leave speak failed:', error);
          if (session.diagnostics) session.diagnostics.lastError = error.message || String(error);
        });
      }
    } catch (error) {
      if (session.diagnostics) session.diagnostics.lastError = error.message || String(error);
      console.error('idle leave failed:', error);
    } finally {
      autoJoinSuppressedUntilManualJoin = true;
      if (session.connection && session.connection.state.status !== VoiceConnectionStatus.Destroyed) {
        session.connection.destroy();
      }
      sessions.delete(guildId);
      appendEvent('voice_left_idle', {
        guildId,
        voiceChannelId: session.voiceChannel?.id,
        minutes: getIdleLeaveMinutes(),
      });
      session.busy = false;
      session.idleLeaveInProgress = false;
      await writeStatusSnapshot();
    }
  }
}

async function runHealthCheck() {
  if (!isHealthcheckEnabled() || !isBotEnabled() || healthcheckInProgress) return;
  healthcheckInProgress = true;
  try {
    let removed = 0;
    for (const [key, session] of sessions.entries()) {
      cleanupStaleActiveCaptures(session);
      if (session?.connection?.state?.status === VoiceConnectionStatus.Destroyed) {
        sessions.delete(key);
        removed += 1;
        appendEvent('healthcheck_removed_dead_session', {
          guildId: session.guild?.id,
          voiceChannelId: session.voiceChannel?.id,
        });
      }
    }

    if (!sessions.size && hasConfiguredAutoJoin() && !autoJoinInProgress && !autoJoinSuppressedUntilManualJoin) {
      autoJoinInProgress = true;
      try {
        await autoJoinConfiguredVoice();
        appendEvent('healthcheck_auto_joined', {
          guildId: AUTO_JOIN_GUILD_ID,
          voiceChannelId: AUTO_JOIN_VOICE_CHANNEL_ID,
        });
      } finally {
        autoJoinInProgress = false;
      }
    }

    if (removed) await writeStatusSnapshot();
  } catch (error) {
    appendEvent('healthcheck_error', { message: error.message || String(error) });
    console.error('healthcheck failed:', error);
  } finally {
    healthcheckInProgress = false;
  }
}

async function runCommand(command, args, label) {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: ['ignore', 'ignore', 'pipe'],
    });
    let stderr = '';
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });
    child.on('error', (error) => {
      if (error.code === 'ENOENT') {
        reject(new Error(`${label || command} is not installed or not in PATH`));
      } else {
        reject(error);
      }
    });
    child.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(stderr || `${label || command} exited with code ${code}`));
    });
  });
}

function edgeTtsCommandCandidates() {
  return [
    EDGE_TTS_COMMAND,
    path.join(__dirname, '.venv', 'bin', 'edge-tts'),
    '/opt/edge-tts/bin/edge-tts',
    'edge-tts',
  ].filter(Boolean);
}

async function runFirstAvailableCommand(commands, args, label) {
  let lastError = null;
  for (const command of commands) {
    try {
      await runCommand(command, args, label);
      return command;
    } catch (error) {
      lastError = error;
      if (!/not installed|ENOENT|no such file/i.test(error.message || '')) throw error;
    }
  }
  throw lastError || new Error(`${label} is not installed`);
}

async function convertToDiscordWav(inputPath, outputPath) {
  if (process.platform === 'darwin') {
    await runCommand('afconvert', ['-f', 'WAVE', '-d', 'LEI16@48000', inputPath, outputPath], 'afconvert');
    return;
  }
  await runCommand(
    'ffmpeg',
    ['-y', '-hide_banner', '-loglevel', 'error', '-i', inputPath, '-ac', '2', '-ar', '48000', '-sample_fmt', 's16', outputPath],
    'ffmpeg',
  );
}

async function synthesizeMacOS(text) {
  const filename = path.join(tmpDir, `tts-${Date.now()}-${Math.random().toString(16).slice(2)}.wav`);
  await runCommand('say', ['-v', getMacosVoice(), '-o', filename, '--data-format=LEI16@48000', text], 'macOS say');
  return filename;
}

async function synthesizeEspeak(text) {
  const id = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const rawPath = path.join(tmpDir, `tts-${id}-raw.wav`);
  const filename = path.join(tmpDir, `tts-${id}.wav`);

  try {
    await runCommand(
      'espeak-ng',
      ['-v', getEspeakVoice(), '-s', String(getEspeakSpeed()), '-w', rawPath, text],
      'espeak-ng',
    );
    await runCommand(
      'ffmpeg',
      ['-y', '-hide_banner', '-loglevel', 'error', '-i', rawPath, '-ac', '2', '-ar', '48000', '-sample_fmt', 's16', filename],
      'ffmpeg',
    );
    return filename;
  } finally {
    fs.unlink(rawPath).catch(() => {});
  }
}

async function synthesizeEdge(text) {
  const id = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const textPath = path.join(tmpDir, `tts-${id}.txt`);
  const mediaPath = path.join(tmpDir, `tts-${id}.mp3`);
  const filename = path.join(tmpDir, `tts-${id}.wav`);

  try {
    await fs.writeFile(textPath, text);
    await runFirstAvailableCommand(
      edgeTtsCommandCandidates(),
      [
        '--voice', getEdgeVoiceForText(text),
        '--rate', getEdgeRate(),
        '--pitch', getEdgePitch(),
        '--file', textPath,
        '--write-media', mediaPath,
      ],
      'edge-tts',
    );
    await convertToDiscordWav(mediaPath, filename);
    return filename;
  } finally {
    fs.unlink(textPath).catch(() => {});
    fs.unlink(mediaPath).catch(() => {});
  }
}

async function synthesizeSpeech(text) {
  const provider = getTtsProvider();
  switch (provider) {
    case 'edge':
    case 'edge-tts':
    case 'microsoft':
      return synthesizeEdge(text);
    case 'macos':
      return synthesizeMacOS(text);
    case 'espeak':
    case 'linux':
      return synthesizeEspeak(text);
    default:
      throw new Error(`Unsupported TTS_PROVIDER="${provider}". Use "edge", "macos" or "espeak".`);
  }
}

function streamPcm(pcm) {
  function* chunks() {
    for (let offset = 0; offset < pcm.length; offset += 3840) {
      yield pcm.subarray(offset, Math.min(offset + 3840, pcm.length));
    }
  }
  return Readable.from(chunks());
}

async function speak(session, text) {
  if (!session.connection || session.connection.state.status === VoiceConnectionStatus.Destroyed) return;

  const speechVersion = beginSpeech(session);
  const wavPath = await synthesizeSpeech(text);
  try {
    if (isSpeechCancelled(session, speechVersion)) return;
    const wav = await fs.readFile(wavPath);
    if (isSpeechCancelled(session, speechVersion)) return;
    const pcm = parseWav(wav);
    if (isSpeechCancelled(session, speechVersion)) return;
    const resource = createAudioResource(streamPcm(pcm), { inputType: StreamType.Raw });

    await new Promise((resolve, reject) => {
      const onError = (error) => {
        cleanup();
        reject(error);
      };
      const onIdle = () => {
        cleanup();
        resolve();
      };
      const cleanup = () => {
        session.player.off('error', onError);
        session.player.off(AudioPlayerStatus.Idle, onIdle);
      };
      session.player.once('error', onError);
      session.player.once(AudioPlayerStatus.Idle, onIdle);
      if (isSpeechCancelled(session, speechVersion)) {
        cleanup();
        resolve();
        return;
      }
      session.player.play(resource);
    });
  } finally {
    fs.unlink(wavPath).catch(() => {});
  }
}

async function captureUser(session, userId) {
  cleanupStaleActiveCaptures(session);
  if (!isBotEnabled()) {
    markIgnored(session, 'bot_disabled');
    return;
  }
  if (Date.now() < session.listenAfter) {
    markIgnored(session, 'join_warmup');
    return;
  }
  if (session.activeUsers.has(userId)) {
    markIgnored(session, 'already_capturing_user');
    return;
  }
  if (userId === client.user.id) {
    markIgnored(session, 'self_voice');
    return;
  }
  const busyAtStart = session.busy;
  if (busyAtStart && session.interruptBusy) {
    markIgnored(session, 'busy_interrupt_in_progress');
    return;
  }
  session.activeUsers.add(userId);
  session.activeUserStartedAt ||= new Map();
  session.activeUserStartedAt.set(userId, Date.now());

  let member = session.guild.members.cache.get(userId);
  if (!member) member = await session.guild.members.fetch(userId).catch(() => null);
  if (member?.user?.bot) {
    session.activeUsers.delete(userId);
    session.activeUserStartedAt?.delete(userId);
    markIgnored(session, 'speaker_is_bot');
    return;
  }

  logVoiceDebug(`capture start user=${userId}`);
  const opusStream = session.connection.receiver.subscribe(userId, {
    end: { behavior: EndBehaviorType.AfterSilence, duration: SILENCE_MS },
  });
  const decoder = new prism.opus.Decoder({ rate: 48000, channels: 2, frameSize: 960 });
  const chunks = [];
  let finished = false;

  const hardTimeout = setTimeout(() => {
    if (!finished) {
      const error = captureTimeoutError();
      opusStream.destroy(error);
      decoder.destroy(error);
    }
  }, MAX_UTTERANCE_MS);

  decoder.on('data', (chunk) => chunks.push(chunk));
  decoder.on('error', (error) => {
    if (!isExpectedReceiveClose(error)) console.error(`decoder error user=${userId}:`, error);
  });
  opusStream.on('error', (error) => {
    if (!isExpectedReceiveClose(error)) console.error(`opus receive error user=${userId}:`, error);
  });

  try {
    await pipeline(opusStream, decoder);
  } catch (error) {
    if (!isExpectedReceiveClose(error)) {
      console.error(`receive pipeline failed user=${userId}:`, error);
    }
  } finally {
    finished = true;
    clearTimeout(hardTimeout);
    session.activeUsers.delete(userId);
    session.activeUserStartedAt?.delete(userId);
  }

  const pcm = Buffer.concat(chunks);
  const { durationMs, rms } = pcmStats(pcm);
  if (session.diagnostics) {
    session.diagnostics.captures += 1;
    session.diagnostics.lastCaptureAt = Date.now();
    session.diagnostics.lastCaptureStats = {
      userId,
      bytes: pcm.length,
      durationMs: Math.round(durationMs),
      rms: Math.round(rms),
    };
  }
  logVoiceDebug(`capture end user=${userId} bytes=${pcm.length} duration=${durationMs.toFixed(0)}ms rms=${rms.toFixed(0)}`);

  if (durationMs < MIN_AUDIO_MS) {
    markIgnored(session, 'too_short', { lastTranscript: null });
    return;
  }
  if (rms < MIN_RMS) {
    markIgnored(session, 'too_quiet', { lastTranscript: null });
    return;
  }

  if (busyAtStart || session.busy) {
    if (session.interruptBusy) return;
    session.interruptBusy = true;
    try {
      const transcript = await transcribePcm(pcm, userId, session);
      if (session.diagnostics) session.diagnostics.lastTranscript = transcript || null;
      if (!transcript) {
        markIgnored(session, 'empty_transcript');
        return;
      }
      if (!shouldAnswer(transcript, session)) {
        markIgnored(session, 'no_wake_word', { lastTranscript: transcript });
        return;
      }
      const prompt = promptFromTranscript(session, transcript);
      markAssistantInteraction(session, 'voice_interrupt');
      const simpleAction = parseSimpleAction(prompt);
      if (!simpleAction || !['stop_speaking', 'pause_listening', 'resume_listening'].includes(simpleAction.action)) {
        markIgnored(session, 'busy_non_interrupt_action', { lastTranscript: transcript });
        return;
      }

      console.log(`interrupt transcript user=${userId}: ${transcript}`);
      appendEvent('voice_interrupt', {
        guildId: session.guild?.id,
        voiceChannelId: session.voiceChannel?.id,
        userId,
        transcript,
        prompt,
      });
      const actionResult = await tryHandleVoiceAction(session, member, prompt);
      if (!actionResult) return;

      const actionText = typeof actionResult === 'string' ? actionResult : actionResult.text;
      const shouldSend = typeof actionResult === 'string' || actionResult.send !== false;
      if (shouldSend) await sendText(session.textChannel, `🤖 ${actionText}`);
      session.lastReplyAt = Date.now();
      if (session.diagnostics) session.diagnostics.lastAnswerAt = session.lastReplyAt;
    } catch (error) {
      if (session.diagnostics) session.diagnostics.lastError = error.message || String(error);
      console.error('interrupt processing failed:', error);
    } finally {
      session.interruptBusy = false;
    }
    return;
  }

  if (Date.now() - session.lastReplyAt < REPLY_COOLDOWN_MS) {
    markIgnored(session, 'cooldown');
    logVoiceDebug(`capture skipped by cooldown user=${userId}`);
    return;
  }

  if (session.busy) {
    markIgnored(session, 'busy');
    return;
  }

  const turnStartedAt = Date.now();
  const timings = {};
  const turnId = beginCancellableTurn(session);
  session.busy = true;
  session.queue = session.queue
    .then(async () => {
      const sttStartedAt = Date.now();
      const transcript = await transcribePcm(pcm, userId, session);
      timings.stt = Date.now() - sttStartedAt;
      if (session.diagnostics) session.diagnostics.lastTranscript = transcript || null;
      if (!transcript) {
        markIgnored(session, 'empty_transcript');
        return;
      }
      if (!shouldAnswer(transcript, session)) {
        markIgnored(session, 'no_wake_word', { lastTranscript: transcript });
        return;
      }

      const prompt = promptFromTranscript(session, transcript);
      markAssistantInteraction(session, 'voice');
      if (getWakeWord() && !LISTEN_WITHOUT_WAKE_WORD && hasWakeWord(transcript) && !prompt) {
        markIgnored(session, 'wake_without_prompt', { lastTranscript: transcript });
        await sendText(session.textChannel, `Слушаю. Скажи вопрос после слова "${getWakeWord()}".`);
        return;
      }
      markActiveDialogue(session);
      const userName = member?.displayName || member?.user?.username || userId;
      console.log(`transcript user=${userId}: ${transcript}`);
      appendEvent('voice_transcript', {
        guildId: session.guild?.id,
        voiceChannelId: session.voiceChannel?.id,
        userId,
        userName,
        transcript,
        prompt,
        wake: hasWakeWord(transcript),
      });
      await sendText(session.textChannel, `🎙️ <@${userId}>: ${prompt}`);

      const actionStartedAt = Date.now();
      const actionResult = await tryHandleVoiceAction(session, member, prompt);
      timings.action = Date.now() - actionStartedAt;
      if (actionResult) {
        if (isTurnCancelled(session, turnId) && parseSimpleAction(prompt)?.action !== 'stop_speaking') return;
        const actionText = typeof actionResult === 'string' ? actionResult : actionResult.text;
        const shouldSpeak = typeof actionResult === 'string' || actionResult.speak !== false;
        const shouldSend = typeof actionResult === 'string' || actionResult.send !== false;
        console.log(`action result: ${actionText}`);
        appendEvent('voice_action', {
          guildId: session.guild?.id,
          voiceChannelId: session.voiceChannel?.id,
          userId,
          prompt,
          result: actionText,
        });
        if (shouldSend) await sendText(session.textChannel, `🤖 ${actionText}`);
        if (shouldSpeak && !isTurnCancelled(session, turnId)) {
          const ttsStartedAt = Date.now();
          await speak(session, actionText);
          timings.tts = Date.now() - ttsStartedAt;
        }
        session.lastReplyAt = Date.now();
        if (session.diagnostics) {
          session.diagnostics.lastAnswerAt = session.lastReplyAt;
          session.diagnostics.lastTimingsMs = { ...timings, total: Date.now() - turnStartedAt };
        }
        return;
      }

      if (isListeningPaused(session)) {
        const text = `Голосовая обработка на паузе. Скажи: "${getWakeWord()} продолжай".`;
        await sendText(session.textChannel, `🤖 ${text}`);
        session.lastReplyAt = Date.now();
        if (session.diagnostics) {
          session.diagnostics.lastAnswerAt = session.lastReplyAt;
          session.diagnostics.lastTimingsMs = { ...timings, total: Date.now() - turnStartedAt };
        }
        return;
      }

      const chatStartedAt = Date.now();
      const answer = await askGroq(session, userName, prompt, member);
      timings.chat = Date.now() - chatStartedAt;
      if (!answer) return;
      if (isTurnCancelled(session, turnId)) return;

      console.log(`assistant: ${answer}`);
      appendEvent('assistant_answer', {
        guildId: session.guild?.id,
        voiceChannelId: session.voiceChannel?.id,
        userId,
        prompt,
        answer,
        web: shouldUseWebSearch(prompt),
      });
      await sendText(session.textChannel, `🤖 ${answer}`);
      if (isTurnCancelled(session, turnId)) return;
      const ttsStartedAt = Date.now();
      await speak(session, answer);
      timings.tts = Date.now() - ttsStartedAt;
      session.lastReplyAt = Date.now();
      if (session.diagnostics) {
        session.diagnostics.lastAnswerAt = session.lastReplyAt;
        session.diagnostics.lastTimingsMs = { ...timings, total: Date.now() - turnStartedAt };
      }
    })
    .catch((error) => {
      if (session.diagnostics) session.diagnostics.lastError = error.message || String(error);
      console.error('processing failed:', error);
      sendText(session.textChannel, `Ошибка обработки речи: \`${error.message || error}\``);
    })
    .finally(() => {
      session.busy = false;
    });
}

async function connectVoiceSession({ guild, textChannel, voiceChannel, noticeChannel = textChannel }) {
  autoJoinSuppressedUntilManualJoin = false;
  const old = sessions.get(guild.id);
  if (old?.connection && old.connection.state.status !== VoiceConnectionStatus.Destroyed) old.connection.destroy();

  const player = createAudioPlayer({
    behaviors: { noSubscriber: NoSubscriberBehavior.Play },
  });
  const connection = joinVoiceChannel({
    channelId: voiceChannel.id,
    guildId: guild.id,
    adapterCreator: guild.voiceAdapterCreator,
    selfDeaf: false,
    selfMute: false,
    daveEncryption: true,
    debug: true,
  });

  connection.on('stateChange', (oldState, newState) => {
    console.log(`voice state ${oldState.status} -> ${newState.status}`);
  });
  if (VOICE_DEBUG) connection.on('debug', (message) => console.log(`voice debug: ${message}`));
  connection.on('error', (error) => {
    console.error('voice connection error:', error);
    void sendMonitorNotice(`Voice connection error: \`${error.message || error}\``, noticeChannel).catch(() => {});
  });
  player.on('error', (error) => {
    console.error('audio player error:', error);
    void sendMonitorNotice(`Audio player error: \`${error.message || error}\``, noticeChannel).catch(() => {});
  });

  connection.subscribe(player);
  await entersState(connection, VoiceConnectionStatus.Ready, 30_000);

  const session = {
    sessionKey: guild.id,
    guild,
    textChannel,
    voiceChannel,
    connection,
    player,
    activeUsers: new Set(),
    activeUserStartedAt: new Map(),
    knownVoiceMemberIds: new Set(),
    history: [],
    queue: Promise.resolve(),
    presenceQueue: Promise.resolve(),
    presenceEventTimes: new Map(),
    busy: false,
    interruptBusy: false,
    paused: false,
    pendingAction: null,
    lastReplyAt: 0,
    activeDialogueUntil: 0,
    currentTurnId: 0,
    cancelCurrentTurn: false,
    speechVersion: 0,
    stopSpeechRequested: false,
    joinedAt: Date.now(),
    lastHumanActivityAt: Date.now(),
    lastAssistantInteractionAt: Date.now(),
    lastAssistantInteractionSource: 'join',
    lastIdleChatterAt: 0,
    listenAfter: Date.now() + IGNORE_AFTER_JOIN_MS,
    diagnostics: createVoiceDiagnostics(),
  };
  session.knownVoiceMemberIds = new Set(getHumanVoiceMembers(session).map((member) => member.id));
  sessions.set(guild.id, session);

  connection.receiver.speaking.on('start', (userId) => {
    if (userId !== client.user.id) session.lastHumanActivityAt = Date.now();
    if (session.diagnostics) {
      session.diagnostics.voiceEvents += 1;
      session.diagnostics.lastVoiceEventAt = Date.now();
    }
    captureUser(session, userId);
  });
  console.log(`joined voice channel ${voiceChannel.name} (${voiceChannel.id})`);
  appendEvent('voice_joined', {
    guildId: guild.id,
    guildName: guild.name,
    textChannelId: textChannel.id,
    voiceChannelId: voiceChannel.id,
    voiceChannelName: voiceChannel.name,
  });
  enqueuePresenceAnnouncement(session, buildBotJoinAnnouncement(session), `bot_join:${voiceChannel.id}:${session.joinedAt}`);
  return session;
}

async function autoJoinConfiguredVoice() {
  if (!hasConfiguredAutoJoin()) return;
  if (!isBotEnabled()) return;

  const guild = await client.guilds.fetch(AUTO_JOIN_GUILD_ID);
  const [voiceChannel, textChannel] = await Promise.all([
    guild.channels.fetch(AUTO_JOIN_VOICE_CHANNEL_ID),
    guild.channels.fetch(AUTO_JOIN_TEXT_CHANNEL_ID),
  ]);

  if (![ChannelType.GuildVoice, ChannelType.GuildStageVoice].includes(voiceChannel?.type)) {
    throw new Error(`AUTO_JOIN_VOICE_CHANNEL_ID is not a voice/stage channel: ${AUTO_JOIN_VOICE_CHANNEL_ID}`);
  }
  if (!textChannel?.isTextBased?.()) {
    throw new Error(`AUTO_JOIN_TEXT_CHANNEL_ID is not text based: ${AUTO_JOIN_TEXT_CHANNEL_ID}`);
  }

  setMonitorChannel(textChannel);
  await connectVoiceSession({ guild, textChannel, voiceChannel, noticeChannel: textChannel });
  await sendText(textChannel, `🤖 Автоподключился к \`${voiceChannel.name}\`. Триггер: "${getWakeWord() || 'выключен'}".`);
}

async function registerCommands() {
  const commands = [
    new SlashCommandBuilder().setName('join').setDescription('Подключить голосового собеседника к вашему voice channel'),
    new SlashCommandBuilder().setName('leave').setDescription('Отключить голосового собеседника'),
    new SlashCommandBuilder()
      .setName('ask')
      .setDescription('Текстовый вопрос; если бот в voice, он ответит голосом')
      .addStringOption((option) => option.setName('text').setDescription('Вопрос').setRequired(true)),
    new SlashCommandBuilder()
      .setName('disconnect')
      .setDescription('Отключить участника от голосового канала')
      .setDefaultMemberPermissions(PermissionFlagsBits.MoveMembers)
      .addUserOption((option) => option.setName('user').setDescription('Кого отключить').setRequired(true)),
    new SlashCommandBuilder().setName('actions').setDescription('Показать голосовые действия ассистента'),
    new SlashCommandBuilder().setName('limits').setDescription('Показать последние известные лимиты Groq API'),
    new SlashCommandBuilder().setName('stop').setDescription('Остановить текущую голосовую речь бота'),
    new SlashCommandBuilder().setName('reset').setDescription('Сбросить память текущего диалога'),
    new SlashCommandBuilder()
      .setName('remember')
      .setDescription('Записать факт в локальную память')
      .addStringOption((option) => option.setName('text').setDescription('Что запомнить').setRequired(true)),
    new SlashCommandBuilder().setName('memories').setDescription('Показать последние записи локальной памяти'),
    new SlashCommandBuilder()
      .setName('remind')
      .setDescription('Создать напоминание через N минут')
      .addIntegerOption((option) => option.setName('minutes').setDescription('Через сколько минут').setRequired(true).setMinValue(1).setMaxValue(10080))
      .addStringOption((option) => option.setName('text').setDescription('Что напомнить').setRequired(true)),
    new SlashCommandBuilder().setName('reminders').setDescription('Показать активные напоминания'),
    new SlashCommandBuilder().setName('pause').setDescription('Поставить голосовую обработку на паузу'),
    new SlashCommandBuilder().setName('resume').setDescription('Продолжить голосовую обработку'),
    new SlashCommandBuilder().setName('status').setDescription('Показать статус голосового собеседника'),
  ].map((command) => command.toJSON());

  if (DISCORD_GUILD_ID) {
    const guild = await client.guilds.fetch(DISCORD_GUILD_ID);
    await guild.commands.set(commands);
    console.log(`Registered guild slash commands for ${DISCORD_GUILD_ID}`);
  } else {
    await client.application.commands.set(commands);
    console.log('Registered global slash commands');
  }
}

async function handleVoicePresenceChange(oldState, newState) {
  const guildId = newState.guild?.id || oldState.guild?.id;
  const session = sessions.get(guildId);
  if (!session || !isSessionVoiceReady(session)) return;

  const oldChannelId = oldState.channelId || null;
  const newChannelId = newState.channelId || null;
  if (oldChannelId === newChannelId) return;

  const watchedChannelId = session.voiceChannel?.id;
  if (!watchedChannelId) return;

  const userId = newState.id || oldState.id;
  if (!userId || userId === client.user.id) return;

  let member = newState.member || oldState.member || session.guild.members.cache.get(userId);
  if (!member) member = await session.guild.members.fetch(userId).catch(() => null);
  if (!member || member.user?.bot) return;

  const joinedWatchedChannel = newChannelId === watchedChannelId && oldChannelId !== watchedChannelId;
  const leftWatchedChannel = oldChannelId === watchedChannelId && newChannelId !== watchedChannelId;
  if (!joinedWatchedChannel && !leftWatchedChannel) return;

  session.lastHumanActivityAt = Date.now();
  if (joinedWatchedChannel) {
    session.knownVoiceMemberIds?.add(member.id);
    enqueuePresenceAnnouncement(
      session,
      buildMemberJoinAnnouncement(member),
      `member_join:${watchedChannelId}:${member.id}`,
    );
  } else {
    session.knownVoiceMemberIds?.delete(member.id);
    if (getHumanVoiceMembers(session).length) {
      enqueuePresenceAnnouncement(
        session,
        buildMemberLeaveAnnouncement(member),
        `member_leave:${watchedChannelId}:${member.id}`,
      );
    }
  }

  await writeStatusSnapshot();
}

client.once('clientReady', async () => {
  console.log(`Logged in as ${client.user.tag} (${client.user.id})`);
  await registerCommands();
  schedulePendingReminders();
  await saveRuntimeConfig();
  await autoJoinConfiguredVoice().catch((error) => console.error('auto join failed:', error));
  await writeStatusSnapshot();
});

client.on('voiceStateUpdate', (oldState, newState) => {
  handleVoicePresenceChange(oldState, newState).catch((error) => {
    console.error('voice presence change failed:', error);
  });
});

client.on('interactionCreate', async (interaction) => {
  if (!interaction.isChatInputCommand()) return;
  setMonitorChannel(interaction.channel);

  try {
    if (interaction.commandName !== 'join') {
      const activeSession = getInteractionSession(interaction);
      if (activeSession) markAssistantInteraction(activeSession, `slash:${interaction.commandName}`);
    }

    if (interaction.commandName === 'join') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      if (!isBotEnabled()) {
        await reply(interaction, 'Бот выключен в веб-панели.');
        return;
      }
      const voiceChannel = interaction.member?.voice?.channel;
      if (!voiceChannel) {
        await reply(interaction, 'Сначала зайди в голосовой канал.');
        return;
      }

      const old = getInteractionSession(interaction);
      if (old?.connection) old.connection.destroy();

      const session = await connectVoiceSession({
        guild: interaction.guild,
        textChannel: interaction.channel,
        voiceChannel,
        noticeChannel: interaction.channel,
      });
      markAssistantInteraction(session, 'slash:join');
      await reply(
        interaction,
        `Слушаю \`${voiceChannel.name}\`. Триггер: "${getWakeWord() || 'выключен'}". Для действия скажи: "${getWakeWord()} отключи имя".`,
      );
    }

    if (interaction.commandName === 'leave') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      const session = getInteractionSession(interaction);
      const connection = getVoiceConnection(interaction.guildId);
      autoJoinSuppressedUntilManualJoin = true;
      if (session?.connection) session.connection.destroy();
      else if (connection) connection.destroy();
      sessions.delete(interaction.guildId);
      await reply(interaction, 'Отключился.');
    }

    if (interaction.commandName === 'ask') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      const text = interaction.options.getString('text', true);
      const session = getInteractionSession(interaction) || {
        guild: interaction.guild,
        textChannel: interaction.channel,
        voiceChannel: interaction.member?.voice?.channel ?? null,
        connection: null,
        player: null,
        activeUsers: new Set(),
        history: [],
        queue: Promise.resolve(),
        busy: false,
        interruptBusy: false,
        paused: false,
        lastReplyAt: 0,
        listenAfter: 0,
      };
      const answer = await askGroq(session, interaction.member?.displayName || interaction.user.username, text, interaction.member);
      await reply(interaction, answer);
      if (session.connection && session.player) await speak(session, answer);
    }

    if (interaction.commandName === 'disconnect') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      const actorMember = interaction.member;
      const user = interaction.options.getUser('user', true);
      const targetMember = await interaction.guild.members.fetch(user.id).catch(() => null);
      if (!targetMember) {
        await reply(interaction, 'Не нашел этого участника на сервере.');
        return;
      }

      const result = await disconnectMember(
        targetMember,
        actorMember,
        `Slash command by ${interaction.user.tag}`,
      );
      await reply(interaction, result);
    }

    if (interaction.commandName === 'actions') {
      const prefix = getWakeWord() || 'бот';
      await reply(interaction, `Голосовые действия через "${prefix}":\n${ACTION_HELP.map((item) => `• ${prefix} ${item}`).join('\n')}`);
    }

    if (interaction.commandName === 'limits') {
      await reply(interaction, `Groq API limits:\n${formatGroqLimits()}`);
    }

    if (interaction.commandName === 'stop') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      const session = getInteractionSession(interaction);
      const stopped = stopPlayback(session);
      await reply(interaction, stopped ? 'Остановил текущую речь.' : 'Сейчас нечего останавливать.');
    }

    if (interaction.commandName === 'reset') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      const session = getInteractionSession(interaction);
      if (session?.history) session.history.splice(0);
      await reply(interaction, 'Сбросил память текущего диалога.');
    }

    if (interaction.commandName === 'remember') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      const text = interaction.options.getString('text', true);
      addMemoryItem(interaction.guildId, interaction.member, text);
      await reply(interaction, 'Запомнил.');
    }

    if (interaction.commandName === 'memories') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      await reply(interaction, `Память:\n${formatMemoryList(interaction.guildId, interaction.member?.id)}`);
    }

    if (interaction.commandName === 'remind') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      const minutes = interaction.options.getInteger('minutes', true);
      const text = interaction.options.getString('text', true);
      const session = getInteractionSession(interaction) || {
        guild: interaction.guild,
        textChannel: interaction.channel,
      };
      const reminder = addReminderItem(session, interaction.member, text, Date.now() + minutes * 60 * 1000);
      await reply(interaction, `Хорошо, напомню ${formatDueTime(reminder.dueAt)}.`);
    }

    if (interaction.commandName === 'reminders') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      await reply(interaction, `Напоминания:\n${formatReminderList(interaction.guildId)}`);
    }

    if (interaction.commandName === 'pause') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      const session = getInteractionSession(interaction);
      if (!session) {
        await reply(interaction, 'Сначала подключи меня через /join.');
        return;
      }
      session.paused = true;
      updateRuntimeConfig({ listeningPaused: true });
      await reply(interaction, 'Поставил голосовую обработку на паузу.');
    }

    if (interaction.commandName === 'resume') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      const session = getInteractionSession(interaction);
      if (!session) {
        await reply(interaction, 'Сначала подключи меня через /join.');
        return;
      }
      session.paused = false;
      updateRuntimeConfig({ listeningPaused: false });
      await reply(interaction, 'Продолжаю голосовую обработку.');
    }

    if (interaction.commandName === 'status') {
      const session = getInteractionSession(interaction);
      if (!session?.connection) {
        await reply(interaction, 'Не подключен.');
        return;
      }
      await reply(interaction, formatSessionStatus(session));
    }
  } catch (error) {
    console.error('interaction failed:', error);
    if (!interaction.replied && !interaction.deferred) {
      await reply(interaction, `Ошибка: \`${error.message || error}\``);
    } else {
      await interaction.editReply(silentOptions(`Ошибка: \`${error.message || error}\``)).catch(() => {});
    }
  }
});

process.on('unhandledRejection', (error) => {
  console.error('unhandledRejection:', error);
  void sendMonitorNotice(`Runtime warning: \`${error?.message || error}\``).catch(() => {});
});
process.on('uncaughtException', (error) => {
  console.error('uncaughtException:', error);
  void sendMonitorNotice(`Runtime error: \`${error?.message || error}\``).catch(() => {});
});

setInterval(() => {
  void applyRuntimeConfigEffects().catch((error) => console.error('runtime tick failed:', error));
}, 3_000).unref();

setInterval(() => {
  void maybeRunIdleChatter().catch((error) => console.error('idle chatter tick failed:', error));
}, IDLE_CHATTER_CHECK_MS).unref();

setInterval(() => {
  void maybeRunIdleLeave().catch((error) => console.error('idle leave tick failed:', error));
}, IDLE_LEAVE_CHECK_MS).unref();

setInterval(() => {
  void runHealthCheck().catch((error) => console.error('healthcheck tick failed:', error));
}, HEALTHCHECK_INTERVAL_MS).unref();

await client.login(DISCORD_TOKEN);
