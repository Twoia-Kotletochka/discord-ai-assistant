import 'dotenv/config';

import { spawn } from 'node:child_process';
import { promises as fs, rmSync } from 'node:fs';
import path from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';

import { createStorage } from './storage.mjs';
import {
  ActionRowBuilder,
  ChannelType,
  Client,
  GatewayIntentBits,
  MessageFlags,
  ModalBuilder,
  PermissionFlagsBits,
  SlashCommandBuilder,
  TextInputBuilder,
  TextInputStyle,
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

const storage = await createStorage({ dataDir, logger: console });

const DISCORD_TOKEN = process.env.DISCORD_TOKEN?.trim();
const DISCORD_GUILD_ID = process.env.DISCORD_GUILD_ID?.trim();
const GROQ_API_KEY = process.env.GROQ_API_KEY?.trim();
const AUTO_JOIN_ENABLED = (process.env.AUTO_JOIN_ENABLED || 'false') === 'true';
const AUTO_JOIN_GUILD_ID = process.env.AUTO_JOIN_GUILD_ID?.trim() || '';
const AUTO_JOIN_VOICE_CHANNEL_ID = process.env.AUTO_JOIN_VOICE_CHANNEL_ID?.trim() || '';
const AUTO_JOIN_TEXT_CHANNEL_ID = process.env.AUTO_JOIN_TEXT_CHANNEL_ID?.trim() || '';

const DEFAULT_GROQ_CHAT_MODEL = process.env.GROQ_CHAT_MODEL?.trim() || 'llama-3.3-70b-versatile';
const DEFAULT_GROQ_STT_MODEL = process.env.GROQ_STT_MODEL?.trim() || 'whisper-large-v3-turbo';
const DEFAULT_ACTION_PARSER_MODEL = process.env.ACTION_PARSER_MODEL?.trim() || 'llama-3.1-8b-instant';
const DEFAULT_WEB_SEARCH_ENABLED = (process.env.WEB_SEARCH_ENABLED || 'true') === 'true';
const DEFAULT_WEB_SEARCH_MODEL = process.env.WEB_SEARCH_MODEL?.trim() || 'groq/compound';
const GROQ_AUTO_MODEL_FALLBACK = (process.env.GROQ_AUTO_MODEL_FALLBACK || 'true') !== 'false';
const GROQ_MODEL_LIMIT_COOLDOWN_MS = Math.max(60_000, Number(process.env.GROQ_MODEL_LIMIT_COOLDOWN_MS || 10 * 60_000));
const GROQ_CHAT_FALLBACK_MODELS = parseCsvList(process.env.GROQ_CHAT_FALLBACK_MODELS
  || 'llama-3.3-70b-versatile,openai/gpt-oss-120b,meta-llama/llama-4-scout-17b-16e-instruct,qwen/qwen3-32b,openai/gpt-oss-20b,llama-3.1-8b-instant');
const GROQ_ACTION_FALLBACK_MODELS = parseCsvList(process.env.GROQ_ACTION_FALLBACK_MODELS
  || 'llama-3.1-8b-instant,openai/gpt-oss-20b,qwen/qwen3-32b,llama-3.3-70b-versatile');
const GROQ_STT_FALLBACK_MODELS = parseCsvList(process.env.GROQ_STT_FALLBACK_MODELS
  || 'whisper-large-v3-turbo,whisper-large-v3');
const GROQ_WEB_FALLBACK_MODELS = parseCsvList(process.env.GROQ_WEB_FALLBACK_MODELS
  || 'groq/compound,groq/compound-mini');
const DEFAULT_IDLE_CHATTER_ENABLED = (process.env.IDLE_CHATTER_ENABLED || 'false') === 'true';
const DEFAULT_IDLE_CHATTER_MINUTES = Math.max(1, Math.min(180, Number(process.env.IDLE_CHATTER_MINUTES || 5)));
const DEFAULT_IDLE_CHATTER_USE_WEB = (process.env.IDLE_CHATTER_USE_WEB || 'true') === 'true';
const DEFAULT_IDLE_CHATTER_STYLE = process.env.IDLE_CHATTER_STYLE?.trim() || 'mixed';
const DEFAULT_IDLE_LEAVE_ENABLED = (process.env.IDLE_LEAVE_ENABLED || 'true') === 'true';
const DEFAULT_IDLE_LEAVE_MINUTES = Math.max(1, Math.min(1440, Number(process.env.IDLE_LEAVE_MINUTES || 60)));
const DEFAULT_IDLE_LEAVE_PHRASE = process.env.IDLE_LEAVE_PHRASE?.trim() || '';
const DEFAULT_ACTIVE_DIALOGUE_ENABLED = (process.env.ACTIVE_DIALOGUE_ENABLED || 'false') === 'true';
const DEFAULT_ACTIVE_DIALOGUE_SECONDS = Math.max(10, Math.min(300, Number(process.env.ACTIVE_DIALOGUE_SECONDS || 45)));
const DEFAULT_CONFIRM_DANGEROUS_ACTIONS = (process.env.CONFIRM_DANGEROUS_ACTIONS || 'false') === 'true';
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
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN?.trim() || '';
const TELEGRAM_DEFAULT_CHAT_ID = process.env.TELEGRAM_DEFAULT_CHAT_ID?.trim() || '';

const LISTEN_WITHOUT_WAKE_WORD = (process.env.LISTEN_WITHOUT_WAKE_WORD || 'false') === 'true';
const ENV_BOT_WAKE_WORD = (process.env.BOT_WAKE_WORD || DEFAULT_ASSISTANT_NAME || 'бот').trim().toLowerCase();
const DEFAULT_BOT_WAKE_ALIASES = ENV_BOT_WAKE_WORD === 'бот'
  ? 'вот,от,робот,роботик,ботик,бота,боту,боте,боты,ботом,бод,бат,борт,вод,бо,ботт'
  : '';
const ENV_BOT_WAKE_ALIASES = process.env.BOT_WAKE_ALIASES || DEFAULT_BOT_WAKE_ALIASES;
const ENV_BOT_WAKE_FUZZY = (process.env.BOT_WAKE_FUZZY || 'true') === 'true';
const MAX_REPLY_CHARS = Math.max(120, Number(process.env.MAX_REPLY_CHARS || 500));
const SILENT_MESSAGES = (process.env.SILENT_MESSAGES || 'true') === 'true';
const SILENCE_MS = Math.max(450, Number(process.env.SILENCE_MS || 900));
const MAX_UTTERANCE_MS = Math.max(3000, Number(process.env.MAX_UTTERANCE_MS || 8000));
const STALE_CAPTURE_MS = MAX_UTTERANCE_MS + SILENCE_MS + 5000;
const MIN_AUDIO_MS = Math.max(250, Number(process.env.MIN_AUDIO_MS || 350));
const MIN_RMS = Math.max(1, Number(process.env.MIN_RMS || 60));
const WAKE_LISTEN_WINDOW_MS = Math.max(2000, Number(process.env.WAKE_LISTEN_WINDOW_MS || 15000));
const WAKE_LISTEN_PREOPEN_GRACE_MS = Math.max(0, Number(process.env.WAKE_LISTEN_PREOPEN_GRACE_MS || 5000));
const REPLY_COOLDOWN_MS = Math.max(0, Number(process.env.REPLY_COOLDOWN_MS || 900));
const IGNORE_AFTER_JOIN_MS = Math.max(0, Number(process.env.IGNORE_AFTER_JOIN_MS || 500));
const DEFAULT_PRESENCE_ANNOUNCEMENTS_ENABLED = (process.env.PRESENCE_ANNOUNCEMENTS_ENABLED || 'true') === 'true';
const PRESENCE_ANNOUNCEMENT_DELAY_MS = Math.max(0, Number(process.env.PRESENCE_ANNOUNCEMENT_DELAY_MS || 900));
const PRESENCE_ANNOUNCEMENT_COOLDOWN_MS = Math.max(0, Number(process.env.PRESENCE_ANNOUNCEMENT_COOLDOWN_MS || 25_000));
const PRESENCE_ANNOUNCEMENT_QUIET_WAIT_MS = Math.max(0, Number(process.env.PRESENCE_ANNOUNCEMENT_QUIET_WAIT_MS || 8_000));
const VOICE_DEBUG = (process.env.VOICE_DEBUG || 'false') === 'true';
const API_LIMIT_ALERT_START_PERCENT = Math.max(1, Math.min(99, Number(process.env.API_LIMIT_ALERT_START_PERCENT || 35)));
const API_LIMIT_ALERT_STEP_PERCENT = Math.max(1, Math.min(50, Number(process.env.API_LIMIT_ALERT_STEP_PERCENT || 15)));
const API_LIMIT_ALERT_THRESHOLDS = Array.from(
  { length: Math.ceil(API_LIMIT_ALERT_START_PERCENT / API_LIMIT_ALERT_STEP_PERCENT) },
  (_, index) => API_LIMIT_ALERT_START_PERCENT - index * API_LIMIT_ALERT_STEP_PERCENT,
).filter((value) => value > 0);
const MAX_MEMORY_ITEMS = Math.max(10, Number(process.env.MAX_MEMORY_ITEMS || 200));
const MEMORY_CONTEXT_LIMIT = Math.max(0, Number(process.env.MEMORY_CONTEXT_LIMIT || 8));
const MAX_REMINDER_ITEMS = Math.max(10, Number(process.env.MAX_REMINDER_ITEMS || 200));
const MAX_REMINDER_TIMEOUT_MS = 2_147_000_000;
const IDLE_CHATTER_CHECK_MS = 30_000;
const IDLE_LEAVE_CHECK_MS = 30_000;
const HEALTHCHECK_INTERVAL_MS = 60_000;
const EVENT_LOG_MAX_PAYLOAD_CHARS = 2500;
const STT_PROMPT_MAX_CHARS = Math.max(100, Math.min(640, Number(process.env.STT_PROMPT_MAX_CHARS || 420)));
const STT_PROMPT_MAX_BYTES = Math.max(256, Math.min(896, Number(process.env.STT_PROMPT_MAX_BYTES || 780)));
const STT_TRANSIENT_RETRIES = Math.max(1, Math.min(5, Number(process.env.STT_TRANSIENT_RETRIES || 3)));
const STT_WAKE_RETRY_ENABLED = (process.env.STT_WAKE_RETRY_ENABLED || 'true') !== 'false';
const STT_LANGUAGE_GUARD_ENABLED = (process.env.STT_LANGUAGE_GUARD_ENABLED || 'true') !== 'false';
const STT_ALLOWED_LANGUAGES = process.env.STT_ALLOWED_LANGUAGES?.trim() || 'ru,uk,en';
const STT_LANGUAGE_HINT = process.env.STT_LANGUAGE_HINT?.trim()
  || 'Речь только на русском или украинском; отдельные английские слова оставляй как есть.';
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

function parseCsvList(value) {
  return String(value || '')
    .split(',')
    .map((item) => item.trim())
    .filter((item, index, list) => item && list.indexOf(item) === index);
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
  void storage.appendEvent(row).catch((error) => {
    console.error('event log write failed:', error);
  });
}

const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates],
});

const sessions = new Map();
const groqLimitAlertState = new Map();
const groqLastLimits = new Map();
const groqModelCooldowns = new Map();
const reminderTimers = new Map();
const stateStore = await loadStateStore();
let runtimeConfig = await loadRuntimeConfig();
let runtimeConfigMtime = 0;
let stateStoreMtime = 0;
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
  return await storage.loadState();
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
  saveStoreQueue = saveStoreQueue
    .catch(() => {})
    .then(async () => {
      await storage.saveState(stateStore);
      const stat = await fs.stat(statePath).catch(() => null);
      stateStoreMtime = stat?.mtimeMs || Date.now();
    })
    .catch((error) => console.error('state store save failed:', error));
  return saveStoreQueue;
}

function replaceStateStore(nextStore) {
  stateStore.version = nextStore?.version || 1;
  stateStore.guilds = nextStore?.guilds && typeof nextStore.guilds === 'object' ? nextStore.guilds : {};
}

async function reloadStateStoreIfChanged() {
  const stat = await fs.stat(statePath).catch(() => null);
  if (!stat) return;
  if (stat.mtimeMs <= stateStoreMtime) return;
  await saveStoreQueue.catch(() => {});
  const latestStat = await fs.stat(statePath).catch(() => stat);
  if (latestStat.mtimeMs <= stateStoreMtime) return;
  const nextStore = await loadStateStore();
  replaceStateStore(nextStore);
  const afterLoadStat = await fs.stat(statePath).catch(() => latestStat);
  stateStoreMtime = afterLoadStat?.mtimeMs || latestStat.mtimeMs;
  reschedulePendingReminders();
  appendEvent('state_reloaded', { source: 'storage_mirror', guilds: Object.keys(stateStore.guilds || {}).length });
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
  if (normalizedWake === 'зеро' || normalizedWake === 'zero') {
    return 'zero,зеро,зэро,зиро,зера,зеру,зэру,зерро,зэрро,зер,зироу,зара,заро,зоро,зерно,зено,зена,зина,зэра,зэна,серо,сиро,сера,сэро,сено,церо,цено,геро,жеро,ксеро,zerro,zeroo,zeero,ziro,zera,zaro,zoro,zeno,zenu,zena,zina,zere,zerre,sero,seno,cero,ceno,xero,xeno,hero';
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
  return [defaultWakeAliasesFor(wakeWord), raw]
    .filter(Boolean)
    .join(',')
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
    actionParserModel: DEFAULT_ACTION_PARSER_MODEL,
    webSearchEnabled: DEFAULT_WEB_SEARCH_ENABLED,
    webSearchModel: DEFAULT_WEB_SEARCH_MODEL,
    idleChatterEnabled: DEFAULT_IDLE_CHATTER_ENABLED,
    idleChatterMinutes: DEFAULT_IDLE_CHATTER_MINUTES,
    idleChatterUseWeb: DEFAULT_IDLE_CHATTER_USE_WEB,
    idleChatterStyle: DEFAULT_IDLE_CHATTER_STYLE,
    idleLeaveEnabled: DEFAULT_IDLE_LEAVE_ENABLED,
    idleLeaveMinutes: DEFAULT_IDLE_LEAVE_MINUTES,
    idleLeavePhrase: DEFAULT_IDLE_LEAVE_PHRASE,
    presenceAnnouncementsEnabled: DEFAULT_PRESENCE_ANNOUNCEMENTS_ENABLED,
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
    telegramBotToken: '',
    telegramDefaultChatId: TELEGRAM_DEFAULT_CHAT_ID,
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
    actionParserModel: String(value.actionParserModel || defaults.actionParserModel),
    webSearchEnabled: value.webSearchEnabled === undefined ? defaults.webSearchEnabled : value.webSearchEnabled !== false,
    webSearchModel: String(value.webSearchModel || defaults.webSearchModel),
    idleChatterEnabled: value.idleChatterEnabled === undefined ? defaults.idleChatterEnabled : value.idleChatterEnabled === true,
    idleChatterMinutes: Math.max(1, Math.min(180, Number(value.idleChatterMinutes || defaults.idleChatterMinutes))),
    idleChatterUseWeb: value.idleChatterUseWeb === undefined ? defaults.idleChatterUseWeb : value.idleChatterUseWeb !== false,
    idleChatterStyle: String(value.idleChatterStyle || defaults.idleChatterStyle),
    idleLeaveEnabled: value.idleLeaveEnabled === undefined ? defaults.idleLeaveEnabled : value.idleLeaveEnabled === true,
    idleLeaveMinutes: Math.max(1, Math.min(1440, Number(value.idleLeaveMinutes || defaults.idleLeaveMinutes))),
    idleLeavePhrase: String(value.idleLeavePhrase ?? defaults.idleLeavePhrase).replace(/\s+/g, ' ').trim().slice(0, 240),
    presenceAnnouncementsEnabled: value.presenceAnnouncementsEnabled === undefined ? defaults.presenceAnnouncementsEnabled : value.presenceAnnouncementsEnabled === true,
    activeDialogueEnabled: value.activeDialogueEnabled === undefined ? defaults.activeDialogueEnabled : value.activeDialogueEnabled === true,
    activeDialogueSeconds: Math.max(10, Math.min(300, Number(value.activeDialogueSeconds || defaults.activeDialogueSeconds))),
    confirmDangerousActions: false,
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
    telegramBotToken: String(value.telegramBotToken || '').trim(),
    telegramDefaultChatId: String(value.telegramDefaultChatId ?? defaults.telegramDefaultChatId).trim().slice(0, 120),
  };
}

async function loadRuntimeConfig() {
  try {
    return normalizeRuntimeConfig(await storage.loadRuntimeConfig(defaultRuntimeConfig()));
  } catch (error) {
    console.error('runtime config load failed:', error);
    return defaultRuntimeConfig();
  }
}

function saveRuntimeConfig() {
  runtimeConfig.updatedAt = Date.now();
  saveRuntimeConfigQueue = saveRuntimeConfigQueue
    .catch(() => {})
    .then(async () => {
      await storage.saveRuntimeConfig(runtimeConfig);
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

function getActionParserModel() {
  return runtimeConfig.actionParserModel || DEFAULT_ACTION_PARSER_MODEL;
}

function isWebSearchEnabled() {
  return runtimeConfig.webSearchEnabled !== false;
}

function getWebSearchModel() {
  return runtimeConfig.webSearchModel || DEFAULT_WEB_SEARCH_MODEL;
}

function getTelegramBotToken() {
  return runtimeConfig.telegramBotToken?.trim() || TELEGRAM_BOT_TOKEN;
}

function getTelegramDefaultChatId() {
  return runtimeConfig.telegramDefaultChatId?.trim() || TELEGRAM_DEFAULT_CHAT_ID;
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

function isPresenceAnnouncementsEnabled() {
  return runtimeConfig.presenceAnnouncementsEnabled !== false;
}

function isActiveDialogueEnabled() {
  return runtimeConfig.activeDialogueEnabled === true;
}

function getActiveDialogueSeconds() {
  return Math.max(10, Math.min(300, Number(runtimeConfig.activeDialogueSeconds || DEFAULT_ACTIVE_DIALOGUE_SECONDS)));
}

function shouldConfirmDangerousActions() {
  return false;
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

function wakeTermPattern(term) {
  return new RegExp(`(^|[^\\p{L}\\p{N}_])(${escapeRegExp(term)})(?=$|[^\\p{L}\\p{N}_])`, 'iu');
}

function wakeHasAddressContext(rawText, index) {
  const before = String(rawText || '').slice(0, Math.max(0, index));
  if (!before.trim()) return true;
  const currentPhrase = before.split(/[.!?;:,\n]/u).pop() || '';
  const wordsBefore = normalizeCommandText(currentPhrase).split(/\s+/g).filter(Boolean);
  return wordsBefore.length <= 8;
}

function isStrongWakeTerm(term) {
  const normalizedTerm = normalizeCommandText(term);
  const normalizedWake = normalizeCommandText(getWakeWord());
  if (!normalizedTerm || !normalizedWake) return false;
  if (normalizedTerm === normalizedWake) return true;

  if (normalizedWake === 'зеро' || normalizedWake === 'zero') {
    return normalizedTerm.length >= 3;
  }

  const riskyBotAliases = new Set(['вот', 'от', 'бо', 'вод', 'бод', 'бат', 'борт']);
  if (normalizedWake === 'бот' && riskyBotAliases.has(normalizedTerm)) {
    return false;
  }

  return normalizedTerm.length >= 5;
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

function ephemeralOptions(content, extra = {}) {
  return silentOptions(content, {
    flags: MessageFlags.Ephemeral | (SILENT_MESSAGES ? MessageFlags.SuppressNotifications : 0),
    ...extra,
  });
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

function parseGroqResetMs(reset) {
  if (!reset) return null;
  const raw = String(reset).trim().toLowerCase();
  const numeric = Number(raw);
  if (Number.isFinite(numeric) && numeric > 0) {
    if (numeric > 1_000_000_000_000) return Math.max(0, numeric - Date.now());
    if (numeric > 1_000_000_000) return Math.max(0, numeric * 1000 - Date.now());
    return numeric > 10_000 ? numeric : numeric * 1000;
  }

  let totalMs = 0;
  for (const match of raw.matchAll(/(\d+(?:\.\d+)?)\s*(ms|s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)/gu)) {
    const value = Number(match[1]);
    const unit = match[2];
    if (!Number.isFinite(value)) continue;
    if (unit === 'ms') totalMs += value;
    else if (unit.startsWith('s')) totalMs += value * 1000;
    else if (unit.startsWith('m')) totalMs += value * 60_000;
    else if (unit.startsWith('h')) totalMs += value * 3_600_000;
    else if (unit.startsWith('d')) totalMs += value * 86_400_000;
  }
  return totalMs > 0 ? totalMs : null;
}

function markGroqModelOnCooldown(model, label = 'unknown', reset = null) {
  if (!model) return;
  const resetMs = parseGroqResetMs(reset);
  const until = Date.now() + Math.max(60_000, Math.min(resetMs || GROQ_MODEL_LIMIT_COOLDOWN_MS, 24 * 60 * 60_000));
  const current = groqModelCooldowns.get(model);
  if (!current || current.until < until) {
    groqModelCooldowns.set(model, { until, label, reset: reset || null });
    console.warn(`Groq model ${model} cooldown until ${new Date(until).toISOString()} (${label})`);
  }
}

function groqResetHeaderFromError(error, preferredMetric = 'tokens') {
  const headers = getRateLimitHeaders(error);
  return getHeader(headers, `x-ratelimit-reset-${preferredMetric}`)
    || getHeader(headers, 'x-ratelimit-reset-requests')
    || getHeader(headers, 'x-ratelimit-reset-tokens');
}

function isGroqModelOnCooldown(model) {
  const item = groqModelCooldowns.get(model);
  if (!item) return false;
  if (Date.now() >= item.until) {
    groqModelCooldowns.delete(model);
    return false;
  }
  return true;
}

function groqModelsToTry(primary, fallbackModels = []) {
  const ordered = GROQ_AUTO_MODEL_FALLBACK
    ? parseCsvList([primary, ...fallbackModels].filter(Boolean).join(','))
    : parseCsvList(primary);
  const available = ordered.filter((model) => !isGroqModelOnCooldown(model));
  return available.length ? available : ordered.slice(0, 1);
}

function chatModelsToTry(preferredModel = getChatModel()) {
  return groqModelsToTry(preferredModel, GROQ_CHAT_FALLBACK_MODELS);
}

function actionModelsToTry(preferredModel = getActionParserModel()) {
  return groqModelsToTry(preferredModel, GROQ_ACTION_FALLBACK_MODELS);
}

function sttModelsToTry(preferredModel = getSttModel()) {
  return groqModelsToTry(preferredModel, GROQ_STT_FALLBACK_MODELS);
}

function webSearchModelsToTry(preferredModel = getWebSearchModel()) {
  return groqModelsToTry(preferredModel, GROQ_WEB_FALLBACK_MODELS);
}

function groqErrorStatus(error) {
  return error?.status || error?.statusCode || error?.response?.status || error?.error?.status || error?.error?.error?.status || null;
}

function isGroqRateLimitError(error) {
  const status = groqErrorStatus(error);
  const message = error?.error?.error?.message || error?.error?.message || error?.message || '';
  return status === 429 || /rate limit|too many requests|quota|tokens.*exceeded|requests.*exceeded/i.test(message);
}

function isGroqModelUnavailableError(error) {
  const status = groqErrorStatus(error);
  const message = error?.error?.error?.message || error?.error?.message || error?.message || '';
  return [400, 404].includes(status) && /model|does not exist|not found|decommissioned|not available|unsupported/i.test(message);
}

function shouldFallbackGroqModel(error) {
  return isGroqRateLimitError(error) || isGroqModelUnavailableError(error) || isTransientGroqConnectionError(error);
}

function groqModelCooldownsObject() {
  const items = {};
  for (const [model, item] of groqModelCooldowns.entries()) {
    if (!isGroqModelOnCooldown(model)) continue;
    items[model] = {
      ...item,
      remainingMs: Math.max(0, item.until - Date.now()),
    };
  }
  return items;
}

async function maybeAlertGroqLimit(channel, label, metric, limit, remaining, reset) {
  if (!Number.isFinite(limit) || !Number.isFinite(remaining) || limit <= 0) return;

  const percent = remaining / limit * 100;
  const threshold = API_LIMIT_ALERT_THRESHOLDS
    .filter((item) => percent <= item)
    .at(-1);
  const key = `${label}:${metric}`;
  const current = groqLimitAlertState.get(key) || { threshold: null, remaining: null };

  if (percent > API_LIMIT_ALERT_START_PERCENT + 5 || (Number.isFinite(current.remaining) && remaining > current.remaining)) {
    current.threshold = null;
  }
  current.remaining = remaining;

  if (threshold && (current.threshold === null || threshold < current.threshold)) {
    current.threshold = threshold;
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
    if (metric.remaining <= 0) markGroqModelOnCooldown(model, `${label}:${metric.name}`, metric.reset);
    void maybeAlertGroqLimit(channel || monitorChannel, `${model} / ${label}`, metric.name, metric.limit, metric.remaining, metric.reset)
      .catch((error) => console.error('Groq limit alert failed:', error));
  }
}

function formatGroqLimits() {
  const cooldownLines = [...groqModelCooldowns.entries()]
    .filter(([model]) => isGroqModelOnCooldown(model))
    .map(([model, item]) => `${model}: временно пропускаю до ${new Date(item.until).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}, причина=${item.label}`);
  if (!groqLastLimits.size && !cooldownLines.length) {
    return 'Пока нет данных по лимитам Groq. Они появятся после первого запроса к STT или chat model.';
  }

  const limitLines = [...groqLastLimits.values()]
    .map((metric) => {
      const percent = metric.limit > 0 ? metric.remaining / metric.limit * 100 : NaN;
      const checked = new Date(metric.checkedAt).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
      return `${metric.model || 'unknown'} ${metric.name}: ${metric.remaining}/${metric.limit} (${formatPercent(percent)}%), reset=${metric.reset || 'unknown'}, source=${metric.label}, checked=${checked}`;
    });
  return [...limitLines, ...cooldownLines].join('\n');
}

function formatSessionStatus(session) {
  if (!session?.connection) return 'Не подключен к voice channel.';
  const diag = session.diagnostics || createVoiceDiagnostics();
  const idleSeconds = session.lastHumanActivityAt ? Math.round((Date.now() - session.lastHumanActivityAt) / 1000) : 0;
  const assistantIdleSeconds = Math.round((Date.now() - (session.lastAssistantInteractionAt || session.joinedAt || Date.now())) / 1000);
  const activeLeft = session.activeDialogueUntil ? Math.max(0, Math.round((session.activeDialogueUntil - Date.now()) / 1000)) : 0;
  const wakeListenLeft = session.wakeListenUntil ? Math.max(0, Math.round((session.wakeListenUntil - Date.now()) / 1000)) : 0;
  return `Voice: ${session.voiceChannel?.name || 'unknown'}, state=${session.connection.state.status}, assistant=${getAssistantName()}, trigger="${getWakeWord() || 'off'}", enabled=${isBotEnabled()}, paused=${isListeningPaused(session)}, persona=${getAssistantPersona()}, wakeListen=${wakeListenLeft}s, wakeListenUser=${session.wakeListenUserId || 'none'}, activeDialogue=${activeLeft}s, webSearch=${isWebSearchEnabled()}, idleChatter=${isIdleChatterEnabled()} every ${getIdleChatterMinutes()}m style=${getIdleChatterStyle()} web=${isIdleChatterWebEnabled()}, idleLeave=${isIdleLeaveEnabled()} after ${getIdleLeaveMinutes()}m, humanIdle=${idleSeconds}s, assistantIdle=${assistantIdleSeconds}s, busy=${Boolean(session.busy)}, activeCaptures=${session.activeUsers?.size || 0}, history=${session.history?.length || 0}, voiceEvents=${diag.voiceEvents}, captures=${diag.captures}, ignored=${diag.ignored}, lastIgnored=${diag.lastIgnoredReason || 'none'}, lastTranscript=${diag.lastTranscript || 'none'}.`;
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

const ZERO_WAKE_FALSE_POSITIVE_TOKENS = new Set([
  'send', 'sent', 'sand', 'sense', 'seen', 'scene',
  'certo', 'certa', 'certos', 'certas', 'certeza',
]);

function isWakeLikeToken(token) {
  const normalizedWake = normalizeCommandText(getWakeWord());
  if (!token || !normalizedWake) return false;
  const zeroWake = normalizedWake === 'зеро' || normalizedWake === 'zero';
  if (zeroWake && ZERO_WAKE_FALSE_POSITIVE_TOKENS.has(token)) return false;
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

  if (normalizedWake === 'зеро' || normalizedWake === 'zero') {
    const knownZeroVariants = new Set([
      'зеро', 'зэро', 'зиро', 'зера', 'зеру', 'зэру', 'зерро', 'зэрро', 'зер',
      'зироу', 'зара', 'заро', 'зоро', 'зерно', 'зено', 'зена', 'зина',
      'зэра', 'зэна', 'серо', 'сиро', 'сера', 'сэро', 'сено', 'церо',
      'цено', 'геро', 'жеро', 'ксеро', 'zero', 'zerro', 'zeroo', 'zeero',
      'ziro', 'zera', 'zaro', 'zoro', 'zeno', 'zenu', 'zena', 'zina',
      'zere', 'zerre', 'sero', 'seno', 'cero', 'ceno', 'xero', 'xeno', 'hero',
    ]);
    if (knownZeroVariants.has(token)) return true;
  }

  const compactToken = compactText(token);
  if (compactToken.length < (zeroWake ? 3 : 2) || compactToken.length > 18) return false;
  const latinToken = /^[a-z0-9_-]+$/iu.test(compactToken);

  const candidates = [normalizedWake, ...aliases]
    .map((item) => compactText(item))
    .filter((item, index, list) => item && list.indexOf(item) === index);
  for (const candidate of candidates) {
    const distance = levenshteinDistance(compactToken, candidate);
    const maxDistance = zeroWake
      ? (candidate.length <= 4 ? 1 : candidate.length <= 8 ? 2 : 3)
      : (candidate.length <= 4 ? 1 : candidate.length <= 8 ? 2 : 3);
    const similarEnough = similarity(compactToken, candidate) >= (
      zeroWake ? (candidate.length <= 4 ? 0.58 : 0.64) : (candidate.length <= 4 ? 0.58 : 0.68)
    );
    const firstLetterClose = compactToken[0] === candidate[0] || distance <= 1;
    if (zeroWake && latinToken && !firstLetterClose) continue;
    if (distance <= maxDistance && similarEnough && firstLetterClose) return true;
  }
  return false;
}

function isLowRiskWakeLikeToken(token) {
  const normalizedWake = normalizeCommandText(getWakeWord());
  if (!(normalizedWake === 'зеро' || normalizedWake === 'zero')) return false;
  const normalizedToken = normalizeCommandText(token);
  return normalizedToken.length >= 3 && isWakeLikeToken(normalizedToken);
}

function findWakeWord(text) {
  const rawText = String(text || '');
  const wakeWord = getWakeWord();
  if (!wakeWord) return null;

  let best = null;
  const terms = [wakeWord, ...getWakeAliases()]
    .map((term) => normalizeCommandText(term))
    .filter((term, index, list) => term && list.indexOf(term) === index);
  for (const term of terms) {
    const match = wakeTermPattern(term).exec(rawText);
    if (!match) continue;
    const candidate = {
      index: match.index + (match[1]?.length || 0),
      length: match[2]?.length || term.length,
    };
    if (!isStrongWakeTerm(term) && !wakeHasAddressContext(rawText, candidate.index)) continue;
    if (!best || candidate.index < best.index) best = candidate;
  }
  if (best) return best;

  const tokenPattern = /[\p{L}\p{N}_-]{1,20}/giu;
  let scanned = 0;
  for (const match of rawText.matchAll(tokenPattern)) {
    scanned += 1;
    const token = normalizeCommandText(match[0]);
    if (isWakeLikeToken(token) && (isLowRiskWakeLikeToken(token) || wakeHasAddressContext(rawText, match.index || 0))) {
      return { index: match.index || 0, length: match[0].length };
    }
    if (scanned >= 24) break;
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

function isWakeListenWindow(session, at = Date.now(), userId = null) {
  if (!session?.wakeListenUntil || at > session.wakeListenUntil) return false;
  if (session.wakeListenUserId && userId && String(session.wakeListenUserId) !== String(userId)) return false;
  const openedAt = session.wakeListenStartedAt || 0;
  return !openedAt || at >= openedAt - WAKE_LISTEN_PREOPEN_GRACE_MS;
}

function markWakeListen(session, userId = null) {
  if (!session) return;
  session.wakeListenStartedAt = Date.now();
  session.wakeListenUntil = Date.now() + WAKE_LISTEN_WINDOW_MS;
  session.wakeListenUserId = userId ? String(userId) : null;
}

function clearWakeListen(session) {
  if (!session) return;
  session.wakeListenStartedAt = 0;
  session.wakeListenUntil = 0;
  session.wakeListenUserId = null;
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

function shouldAnswer(text, session = null, at = Date.now(), userId = null) {
  if (LISTEN_WITHOUT_WAKE_WORD || !getWakeWord()) return true;
  return hasWakeWord(text) || isWakeListenWindow(session, at, userId) || isActiveDialogue(session);
}

function stripWakeWord(text) {
  if (!getWakeWord()) return text.trim();
  const wake = findWakeWord(text);
  if (!wake) return text.trim();
  return stripLeadingWakeTerms(text.slice(wake.index + wake.length));
}

function promptFromTranscript(session, transcript) {
  const prompt = hasWakeWord(transcript) ? stripWakeWord(transcript) : String(transcript || '').trim();
  return isWakeOnlyPrompt(prompt) ? '' : prompt;
}

function isWakeOnlyPrompt(text) {
  const tokens = normalizeCommandText(text)
    .split(/\s+/u)
    .filter(Boolean);
  return tokens.length > 0 && tokens.length <= 8 && tokens.every((token) => isWakeLikeToken(token));
}

function isSttBoilerplateTranscript(transcript) {
  if (isSttPromptEchoTranscript(transcript)) return true;
  const normalized = normalizeCommandText(transcript);
  if (!normalized) return false;
  return [
    /^subtitles?\s+by\s+.*amara/u,
    /amara\s+org\s+community/u,
    /^subtitles?\s+by\s+the\s+.*community$/u,
    /^thanks?\s+for\s+watching$/u,
  ].some((pattern) => pattern.test(normalized));
}

function isSttPromptEchoTranscript(transcript) {
  const normalized = normalizeCommandText(transcript);
  if (!normalized) return false;
  return [
    /^mixed language$/u,
    /^речь\s+только\s+на\s+русском/u,
    /^русская\s+и\s+английская\s+речь/u,
    /^русская\s+и\s+украинская\s+речь/u,
    /^частые\s+слова/u,
    /разрешенн\p{L}*\s+язык/u,
    /текущее\s+имя\s+ассистента/u,
    /триггерн\p{L}*\s+слов/u,
    /имена\s+и\s+ники\s+в\s+войсе/u,
  ].some((pattern) => pattern.test(normalized));
}

function looksLikeMissedWakeTranscript(transcript) {
  const normalizedWake = normalizeCommandText(getWakeWord());
  if (!(normalizedWake === 'зеро' || normalizedWake === 'zero')) return false;
  const tokens = normalizeCommandText(transcript).split(/\s+/u).filter(Boolean).slice(0, 3);
  if (!tokens.length) return false;
  const likelyZeroTokens = new Set([
    'зено', 'зена', 'зина', 'зэна', 'зэра', 'сэро', 'сено', 'церо', 'цено',
    'ceno', 'seno', 'zeno', 'zenu', 'zena', 'zina', 'zere', 'zerre', 'xeno',
  ]);
  if (tokens.some((token) => likelyZeroTokens.has(token))) return true;
  const compact = tokens.join('');
  if (likelyZeroTokens.has(compact)) return true;
  return false;
}

function shouldRetrySttForWake(transcript, session = null, userId = null) {
  if (!STT_WAKE_RETRY_ENABLED) return false;
  if (!session || !getWakeWord() || LISTEN_WITHOUT_WAKE_WORD) return false;
  if (!transcript || hasWakeWord(transcript)) return false;
  return isSttPromptEchoTranscript(transcript)
    || (!isWakeListenWindow(session, Date.now(), userId) && looksLikeMissedWakeTranscript(transcript));
}

const LATIN_RE = /\p{Script=Latin}/u;
const CYRILLIC_RE = /\p{Script=Cyrillic}/u;
const LETTER_RE = /\p{L}/u;
const ASCII_LATIN_RE = /^[a-z]+$/u;
const FOREIGN_LATIN_TOKENS = new Set([
  'ao', 'isso', 'estou', 'esta', 'estao', 'voce', 'acenando', 'obrigado', 'obrigada',
  'tambem', 'porque', 'quando', 'donde', 'hola', 'gracias', 'adios', 'merci',
  'bak', 'yikildi', 'yıkıldı', 'tamam', 'evet', 'hayir', 'hayır', 'merhaba',
  'tesekkur', 'teşekkür', 'arkadas', 'arkadaş', 'degil', 'değil',
]);
const ENGLISH_CONTEXT_TOKENS = new Set([
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'bot', 'by', 'can', 'chat', 'channel',
  'clear', 'close', 'create', 'delete', 'discord', 'disconnect', 'docker', 'find',
  'for', 'from', 'groq', 'hello', 'hey', 'hi', 'how', 'in', 'is', 'it', 'kick',
  'list', 'memory', 'move', 'mute', 'news', 'note', 'open', 'play', 'please',
  'read', 'remember', 'remind', 'remove', 'resume', 'search', 'send', 'show',
  'status', 'stop', 'telegram', 'tell', 'thanks', 'the', 'time', 'to', 'unmute',
  'voice', 'weather', 'what', 'when', 'where', 'who', 'why', 'with', 'zero',
]);

function transcriptLanguageStats(text) {
  const stats = {
    letters: 0,
    cyrillic: 0,
    latin: 0,
    asciiLatin: 0,
    nonAsciiLatin: 0,
    otherLetters: 0,
  };
  for (const char of String(text || '').toLowerCase()) {
    if (!LETTER_RE.test(char)) continue;
    stats.letters += 1;
    if (CYRILLIC_RE.test(char)) {
      stats.cyrillic += 1;
      continue;
    }
    if (LATIN_RE.test(char)) {
      stats.latin += 1;
      if (/^[a-z]$/u.test(char)) stats.asciiLatin += 1;
      else stats.nonAsciiLatin += 1;
      continue;
    }
    stats.otherLetters += 1;
  }
  return stats;
}

function latinTokens(text) {
  return normalizeCommandText(text)
    .split(/\s+/u)
    .map((token) => token.trim())
    .filter((token) => token && /[a-z]/u.test(token));
}

function hasEnglishContext(tokens) {
  return tokens.some((token) => {
    const normalized = collapseRepeatedLetters(token);
    return ENGLISH_CONTEXT_TOKENS.has(token)
      || ENGLISH_CONTEXT_TOKENS.has(normalized)
      || /^(discord|docker|groq|github|google|openai|telegram|youtube)$/u.test(normalized);
  });
}

function foreignLatinHits(tokens) {
  return tokens.filter((token) => {
    const normalized = collapseRepeatedLetters(token);
    return FOREIGN_LATIN_TOKENS.has(token) || FOREIGN_LATIN_TOKENS.has(normalized);
  });
}

function stripWakeTokensForLanguageGuard(text) {
  return normalizeCommandText(text)
    .split(/\s+/u)
    .filter((token) => token && !isWakeLikeToken(token))
    .join(' ')
    .trim();
}

function transcriptLanguageGuardReason(transcript, session = null) {
  if (!STT_LANGUAGE_GUARD_ENABLED) return '';
  const text = String(transcript || '').trim();
  if (!text) return '';

  const prompt = promptFromTranscript(session, text);
  const target = prompt || stripWakeTokensForLanguageGuard(text) || text;
  const stats = transcriptLanguageStats(target);
  if (stats.letters < 4) return '';

  if (stats.otherLetters > 0) return 'language_guard_other_script';
  if (stats.nonAsciiLatin > 0) return 'language_guard_foreign_latin_chars';

  const tokens = latinTokens(target);
  const foreignHits = foreignLatinHits(tokens);
  if (foreignHits.length >= 2) return 'language_guard_foreign_tokens';
  if (foreignHits.length && stats.cyrillic === 0 && !hasEnglishContext(tokens)) {
    return 'language_guard_foreign_tokens';
  }

  if (stats.cyrillic > 0) return '';
  if (!stats.latin) return '';

  if (hasEnglishContext(tokens)) return '';
  if (hasWakeWord(text) && tokens.length <= 1 && tokens.every((token) => ASCII_LATIN_RE.test(token))) return '';

  return 'language_guard_latin_without_context';
}

function stripLeadingWakeTerms(text) {
  let value = String(text || '').trim().replace(/^[,!.?:;\s-]+/u, '');
  for (let index = 0; index < 3; index += 1) {
    const match = value.match(/^([\p{L}\p{N}_-]{1,24})(?=$|[\s,!.?:;-])/iu);
    if (!match || !isWakeLikeToken(normalizeCommandText(match[1]))) break;
    value = value.slice(match[0].length).trim().replace(/^[,!.?:;\s-]+/u, '');
  }
  return value.trim();
}

function normalizeCommandText(text) {
  return text
    .toLowerCase()
    .replaceAll('ё', 'е')
    .replace(/[^\p{L}\p{N}\s_-]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

const SEARCH_STOP_TOKENS = new Set([
  'в', 'во', 'на', 'с', 'со', 'из', 'от', 'для', 'и', 'а', 'по', 'к', 'ко',
  'у', 'за', 'про', 'об', 'о',
  'канал', 'канала', 'канале', 'каналу', 'войс', 'воис', 'voice', 'channel',
  'чата', 'чат', 'сервер', 'сервера', 'участник', 'участника', 'пользователь', 'пользователя',
  'микрофон', 'микрофона', 'микрофончик', 'звук', 'звука', 'microphone', 'mic',
]);

const CYR_TO_LAT = new Map(Object.entries({
  а: 'a', б: 'b', в: 'v', г: 'g', ґ: 'g', д: 'd', е: 'e', є: 'ye', ж: 'zh', з: 'z',
  и: 'i', і: 'i', ї: 'yi', й: 'y', к: 'k', л: 'l', м: 'm', н: 'n', о: 'o', п: 'p',
  р: 'r', с: 's', т: 't', у: 'u', ф: 'f', х: 'h', ц: 'ts', ч: 'ch', ш: 'sh',
  щ: 'sch', ъ: '', ы: 'y', ь: '', э: 'e', ю: 'yu', я: 'ya',
}));

const LAT_TO_CYR_DIGRAPHS = [
  ['sch', 'щ'], ['sh', 'ш'], ['ch', 'ч'], ['zh', 'ж'], ['ts', 'ц'],
  ['yu', 'ю'], ['ya', 'я'], ['ye', 'е'], ['yi', 'и'],
];

const LAT_TO_CYR = new Map(Object.entries({
  a: 'а', b: 'б', c: 'к', d: 'д', e: 'е', f: 'ф', g: 'г', h: 'х', i: 'и', j: 'дж',
  k: 'к', l: 'л', m: 'м', n: 'н', o: 'о', p: 'п', q: 'к', r: 'р', s: 'с', t: 'т',
  u: 'у', v: 'в', w: 'в', x: 'кс', y: 'и', z: 'з',
}));

function transliterateCyrillicToLatin(text) {
  return [...String(text || '')]
    .map((char) => CYR_TO_LAT.get(char) ?? char)
    .join('');
}

function transliterateLatinToCyrillic(text) {
  let value = String(text || '');
  for (const [latin, cyrillic] of LAT_TO_CYR_DIGRAPHS) {
    value = value.replaceAll(latin, cyrillic);
  }
  return [...value]
    .map((char) => LAT_TO_CYR.get(char) ?? char)
    .join('');
}

function collapseRepeatedLetters(text) {
  return String(text || '').replace(/([\p{L}\p{N}])\1+/gu, '$1');
}

function stripNameEnding(token) {
  const variants = new Set([token]);
  const cyrEndings = ['ами', 'ями', 'ого', 'ему', 'ими', 'ыми', 'ом', 'ем', 'ой', 'ою', 'ую', 'ах', 'ях', 'ов', 'ев', 'ам', 'ям', 'а', 'у', 'е', 'ы', 'и', 'ю', 'я'];
  const latEndings = ['ami', 'yami', 'ogo', 'emu', 'om', 'em', 'oy', 'ov', 'ev', 'am', 'yam', 'a', 'u', 'e', 'y', 'i'];
  const endings = /[\p{Script=Cyrillic}]/u.test(token) ? cyrEndings : latEndings;
  for (const ending of endings) {
    if (!token.endsWith(ending)) continue;
    const stripped = token.slice(0, -ending.length);
    if (stripped.length >= 3) variants.add(stripped);
  }
  return [...variants];
}

function addSearchVariant(set, value) {
  const normalized = normalizeCommandText(value);
  if (!normalized) return;
  set.add(normalized);
  const compact = normalized.replace(/\s+/g, '');
  if (compact) set.add(compact);
  const collapsed = collapseRepeatedLetters(normalized);
  if (collapsed) {
    set.add(collapsed);
    set.add(collapsed.replace(/\s+/g, ''));
  }
  const latin = normalizeCommandText(transliterateCyrillicToLatin(normalized));
  if (latin && latin !== normalized) {
    set.add(latin);
    set.add(latin.replace(/\s+/g, ''));
    set.add(collapseRepeatedLetters(latin));
  }
  const cyrillic = normalizeCommandText(transliterateLatinToCyrillic(normalized));
  if (cyrillic && cyrillic !== normalized) {
    set.add(cyrillic);
    set.add(cyrillic.replace(/\s+/g, ''));
    set.add(collapseRepeatedLetters(cyrillic));
  }
}

function nameSearchVariants(text) {
  const variants = new Set();
  const normalized = normalizeCommandText(text);
  addSearchVariant(variants, normalized);
  for (const token of normalized.split(/\s+/g).filter(Boolean)) {
    if (SEARCH_STOP_TOKENS.has(token) || token.length < 2) continue;
    for (const tokenVariant of stripNameEnding(token)) {
      addSearchVariant(variants, tokenVariant);
      addSearchVariant(variants, collapseRepeatedLetters(tokenVariant));
      addSearchVariant(variants, transliterateCyrillicToLatin(tokenVariant));
      addSearchVariant(variants, transliterateLatinToCyrillic(tokenVariant));
    }
  }
  return [...variants].filter((item) => item.length >= 2 && !SEARCH_STOP_TOKENS.has(item));
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

function rawCandidateMemberNames(member) {
  return [
    member.displayName,
    member.nickname,
    member.user?.globalName,
    member.user?.username,
    member.user?.tag,
    member.id,
  ]
    .filter(Boolean);
}

function candidateMemberNames(member) {
  return rawCandidateMemberNames(member).map((name) => normalizeCommandText(name));
}

function candidateMemberSearchNames(member) {
  return [...new Set(rawCandidateMemberNames(member).flatMap((name) => nameSearchVariants(name)))];
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

function basicSimilarity(a, b) {
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

function similarity(a, b) {
  const leftVariants = nameSearchVariants(a);
  const rightVariants = nameSearchVariants(b);
  if (!leftVariants.length || !rightVariants.length) return 0;

  let best = 0;
  for (const left of leftVariants) {
    for (const right of rightVariants) {
      best = Math.max(best, basicSimilarity(left, right));
      if (best >= 1) return 1;
    }
  }
  return best;
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

function fallbackGeneratedNotes(topic, count) {
  const cleanTopic = String(topic || '').replace(/\s+/g, ' ').trim();
  const generic = [
    'Проверить список важных задач на завтра.',
    'Уточнить сроки по текущим договоренностям.',
    'Записать идеи, которые стоит обсудить с командой.',
    'Проверить состояние сервера и резервных копий.',
    'Вернуться к незавершенным вопросам вечером.',
    'Подготовить короткий список приоритетов на день.',
    'Проверить сообщения, которые требуют ответа.',
    'Сохранить полезные ссылки в одном месте.',
    'Отметить, что нужно протестировать после изменений.',
    'Разобрать старые заметки и удалить лишнее.',
  ];
  const themed = [
    `По теме "${cleanTopic}" уточнить главные детали и сроки.`,
    `По теме "${cleanTopic}" собрать короткий список вопросов.`,
    `По теме "${cleanTopic}" проверить, что уже сделано.`,
    `По теме "${cleanTopic}" записать следующий практический шаг.`,
    `По теме "${cleanTopic}" вернуться к обсуждению позже.`,
  ];
  const source = cleanTopic ? themed : generic;
  return Array.from({ length: count }, (_, index) => source[index % source.length]);
}

function extractJsonArray(text) {
  const raw = String(text || '');
  const start = raw.indexOf('[');
  const end = raw.lastIndexOf(']');
  if (start === -1 || end === -1 || end <= start) return null;
  return raw.slice(start, end + 1);
}

function cleanGeneratedNoteText(text) {
  return String(text || '')
    .replace(/^\s*(?:[-*•]|\d+[.)])\s*/u, '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 240);
}

async function generateMemoryNotes(session, actorMember, requestText, count, topic = '') {
  const safeCount = Math.max(1, Math.min(10, Number(count) || 5));
  const request = String(requestText || '').replace(/\s+/g, ' ').trim().slice(0, 500);
  const cleanTopic = String(topic || '').replace(/\s+/g, ' ').trim().slice(0, 240);
  const fallback = fallbackGeneratedNotes(cleanTopic, safeCount);

  let lastError = null;
  try {
    let result = null;
    const modelsToTry = chatModelsToTry();
    for (const [index, model] of modelsToTry.entries()) {
      try {
        result = await getGroqClient().chat.completions.create({
          model,
          temperature: 0.8,
          max_completion_tokens: Math.min(700, 120 + safeCount * 70),
          messages: [
            {
              role: 'system',
              content:
                'Сгенерируй короткие полезные заметки для локальной памяти Discord-бота. '
                + 'Верни только JSON-массив строк без markdown. '
                + 'Каждая строка до 120 символов, без нумерации, без кавычек внутри текста, без выдуманных личных фактов о реальных людях.',
            },
            {
              role: 'user',
              content: [
                `Количество заметок: ${safeCount}.`,
                cleanTopic ? `Тема: ${cleanTopic}.` : 'Тема: на свое усмотрение.',
                `Исходная голосовая команда: ${request}.`,
              ].join('\n'),
            },
          ],
        }).withResponse();
        trackGroqRateLimits(session?.textChannel, 'generate-memory-notes', result.response, model);
        break;
      } catch (error) {
        lastError = error;
        trackGroqRateLimits(session?.textChannel, 'generate-memory-notes', error, model);
        if (isGroqRateLimitError(error)) markGroqModelOnCooldown(model, 'generate-memory-notes', groqResetHeaderFromError(error, 'tokens'));
        if (GROQ_AUTO_MODEL_FALLBACK && shouldFallbackGroqModel(error) && index < modelsToTry.length - 1) {
          console.warn(`generate memory notes model ${model} failed, trying fallback ${modelsToTry[index + 1]}:`, error.message || error);
          continue;
        }
        throw error;
      }
    }
    if (!result) throw lastError || new Error('No generated notes completion');
    const raw = result.data?.choices?.[0]?.message?.content || '[]';
    const json = extractJsonArray(raw) || raw;
    const parsed = JSON.parse(json);
    const notes = (Array.isArray(parsed) ? parsed : [])
      .map(cleanGeneratedNoteText)
      .filter(Boolean)
      .slice(0, safeCount);
    if (notes.length) return notes;
  } catch (error) {
    console.warn('generate memory notes failed, using fallback:', error.message || error);
  }

  return fallback.slice(0, safeCount);
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

function memoryEntryKey(entry) {
  return `${entry.scope}:${entry.ownerId || ''}:${entry.memory?.id || entry.index}`;
}

function allMemoryEntries(guildId, userId = null) {
  const guildState = getGuildState(guildId);
  const entries = [];
  for (const [index, memory] of (guildState.memories || []).entries()) {
    entries.push({
      key: `guild::${memory.id || index}`,
      scope: 'guild',
      ownerId: '',
      index,
      memory,
    });
  }
  if (userId) {
    for (const [index, memory] of (guildState.userMemories?.[userId] || []).entries()) {
      entries.push({
        key: `user:${userId}:${memory.id || index}`,
        scope: 'user',
        ownerId: userId,
        index,
        memory,
      });
    }
  }
  return entries;
}

function memoryDateScore(memory, normalizedQuery) {
  let score = 0;
  const createdAt = memory.createdAt || 0;
  if (!createdAt) return score;
  if (normalizedQuery.includes('сегодня') || normalizedQuery.includes('сегодняш')) {
    if (isSameLocalDay(createdAt, 0)) score += 0.55;
  }
  if (normalizedQuery.includes('вчера') || normalizedQuery.includes('вчераш')) {
    if (isSameLocalDay(createdAt, -1)) score += 0.55;
  }
  if (normalizedQuery.includes('позавчера')) {
    if (isSameLocalDay(createdAt, -2)) score += 0.55;
  }
  if (normalizedQuery.includes('недел')) {
    if (Date.now() - createdAt <= 7 * 24 * 60 * 60 * 1000) score += 0.25;
  }
  return score;
}

function memorySearchText(entry) {
  const memory = entry.memory || {};
  const createdDate = memory.createdAt
    ? new Date(memory.createdAt).toLocaleString('ru-RU', { dateStyle: 'full', timeStyle: 'short' })
    : '';
  return [
    memory.text,
    memory.userName,
    createdDate,
    entry.scope === 'user' ? 'персональная память обо мне личная заметка' : 'общая память сервера заметка',
    'память заметка запомнил записал сохранил просил',
  ].filter(Boolean).join(' ');
}

function cleanMemoryQuery(text) {
  return String(text || '')
    .replace(/^(?:что\s+ты\s+)?(?:помнишь|знаешь)\s+(?:о|об|про|по)\s+/iu, '')
    .replace(/^(?:что\s+я\s+)?(?:просил|говорил|записывал|сохранял)\s*/iu, '')
    .replace(/^(?:найди|поищи|покажи|выведи)\s+(?:в\s+)?(?:памяти|память|заметках|заметки)\s*(?:о|об|про|по|за)?\s*/iu, '')
    .replace(/^(?:покажи|выведи)\s+(?:память|заметки)\s*(?:о|об|про|по|за)?\s*/iu, '')
    .replace(/^(?:о|об|про|по|за|там|то|котор(?:ое|ые|ый|ую)|которые|что|где|я|мне)\s+/iu, '')
    .trim();
}

function findMemoryMatches(guildId, userId, query) {
  const entries = allMemoryEntries(guildId, userId);
  const normalizedQuery = normalizeCommandText(cleanMemoryQuery(query) || query);
  if (!entries.length) return [];
  if (!normalizedQuery) return entries.map((entry, index) => ({ ...entry, score: 0.1, matchIndex: index }));

  const scored = entries.map((entry, matchIndex) => {
    const text = memorySearchText(entry);
    const textScore = scoreTextRelevance(text, normalizedQuery);
    const directTextScore = scoreTextRelevance(entry.memory?.text || '', normalizedQuery) * 0.9;
    const fuzzyTextScore = normalizedQuery.length >= 5
      ? similarity(entry.memory?.text || '', normalizedQuery) * 0.35
      : 0;
    const dateScore = memoryDateScore(entry.memory || {}, normalizedQuery);
    return {
      ...entry,
      matchIndex,
      score: Math.max(textScore, directTextScore, fuzzyTextScore) + dateScore,
    };
  });

  return scored
    .filter((item) => item.score >= 0.18)
    .sort((a, b) => b.score - a.score);
}

function formatMemoryChoice(entry, index) {
  const memory = entry.memory || {};
  const date = memory.createdAt
    ? new Date(memory.createdAt).toLocaleString('ru-RU', { dateStyle: 'short', timeStyle: 'short' })
    : 'без даты';
  const scope = entry.scope === 'user' ? 'личная' : 'сервер';
  const author = memory.userName ? `${memory.userName}: ` : '';
  return `${index + 1}. [${scope}] ${author}${memory.text} (${date})`;
}

function formatMemorySearchResults(matches) {
  if (!matches.length) return 'Ничего не нашел в памяти.';
  return matches.slice(0, 8).map((entry, index) => formatMemoryChoice(entry, index)).join('\n');
}

function removeMemoryItemsByKeys(guildId, keys) {
  const keySet = new Set(keys);
  const guildState = getGuildState(guildId);
  const removed = [];
  guildState.memories = (guildState.memories || []).filter((memory, index) => {
    const key = `guild::${memory.id || index}`;
    if (!keySet.has(key)) return true;
    removed.push({ scope: 'guild', memory });
    return false;
  });
  for (const [userId, memories] of Object.entries(guildState.userMemories || {})) {
    if (!Array.isArray(memories)) continue;
    guildState.userMemories[userId] = memories.filter((memory, index) => {
      const key = `user:${userId}:${memory.id || index}`;
      if (!keySet.has(key)) return true;
      removed.push({ scope: 'user', ownerId: userId, memory });
      return false;
    });
    if (!guildState.userMemories[userId].length) delete guildState.userMemories[userId];
  }
  if (removed.length) void saveStateStore();
  return removed;
}

function parseSearchMemoryCommand(prompt) {
  const raw = String(prompt || '').trim();
  const normalized = normalizeCommandText(raw);
  if (!normalized) return null;
  const aboutMemory = normalized.includes('памят')
    || normalized.includes('замет')
    || normalized.includes('note')
    || normalized.includes('remember');
  const asksRememberedTopic = /(?:что\s+ты\s+)?(?:помнишь|знаешь)\s+(?:о|об|про|по)\s+.+/u.test(normalized);
  const asksPastRequests = /(?:что\s+я\s+)?(?:просил|говорил|записывал|сохранял)/u.test(normalized);
  const asksSearchMemory = /(найди|поищи|покажи|выведи).{0,20}(памят|замет|note)/u.test(normalized);
  if (!asksRememberedTopic && !asksPastRequests && !asksSearchMemory) return null;
  if (!aboutMemory && !asksRememberedTopic && !asksPastRequests) return null;
  return { action: 'search_memory', text: cleanMemoryQuery(raw).slice(0, 500) || raw.slice(0, 500) };
}

function parseDeleteMemoryCommand(prompt) {
  const raw = String(prompt || '').trim();
  const normalized = normalizeCommandText(raw);
  if (!/(памят|замет|note|memory)/u.test(normalized)) return null;
  if (!/(удал|убер|убери|отмен|отмени|сотри|стери|забудь|delete|remove|forget)/u.test(normalized)) {
    return null;
  }
  if (
    normalized.includes('всю память')
    || normalized.includes('все заметки')
    || normalized.includes('очисти память')
    || normalized.includes('сбрось память')
    || normalized === 'забудь память'
  ) {
    return { action: 'clear_memory' };
  }
  let query = raw
    .replace(/^(?:пожалуйста\s+)?(?:удали|убери|отмени|сотри|стереть|стери|забудь|delete|remove|forget)\s+(?:мне\s+|мо[её]\s+|мои\s+)?(?:память|заметк[уи]?|note|memory)/iu, '')
    .replace(/^(?:память|заметк[ауи]?|note|memory)\s+(?:удали|убери|отмени|сотри|стери|забудь|delete|remove|forget)/iu, '')
    .replace(/^(?:о|об|про|по|за|там|то|котор(?:ое|ые|ый|ую)|которые|что|где|я|мне)\s+/iu, '')
    .trim();
  if (!query) {
    const number = parseSelectionNumber(raw);
    if (number) query = String(number);
  }
  return { action: 'delete_memory', text: query.slice(0, 500) };
}

function parseAmount(value) {
  const normalized = normalizeCommandText(String(value || '').replace(/[’'ʼ`]/g, ''));
  const direct = Number(normalized.replace(',', '.'));
  if (Number.isFinite(direct) && direct > 0) return direct;

  const words = new Map([
    ['один', 1], ['одну', 1], ['одна', 1], ['раз', 1],
    ['два', 2], ['две', 2], ['дві', 2],
    ['три', 3], ['четыре', 4], ['чотири', 4], ['пять', 5], ['шесть', 6], ['шість', 6], ['семь', 7], ['сім', 7],
    ['восемь', 8], ['вісім', 8], ['девять', 9], ['десять', 10], ['пятнадцать', 15],
    ['двадцать', 20], ['тридцать', 30], ['сорок', 40], ['пятьдесят', 50],
    ['шестьдесят', 60],
    ['one', 1], ['a', 1], ['an', 1],
    ['two', 2], ['three', 3], ['four', 4], ['five', 5], ['six', 6], ['seven', 7],
    ['eight', 8], ['nine', 9], ['ten', 10], ['fifteen', 15],
  ]);
  return words.get(normalized) || null;
}

function unitToMs(unit) {
  const normalized = normalizeCommandText(unit);
  if (/^(сек|sec|second)/.test(normalized)) return 1000;
  if (/^(мин|min|minute|хв)/.test(normalized)) return 60 * 1000;
  if (/^(час|hour|hr|годин|год)/.test(normalized)) return 60 * 60 * 1000;
  if (/^(день|дня|днеи|дні|дни|доб|сут|day)/.test(normalized)) return 24 * 60 * 60 * 1000;
  return null;
}

function recurringUnitToMs(unit) {
  const normalized = normalizeCommandText(unit);
  if (/^час/.test(normalized)) return 60 * 60 * 1000;
  if (/^(день|дня|днеи|сут)/.test(normalized)) return 24 * 60 * 60 * 1000;
  if (/^(недел|тижн|week)/.test(normalized)) return 7 * 24 * 60 * 60 * 1000;
  if (/^(месяц|місяц|month)/.test(normalized)) return 30 * 24 * 60 * 60 * 1000;
  return unitToMs(unit);
}

function cleanReminderText(text) {
  return String(text || '')
    .replace(/^(?:что\s+|о том что\s+|про\s+|[:,-]\s*)/iu, '')
    .trim();
}

const REMINDER_CREATE_PATTERN = '(?:напомни(?:ть)?|напоминай|напоминать|нагадай|нагадати|нагадуй|поставь\\s+напоминание|создай\\s+напоминание|добавь\\s+напоминание|сделай\\s+напоминание|запиши\\s+напоминание|постав\\s+нагадування|створи\\s+нагадування|додай\\s+нагадування|напоминание|нагадування|remind)';
const REMINDER_ME_PATTERN = '(?:\\s+(?:мне|меня|мені|me))?';
const REMINDER_UNIT_PATTERN = '(?:секунд[уы]?|сек|seconds?|secs?|минут[уы]?|мин|хвилин[ауыи]?|хв|minutes?|mins?|час(?:а|ов)?|годин[ауыи]?|год|hours?|hrs?|день|дня|дней|дні|дни|доб[ауи]?|сут(?:ки|ок)?|days?)';

function parseReminderCommand(prompt) {
  const text = String(prompt || '').trim();
  const createPrefix = `${REMINDER_CREATE_PATTERN}${REMINDER_ME_PATTERN}`;
  const recurringInterval = text.match(new RegExp(`(?:^|\\s)${createPrefix}\\s+(?:кажд(?:ые|ый|ую|ое)|кожн(?:і|ий|у|е)|every)\\s+(\\d+(?:[.,]\\d+)?|[a-zа-яёіїєґ’'ʼ\`]+)?\\s*(${REMINDER_UNIT_PATTERN}|недел[юияь]*|тижн[іяеів]*|weeks?|месяц(?:а|ев)?|місяц[яіїв]*|months?)\\s*(.*)$`, 'iu'));
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

  const recurringDay = text.match(new RegExp(`(?:^|\\s)${createPrefix}\\s+(?:кажд(?:ый|ое)\\s+день|кожн(?:ий\\s+день|ого\\s+дня)|every\\s+day)\\s*(.*)$`, 'iu'));
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

  const match = text.match(new RegExp(`(?:^|\\s)${createPrefix}\\s+(?:через|in|after)\\s+(.+)$`, 'iu'));
  if (!match) return null;

  const tail = match[1].trim();
  const withAmount = tail.match(new RegExp(`^(\\d+(?:[.,]\\d+)?|[a-zа-яёіїєґ’'ʼ\`]+)\\s*(${REMINDER_UNIT_PATTERN})\\s*(.*)$`, 'iu'));
  const withoutAmount = tail.match(/^(секунду|минуту|хвилину|час|годину|день|добу|сутки|second|minute|hour|day)\s*(.*)$/iu);

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

function reschedulePendingReminders() {
  for (const timer of reminderTimers.values()) clearTimeout(timer);
  reminderTimers.clear();
  schedulePendingReminders();
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
    actionParserModel: getActionParserModel(),
    webSearchEnabled: isWebSearchEnabled(),
    webSearchModel: getWebSearchModel(),
    idleChatterEnabled: isIdleChatterEnabled(),
    idleChatterMinutes: getIdleChatterMinutes(),
    idleChatterUseWeb: isIdleChatterWebEnabled(),
    idleChatterStyle: getIdleChatterStyle(),
    idleLeaveEnabled: isIdleLeaveEnabled(),
    idleLeaveMinutes: getIdleLeaveMinutes(),
    idleLeavePhrase: getIdleLeavePhrase(),
    presenceAnnouncementsEnabled: isPresenceAnnouncementsEnabled(),
    activeDialogueEnabled: isActiveDialogueEnabled(),
    activeDialogueSeconds: getActiveDialogueSeconds(),
    confirmDangerousActions: shouldConfirmDangerousActions(),
    assistantPersona: getAssistantPersona(),
    healthcheckEnabled: isHealthcheckEnabled(),
    presenceAnnouncementCooldownMs: PRESENCE_ANNOUNCEMENT_COOLDOWN_MS,
    sttLanguage: getSttLanguage(),
    sttLanguageGuardEnabled: STT_LANGUAGE_GUARD_ENABLED,
    sttAllowedLanguages: STT_ALLOWED_LANGUAGES,
    ttsProvider: getTtsProvider(),
    macosVoice: getMacosVoice(),
    espeakVoice: getEspeakVoice(),
    espeakSpeed: getEspeakSpeed(),
    edgeVoice: getEdgeVoice(),
    edgeEnglishVoice: getEdgeEnglishVoice(),
    edgeRate: getEdgeRate(),
    edgePitch: getEdgePitch(),
    telegramBotTokenSet: Boolean(getTelegramBotToken()),
    telegramDefaultChatId: getTelegramDefaultChatId(),
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
      wakeListenUntil: session.wakeListenUntil || null,
      wakeListenUserId: session.wakeListenUserId || null,
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
    groqModelCooldowns: groqModelCooldownsObject(),
    memory: memoryStats(),
    storage: storage.info(),
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
  await reloadStateStoreIfChanged().catch((error) => console.error('state store reload failed:', error));
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

function candidateSoundboardNames(sound) {
  return [sound.name, sound.soundId, sound.emoji?.name].filter(Boolean);
}

function soundboardSearchVariants(sound) {
  const variants = new Set();
  for (const name of candidateSoundboardNames(sound)) {
    const normalized = normalizeCommandText(name);
    if (!normalized) continue;
    variants.add(normalized);
    variants.add(compactText(normalized));
    const latin = normalizeCommandText(transliterateCyrillicToLatin(normalized));
    if (latin) {
      variants.add(latin);
      variants.add(compactText(latin));
    }
    const cyrillic = normalizeCommandText(transliterateLatinToCyrillic(normalized));
    if (cyrillic) {
      variants.add(cyrillic);
      variants.add(compactText(cyrillic));
    }
    const collapsed = collapseRepeatedLetters(normalized);
    if (collapsed) {
      variants.add(collapsed);
      variants.add(compactText(collapsed));
    }
  }
  return [...variants].filter((item) => item.length >= 2);
}

function tokenOverlapScore(left, right) {
  const leftTokens = normalizeCommandText(left).split(/\s+/g).filter((token) => token.length >= 2);
  const rightTokens = normalizeCommandText(right).split(/\s+/g).filter((token) => token.length >= 2);
  if (!leftTokens.length || !rightTokens.length) return 0;
  let hits = 0;
  for (const targetToken of rightTokens) {
    if (leftTokens.some((nameToken) => (
      nameToken === targetToken
      || (targetToken.length >= 4 && nameToken.includes(targetToken))
      || (nameToken.length >= 4 && targetToken.includes(nameToken))
    ))) {
      hits += 1;
    }
  }
  return hits / rightTokens.length;
}

function soundboardSimilarity(name, target) {
  const left = normalizeCommandText(name);
  const right = normalizeCommandText(target);
  if (!left || !right) return 0;
  if (left === right) return 1;

  const leftCompact = compactText(left);
  const rightCompact = compactText(right);
  if (leftCompact === rightCompact) return 1;

  const lengthRatio = Math.min(leftCompact.length, rightCompact.length) / Math.max(leftCompact.length, rightCompact.length);
  let best = 0;
  if (lengthRatio >= 0.55 && (leftCompact.includes(rightCompact) || rightCompact.includes(leftCompact))) {
    best = Math.max(best, 0.74 + lengthRatio * 0.18);
  }

  const tokenScore = tokenOverlapScore(left, right);
  if (tokenScore >= 1 && lengthRatio >= 0.45) {
    best = Math.max(best, 0.68 + lengthRatio * 0.18);
  } else if (tokenScore > 0) {
    best = Math.max(best, tokenScore * 0.62);
  }

  const lengthDelta = Math.abs(leftCompact.length - rightCompact.length);
  if (lengthDelta <= 3 && (leftCompact[0] === rightCompact[0] || lengthDelta <= 1)) {
    const distance = levenshteinDistance(leftCompact, rightCompact);
    best = Math.max(best, 1 - distance / Math.max(leftCompact.length, rightCompact.length));
  }

  return best;
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
      getNames: candidateMemberSearchNames,
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
    getNames: candidateMemberSearchNames,
    getLabel: (member) => member.displayName,
    emptyError: 'Кого выбрать? Скажи имя, ник, тег или ID после команды.',
    notFoundError: () => voiceTarget.error,
    ambiguousError: (labels) => `Нашел несколько похожих участников сервера: ${labels}. Скажи имя точнее.`,
  });
  if (!cachedResult.error) return { member: cachedResult.item };

  const rawQuery = String(targetText || '').trim();
  const searchQueries = [...new Set(
    nameSearchVariants(rawQuery)
      .map((item) => item.replace(/\s+/g, ' ').trim())
      .filter((item) => item.length >= 2 && !SEARCH_STOP_TOKENS.has(item)),
  )].slice(0, 10);
  if (!searchQueries.length) return cachedResult;

  const searched = new Map();
  for (const query of searchQueries) {
    const result = await session.guild.members.search({ query, limit: 20 }).catch((error) => {
      console.error(`member search failed query="${query}":`, error);
      return null;
    });
    for (const member of result?.values?.() || []) {
      if (member?.id) searched.set(member.id, member);
    }
    if (searched.size >= 20) break;
  }
  if (!searched.size) return cachedResult;

  const searchResult = findBestFuzzy([...searched.values()].filter((member) => member.id !== client.user.id), targetText, {
    getNames: candidateMemberSearchNames,
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

async function findCategoryChannel(session, channelText) {
  const channels = await session.guild.channels.fetch();
  const categories = [...channels.values()].filter(
    (channel) => channel && channel.type === ChannelType.GuildCategory,
  );

  const result = findBestFuzzy(categories, channelText, {
    getNames: candidateChannelNames,
    getLabel: (channel) => channel.name,
    emptyError: 'Какую категорию выбрать?',
    notFoundError: () => 'Не нашел такую категорию.',
    ambiguousError: (labels) => `Нашел несколько похожих категорий: ${labels}. Скажи точнее.`,
  });
  return result.error ? null : result.item;
}

async function findThreadChannel(session, threadText) {
  const channels = await session.guild.channels.fetch();
  const threads = [...channels.values()].filter(
    (channel) => channel && [
      ChannelType.PublicThread,
      ChannelType.PrivateThread,
      ChannelType.AnnouncementThread,
    ].includes(channel.type),
  );
  if (session.textChannel && [
    ChannelType.PublicThread,
    ChannelType.PrivateThread,
    ChannelType.AnnouncementThread,
  ].includes(session.textChannel.type)) {
    threads.unshift(session.textChannel);
  }

  const target = String(threadText || '').trim();
  if (!target && threads[0]) return threads[0];

  const result = findBestFuzzy(threads, target, {
    getNames: candidateChannelNames,
    getLabel: (channel) => channel.name,
    emptyError: 'Какой тред выбрать?',
    notFoundError: () => 'Не нашел такой тред.',
    ambiguousError: (labels) => `Нашел несколько похожих тредов: ${labels}. Скажи точнее.`,
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

async function fetchSoundboardSounds(session) {
  const sounds = [];
  const guildSounds = await session.guild.soundboardSounds.fetch().catch((error) => {
    console.error('guild soundboard fetch failed:', error);
    return null;
  });
  for (const sound of guildSounds?.values?.() || []) {
    if (sound?.available !== false) sounds.push(sound);
  }

  const defaultSounds = await client.fetchDefaultSoundboardSounds().catch((error) => {
    console.error('default soundboard fetch failed:', error);
    return null;
  });
  for (const sound of defaultSounds?.values?.() || []) {
    if (sound?.available !== false) sounds.push(sound);
  }
  return sounds;
}

async function findSoundboardSound(session, soundText) {
  const sounds = await fetchSoundboardSounds(session);
  const target = cleanSoundboardTarget(soundText);
  if (!target) return { error: 'Какой звук включить? Назови звук с soundboard.' };

  const scored = [];
  for (const sound of sounds) {
    const variants = soundboardSearchVariants(sound);
    let bestScore = 0;
    let bestName = sound.name || sound.soundId;
    for (const variant of variants) {
      const score = soundboardSimilarity(variant, target);
      if (score > bestScore) {
        bestScore = score;
        bestName = variant;
      }
    }
    if (bestScore >= 0.62) scored.push({ sound, score: bestScore, bestName });
  }

  scored.sort((a, b) => b.score - a.score);
  if (!scored.length) return { error: `Не нашел soundboard-звук “${target}”.` };

  const [best, second] = scored;
  const confident = best.score >= 0.86 || (!second && best.score >= 0.74) || (best.score >= 0.76 && (!second || best.score - second.score >= 0.18));
  if (!confident) {
    const labels = scored
      .slice(0, 5)
      .map(({ sound }) => sound.name || sound.soundId)
      .join(', ');
    return { error: `Нашел несколько похожих звуков: ${labels}. Скажи название точнее.` };
  }

  console.log(`soundboard match "${target}" -> "${best.sound.name || best.sound.soundId}" score=${best.score.toFixed(2)} matched="${best.bestName}"`);
  return { sound: best.sound, allSounds: sounds };
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

function normalizeCategoryName(name) {
  const cleaned = String(name || '').trim().replace(/\s+/g, ' ').slice(0, 100);
  return cleaned || 'Новая категория';
}

function parseBooleanIntent(text, defaultValue = true) {
  const normalized = normalizeCommandText(text);
  if (/(выключ|отключ|убери|убрать|скрой|скрыть|не\s+показывай|false|off|disable|hide)/u.test(normalized)) return false;
  if (/(включ|покажи|сделай|true|on|enable|show)/u.test(normalized)) return true;
  return defaultValue;
}

function parseColorValue(text) {
  const raw = String(text || '').trim();
  const hex = raw.match(/#?[0-9a-f]{6}/iu)?.[0];
  if (hex) return `#${hex.replace('#', '')}`;
  const normalized = normalizeCommandText(raw);
  const map = {
    красный: '#ff3b30',
    красная: '#ff3b30',
    red: '#ff3b30',
    синий: '#2997ff',
    синяя: '#2997ff',
    blue: '#2997ff',
    зеленый: '#34c759',
    зеленая: '#34c759',
    зелений: '#34c759',
    green: '#34c759',
    желтый: '#ffd60a',
    желтая: '#ffd60a',
    yellow: '#ffd60a',
    фиолетовый: '#bf5af2',
    фиолетовая: '#bf5af2',
    purple: '#bf5af2',
    розовый: '#ff2d55',
    розовая: '#ff2d55',
    pink: '#ff2d55',
    белый: '#ffffff',
    white: '#ffffff',
    черный: '#111111',
    black: '#111111',
    оранжевый: '#ff9500',
    orange: '#ff9500',
  };
  return map[normalized] || null;
}

const ACTION_KEYWORDS = [
  'отключ', 'відключ', 'выкин', 'выкини', 'викинь', 'дискон',
  'кикни', 'кікни', 'кікні', 'кик', 'кік', 'исключ', 'виключ', 'удали участника',
  'бан', 'забань', 'разбан',
  'таймаут', 'timeout', 'мут на', 'накажи', 'сними таймаут',
  'перемест', 'перемісти', 'перенеси', 'перекин', 'перетащи', 'перетягни', 'верни обратно', 'верни назад',
  'мут', 'замуть', 'зам ють', 'размут', 'размуть', 'розмут', 'заглуш', 'разглуш', 'микрофон', 'мікрофон',
  'деаф', 'оглуш',
  'роль', 'выдай роль', 'дай роль', 'забери роль', 'убери роль',
  'ник', 'никнейм', 'переименуй участника',
  'закрой', 'открой', 'залочь', 'разлочь', 'заблок', 'разблок',
  'переимен', 'назови', 'имя канала',
  'создай канал', 'создай чат', 'создай войс', 'создай голосовой', 'створи канал', 'створи голосовий', 'create channel',
  'удали канал', 'снеси канал',
  'лимит', 'слоумод', 'slowmode', 'медленный режим',
  'очист', 'удали сообщения', 'почист',
  'напиши', 'отправь в чат', 'скажи в чат',
  'стоп', 'замолчи', 'перестань говорить', 'хватит', 'остановись', 'останови', 'харош', 'хорош',
  'сбрось память', 'забудь память', 'очисти память', 'запомни', 'запиши в память',
  'найди в памяти', 'покажи заметки', 'удали заметку', 'удали память', 'что ты помнишь про',
  'напомни', 'напоминания', 'отмени напоминания', 'удали напоминание', 'убери напоминание',
  'забудь диалог', 'сбрось диалог', 'новый диалог',
  'статус', 'лимиты', 'limits',
  'пауза', 'не слушай', 'продолжай', 'слушай дальше',
  'замуть всех', 'размуть всех', 'отключи всех', 'перемести всех',
  'создай роль', 'удали роль',
  'тема чата', 'описание чата', 'закрепи',
  'саундборд', 'soundboard', 'звуковая панель', 'звуковую панель', 'звук панели', 'проиграй звук',
  'инвайт', 'приглашение', 'invite',
  'категория', 'категорию', 'category',
  'тред', 'thread', 'ветку', 'ветка',
  'переименуй сервер', 'назови сервер', 'цвет роли', 'роль цветом',
  'покажи участников', 'покажи роли', 'покажи каналы',
  'телеграм', 'телеграмм', 'телеграмму', 'телега', 'телегу', 'телеге', 'тележк',
  'телиграм', 'telegram', 'telega', 'tg', 'тг',
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
  'что ты помнишь про VPS',
  'найди в памяти созвон',
  'удали заметку про созвон',
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
  'найди свежие новости про Groq и отправь в телеграм',
  'поищи инфу про Groq и скинь в телегу',
  'пробей новости Groq и закинь в тг',
  'напиши заметку в телеграм что завтра созвон в 20:00',
  'сохрани в телеге заметку завтра созвон в 20:00',
  'отправь последний ответ в телеграм',
  'продублируй это в тг',
  'покажи телеграм чаты',
];

function looksLikeAction(prompt) {
  const normalized = normalizeCommandText(prompt);
  if (ACTION_KEYWORDS.some((keyword) => normalized.includes(keyword))) return true;
  return [
    /(^|\s)(создай|создать|створи|зроби|create)\s+(?:новый\s+|новий\s+|new\s+)?(?:голосов\p{L}*|войс|воис|voice|текстов\p{L}*|чат|channel)(\s|$)/u,
    /(^|\s)(верни|вернуть|поверни|повернути)\s+.+\s+(?:обратно|назад)(\s|$)/u,
    /(^|\s)(отключи|выключи|вимкни|увімкни|включи)\s+(?:микрофон|мікрофон|звук|mic|microphone)(\s|$)/u,
    /(^|\s)(проиграй|включи|запусти|поставь|play)\s+(?:звук|саунд|sound)(\s|$)/u,
    /(^|\s)(телеграмм?|телеграмму|телега|телегу|телеге|тележк\p{L}*|телиграмм?|telegramm?|telega|tg|тг)(\s|$)/u,
    /(^|\s)(создай|сделай|create)\s+(?:инвайт|приглашение|invite|тред|thread|категор)/u,
  ].some((pattern) => pattern.test(normalized));
}

const AI_ACTION_VERB_PATTERN = /(^|\s)(сделай|сделать|создай|создать|створи|зроби|удали|удалить|убери|убрать|очист\p{L}*|почист\p{L}*|постав\p{L}*|установ\p{L}*|включ\p{L}*|выключ\p{L}*|выруб\p{L}*|отключ\p{L}*|подключ\p{L}*|заглуш\p{L}*|разглуш\p{L}*|замут\p{L}*|размут\p{L}*|перемест\p{L}*|перенес\p{L}*|перетащ\p{L}*|перекин\p{L}*|верни|вернуть|выдай|дай|забери|сними|назнач\p{L}*|переимен\p{L}*|назови|измени|поменяй|закрой|открой|заблок\p{L}*|разблок\p{L}*|залоч\p{L}*|разлоч\p{L}*|закреп\p{L}*|напиши|отправ\p{L}*|скинь|скини|кинь|кини|закин\p{L}*|передай|запомн\p{L}*|запиши|сохрани|напомн\p{L}*|отмени|сброс\p{L}*|покажи|выведи|проигра\p{L}*|запусти|останов\p{L}*|замолчи|хватит|харош|mute|unmute|disconnect|kick|ban|move|create|delete|remove|rename|lock|unlock|list|show|clear|pin|archive|timeout|remember|remind|pause|resume|stop|send|play)(\s|$)/u;

const AI_ACTION_TARGET_PATTERN = /(^|\s)(участник\p{L}*|пользовател\p{L}*|юзер\p{L}*|люд\p{L}*|человек\p{L}*|всех|всіх|all|его|ее|её|их|войс\p{L}*|воис\p{L}*|голосов\p{L}*|комнат\p{L}*|voice|room|микрофон\p{L}*|мікрофон\p{L}*|звук\p{L}*|саунд\p{L}*|sound|soundboard|канал\p{L}*|чат\p{L}*|текстов\p{L}*|channel|chat|роль|роли|ролью|рол\p{L}*|модер\p{L}*|админ\p{L}*|role|ник\p{L}*|nickname|таймаут\p{L}*|timeout|сервер\p{L}*|server|категор\p{L}*|category|тред\p{L}*|ветк\p{L}*|thread|инвайт\p{L}*|приглаш\p{L}*|invite|сообщен\p{L}*|месседж\p{L}*|message|слоумод\p{L}*|slowmode|лимит\p{L}*|limit|тема|тему|topic|памят\p{L}*|memory|заметк\p{L}*|note|напомин\p{L}*|reminder|статус|status|лимиты|limits|телеграмм?|телега|телегу|телеге|тележк\p{L}*|telegramm?|telega|tg|тг)(\s|$)/u;

function looksLikeKnowledgeQuestion(normalized) {
  return /^(?:расскажи|объясни|обьясни|поясни|что\s+такое|кто\s+такой|как\s+работает|почему|зачем|какая|какой|какие|сколько|what\s+is|how\s+does|explain)(?:\s|$)/u.test(normalized);
}

function shouldTryAiActionParser(prompt) {
  const normalized = normalizeCommandText(prompt);
  if (!normalized) return false;
  if (looksLikeKnowledgeQuestion(normalized)) return false;
  if (looksLikeAction(prompt)) return true;

  const words = normalized.split(/\s+/g).filter(Boolean);
  if (words.length > 32) return false;

  const hasVerb = AI_ACTION_VERB_PATTERN.test(normalized);
  if (!hasVerb) return false;

  if (AI_ACTION_TARGET_PATTERN.test(normalized)) return true;

  if (/^(?:стоп|stop|pause|resume|пауза|продолжай|замолчи|хватит|харош)$/u.test(normalized)) return true;
  if (/^(?:покажи|выведи|show|list)\s+(?:памят\p{L}*|напомин\p{L}*|статус|лимит\p{L}*)/u.test(normalized)) return true;

  return false;
}

function cleanMemberTargetText(value) {
  return normalizeCommandText(value || '')
    .replace(/^(?:пользовател[ья]|участник[а]?|юзер[а]?|user)\s+/u, '')
    .replace(/^(?:микрофон|микрофона|мікрофон|мікрофона|звук|звука|microphone|mic)\s+/u, '')
    .replace(/^у\s+/u, '')
    .replace(/^(?:me|ми)\s+(?=\S)/u, '')
    .replace(/\s+(?:из|с|со|от)\s+(?:голосового\s+)?(?:войса|воиса|voice|voice channel|канала|чата)$/u, '')
    .replace(/\s+(?:в|на)\s+(?:войсе|воисе|voice|канале|чате)$/u, '')
    .replace(/[,\s]+$/u, '')
    .trim();
}

function cleanCreatedChannelName(value, fallback) {
  return String(value || '')
    .replace(/^[,\s:-]+/u, '')
    .replace(/^(?:с\s+именем|с\s+названием|назови|под\s+названием|called|named)\s+/iu, '')
    .trim() || fallback;
}

function cleanSoundboardTarget(value) {
  return normalizeCommandText(value || '')
    .replace(/^(?:звук|саунд|sound|soundboard|саундборд)\s+/u, '')
    .replace(/^(?:из|с|со|на)\s+(?:звуковой\s+панели|саундборда|soundboard)\s+/u, '')
    .replace(/^(?:под\s+названием|с\s+названием|который\s+называется|called|named)\s+/u, '')
    .trim();
}

function cleanInviteCode(value) {
  return String(value || '')
    .trim()
    .replace(/^https?:\/\/(?:www\.)?(?:discord\.gg|discord\.com\/invite)\//iu, '')
    .replace(/[^\w-]/g, '')
    .slice(0, 80);
}

const TELEGRAM_WORD_PATTERN = '(?:телеграмм?|телеграмму|телеграме|телеграмом|телегу|телега|телеге|тележк\\p{L}*|телиграмм?|телигу|телегач|telegramm?|telega|tg|тг|теге)';
const TELEGRAM_SEND_VERB_PATTERN = '(?:отправь|отправи|отправить|скинь|скини|кинь|кини|закинь|закини|перекинь|перекини|перешли|перешли|перешлите|перешли-ка|передай|напиши|написать|черкан[иь]|черкани|черкни|чиркани|добавь|запиши|сохрани|продублируй|дублируй|send|forward|post|write|drop)';
const TELEGRAM_SEARCH_VERB_PATTERN = '(?:найди|поищи|загугли|гуглани|посмотри|пробей|узнай|выясни|проверь|собери|search|find|google|look\\s+up)';
const TELEGRAM_NOTE_WORD_PATTERN = '(?:заметк\\p{L}*|заметочк\\p{L}*|note|notes)';

function telegramRegex(source, flags = 'iu') {
  return new RegExp(
    source
      .replaceAll('{{TG}}', TELEGRAM_WORD_PATTERN)
      .replaceAll('{{SEND}}', TELEGRAM_SEND_VERB_PATTERN)
      .replaceAll('{{SEARCH}}', TELEGRAM_SEARCH_VERB_PATTERN)
      .replaceAll('{{NOTE}}', TELEGRAM_NOTE_WORD_PATTERN),
    flags,
  );
}

function hasTelegramMention(text) {
  const normalized = normalizeCommandText(text);
  return telegramRegex('(^|\\s){{TG}}(\\s|$)').test(normalized);
}

function wantsTelegramOutputDestination(text) {
  const normalized = normalizeCommandText(text);
  return telegramRegex('(^|\\s)(?:в|во|на|to)\\s+{{TG}}(\\s|$)').test(normalized);
}

function stripTelegramPhrases(text) {
  return String(text || '')
    .replace(telegramRegex('(?:и\\s+)?{{SEND}}\\s+(?:это\\s+|туда\\s+)?(?:в|во|на|to)\\s+{{TG}}', 'giu'), ' ')
    .replace(telegramRegex('(?:в|во|на|to)\\s+{{TG}}\\s+{{SEND}}?', 'giu'), ' ')
    .replace(telegramRegex('{{TG}}', 'giu'), ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function cleanTelegramMessageText(text) {
  return stripTelegramPhrases(text)
    .replace(/^(?:сообщение|сообщуху|месседж|пост|текст|message|msg)\s+/iu, '')
    .replace(/^(?:что|:)\s*/iu, '')
    .trim();
}

function cleanTelegramSearchQuery(text) {
  return stripTelegramPhrases(text)
    .replace(telegramRegex('^{{SEARCH}}\\s+(?:в\\s+интернете\\s+|интернет\\s+|web\\s+)?'), '')
    .replace(/^(?:информацию|инфу|данные|сводку|кратко|news|новости)\s+(?:про|о|об|about)\s+/iu, '')
    .replace(/^(?:что|как|какая|какой)\s+там\s+/iu, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseTelegramSimpleAction(prompt) {
  const raw = String(prompt || '').trim();
  const normalized = normalizeCommandText(raw);
  if (!hasTelegramMention(normalized)) return null;
  const toTelegram = wantsTelegramOutputDestination(raw);

  if (/(^|\s)(статус|status|настройк\p{L}*|подключен\p{L}*)(\s|$)/u.test(normalized)) {
    return { action: 'telegram_status', toTelegram };
  }
  if (/(^|\s)(чаты|чат[ыа]?|chat|chats|id|айди|куда)(\s|$)/u.test(normalized) && /(покажи|список|выведи|дай|list|show|какие)/u.test(normalized)) {
    return { action: 'telegram_list_chats', toTelegram };
  }
  if (/(^|\s)(тест|test)(\s|$)/u.test(normalized)) {
    return { action: 'telegram_test' };
  }
  if (/(очисти|удали|сбрось|отключи|clear|remove|delete).{0,40}(телеграм|telegram|tg)/u.test(normalized)) {
    return { action: 'telegram_clear' };
  }
  if (/(память|memories|memory)/u.test(normalized) && telegramRegex('{{SEND}}').test(normalized)) {
    return { action: 'telegram_send_memory' };
  }
  if (/(напомин|reminders)/u.test(normalized) && telegramRegex('{{SEND}}').test(normalized)) {
    return { action: 'telegram_send_reminders' };
  }
  if (/(последн\p{L}*\s+(?:ответ|сообщение|реплик\p{L}*)|то\s+что\s+(?:сказал|ответил)|мой\s+ответ|этот\s+ответ|это|вот\s+это|last answer|last reply)/u.test(normalized) && telegramRegex('{{SEND}}').test(normalized)) {
    return { action: 'telegram_send_last_answer' };
  }

  const noteMatch = raw.match(telegramRegex('(?:{{NOTE}}|сохрани\\s+{{NOTE}}|запиши\\s+{{NOTE}})\\s*(?:в|во|на|to)?\\s*(?:{{TG}})?\\s*(?:что|:)?\\s+([\\s\\S]+)'));
  if (noteMatch?.[1]?.trim()) {
    return { action: 'telegram_send_note', text: cleanTelegramMessageText(noteMatch[1]) };
  }
  const destinationNoteMatch = raw.match(telegramRegex('(?:в|во|на|to)\\s+{{TG}}\\s+(?:{{NOTE}}|сохрани\\s+{{NOTE}}|запиши\\s+{{NOTE}})\\s*(?:что|:)?\\s+([\\s\\S]+)'));
  if (destinationNoteMatch?.[1]?.trim()) {
    return { action: 'telegram_send_note', text: cleanTelegramMessageText(destinationNoteMatch[1]) };
  }

  if (telegramRegex('{{SEARCH}}').test(normalized) || /(новост|курс|цена|погода|сводк|инф\p{L}*|актуальн|свеж\p{L}*|weather|news|price|latest|current)/u.test(normalized)) {
    const query = cleanTelegramSearchQuery(raw);
    if (query) return { action: 'telegram_search_and_send', text: query };
  }

  const destinationFirst = raw.match(telegramRegex('(?:в|во|на|to)\\s+{{TG}}\\s+{{SEND}}\\s+([\\s\\S]+)'));
  if (destinationFirst?.[1]?.trim()) {
    return { action: 'telegram_send_message', text: cleanTelegramMessageText(destinationFirst[1]) };
  }
  const telegramFirst = raw.match(telegramRegex('{{TG}}\\s+{{SEND}}\\s+([\\s\\S]+)'));
  if (telegramFirst?.[1]?.trim()) {
    return { action: 'telegram_send_message', text: cleanTelegramMessageText(telegramFirst[1]) };
  }

  const sendAfterTelegram = raw.match(telegramRegex('{{SEND}}\\s+(?:в|во|на|to)\\s+{{TG}}\\s+([\\s\\S]+)'));
  if (sendAfterTelegram?.[1]?.trim()) {
    return { action: 'telegram_send_message', text: cleanTelegramMessageText(sendAfterTelegram[1]) };
  }

  const sendBeforeTelegram = raw.match(telegramRegex('{{SEND}}\\s+([\\s\\S]+?)\\s+(?:в|во|на|to)\\s+{{TG}}$'));
  if (sendBeforeTelegram?.[1]?.trim()) {
    return { action: 'telegram_send_message', text: cleanTelegramMessageText(sendBeforeTelegram[1]) };
  }

  const cleaned = cleanTelegramMessageText(raw);
  if (cleaned && telegramRegex('{{SEND}}').test(normalized)) {
    return { action: 'telegram_send_message', text: cleaned };
  }

  return null;
}

const BUSY_ALLOWED_SIMPLE_ACTIONS = new Set([
  'stop_speaking',
  'pause_listening',
  'resume_listening',
  'telegram_send_message',
  'telegram_send_note',
  'telegram_search_and_send',
  'telegram_send_last_answer',
  'telegram_send_memory',
  'telegram_send_reminders',
  'telegram_list_chats',
  'telegram_status',
  'telegram_test',
  'generate_memory_notes',
]);

function canHandleSimpleActionWhileBusy(action) {
  return action ? BUSY_ALLOWED_SIMPLE_ACTIONS.has(action) : false;
}

function extractGeneratedNotesCount(prompt) {
  const normalized = normalizeCommandText(prompt);
  const direct = normalized.match(/(?:^|\s)(\d{1,2})(?:\s|$)/u);
  if (direct) return Math.max(1, Math.min(10, Number(direct[1])));
  for (const token of normalized.split(/\s+/u)) {
    const amount = parseAmount(token);
    if (amount) return Math.max(1, Math.min(10, Math.round(amount)));
  }
  return 5;
}

function cleanGeneratedNotesTopic(prompt) {
  return normalizeCommandText(prompt)
    .replace(/^(?:придумай|придумать|сгенерируй|сгенерировать|создай|создать|составь|составить|напиши|написать)\s+/u, '')
    .replace(/(?:мне|нам|для\s+меня|для\s+нас)\s+/gu, '')
    .replace(/\b\d{1,2}\b/gu, '')
    .replace(/\b(?:один|одну|одна|два|две|три|четыре|пять|шесть|семь|восемь|девять|десять|five|notes?)\b/gu, '')
    .replace(/\b(?:заметк\p{L}*|заметочк\p{L}*|нотатк\p{L}*|note|notes)\b/gu, '')
    .replace(/\b(?:и|та|а|их|это|потом|сразу|на\s+свое\s+усмотрение|на\s+своё\s+усмотрение|любые|какие\s+угодно)\b/gu, ' ')
    .replace(/\b(?:запиши|записать|сохрани|сохранить|запомни|запомнить|добавь|добавить|оставь|оставить)\b/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseGenerateMemoryNotesCommand(prompt) {
  const normalized = normalizeCommandText(prompt);
  if (!/(заметк\p{L}*|нотатк\p{L}*|notes?)/u.test(normalized)) return null;
  if (!/(придумай|придумать|сгенерируй|сгенерировать|создай|создать|составь|составить|напиши|написать)/u.test(normalized)) return null;
  if (!/(запиши|записать|сохрани|сохранить|запомни|запомнить|добавь|добавить|оставь|оставить)/u.test(normalized)) return null;
  return {
    action: 'generate_memory_notes',
    value: extractGeneratedNotesCount(prompt),
    text: cleanGeneratedNotesTopic(prompt),
    toTelegram: wantsTelegramOutputDestination(prompt),
    originalPrompt: String(prompt || '').trim(),
  };
}

function isPronounTarget(value) {
  const normalized = normalizeCommandText(value);
  return !normalized || /^(?:его|ее|её|их|туда|обратно|назад|him|her|them|it)$/u.test(normalized);
}

function parseSimpleMemberAction(prompt) {
  const normalized = normalizeCommandText(prompt);
  const moveBackMatch = normalized.match(/^(?:верни|вернуть|поверни|повернути)\s+(.+?)?\s*(?:обратно|назад)(?:\s+(?:в|на)\s+(?:канал|войс|воис|voice))?$/u);
  if (moveBackMatch) {
    return {
      action: 'move_member_back',
      target: isPronounTarget(moveBackMatch[1]) ? '' : cleanMemberTargetText(moveBackMatch[1]),
    };
  }

  const moveMatch = normalized.match(/^(?:перемести|перемісти|перенеси|перекинь|перетащи|перетягни)\s+(.+?)\s+(?:в|на|до)\s+(.+)$/u);
  if (moveMatch?.[1]?.trim() && moveMatch?.[2]?.trim()) {
    return {
      action: 'move_member',
      target: cleanMemberTargetText(moveMatch[1]),
      channel: moveMatch[2].trim(),
    };
  }

  const patterns = [
    { action: 'mute_member', re: /^(?:замуть|замут|зам ють|замють|мутни|заглуши|приглуши|выключи микрофон|отключи микрофон|вимкни мікрофон|відключи мікрофон|mute)\s+(.+)$/u },
    { action: 'unmute_member', re: /^(?:размуть|размут|розмуть|розмут|разглуши|верни микрофон|включи микрофон|увімкни мікрофон|unmute)\s+(.+)$/u },
    { action: 'disconnect_member', re: /^(?:отключи|отключить|відключи|выкинь|выкини|выкин|викинь|дисконнектни|дисконектни|дискон|disconnect)\s+(.+)$/u },
    { action: 'deafen_member', re: /^(?:оглуши|задефай|деафни)\s+(.+)$/u },
    { action: 'undeafen_member', re: /^(?:разоглуши|раздефай|андефни)\s+(.+)$/u },
    { action: 'kick_member', re: /^(?:кикни|кікни|кікні|кик|кік|исключи|виключи|kick)\s+(.+)$/u },
    { action: 'ban_member', re: /^(?:забань|бан|заблокируй|забан|ban)\s+(.+)$/u },
  ];
  for (const { action, re } of patterns) {
    const match = normalized.match(re);
    const target = cleanMemberTargetText(match?.[1]);
    if (target) return { action, target };
  }
  return null;
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

  const telegramAction = parseTelegramSimpleAction(prompt);
  if (telegramAction) return telegramAction;

  const generatedNotes = parseGenerateMemoryNotesCommand(prompt);
  if (generatedNotes) return generatedNotes;

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

  const deleteMemory = parseDeleteMemoryCommand(prompt);
  if (deleteMemory) return deleteMemory;

  const rememberUserMatch = String(prompt || '').trim().match(/^(?:запомни|запиши в память)\s+(?:обо мне|про меня|для меня|мне)\s*(?:что|:)?\s+(.+)$/iu);
  if (rememberUserMatch?.[1]?.trim()) {
    return { action: 'remember_user_memory', text: rememberUserMatch[1].trim() };
  }
  const noteMatch = String(prompt || '').trim().match(/^(?:запиши\s+заметку|добавь\s+заметку|сделай\s+заметку|создай\s+заметку|оставь\s+заметку|сохрани\s+заметку|додай\s+нотатк[ау]|запиши\s+нотатк[ау]|note|remember\s+note)\s*(?:что|:)?\s+(.+)$/iu);
  if (noteMatch?.[1]?.trim()) {
    return { action: 'remember_memory', text: noteMatch[1].trim() };
  }
  const rememberMatch = String(prompt || '').trim().match(/^(?:запомни|запиши в память|запиши|сохрани)\s*(?:что|:)?\s+(.+)$/iu);
  if (rememberMatch?.[1]?.trim()) {
    return { action: 'remember_memory', text: rememberMatch[1].trim() };
  }
  if (normalized.includes('что ты помнишь обо мне') || normalized.includes('что помнишь обо мне') || normalized.includes('покажи память обо мне')) {
    return { action: 'show_user_memory' };
  }
  const searchMemory = parseSearchMemoryCommand(prompt);
  if (searchMemory) return searchMemory;

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
  if ((normalized.includes('отключ') || normalized.includes('відключ') || normalized.includes('выкин') || normalized.includes('викинь') || normalized.includes('дискон')) && /(всех|всіх|all)/u.test(normalized)) {
    return { action: 'disconnect_all' };
  }
  if ((normalized.includes('замуть') || normalized.includes('зам ють') || normalized.includes('замут') || normalized.includes('мут')) && /(всех|всіх|all)/u.test(normalized)) {
    return { action: 'mute_all' };
  }
  if ((normalized.includes('размуть') || normalized.includes('розмуть') || normalized.includes('размут') || normalized.includes('розмут')) && /(всех|всіх|all)/u.test(normalized)) {
    return { action: 'unmute_all' };
  }
  const moveAllMatch = normalized.match(/(?:перемести|перемісти|перенеси|перекинь|перетащи|перетягни)\s+(?:всех|всіх|all)\s+(?:в|на|до)\s+(.+)$/u);
  if (moveAllMatch?.[1]?.trim()) {
    return { action: 'move_all_members', channel: moveAllMatch[1].trim() };
  }
  if (/(?:покажи|список|какие|list).{0,30}(?:звуки|саундборд|soundboard|sounds)/u.test(normalized)) {
    return { action: 'list_soundboard_sounds' };
  }
  const deleteSoundMatch = normalized.match(/^(?:удали|убери|delete|remove)\s+(?:(?:звук|саунд|sound)\s+)?(.+?)(?:\s+(?:из|с)\s+(?:звуковой\s+панели|саундборда|soundboard))?$/u);
  if (deleteSoundMatch?.[1]?.trim() && /(звук|саунд|sound|soundboard|панел)/u.test(normalized)) {
    return { action: 'delete_soundboard_sound', text: cleanSoundboardTarget(deleteSoundMatch[1]) };
  }
  const renameSoundMatch = normalized.match(/^(?:переименуй|rename)\s+(?:(?:звук|саунд|sound)\s+)?(.+?)\s+(?:в|на)\s+(.+)$/u);
  if (renameSoundMatch?.[1]?.trim() && renameSoundMatch?.[2]?.trim() && /(звук|саунд|sound|soundboard|панел)/u.test(normalized)) {
    return {
      action: 'rename_soundboard_sound',
      text: cleanSoundboardTarget(renameSoundMatch[1]),
      value: renameSoundMatch[2].trim(),
    };
  }
  const playSoundMatch = normalized.match(/^(?:проиграй|включи|запусти|поставь|дай|play)\s+(?:(?:звук|саунд|sound)\s+)?(.+?)(?:\s+(?:на|из)\s+(?:звуковой\s+панели|саундборде|саундборда|soundboard))?$/u);
  if (playSoundMatch?.[1]?.trim() && !/(?:микрофон|мікрофон|звука\s+(?:для|у))/.test(normalized)) {
    const target = cleanSoundboardTarget(playSoundMatch[1]);
    if (target && /(звук|саунд|sound|soundboard|панел)/u.test(normalized)) {
      return { action: 'play_soundboard_sound', text: target };
    }
  }
  if (/(?:покажи|список|list).{0,30}(?:участник|людей|members|пользовател)/u.test(normalized) || normalized === 'кто в войсе') {
    return { action: 'list_members' };
  }
  if (/(?:покажи|список|list).{0,30}(?:роли|ролей|roles)/u.test(normalized)) {
    return { action: 'list_roles' };
  }
  if (/(?:покажи|список|list).{0,30}(?:каналы|каналов|channels)/u.test(normalized)) {
    return { action: 'list_channels' };
  }
  if (/(?:покажи|список|list).{0,30}(?:инвайт|приглаш|invite)/u.test(normalized)) {
    return { action: 'list_invites' };
  }
  const inviteMatch = normalized.match(/^(?:создай|сделай|дай|сгенерируй|create)\s+(?:инвайт|приглашение|invite)(?:\s+(?:в|на|для)\s+(.+))?$/u);
  if (inviteMatch) {
    return { action: 'create_invite', channel: inviteMatch[1]?.trim() || '' };
  }
  const deleteInviteMatch = normalized.match(/^(?:удали|убери|отмени|delete|remove)\s+(?:инвайт|приглашение|invite)\s+(.+)$/u);
  if (deleteInviteMatch?.[1]?.trim()) {
    return { action: 'delete_invite', text: cleanInviteCode(deleteInviteMatch[1]) };
  }
  const createCategoryMatch = normalized.match(/^(?:создай|создать|створи|зроби|create)\s+(?:(?:новую|новий|new)\s+)?(?:категор\p{L}*|category)(?:\s+(.+))?$/u);
  if (createCategoryMatch) {
    return { action: 'create_category', text: cleanCreatedChannelName(createCategoryMatch[1], 'Новая категория') };
  }
  const moveChannelCategoryMatch = normalized.match(/^(?:перемести|перенеси|перекинь|move)\s+(?:канал\s+)?(.+?)\s+(?:в|на|до)\s+(?:категор\p{L}*\s+)?(.+)$/u);
  if (moveChannelCategoryMatch?.[1]?.trim() && moveChannelCategoryMatch?.[2]?.trim() && /категор|category/u.test(normalized)) {
    return {
      action: 'move_channel_to_category',
      channel: moveChannelCategoryMatch[1].trim(),
      text: moveChannelCategoryMatch[2].trim(),
    };
  }
  const createThreadMatch = normalized.match(/^(?:создай|создать|открой|create)\s+(?:тред|thread|ветк\p{L}*)(?:\s+(.+))?$/u);
  if (createThreadMatch) {
    return { action: 'create_thread', text: cleanCreatedChannelName(createThreadMatch[1], 'Новый тред') };
  }
  const archiveThreadMatch = normalized.match(/^(?:архивируй|закрой|archive)\s+(?:тред|thread|ветк\p{L}*)(?:\s+(.+))?$/u);
  if (archiveThreadMatch) {
    return { action: 'archive_thread', text: archiveThreadMatch[1]?.trim() || '' };
  }
  const lockThreadMatch = normalized.match(/^(?:залочь|заблокируй|lock)\s+(?:тред|thread|ветк\p{L}*)(?:\s+(.+))?$/u);
  if (lockThreadMatch) {
    return { action: 'lock_thread', text: lockThreadMatch[1]?.trim() || '' };
  }
  const unlockThreadMatch = normalized.match(/^(?:разлочь|разблокируй|unlock)\s+(?:тред|thread|ветк\p{L}*)(?:\s+(.+))?$/u);
  if (unlockThreadMatch) {
    return { action: 'unlock_thread', text: unlockThreadMatch[1]?.trim() || '' };
  }
  const renameServerMatch = normalized.match(/^(?:переименуй|назови|rename)\s+(?:сервер|server)\s+(?:в\s+)?(.+)$/u);
  if (renameServerMatch?.[1]?.trim()) {
    return { action: 'rename_server', text: renameServerMatch[1].trim() };
  }
  const roleColorMatch = normalized.match(/^(?:покрась|измени\s+цвет|цвет)\s+(?:роль\s+)?(.+?)\s+(?:в|на)\s+(.+)$/u);
  if (roleColorMatch?.[1]?.trim() && roleColorMatch?.[2]?.trim()) {
    return { action: 'set_role_color', text: roleColorMatch[1].trim(), value: roleColorMatch[2].trim() };
  }
  const roleMentionMatch = normalized.match(/^(?:сделай|set)\s+(?:роль\s+)?(.+?)\s+(?:упоминаемой|mentionable|пингуемой|пингаемой)$/u);
  if (roleMentionMatch?.[1]?.trim()) {
    return { action: 'set_role_mentionable', text: roleMentionMatch[1].trim(), value: true };
  }
  const roleHoistMatch = normalized.match(/^(?:подними|показывай\s+отдельно|выдели|hoist)\s+(?:роль\s+)?(.+)$/u);
  if (roleHoistMatch?.[1]?.trim()) {
    return { action: 'set_role_hoist', text: roleHoistMatch[1].trim(), value: true };
  }
  const createVoiceMatch = normalized.match(/^(?:создай|создать|створи|зроби|create)\s+(?:(?:новый|новий|new)\s+)?(?:голосов\p{L}*\s+канал|войс\s+канал|воис\s+канал|voice\s+channel|войс|воис|voice)(?:\s+(.+))?$/u);
  if (createVoiceMatch) {
    return { action: 'create_voice_channel', text: cleanCreatedChannelName(createVoiceMatch[1], 'Новый voice') };
  }
  const createTextMatch = normalized.match(/^(?:создай|создать|створи|зроби|create)\s+(?:(?:новый|новий|new)\s+)?(?:текстов\p{L}*\s+канал|чат|text\s+channel)(?:\s+(.+))?$/u);
  if (createTextMatch) {
    return { action: 'create_text_channel', text: cleanCreatedChannelName(createTextMatch[1], 'new-chat') };
  }
  const memberAction = parseSimpleMemberAction(prompt);
  if (memberAction) return memberAction;
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
  if (
    ['тут', 'здесь', 'на месте', 'слушаешь', 'слышишь', 'чуешь'].includes(normalized)
    || normalized.includes('ты тут')
    || normalized.includes('ти тут')
    || normalized.includes('ты здесь')
    || normalized.includes('ти здесь')
    || normalized.includes('ты на месте')
    || normalized.includes('ти на месте')
    || normalized.includes('are you there')
  ) {
    return { action: 'presence_check' };
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
  if (!shouldTryAiActionParser(prompt)) return { action: 'none' };

  let completion;
  const messages = [
    {
      role: 'system',
      content:
        'Ты строгий JSON-парсер голосовых команд Discord. Верни только JSON без markdown. '
        + 'Схема: {"action":"...","target":"...","channel":"...","value":0,"text":"..."}. '
        + 'Доступные action: disconnect_member, disconnect_all, kick_member, ban_member, move_member, move_member_back, move_all_members, mute_member, unmute_member, mute_all, unmute_all, deafen_member, undeafen_member, timeout_member, untimeout_member, add_role, remove_role, create_role, delete_role, set_role_color, set_role_mentionable, set_role_hoist, set_nickname, lock_voice, unlock_voice, rename_voice, set_voice_limit, lock_text, unlock_text, rename_text, set_text_topic, pin_last_message, set_slowmode, clear_messages, send_message, create_text_channel, create_voice_channel, create_category, move_channel_to_category, create_thread, archive_thread, lock_thread, unlock_thread, delete_channel, create_invite, list_invites, delete_invite, list_members, list_roles, list_channels, play_soundboard_sound, list_soundboard_sounds, rename_soundboard_sound, delete_soundboard_sound, rename_server, telegram_send_message, telegram_send_note, telegram_search_and_send, telegram_send_last_answer, telegram_send_memory, telegram_send_reminders, telegram_list_chats, telegram_status, telegram_test, telegram_clear, remember_memory, remember_user_memory, generate_memory_notes, search_memory, delete_memory, show_status, show_limits, reset_memory, pause_listening, resume_listening, stop_speaking, delete_reminder, none. '
        + 'target это имя участника ровно как услышано, даже если ник смешанный русский/English/цифры или склонен: "досика" -> target "досика", "Dosikk" -> target "Dosikk". channel это имя канала назначения или канала для действия. value это число: секунды для timeout/slowmode, лимит voice или количество сообщений. text это имя роли, новый ник, новое имя канала или текст сообщения. '
        + 'Если говорят "отключи/выкинь из войса" это disconnect_member, а "отключи всех" это disconnect_all. Если говорят "кикни/исключи/кікні/виключи с сервера" это kick_member. '
        + 'Если говорят "отключи микрофон/выключи микрофон/вимкни мікрофон/замуть" это mute_member, а не disconnect_member. "размуть/верни микрофон" это unmute_member. '
        + 'Понимай разговорные и неточные варианты для всех команд: "выруби микрофон", "приглуши", "закинь/перекинь/перетащи в канал", "выкинь из войса", "почисти чат", "сделай комнату", "дай модерку", "сними роль", "поставь медленный режим", "поставь ограничение войса", "закрой комнату", "открой чат". '
        + 'Если говорят "замуть всех" это mute_all, а "таймаут на N" это timeout_member. Если говорят "перемести всех в канал" это move_all_members. "верни его/досика обратно" это move_member_back. '
        + '"проиграй/включи звук X", "саундборд X", "звук на звуковой панели X" это play_soundboard_sound и text=X. "покажи звуки" это list_soundboard_sounds. "переименуй/удали звук X" это rename_soundboard_sound/delete_soundboard_sound. '
        + '"отправь/напиши/скинь/кинь/закинь/перекинь/продублируй X в телеграм/телегу/тг/telegram/telega", а также STT-варианты "телега", "тележка", это telegram_send_message и text=X. '
        + '"заметка/запиши заметку/сохрани заметку в телеграм X" это telegram_send_note и text=X. '
        + '"найди/поищи/загугли/пробей/узнай X и отправь/скинь/закинь в телеграм" это telegram_search_and_send и text=X. '
        + '"отправь/скинь/продублируй последний ответ/это/то что сказал в телеграм" это telegram_send_last_answer. "отправь память/напоминания в телеграм" это telegram_send_memory/telegram_send_reminders. "покажи телеграм чаты/айди/статус" это telegram_list_chats/telegram_status. '
        + '"создай инвайт" это create_invite. "покажи инвайты" это list_invites. "удали инвайт CODE" это delete_invite. "создай категорию X" это create_category. "перемести канал X в категорию Y" это move_channel_to_category. '
        + '"создай тред X" это create_thread. "архивируй/залочь/разлочь тред X" это archive_thread/lock_thread/unlock_thread. "покажи участников/роли/каналы" это list_members/list_roles/list_channels. '
        + '"переименуй сервер X" это rename_server. "покрась роль X в #ff0000" это set_role_color, role name в text, color в value или text. '
        + '"запомни/запиши заметку/сохрани X" это remember_memory и text=X. "придумай/сгенерируй N заметок и запиши/сохрани их" это generate_memory_notes, value=N, text=тема если названа. "запомни обо мне X" это remember_user_memory и text=X. "что ты помнишь про X/найди в памяти X/что я просил вчера" это search_memory и text=X. "удали заметку/память про X" это delete_memory и text=X. '
        + '"стоп/замолчи/хватит/остановись/харош" это stop_speaking. "удали напоминание про X" это delete_reminder и text=X. "сбрось диалог/новый диалог" это reset_memory. "покажи статус" это show_status. "покажи лимиты" это show_limits. '
        + 'Если команда не является действием Discord, action=none.',
    },
    { role: 'user', content: prompt },
  ];
  const modelsToTry = actionModelsToTry();
  let lastError = null;
  for (const model of modelsToTry) {
    try {
      const result = await getGroqClient().chat.completions.create({
        model,
        temperature: 0,
        max_completion_tokens: 220,
        messages,
      }).withResponse();
      completion = result.data;
      trackGroqRateLimits(channel, 'action-parser', result.response, model);
      break;
    } catch (error) {
      lastError = error;
      trackGroqRateLimits(channel, 'action-parser', error, model);
      if (isGroqRateLimitError(error)) markGroqModelOnCooldown(model, 'action-parser', groqResetHeaderFromError(error, 'tokens'));
      if (GROQ_AUTO_MODEL_FALLBACK && shouldFallbackGroqModel(error) && model !== modelsToTry.at(-1)) {
        console.warn(`action parser model ${model} failed, trying fallback:`, error.message || error);
        continue;
      }
      throw error;
    }
  }
  if (!completion) throw lastError || new Error('No action parser completion');

  const raw = completion.choices[0]?.message?.content || '{}';
  const json = extractJsonObject(raw) || raw;
  try {
    const parsed = JSON.parse(json);
    if (!parsed || typeof parsed !== 'object') return { action: 'none' };
    return {
      action: String(parsed.action || 'none'),
      target: parsed.target ? String(parsed.target) : '',
      channel: parsed.channel ? String(parsed.channel) : '',
      value: Number.isFinite(Number(parsed.value)) && String(parsed.value ?? '').trim() !== ''
        ? Number(parsed.value)
        : (parsed.value === undefined || parsed.value === null ? 0 : String(parsed.value)),
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

function formatShortList(items, limit = 20) {
  const list = items
    .map((item) => String(item || '').replace(/\s+/g, ' ').trim())
    .filter(Boolean);
  const shown = list.slice(0, limit);
  const tail = list.length > limit ? `\n...и еще ${list.length - limit}` : '';
  return shown.length ? `${shown.join('\n')}${tail}` : 'пусто';
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
  if (!isPresenceAnnouncementsEnabled() || !text || !isSessionVoiceReady(session)) return;
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

function setPendingMemoryDeletion(session, pending) {
  session.pendingAction = {
    type: 'delete_memories',
    createdAt: Date.now(),
    ...pending,
  };
}

function activePendingMemoryDeletion(session) {
  const pending = session.pendingAction;
  if (!pending || pending.type !== 'delete_memories') return null;
  if (Date.now() - pending.createdAt > 120_000) {
    clearPendingAction(session);
    return null;
  }
  return pending;
}

function deleteMemoryKeys(session, keys) {
  const removed = removeMemoryItemsByKeys(session.guild.id, keys);
  clearPendingAction(session);
  if (!removed.length) return 'Эти записи памяти уже не найдены.';
  const list = removed.map((entry, index) => `${index + 1}. ${entry.memory.text}`).join('\n');
  appendEvent('memory_deleted', {
    guildId: session.guild.id,
    count: removed.length,
    texts: removed.map((entry) => entry.memory.text).slice(0, 10),
  });
  return removed.length === 1
    ? `Удалил запись памяти: ${removed[0].memory.text}`
    : `Удалил записей памяти: ${removed.length}.\n${list}`;
}

function askMemorySelection(session, matches, query, { allowDeleteAll = true } = {}) {
  const shown = matches.slice(0, 6);
  const list = shown.map((item, index) => formatMemoryChoice(item, index)).join('\n');
  setPendingMemoryDeletion(session, {
    mode: allowDeleteAll ? 'confirm_or_select' : 'select',
    keys: shown.map((item) => item.key || memoryEntryKey(item)),
    query,
  });
  const suffix = allowDeleteAll
    ? 'Скажи “бот да”, чтобы удалить все эти, “бот номер 2”, чтобы удалить одну, или “бот нет”.'
    : 'Скажи номер, часть текста или “бот нет”.';
  return `Нашел несколько подходящих записей памяти:\n${list}\n${suffix}`;
}

function handlePendingMemoryDeletion(session, actorMember, prompt) {
  const pending = activePendingMemoryDeletion(session);
  if (!pending) return null;

  if (isNegativeConfirmation(prompt)) {
    clearPendingAction(session);
    return { text: 'Ок, память не трогаю.', speak: false };
  }

  const entries = allMemoryEntries(session.guild.id, actorMember?.id);
  const activeByKey = new Map(entries.map((entry) => [entry.key || memoryEntryKey(entry), entry]));
  const candidates = pending.keys.map((key) => activeByKey.get(key)).filter(Boolean);
  if (!candidates.length) {
    clearPendingAction(session);
    return 'Эти записи памяти уже не найдены.';
  }

  const selectedNumber = parseSelectionNumber(prompt);
  if (selectedNumber && selectedNumber >= 1 && selectedNumber <= candidates.length) {
    return deleteMemoryKeys(session, [candidates[selectedNumber - 1].key]);
  }

  if (isPositiveConfirmation(prompt)) {
    if (pending.mode === 'select' && candidates.length > 1) {
      return { text: 'Скажи номер записи или часть текста. “Да” тут слишком широко.', speak: false };
    }
    return deleteMemoryKeys(session, candidates.map((entry) => entry.key));
  }

  const matches = findMemoryMatches(session.guild.id, actorMember?.id, prompt)
    .filter((item) => candidates.some((entry) => entry.key === item.key));
  if (matches.length === 1) return deleteMemoryKeys(session, [matches[0].key]);
  if (matches.length > 1) return askMemorySelection(session, matches, prompt, { allowDeleteAll: pending.mode !== 'select' });

  return null;
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

function handleSearchMemoryCommand(session, actorMember, parsed) {
  const query = String(parsed.text || '').trim();
  const matches = findMemoryMatches(session.guild.id, actorMember?.id, query);
  if (!matches.length) {
    return `Не нашел в памяти ничего по запросу “${query || 'пустой запрос'}”.`;
  }
  const title = query ? `Память по запросу “${query}”:` : 'Память:';
  void sendText(session.textChannel, `${title}\n${formatMemorySearchResults(matches)}`);
  return {
    text: matches.length === 1
      ? `Нашел одну запись в памяти: ${matches[0].memory.text}`
      : `Нашел записей в памяти: ${Math.min(matches.length, 8)}. Отправил список в чат.`,
    speak: matches.length === 1,
  };
}

function handleDeleteMemoryCommand(session, actorMember, parsed) {
  const entries = allMemoryEntries(session.guild.id, actorMember?.id);
  if (!entries.length) return 'Память пока пустая.';

  const query = String(parsed.text || '').trim();
  const selectedNumber = parseSelectionNumber(query);
  const ordered = entries.slice().sort((a, b) => (a.memory.createdAt || 0) - (b.memory.createdAt || 0));
  if (selectedNumber && selectedNumber >= 1 && selectedNumber <= ordered.length) {
    return deleteMemoryKeys(session, [ordered[selectedNumber - 1].key]);
  }

  if (!query) {
    if (entries.length === 1) {
      setPendingMemoryDeletion(session, { mode: 'confirm', keys: [entries[0].key], query: '' });
      return `Удалить эту запись памяти?\n${formatMemoryChoice(entries[0], 0)}\nСкажи “бот да” или “бот нет”.`;
    }
    return askMemorySelection(session, entries.map((entry, index) => ({ ...entry, score: 0.1, matchIndex: index })), '', {
      allowDeleteAll: false,
    });
  }

  const matches = findMemoryMatches(session.guild.id, actorMember?.id, query);
  if (!matches.length) {
    return `Не нашел запись памяти по запросу “${query}”. Скажи “бот что ты помнишь”, если нужно увидеть список.`;
  }

  const [best, second] = matches;
  const confident = best.score >= 0.65 || !second || best.score - second.score >= 0.28;
  if (confident) return deleteMemoryKeys(session, [best.key]);
  return askMemorySelection(session, matches, query, { allowDeleteAll: true });
}

const DANGEROUS_ACTIONS = new Set([
  'disconnect_member',
  'disconnect_all',
  'kick_member',
  'ban_member',
  'move_member',
  'move_member_back',
  'move_all_members',
  'mute_member',
  'mute_all',
  'deafen_member',
  'timeout_member',
  'remove_role',
  'set_role_color',
  'set_role_mentionable',
  'set_role_hoist',
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
  'create_category',
  'move_channel_to_category',
  'create_thread',
  'archive_thread',
  'lock_thread',
  'unlock_thread',
  'create_invite',
  'delete_invite',
  'rename_soundboard_sound',
  'delete_soundboard_sound',
  'rename_server',
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

const ACTION_SEGMENT_START_PATTERN = [
  'отключ', 'відключ', 'выкин', 'викинь', 'дискон',
  'замут', 'замуть', 'зам ють', 'размут', 'размуть', 'розмут', 'розмуть',
  'перемест', 'перемісти', 'перенеси', 'перекин', 'верни',
  'кик', 'кік', 'забан', 'бан',
  'создай', 'создать', 'створи', 'зроби', 'удали', 'убери',
  'дай', 'забери', 'сними', 'поставь', 'включи', 'выключи', 'проиграй',
  'напиши', 'отправь', 'покажи', 'список', 'закрой', 'открой',
  'переименуй', 'назови', 'очисти', 'закрепи', 'залочь', 'разлочь',
  'запомни', 'напомни', 'пауза', 'продолжай', 'стоп', 'хватит',
  'create', 'delete', 'remove', 'move', 'mute', 'unmute', 'kick', 'ban',
  'play', 'send', 'show', 'list', 'lock', 'unlock', 'rename',
].join('|');

function splitActionSegments(prompt) {
  const text = String(prompt || '').trim();
  if (!text) return [];
  const normalized = normalizeCommandText(text);
  if (!/(^|\s)(и|потом|затем|далее|then|and)(\s|$)/u.test(normalized)) return [];

  const splitter = new RegExp(
    `\\s+(?:и\\s+потом|а\\s+потом|а\\s+затем|потом|затем|после\\s+этого|далее|and\\s+then|then)\\s+`
      + `|\\s+(?:и|and)\\s+(?=(?:${ACTION_SEGMENT_START_PATTERN}))`,
    'giu',
  );
  const parts = text
    .split(splitter)
    .map((part) => part.trim())
    .filter((part) => part.length >= 2)
    .slice(0, 5);
  if (parts.length < 2) return [];
  return parts;
}

async function tryHandleMultiAction(session, actorMember, prompt) {
  if (parseGenerateMemoryNotesCommand(prompt)) return null;
  const segments = splitActionSegments(prompt);
  if (segments.length < 2) return null;

  const parsedSegments = [];
  for (const segment of segments) {
    if (!shouldTryAiActionParser(segment)) return null;
    const parsed = await parseAction(segment, session.textChannel);
    if (!parsed || parsed.action === 'none') return null;
    if (isDangerousAction(parsed)) return null;
    parsedSegments.push({ segment, parsed });
  }
  if (parsedSegments.length < 2) return null;

  const replies = [];
  for (const { segment, parsed } of parsedSegments) {
    const result = await executeParsedAction(session, actorMember, parsed);
    const text = typeof result === 'string' ? result : result?.text;
    replies.push(text || `Команда “${segment}” распознана как ${parsed.action}, но результата нет.`);
  }

  return {
    text: `Выполнил команды по порядку: ${replies.map((reply, index) => `${index + 1}) ${reply}`).join(' ')}`,
    speak: replies.length <= 3,
  };
}

async function tryHandleVoiceAction(session, actorMember, prompt) {
  const pendingResult = handlePendingReminderDeletion(session, prompt);
  if (pendingResult) return pendingResult;

  const pendingMemoryResult = handlePendingMemoryDeletion(session, actorMember, prompt);
  if (pendingMemoryResult) return pendingMemoryResult;

  const pendingDangerousAction = activePendingDangerousAction(session);
  if (pendingDangerousAction) {
    if (!shouldConfirmDangerousActions()) {
      clearPendingAction(session);
    } else {
      const pendingDangerous = await handlePendingDangerousAction(session, actorMember, prompt);
      return pendingDangerous || {
        text: `Жду подтверждение опасного действия: ${describeParsedAction(pendingDangerousAction.parsed)}. Скажи “${getWakeWord() || 'бот'} да” или “${getWakeWord() || 'бот'} нет”.`,
        speak: false,
      };
    }
  }

  const multiActionResult = await tryHandleMultiAction(session, actorMember, prompt);
  if (multiActionResult) return multiActionResult;

  const parsed = await parseAction(prompt, session.textChannel);
  if (!parsed || parsed.action === 'none') {
    if (shouldTryAiActionParser(prompt)) {
      return {
        text: 'Похоже на команду Discord, но я не понял точное действие или цель. Ничего не сделал.',
      };
    }
    return null;
  }
  if (!['delete_reminder', 'delete_memory'].includes(parsed.action) && session.pendingAction) clearPendingAction(session);

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

  const result = await executeParsedAction(session, actorMember, parsed);
  if (!result) {
    return {
      text: `Команда распознана как ${parsed.action}, но для нее нет рабочего обработчика. Ничего не сделал.`,
    };
  }
  return result;
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
  const fetchMemberById = async (memberId) => {
    if (!memberId) return null;
    return session.guild.members.cache.get(memberId)
      || await session.guild.members.fetch(memberId).catch(() => null);
  };
  const roleText = () => (parsed.text || parsed.target || parsed.channel || '').trim();
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
      case 'generate_memory_notes': {
        const count = Math.max(1, Math.min(10, Number(parsed.value) || 5));
        const notes = await generateMemoryNotes(session, actorMember, parsed.originalPrompt || parsed.prompt || parsed.text || '', count, parsed.text || '');
        const saved = notes.map((note) => addMemoryItem(session.guild.id, actorMember, note));
        appendEvent('memory_notes_generated', {
          guildId: session.guild.id,
          userId: actorMember?.id,
          count: saved.length,
          topic: parsed.text || '',
          toTelegram: Boolean(parsed.toTelegram),
          notes: saved.map((item) => item.text),
        });
        const list = saved.map((item, index) => `${index + 1}. ${item.text}`).join('\n');
        await sendText(session.textChannel, `Сохранил заметки:\n${list}`);
        if (parsed.toTelegram) {
          await sendTelegramMessage(`Сохраненные заметки:\n${list}`);
          return `Придумал, сохранил и отправил в Telegram ${saved.length} ${pluralRu(saved.length, 'заметку', 'заметки', 'заметок')}.`;
        }
        return `Придумал и сохранил ${saved.length} ${pluralRu(saved.length, 'заметку', 'заметки', 'заметок')}.`;
      }
      case 'show_memory': {
        await sendText(session.textChannel, `Память:\n${formatMemoryList(session.guild.id, actorMember?.id)}`);
        return { text: 'Отправил память в чат.', speak: false };
      }
      case 'show_user_memory': {
        await sendText(session.textChannel, `Память о тебе:\n${formatMemoryList(session.guild.id, actorMember?.id)}`);
        return { text: 'Отправил твою память в чат.', speak: false };
      }
      case 'search_memory': {
        return handleSearchMemoryCommand(session, actorMember, parsed);
      }
      case 'delete_memory': {
        return handleDeleteMemoryCommand(session, actorMember, parsed);
      }
      case 'clear_memory': {
        const count = clearMemoryItems(session.guild.id);
        clearPendingAction(session);
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
        const fromChannel = target.voice.channel;
        await target.voice.setChannel(destination, reason);
        session.lastMemberMove = {
          memberId: target.id,
          memberName: target.displayName,
          fromChannelId: fromChannel.id,
          fromChannelName: fromChannel.name,
          toChannelId: destination.id,
          toChannelName: destination.name,
          actorId: actorMember?.id || null,
          at: Date.now(),
        };
        return `Переместил ${target.displayName} в ${destination.name}.`;
      }
      case 'move_member_back': {
        const denied = requirePermission(PermissionFlagsBits.MoveMembers, 'Move Members');
        if (denied) return denied;
        const lastMove = session.lastMemberMove;
        if (!lastMove || Date.now() - lastMove.at > 30 * 60_000) {
          return 'Не помню последнее перемещение. Скажи точнее: кого и в какой канал вернуть.';
        }
        const target = parsed.target
          ? await getTarget()
          : await fetchMemberById(lastMove.memberId);
        if (!target || target.error) return target?.error || 'Не нашел участника, которого нужно вернуть.';
        if (!target.voice?.channel) return `${target.displayName} сейчас не в голосовом канале.`;
        const destination = await session.guild.channels.fetch(lastMove.fromChannelId).catch(() => null);
        if (!destination || ![ChannelType.GuildVoice, ChannelType.GuildStageVoice].includes(destination.type)) {
          return `Не нашел прошлый голосовой канал “${lastMove.fromChannelName || lastMove.fromChannelId}”.`;
        }
        const fromChannel = target.voice.channel;
        await target.voice.setChannel(destination, reason);
        session.lastMemberMove = {
          memberId: target.id,
          memberName: target.displayName,
          fromChannelId: fromChannel.id,
          fromChannelName: fromChannel.name,
          toChannelId: destination.id,
          toChannelName: destination.name,
          actorId: actorMember?.id || null,
          at: Date.now(),
        };
        return `Вернул ${target.displayName} в ${destination.name}.`;
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
        if (!target.voice?.channel) return `${target.displayName} сейчас не в голосовом канале.`;
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
      case 'set_role_color': {
        const denied = requirePermission(PermissionFlagsBits.ManageRoles, 'Manage Roles');
        if (denied) return denied;
        const roleResult = await findRole(session, parsed.target || parsed.text || parsed.channel);
        if (roleResult.error) return roleResult.error;
        const colorText = String(parsed.value || parsed.channel || '').trim();
        const color = parseColorValue(colorText);
        if (!color) return 'Не понял цвет роли. Скажи цвет словом или hex, например #ff0000.';
        await roleResult.role.setColor(color, reason);
        return `Покрасил роль ${roleResult.role.name} в ${color}.`;
      }
      case 'set_role_mentionable':
      case 'set_role_hoist': {
        const denied = requirePermission(PermissionFlagsBits.ManageRoles, 'Manage Roles');
        if (denied) return denied;
        const roleResult = await findRole(session, parsed.target || parsed.text || parsed.channel);
        if (roleResult.error) return roleResult.error;
        const enabled = parseBooleanIntent(String(parsed.value || parsed.channel || ''), true);
        if (parsed.action === 'set_role_mentionable') {
          await roleResult.role.setMentionable(enabled, reason);
          return enabled ? `Роль ${roleResult.role.name} теперь можно упоминать.` : `Роль ${roleResult.role.name} больше нельзя упоминать.`;
        }
        await roleResult.role.setHoist(enabled, reason);
        return enabled ? `Роль ${roleResult.role.name} теперь показывается отдельно.` : `Роль ${roleResult.role.name} больше не показывается отдельно.`;
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
      case 'create_category': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        const name = normalizeCategoryName(parsed.text || parsed.channel);
        const created = await session.guild.channels.create({ name, type: ChannelType.GuildCategory, reason });
        return `Создал категорию ${created.name}.`;
      }
      case 'move_channel_to_category': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        const targetChannel = await findAnyChannel(session, parsed.channel);
        if (!targetChannel) return `Не нашел канал “${parsed.channel}”.`;
        if (!targetChannel.setParent) return 'Этот канал нельзя переместить в категорию.';
        const category = await findCategoryChannel(session, parsed.text || parsed.target);
        if (!category) return `Не нашел категорию “${parsed.text || parsed.target}”.`;
        await targetChannel.setParent(category, { lockPermissions: false, reason });
        return `Переместил канал ${targetChannel.name} в категорию ${category.name}.`;
      }
      case 'create_thread': {
        const denied = requirePermission(PermissionFlagsBits.CreatePublicThreads, 'Create Public Threads');
        if (denied) return denied;
        const baseChannel = [ChannelType.PublicThread, ChannelType.PrivateThread, ChannelType.AnnouncementThread].includes(session.textChannel?.type)
          ? session.textChannel.parent
          : session.textChannel;
        if (!baseChannel?.threads?.create) return 'В этом текстовом канале нельзя создать тред.';
        const name = String(parsed.text || parsed.channel || 'Новый тред').replace(/\s+/g, ' ').trim().slice(0, 100);
        const thread = await baseChannel.threads.create({ name, autoArchiveDuration: 1440, reason });
        return `Создал тред ${thread.name}.`;
      }
      case 'archive_thread':
      case 'lock_thread':
      case 'unlock_thread': {
        const denied = requirePermission(PermissionFlagsBits.ManageThreads, 'Manage Threads');
        if (denied) return denied;
        const thread = await findThreadChannel(session, parsed.text || parsed.channel);
        if (!thread) return `Не нашел тред “${parsed.text || parsed.channel || 'текущий'}”.`;
        if (parsed.action === 'archive_thread') {
          await thread.setArchived(true, reason);
          return `Архивировал тред ${thread.name}.`;
        }
        await thread.setLocked(parsed.action === 'lock_thread', reason);
        return parsed.action === 'lock_thread'
          ? `Залочил тред ${thread.name}.`
          : `Разлочил тред ${thread.name}.`;
      }
      case 'create_invite': {
        const denied = requirePermission(PermissionFlagsBits.CreateInstantInvite, 'Create Instant Invite');
        if (denied) return denied;
        const targetChannel = channelText()
          ? await findAnyChannel(session, channelText())
          : (session.voiceChannel || session.textChannel);
        if (!targetChannel || ![ChannelType.GuildText, ChannelType.GuildAnnouncement, ChannelType.GuildVoice, ChannelType.GuildStageVoice, ChannelType.GuildForum].includes(targetChannel.type)) {
          return `Не могу создать invite для “${channelText() || 'текущего канала'}”.`;
        }
        const invite = await session.guild.invites.create(targetChannel, {
          maxAge: 0,
          maxUses: 0,
          unique: true,
          reason,
        });
        await sendText(session.textChannel, `Invite: ${invite.url}`);
        return { text: 'Создал invite и отправил ссылку в чат.', speak: false };
      }
      case 'list_invites': {
        const denied = requirePermission(PermissionFlagsBits.ManageGuild, 'Manage Server');
        if (denied) return denied;
        const invites = await session.guild.invites.fetch();
        const lines = [...invites.values()]
          .slice(0, 25)
          .map((invite) => `${invite.code} -> #${invite.channel?.name || invite.channelId || 'unknown'} · uses=${invite.uses ?? 0}`);
        await sendText(session.textChannel, `Invites:\n${formatShortList(lines, 25)}`);
        return { text: 'Отправил invite-ссылки в чат.', speak: false };
      }
      case 'delete_invite': {
        const denied = requirePermission(PermissionFlagsBits.ManageGuild, 'Manage Server');
        if (denied) return denied;
        const code = cleanInviteCode(parsed.text || parsed.channel);
        if (!code) return 'Какой invite удалить? Скажи код или ссылку.';
        await session.guild.invites.delete(code, reason);
        return `Удалил invite ${code}.`;
      }
      case 'list_members': {
        const voiceNames = getHumanVoiceMembers(session)
          .map((member) => member.displayName || member.user?.username)
          .filter(Boolean);
        const cachedMembers = [...session.guild.members.cache.values()]
          .filter((member) => !member.user.bot)
          .map((member) => member.displayName)
          .sort((a, b) => a.localeCompare(b, 'ru'))
          .slice(0, 60);
        await sendText(session.textChannel, [
          `Участники в voice:\n${formatShortList(voiceNames, 30)}`,
          `\nУчастники в кеше сервера:\n${formatShortList(cachedMembers, 60)}`,
        ].join('\n'));
        return { text: 'Отправил список участников в чат.', speak: false };
      }
      case 'list_roles': {
        await session.guild.roles.fetch().catch(() => null);
        const roles = [...session.guild.roles.cache.values()]
          .filter((role) => role.id !== session.guild.id)
          .sort((a, b) => b.position - a.position)
          .map((role) => `${role.name} · ${role.members?.size ?? 0} users`);
        await sendText(session.textChannel, `Роли:\n${formatShortList(roles, 60)}`);
        return { text: 'Отправил список ролей в чат.', speak: false };
      }
      case 'list_channels': {
        const channels = [...(await session.guild.channels.fetch()).values()]
          .filter(Boolean)
          .sort((a, b) => (a.rawPosition ?? 0) - (b.rawPosition ?? 0))
          .map((channel) => `${channel.name} · ${ChannelType[channel.type] || channel.type}`);
        await sendText(session.textChannel, `Каналы:\n${formatShortList(channels, 80)}`);
        return { text: 'Отправил список каналов в чат.', speak: false };
      }
      case 'list_soundboard_sounds': {
        const sounds = await fetchSoundboardSounds(session);
        const lines = sounds.map((sound) => `${sound.name || sound.soundId}${sound.guildId ? ' · server' : ' · default'}`);
        await sendText(session.textChannel, `Soundboard:\n${formatShortList(lines, 80)}`);
        return { text: 'Отправил список звуков в чат.', speak: false };
      }
      case 'play_soundboard_sound': {
        const denied = requirePermission(PermissionFlagsBits.UseSoundboard, 'Use Soundboard');
        if (denied) return denied;
        if (!session.voiceChannel?.id) return 'Я не подключен к голосовому каналу.';
        const result = await findSoundboardSound(session, parsed.text || parsed.channel);
        if (result.error) return result.error;
        await client.rest.post(`/channels/${session.voiceChannel.id}/send-soundboard-sound`, {
          body: {
            sound_id: result.sound.soundId,
            source_guild_id: result.sound.guildId || undefined,
          },
        });
        return `Включил звук ${result.sound.name || result.sound.soundId}.`;
      }
      case 'rename_soundboard_sound': {
        const denied = requirePermission(PermissionFlagsBits.ManageGuildExpressions, 'Manage Expressions');
        if (denied) return denied;
        const result = await findSoundboardSound(session, parsed.text || parsed.target);
        if (result.error) return result.error;
        if (result.sound.guildId !== session.guild.id) return 'Этот звук стандартный или с другого сервера, его нельзя переименовать здесь.';
        const newName = String(parsed.value || parsed.channel || '').replace(/\s+/g, ' ').trim().slice(0, 32);
        if (!newName) return 'Как назвать звук?';
        const updated = await session.guild.soundboardSounds.edit(result.sound, { name: newName, reason });
        return `Переименовал звук в ${updated.name}.`;
      }
      case 'delete_soundboard_sound': {
        const denied = requirePermission(PermissionFlagsBits.ManageGuildExpressions, 'Manage Expressions');
        if (denied) return denied;
        const result = await findSoundboardSound(session, parsed.text || parsed.channel);
        if (result.error) return result.error;
        if (result.sound.guildId !== session.guild.id) return 'Этот звук стандартный или с другого сервера, его нельзя удалить здесь.';
        const name = result.sound.name || result.sound.soundId;
        await session.guild.soundboardSounds.delete(result.sound, reason);
        return `Удалил soundboard-звук ${name}.`;
      }
      case 'rename_server': {
        const denied = requirePermission(PermissionFlagsBits.ManageGuild, 'Manage Server');
        if (denied) return denied;
        const name = String(parsed.text || parsed.channel || '').replace(/\s+/g, ' ').trim().slice(0, 100);
        if (!name) return 'Как назвать сервер?';
        await session.guild.setName(name, reason);
        return `Переименовал сервер в ${name}.`;
      }
      case 'telegram_send_message': {
        const text = String(parsed.text || parsed.channel || '').trim();
        if (!text) return 'Что отправить в Telegram?';
        await sendTelegramMessage(text);
        return 'Отправил сообщение в Telegram.';
      }
      case 'telegram_send_note': {
        const text = String(parsed.text || parsed.channel || '').trim();
        if (!text) return 'Какую заметку отправить в Telegram?';
        await sendTelegramMessage(formatTelegramNote(actorMember, text));
        return 'Отправил заметку в Telegram.';
      }
      case 'telegram_search_and_send': {
        const query = String(parsed.text || parsed.channel || '').trim();
        if (!query) return 'Что найти и отправить в Telegram?';
        const summary = await generateTelegramWebSearchSummary(session, actorMember, query);
        await sendTelegramMessage(summary);
        return 'Нашел информацию и отправил в Telegram.';
      }
      case 'telegram_send_last_answer': {
        const text = getLastAssistantReply(session);
        if (!text) return 'Пока нет последнего ответа, который можно отправить в Telegram.';
        await sendTelegramMessage(text);
        return 'Отправил последний ответ в Telegram.';
      }
      case 'telegram_send_memory': {
        await sendTelegramMessage(`Память Discord:\n${formatMemoryList(session.guild.id, actorMember?.id)}`);
        return 'Отправил память в Telegram.';
      }
      case 'telegram_send_reminders': {
        await sendTelegramMessage(`Напоминания Discord:\n${formatReminderList(session.guild.id)}`);
        return 'Отправил напоминания в Telegram.';
      }
      case 'telegram_list_chats': {
        const chats = await getRecentTelegramChats();
        const lines = chats.map(formatTelegramChat);
        const text = `Telegram chats:\n${formatShortList(lines, 30)}\nЕсли списка нет, напиши боту в Telegram /start или добавь его в группу и отправь туда сообщение.`;
        if (parsed.toTelegram) {
          await sendTelegramMessage(text);
          return 'Отправил список Telegram-чатов в Telegram.';
        }
        await sendText(session.textChannel, text);
        return { text: 'Отправил список Telegram-чатов в Discord.', speak: false };
      }
      case 'telegram_status': {
        const text = `Telegram status:\n${formatTelegramStatus()}`;
        if (parsed.toTelegram) {
          await sendTelegramMessage(text);
          return 'Отправил статус Telegram в Telegram.';
        }
        await sendText(session.textChannel, text);
        return { text: 'Отправил статус Telegram в Discord.', speak: false };
      }
      case 'telegram_test': {
        await sendTelegramMessage(`Тест из Discord от ${actorMember?.displayName || actorMember?.user?.username || 'пользователя'}.`);
        return 'Тестовое сообщение ушло в Telegram.';
      }
      case 'telegram_clear': {
        const denied = requirePermission(PermissionFlagsBits.ManageGuild, 'Manage Server');
        if (denied) return denied;
        updateRuntimeConfig({ telegramBotToken: '', telegramDefaultChatId: '' });
        return TELEGRAM_BOT_TOKEN || TELEGRAM_DEFAULT_CHAT_ID
          ? 'Очистил Telegram-настройки runtime-config. Но в .env есть Telegram-настройки, они останутся активны до изменения .env.'
          : 'Очистил Telegram-настройки.';
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
      case 'presence_check':
        return `Да, я тут. Для следующей команды снова начни с “${getWakeWord() || 'бот'}”.`;
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

function charLength(text) {
  return Array.from(String(text || '')).length;
}

function sttPromptFits(text) {
  const value = String(text || '');
  return charLength(value) <= STT_PROMPT_MAX_CHARS && Buffer.byteLength(value, 'utf8') <= STT_PROMPT_MAX_BYTES;
}

function truncateSttPrompt(text, maxChars = STT_PROMPT_MAX_CHARS, maxBytes = STT_PROMPT_MAX_BYTES) {
  const normalized = String(text || '').replace(/\s+/gu, ' ').trim();
  let value = '';
  for (const char of Array.from(normalized)) {
    const candidate = `${value}${char}`;
    if (charLength(candidate) > maxChars || Buffer.byteLength(candidate, 'utf8') > maxBytes) break;
    value = candidate;
  }
  return value.trimEnd();
}

function buildSttPrompt(session) {
  const wakeTerms = [
    getAssistantName(),
    getWakeWord(),
    ...getWakeAliases(),
  ]
    .filter(Boolean)
    .map((term) => String(term).replace(/\s+/gu, ' ').trim())
    .filter((term) => term && term.length <= 32);
  const names = session
    ? [...new Set(
      getCurrentVoiceMembers(session)
        .flatMap((member) => candidateMemberNames(member))
        .filter((name) => name && name.length <= 32),
    )]
    : [];
  const base = truncateSttPrompt(STT_PROMPT_BASE, Math.min(240, STT_PROMPT_MAX_CHARS), Math.min(460, STT_PROMPT_MAX_BYTES));
  const uniqueWakeTerms = [...new Set(wakeTerms)].slice(0, 16);
  const languageHint = truncateSttPrompt(
    `${STT_LANGUAGE_HINT} Разрешённые языки: ${STT_ALLOWED_LANGUAGES}.`,
    140,
    260,
  );
  let prompt = `${languageHint} ${base} Текущее имя ассистента: ${getAssistantName()}. Триггерные слова: ${uniqueWakeTerms.join(', ')}.`;
  prompt = truncateSttPrompt(prompt);
  if (!names.length || !sttPromptFits(`${prompt} Имена и ники в войсе: A.`)) return prompt;

  const prefix = `${prompt} Имена и ники в войсе: `;
  const selectedNames = [];
  for (const name of names) {
    const candidateNames = [...selectedNames, name].join(', ');
    const candidate = `${prefix}${candidateNames}.`;
    if (!sttPromptFits(candidate)) break;
    selectedNames.push(name);
  }
  return selectedNames.length ? `${prefix}${selectedNames.join(', ')}.` : prompt;
}

async function transcribePcm(pcm, userId, sessionOrChannel = monitorChannel) {
  const session = sessionOrChannel?.voiceChannel ? sessionOrChannel : null;
  const channel = session?.textChannel || sessionOrChannel || monitorChannel;
  const wav = wavFromPcm(pcm);
  const prompt = buildSttPrompt(session);
  const modelsToTry = sttModelsToTry();
  let lastModelError = null;

  const transcribe = async (model, language, label, usePrompt = true) => {
    const file = await toFile(wav, `${userId}.wav`, { type: 'audio/wav' });
    const result = await getGroqClient().audio.transcriptions.create({
      file,
      model,
      ...(language ? { language } : {}),
      ...(usePrompt && prompt ? { prompt } : {}),
      temperature: 0,
      response_format: 'json',
    }).withResponse();
    trackGroqRateLimits(channel, label, result.response, model);
    return (result.data?.text || '').trim();
  };
  const transcribeWithRetry = async (model, language, label, usePrompt = true) => {
    let lastError = null;
    for (let attempt = 1; attempt <= STT_TRANSIENT_RETRIES; attempt += 1) {
      try {
        return await transcribe(model, language, label, usePrompt);
      } catch (error) {
        lastError = error;
        if (usePrompt && isGroqPromptLengthError(error) && prompt) {
          console.warn(`${label} prompt too long for provider, retrying without prompt`);
          return transcribe(model, language, `${label}-no-prompt`, false);
        }
        if (!isTransientGroqConnectionError(error) || attempt >= STT_TRANSIENT_RETRIES) throw error;
        console.warn(`${label} transient connection error (${error?.cause?.code || error?.code || error?.message}), retrying`);
        await delay(350 * attempt);
      }
    }
    throw lastError;
  };

  for (const [modelIndex, model] of modelsToTry.entries()) {
    try {
      const first = await transcribeWithRetry(model, getSttLanguage(), 'speech-to-text');
      if (first) {
        if (shouldRetrySttForWake(first, session, userId)) {
          const retries = [];
          if (getSttLanguage() !== 'ru') retries.push({ language: 'ru', label: 'speech-to-text-ru-fallback' });
          retries.push({ language: getSttLanguage(), label: 'speech-to-text-no-prompt', usePrompt: false });
          for (const retryConfig of retries) {
            const retry = await transcribeWithRetry(model, retryConfig.language, retryConfig.label, retryConfig.usePrompt !== false)
              .catch((error) => {
                console.warn(`${retryConfig.label} failed after first transcript "${first}" model=${model}:`, error?.message || error);
                return '';
              });
            if (!retry) continue;
            const improved = hasWakeWord(retry)
              || (isWakeListenWindow(session, Date.now(), userId) && !isSttPromptEchoTranscript(retry))
              || (isSttPromptEchoTranscript(first) && !isSttPromptEchoTranscript(retry));
            if (improved) {
              console.log(`stt fallback improved transcript user=${userId}: "${first}" -> "${retry}"`);
              return retry;
            }
          }
        }
        return first;
      }
      if (getSttLanguage()) {
        const retry = await transcribeWithRetry(model, '', 'speech-to-text-retry');
        if (retry) return retry;
      }
    } catch (error) {
      lastModelError = error;
      trackGroqRateLimits(channel, 'speech-to-text', error, model);
      if (isGroqRateLimitError(error)) markGroqModelOnCooldown(model, 'speech-to-text', groqResetHeaderFromError(error, 'requests'));
      if (GROQ_AUTO_MODEL_FALLBACK && shouldFallbackGroqModel(error) && modelIndex < modelsToTry.length - 1) {
        console.warn(`STT model ${model} failed, trying fallback ${modelsToTry[modelIndex + 1]}:`, error.message || error);
        continue;
      }
      throw error;
    }
  }
  if (lastModelError) throw lastModelError;
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
    'крипто', 'акции', 'как сейчас', 'что там с', 'есть ли новости', 'на сегодня',
    'на завтра', 'текущ', 'актуально ли', 'сколько стоит', 'курс валют', 'курс гривны',
    'когда выйдет', 'когда будет', 'кто победил', 'пробки', 'карта',
    'latest', 'current', 'news', 'weather', 'forecast', 'price', 'today', 'tomorrow',
    'yesterday', 'live', 'real time', 'real-time', 'schedule', 'status', 'release',
  ];
  return webPhrases.some((phrase) => normalized.includes(phrase));
}

function isWeatherQuery(prompt) {
  const normalized = normalizeCommandText(prompt);
  return /погод|weather|forecast|температур|temperature/.test(normalized);
}

function isTimeQuery(prompt) {
  const normalized = normalizeCommandText(prompt);
  return /(^|\s)(время|времени|час|часов|time)(\s|$)/u.test(normalized)
    || normalized.includes('который час')
    || normalized.includes('сколько времени')
    || normalized.includes('what time');
}

const MATH_UNITS = new Map(Object.entries({
  ноль: 0, нуль: 0, zero: 0,
  один: 1, одна: 1, одно: 1, одну: 1, раз: 1, one: 1,
  два: 2, две: 2, two: 2,
  три: 3, three: 3,
  четыре: 4, four: 4,
  пять: 5, five: 5,
  шесть: 6, six: 6,
  семь: 7, seven: 7,
  восемь: 8, eight: 8,
  девять: 9, nine: 9,
  десять: 10, ten: 10,
  одиннадцать: 11, eleven: 11,
  двенадцать: 12, twelve: 12,
  тринадцать: 13, thirteen: 13,
  четырнадцать: 14, fourteen: 14,
  пятнадцать: 15, fifteen: 15,
  шестнадцать: 16, sixteen: 16,
  семнадцать: 17, seventeen: 17,
  восемнадцать: 18, eighteen: 18,
  девятнадцать: 19, nineteen: 19,
}));

const MATH_TENS = new Map(Object.entries({
  двадцать: 20, тридцать: 30, сорок: 40, пятьдесят: 50, шестьдесят: 60,
  семьдесят: 70, восемьдесят: 80, девяносто: 90,
  twenty: 20, thirty: 30, forty: 40, fifty: 50, sixty: 60, seventy: 70, eighty: 80, ninety: 90,
}));

const MATH_HUNDREDS = new Map(Object.entries({
  сто: 100, двести: 200, триста: 300, четыреста: 400, пятьсот: 500,
  шестьсот: 600, семьсот: 700, восемьсот: 800, девятьсот: 900,
  hundred: 100,
}));

const MATH_FILLER_WORDS = new Set([
  'сколько', 'будет', 'равно', 'равняется', 'посчитай', 'подсчитай', 'вычисли', 'считай', 'реши',
  'пример', 'математика', 'математически', 'чему', 'это', 'пожалуйста', 'плиз',
  'what', 'is', 'calculate', 'count', 'please', 'equals', 'equal',
]);

function normalizeMathText(prompt) {
  return String(prompt || '')
    .toLowerCase()
    .replaceAll('ё', 'е')
    .replace(/(\d),(\d)/g, '$1.$2')
    .replace(/[×✕]/g, ' * ')
    .replace(/[÷]/g, ' / ')
    .replace(/(?<![\p{L}\p{N}_])(?:умножить|умножь|помножить|помножь|перемножь|множить|multiplied|multiply)\s+(?:на|by)(?![\p{L}\p{N}_])/giu, ' * ')
    .replace(/(?<![\p{L}\p{N}_])(?:умножить|умножь|помножить|помножь|перемножь|множить|times|multiplied|multiply)(?![\p{L}\p{N}_])/giu, ' * ')
    .replace(/(?<![\p{L}\p{N}_])(?:разделить|поделить|подели|делить|деленное|деленое|divided|divide)\s+(?:на|by)(?![\p{L}\p{N}_])/giu, ' / ')
    .replace(/(?<![\p{L}\p{N}_])(?:разделить|поделить|подели|делить|деленное|деленое|divided|divide)(?![\p{L}\p{N}_])/giu, ' / ')
    .replace(/(?<![\p{L}\p{N}_])(?:плюс|plus)(?![\p{L}\p{N}_])/giu, ' + ')
    .replace(/(?<![\p{L}\p{N}_])(?:минус|minus)(?![\p{L}\p{N}_])/giu, ' - ')
    .replace(/(?<![\p{L}\p{N}_])(?:в\s+степени|степени|power|powered)(?![\p{L}\p{N}_])/giu, ' ^ ')
    .replace(/(?<![\p{L}\p{N}_])(?:открыва(?:ется|й)?\s+скобк\p{L}*|открытая\s+скобк\p{L}*|open\s+parenthesis)(?![\p{L}\p{N}_])/giu, ' ( ')
    .replace(/(?<![\p{L}\p{N}_])(?:закрыва(?:ется|й)?\s+скобк\p{L}*|закрытая\s+скобк\p{L}*|close\s+parenthesis)(?![\p{L}\p{N}_])/giu, ' ) ')
    .replace(/(?<=\d)\s*[xх]\s*(?=\d)/giu, ' * ');
}

function readSpokenNumber(tokens, start) {
  let index = start;
  let total = 0;
  let consumed = 0;

  const hundred = MATH_HUNDREDS.get(tokens[index]);
  if (hundred !== undefined) {
    total += hundred;
    index += 1;
    consumed += 1;
  }

  const ten = MATH_TENS.get(tokens[index]);
  if (ten !== undefined) {
    total += ten;
    index += 1;
    consumed += 1;
  }

  const unit = MATH_UNITS.get(tokens[index]);
  if (unit !== undefined) {
    total += unit;
    index += 1;
    consumed += 1;
  }

  return consumed ? { value: total, consumed } : null;
}

function extractMathExpression(prompt) {
  const raw = String(prompt || '');
  const normalized = normalizeCommandText(raw);
  const hasMathCue = [
    'сколько будет', 'посчитай', 'подсчитай', 'вычисли', 'реши пример', 'чему равно',
    'calculate', 'what is',
  ].some((phrase) => normalized.includes(phrase));
  const hasOperatorWord = /(^|\s)(плюс|минус|умнож\p{L}*|помнож\p{L}*|перемнож\p{L}*|раздел\p{L}*|подел\p{L}*|делить|деленное|деленое|степен\p{L}*|plus|minus|times|multiply|multiplied|divide|divided|power)(\s|$)/u.test(normalized);
  const hasOperatorSymbol = /(?:\d|\))\s*[+\-*/^xх×÷]\s*(?:\d|\()/iu.test(raw);
  if (!hasMathCue && !hasOperatorWord && !hasOperatorSymbol) return null;

  const text = normalizeMathText(raw)
    .replace(/([()+\-*/^])/g, ' $1 ')
    .replace(/[^\p{L}\p{N}()+\-*/^.\s]/gu, ' ');
  const sourceTokens = text.split(/\s+/g).filter(Boolean);
  const expressionTokens = [];

  for (let index = 0; index < sourceTokens.length; index += 1) {
    const token = sourceTokens[index];
    if (MATH_FILLER_WORDS.has(token)) continue;
    if (/^[()+\-*/^]$/.test(token) || /^\d+(?:\.\d+)?$/.test(token)) {
      expressionTokens.push(token);
      continue;
    }
    const number = readSpokenNumber(sourceTokens, index);
    if (number) {
      expressionTokens.push(String(number.value));
      index += number.consumed - 1;
      continue;
    }
    if (token === 'на' || token === 'by') continue;
    return null;
  }

  const operatorCount = expressionTokens.filter((token) => /^[+\-*/^]$/.test(token)).length;
  const numberCount = expressionTokens.filter((token) => /^\d+(?:\.\d+)?$/.test(token)).length;
  if (operatorCount < 1 || numberCount < 2) return null;
  return expressionTokens.join(' ');
}

function tokenizeMathExpression(expression) {
  const tokens = [];
  const pattern = /\s*([()+\-*/^]|\d+(?:\.\d+)?|\.\d+)/gy;
  let index = 0;
  while (index < expression.length) {
    pattern.lastIndex = index;
    const match = pattern.exec(expression);
    if (!match) {
      if (/^\s+$/.test(expression.slice(index))) break;
      throw new Error('bad_math_expression');
    }
    tokens.push(match[1]);
    index = pattern.lastIndex;
  }
  return tokens;
}

function evaluateMathExpression(expression) {
  const tokens = tokenizeMathExpression(expression);
  let position = 0;

  const peek = () => tokens[position];
  const take = () => tokens[position++];

  const parsePrimary = () => {
    const token = take();
    if (token === '(') {
      const value = parseExpression();
      if (take() !== ')') throw new Error('bad_math_expression');
      return value;
    }
    if (/^\d+(?:\.\d+)?$|^\.\d+$/.test(token || '')) return Number(token);
    throw new Error('bad_math_expression');
  };

  const parseUnary = () => {
    if (peek() === '+') {
      take();
      return parseUnary();
    }
    if (peek() === '-') {
      take();
      return -parseUnary();
    }
    return parsePrimary();
  };

  const parsePower = () => {
    let value = parseUnary();
    if (peek() === '^') {
      take();
      value = Math.pow(value, parsePower());
    }
    return value;
  };

  const parseTerm = () => {
    let value = parsePower();
    while (peek() === '*' || peek() === '/') {
      const operator = take();
      const right = parsePower();
      if (operator === '*') {
        value *= right;
      } else {
        if (right === 0) throw new Error('division_by_zero');
        value /= right;
      }
    }
    return value;
  };

  function parseExpression() {
    let value = parseTerm();
    while (peek() === '+' || peek() === '-') {
      const operator = take();
      const right = parseTerm();
      value = operator === '+' ? value + right : value - right;
    }
    return value;
  }

  const result = parseExpression();
  if (position !== tokens.length) throw new Error('bad_math_expression');
  if (!Number.isFinite(result) || Math.abs(result) > 1e15) throw new Error('math_result_too_large');
  return result;
}

function formatMathNumber(value) {
  if (Number.isInteger(value)) return String(value);
  const rounded = Math.round(value * 100_000_000) / 100_000_000;
  return String(rounded).replace(/\.?0+$/u, '').replace('.', ',');
}

function formatMathExpression(expression) {
  return expression
    .replace(/\*/g, '×')
    .replace(/\//g, '÷')
    .replace(/\^/g, '^')
    .replace(/\s+/g, ' ')
    .trim();
}

function tryAnswerMathQuery(prompt) {
  const expression = extractMathExpression(prompt);
  if (!expression) return '';
  try {
    const result = evaluateMathExpression(expression);
    return `${formatMathExpression(expression)} = ${formatMathNumber(result)}.`;
  } catch (error) {
    if (error.message === 'division_by_zero') return 'На ноль делить нельзя.';
    if (error.message === 'math_result_too_large') return 'Результат слишком большой для голосового ответа.';
    return '';
  }
}

function firstIntentIndex(prompt, patterns) {
  const text = String(prompt || '');
  let best = Number.POSITIVE_INFINITY;
  for (const pattern of patterns) {
    const match = pattern.exec(text);
    if (match && match.index < best) best = match.index;
  }
  return Number.isFinite(best) ? best : 9999;
}

function cleanupWeatherLocation(value) {
  return String(value || '')
    .replace(/\s+(?:и|а\s+также|плюс|and)\s+(?:врем\p{L}*|который\s+час|сколько\s+времени|time)[\s\S]*$/iu, '')
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
    /(?:погод\p{L}*|weather|forecast|температур\p{L}*)\s+([\p{L}\p{N} .'-]{2,80})/iu,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    const location = cleanupWeatherLocation(match?.[1]);
    if (location) return location;
  }
  return '';
}

function cleanupTimeLocation(value) {
  return String(value || '')
    .replace(/\s+(?:и|а\s+также|плюс|and)\s+(?:погод\p{L}*|weather|forecast|температур\p{L}*)[\s\S]*$/iu, '')
    .replace(/[?.!,;:]+$/g, '')
    .replace(/(^|\s)(сейчас|сегодня|пожалуйста|please|now|today|там|there)(?=\s|$)/giu, ' ')
    .replace(/(^|\s)(какое|какой|какая|сколько|который|что|время|времени|час|часов|time|current)(?=\s|$)/giu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractTimeLocation(prompt, session = null) {
  const text = String(prompt || '').trim();
  const patterns = [
    /(?:врем\p{L}*|сколько\s+времени|который\s+час|time|what\s+time)[\s\S]{0,60}?(?:в|во|на|для|in|for)\s+([\p{L}\p{N} .'-]{2,80})/iu,
    /(?:в|во|на|для|in|for)\s+([\p{L}\p{N} .'-]{2,80})[\s\S]{0,50}?(?:врем\p{L}*|час|time)/iu,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    const location = cleanupTimeLocation(match?.[1]);
    if (location) return location;
  }
  const normalized = normalizeCommandText(text);
  if (/(^|\s)(там|there)(\s|$)/u.test(normalized) && session?.lastGeoContext?.name) {
    return session.lastGeoContext.name;
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
  if (/токи|tokyo/.test(lower)) names.unshift('Токио', 'Tokyo');
  if (/япон|japan/.test(lower)) names.unshift('Япония', 'Japan');
  if (/бангладеш|bangladesh/.test(lower)) names.unshift('Бангладеш', 'Bangladesh');
  if (/польш|poland/.test(lower)) names.unshift('Польша', 'Poland');
  if (/герман|germany/.test(lower)) names.unshift('Германия', 'Germany');
  if (/америк|сша|usa|united states/.test(lower)) names.unshift('США', 'United States');
  if (/^[\p{Script=Cyrillic} -]+$/u.test(raw) && raw.length > 4) {
    names.push(raw.replace(/[еуіыа]$/iu, ''));
    names.push(raw.replace(/(ом|ем|ой|ий|ый)$/iu, ''));
  }
  return [...new Set(names.map((name) => cleanupWeatherLocation(name)).filter(Boolean))];
}

function timeSearchNames(location) {
  const raw = cleanupTimeLocation(location);
  if (!raw) return [];
  return weatherSearchNames(raw);
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

function looksLikeTelegramToken(value) {
  return /^\d{5,}:[A-Za-z0-9_-]{20,}$/u.test(String(value || '').trim());
}

function normalizeTelegramChatId(value) {
  return String(value || '').replace(/\s+/g, '').trim().slice(0, 120);
}

function telegramChatIdOrDefault(chatId = '') {
  return normalizeTelegramChatId(chatId) || getTelegramDefaultChatId();
}

async function callTelegramApi(method, payload = {}, { token = getTelegramBotToken(), timeoutMs = 9000 } = {}) {
  const effectiveToken = String(token || '').trim();
  if (!effectiveToken) {
    throw new Error('Telegram token не задан. Используй /telegram_setup.');
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`https://api.telegram.org/bot${effectiveToken}/${method}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload || {}),
      signal: controller.signal,
    });
    const data = await response.json().catch(() => null);
    if (!response.ok || data?.ok === false) {
      throw new Error(`Telegram ${method}: ${data?.description || `HTTP ${response.status}`}`);
    }
    return data?.result;
  } finally {
    clearTimeout(timeout);
  }
}

function telegramMessageChunks(text) {
  const value = String(text || '').replace(/\r/g, '').trim();
  if (!value) return [];
  const chunks = [];
  let rest = value;
  while (rest.length > 3200) {
    const slice = rest.slice(0, 3200);
    const splitAt = Math.max(slice.lastIndexOf('\n\n'), slice.lastIndexOf('\n'), slice.lastIndexOf('. '), slice.lastIndexOf(' '));
    const end = splitAt > 2200 ? splitAt + (slice[splitAt] === '.' ? 1 : 0) : 3200;
    chunks.push(rest.slice(0, end).trim());
    rest = rest.slice(end).trim();
  }
  if (rest) chunks.push(rest);
  return chunks;
}

function escapeTelegramHtml(value) {
  return String(value || '').replace(/[&<>"]/gu, (char) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
  })[char]);
}

function markdownToTelegramHtml(text) {
  let value = String(text || '').replace(/\r/g, '').trim();
  if (!value) return '';

  const placeholders = [];
  const hold = (html) => {
    const key = `\u0000${placeholders.length}\u0000`;
    placeholders.push([key, html]);
    return key;
  };

  value = value
    .replace(/```(?:[\w-]+)?\n?([\s\S]*?)```/gu, (_, code) => hold(`<pre>${escapeTelegramHtml(code.trim())}</pre>`))
    .replace(/`([^`\n]+)`/gu, (_, code) => hold(`<code>${escapeTelegramHtml(code)}</code>`))
    .replace(/!\[([^\]]*)\]\([^)]+\)/gu, '$1')
    .replace(/\[([^\]\n]+)\]\((https?:\/\/[^)\s]+)\)/gu, (_, label, url) => hold(`<a href="${escapeTelegramHtml(url)}">${escapeTelegramHtml(label)}</a>`));

  value = escapeTelegramHtml(value)
    .replace(/\*\*([^*\n]+)\*\*/gu, '<b>$1</b>')
    .replace(/__([^_\n]+)__/gu, '<b>$1</b>')
    .replace(/~~([^~\n]+)~~/gu, '<s>$1</s>')
    .replace(/\*([^*\n]+)\*/gu, '<i>$1</i>')
    .replace(/_([^_\n]+)_/gu, '<i>$1</i>');

  for (const [key, html] of placeholders) {
    value = value.replaceAll(key, html);
  }
  return value.trim();
}

function stripMarkdownFormatting(text) {
  return String(text || '')
    .replace(/\r/g, '')
    .replace(/```(?:[\w-]+)?\n?([\s\S]*?)```/gu, '$1')
    .replace(/`([^`\n]+)`/gu, '$1')
    .replace(/\[([^\]]+)\]\(([^)\s]+)(?:\s+"[^"]*")?\)/gu, '$1 ($2)')
    .replace(/!\[([^\]]*)\]\([^)]+\)/gu, '$1')
    .replace(/^\s{0,3}#{1,6}\s+/gmu, '')
    .replace(/^\s{0,3}>\s?/gmu, '')
    .replace(/^\s*[*+]\s+/gmu, '- ')
    .replace(/\*\*([^*\n]+)\*\*/gu, '$1')
    .replace(/__([^_\n]+)__/gu, '$1')
    .replace(/\*([^*\n]+)\*/gu, '$1')
    .replace(/_([^_\n]+)_/gu, '$1')
    .replace(/~~([^~\n]+)~~/gu, '$1')
    .replace(/\*{2,}/gu, '')
    .replace(/[ \t]+$/gmu, '')
    .replace(/\n{3,}/gu, '\n\n')
    .trim();
}

function shouldRetryTelegramAsPlainText(error) {
  return /can't parse entities|can't find end tag|unsupported start tag|parse entities|entity/i.test(error?.message || '');
}

async function sendTelegramMessage(text, { chatId = '', disableWebPagePreview = false } = {}) {
  const targetChatId = telegramChatIdOrDefault(chatId);
  if (!targetChatId) {
    throw new Error('Telegram chat_id не задан. Используй /telegram_chat или укажи chat_id в команде.');
  }
  const chunks = telegramMessageChunks(text);
  if (!chunks.length) throw new Error('Пустой текст для Telegram.');

  const sent = [];
  for (const chunk of chunks) {
    let result;
    const html = markdownToTelegramHtml(chunk);
    try {
      result = await callTelegramApi('sendMessage', {
        chat_id: targetChatId,
        text: html || stripMarkdownFormatting(chunk),
        parse_mode: 'HTML',
        disable_web_page_preview: disableWebPagePreview,
      });
    } catch (error) {
      if (!shouldRetryTelegramAsPlainText(error)) throw error;
      console.warn('Telegram HTML parse failed, retrying as plain text:', error.message || error);
      result = await callTelegramApi('sendMessage', {
        chat_id: targetChatId,
        text: stripMarkdownFormatting(chunk),
        disable_web_page_preview: disableWebPagePreview,
      });
    }
    sent.push(result);
  }
  appendEvent('telegram_sent', { chatId: targetChatId, chunks: sent.length });
  return sent;
}

async function validateTelegramSettings(token, chatId = '') {
  const bot = await callTelegramApi('getMe', {}, { token });
  const targetChatId = normalizeTelegramChatId(chatId);
  let chat = null;
  if (targetChatId) {
    chat = await callTelegramApi('getChat', { chat_id: targetChatId }, { token });
  }
  return { bot, chat };
}

async function getRecentTelegramChats() {
  const updates = await callTelegramApi('getUpdates', { limit: 30, timeout: 0 });
  const chats = new Map();
  for (const update of updates || []) {
    const chat = update.message?.chat
      || update.edited_message?.chat
      || update.channel_post?.chat
      || update.my_chat_member?.chat;
    if (!chat?.id) continue;
    chats.set(String(chat.id), chat);
  }
  return [...chats.values()];
}

function formatTelegramChat(chat) {
  const title = chat.title || [chat.first_name, chat.last_name].filter(Boolean).join(' ') || chat.username || 'Без названия';
  const username = chat.username ? ` @${chat.username}` : '';
  return `${chat.id} · ${chat.type || 'chat'} · ${title}${username}`;
}

function formatTelegramStatus() {
  const tokenSource = runtimeConfig.telegramBotToken?.trim()
    ? 'runtime-config'
    : (TELEGRAM_BOT_TOKEN ? '.env' : 'not set');
  const chatId = getTelegramDefaultChatId();
  return [
    `Telegram token: ${getTelegramBotToken() ? `set (${tokenSource})` : 'not set'}`,
    `Default chat_id: ${chatId || 'not set'}`,
    'Для настройки: /telegram_setup, затем /telegram_chat или /telegram_chats.',
  ].join('\n');
}

function formatTelegramNote(actorMember, text) {
  const now = new Intl.DateTimeFormat('ru-RU', {
    timeZone: 'Europe/Kyiv',
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(new Date());
  const author = actorMember?.displayName || actorMember?.user?.username || 'Discord';
  return `Заметка из Discord\nАвтор: ${author}\nВремя: ${now} Киев\n\n${String(text || '').trim()}`;
}

function getLastAssistantReply(session) {
  const item = [...(session?.history || [])].reverse().find((entry) => entry.role === 'assistant' && entry.content);
  return item?.content || '';
}

async function generateTelegramWebSearchSummary(session, actorMember, query) {
  const cleanQuery = String(query || '').replace(/\s+/g, ' ').trim();
  if (!cleanQuery) throw new Error('Что искать для Telegram?');
  if (!isWebSearchEnabled()) throw new Error('Интернет-поиск выключен в настройках.');

  const userName = actorMember?.displayName || actorMember?.user?.username || 'Discord user';
  const today = new Intl.DateTimeFormat('ru-RU', {
    timeZone: 'Europe/Kyiv',
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  }).format(new Date());
  const messages = [
    {
      role: 'system',
      content:
        'Ты готовишь сообщение для Telegram по запросу из Discord. '
        + 'Всегда используй web_search и visit_website для актуальной информации. '
        + 'Ответь на языке запроса: русский, English или mixed. '
        + 'Формат: короткий заголовок, 4-7 плотных пунктов, затем "Источники:" с 2-4 доменами/названиями. '
        + 'Можно использовать простой Markdown только для выделения: **жирный заголовок** и `code`. Не используй markdown-таблицы и # заголовки. '
        + 'Не вставляй длинные URL, не выдумывай источники. '
        + `Текущая дата: ${today}, timezone Europe/Kyiv.`,
    },
    { role: 'user', content: `${userName} просит найти и отправить в Telegram: ${cleanQuery}` },
  ];

  let completion;
  let usedModel = getWebSearchModel();
  let lastError = null;
  const modelsToTry = webSearchModelsToTry(getWebSearchModel());
  for (const [modelIndex, model] of modelsToTry.entries()) {
    usedModel = model;
    try {
      console.log(`telegram web search model=${model} query=${cleanQuery.slice(0, 160)}`);
      const result = await getGroqClient().chat.completions.create({
        model,
        messages,
        temperature: 0.25,
        max_completion_tokens: 900,
        compound_custom: {
          tools: {
            enabled_tools: ['web_search', 'visit_website'],
          },
        },
      }).withResponse();
      completion = result.data;
      trackGroqRateLimits(session?.textChannel, 'telegram-web-search', result.response, model);
      break;
    } catch (error) {
      lastError = error;
      trackGroqRateLimits(session?.textChannel, 'telegram-web-search', error, model);
      if (isGroqRateLimitError(error)) markGroqModelOnCooldown(model, 'telegram-web-search', groqResetHeaderFromError(error, 'tokens'));
      if (
        GROQ_AUTO_MODEL_FALLBACK
        && (shouldFallbackGroqModel(error) || isRequestTooLargeError(error))
        && modelIndex < modelsToTry.length - 1
      ) {
        console.warn(`telegram web search model ${model} failed, trying fallback ${modelsToTry[modelIndex + 1]}:`, error.message || error);
        continue;
      }
      throw error;
    }
  }
  if (!completion) throw lastError || new Error(`No Telegram search completion from ${usedModel}`);
  return trimTelegramReply(completion.choices[0]?.message?.content || '', 3200);
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

async function geocodeTimeLocation(location) {
  for (const name of timeSearchNames(location)) {
    const url = `https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(name)}&count=5&language=ru&format=json`;
    const data = await fetchJson(url).catch(() => null);
    const result = data?.results?.find((item) => item.latitude && item.longitude && item.timezone);
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

function placeDisplayName(place) {
  return [place.name, place.admin1, place.country].filter(Boolean).slice(0, 3).join(', ');
}

function rememberGeoContext(session, place) {
  if (!session || !place) return;
  session.lastGeoContext = {
    name: place.name,
    admin1: place.admin1 || '',
    country: place.country || '',
    timezone: place.timezone || '',
    placeName: placeDisplayName(place),
    at: Date.now(),
  };
}

function timeZoneOffsetMinutes(timeZone, date = new Date()) {
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  });
  const parts = Object.fromEntries(
    formatter.formatToParts(date)
      .filter((part) => part.type !== 'literal')
      .map((part) => [part.type, Number(part.value)]),
  );
  const asUtc = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
  return Math.round((asUtc - date.getTime()) / 60_000);
}

function pluralRu(value, one, few, many) {
  const number = Math.abs(Math.round(value));
  const mod10 = number % 10;
  const mod100 = number % 100;
  if (mod10 === 1 && mod100 !== 11) return one;
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return few;
  return many;
}

function formatMinutesAsRuDuration(totalMinutes) {
  const abs = Math.abs(Math.round(totalMinutes));
  const hours = Math.floor(abs / 60);
  const minutes = abs % 60;
  const parts = [];
  if (hours) parts.push(`${hours} ${pluralRu(hours, 'час', 'часа', 'часов')}`);
  if (minutes) parts.push(`${minutes} ${pluralRu(minutes, 'минута', 'минуты', 'минут')}`);
  return parts.join(' ') || '0 минут';
}

function formatKyivTimeDifference(timeZone, date = new Date()) {
  const diff = timeZoneOffsetMinutes(timeZone, date) - timeZoneOffsetMinutes('Europe/Kyiv', date);
  if (diff === 0) return 'время такое же, как в Киеве';
  return diff > 0
    ? `на ${formatMinutesAsRuDuration(diff)} больше, чем в Киеве`
    : `на ${formatMinutesAsRuDuration(diff)} меньше, чем в Киеве`;
}

function formatLocalTimeForPlace(place, prompt) {
  const english = isMostlyEnglishText(prompt);
  const timeZone = place.timezone;
  const now = new Date();
  const locale = english ? 'en-US' : 'ru-RU';
  const local = new Intl.DateTimeFormat(locale, {
    timeZone,
    weekday: 'long',
    day: '2-digit',
    month: 'long',
    hour: '2-digit',
    minute: '2-digit',
  }).format(now);
  const placeName = placeDisplayName(place);
  if (english) {
    const diff = timeZoneOffsetMinutes(timeZone, now) - timeZoneOffsetMinutes('Europe/Kyiv', now);
    const diffText = diff === 0
      ? 'same time as Kyiv'
      : `${Math.abs(diff / 60)} hours ${diff > 0 ? 'ahead of' : 'behind'} Kyiv`;
    return `Current time in ${placeName}: ${local}. That is ${diffText}. Source: Open-Meteo timezone plus server clock.`;
  }
  return `Сейчас, ${placeName}: ${local}. Это ${formatKyivTimeDifference(timeZone, now)}. Источник: Open-Meteo timezone и часы сервера.`;
}

async function tryAnswerTimeQuery(prompt, session = null) {
  if (!isTimeQuery(prompt)) return '';
  const location = extractTimeLocation(prompt, session);
  if (!location) return '';
  const place = await geocodeTimeLocation(location);
  if (!place?.timezone) return '';
  rememberGeoContext(session, place);
  return formatLocalTimeForPlace(place, prompt);
}

async function tryAnswerWeatherQuery(prompt, session = null) {
  if (!isWeatherQuery(prompt)) return '';
  const location = extractWeatherLocation(prompt);
  if (!location) return '';
  const place = await geocodeWeatherLocation(location);
  if (!place) return '';
  rememberGeoContext(session, place);

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
  const placeName = placeDisplayName(place);
  if (english) {
    return `Current weather in ${placeName}: ${temp} C, feels like ${feels} C, ${label}, wind ${wind} km/h, humidity ${humidity}%. Source: Open-Meteo.`;
  }
  return `Сейчас, ${placeName}: ${temp} градусов, ощущается как ${feels}, ${label}, ветер ${wind} км/ч, влажность ${humidity}%. Источник: Open-Meteo.`;
}

async function tryAnswerDeterministicQuery(session, prompt) {
  const mathReply = tryAnswerMathQuery(prompt);
  if (mathReply) return mathReply;

  const intents = [];
  if (isTimeQuery(prompt)) {
    intents.push({
      type: 'time',
      index: firstIntentIndex(prompt, [/врем/iu, /который\s+час/iu, /сколько\s+времени/iu, /\btime\b/iu]),
    });
  }
  if (isWeatherQuery(prompt)) {
    intents.push({
      type: 'weather',
      index: firstIntentIndex(prompt, [/погод/iu, /температур/iu, /\bweather\b/iu, /\bforecast\b/iu]),
    });
  }
  if (!intents.length) return '';

  intents.sort((left, right) => left.index - right.index);
  const replies = [];
  for (const intent of intents) {
    const reply = intent.type === 'time'
      ? await tryAnswerTimeQuery(prompt, session)
      : await tryAnswerWeatherQuery(prompt, session);
    if (reply) replies.push(reply);
  }
  if (!replies.length && intents.some((intent) => intent.type === 'time')) {
    return 'Не смог точно определить локацию для времени. Скажи город или страну, например: время в Киеве.';
  }
  return replies.join(' ');
}

function isRequestTooLargeError(error) {
  const code = error?.error?.error?.code || error?.error?.code || error?.code;
  return groqErrorStatus(error) === 413 || code === 'request_too_large' || /request entity too large/i.test(error?.message || '');
}

function isGroqPromptLengthError(error) {
  const message = error?.error?.error?.message || error?.error?.message || error?.message || '';
  return groqErrorStatus(error) === 400 && /prompt length/i.test(message);
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

function removeOpenEndedHookSentences(text) {
  const original = String(text || '').replace(/\s+/g, ' ').trim();
  if (!original) return '';
  const hookPattern = /(что нового|чем помочь|что дальше|что у тебя|что скажешь|какие новости|к чему привел[аи]? тебя|рассказывай|ну что)/iu;
  const sentences = original.match(/[^.!?]+[.!?]+|[^.!?]+$/gu) || [original];
  let next = sentences
    .filter((sentence) => !(sentence.includes('?') && hookPattern.test(sentence)))
    .join(' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!next) next = original;

  if (/\?\s*$/.test(next)) {
    const parts = next.match(/[^.!?]+[.!?]+|[^.!?]+$/gu) || [next];
    if (parts.length > 1) {
      next = parts.slice(0, -1).join(' ').replace(/\s+/g, ' ').trim();
    } else if (hookPattern.test(next)) {
      next = next.replace(/\?+$/u, '.');
    }
  }
  return next || original.replace(/\?+$/u, '.');
}

function trimAssistantReply(text, limit = MAX_REPLY_CHARS) {
  let replyText = stripMarkdownFormatting(removeOpenEndedHookSentences(text));
  if (replyText.length > limit) {
    replyText = `${replyText.slice(0, limit).replace(/\s+\S*$/, '').replace(/[,\s;:]+$/, '')}.`;
  }
  return replyText;
}

function trimTelegramReply(text, limit = 3200) {
  let replyText = removeOpenEndedHookSentences(text).replace(/\r/g, '').replace(/\n{4,}/gu, '\n\n\n').trim();
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
      return 'Тон живой, дерзкий и быстрый, можно дружески подкалывать и иногда материться, но без ненависти к защищенным группам.';
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

function profanityStyleInstruction() {
  return 'Это закрытый Discord-сервер друзей: обычный мат, грубоватые слова, сарказм и дружеские подколы разрешены, если это уместно по тону пользователя. Не отвечай отказом только из-за нецензурной лексики. Не превращай мат в травлю: нельзя угрожать, деанонить, призывать к вреду или унижать людей по национальности, расе, религии, полу, ориентации, инвалидности или болезни.';
}

async function askGroq(session, userName, prompt, actorMember = null) {
  const useWebSearch = shouldUseWebSearch(prompt);
  try {
    const deterministicReply = await tryAnswerDeterministicQuery(session, prompt);
    if (deterministicReply) {
      const replyText = trimAssistantReply(deterministicReply, 520);
      session.history.push({ role: 'user', content: `${userName}: ${prompt}` });
      session.history.push({ role: 'assistant', content: replyText });
      session.history.splice(0, Math.max(0, session.history.length - 12));
      return replyText;
    }
  } catch (error) {
    console.warn(`deterministic query fallback failed: ${error.message || error}`);
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
        + profanityStyleInstruction()
        + ' '
        + 'Никогда не утверждай, что выполнил Discord-действие: кик, бан, мут, перенос, создание/удаление канала, роли или сообщения. Такие действия выполняет только командный обработчик; если он не сработал, скажи, что действие не выполнено. '
        + 'Если спрашивают точное текущее время, не вычисляй его сам и не сравнивай с Москвой; лучше скажи, что нужен обработчик времени или актуальный источник. '
        + `Не заканчивай ответ открытым вопросом без необходимости: следующая реплика пользователя будет обработана только если он снова начнет с "${getWakeWord() || getAssistantName()}". `
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
  const modelsToTry = useWebSearch ? webSearchModelsToTry(preferredModel) : chatModelsToTry(preferredModel);
  let usedModel = preferredModel;
  let lastError = null;
  let webSearchRequestTooLarge = false;
  for (const [modelIndex, model] of modelsToTry.entries()) {
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
      if (isGroqRateLimitError(error)) markGroqModelOnCooldown(model, useWebSearch ? 'web-search' : 'chat', groqResetHeaderFromError(error, 'tokens'));
      if (useWebSearch && isRequestTooLargeError(error)) {
        if (modelIndex < modelsToTry.length - 1) {
          console.warn(`web search model ${model} failed with request_too_large, trying next web model`);
          continue;
        }
        webSearchRequestTooLarge = true;
        console.warn('web search failed with request_too_large, falling back to regular chat model');
        break;
      }
      if (GROQ_AUTO_MODEL_FALLBACK && shouldFallbackGroqModel(error) && modelIndex < modelsToTry.length - 1) {
        console.warn(`${useWebSearch ? 'web search' : 'chat'} model ${model} failed, trying fallback ${modelsToTry[modelIndex + 1]}:`, error.message || error);
        continue;
      }
      throw error;
    }
  }
  if (!completion && useWebSearch && webSearchRequestTooLarge) {
    const fallbackMessages = [
      messages[0],
      {
        role: 'system',
        content:
          'Интернет-поиск у провайдера сейчас не прошел из-за ограничения размера запроса. '
          + 'Ответь кратко по общим знаниям и прямо скажи, если для точного ответа нужны актуальные данные.',
      },
      { role: 'user', content: `${userName}: ${prompt}` },
    ];
    const fallbackModels = chatModelsToTry(getChatModel());
    for (const [modelIndex, model] of fallbackModels.entries()) {
      usedModel = model;
      try {
        const result = await getGroqClient().chat.completions.create({
          model,
          messages: fallbackMessages,
          temperature: 0.35,
          max_completion_tokens: 180,
        }).withResponse();
        completion = result.data;
        trackGroqRateLimits(session.textChannel, 'chat-fallback', result.response, model);
        break;
      } catch (error) {
        lastError = error;
        trackGroqRateLimits(session.textChannel, 'chat-fallback', error, model);
        if (isGroqRateLimitError(error)) markGroqModelOnCooldown(model, 'chat-fallback', groqResetHeaderFromError(error, 'tokens'));
        if (GROQ_AUTO_MODEL_FALLBACK && shouldFallbackGroqModel(error) && modelIndex < fallbackModels.length - 1) {
          console.warn(`chat fallback model ${model} failed, trying fallback ${fallbackModels[modelIndex + 1]}:`, error.message || error);
          continue;
        }
        throw error;
      }
    }
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
  const modelsToTry = isWebMode ? webSearchModelsToTry(getWebSearchModel()) : chatModelsToTry(getChatModel());
  const modeInstruction = {
    roast: 'Сделай дерзкий дружеский подкол по никам участников или ситуации в войсе.',
    context: 'Зацепись за локальную память или недавний контекст беседы и кинь смешной комментарий.',
    facts: 'Расскажи неожиданный интересный факт или короткую абсурдную мысль, можно не про участников.',
    news: 'Найди свежую интересную новость из мира и перескажи ее одной живой фразой.',
  }[mode] || 'Скажи живую фразу для продолжения беседы.';
  const prompt = [
    'Сервер закрытый, люди свои. Стиль можно делать острее: сарказм, дружеский roast, черный юмор без занудства.',
    profanityStyleInstruction(),
    modeInstruction,
    'Можно шутить не только о пользователях, а вообще о чем угодно. Можно использовать видимые ники, локальную память и недавний контекст.',
    'Можно говорить по-русски, English или mixed, если так звучит смешнее или естественнее.',
    'Не произноси токены, API-ключи, пароли и длинные секретные строки целиком.',
    'Без markdown. Максимум 1-2 коротких предложения, чтобы это нормально звучало голосом.',
    `Участники в voice: ${names.join(', ')}.`,
    memoryContext ? `Локальная память:\n${memoryContext}` : '',
    recentContext ? `Недавний контекст:\n${recentContext}` : '',
  ].filter(Boolean).join('\n');

  let lastError = null;
  for (const [modelIndex, model] of modelsToTry.entries()) {
    try {
      const request = {
        model,
        messages: [
          {
            role: 'system',
            content: `Ты голосовой собеседник для закрытого Discord-сервера друзей. Говори живо, дерзко, коротко и смешно, как свой человек. ${profanityStyleInstruction()}`,
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
      lastError = error;
      trackGroqRateLimits(session.textChannel, `idle-chatter-${mode}`, error, model);
      if (isGroqRateLimitError(error)) markGroqModelOnCooldown(model, `idle-chatter-${mode}`, groqResetHeaderFromError(error, 'tokens'));
      if (
        GROQ_AUTO_MODEL_FALLBACK
        && (shouldFallbackGroqModel(error) || (isWebMode && isRequestTooLargeError(error)))
        && modelIndex < modelsToTry.length - 1
      ) {
        console.warn(`idle chatter model ${model} failed, trying fallback ${modelsToTry[modelIndex + 1]}:`, error.message || error);
        continue;
      }
      throw error;
    }
  }
  throw lastError || new Error('No idle chatter completion');
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

  const spokenText = stripMarkdownFormatting(text);
  if (!spokenText) return;

  const speechVersion = beginSpeech(session);
  const wavPath = await synthesizeSpeech(spokenText);
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
  const captureStartedAt = Date.now();
  session.activeUsers.add(userId);
  session.activeUserStartedAt ||= new Map();
  session.activeUserStartedAt.set(userId, captureStartedAt);

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
      if (isSttBoilerplateTranscript(transcript)) {
        markIgnored(session, 'stt_boilerplate', { lastTranscript: transcript });
        return;
      }
      const languageGuardReason = transcriptLanguageGuardReason(transcript, session);
      if (languageGuardReason) {
        markIgnored(session, languageGuardReason, { lastTranscript: transcript });
        return;
      }
      if (!shouldAnswer(transcript, session, captureStartedAt, userId)) {
        markIgnored(session, 'no_wake_word', { lastTranscript: transcript });
        return;
      }
      const wakeDetected = hasWakeWord(transcript);
      const fromWakeListen = !wakeDetected && isWakeListenWindow(session, captureStartedAt, userId);
      const prompt = promptFromTranscript(session, transcript);
      markAssistantInteraction(session, 'voice_interrupt');
      if (getWakeWord() && !LISTEN_WITHOUT_WAKE_WORD && wakeDetected && !prompt) {
        markWakeListen(session, userId);
        console.log(`wake listen opened user=${userId}: ${transcript}`);
        markIgnored(session, 'wake_listening_interrupt', { lastTranscript: transcript });
        await sendText(session.textChannel, `Слушаю ${Math.round(WAKE_LISTEN_WINDOW_MS / 1000)} секунд. Говори вопрос без повторного "${getWakeWord()}".`);
        return;
      }
      if (fromWakeListen) clearWakeListen(session);
      const simpleAction = parseSimpleAction(prompt);
      if (!canHandleSimpleActionWhileBusy(simpleAction?.action)) {
        markIgnored(session, 'busy_non_interrupt_action', {
          lastTranscript: transcript,
          lastIgnoredAction: simpleAction?.action || null,
        });
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

  if (Date.now() - session.lastReplyAt < REPLY_COOLDOWN_MS && !isWakeListenWindow(session, captureStartedAt, userId)) {
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
      if (isSttBoilerplateTranscript(transcript)) {
        markIgnored(session, 'stt_boilerplate', { lastTranscript: transcript });
        return;
      }
      const languageGuardReason = transcriptLanguageGuardReason(transcript, session);
      if (languageGuardReason) {
        markIgnored(session, languageGuardReason, { lastTranscript: transcript });
        return;
      }
      if (!shouldAnswer(transcript, session, captureStartedAt, userId)) {
        markIgnored(session, 'no_wake_word', { lastTranscript: transcript });
        return;
      }

      const wakeDetected = hasWakeWord(transcript);
      const fromWakeListen = !wakeDetected && isWakeListenWindow(session, captureStartedAt, userId);
      const prompt = promptFromTranscript(session, transcript);
      markAssistantInteraction(session, 'voice');
      if (getWakeWord() && !LISTEN_WITHOUT_WAKE_WORD && wakeDetected && !prompt) {
        markWakeListen(session, userId);
        console.log(`wake listen opened user=${userId}: ${transcript}`);
        markIgnored(session, 'wake_listening', { lastTranscript: transcript });
        await sendText(session.textChannel, `Слушаю ${Math.round(WAKE_LISTEN_WINDOW_MS / 1000)} секунд. Говори вопрос без повторного "${getWakeWord()}".`);
        return;
      }
      if (fromWakeListen) clearWakeListen(session);
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
        wake: wakeDetected,
        wakeListen: fromWakeListen,
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
    wakeListenStartedAt: 0,
    wakeListenUntil: 0,
    wakeListenUserId: null,
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

function buildTelegramSetupModal() {
  const chatId = getTelegramDefaultChatId();
  const tokenInput = new TextInputBuilder()
    .setCustomId('telegram_token')
    .setLabel('Telegram bot token')
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setPlaceholder('123456789:AA...');
  const chatInput = new TextInputBuilder()
    .setCustomId('telegram_chat_id')
    .setLabel('Default chat_id, optional')
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setPlaceholder('-1001234567890 or 123456789');
  if (chatId) chatInput.setValue(chatId);

  return new ModalBuilder()
    .setCustomId('telegram_setup_modal')
    .setTitle('Telegram setup')
    .addComponents(
      new ActionRowBuilder().addComponents(tokenInput),
      new ActionRowBuilder().addComponents(chatInput),
    );
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
    new SlashCommandBuilder()
      .setName('telegram_setup')
      .setDescription('Безопасно сохранить Telegram bot token через приватное окно')
      .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),
    new SlashCommandBuilder()
      .setName('telegram_chat')
      .setDescription('Установить default Telegram chat_id')
      .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)
      .addStringOption((option) => option.setName('chat_id').setDescription('Telegram chat_id').setRequired(true)),
    new SlashCommandBuilder()
      .setName('telegram_chats')
      .setDescription('Показать последние Telegram-чаты из getUpdates')
      .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),
    new SlashCommandBuilder()
      .setName('telegram_status')
      .setDescription('Показать статус Telegram-интеграции')
      .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),
    new SlashCommandBuilder()
      .setName('telegram_clear')
      .setDescription('Очистить Telegram token/chat_id из runtime-config')
      .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),
    new SlashCommandBuilder()
      .setName('telegram_send')
      .setDescription('Отправить сообщение в Telegram')
      .addStringOption((option) => option.setName('text').setDescription('Текст сообщения').setRequired(true))
      .addStringOption((option) => option.setName('chat_id').setDescription('Опциональный Telegram chat_id').setRequired(false)),
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
  if (interaction.isModalSubmit() && interaction.customId === 'telegram_setup_modal') {
    try {
      await interaction.deferReply({ flags: MessageFlags.Ephemeral | MessageFlags.SuppressNotifications });
      if (!canUsePermission(interaction.member, PermissionFlagsBits.ManageGuild)) {
        await reply(interaction, 'Нужно право Manage Server или Administrator для настройки Telegram.', { flags: MessageFlags.Ephemeral });
        return;
      }

      const token = interaction.fields.getTextInputValue('telegram_token')?.trim();
      const chatId = normalizeTelegramChatId(interaction.fields.getTextInputValue('telegram_chat_id'));
      if (!looksLikeTelegramToken(token)) {
        await reply(interaction, 'Это не похоже на Telegram bot token. Возьми токен у @BotFather.', { flags: MessageFlags.Ephemeral });
        return;
      }

      const { bot, chat } = await validateTelegramSettings(token, chatId);
      updateRuntimeConfig({
        telegramBotToken: token,
        telegramDefaultChatId: chatId || getTelegramDefaultChatId(),
      });
      appendEvent('telegram_configured', {
        guildId: interaction.guildId,
        actorId: interaction.user?.id,
        botUsername: bot?.username || null,
        chatId: chatId || null,
      });
      await reply(
        interaction,
        [
          `Telegram подключен: @${bot?.username || bot?.first_name || 'bot'}.`,
          chat ? `Default chat: ${formatTelegramChat(chat)}.` : (chatId ? `Default chat_id сохранен: ${chatId}.` : 'Default chat_id пока не задан. Используй /telegram_chat или /telegram_chats.'),
          'Токен не отправлялся в канал и сохранен только в runtime-config.',
        ].join('\n'),
        { flags: MessageFlags.Ephemeral },
      );
    } catch (error) {
      console.error('telegram setup modal failed:', error);
      if (!interaction.replied && !interaction.deferred) {
        await interaction.reply(ephemeralOptions(`Ошибка Telegram setup: \`${error.message || error}\``)).catch(() => {});
      } else {
        await interaction.editReply(ephemeralOptions(`Ошибка Telegram setup: \`${error.message || error}\``)).catch(() => {});
      }
    }
    return;
  }

  if (!interaction.isChatInputCommand()) return;
  setMonitorChannel(interaction.channel);

  try {
    if (interaction.commandName !== 'join') {
      const activeSession = getInteractionSession(interaction);
      if (activeSession) markAssistantInteraction(activeSession, `slash:${interaction.commandName}`);
    }

    if (interaction.commandName === 'telegram_setup') {
      if (!canUsePermission(interaction.member, PermissionFlagsBits.ManageGuild)) {
        await interaction.reply(ephemeralOptions('Нужно право Manage Server или Administrator.'));
        return;
      }
      await interaction.showModal(buildTelegramSetupModal());
      return;
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

    if (interaction.commandName === 'telegram_chat') {
      await interaction.deferReply({ flags: MessageFlags.Ephemeral | MessageFlags.SuppressNotifications });
      const chatId = normalizeTelegramChatId(interaction.options.getString('chat_id', true));
      if (!getTelegramBotToken()) {
        await reply(interaction, 'Telegram token не задан. Сначала используй /telegram_setup.', { flags: MessageFlags.Ephemeral });
        return;
      }
      const chat = await callTelegramApi('getChat', { chat_id: chatId });
      updateRuntimeConfig({ telegramDefaultChatId: chatId });
      await reply(interaction, `Default Telegram chat сохранен: ${formatTelegramChat(chat)}.`, { flags: MessageFlags.Ephemeral });
    }

    if (interaction.commandName === 'telegram_chats') {
      await interaction.deferReply({ flags: MessageFlags.Ephemeral | MessageFlags.SuppressNotifications });
      const chats = await getRecentTelegramChats();
      const lines = chats.map(formatTelegramChat);
      await reply(
        interaction,
        `Telegram chats:\n${formatShortList(lines, 30)}\nЕсли списка нет, напиши Telegram-боту /start или добавь его в группу и отправь туда сообщение.`,
        { flags: MessageFlags.Ephemeral },
      );
    }

    if (interaction.commandName === 'telegram_status') {
      await interaction.deferReply({ flags: MessageFlags.Ephemeral | MessageFlags.SuppressNotifications });
      let extra = '';
      if (getTelegramBotToken()) {
        const bot = await callTelegramApi('getMe').catch((error) => ({ error: error.message || String(error) }));
        extra = bot.error ? `\ngetMe: ${bot.error}` : `\nBot: @${bot.username || bot.first_name || 'unknown'}`;
      }
      await reply(interaction, `${formatTelegramStatus()}${extra}`, { flags: MessageFlags.Ephemeral });
    }

    if (interaction.commandName === 'telegram_clear') {
      await interaction.deferReply({ flags: MessageFlags.Ephemeral | MessageFlags.SuppressNotifications });
      updateRuntimeConfig({ telegramBotToken: '', telegramDefaultChatId: '' });
      await reply(
        interaction,
        TELEGRAM_BOT_TOKEN || TELEGRAM_DEFAULT_CHAT_ID
          ? 'Очистил Telegram runtime-config. В .env есть Telegram-настройки, они останутся активны до изменения .env.'
          : 'Очистил Telegram runtime-config.',
        { flags: MessageFlags.Ephemeral },
      );
    }

    if (interaction.commandName === 'telegram_send') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      const text = interaction.options.getString('text', true);
      const chatId = interaction.options.getString('chat_id', false) || '';
      await sendTelegramMessage(text, { chatId });
      await reply(interaction, 'Отправил сообщение в Telegram.');
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
