import 'dotenv/config';

import { spawn } from 'node:child_process';
import { promises as fs, rmSync } from 'node:fs';
import path from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';

import { createStorage } from './storage.mjs';
import { maskBackupTarget, normalizeBackupTargetPath, splitBackupTargetCredentials, syncBackupToTarget } from './backup-targets.mjs';
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

process.env.TZ ||= process.env.REMINDER_TIME_ZONE || 'Europe/Kyiv';

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
const panelCommandsPath = path.join(dataDir, 'panel-commands.jsonl');

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
const GROQ_MODEL_DISCOVERY_ENABLED = (process.env.GROQ_MODEL_DISCOVERY_ENABLED || 'true') !== 'false';
const GROQ_MODEL_DISCOVERY_INTERVAL_MS = Math.max(6 * 60 * 60_000, Number(process.env.GROQ_MODEL_DISCOVERY_INTERVAL_MS || 48 * 60 * 60_000));
const GROQ_MODEL_DISCOVERY_INITIAL_DELAY_MS = Math.max(5_000, Number(process.env.GROQ_MODEL_DISCOVERY_INITIAL_DELAY_MS || 30_000));
const GROQ_AUTO_SELECT_DISCOVERED_MODELS = (process.env.GROQ_AUTO_SELECT_DISCOVERED_MODELS || 'true') !== 'false';
const GROQ_DISCOVERED_CHAT_LIMIT = Math.max(1, Math.min(20, Number(process.env.GROQ_DISCOVERED_CHAT_LIMIT || 8)));
const GROQ_DISCOVERED_ACTION_LIMIT = Math.max(1, Math.min(12, Number(process.env.GROQ_DISCOVERED_ACTION_LIMIT || 6)));
const GROQ_DISCOVERED_STT_LIMIT = Math.max(1, Math.min(8, Number(process.env.GROQ_DISCOVERED_STT_LIMIT || 4)));
const GROQ_DISCOVERED_WEB_LIMIT = Math.max(1, Math.min(8, Number(process.env.GROQ_DISCOVERED_WEB_LIMIT || 4)));
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
const DEFAULT_STT_LANGUAGE = process.env.STT_LANGUAGE?.trim() || 'ru';
const DEFAULT_TTS_PROVIDER = (process.env.TTS_PROVIDER?.trim() || (process.platform === 'darwin' ? 'macos' : 'espeak')).toLowerCase();
const DEFAULT_MACOS_TTS_VOICE = process.env.MACOS_TTS_VOICE?.trim() || 'Milena';
const DEFAULT_ESPEAK_TTS_VOICE = process.env.ESPEAK_TTS_VOICE?.trim() || 'ru';
const DEFAULT_ESPEAK_TTS_SPEED = Math.max(80, Math.min(260, Number(process.env.ESPEAK_TTS_SPEED || 165)));
const DEFAULT_EDGE_TTS_VOICE = process.env.EDGE_TTS_VOICE?.trim() || 'ru-RU-SvetlanaNeural';
const DEFAULT_EDGE_TTS_ENGLISH_VOICE = process.env.EDGE_TTS_ENGLISH_VOICE?.trim() || 'en-US-AvaMultilingualNeural';
const DEFAULT_EDGE_TTS_RATE = process.env.EDGE_TTS_RATE?.trim() || '+0%';
const DEFAULT_EDGE_TTS_PITCH = process.env.EDGE_TTS_PITCH?.trim() || '+0Hz';
const EDGE_TTS_COMMAND = process.env.EDGE_TTS_COMMAND?.trim() || '';
const MUSIC_YT_DLP_COMMAND = process.env.MUSIC_YT_DLP_COMMAND?.trim() || '';
const MUSIC_MAX_QUEUE = Math.max(1, Math.min(100, Number(process.env.MUSIC_MAX_QUEUE || 25)));
const MUSIC_DEFAULT_VOLUME = Math.max(0, Math.min(1.5, Number(process.env.MUSIC_DEFAULT_VOLUME || 0.45)));
const MUSIC_SEARCH_TIMEOUT_MS = Math.max(5_000, Math.min(60_000, Number(process.env.MUSIC_SEARCH_TIMEOUT_MS || 25_000)));
const MUSIC_FFMPEG_LOG_LIMIT = Math.max(500, Math.min(8000, Number(process.env.MUSIC_FFMPEG_LOG_LIMIT || 2500)));
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN?.trim() || '';
const TELEGRAM_DEFAULT_CHAT_ID = process.env.TELEGRAM_DEFAULT_CHAT_ID?.trim() || '';
const DEFAULT_TELEGRAM_INBOUND_ENABLED = (process.env.TELEGRAM_INBOUND_ENABLED || 'true') !== 'false';
const DEFAULT_TELEGRAM_INBOUND_ALLOWED_CHAT_IDS = process.env.TELEGRAM_INBOUND_ALLOWED_CHAT_IDS?.trim() || '';
const DEFAULT_TELEGRAM_INBOUND_PLAIN_FORWARD = process.env.TELEGRAM_INBOUND_PLAIN_FORWARD === 'true';
const TELEGRAM_INBOUND_POLL_MS = Math.max(3_000, Math.min(60_000, Number(process.env.TELEGRAM_INBOUND_POLL_MS || 7_000)));
const TELEGRAM_INBOUND_LIMIT = Math.max(1, Math.min(100, Number(process.env.TELEGRAM_INBOUND_LIMIT || 20)));
const HEAVY_TASK_MAX_PENDING = Math.max(10, Math.min(500, Number(process.env.HEAVY_TASK_MAX_PENDING || 120)));
const HEAVY_TASK_SLOW_MS = Math.max(1_000, Math.min(120_000, Number(process.env.HEAVY_TASK_SLOW_MS || 15_000)));
const AI_TASK_CONCURRENCY = Math.max(1, Math.min(6, Number(process.env.AI_TASK_CONCURRENCY || 2)));
const WEB_SEARCH_TASK_CONCURRENCY = Math.max(1, Math.min(3, Number(process.env.WEB_SEARCH_TASK_CONCURRENCY || 1)));
const TTS_TASK_CONCURRENCY = Math.max(1, Math.min(3, Number(process.env.TTS_TASK_CONCURRENCY || 1)));
const TELEGRAM_TASK_CONCURRENCY = Math.max(1, Math.min(3, Number(process.env.TELEGRAM_TASK_CONCURRENCY || 1)));
const BACKUP_TASK_CONCURRENCY = Math.max(1, Math.min(2, Number(process.env.BACKUP_TASK_CONCURRENCY || 1)));
const SOUNDBOARD_TASK_CONCURRENCY = Math.max(1, Math.min(3, Number(process.env.SOUNDBOARD_TASK_CONCURRENCY || 1)));

const LISTEN_WITHOUT_WAKE_WORD = (process.env.LISTEN_WITHOUT_WAKE_WORD || 'false') === 'true';
const ENV_BOT_WAKE_WORD = (process.env.BOT_WAKE_WORD || DEFAULT_ASSISTANT_NAME || 'бот').trim().toLowerCase();
const DEFAULT_BOT_WAKE_ALIASES = ENV_BOT_WAKE_WORD === 'бот'
  ? 'вот,от,робот,роботик,ботик,бота,боту,боте,боты,ботом,бод,бат,борт,вод,бо,ботт'
  : '';
const ENV_BOT_WAKE_ALIASES = process.env.BOT_WAKE_ALIASES || DEFAULT_BOT_WAKE_ALIASES;
const ENV_BOT_WAKE_FUZZY = (process.env.BOT_WAKE_FUZZY || 'true') === 'true';
const MAX_REPLY_CHARS = Math.max(120, Number(process.env.MAX_REPLY_CHARS || 500));
const VOICE_REPLY_MAX_CHARS = Math.max(180, Math.min(900, Number(process.env.VOICE_REPLY_MAX_CHARS || Math.min(MAX_REPLY_CHARS, 450))));
const SILENT_MESSAGES = (process.env.SILENT_MESSAGES || 'true') === 'true';
const SILENCE_MS = Math.max(450, Number(process.env.SILENCE_MS || 900));
const MAX_UTTERANCE_MS = Math.max(3000, Number(process.env.MAX_UTTERANCE_MS || 8000));
const POST_WAKE_SILENCE_MS = Math.max(SILENCE_MS, Number(process.env.POST_WAKE_SILENCE_MS || 1200));
const POST_WAKE_MAX_UTTERANCE_MS = Math.max(MAX_UTTERANCE_MS, Number(process.env.POST_WAKE_MAX_UTTERANCE_MS || 20_000));
const STALE_CAPTURE_MS = Math.max(MAX_UTTERANCE_MS, POST_WAKE_MAX_UTTERANCE_MS) + Math.max(SILENCE_MS, POST_WAKE_SILENCE_MS) + 5000;
const MIN_AUDIO_MS = Math.max(250, Number(process.env.MIN_AUDIO_MS || 350));
const MIN_RMS = Math.max(1, Number(process.env.MIN_RMS || 60));
const WAKE_LISTEN_WINDOW_MS = Math.max(2000, Number(process.env.WAKE_LISTEN_WINDOW_MS || 15000));
const WAKE_LISTEN_PREOPEN_GRACE_MS = Math.max(0, Number(process.env.WAKE_LISTEN_PREOPEN_GRACE_MS || 5000));
const WAKE_ACK_AI_ENABLED = (process.env.WAKE_ACK_AI_ENABLED || 'true') !== 'false';
const WAKE_ACK_MAX_CHARS = Math.max(8, Math.min(80, Number(process.env.WAKE_ACK_MAX_CHARS || 32)));
const WAKE_ACK_FALLBACK_PHRASES = parseCsvList(process.env.WAKE_ACK_FALLBACK_PHRASES || 'Слушаю,Говори,На связи,Да, я тут,Внимательно,Давай,Жду вопрос');
const REPLY_COOLDOWN_MS = Math.max(0, Number(process.env.REPLY_COOLDOWN_MS || 900));
const IGNORE_AFTER_JOIN_MS = Math.max(0, Number(process.env.IGNORE_AFTER_JOIN_MS || 500));
const STREAM_DISABLE_RESTORE_MS = Math.max(0, Number(process.env.STREAM_DISABLE_RESTORE_MS || 8000));
const STREAM_DISABLE_VERIFY_DELAY_MS = Math.max(250, Math.min(5000, Number(process.env.STREAM_DISABLE_VERIFY_DELAY_MS || 1500)));
const DEFAULT_PRESENCE_ANNOUNCEMENTS_ENABLED = (process.env.PRESENCE_ANNOUNCEMENTS_ENABLED || 'true') === 'true';
const PRESENCE_ANNOUNCEMENT_DELAY_MS = Math.max(0, Number(process.env.PRESENCE_ANNOUNCEMENT_DELAY_MS || 900));
const PRESENCE_ANNOUNCEMENT_COOLDOWN_MS = Math.max(0, Number(process.env.PRESENCE_ANNOUNCEMENT_COOLDOWN_MS || 25_000));
const PRESENCE_ANNOUNCEMENT_QUIET_WAIT_MS = Math.max(0, Number(process.env.PRESENCE_ANNOUNCEMENT_QUIET_WAIT_MS || 8_000));
const PRESENCE_NAME_ANNOUNCEMENT_MAX_MEMBERS = Math.max(1, Number(process.env.PRESENCE_NAME_ANNOUNCEMENT_MAX_MEMBERS || 2));
const PRESENCE_BOT_JOIN_NAMED_MAX_MEMBERS = Math.max(1, Number(process.env.PRESENCE_BOT_JOIN_NAMED_MAX_MEMBERS || 3));
const PRESENCE_MEMBER_GREETING_COOLDOWN_MS = Math.max(0, Number(process.env.PRESENCE_MEMBER_GREETING_COOLDOWN_MS || 12 * 60 * 60_000));
const PRESENCE_ANNOUNCEMENT_MAX_CHARS = Math.max(32, Math.min(120, Number(process.env.PRESENCE_ANNOUNCEMENT_MAX_CHARS || 60)));
const VOICE_DEBUG = (process.env.VOICE_DEBUG || 'false') === 'true';
const API_LIMIT_ALERT_THRESHOLDS = [...new Set(parseCsvList(process.env.API_LIMIT_ALERT_THRESHOLDS || '15,5')
  .map((value) => Number(value))
  .filter((value) => Number.isFinite(value) && value > 0 && value < 100)
  .map((value) => Math.round(value)))]
  .sort((a, b) => b - a);
const API_LIMIT_ALERT_START_PERCENT = API_LIMIT_ALERT_THRESHOLDS[0] || 15;
const API_LIMIT_ALERT_RESET_PERCENT = Math.max(
  API_LIMIT_ALERT_START_PERCENT + 1,
  Math.min(99, Number(process.env.API_LIMIT_ALERT_RESET_PERCENT || 50)),
);
const MAX_MEMORY_ITEMS = Math.max(10, Number(process.env.MAX_MEMORY_ITEMS || 200));
const MEMORY_CONTEXT_LIMIT = Math.max(0, Number(process.env.MEMORY_CONTEXT_LIMIT || 8));
const MAX_REMINDER_ITEMS = Math.max(10, Number(process.env.MAX_REMINDER_ITEMS || 200));
const MAX_REMINDER_TIMEOUT_MS = 2_147_000_000;
const REMINDER_TIME_ZONE = process.env.REMINDER_TIME_ZONE?.trim() || process.env.TZ || 'Europe/Kyiv';
const DEFAULT_BACKUP_ENABLED = (process.env.BACKUP_ENABLED || 'false') === 'true';
const DEFAULT_BACKUP_TARGET_PATH = process.env.BACKUP_TARGET_PATH?.trim() || path.join(dataDir, 'backups');
const DEFAULT_BACKUP_INTERVAL_HOURS = Math.max(1, Math.min(720, Number(process.env.BACKUP_INTERVAL_HOURS || 24)));
const DEFAULT_BACKUP_RETENTION = Math.max(1, Math.min(20, Number(process.env.BACKUP_RETENTION || 2)));
const DEFAULT_BACKUP_IDLE_ONLY = (process.env.BACKUP_IDLE_ONLY || 'true') !== 'false';
const IDLE_CHATTER_CHECK_MS = 30_000;
const IDLE_LEAVE_CHECK_MS = 30_000;
const BACKUP_CHECK_MS = 5 * 60_000;
const HEALTHCHECK_INTERVAL_MS = 60_000;
const EVENT_LOG_MAX_PAYLOAD_CHARS = 2500;
const STT_PROMPT_MAX_CHARS = Math.max(100, Math.min(640, Number(process.env.STT_PROMPT_MAX_CHARS || 420)));
const STT_PROMPT_MAX_BYTES = Math.max(256, Math.min(896, Number(process.env.STT_PROMPT_MAX_BYTES || 780)));
const STT_TRANSIENT_RETRIES = Math.max(1, Math.min(5, Number(process.env.STT_TRANSIENT_RETRIES || 3)));
const STT_WAKE_RETRY_ENABLED = (process.env.STT_WAKE_RETRY_ENABLED || 'true') !== 'false';
const STT_LANGUAGE_GUARD_ENABLED = (process.env.STT_LANGUAGE_GUARD_ENABLED || 'true') !== 'false';
const STT_ALLOWED_LANGUAGES = process.env.STT_ALLOWED_LANGUAGES?.trim() || 'ru,en';
const STT_LANGUAGE_HINT = process.env.STT_LANGUAGE_HINT?.trim()
  || 'Основная речь на русском. Английские слова допускаются только как короткие термины, команды, ники или названия.';
const STT_PROMPT_BASE = process.env.STT_PROMPT?.trim()
  || 'Русская речь в Discord. Английские слова только как отдельные термины, команды, ники и названия. Частые слова: Бот, bot, what, вот, от, робот, роботик, ботик, бота, боду, бод, bat, board, борт, войс, voice, channel, disconnect, mute, move, stream, screen, запомни, remember, remind, stop, хватит, остановись, харош, хорош.';

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
    sttRequests: 0,
    sttTransientErrors: 0,
    sttPromptLengthRetries: 0,
    sttModelFallbacks: 0,
    lastSttStats: null,
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

class TaskQueue {
  constructor(name, { concurrency = 1, maxPending = HEAVY_TASK_MAX_PENDING, slowMs = HEAVY_TASK_SLOW_MS } = {}) {
    this.name = name;
    this.concurrency = concurrency;
    this.maxPending = maxPending;
    this.slowMs = slowMs;
    this.pending = [];
    this.active = 0;
    this.nextId = 1;
    this.completed = 0;
    this.failed = 0;
    this.rejected = 0;
    this.lastQueuedAt = 0;
    this.lastStartedAt = 0;
    this.lastFinishedAt = 0;
    this.lastDurationMs = 0;
    this.lastWaitMs = 0;
    this.lastError = '';
    this.lastLabel = '';
    this.activeLabels = new Map();
  }

  run(label, task, meta = {}) {
    if (this.pending.length >= this.maxPending) {
      this.rejected += 1;
      const error = new Error(`Очередь ${this.name} переполнена. Попробуй позже.`);
      this.lastError = error.message;
      appendEvent('task_queue_rejected', {
        queue: this.name,
        label,
        pending: this.pending.length,
        active: this.active,
        meta,
      });
      return Promise.reject(error);
    }

    return new Promise((resolve, reject) => {
      const item = {
        id: this.nextId++,
        label: String(label || 'task').slice(0, 120),
        task,
        meta,
        queuedAt: Date.now(),
        resolve,
        reject,
      };
      this.pending.push(item);
      this.lastQueuedAt = item.queuedAt;
      this.lastLabel = item.label;
      scheduleStatusSnapshot();
      this.drain();
    });
  }

  drain() {
    while (this.active < this.concurrency && this.pending.length) {
      const item = this.pending.shift();
      this.active += 1;
      const startedAt = Date.now();
      const waitMs = startedAt - item.queuedAt;
      this.lastStartedAt = startedAt;
      this.lastWaitMs = waitMs;
      this.lastLabel = item.label;
      this.activeLabels.set(item.id, item.label);
      scheduleStatusSnapshot();
      if (waitMs >= this.slowMs) {
        appendEvent('task_queue_wait_slow', {
          queue: this.name,
          label: item.label,
          waitMs,
          pending: this.pending.length,
          active: this.active,
          meta: item.meta,
        });
      }

      Promise.resolve()
        .then(() => item.task())
        .then((result) => {
          this.completed += 1;
          this.lastError = '';
          item.resolve(result);
        })
        .catch((error) => {
          this.failed += 1;
          this.lastError = error.message || String(error);
          appendEvent('task_queue_failed', {
            queue: this.name,
            label: item.label,
            waitMs,
            durationMs: Date.now() - startedAt,
            error: this.lastError,
            meta: item.meta,
          });
          item.reject(error);
        })
        .finally(() => {
          this.active -= 1;
          this.activeLabels.delete(item.id);
          this.lastFinishedAt = Date.now();
          this.lastDurationMs = this.lastFinishedAt - startedAt;
          scheduleStatusSnapshot();
          this.drain();
        });
    }
  }

  snapshot() {
    return {
      name: this.name,
      concurrency: this.concurrency,
      active: this.active,
      pending: this.pending.length,
      completed: this.completed,
      failed: this.failed,
      rejected: this.rejected,
      lastQueuedAt: this.lastQueuedAt,
      lastStartedAt: this.lastStartedAt,
      lastFinishedAt: this.lastFinishedAt,
      lastDurationMs: this.lastDurationMs,
      lastWaitMs: this.lastWaitMs,
      lastLabel: this.lastLabel,
      lastError: this.lastError,
      activeLabels: [...this.activeLabels.values()],
      pendingLabels: this.pending.slice(0, 8).map((item) => item.label),
    };
  }
}

const taskQueues = {
  ai: new TaskQueue('ai', { concurrency: AI_TASK_CONCURRENCY }),
  webSearch: new TaskQueue('web-search', { concurrency: WEB_SEARCH_TASK_CONCURRENCY }),
  tts: new TaskQueue('tts', { concurrency: TTS_TASK_CONCURRENCY }),
  telegram: new TaskQueue('telegram', { concurrency: TELEGRAM_TASK_CONCURRENCY }),
  backup: new TaskQueue('backup', { concurrency: BACKUP_TASK_CONCURRENCY }),
  soundboard: new TaskQueue('soundboard', { concurrency: SOUNDBOARD_TASK_CONCURRENCY }),
};

function taskQueueSnapshot() {
  return Object.fromEntries(Object.entries(taskQueues).map(([key, queue]) => [key, queue.snapshot()]));
}

function queueMetaForSession(session, extra = {}) {
  return {
    guildId: session?.guild?.id || null,
    voiceChannelId: session?.voiceChannel?.id || null,
    textChannelId: session?.textChannel?.id || null,
    ...extra,
  };
}

function runQueuedTask(queueName, label, task, meta = {}) {
  const queue = taskQueues[queueName];
  if (!queue) return task();
  return queue.run(label, task, meta);
}

const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates],
});

const sessions = new Map();
const telegramSessionHistories = new Map();
let telegramInboundPollInProgress = false;
let telegramInboundBackoffUntil = 0;
const groqLimitAlertState = new Map();
const groqLastLimits = new Map();
const groqModelCooldowns = new Map();
const groqDiscoveredModels = {
  checkedAt: 0,
  nextCheckAt: 0,
  source: 'not-run',
  error: '',
  chat: [],
  action: [],
  stt: [],
  web: [],
  modelInfo: [],
};
const reminderTimers = new Map();
const REMINDER_KIND_TEXT = 'text';
const REMINDER_KIND_SOUNDBOARD = 'soundboard_sound';
const stateStore = await loadStateStore();
let runtimeConfig = await loadRuntimeConfig();
let runtimeConfigMtime = 0;
let stateStoreMtime = 0;
let saveStoreQueue = Promise.resolve();
let saveRuntimeConfigQueue = Promise.resolve();
let groqClient = null;
let groqClientKey = '';
let groqModelDiscoveryRunning = false;
let monitorChannel = null;
let lastBotEnabled = runtimeConfig.botEnabled !== false;
let autoJoinInProgress = false;
let autoJoinSuppressedUntilManualJoin = false;
let healthcheckInProgress = false;
let panelCommandOffset = 0;
let panelCommandPollInProgress = false;
let statusSnapshotTimer = null;
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
    stateStore.guilds[key] = { memories: [], userMemories: {}, userProfiles: {}, reminders: [] };
  }
  const guildState = stateStore.guilds[key];
  if (!Array.isArray(guildState.memories)) guildState.memories = [];
  if (!guildState.userMemories || typeof guildState.userMemories !== 'object') guildState.userMemories = {};
  if (!guildState.userProfiles || typeof guildState.userProfiles !== 'object') guildState.userProfiles = {};
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
  if (!guildState.userProfiles || typeof guildState.userProfiles !== 'object') guildState.userProfiles = {};
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
  const backupTarget = splitBackupTargetCredentials(DEFAULT_BACKUP_TARGET_PATH);
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
    presenceGreetingLastSeen: {},
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
    telegramInboundEnabled: DEFAULT_TELEGRAM_INBOUND_ENABLED,
    telegramInboundAllowedChatIds: DEFAULT_TELEGRAM_INBOUND_ALLOWED_CHAT_IDS,
    telegramInboundPlainForward: DEFAULT_TELEGRAM_INBOUND_PLAIN_FORWARD,
    telegramUpdateOffset: 0,
    telegramInboundLastAt: 0,
    telegramInboundLastError: '',
    telegramInboundLastErrorAt: 0,
    telegramKnownChats: [],
    backupEnabled: DEFAULT_BACKUP_ENABLED,
    backupTargetPath: backupTarget.targetPath,
    backupTargetUsername: process.env.BACKUP_TARGET_USERNAME?.trim() || backupTarget.username || '',
    backupTargetPassword: process.env.BACKUP_TARGET_PASSWORD?.trim() || backupTarget.password || '',
    backupIntervalHours: DEFAULT_BACKUP_INTERVAL_HOURS,
    backupRetention: DEFAULT_BACKUP_RETENTION,
    backupIdleOnly: DEFAULT_BACKUP_IDLE_ONLY,
    backupLastRunAt: 0,
    backupNextRunAt: 0,
    backupLastFile: '',
    backupLastTarget: '',
    backupLastError: '',
    backupLastErrorAt: 0,
    updatedAt: Date.now(),
  };
}

function normalizeRuntimeConfig(value = {}) {
  const defaults = defaultRuntimeConfig();
  const wakeWord = normalizeWakeWordValue(value.wakeWord, defaults.wakeWord);
  const backupTarget = splitBackupTargetCredentials(value.backupTargetPath ?? defaults.backupTargetPath);
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
    telegramInboundEnabled: value.telegramInboundEnabled === undefined ? defaults.telegramInboundEnabled : value.telegramInboundEnabled !== false,
    telegramInboundAllowedChatIds: String(value.telegramInboundAllowedChatIds ?? defaults.telegramInboundAllowedChatIds).trim().slice(0, 500),
    telegramInboundPlainForward: value.telegramInboundPlainForward === undefined ? defaults.telegramInboundPlainForward : value.telegramInboundPlainForward !== false,
    telegramUpdateOffset: Math.max(0, Number(value.telegramUpdateOffset || 0)),
    telegramInboundLastAt: Number(value.telegramInboundLastAt || 0),
    telegramInboundLastError: String(value.telegramInboundLastError || '').slice(0, 500),
    telegramInboundLastErrorAt: Number(value.telegramInboundLastErrorAt || 0),
    telegramKnownChats: normalizeTelegramKnownChats(value.telegramKnownChats ?? defaults.telegramKnownChats),
    backupEnabled: value.backupEnabled === undefined ? defaults.backupEnabled : value.backupEnabled === true,
    backupTargetPath: normalizeBackupTargetPath(backupTarget.targetPath || defaults.backupTargetPath).slice(0, 500),
    backupTargetUsername: String(value.backupTargetUsername || backupTarget.username || defaults.backupTargetUsername || '').trim().slice(0, 120),
    backupTargetPassword: String(value.backupTargetPassword || backupTarget.password || defaults.backupTargetPassword || '').trim().slice(0, 240),
    backupIntervalHours: Math.max(1, Math.min(720, Number(value.backupIntervalHours || defaults.backupIntervalHours))),
    backupRetention: Math.max(1, Math.min(20, Number(value.backupRetention || defaults.backupRetention))),
    backupIdleOnly: value.backupIdleOnly === undefined ? defaults.backupIdleOnly : value.backupIdleOnly !== false,
    backupLastRunAt: Number(value.backupLastRunAt || 0),
    backupNextRunAt: Number(value.backupNextRunAt || 0),
    backupLastFile: String(value.backupLastFile || '').slice(0, 255),
    backupLastTarget: String(value.backupLastTarget || '').slice(0, 500),
    backupLastError: String(value.backupLastError || '').slice(0, 500),
    backupLastErrorAt: Number(value.backupLastErrorAt || 0),
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

function groqMessagesSize(messages = []) {
  return messages.reduce((sum, item) => sum + String(item?.content || '').length, 0);
}

async function createGroqChatCompletion(request, { queue = 'ai', label = 'chat', session = null, model = request?.model } = {}) {
  return await runQueuedTask(
    queue,
    `${label}:${model || 'unknown'}`,
    () => getGroqClient().chat.completions.create(request).withResponse(),
    queueMetaForSession(session, {
      model: model || request?.model || 'unknown',
      label,
      maxCompletionTokens: request?.max_completion_tokens || null,
      promptChars: groqMessagesSize(request?.messages),
    }),
  );
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

function isTelegramInboundEnabled() {
  return runtimeConfig.telegramInboundEnabled !== false;
}

function isTelegramInboundPlainForwardEnabled() {
  return runtimeConfig.telegramInboundPlainForward !== false;
}

function getTelegramInboundAllowedChatIds() {
  const configured = [
    runtimeConfig.telegramInboundAllowedChatIds,
    DEFAULT_TELEGRAM_INBOUND_ALLOWED_CHAT_IDS,
  ].filter(Boolean).join(',');
  const ids = configured
    .split(',')
    .map((item) => normalizeTelegramChatId(item))
    .filter(Boolean);
  const defaultChatId = getTelegramDefaultChatId();
  if (defaultChatId) ids.push(defaultChatId);
  return [...new Set(ids)];
}

function isBackupEnabled() {
  return runtimeConfig.backupEnabled === true;
}

function getBackupTargetPath() {
  return normalizeBackupTargetPath(runtimeConfig.backupTargetPath || DEFAULT_BACKUP_TARGET_PATH);
}

function getBackupTargetUsername() {
  return String(runtimeConfig.backupTargetUsername || '').trim();
}

function getBackupTargetPassword() {
  return String(runtimeConfig.backupTargetPassword || '').trim();
}

function getBackupIntervalHours() {
  return Math.max(1, Math.min(720, Number(runtimeConfig.backupIntervalHours || DEFAULT_BACKUP_INTERVAL_HOURS)));
}

function getBackupRetention() {
  return Math.max(1, Math.min(20, Number(runtimeConfig.backupRetention || DEFAULT_BACKUP_RETENTION)));
}

function isBackupIdleOnly() {
  return runtimeConfig.backupIdleOnly !== false;
}

function backupNextRunAt() {
  const lastRunAt = Number(runtimeConfig.backupLastRunAt || 0);
  if (!lastRunAt) return 0;
  return lastRunAt + getBackupIntervalHours() * 60 * 60_000;
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
  const raw = value === undefined || value === null || String(value).trim() === '' ? fallback : value;
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

function safeDiscordContent(content, fallback = 'Не получил текст для отправки.') {
  const text = String(content ?? '').trim();
  return text || fallback;
}

async function sendText(channel, content) {
  try {
    if (!channel?.send) return null;
    return await channel.send(silentOptions(safeDiscordContent(content)));
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
  const safeContent = safeDiscordContent(content, 'Команда выполнена, но текст ответа пустой.');
  const payload = silentOptions(safeContent, extra);
  try {
    if (interaction.deferred || interaction.replied) {
      return await interaction.editReply(payload);
    }
    return await interaction.reply(payload);
  } catch (error) {
    console.error('interaction reply failed:', error);
    if (interaction.channel) return sendText(interaction.channel, safeContent);
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

function groqModelId(model) {
  return String(model?.id || model || '').trim();
}

function groqModelNumber(id, suffix = 'b') {
  const matches = [...String(id || '').toLowerCase().matchAll(/(\d+(?:\.\d+)?)\s*([bm])/gu)];
  const values = matches
    .filter((match) => match[2] === suffix)
    .map((match) => Number(match[1]))
    .filter(Number.isFinite);
  return values.length ? Math.max(...values) : 0;
}

function isGroqSttModelInfo(model) {
  const id = groqModelId(model).toLowerCase();
  return /whisper|transcrib|speech-to-text|asr/u.test(id);
}

function isGroqWebSearchModelInfo(model) {
  const id = groqModelId(model).toLowerCase();
  return /^groq\/compound/u.test(id);
}

function isGroqChatModelInfo(model) {
  const id = groqModelId(model).toLowerCase();
  if (!id || model?.active === false) return false;
  if (isGroqSttModelInfo(model) || isGroqWebSearchModelInfo(model)) return false;
  return !/(tts|speech|audio|voice|embed|embedding|moderation|guard|safeguard|prompt-guard|playai|orpheus|arabic|saudi|allam|distil-whisper)/u.test(id);
}

function scoreGroqChatModel(model) {
  const id = groqModelId(model).toLowerCase();
  const paramsB = groqModelNumber(id, 'b');
  let score = 0;
  if (paramsB) score += Math.min(80, Math.log2(paramsB + 1) * 12);
  if (/llama-4/u.test(id)) score += 34;
  if (/llama-3\.3/u.test(id)) score += 28;
  if (/gpt-oss-120b/u.test(id)) score += 30;
  if (/qwen3|qwen-3/u.test(id)) score += 22;
  if (/deepseek|kimi|mistral-large|mixtral/u.test(id)) score += 18;
  if (/instruct/u.test(id)) score += 6;
  if (/versatile/u.test(id)) score += 5;
  if (/preview|beta|experimental/u.test(id)) score -= 4;
  if (/instant|mini|small|8b/u.test(id)) score -= 12;
  if (/20b/u.test(id)) score -= 3;
  const contextWindow = Number(model?.context_window || model?.contextWindow || 0);
  if (Number.isFinite(contextWindow) && contextWindow > 0) score += Math.min(12, Math.log2(contextWindow / 8192 + 1) * 4);
  return score;
}

function scoreGroqActionModel(model) {
  const id = groqModelId(model).toLowerCase();
  const paramsB = groqModelNumber(id, 'b');
  let score = scoreGroqChatModel(model);
  if (/instant|mini|small|8b|20b/u.test(id)) score += 18;
  if (paramsB > 32) score -= Math.min(60, (paramsB - 32) * 0.8);
  return score;
}

function scoreGroqSttModel(model) {
  const id = groqModelId(model).toLowerCase();
  let score = 0;
  if (/whisper/u.test(id)) score += 20;
  const version = /large-v(\d+)/u.exec(id)?.[1];
  if (version) score += Number(version) * 8;
  if (/large/u.test(id)) score += 8;
  if (/turbo/u.test(id)) score -= 3;
  return score;
}

function scoreGroqWebModel(model) {
  const id = groqModelId(model).toLowerCase();
  let score = 0;
  if (id === 'groq/compound') score += 30;
  if (id.startsWith('groq/compound')) score += 20;
  if (/mini/u.test(id)) score -= 8;
  return score;
}

function sortGroqModels(models, scoreFn, limit) {
  return models
    .map((model) => ({ model, id: groqModelId(model), score: scoreFn(model) }))
    .filter((item) => item.id && item.score > 0)
    .sort((left, right) => right.score - left.score || left.id.localeCompare(right.id))
    .map((item) => item.id)
    .slice(0, limit);
}

function groqModelDiscoveryStatus() {
  return {
    enabled: GROQ_MODEL_DISCOVERY_ENABLED,
    autoSelect: GROQ_AUTO_SELECT_DISCOVERED_MODELS,
    checkedAt: groqDiscoveredModels.checkedAt || null,
    nextCheckAt: groqDiscoveredModels.nextCheckAt || null,
    source: groqDiscoveredModels.source,
    error: groqDiscoveredModels.error,
    chat: groqDiscoveredModels.chat,
    action: groqDiscoveredModels.action,
    stt: groqDiscoveredModels.stt,
    web: groqDiscoveredModels.web,
    modelCount: groqDiscoveredModels.modelInfo.length,
  };
}

async function refreshGroqModelDiscovery({ force = false, reason = 'timer' } = {}) {
  if (!GROQ_MODEL_DISCOVERY_ENABLED) return groqModelDiscoveryStatus();
  const now = Date.now();
  if (!force && groqDiscoveredModels.nextCheckAt && now < groqDiscoveredModels.nextCheckAt) {
    return groqModelDiscoveryStatus();
  }
  if (groqModelDiscoveryRunning) return groqModelDiscoveryStatus();

  groqModelDiscoveryRunning = true;
  try {
    const models = await getGroqClient().models.list();
    const active = (models.data || [])
      .filter((model) => model?.active !== false && groqModelId(model))
      .sort((left, right) => groqModelId(left).localeCompare(groqModelId(right)));

    const chat = sortGroqModels(active.filter(isGroqChatModelInfo), scoreGroqChatModel, GROQ_DISCOVERED_CHAT_LIMIT);
    const action = sortGroqModels(active.filter(isGroqChatModelInfo), scoreGroqActionModel, GROQ_DISCOVERED_ACTION_LIMIT);
    const stt = sortGroqModels(active.filter(isGroqSttModelInfo), scoreGroqSttModel, GROQ_DISCOVERED_STT_LIMIT);
    const web = sortGroqModels(active.filter(isGroqWebSearchModelInfo), scoreGroqWebModel, GROQ_DISCOVERED_WEB_LIMIT);
    const previousTopChat = groqDiscoveredModels.chat[0] || '';

    groqDiscoveredModels.checkedAt = now;
    groqDiscoveredModels.nextCheckAt = now + GROQ_MODEL_DISCOVERY_INTERVAL_MS;
    groqDiscoveredModels.source = reason;
    groqDiscoveredModels.error = '';
    groqDiscoveredModels.chat = chat;
    groqDiscoveredModels.action = action;
    groqDiscoveredModels.stt = stt;
    groqDiscoveredModels.web = web;
    groqDiscoveredModels.modelInfo = active.map((model) => ({
      id: groqModelId(model),
      ownedBy: model.owned_by || model.ownedBy || '',
      contextWindow: model.context_window || model.contextWindow || null,
      maxCompletionTokens: model.max_completion_tokens || model.maxCompletionTokens || null,
    }));

    if (chat[0] && chat[0] !== previousTopChat) {
      appendEvent('groq_model_discovery_top_changed', { previousTopChat, topChat: chat[0], reason });
      void sendMonitorNotice(`Groq models: нашел лучший chat model: ${chat[0]}. Добавил в auto-fallback.`).catch(() => {});
    }
    console.log(`Groq model discovery updated: chat=${chat.slice(0, 4).join(', ') || 'none'} stt=${stt.join(', ') || 'none'} web=${web.join(', ') || 'none'}`);
    await writeStatusSnapshot();
  } catch (error) {
    groqDiscoveredModels.checkedAt = Date.now();
    groqDiscoveredModels.nextCheckAt = Date.now() + Math.min(GROQ_MODEL_DISCOVERY_INTERVAL_MS, 6 * 60 * 60_000);
    groqDiscoveredModels.source = reason;
    groqDiscoveredModels.error = error?.message || String(error);
    console.warn('Groq model discovery failed:', error?.message || error);
    await writeStatusSnapshot();
  } finally {
    groqModelDiscoveryRunning = false;
  }
  return groqModelDiscoveryStatus();
}

function groqModelsToTry(primary, fallbackModels = [], discoveredModels = [], options = {}) {
  const preferDiscovered = options.preferDiscovered ?? GROQ_AUTO_SELECT_DISCOVERED_MODELS;
  const ordered = GROQ_AUTO_MODEL_FALLBACK
    ? parseCsvList([
      ...(preferDiscovered ? discoveredModels : []),
      primary,
      ...fallbackModels,
      ...(preferDiscovered ? [] : discoveredModels),
    ].filter(Boolean).join(','))
    : parseCsvList(primary);
  const available = ordered.filter((model) => !isGroqModelOnCooldown(model));
  return available.length ? available : ordered.slice(0, 1);
}

function chatModelsToTry(preferredModel = getChatModel()) {
  return groqModelsToTry(preferredModel, GROQ_CHAT_FALLBACK_MODELS, groqDiscoveredModels.chat, { preferDiscovered: true });
}

function actionModelsToTry(preferredModel = getActionParserModel()) {
  return groqModelsToTry(preferredModel, GROQ_ACTION_FALLBACK_MODELS, groqDiscoveredModels.action, { preferDiscovered: false });
}

function sttModelsToTry(preferredModel = getSttModel()) {
  return groqModelsToTry(preferredModel, GROQ_STT_FALLBACK_MODELS, groqDiscoveredModels.stt, { preferDiscovered: true });
}

function webSearchModelsToTry(preferredModel = getWebSearchModel()) {
  return groqModelsToTry(preferredModel, GROQ_WEB_FALLBACK_MODELS, groqDiscoveredModels.web, { preferDiscovered: true });
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

async function maybeAlertGroqLimit(channel, label, metric, limit, remaining, reset, dedupeKey = `${label}:${metric}`) {
  if (!Number.isFinite(limit) || !Number.isFinite(remaining) || limit <= 0) return;

  const percent = remaining / limit * 100;
  const threshold = API_LIMIT_ALERT_THRESHOLDS
    .filter((item) => percent <= item)
    .at(-1);
  const key = dedupeKey;
  const current = groqLimitAlertState.get(key) || { threshold: null, remaining: null };

  if (percent >= API_LIMIT_ALERT_RESET_PERCENT) {
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
    const alertKey = `groq:${model}:${metric.name}`;
    void maybeAlertGroqLimit(channel || monitorChannel, model, metric.name, metric.limit, metric.remaining, metric.reset, alertKey)
      .catch((error) => console.error('Groq limit alert failed:', error));
  }
}

function formatGroqLimits() {
  const cooldownLines = [...groqModelCooldowns.entries()]
    .filter(([model]) => isGroqModelOnCooldown(model))
    .map(([model, item]) => `${model}: временно пропускаю до ${new Date(item.until).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}, причина=${item.label}`);
  const discovery = groqModelDiscoveryStatus();
  const discoveryLines = discovery.enabled
    ? [
      `Model discovery: checked=${discovery.checkedAt ? new Date(discovery.checkedAt).toLocaleString('ru-RU') : 'not yet'}, next=${discovery.nextCheckAt ? new Date(discovery.nextCheckAt).toLocaleString('ru-RU') : 'unknown'}, models=${discovery.modelCount}, error=${discovery.error || 'none'}`,
      discovery.chat.length ? `Discovered chat priority: ${discovery.chat.join(', ')}` : '',
      discovery.stt.length ? `Discovered STT priority: ${discovery.stt.join(', ')}` : '',
      discovery.web.length ? `Discovered web priority: ${discovery.web.join(', ')}` : '',
    ].filter(Boolean)
    : ['Model discovery: disabled'];
  if (!groqLastLimits.size && !cooldownLines.length) {
    return [
      'Пока нет данных по лимитам Groq. Они появятся после первого запроса к STT или chat model.',
      ...discoveryLines,
    ].join('\n');
  }

  const limitLines = [...groqLastLimits.values()]
    .map((metric) => {
      const percent = metric.limit > 0 ? metric.remaining / metric.limit * 100 : NaN;
      const checked = new Date(metric.checkedAt).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
      return `${metric.model || 'unknown'} ${metric.name}: ${metric.remaining}/${metric.limit} (${formatPercent(percent)}%), reset=${metric.reset || 'unknown'}, source=${metric.label}, checked=${checked}`;
    });
  return [...limitLines, ...cooldownLines, ...discoveryLines].join('\n');
}

function formatSessionStatus(session) {
  if (!session?.connection) return 'Не подключен к voice channel.';
  const diag = session.diagnostics || createVoiceDiagnostics();
  const idleSeconds = session.lastHumanActivityAt ? Math.round((Date.now() - session.lastHumanActivityAt) / 1000) : 0;
  const assistantIdleSeconds = Math.round((Date.now() - (session.lastAssistantInteractionAt || session.joinedAt || Date.now())) / 1000);
  const activeLeft = session.activeDialogueUntil ? Math.max(0, Math.round((session.activeDialogueUntil - Date.now()) / 1000)) : 0;
  const wakeListenLeft = session.wakeListenUntil ? Math.max(0, Math.round((session.wakeListenUntil - Date.now()) / 1000)) : 0;
  const lastStt = diag.lastSttStats
    ? `${diag.lastSttStats.success ? 'ok' : 'fail'}:${diag.lastSttStats.attempts || 0} tries/${diag.lastSttStats.durationMs || 0}ms/transient=${diag.lastSttStats.transientErrors || 0}`
    : 'none';
  const queues = taskQueueSnapshot();
  const queueText = Object.values(queues)
    .filter((queue) => queue.active || queue.pending || queue.lastError)
    .map((queue) => `${queue.name}:${queue.active}/${queue.pending}${queue.lastError ? ` error=${queue.lastError}` : ''}`)
    .join(', ') || 'idle';
  return `Voice: ${session.voiceChannel?.name || 'unknown'}, state=${session.connection.state.status}, assistant=${getAssistantName()}, trigger="${getWakeWord() || 'off'}", enabled=${isBotEnabled()}, paused=${isListeningPaused(session)}, persona=${getAssistantPersona()}, wakeAck=${Boolean(session.wakeAckInProgress)}, wakeListen=${wakeListenLeft}s, wakeListenUser=${session.wakeListenUserId || 'none'}, activeDialogue=${activeLeft}s, webSearch=${isWebSearchEnabled()}, idleChatter=${isIdleChatterEnabled()} every ${getIdleChatterMinutes()}m style=${getIdleChatterStyle()} web=${isIdleChatterWebEnabled()}, idleLeave=${isIdleLeaveEnabled()} after ${getIdleLeaveMinutes()}m, humanIdle=${idleSeconds}s, assistantIdle=${assistantIdleSeconds}s, busy=${Boolean(session.busy)}, activeCaptures=${session.activeUsers?.size || 0}, queues=${queueText}, history=${session.history?.length || 0}, voiceEvents=${diag.voiceEvents}, captures=${diag.captures}, ignored=${diag.ignored}, sttRequests=${diag.sttRequests || 0}, sttTransient=${diag.sttTransientErrors || 0}, lastStt=${lastStt}, lastIgnored=${diag.lastIgnoredReason || 'none'}, lastTranscript=${diag.lastTranscript || 'none'}.`;
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
  'здаро', 'здарова', 'здорово', 'здрасьте', 'здравствуйте',
  'верно', 'правильно',
]);

const ZERO_WAKE_FALSE_POSITIVE_PHRASES = [
  /^все\s+верно$/u,
  /^все\s+правильно$/u,
  /^вот\s+так$/u,
  /^здаро(?:ва)?$/u,
  /^здорово$/u,
  /^здравствуйте$/u,
];

function isWakeFalsePositiveTranscript(text) {
  const normalizedWake = normalizeCommandText(getWakeWord());
  if (!(normalizedWake === 'зеро' || normalizedWake === 'zero')) return false;
  const normalized = normalizeCommandText(text);
  if (!normalized) return false;
  return ZERO_WAKE_FALSE_POSITIVE_PHRASES.some((pattern) => pattern.test(normalized));
}

function hasMentionOnlyWakeContext(rawText, index) {
  const before = String(rawText || '').slice(0, Math.max(0, index));
  const currentPhrase = normalizeCommandText(before.split(/[.!?;:,\n]/u).pop() || '');
  if (!currentPhrase) return false;
  return /(?:что|шо|чего|зачем|почему|кто|как)\s+.{0,40}\s(?:от|про|о|об)$/u.test(currentPhrase)
    || /(?:ты|вы|он|она|они)\s+(?:от|про|о|об)$/u.test(currentPhrase)
    || /(?:от|про|о|об)$/u.test(currentPhrase) && currentPhrase.split(/\s+/u).length >= 3;
}

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
  if (isWakeFalsePositiveTranscript(rawText)) return null;

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
    if (hasMentionOnlyWakeContext(rawText, candidate.index)) continue;
    if (!isStrongWakeTerm(term) && !wakeHasAddressContext(rawText, candidate.index)) continue;
    if (!best || candidate.index < best.index) best = candidate;
  }
  if (best) return best;

  const tokenPattern = /[\p{L}\p{N}_-]{1,20}/giu;
  let scanned = 0;
  for (const match of rawText.matchAll(tokenPattern)) {
    scanned += 1;
    const token = normalizeCommandText(match[0]);
    if (hasMentionOnlyWakeContext(rawText, match.index || 0)) continue;
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

function isWakeListenForOtherUser(session, userId, at = Date.now()) {
  return Boolean(
    session?.wakeListenUntil
      && at <= session.wakeListenUntil
      && session.wakeListenUserId
      && userId
      && String(session.wakeListenUserId) !== String(userId),
  );
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
  return hasWakeWord(text)
    || isWakeListenWindow(session, at, userId)
    || isActiveDialogue(session)
    || isNoWakeMusicControl(text, session);
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
    /^продолжение\s+следует$/u,
    /^спасибо\s+за\s+просмотр$/u,
    /^спасибо$/u,
    /^пока$/u,
    /^субтитры\s+(?:сделал|сделала|сделали|создал|создала|создали)\s+.+/u,
    /^приятного\s+просмотра$/u,
  ].some((pattern) => pattern.test(normalized));
}

function isSttPromptEchoTranscript(transcript) {
  const normalized = normalizeCommandText(transcript);
  if (!normalized) return false;
  return [
    /^mixed language$/u,
    /^речь\s+только\s+на\s+русском/u,
    /^основная\s+речь\s+на\s+русском/u,
    /^русская\s+и\s+английская\s+речь/u,
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
const MAX_LATIN_ONLY_TOKENS = 3;

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

  if (tokens.length > MAX_LATIN_ONLY_TOKENS) return 'language_guard_latin_only_long';
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

const SEMANTIC_STOP_TOKENS = new Set([
  'а', 'без', 'бы', 'был', 'была', 'были', 'быть', 'в', 'вам', 'вас', 'весь',
  'во', 'вот', 'все', 'всех', 'где', 'да', 'для', 'до', 'его', 'ее', 'еще',
  'за', 'зачем', 'и', 'из', 'или', 'как', 'какой', 'когда', 'которое', 'которые',
  'который', 'которую', 'кто', 'ли', 'мне', 'мной', 'мои', 'мой', 'на', 'над',
  'надо', 'нам', 'нас', 'не', 'него', 'нее', 'нет', 'но', 'ну', 'о', 'об', 'ок',
  'он', 'она', 'они', 'оно', 'от', 'по', 'под', 'пока', 'после', 'потом', 'почему',
  'при', 'про', 'с', 'со', 'там', 'тебе', 'тебя', 'то', 'тобой', 'тоже', 'только',
  'ты', 'у', 'уже', 'чем', 'что', 'чтоб', 'чтобы', 'это', 'этот', 'эту', 'я',
  'bot', 'find', 'for', 'in', 'me', 'my', 'note', 'notes', 'of', 'on', 'please',
  'remember', 'show', 'the', 'to', 'what',
  'бот', 'зеро', 'zero', 'ассистент', 'память', 'памяти', 'памят', 'заметка',
  'заметки', 'заметку', 'запись', 'записи', 'напоминание', 'напоминания',
  'напоминалка', 'напоминалки', 'удали', 'удалить', 'убери', 'отмени', 'покажи',
  'найди', 'поищи', 'выведи', 'забудь', 'помнишь', 'знаешь', 'говорил', 'говорила',
  'просил', 'просила', 'записывал', 'записывала', 'сохранял', 'сохраняла',
  'сегодня', 'сегодняшний', 'сегодняшнее', 'вчера', 'вчерашний', 'вчерашнее',
  'завтра', 'завтрашний', 'завтрашнее', 'позавчера', 'неделя', 'неделю', 'неделе',
]);

const SEMANTIC_TOPIC_GROUPS = [
  [
    'сервер', 'сервак', 'server', 'host', 'hosting', 'хост', 'хостинг',
    'vps', 'vds', 'впс', 'вдс', 'linux', 'линукс', 'ubuntu', 'debian',
    'ssh', 'deploy', 'deployment', 'деплой', 'деплоить', 'развернуть',
    'docker', 'докер', 'compose', 'docker-compose', 'контейнер', 'container',
    'контейнеры', 'volume', 'volumes', 'том', 'тома', 'mysql', 'mariadb',
    'database', 'db', 'база', 'бд', 'backup', 'backups', 'бекап', 'бэкап',
    'резерв', 'резервная', 'копия', 'restore', 'восстановление', 'git',
    'github', 'репозиторий', 'repo', 'панель', 'panel',
  ],
  [
    'backup', 'backups', 'бекап', 'бекапы', 'бэкап', 'бэкапы', 'резерв',
    'резервная', 'резервные', 'копия', 'копии', 'архив', 'restore',
    'восстановить', 'восстановление', 'smb', 'ftp', 'nas', 'storage',
    'хранилище', 'папка', 'диск',
  ],
  [
    'telegram', 'телеграм', 'телега', 'тг', 'tg', 'telega', 'чат', 'chat',
    'бот', 'bot', 'сообщение', 'заметка',
  ],
  [
    'discord', 'дискорд', 'guild', 'voice', 'войс',
    'канал', 'channel', 'role', 'роль', 'права', 'permissions', 'иерархия',
    'mute', 'мьют', 'микрофон', 'stream', 'стрим', 'трансляция',
  ],
  [
    'voice', 'войс', 'голос', 'голосовой', 'микрофон', 'слушать', 'слышать',
    'stt', 'whisper', 'tts', 'голос', 'озвучка', 'триггер', 'wake', 'wakeword',
  ],
  [
    'music', 'музыка', 'песня', 'трек', 'радио', 'lofi', 'youtube', 'ютуб',
    'spotify', 'спотифай', 'yt-dlp', 'плеер', 'очередь', 'volume', 'громкость',
  ],
  [
    'api', 'апи', 'groq', 'grok', 'грок', 'nvidia', 'модель', 'models',
    'model', 'лимит', 'лимиты', 'quota', 'rate', 'token', 'tokens', 'токен',
    'токены', 'fallback', 'whisper',
  ],
  [
    'панель', 'panel', 'web', 'веб', 'dashboard', 'статус', 'настройки',
    'settings', 'кнопка', 'вкладка', 'интерфейс', 'ui',
  ],
  [
    'маршрут', 'route', 'путь', 'дорога', 'поездка', 'доставка', 'rovex',
    'ровекс', 'логистика',
  ],
  [
    'погода', 'weather', 'температура', 'дождь', 'снег', 'ветер', 'чернигов',
    'chernihiv', 'чернигове',
  ],
];

let semanticTopicTokenGroupsCache = null;

function stripSemanticEnding(token) {
  const variants = new Set([token]);
  const normalized = String(token || '');
  if (normalized.length < 4) return [...variants];
  const cyrEndings = [
    'иями', 'ями', 'ами', 'ого', 'ему', 'ому', 'ыми', 'ими', 'ная', 'ное', 'ные',
    'ний', 'его', 'ать', 'ять', 'ить', 'ться', 'ешь', 'ете', 'али',
    'или', 'ах', 'ях', 'ов', 'ев', 'ой', 'ей', 'ом', 'ем', 'ам',
    'ям', 'ую', 'юю', 'ая', 'яя', 'ое', 'ее', 'ые', 'ие', 'ый', 'ий', 'а', 'у',
    'е', 'ы', 'и', 'ю', 'я',
  ];
  const latEndings = [
    'tion', 'sion', 'ing', 'ers', 'ies', 'ied', 'ed', 'es', 's', 'er', 'or',
  ];
  const endings = /[\p{Script=Cyrillic}]/u.test(normalized) ? cyrEndings : latEndings;
  for (const ending of endings) {
    if (!normalized.endsWith(ending)) continue;
    const stripped = normalized.slice(0, -ending.length);
    if (stripped.length >= 3) variants.add(stripped);
  }
  return [...variants];
}

function semanticTokenVariants(token) {
  const variants = new Set();
  const normalized = normalizeCommandText(token).replace(/[_-]+/g, ' ').trim();
  for (const part of normalized.split(' ').filter(Boolean)) {
    if (part.length < 2) continue;
    variants.add(part);
    variants.add(collapseRepeatedLetters(part));
    for (const stripped of stripSemanticEnding(part)) variants.add(stripped);
    const latin = normalizeCommandText(transliterateCyrillicToLatin(part));
    if (latin && latin !== part) {
      variants.add(latin);
      variants.add(collapseRepeatedLetters(latin));
      for (const stripped of stripSemanticEnding(latin)) variants.add(stripped);
    }
    const cyrillic = normalizeCommandText(transliterateLatinToCyrillic(part));
    if (cyrillic && cyrillic !== part) {
      variants.add(cyrillic);
      variants.add(collapseRepeatedLetters(cyrillic));
      for (const stripped of stripSemanticEnding(cyrillic)) variants.add(stripped);
    }
  }
  return [...variants].filter((item) => item.length >= 2 && !SEMANTIC_STOP_TOKENS.has(item));
}

function semanticTokens(text) {
  const tokens = new Set();
  for (const token of normalizeCommandText(text).split(' ').filter(Boolean)) {
    if (SEMANTIC_STOP_TOKENS.has(token)) continue;
    for (const variant of semanticTokenVariants(token)) tokens.add(variant);
  }
  return tokens;
}

function semanticTopicTokenGroups() {
  if (!semanticTopicTokenGroupsCache) {
    semanticTopicTokenGroupsCache = SEMANTIC_TOPIC_GROUPS.map((group) => {
      const tokens = new Set();
      for (const term of group) {
        for (const variant of semanticTokenVariants(term)) tokens.add(variant);
      }
      return tokens;
    });
  }
  return semanticTopicTokenGroupsCache;
}

function semanticGroupIndexes(tokens) {
  const indexes = new Set();
  const groups = semanticTopicTokenGroups();
  groups.forEach((group, index) => {
    for (const token of tokens) {
      if (group.has(token)) {
        indexes.add(index);
        return;
      }
    }
  });
  return indexes;
}

function semanticExpandedTokens(tokens) {
  const expanded = new Set(tokens);
  const groups = semanticTopicTokenGroups();
  for (const index of semanticGroupIndexes(tokens)) {
    for (const token of groups[index]) expanded.add(token);
  }
  return expanded;
}

function semanticTokenMatchScore(queryToken, textTokens, expandedTextTokens) {
  if (textTokens.has(queryToken)) return 1;
  if (expandedTextTokens.has(queryToken)) return 0.82;
  if (queryToken.length < 4) return 0;
  let best = 0;
  for (const textToken of textTokens) {
    if (textToken.length < 4) continue;
    if (textToken.includes(queryToken) || queryToken.includes(textToken)) return 0.72;
    const distance = levenshteinDistance(queryToken, textToken);
    const similarityScore = 1 - distance / Math.max(queryToken.length, textToken.length);
    if (similarityScore >= 0.78) best = Math.max(best, similarityScore * 0.62);
  }
  return best;
}

function semanticSearchScore(query, text) {
  const queryTokens = semanticTokens(query);
  if (!queryTokens.size) return 0;
  const textTokens = semanticTokens(text);
  if (!textTokens.size) return 0;

  const expandedQueryTokens = semanticExpandedTokens(queryTokens);
  const expandedTextTokens = semanticExpandedTokens(textTokens);
  let hits = 0;
  for (const token of queryTokens) {
    hits += semanticTokenMatchScore(token, textTokens, expandedTextTokens);
  }
  const tokenScore = hits / Math.max(1, queryTokens.size);

  const queryGroups = semanticGroupIndexes(queryTokens);
  const textGroups = semanticGroupIndexes(textTokens);
  let sharedGroups = 0;
  for (const group of queryGroups) {
    if (textGroups.has(group)) sharedGroups += 1;
  }
  const groupScore = queryGroups.size ? Math.min(1, sharedGroups / queryGroups.size) * 0.72 : 0;

  let expandedHits = 0;
  for (const token of expandedQueryTokens) {
    if (expandedTextTokens.has(token)) expandedHits += 1;
  }
  const expandedScore = expandedQueryTokens.size ? Math.min(1, expandedHits / expandedQueryTokens.size) * 0.55 : 0;
  const normalizedQuery = normalizeCommandText(query);
  const normalizedText = normalizeCommandText(text);
  const phraseScore = normalizedQuery.length >= 4 && normalizedText.includes(normalizedQuery) ? 1 : 0;

  return Math.max(tokenScore, groupScore, expandedScore, phraseScore);
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

const USER_PROFILE_ARRAY_FIELDS = new Set(['favoriteTopics', 'frequentTasks', 'habitualCommands', 'personalNotes']);

const USER_PROFILE_FIELD_LABELS = {
  preferredName: 'как обращаться',
  favoriteTopics: 'любимые темы',
  communicationStyle: 'стиль общения',
  frequentTasks: 'частые задачи',
  timezone: 'часовой пояс',
  habitualCommands: 'привычные команды',
  personalNotes: 'персональные заметки',
  jokeTone: 'шутки и тон',
};

const USER_PROFILE_FIELD_ALIASES = new Map(Object.entries({
  name: 'preferredName',
  nickname: 'preferredName',
  preferred_name: 'preferredName',
  preferredname: 'preferredName',
  обращение: 'preferredName',
  имя: 'preferredName',
  favorite_topics: 'favoriteTopics',
  favoritetopics: 'favoriteTopics',
  topics: 'favoriteTopics',
  темы: 'favoriteTopics',
  интересы: 'favoriteTopics',
  style: 'communicationStyle',
  communication_style: 'communicationStyle',
  communicationstyle: 'communicationStyle',
  стиль: 'communicationStyle',
  frequent_tasks: 'frequentTasks',
  frequenttasks: 'frequentTasks',
  tasks: 'frequentTasks',
  задачи: 'frequentTasks',
  time_zone: 'timezone',
  timezone: 'timezone',
  tz: 'timezone',
  habitual_commands: 'habitualCommands',
  habitualcommands: 'habitualCommands',
  commands: 'habitualCommands',
  команды: 'habitualCommands',
  personal_notes: 'personalNotes',
  personalnotes: 'personalNotes',
  notes: 'personalNotes',
  заметки: 'personalNotes',
  joke_tone: 'jokeTone',
  joketone: 'jokeTone',
  jokes: 'jokeTone',
  шутки: 'jokeTone',
  тон: 'jokeTone',
}));

function normalizeProfileFieldName(field) {
  const raw = String(field || '').trim();
  if (raw in USER_PROFILE_FIELD_LABELS) return raw;
  const normalized = normalizeCommandText(raw).replace(/\s+/g, '_');
  return USER_PROFILE_FIELD_ALIASES.get(normalized) || raw;
}

function userProfileBaseName(member) {
  return member?.displayName || member?.user?.globalName || member?.user?.username || '';
}

function normalizeProfileString(value, limit = 240) {
  const cleaned = sanitizeVoiceOutputText(stripMarkdownFormatting(value || ''))
    .replace(/\s+/g, ' ')
    .replace(/^[«"“”'`]+|[»"“”'`]+$/gu, '')
    .replace(/[.!?]+$/u, '')
    .trim();
  if (charLength(cleaned) <= limit) return cleaned;
  return [...cleaned].slice(0, limit).join('').replace(/\s+\S*$/u, '').trim();
}

function profileListItems(value, limit = 8) {
  const rawItems = Array.isArray(value)
    ? value
    : String(value || '')
      .split(/[,;]|\s+(?:и|and)\s+/giu);
  return [...new Set(rawItems
    .map((item) => normalizeProfileString(item, 120))
    .filter((item) => item.length >= 2))]
    .slice(0, limit);
}

function normalizeTimezonePreference(value) {
  const raw = normalizeProfileString(value, 80);
  if (!raw) return '';
  const normalized = normalizeCommandText(raw);
  const aliases = [
    { re: /^(?:киев|kyiv|kiev|украин|ukraine|eest|eet)$/u, zone: 'Europe/Kyiv' },
    { re: /^(?:герман|germany|berlin|берлин)$/u, zone: 'Europe/Berlin' },
    { re: /^(?:польш|poland|warsaw|варшав)$/u, zone: 'Europe/Warsaw' },
    { re: /^(?:москв|moscow|russia)$/u, zone: 'Europe/Moscow' },
    { re: /^(?:лондон|london|uk|британ|england)$/u, zone: 'Europe/London' },
    { re: /^(?:new york|нью йорк|сша|usa|america)$/u, zone: 'America/New_York' },
  ];
  for (const alias of aliases) {
    if (alias.re.test(normalized)) return alias.zone;
  }
  const candidate = raw.includes('/') ? raw : raw.replace(/\s+/g, '_');
  try {
    new Intl.DateTimeFormat('ru-RU', { timeZone: candidate }).format(new Date());
    return candidate;
  } catch {
    return raw;
  }
}

function normalizeUserProfile(profile = {}, member = null) {
  const now = Date.now();
  const normalized = {
    userId: String(profile.userId || member?.id || 'unknown'),
    userName: normalizeProfileString(profile.userName || userProfileBaseName(member), 120),
    preferredName: normalizeProfileString(profile.preferredName || '', 80),
    favoriteTopics: profileListItems(profile.favoriteTopics || []),
    communicationStyle: normalizeProfileString(profile.communicationStyle || '', 240),
    frequentTasks: profileListItems(profile.frequentTasks || []),
    timezone: normalizeTimezonePreference(profile.timezone || ''),
    habitualCommands: profileListItems(profile.habitualCommands || []),
    personalNotes: profileListItems(profile.personalNotes || [], 20),
    jokeTone: normalizeProfileString(profile.jokeTone || '', 240),
    createdAt: Number(profile.createdAt || now),
    updatedAt: Number(profile.updatedAt || now),
  };
  return normalized;
}

function getUserProfile(guildId, userId, member = null, { create = false } = {}) {
  if (!userId && !member?.id) return null;
  const guildState = getGuildState(guildId);
  const id = String(userId || member.id);
  const existing = guildState.userProfiles[id];
  if (!existing && !create) return null;
  const profile = normalizeUserProfile(existing || { userId: id }, member);
  if (create || existing) guildState.userProfiles[id] = profile;
  return profile;
}

function mergeProfileList(existing, next, limit = 20) {
  const result = [];
  const seen = new Set();
  for (const item of [...profileListItems(existing, limit), ...profileListItems(next, limit)]) {
    const key = normalizeCommandText(item);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    result.push(item);
    if (result.length >= limit) break;
  }
  return result;
}

function updateUserProfile(guildId, member, patch = {}, source = 'manual') {
  const guildState = getGuildState(guildId);
  const userId = String(member?.id || patch.userId || 'unknown');
  const profile = getUserProfile(guildId, userId, member, { create: true });
  profile.userName = normalizeProfileString(userProfileBaseName(member) || profile.userName, 120);
  for (const [field, value] of Object.entries(patch)) {
    if (value === undefined || value === null) continue;
    if (USER_PROFILE_ARRAY_FIELDS.has(field)) {
      profile[field] = mergeProfileList(profile[field], value, field === 'personalNotes' ? 30 : 20);
    } else if (field === 'timezone') {
      profile.timezone = normalizeTimezonePreference(value);
    } else if (field in USER_PROFILE_FIELD_LABELS) {
      profile[field] = normalizeProfileString(value, field === 'preferredName' ? 80 : 240);
    }
  }
  profile.updatedAt = Date.now();
  profile.source = source;
  guildState.userProfiles[userId] = normalizeUserProfile(profile, member);
  void saveStateStore();
  return guildState.userProfiles[userId];
}

function profilePreferredName(guildId, member) {
  const name = getUserProfile(guildId, member?.id, member)?.preferredName;
  return normalizeProfileString(name, 80) || '';
}

function formatUserProfile(profile, { emptyText = 'Профиль пока пустой.' } = {}) {
  const normalized = normalizeUserProfile(profile || {});
  const lines = [];
  if (normalized.preferredName) lines.push(`${USER_PROFILE_FIELD_LABELS.preferredName}: ${normalized.preferredName}`);
  if (normalized.timezone) lines.push(`${USER_PROFILE_FIELD_LABELS.timezone}: ${normalized.timezone}`);
  if (normalized.communicationStyle) lines.push(`${USER_PROFILE_FIELD_LABELS.communicationStyle}: ${normalized.communicationStyle}`);
  if (normalized.jokeTone) lines.push(`${USER_PROFILE_FIELD_LABELS.jokeTone}: ${normalized.jokeTone}`);
  if (normalized.favoriteTopics.length) lines.push(`${USER_PROFILE_FIELD_LABELS.favoriteTopics}: ${normalized.favoriteTopics.join(', ')}`);
  if (normalized.frequentTasks.length) lines.push(`${USER_PROFILE_FIELD_LABELS.frequentTasks}: ${normalized.frequentTasks.join(', ')}`);
  if (normalized.habitualCommands.length) lines.push(`${USER_PROFILE_FIELD_LABELS.habitualCommands}: ${normalized.habitualCommands.join(', ')}`);
  if (normalized.personalNotes.length) lines.push(`${USER_PROFILE_FIELD_LABELS.personalNotes}: ${normalized.personalNotes.join('; ')}`);
  if (!lines.length) return emptyText;
  const updated = normalized.updatedAt
    ? new Date(normalized.updatedAt).toLocaleString('ru-RU', { dateStyle: 'short', timeStyle: 'short' })
    : '';
  return [...lines, updated ? `обновлен: ${updated}` : ''].filter(Boolean).join('\n');
}

function formatUserProfileContext(guildId, member) {
  const profile = getUserProfile(guildId, member?.id, member);
  if (!profile) return '';
  const context = formatUserProfile(profile, { emptyText: '' });
  return context ? `Профиль пользователя ${profile.preferredName || profile.userName || member?.displayName || 'участник'}:\n${context}` : '';
}

function setProfileFieldFromText(field, text) {
  const normalizedField = normalizeProfileFieldName(field);
  if (!normalizedField || text === undefined || text === null) return null;
  if (USER_PROFILE_ARRAY_FIELDS.has(normalizedField)) return { [normalizedField]: profileListItems(text, normalizedField === 'personalNotes' ? 12 : 8) };
  if (normalizedField in USER_PROFILE_FIELD_LABELS) return { [normalizedField]: text };
  return null;
}

function parseUserProfileCommand(prompt) {
  const raw = String(prompt || '').replace(/\s+/g, ' ').trim();
  const normalized = normalizeCommandText(raw);
  if (!normalized) return null;

  if (/(профил|profile)/u.test(normalized) && /(покажи|выведи|что|какой|show|list)/u.test(normalized)) {
    return { action: 'show_user_profile' };
  }

  const nameMatch = raw.match(/^(?:называй|зови)\s+(?:меня|мне)\s+(?:как\s+|по\s+имени\s+|словом\s+)?(.+)$/iu)
    || raw.match(/^(?:обращайся\s+ко\s+мне|обращайся\s+к\s+мне)\s+(?:как\s+|по\s+имени\s+)?(.+)$/iu)
    || raw.match(/^(?:мой\s+профиль\s*[:,-]\s*)?(?:имя|обращение|как\s+меня\s+называть)\s*(?:это|:|-)?\s+(.+)$/iu);
  if (nameMatch?.[1]?.trim()) {
    return { action: 'update_user_profile', field: 'preferredName', text: cleanCallNameAlias(nameMatch[1]) };
  }

  const patterns = [
    { field: 'timezone', re: /^(?:запомни\s+)?(?:мой\s+)?(?:часовой\s+пояс|таймзон[ау]|timezone|time\s*zone)\s*(?:это|:|-)?\s+(.+)$/iu },
    { field: 'favoriteTopics', re: /^(?:запомни\s+)?(?:мои\s+)?(?:любимые\s+темы|интересы|темы\s+которые\s+мне\s+нравятся|favorite\s+topics)\s*(?:это|:|-)?\s+(.+)$/iu },
    { field: 'communicationStyle', re: /^(?:запомни\s+)?(?:мой\s+)?(?:стиль\s+общения|стиль\s+ответов|как\s+со\s+мной\s+общаться|communication\s+style)\s*(?:это|:|-)?\s+(.+)$/iu },
    { field: 'communicationStyle', re: /^(?:общайся\s+со\s+мной|отвечай\s+мне)\s+(.+)$/iu },
    { field: 'frequentTasks', re: /^(?:запомни\s+)?(?:мои\s+)?(?:частые\s+задачи|обычные\s+задачи|типовые\s+задачи|frequent\s+tasks)\s*(?:это|:|-)?\s+(.+)$/iu },
    { field: 'habitualCommands', re: /^(?:запомни\s+)?(?:мои\s+)?(?:привычные\s+команды|частые\s+команды|обычные\s+команды|habitual\s+commands)\s*(?:это|:|-)?\s+(.+)$/iu },
    { field: 'jokeTone', re: /^(?:запомни\s+)?(?:мои\s+)?(?:предпочтения\s+по\s+шуткам|стиль\s+шуток|тон\s+шуток|joke\s+tone)\s*(?:это|:|-)?\s+(.+)$/iu },
    { field: 'jokeTone', re: /^(?:шути\s+со\s+мной|шути\s+мне)\s+(.+)$/iu },
    { field: 'personalNotes', re: /^(?:запомни\s+)?(?:в\s+мой\s+профиль\s+)?(?:персональн\p{L}*\s+заметк\p{L}*|личн\p{L}*\s+заметк\p{L}*|заметк\p{L}*\s+в\s+профиль)\s*(?:что|это|:|-)?\s+(.+)$/iu },
    { field: 'personalNotes', re: /^(?:запомни|сохрани|запиши)\s+(?:в\s+мой\s+профиль|в\s+профиль)\s*(?:что|:)?\s+(.+)$/iu },
  ];
  for (const { field, re } of patterns) {
    const match = raw.match(re);
    if (!match?.[1]?.trim()) continue;
    return { action: 'update_user_profile', field, text: match[1].trim() };
  }

  return null;
}

function profilePatchFromPersonalMemory(text) {
  const parsed = parseUserProfileCommand(text);
  if (parsed?.action === 'update_user_profile') return setProfileFieldFromText(parsed.field, parsed.text);
  return null;
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
        result = await createGroqChatCompletion({
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
        }, {
          queue: 'ai',
          label: 'generate-memory-notes',
          session,
          model,
        });
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
    score: Math.max(
      scoreTextRelevance(memory.text, prompt),
      semanticSearchScore(prompt, memory.text) * 0.95,
    ) + index / Math.max(1, memories.length) * 0.05,
  }));
  scored.sort((a, b) => b.score - a.score);

  const relevant = scored.filter((item) => item.score > 0.08).slice(0, limit);
  if (relevant.length) return relevant.map((item) => item.memory);
  return memories.slice(-Math.min(limit, 5));
}

function relevantUserMemories(guildId, userId, prompt, limit = MEMORY_CONTEXT_LIMIT) {
  if (!limit || !userId) return [];
  const memories = [...(getGuildState(guildId).userMemories?.[userId] || [])];
  if (!memories.length) return [];

  const scored = memories.map((memory, index) => ({
    memory,
    score: Math.max(
      scoreTextRelevance(memory.text, prompt),
      semanticSearchScore(prompt, memory.text) * 0.95,
    ) + index / Math.max(1, memories.length) * 0.05,
  }));
  scored.sort((a, b) => b.score - a.score);

  const relevant = scored.filter((item) => item.score > 0.08).slice(0, limit);
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
  const cleanedQuery = cleanMemoryQuery(query) || query;
  const normalizedQuery = normalizeCommandText(cleanedQuery);
  if (!entries.length) return [];
  if (!normalizedQuery) return entries.map((entry, index) => ({ ...entry, score: 0.1, matchIndex: index }));

  const scored = entries.map((entry, matchIndex) => {
    const text = memorySearchText(entry);
    const textScore = scoreTextRelevance(text, normalizedQuery);
    const directTextScore = scoreTextRelevance(entry.memory?.text || '', normalizedQuery) * 0.9;
    const fuzzyTextScore = normalizedQuery.length >= 5
      ? similarity(entry.memory?.text || '', normalizedQuery) * 0.35
      : 0;
    const semanticScore = semanticSearchScore(cleanedQuery, text);
    const dateScore = memoryDateScore(entry.memory || {}, normalizedQuery);
    return {
      ...entry,
      matchIndex,
      textScore,
      semanticScore,
      dateScore,
      score: Math.max(textScore, directTextScore, fuzzyTextScore, semanticScore) + dateScore,
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
    ['два', 2], ['две', 2],
    ['три', 3], ['четыре', 4], ['пять', 5], ['шесть', 6], ['семь', 7],
    ['восемь', 8], ['девять', 9], ['десять', 10], ['пятнадцать', 15],
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
  if (/^(мин|min|minute)/.test(normalized)) return 60 * 1000;
  if (/^(час|hour|hr)/.test(normalized)) return 60 * 60 * 1000;
  if (/^(день|дня|днеи|дни|сут|day)/.test(normalized)) return 24 * 60 * 60 * 1000;
  return null;
}

function recurringUnitToMs(unit) {
  const normalized = normalizeCommandText(unit);
  if (/^час/.test(normalized)) return 60 * 60 * 1000;
  if (/^(день|дня|днеи|сут)/.test(normalized)) return 24 * 60 * 60 * 1000;
  if (/^(недел|week)/.test(normalized)) return 7 * 24 * 60 * 60 * 1000;
  if (/^(месяц|month)/.test(normalized)) return 30 * 24 * 60 * 60 * 1000;
  return unitToMs(unit);
}

function cleanReminderText(text) {
  return String(text || '')
    .replace(/^(?:что\s+|о том что\s+|про\s+|[:,-]\s*)/iu, '')
    .replace(/[.!?]+$/u, '')
    .trim();
}

const REMINDER_CREATE_PATTERN = '(?:напомни(?:ть)?|напоминай|напоминать|поставь\\s+напоминание|создай\\s+напоминание|добавь\\s+напоминание|сделай\\s+напоминание|запиши\\s+напоминание|напоминание|remind)';
const REMINDER_ME_PATTERN = '(?:\\s+(?:мне|меня|me))?';
const REMINDER_UNIT_PATTERN = '(?:секунд[уы]?|сек|seconds?|secs?|минут[уы]?|мин|minutes?|mins?|час(?:а|ов)?|год|hours?|hrs?|день|дня|дней|дни|сут(?:ки|ок)?|days?)';

const REMINDER_MONTHS = new Map([
  ['января', 0], ['январь', 0], ['january', 0], ['jan', 0],
  ['февраля', 1], ['февраль', 1], ['february', 1], ['feb', 1],
  ['марта', 2], ['март', 2], ['march', 2], ['mar', 2],
  ['апреля', 3], ['апрель', 3], ['april', 3], ['apr', 3],
  ['мая', 4], ['май', 4], ['may', 4],
  ['июня', 5], ['июнь', 5], ['june', 5], ['jun', 5],
  ['июля', 6], ['июль', 6], ['july', 6], ['jul', 6],
  ['августа', 7], ['август', 7], ['august', 7], ['aug', 7],
  ['сентября', 8], ['сентябрь', 8], ['september', 8], ['sep', 8],
  ['октября', 9], ['октябрь', 9], ['october', 9], ['oct', 9],
  ['ноября', 10], ['ноябрь', 10], ['november', 10], ['nov', 10],
  ['декабря', 11], ['декабрь', 11], ['december', 11], ['dec', 11],
]);

const REMINDER_WEEKDAYS = new Map([
  ['воскресенье', 0], ['воскресенья', 0], ['sunday', 0], ['sun', 0],
  ['понедельник', 1], ['понедельника', 1], ['monday', 1], ['mon', 1],
  ['вторник', 2], ['вторника', 2], ['tuesday', 2], ['tue', 2],
  ['среду', 3], ['среда', 3], ['среды', 3], ['wednesday', 3], ['wed', 3],
  ['четверг', 4], ['четверга', 4], ['thursday', 4], ['thu', 4],
  ['пятницу', 5], ['пятница', 5], ['пятницы', 5], ['friday', 5], ['fri', 5],
  ['субботу', 6], ['суббота', 6], ['субботы', 6], ['saturday', 6], ['sat', 6],
]);

function startOfLocalDay(timestamp = Date.now()) {
  const date = new Date(timestamp);
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

function addLocalDays(timestamp, days) {
  const date = startOfLocalDay(timestamp);
  date.setDate(date.getDate() + days);
  return date;
}

function parseTimeOfDay(text) {
  const raw = String(text || '').trim().replace(/^[-,:]\s*/u, '');
  const match = raw.match(/^(?:в|во|к|ко|на|at)?\s*(\d{1,2})(?:[:.](\d{1,2}))?\s*(.*)$/iu);
  if (!match) return null;

  let hour = Number(match[1]);
  const minute = match[2] === undefined ? 0 : Number(match[2]);
  if (!Number.isInteger(hour) || !Number.isInteger(minute) || minute < 0 || minute > 59) return null;

  let rest = String(match[3] || '').trim();
  rest = rest.replace(/^(?:час(?:а|ов)?|ч|hours?|hrs?)(?=$|\s|[.,:;!?-])\s*/iu, '').trim();
  const dayPartMatch = rest.match(/^(утра|утром|дня|днем|днём|вечера|вечером|ночи|ночью|am|pm)(?=$|\s|[.,:;!?-])\s*(.*)$/iu);
  const part = normalizeCommandText(dayPartMatch?.[1] || '');
  if (dayPartMatch) rest = dayPartMatch[2] || '';
  if (part === 'pm' || part === 'вечера' || part === 'вечером') {
    if (hour >= 1 && hour < 12) hour += 12;
  } else if (part === 'am') {
    if (hour === 12) hour = 0;
  } else if (part === 'ночи' || part === 'ночью') {
    if (hour === 12) hour = 0;
    else if (hour >= 8 && hour < 12) hour += 12;
  } else if (part === 'дня' || part === 'днем') {
    if (hour >= 1 && hour <= 7) hour += 12;
  }

  if (hour < 0 || hour > 23) return null;
  return { hour, minute, rest: cleanReminderText(rest) };
}

function parseReminderDatePrefix(tail, now = Date.now()) {
  const raw = String(tail || '').trim();
  const normalized = normalizeCommandText(raw);
  const relative = [
    { re: /^(?:сегодня|today)(?=$|\s|[.,:;!?-])/iu, days: 0 },
    { re: /^(?:завтра|tomorrow)(?=$|\s|[.,:;!?-])/iu, days: 1 },
    { re: /^(?:послезавтра|after\s+tomorrow|day\s+after\s+tomorrow)(?=$|\s|[.,:;!?-])/iu, days: 2 },
  ];
  for (const item of relative) {
    const match = raw.match(item.re);
    if (match) {
      return { date: addLocalDays(now, item.days), rest: raw.slice(match[0].length).trim() };
    }
  }

  const weekdayMatch = normalized.match(/^(?:в|во|на)?\s*([a-zа-яё]+)/u);
  const weekday = REMINDER_WEEKDAYS.get(weekdayMatch?.[1] || '');
  if (weekday !== undefined) {
    const today = startOfLocalDay(now);
    const diff = (weekday - today.getDay() + 7) % 7 || 7;
    const consumed = raw.match(/^(?:в|во|на)?\s*[a-zа-яё]+/iu)?.[0] || '';
    return { date: addLocalDays(now, diff), rest: raw.slice(consumed.length).trim() };
  }

  const numericDate = raw.match(/^(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?\s*(.*)$/u);
  if (numericDate) {
    const day = Number(numericDate[1]);
    const month = Number(numericDate[2]) - 1;
    let year = numericDate[3] ? Number(numericDate[3]) : new Date(now).getFullYear();
    if (year < 100) year += 2000;
    return { date: new Date(year, month, day), rest: numericDate[4].trim() };
  }

  const monthDate = raw.match(/^(\d{1,2})\s+([a-zа-яё]+)(?:\s+(\d{2,4}))?\s*(.*)$/iu);
  if (monthDate) {
    const month = REMINDER_MONTHS.get(normalizeCommandText(monthDate[2]));
    if (month !== undefined) {
      const day = Number(monthDate[1]);
      let year = monthDate[3] ? Number(monthDate[3]) : new Date(now).getFullYear();
      if (year < 100) year += 2000;
      return { date: new Date(year, month, day), rest: monthDate[4].trim() };
    }
  }

  return null;
}

function parseAbsoluteReminderTail(tail, now = Date.now()) {
  const datePrefix = parseReminderDatePrefix(tail, now);
  if (!datePrefix) return null;
  const time = parseTimeOfDay(datePrefix.rest);
  if (!time) return { error: 'Понял дату, но не понял время. Пример: “бот напомни завтра в 10:00 проверить маршрут”.' };
  if (!time.rest) return { error: 'Что именно напомнить?' };

  const due = new Date(datePrefix.date);
  due.setHours(time.hour, time.minute, 0, 0);
  if (Number.isNaN(due.getTime())) return { error: 'Не понял дату напоминания.' };
  if (due.getTime() <= now) return { error: `Это время уже прошло: ${formatDueTime(due.getTime())}. Назови будущее время.` };
  return { dueAt: due.getTime(), text: time.rest.slice(0, 1000) };
}

function looksLikeReminderCreate(prompt) {
  const normalized = normalizeCommandText(prompt);
  if (!/(напомн|напомин|remind)/u.test(normalized)) return false;
  return !/(удал|убер|убери|отмен|отмени|сотри|стери|забудь|покажи|список|какие|какой|какое|есть|активн|delete|remove|cancel|show|list)/u.test(normalized);
}

function parseListRemindersCommand(prompt) {
  const normalized = normalizeCommandText(prompt);
  if (!/(напомин|reminder|reminders)/u.test(normalized)) return null;
  if (/(удал|убер|убери|отмен|отмени|очист|сброс|сотри|стери|забудь|delete|remove|cancel|clear)/u.test(normalized)) return null;

  const listIntent = /(покажи|выведи|скажи|расскажи|назови|прочитай|озвучь|список|какие|какой|какое|что\s+по|есть\s+ли|активн|show|list|tell|read|what|any)/u.test(normalized)
    || normalized === 'напоминания'
    || normalized === 'reminders';
  if (!listIntent) return null;

  let range = 'all';
  if (/(сегодня|сегодняш|today)/u.test(normalized)) range = 'today';
  else if (/(завтра|завтраш|tomorrow)/u.test(normalized)) range = 'tomorrow';
  else if (/(недел|7\s*дн|week)/u.test(normalized)) range = 'week';
  else if (/(просроч|опоздавш|overdue)/u.test(normalized)) range = 'overdue';

  const userOnly = /(^|\s)(мои|мо[иеё]|личн\p{L}*|персональн\p{L}*|у\s+меня|для\s+меня|мне|my|personal)(\s|$)/u.test(normalized);
  return { action: 'list_reminders', range, userOnly };
}

function parseReminderCommand(prompt) {
  const text = String(prompt || '').trim();
  const createPrefix = `${REMINDER_CREATE_PATTERN}${REMINDER_ME_PATTERN}`;
  const recurringInterval = text.match(new RegExp(`(?:^|\\s)${createPrefix}\\s+(?:кажд(?:ые|ый|ую|ое)|every)\\s+(\\d+(?:[.,]\\d+)?|[a-zа-яё’'ʼ\`]+)?\\s*(${REMINDER_UNIT_PATTERN}|недел[юияь]*|weeks?|месяц(?:а|ев)?|months?)\\s*(.*)$`, 'iu'));
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

  const recurringDay = text.match(new RegExp(`(?:^|\\s)${createPrefix}\\s+(?:кажд(?:ый|ое)\\s+день|every\\s+day)\\s*(.*)$`, 'iu'));
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
  if (!match) {
    const absolute = text.match(new RegExp(`(?:^|\\s)${createPrefix}\\s+(.+)$`, 'iu'));
    if (!absolute) return null;
    return parseAbsoluteReminderTail(absolute[1]);
  }

  const tail = match[1].trim();
  const withAmount = tail.match(new RegExp(`^(\\d+(?:[.,]\\d+)?|[a-zа-яё’'ʼ\`]+)\\s*(${REMINDER_UNIT_PATTERN})\\s*(.*)$`, 'iu'));
  const withoutAmount = tail.match(/^(секунду|минуту|час|день|сутки|second|minute|hour|day)\s*(.*)$/iu);

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
  const exact = new Date(dueAt).toLocaleString('ru-RU', {
    timeZone: REMINDER_TIME_ZONE,
    dateStyle: 'short',
    timeStyle: 'short',
  });
  if (minutes < 1) return `меньше чем через минуту (${exact}, ${REMINDER_TIME_ZONE})`;
  if (minutes < 60) return `через ${minutes} мин. (${exact}, ${REMINDER_TIME_ZONE})`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `через ${hours} ч. (${exact}, ${REMINDER_TIME_ZONE})`;
  return `${exact}, ${REMINDER_TIME_ZONE}`;
}

function formatDueTimeForSpeech(dueAt) {
  const delayMs = Math.max(0, dueAt - Date.now());
  const minutes = Math.round(delayMs / 60000);
  if (minutes < 1) return 'меньше чем через минуту';
  if (minutes < 60) return `через ${minutes} ${pluralRu(minutes, 'минуту', 'минуты', 'минут')}`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `через ${hours} ${pluralRu(hours, 'час', 'часа', 'часов')}`;
  const exact = new Intl.DateTimeFormat('ru-RU', {
    timeZone: REMINDER_TIME_ZONE,
    day: 'numeric',
    month: 'long',
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(dueAt));
  return `на ${exact}`;
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
    kind: options.kind || REMINDER_KIND_TEXT,
    soundboardSoundName: options.soundboardSoundName || null,
    soundboardSoundId: options.soundboardSoundId || null,
    soundboardSourceGuildId: options.soundboardSourceGuildId || null,
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
    if (reminder.kind === REMINDER_KIND_SOUNDBOARD) {
      await deliverSoundboardReminder(reminder);
      return;
    }

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
    kind: stored.kind || REMINDER_KIND_TEXT,
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

function normalizeReminderListRange(range) {
  const normalized = normalizeCommandText(range || '');
  if (['today', 'сегодня', 'сегодняшние'].includes(normalized)) return 'today';
  if (['tomorrow', 'завтра', 'завтрашние'].includes(normalized)) return 'tomorrow';
  if (['week', 'неделя', 'неделю', '7 дней'].includes(normalized)) return 'week';
  if (['overdue', 'просроченные', 'просрочка'].includes(normalized)) return 'overdue';
  return 'all';
}

function reminderListTitle(options = {}) {
  const owner = options.userOnly ? 'Мои ' : '';
  switch (normalizeReminderListRange(options.range)) {
    case 'today':
      return `${owner}напоминания на сегодня`;
    case 'tomorrow':
      return `${owner}напоминания на завтра`;
    case 'week':
      return `${owner}напоминания на ближайшие 7 дней`;
    case 'overdue':
      return `${owner}просроченные напоминания`;
    default:
      return `${owner}активные напоминания`;
  }
}

function reminderListEmptyText(options = {}) {
  switch (normalizeReminderListRange(options.range)) {
    case 'today':
      return options.userOnly ? 'У тебя на сегодня активных напоминаний нет.' : 'На сегодня активных напоминаний нет.';
    case 'tomorrow':
      return options.userOnly ? 'У тебя на завтра активных напоминаний нет.' : 'На завтра активных напоминаний нет.';
    case 'week':
      return options.userOnly ? 'У тебя на ближайшие 7 дней активных напоминаний нет.' : 'На ближайшие 7 дней активных напоминаний нет.';
    case 'overdue':
      return options.userOnly ? 'У тебя просроченных активных напоминаний нет.' : 'Просроченных активных напоминаний нет.';
    default:
      return options.userOnly ? 'У тебя активных напоминаний нет.' : 'Активных напоминаний нет.';
  }
}

function filterRemindersForList(reminders, options = {}) {
  const range = normalizeReminderListRange(options.range);
  const now = Date.now();
  const startToday = startOfLocalDay(now).getTime();
  const startTomorrow = addLocalDays(now, 1).getTime();
  const startAfterTomorrow = addLocalDays(now, 2).getTime();
  const endWeek = addLocalDays(now, 7).getTime();

  return reminders.filter((reminder) => {
    if (options.userOnly && options.userId && reminder.userId !== options.userId) return false;
    const dueAt = Number(reminder.dueAt || 0);
    if (!dueAt) return false;
    if (range === 'today') return dueAt >= startToday && dueAt < startTomorrow;
    if (range === 'tomorrow') return dueAt >= startTomorrow && dueAt < startAfterTomorrow;
    if (range === 'week') return dueAt >= startToday && dueAt < endWeek;
    if (range === 'overdue') return dueAt <= now;
    return true;
  });
}

function formatReminderList(guildId, options = {}) {
  const limit = Math.max(1, Math.min(25, Number(options.limit || 10)));
  const reminders = filterRemindersForList(
    getGuildState(guildId).reminders
      .slice()
      .sort((a, b) => a.dueAt - b.dueAt),
    options,
  );
  if (!reminders.length) return reminderListEmptyText(options);
  const shown = reminders.slice(0, limit);
  const more = reminders.length > shown.length
    ? `\n...и еще ${reminders.length - shown.length}.`
    : '';
  return shown
    .map((reminder, index) => formatReminderChoice(reminder, index))
    .join('\n') + more;
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
    const semanticScore = semanticSearchScore(query, reminderSearchText(reminder));
    const dateScore = reminderDateScore(reminder, normalizedQuery);
    return {
      reminder,
      index,
      textScore,
      semanticScore,
      dateScore,
      score: Math.max(textScore, directTextScore, fuzzyTextScore, semanticScore) + dateScore,
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

const DISCORD_PERMISSION_CHECKS = [
  { key: 'Administrator', label: 'Administrator', bit: PermissionFlagsBits.Administrator, hint: 'полный доступ к большинству действий, но иерархию ролей не обходит' },
  { key: 'ViewChannel', label: 'View Channels', bit: PermissionFlagsBits.ViewChannel, hint: 'видеть текстовые и voice-каналы' },
  { key: 'SendMessages', label: 'Send Messages', bit: PermissionFlagsBits.SendMessages, hint: 'писать ответы в чат' },
  { key: 'ReadMessageHistory', label: 'Read Message History', bit: PermissionFlagsBits.ReadMessageHistory, hint: 'читать историю для команд с сообщениями' },
  { key: 'Connect', label: 'Connect', bit: PermissionFlagsBits.Connect, hint: 'заходить в voice' },
  { key: 'Speak', label: 'Speak', bit: PermissionFlagsBits.Speak, hint: 'говорить и проигрывать TTS/музыку' },
  { key: 'UseVAD', label: 'Use Voice Activity', bit: PermissionFlagsBits.UseVAD, hint: 'voice activity для речи' },
  { key: 'MoveMembers', label: 'Move Members', bit: PermissionFlagsBits.MoveMembers, hint: 'перемещать/отключать участников voice' },
  { key: 'MuteMembers', label: 'Mute Members', bit: PermissionFlagsBits.MuteMembers, hint: 'мьютить микрофоны' },
  { key: 'DeafenMembers', label: 'Deafen Members', bit: PermissionFlagsBits.DeafenMembers, hint: 'глушить звук участникам' },
  { key: 'ManageChannels', label: 'Manage Channels', bit: PermissionFlagsBits.ManageChannels, hint: 'создавать/переименовывать/закрывать каналы и stream-overwrite' },
  { key: 'ManageRoles', label: 'Manage Roles', bit: PermissionFlagsBits.ManageRoles, hint: 'выдавать/забирать роли ниже роли бота' },
  { key: 'KickMembers', label: 'Kick Members', bit: PermissionFlagsBits.KickMembers, hint: 'кикать участников' },
  { key: 'BanMembers', label: 'Ban Members', bit: PermissionFlagsBits.BanMembers, hint: 'банить участников' },
  { key: 'ModerateMembers', label: 'Moderate Members', bit: PermissionFlagsBits.ModerateMembers, hint: 'timeout участников' },
  { key: 'ManageMessages', label: 'Manage Messages', bit: PermissionFlagsBits.ManageMessages, hint: 'чистить сообщения' },
  { key: 'PinMessages', label: 'Pin Messages', bit: PermissionFlagsBits.PinMessages, hint: 'закреплять сообщения' },
  { key: 'CreateInstantInvite', label: 'Create Invite', bit: PermissionFlagsBits.CreateInstantInvite, hint: 'создавать invite-ссылки' },
  { key: 'ManageGuild', label: 'Manage Server', bit: PermissionFlagsBits.ManageGuild, hint: 'управлять серверными настройками и Telegram setup' },
  { key: 'UseSoundboard', label: 'Use Soundboard', bit: PermissionFlagsBits.UseSoundboard, hint: 'проигрывать soundboard-звуки' },
  { key: 'ManageGuildExpressions', label: 'Manage Expressions', bit: PermissionFlagsBits.ManageGuildExpressions, hint: 'переименовывать/удалять soundboard-звуки сервера' },
  { key: 'CreatePublicThreads', label: 'Create Public Threads', bit: PermissionFlagsBits.CreatePublicThreads, hint: 'создавать треды' },
  { key: 'ManageThreads', label: 'Manage Threads', bit: PermissionFlagsBits.ManageThreads, hint: 'архивировать/закрывать треды' },
];

function roleSummary(role) {
  if (!role) return null;
  return {
    id: role.id,
    name: role.name,
    position: role.position,
    managed: Boolean(role.managed),
    color: role.hexColor || '#000000',
  };
}

function summarizeChannelPermissions(channel, member) {
  const permissions = channel?.permissionsFor?.(member);
  if (!permissions) return null;
  return {
    channelId: channel.id,
    channelName: channel.name,
    type: ChannelType[channel.type] || String(channel.type),
    allowed: DISCORD_PERMISSION_CHECKS
      .filter((item) => permissions.has(item.bit))
      .map((item) => item.key),
    missing: DISCORD_PERMISSION_CHECKS
      .filter((item) => !permissions.has(item.bit))
      .map((item) => item.key),
  };
}

function summarizeGuildDiscordPermissions(guild, session = null) {
  const member = guild?.members?.me || (client.user?.id ? guild?.members?.cache?.get(client.user.id) : null);
  if (!guild || !member) {
    return {
      guildId: guild?.id || '',
      guildName: guild?.name || '',
      ok: false,
      error: 'Не смог получить GuildMember бота. Попробуй перезапустить бота или пригласить его заново.',
    };
  }

  const botTopRole = member.roles?.highest || null;
  const roles = [...(guild.roles?.cache?.values?.() || [])]
    .filter((role) => role.id !== guild.id)
    .sort((a, b) => b.position - a.position);
  const permissions = DISCORD_PERMISSION_CHECKS.map((item) => ({
    key: item.key,
    label: item.label,
    hint: item.hint,
    granted: member.permissions.has(item.bit),
  }));
  const missingPermissions = permissions.filter((item) => !item.granted);
  const rolesAboveBot = botTopRole
    ? roles.filter((role) => role.comparePositionTo(botTopRole) > 0)
    : [];
  const hierarchyBlockedRoles = botTopRole
    ? roles.filter((role) => !role.managed && role.id !== botTopRole.id && role.comparePositionTo(botTopRole) >= 0)
    : [];
  const managedRoles = roles.filter((role) => role.managed && role.comparePositionTo(botTopRole) < 0);
  const hintRole = rolesAboveBot[0] || hierarchyBlockedRoles[0] || null;

  return {
    guildId: guild.id,
    guildName: guild.name,
    ok: true,
    botId: member.id,
    botName: member.displayName || member.user?.username || client.user?.username || 'bot',
    topRole: roleSummary(botTopRole),
    permissions,
    grantedPermissions: permissions.filter((item) => item.granted).map((item) => item.key),
    missingPermissions,
    rolesAboveBot: rolesAboveBot.map(roleSummary).slice(0, 80),
    hierarchyBlockedRoles: hierarchyBlockedRoles.map(roleSummary).slice(0, 80),
    managedRolesBelowBot: managedRoles.map(roleSummary).slice(0, 40),
    hint: hintRole
      ? `подними роль бота выше роли ${hintRole.name}`
      : 'роль бота уже выше всех обычных ролей',
    textChannel: summarizeChannelPermissions(session?.textChannel, member),
    voiceChannel: summarizeChannelPermissions(session?.voiceChannel, member),
    updatedAt: Date.now(),
  };
}

function summarizeDiscordPermissions() {
  const byGuild = [];
  for (const guild of client.guilds.cache.values()) {
    byGuild.push(summarizeGuildDiscordPermissions(guild, sessions.get(guild.id) || null));
  }
  byGuild.sort((a, b) => {
    if (a.guildId === DISCORD_GUILD_ID) return -1;
    if (b.guildId === DISCORD_GUILD_ID) return 1;
    return String(a.guildName || a.guildId).localeCompare(String(b.guildName || b.guildId), 'ru');
  });
  return {
    guilds: byGuild,
    primary: byGuild.find((item) => item.guildId === DISCORD_GUILD_ID) || byGuild[0] || null,
    permissionChecks: DISCORD_PERMISSION_CHECKS.map(({ key, label, hint }) => ({ key, label, hint })),
  };
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
    groqModelDiscoveryEnabled: GROQ_MODEL_DISCOVERY_ENABLED,
    groqAutoSelectDiscoveredModels: GROQ_AUTO_SELECT_DISCOVERED_MODELS,
    groqModelDiscoveryIntervalMs: GROQ_MODEL_DISCOVERY_INTERVAL_MS,
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
    presenceNameAnnouncementMaxMembers: PRESENCE_NAME_ANNOUNCEMENT_MAX_MEMBERS,
    presenceBotJoinNamedMaxMembers: PRESENCE_BOT_JOIN_NAMED_MAX_MEMBERS,
    presenceMemberGreetingCooldownMs: PRESENCE_MEMBER_GREETING_COOLDOWN_MS,
    presenceAnnouncementMaxChars: PRESENCE_ANNOUNCEMENT_MAX_CHARS,
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
    telegramInboundEnabled: isTelegramInboundEnabled(),
    telegramInboundAllowedChatIds: getTelegramInboundAllowedChatIds(),
    telegramInboundPlainForward: isTelegramInboundPlainForwardEnabled(),
    telegramInboundLastAt: runtimeConfig.telegramInboundLastAt || 0,
    telegramInboundLastError: runtimeConfig.telegramInboundLastError || '',
    telegramInboundLastErrorAt: runtimeConfig.telegramInboundLastErrorAt || 0,
    telegramKnownChats: normalizeTelegramKnownChats(runtimeConfig.telegramKnownChats || []),
    backupEnabled: isBackupEnabled(),
    backupTargetPath: getBackupTargetPath(),
    backupTargetUsername: getBackupTargetUsername(),
    backupTargetPasswordSet: Boolean(getBackupTargetPassword()),
    backupTargetMasked: maskBackupTarget(getBackupTargetPath()),
    backupIntervalHours: getBackupIntervalHours(),
    backupRetention: getBackupRetention(),
    backupIdleOnly: isBackupIdleOnly(),
    backupLastRunAt: runtimeConfig.backupLastRunAt || 0,
    backupNextRunAt: runtimeConfig.backupNextRunAt || backupNextRunAt(),
    backupLastFile: runtimeConfig.backupLastFile || '',
    backupLastTarget: runtimeConfig.backupLastTarget || '',
    backupLastTargetMasked: maskBackupTarget(runtimeConfig.backupLastTarget || ''),
    backupLastError: runtimeConfig.backupLastError || '',
    backupLastErrorAt: runtimeConfig.backupLastErrorAt || 0,
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
      wakeAckInProgress: Boolean(session.wakeAckInProgress),
      wakeAckUserId: session.wakeAckUserId || null,
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
      music: summarizeMusic(session),
      diagnostics: session.diagnostics || createVoiceDiagnostics(),
    };
  });
}

function memoryStats() {
  const guilds = Object.entries(stateStore.guilds || {});
  const userMemories = (guildState) => Object.values(guildState.userMemories || {})
    .reduce((sum, memories) => sum + (Array.isArray(memories) ? memories.length : 0), 0);
  const userProfiles = (guildState) => Object.keys(guildState.userProfiles || {}).length;
  return {
    guilds: guilds.length,
    memories: guilds.reduce((sum, [, guildState]) => sum + (guildState.memories?.length || 0) + userMemories(guildState), 0),
    profiles: guilds.reduce((sum, [, guildState]) => sum + userProfiles(guildState), 0),
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
    discordPermissions: summarizeDiscordPermissions(),
    taskQueues: taskQueueSnapshot(),
    groqLimits: Object.fromEntries(groqLastLimits.entries()),
    groqModelCooldowns: groqModelCooldownsObject(),
    groqModelDiscovery: groqModelDiscoveryStatus(),
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

function scheduleStatusSnapshot(delayMs = 1000) {
  if (statusSnapshotTimer) return;
  statusSnapshotTimer = setTimeout(() => {
    statusSnapshotTimer = null;
    void writeStatusSnapshot().catch((error) => console.error('status snapshot tick failed:', error));
  }, delayMs);
  statusSnapshotTimer.unref?.();
}

async function initPanelCommandOffset() {
  const stat = await fs.stat(panelCommandsPath).catch(() => null);
  panelCommandOffset = stat?.size || 0;
}

function panelCommandSession(guildId = '') {
  if (guildId && sessions.has(guildId)) return sessions.get(guildId);
  return sessions.values().next().value || null;
}

async function handlePanelCommand(command) {
  if (!command || typeof command !== 'object') return;
  if (command.createdAt && Date.now() - Number(command.createdAt) > 5 * 60_000) {
    appendEvent('panel_command_ignored_stale', { id: command.id, action: command.action, createdAt: command.createdAt });
    return;
  }
  if (command.type !== 'music') {
    appendEvent('panel_command_ignored_unknown', { id: command.id, type: command.type, action: command.action });
    return;
  }
  const session = panelCommandSession(command.guildId);
  if (!session) {
    appendEvent('panel_music_command_failed', { id: command.id, action: command.action, message: 'no active voice session' });
    return;
  }
  const parsed = {
    action: command.action,
    text: command.text || '',
    value: command.value,
    delta: command.delta,
    channel: command.channel || '',
  };
  try {
    const result = await executeMusicAction(session, null, parsed, { source: 'panel' });
    appendEvent('panel_music_command_result', {
      id: command.id,
      guildId: session.guild?.id,
      voiceChannelId: session.voiceChannel?.id,
      action: command.action,
      text: typeof result === 'string' ? result : result?.text,
    });
    await writeStatusSnapshot();
  } catch (error) {
    appendEvent('panel_music_command_failed', {
      id: command.id,
      guildId: session.guild?.id,
      action: command.action,
      message: error.message || String(error),
    });
    console.error('panel music command failed:', error);
  }
}

async function pollPanelCommands() {
  if (panelCommandPollInProgress) return;
  panelCommandPollInProgress = true;
  try {
    const stat = await fs.stat(panelCommandsPath).catch(() => null);
    if (!stat?.isFile()) return;
    if (stat.size < panelCommandOffset) panelCommandOffset = 0;
    if (stat.size === panelCommandOffset) return;
    const handle = await fs.open(panelCommandsPath, 'r');
    try {
      const length = stat.size - panelCommandOffset;
      const buffer = Buffer.alloc(length);
      await handle.read(buffer, 0, length, panelCommandOffset);
      panelCommandOffset = stat.size;
      const lines = buffer.toString('utf8').split(/\r?\n/u).map((line) => line.trim()).filter(Boolean);
      for (const line of lines) {
        let command = null;
        try {
          command = JSON.parse(line);
        } catch (error) {
          appendEvent('panel_command_parse_failed', { line: line.slice(0, 500), message: error.message || String(error) });
          continue;
        }
        await handlePanelCommand(command);
      }
    } finally {
      await handle.close();
    }
  } finally {
    panelCommandPollInProgress = false;
  }
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
        stopMusic(session, { clearQueue: true, reason: 'bot_disabled' });
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

function cleanCallNameTargetText(value) {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/^(?:пользовател[яю]|участник[ау]?|юзер[ау]?|user|member)\s+/iu, '')
    .replace(/^(?:по\s+имени|с\s+ником|по\s+нику)\s+/iu, '')
    .trim();
}

function cleanCallNameAlias(value) {
  const cleaned = sanitizeVoiceOutputText(stripMarkdownFormatting(value || ''))
    .replace(/\s+/g, ' ')
    .replace(/^[«"“”'`]+|[»"“”'`]+$/gu, '')
    .replace(/[.!?]+$/u, '')
    .trim();
  if (charLength(cleaned) <= 48) return cleaned;
  return [...cleaned].slice(0, 48).join('').replace(/\s+\S*$/u, '').trim();
}

function parseCallNamePreference(prompt) {
  const raw = String(prompt || '').replace(/\s+/g, ' ').trim();
  if (!raw) return null;

  const patterns = [
    /^(?:называй|зови)\s+(.+?)(?:\s+(?:как|словом|именем|по имени|as|like|by)\s+|\s*[,;:–—-]\s*)(.+)$/iu,
    /^(?:обращайся\s+к)\s+(.+?)(?:\s+(?:как|словом|именем|по имени|as|like|by)\s+|\s*[,;:–—-]\s*)(.+)$/iu,
    /^(?:call)\s+(.+?)\s+(?:as|like|by)\s+(.+)$/iu,
  ];

  for (const pattern of patterns) {
    const match = raw.match(pattern);
    if (!match?.[1]?.trim() || !match?.[2]?.trim()) continue;
    const target = cleanCallNameTargetText(match[1]);
    const alias = cleanCallNameAlias(match[2]);
    if (!target || !alias) continue;
    return { target, alias };
  }

  return null;
}

async function handleCallNamePreferenceCommand(session, actorMember, prompt) {
  const parsed = parseCallNamePreference(prompt);
  if (!parsed) return null;

  const selfTarget = await resolveSelfMemberTarget(session, actorMember, parsed.target);
  const target = selfTarget
    ? (selfTarget.error ? selfTarget : { member: selfTarget.member })
    : await findMemberTarget(session, parsed.target);
  if (target.error) return { text: target.error, speak: true };

  const targetMember = target.member;
  const targetName = displayMemberName(targetMember);
  const memoryText = `Обращение: пользователя ${targetName} называй "${parsed.alias}".`;
  addUserMemoryItem(session.guild.id, targetMember, memoryText);
  updateUserProfile(session.guild.id, targetMember, { preferredName: parsed.alias }, 'call_name_preference');
  appendEvent('memory_added', {
    guildId: session.guild.id,
    actorId: actorMember?.id,
    userId: targetMember.id,
    scope: 'user',
    text: memoryText,
    source: 'call_name_preference',
  });
  return {
    text: `Запомнил: ${targetName} буду называть "${parsed.alias}".`,
    speak: true,
  };
}

function cleanVoiceChannelTargetText(value) {
  return normalizeCommandText(value || '')
    .replace(/^(?:голосов\p{L}*\s+)?(?:канал|комнату|комната|войс|воис|voice|voice channel|room)\s+/u, '')
    .replace(/^(?:в|во|на|до)\s+(?:голосов\p{L}*\s+)?(?:канал|комнату|комната|войс|воис|voice|voice channel|room)\s+/u, '')
    .trim();
}

async function findVoiceChannel(session, channelText) {
  const channels = await session.guild.channels.fetch();
  const voiceChannels = [...channels.values()].filter(
    (channel) => channel && [ChannelType.GuildVoice, ChannelType.GuildStageVoice].includes(channel.type),
  );
  const cleanedChannelText = cleanVoiceChannelTargetText(channelText);

  const result = findBestFuzzy(voiceChannels, cleanedChannelText || channelText, {
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

async function botRoleManageError(session, targetMember, role) {
  const me = session.guild.members.me
    || (typeof session.guild.members.fetchMe === 'function' ? await session.guild.members.fetchMe().catch(() => null) : null)
    || (client.user?.id ? await session.guild.members.fetch(client.user.id).catch(() => null) : null);
  if (!me) return 'Не смог проверить свою роль на сервере.';
  if (!me.permissions.has(PermissionFlagsBits.ManageRoles)) {
    return 'У меня нет права Manage Roles. Выдай его роли бота.';
  }
  if (role.managed) {
    return `Не могу менять роль ${role.name}: это интеграционная/managed роль Discord.`;
  }
  if (role.comparePositionTo(me.roles.highest) >= 0) {
    return `Не могу менять роль ${role.name}: моя верхняя роль ниже или на одном уровне. Подними роль бота выше роли ${role.name} в настройках сервера.`;
  }
  if (targetMember?.id === session.guild.ownerId) {
    return 'Не могу менять роли владельца сервера.';
  }
  if (targetMember?.roles?.highest && targetMember.roles.highest.comparePositionTo(me.roles.highest) >= 0) {
    return `Не могу менять роли ${targetMember.displayName}: его верхняя роль выше или на одном уровне с ролью бота. Подними роль бота выше роли этого участника.`;
  }
  return '';
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

async function postSoundboardSound(session, sound) {
  if (!session?.voiceChannel?.id) throw new Error('Я не подключен к голосовому каналу.');
  return await runQueuedTask(
    'soundboard',
    `play:${sound.name || sound.soundId}`,
    async () => {
      if (!session?.voiceChannel?.id) throw new Error('Я не подключен к голосовому каналу.');
      return await client.rest.post(`/channels/${session.voiceChannel.id}/send-soundboard-sound`, {
        body: {
          sound_id: sound.soundId,
          source_guild_id: sound.guildId || undefined,
        },
      });
    },
    queueMetaForSession(session, {
      soundId: sound.soundId,
      soundName: sound.name || '',
      sourceGuildId: sound.guildId || null,
    }),
  );
}

async function deliverSoundboardReminder(reminder) {
  const session = findReminderSession(reminder);
  const canPlayInCurrentVoice = session?.connection
    && session.connection.state.status !== VoiceConnectionStatus.Destroyed
    && (!reminder.voiceChannelId || session.voiceChannel?.id === reminder.voiceChannelId);

  if (!canPlayInCurrentVoice) {
    console.log(`soundboard reminder skipped: reminder channel=${reminder.voiceChannelId || 'any'}, current=${session?.voiceChannel?.id || 'none'}`);
    appendEvent('soundboard_reminder_skipped', {
      guildId: reminder.guildId,
      voiceChannelId: reminder.voiceChannelId,
      sound: reminder.soundboardSoundName || reminder.text,
      reason: 'voice_not_connected_or_different_channel',
    });
    return;
  }

  let sound = null;
  if (reminder.soundboardSoundId) {
    const sounds = await fetchSoundboardSounds(session);
    sound = sounds.find((item) => item.soundId === reminder.soundboardSoundId && (!reminder.soundboardSourceGuildId || item.guildId === reminder.soundboardSourceGuildId));
  }
  if (!sound) {
    const result = await findSoundboardSound(session, reminder.soundboardSoundName || reminder.text);
    if (result.error) throw new Error(result.error);
    sound = result.sound;
  }

  await postSoundboardSound(session, sound);
  appendEvent('soundboard_reminder_played', {
    guildId: reminder.guildId,
    voiceChannelId: reminder.voiceChannelId,
    sound: sound.name || sound.soundId,
    repeatLabel: reminder.repeatLabel,
    acceptedByDiscord: true,
  });
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
  'отключ', 'выкин', 'выкини', 'дискон',
  'кикни', 'кик', 'исключ', 'удали участника',
  'бан', 'забань', 'разбан',
  'таймаут', 'timeout', 'мут на', 'накажи', 'сними таймаут',
  'перемест', 'перенеси', 'перекин', 'перетащи', 'верни обратно', 'верни назад',
  'мут', 'замуть', 'зам ють', 'размут', 'размуть', 'заглуш', 'разглуш', 'микрофон',
  'деаф', 'оглуш',
  'роль', 'выдай роль', 'дай роль', 'забери роль', 'убери роль',
  'ник', 'никнейм', 'переименуй участника',
  'закрой', 'открой', 'залочь', 'разлочь', 'заблок', 'разблок',
  'переимен', 'назови', 'имя канала',
  'создай канал', 'создай чат', 'создай войс', 'создай голосовой', 'create channel',
  'удали канал', 'снеси канал',
  'лимит', 'слоумод', 'slowmode', 'медленный режим',
  'очист', 'удали сообщения', 'почист',
  'напиши', 'отправь в чат', 'скажи в чат',
  'стоп', 'замолчи', 'перестань говорить', 'хватит', 'остановись', 'останови', 'харош', 'хорош',
  'сбрось память', 'забудь память', 'очисти память', 'запомни', 'запиши в память',
  'найди в памяти', 'покажи заметки', 'удали заметку', 'удали память', 'что ты помнишь про',
  'профиль', 'мой профиль', 'часовой пояс', 'любимые темы', 'стиль общения',
  'частые задачи', 'привычные команды', 'персональная заметка', 'предпочтения по шуткам',
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
  'покажи мой профиль',
  'мой часовой пояс Europe/Kyiv',
  'любимые темы Dota 2, Docker и Telegram',
  'стиль общения коротко и по делу',
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
    /(^|\s)(создай|создать|create)\s+(?:новый\s+|new\s+)?(?:голосов\p{L}*|войс|воис|voice|текстов\p{L}*|чат|channel)(\s|$)/u,
    /(^|\s)(верни|вернуть)\s+.+\s+(?:обратно|назад)(\s|$)/u,
    /(^|\s)(отключи|выключи|включи)\s+(?:микрофон|звук|mic|microphone)(\s|$)/u,
    /(^|\s)(проиграй|включи|запусти|поставь|play)\s+(?:звук|саунд|sound)(\s|$)/u,
    /(^|\s)(телеграмм?|телеграмму|телега|телегу|телеге|тележк\p{L}*|телиграмм?|telegramm?|telega|tg|тг)(\s|$)/u,
    /(^|\s)(создай|сделай|create)\s+(?:инвайт|приглашение|invite|тред|thread|категор)/u,
  ].some((pattern) => pattern.test(normalized));
}

const AI_ACTION_VERB_PATTERN = /(^|\s)(сделай|сделать|создай|создать|удали|удалить|убери|убрать|очист\p{L}*|почист\p{L}*|постав\p{L}*|установ\p{L}*|включ\p{L}*|выключ\p{L}*|выруб\p{L}*|отключ\p{L}*|подключ\p{L}*|заглуш\p{L}*|разглуш\p{L}*|замут\p{L}*|размут\p{L}*|перемест\p{L}*|перенес\p{L}*|перетащ\p{L}*|перекин\p{L}*|верни|вернуть|выдай|дай|забери|сними|назнач\p{L}*|переимен\p{L}*|назови|называй|зови|обращайся|измени|поменяй|закрой|открой|заблок\p{L}*|разблок\p{L}*|залоч\p{L}*|разлоч\p{L}*|закреп\p{L}*|напиши|отправ\p{L}*|скинь|скини|кинь|кини|закин\p{L}*|передай|запомн\p{L}*|запиши|сохрани|напомн\p{L}*|отмени|сброс\p{L}*|покажи|выведи|проигра\p{L}*|запусти|останов\p{L}*|замолчи|хватит|харош|mute|unmute|disconnect|kick|ban|move|create|delete|remove|rename|lock|unlock|list|show|clear|pin|archive|timeout|remember|remind|pause|resume|stop|send|play)(\s|$)/u;

const AI_ACTION_TARGET_PATTERN = /(^|\s)(участник\p{L}*|пользовател\p{L}*|юзер\p{L}*|люд\p{L}*|человек\p{L}*|всех|all|его|ее|её|их|меня|мне|себя|себе|тебя|тебе|сам\p{L}*|бот\p{L}*|ассистент\p{L}*|me|myself|you|yourself|bot|assistant|войс\p{L}*|воис\p{L}*|голосов\p{L}*|комнат\p{L}*|voice|room|микрофон\p{L}*|трансляц\p{L}*|стрим\p{L}*|демк\p{L}*|демонстрац\p{L}*|экран|screen|screenshare|stream|streaming|video|звук\p{L}*|саунд\p{L}*|sound|soundboard|музык\p{L}*|песн\p{L}*|трек\p{L}*|радио|youtube|ютуб|spotify|спотиф\p{L}*|плейлист|playlist|канал\p{L}*|чат\p{L}*|текстов\p{L}*|channel|chat|роль|роли|ролью|рол\p{L}*|модер\p{L}*|админ\p{L}*|role|ник\p{L}*|nickname|таймаут\p{L}*|timeout|сервер\p{L}*|server|категор\p{L}*|category|тред\p{L}*|ветк\p{L}*|thread|инвайт\p{L}*|приглаш\p{L}*|invite|сообщен\p{L}*|месседж\p{L}*|message|слоумод\p{L}*|slowmode|лимит\p{L}*|limit|тема|тему|topic|памят\p{L}*|memory|заметк\p{L}*|note|напомин\p{L}*|reminder|профил\p{L}*|profile|часовой\s+пояс|timezone|стиль\s+общения|статус|status|лимиты|limits|телеграмм?|телега|телегу|телеге|тележк\p{L}*|telegramm?|telega|tg|тг)(\s|$)/u;

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
    .replace(/^(?:микрофон|микрофона|звук|звука|microphone|mic)\s+/u, '')
    .replace(/^у\s+/u, '')
    .replace(/^(?:me|ми)\s+(?=\S)/u, '')
    .replace(/\s+(?:из|с|со|от)\s+(?:голосового\s+)?(?:войса|воиса|voice|voice channel|канала|чата)$/u, '')
    .replace(/\s+(?:в|на)\s+(?:войсе|воисе|voice|канале|чате)$/u, '')
    .replace(/[,\s]+$/u, '')
    .trim();
}

function normalizeMemberTargetReference(value) {
  return normalizeCommandText(value || '').replace(/\s+/g, ' ').trim();
}

function isActorSelfTarget(value) {
  const normalized = normalizeMemberTargetReference(value);
  return /^(?:я|меня|мне|мной|мною|мой|моя|мое|моё|мою|моего|me|myself)$/u.test(normalized);
}

function assistantSelfTargetVariants(session) {
  const botMember = client.user?.id
    ? (session?.guild?.members?.cache?.get(client.user.id) || session?.guild?.members?.me)
    : null;
  const values = [
    getAssistantName(),
    getWakeWord(),
    ...getWakeAliases(),
    client.user?.username,
    client.user?.tag,
    botMember?.displayName,
    botMember?.nickname,
  ];

  return new Set(values
    .filter(Boolean)
    .flatMap((value) => nameSearchVariants(value)));
}

function isAssistantSelfTarget(value, session = null) {
  const normalized = normalizeMemberTargetReference(value);
  if (!normalized) return false;
  if (/^(?:себя|себе|собой|сам|сама|самого|саму|самого себя|саму себя|сам себя|сама себя|тебя|тебе|тобой|ты|бот|бота|боту|боте|ботом|ассистент|ассистента|ассистенту|ассистентом|you|yourself|bot|assistant)$/u.test(normalized)) {
    return true;
  }

  const targetVariants = new Set(nameSearchVariants(normalized));
  for (const variant of assistantSelfTargetVariants(session)) {
    if (targetVariants.has(variant) || compactText(variant) === compactText(normalized)) return true;
  }
  return false;
}

async function resolveSelfMemberTarget(session, actorMember, targetText) {
  if (isActorSelfTarget(targetText)) {
    const member = actorMember?.id
      ? (session.guild.members.cache.get(actorMember.id) || await session.guild.members.fetch(actorMember.id).catch(() => null))
      : null;
    return member ? { member } : { error: 'Не нашел тебя на сервере.' };
  }

  if (isAssistantSelfTarget(targetText, session)) {
    const member = client.user?.id
      ? (session.guild.members.cache.get(client.user.id) || session.guild.members.me || await session.guild.members.fetch(client.user.id).catch(() => null))
      : null;
    return member ? { member } : { error: 'Не нашел самого себя на сервере.' };
  }

  return null;
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

function parseSoundboardScheduleCommand(prompt) {
  const raw = String(prompt || '').replace(/\s+/g, ' ').trim();
  const normalized = normalizeCommandText(raw);
  if (!/(звук|саунд|sound|soundboard|саундборд)/u.test(normalized)) return null;
  if (!/(проигр|воспроиз|производ|произвед|включ|запуск|запусти|play|voice)/u.test(normalized)) return null;

  const verbPattern = '(?:voice\\s+)?(?:проигрывай|проиграй|воспроизводи|воспроизведи|производи|произведи|производит|включай|включи|запускай|запусти|play)';
  const soundWordPattern = '(?:звук|саунд|sound|soundboard|саундборд)';
  const unitPattern = `${REMINDER_UNIT_PATTERN}|недел[юияь]*|weeks?|месяц(?:а|ев)?|months?`;
  const amountPattern = "(?:\\d+(?:[.,]\\d+)?|[a-zа-яё’'ʼ`]+)";

  const recurring = raw.match(new RegExp(`^${verbPattern}\\s+${soundWordPattern}\\s+(.+?)\\s+(?:кажд(?:ые|ый|ую|ое)|every)\\s+(${amountPattern})?\\s*(${unitPattern})\\s*$`, 'iu'));
  if (recurring?.[1]?.trim()) {
    const amount = recurring[2] ? parseAmount(recurring[2]) : 1;
    const unit = recurring[3];
    const intervalMs = amount ? Math.round(amount * recurringUnitToMs(unit)) : 0;
    const target = cleanSoundboardTarget(recurring[1]);
    if (!target) return { action: 'action_error', text: 'Какой soundboard-звук повторять?' };
    if (!intervalMs) return { action: 'action_error', text: 'Не понял период для soundboard. Пример: “проигрывай звук Arigato каждую минуту”.' };
    return {
      action: 'schedule_soundboard_sound',
      text: target,
      dueAt: Date.now() + intervalMs,
      repeatIntervalMs: intervalMs,
      repeatLabel: `каждые ${amount || 1} ${unit}`,
    };
  }

  const delayed = raw.match(new RegExp(`^${verbPattern}\\s+${soundWordPattern}\\s+(.+?)\\s+(?:через|in|after)\\s+(.+)$`, 'iu'));
  if (delayed?.[1]?.trim() && delayed?.[2]?.trim()) {
    const tail = delayed[2].trim();
    const withAmount = tail.match(new RegExp(`^(${amountPattern})\\s*(${REMINDER_UNIT_PATTERN})\\s*$`, 'iu'));
    const withoutAmount = tail.match(/^(секунду|минуту|час|день|сутки|second|minute|hour|day)$/iu);
    const amount = withAmount ? parseAmount(withAmount[1]) : (withoutAmount ? 1 : null);
    const unit = withAmount ? withAmount[2] : (withoutAmount ? withoutAmount[1] : '');
    const unitMs = unitToMs(unit);
    const target = cleanSoundboardTarget(delayed[1]);
    if (!target) return { action: 'action_error', text: 'Какой soundboard-звук включить позже?' };
    if (!amount || !unitMs) return { action: 'action_error', text: 'Не понял задержку. Пример: “проиграй звук Arigato через одну минуту”.' };
    return {
      action: 'schedule_soundboard_sound',
      text: target,
      dueAt: Date.now() + Math.round(amount * unitMs),
    };
  }

  return null;
}

function parseThirdPartyBotCommand(prompt) {
  const normalized = normalizeCommandText(prompt);
  if (!normalized) return null;
  const mentionsLofiRadio = /(?:lo\s*[- ]?\s*fi|ло\s*[- ]?\s*фи|лофи).{0,20}(?:radio|радио)/u.test(normalized);
  const mentionsOtherBot = /^(?:запусти|включи|поставь|play|start)\s+(?:бота?|пользовател[яья]|юзера?|bot|user)\s+.+/u.test(normalized)
    || mentionsLofiRadio;
  const asksBotCommand = /(?:команд[ао]й?\s+play|через\s+команду|slash|слэш|\/play|\bplay\b)/u.test(normalized);
  if (!mentionsLofiRadio && (!mentionsOtherBot || !asksBotCommand)) return null;
  return {
    action: 'action_error',
    text: 'Не могу запускать команды другого Discord-бота. Discord API не дает ботам нажимать /play или другие команды за пользователей. Могу включить soundboard-звук или, если добавим свой music-плеер, запускать радио сам.',
  };
}

function mentionsMusicTarget(text) {
  const normalized = normalizeCommandText(text);
  return /(?:музык\p{L}*|песн\p{L}*|трек\p{L}*|композици\p{L}*|радио|ло\s*[- ]?\s*фи|lo\s*[- ]?\s*fi|youtube|ютуб|you\s*tube|spotify|спотиф\p{L}*|плейлист|playlist|аудио|audio)/u.test(normalized);
}

function cleanMusicQuery(text) {
  return String(text || '')
    .replace(/[“”«»]/g, '"')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/^(?:пожалуйста\s+)?(?:найди\s+(?:и\s+)?(?:включи|поставь|запусти|проиграй)|включи|поставь|запусти|проиграй|play|start|put\s+on)\s+/iu, '')
    .replace(/^(?:мне|нам)\s+/iu, '')
    .replace(/^(?:песню|музыку|трек|композицию|радио|lo\s*[- ]?\s*fi\s*radio|ло\s*[- ]?\s*фи\s*радио|youtube\s+music|ютуб\s+музыку|spotify|спотифай|аудио)\s+/iu, '')
    .replace(/\s+(?:на|в|через)\s+(?:youtube\s+music|youtube|ютуб(?:е)?|spotify|спотифай)$/iu, '')
    .replace(/^(?:под\s+названием|с\s+названием|которая\s+называется|который\s+называется|called|named)\s+/iu, '')
    .replace(/^["']|["']$/g, '')
    .trim();
}

function parseMusicAction(prompt) {
  const raw = String(prompt || '').replace(/\s+/g, ' ').trim();
  const normalized = normalizeCommandText(raw);
  if (!normalized) return null;
  const musicMention = mentionsMusicTarget(raw);
  const soundboardMention = /(?:звук|саунд|soundboard|саундборд|звуков\p{L}*\s+панел)/u.test(normalized);
  if (soundboardMention && !musicMention) return null;
  const genericMusicPause = /^(?:поставь|ставь|поставить)\s+(?:это\s+|ее\s+|её\s+|его\s+|трек\s+|песню\s+|музыку\s+)?(?:на\s+)?паузу$/u.test(normalized)
    || /^pause\s+(?:the\s+)?music$/u.test(normalized);

  if (musicMention && /(?:очеред|queue|что\s+играет|сейчас\s+играет|now\s+playing|current\s+track|список\s+трек)/u.test(normalized)) {
    return { action: 'music_queue' };
  }

  const volumeMatch = normalized.match(/(?:громк\p{L}*|volume|звук\s+музык\p{L}*).{0,30}?(\d{1,3})\s*%?/u);
  if (musicMention && volumeMatch) {
    return { action: 'music_volume', value: Math.max(0, Math.min(150, Number(volumeMatch[1]))) };
  }
  if (musicMention && /(?:сделай|сделать|убавь|уменьши|потише|тише|lower|down)/u.test(normalized) && /(?:громк|музык|звук|volume)/u.test(normalized)) {
    return { action: 'music_volume', delta: -0.1 };
  }
  if (musicMention && /(?:сделай|сделать|добавь|увеличь|погромче|громче|raise|up)/u.test(normalized) && /(?:громк|музык|звук|volume)/u.test(normalized)) {
    return { action: 'music_volume', delta: 0.1 };
  }

  if ((musicMention || genericMusicPause) && /(?:пауза|паузу|приостанови|pause|hold)/u.test(normalized)) {
    return { action: 'music_pause' };
  }
  if (musicMention && /(?:продолжи|возобнови|сними\s+паузу|resume|continue|unpause)/u.test(normalized)) {
    return { action: 'music_resume' };
  }
  if (musicMention && /(?:следующ\p{L}*|пропусти|скип|skip|next)/u.test(normalized)) {
    return { action: 'music_skip' };
  }
  if (musicMention && /(?:выключи|отключи|останови|стоп|убери|stop|turn\s+off)/u.test(normalized)) {
    return { action: 'music_stop' };
  }

  const playIntent = /^(?:найди\s+(?:и\s+)?(?:включи|поставь|запусти|проиграй)|включи|поставь|запусти|проиграй|play|start|put\s+on)\b/iu.test(raw)
    || (musicMention && /(?:найди|поищи|search|find)/u.test(normalized) && /(?:включи|поставь|play|start)/u.test(normalized));
  if (playIntent && musicMention) {
    const query = cleanMusicQuery(raw);
    if (query) return { action: 'music_play', text: query };
    return { action: 'action_error', text: 'Что включить? Назови песню, музыку, радио или ссылку.' };
  }

  return null;
}

function parseMusicInterruptAction(prompt, session = null) {
  if (!isMusicLoaded(session)) return null;
  const raw = String(prompt || '').replace(/\s+/g, ' ').trim();
  const normalized = normalizeCommandText(raw);
  if (!normalized) return null;

  if (/^(?:стоп|stop|хватит|харош|хорош|остановись|замолчи|тихо|выключи|отключи)$/u.test(normalized)) {
    return { action: 'music_stop' };
  }
  if (/^(?:пауза|pause)$/u.test(normalized)) {
    return { action: 'music_pause' };
  }
  if (/^(?:дальше|продолжай|resume|continue|play)$/u.test(normalized)) {
    return { action: 'music_resume' };
  }

  const parsed = parseMusicAction(prompt);
  if (parsed && [
    'music_pause',
    'music_resume',
    'music_stop',
    'music_skip',
    'music_volume',
    'music_queue',
  ].includes(parsed.action)) {
    return parsed;
  }
  return null;
}

function isNoWakeMusicControl(prompt, session = null) {
  return Boolean(parseMusicInterruptAction(prompt, session));
}

const DISCORD_CHAT_SEND_VERB_PATTERN = '(?:отправь|отправи|скинь|скини|кинь|кини|напиши|пошли|закинь|закини|send|post|write)';
const DISCORD_CHAT_DEST_PATTERN = '(?:чат|текстов\\p{L}*\\s+канал|канал|text\\s+channel|chat)';
const WEB_SEARCH_VERB_PATTERN = '(?:найди|поищи|загугли|гуглани|пробей|посмотри|узнай|search|find|google|look\\s+up)';

function discordChatRegex(source, flags = 'iu') {
  return new RegExp(
    source
      .replaceAll('{{SEND}}', DISCORD_CHAT_SEND_VERB_PATTERN)
      .replaceAll('{{DEST}}', DISCORD_CHAT_DEST_PATTERN)
      .replaceAll('{{WEB}}', WEB_SEARCH_VERB_PATTERN),
    flags,
  );
}

function cleanDiscordWebQuery(text) {
  return String(text || '')
    .replace(discordChatRegex('^{{WEB}}\\s+'), '')
    .replace(/^(?:в\s+интернете|интернет|web)\s+/iu, '')
    .replace(/^(?:информацию|инфу|данные|сводку|ссылку\s+на\s+сайт|ссылку\s+на|ссылку|сайт)\s+(?:о|об|про|на|about)?\s*/iu, '')
    .replace(/^(?:о|об|про|about)\s+/iu, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function shouldUseWebForDiscordSend(text) {
  const normalized = normalizeCommandText(text);
  return /(?:ссылк|сайт|url|адрес|официальн|найди|поищи|загугли|интернет|web|search|find|google)/u.test(normalized);
}

function parseDiscordChatSendAction(prompt) {
  const raw = String(prompt || '').replace(/\s+/g, ' ').trim();
  const normalized = normalizeCommandText(raw);
  if (!normalized) return null;
  if (hasTelegramMention(normalized)) return null;

  const searchThenSend = raw.match(discordChatRegex('^{{WEB}}\\s+([\\s\\S]+?)\\s+(?:и\\s+)?{{SEND}}\\s+(?:это\\s+)?(?:в|во|на|to)\\s+{{DEST}}(?:\\s+(.+))?$'));
  if (searchThenSend?.[1]?.trim()) {
    return {
      action: 'web_search_send_message',
      text: cleanDiscordWebQuery(searchThenSend[1]),
      channel: searchThenSend[2]?.trim() || '',
    };
  }

  const sendToPlainChat = raw.match(discordChatRegex('^{{SEND}}\\s+(?:в|во|на|to)\\s+(?:чат|chat)\\s+([\\s\\S]+)$'));
  if (sendToPlainChat?.[1]?.trim()) {
    const text = sendToPlainChat[1].trim();
    return shouldUseWebForDiscordSend(text)
      ? { action: 'web_search_send_message', text: cleanDiscordWebQuery(text), channel: '' }
      : { action: 'send_message', text, channel: '' };
  }

  const sendToNamedChannel = raw.match(discordChatRegex('^{{SEND}}\\s+(?:в|во|на|to)\\s+(?:текстов\\p{L}*\\s+канал|канал|text\\s+channel)\\s+([^:,.]+?)\\s+([\\s\\S]+)$'));
  if (sendToNamedChannel?.[2]?.trim()) {
    const text = sendToNamedChannel[2].trim();
    return shouldUseWebForDiscordSend(text)
      ? { action: 'web_search_send_message', text: cleanDiscordWebQuery(text), channel: sendToNamedChannel[1]?.trim() || '' }
      : { action: 'send_message', text, channel: sendToNamedChannel[1]?.trim() || '' };
  }

  const sendBeforeDest = raw.match(discordChatRegex('^{{SEND}}\\s+([\\s\\S]+?)\\s+(?:в|во|на|to)\\s+{{DEST}}(?:\\s+(.+))?$'));
  if (sendBeforeDest?.[1]?.trim()) {
    const text = sendBeforeDest[1].trim();
    return shouldUseWebForDiscordSend(text)
      ? { action: 'web_search_send_message', text: cleanDiscordWebQuery(text), channel: sendBeforeDest[2]?.trim() || '' }
      : { action: 'send_message', text, channel: sendBeforeDest[2]?.trim() || '' };
  }

  return null;
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
  'list_reminders',
  'update_user_profile',
  'show_user_profile',
  'web_search_send_message',
  'schedule_soundboard_sound',
  'music_play',
  'music_pause',
  'music_resume',
  'music_stop',
  'music_skip',
  'music_volume',
  'music_queue',
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
    .replace(/\b(?:заметк\p{L}*|заметочк\p{L}*|note|notes)\b/gu, '')
    .replace(/\b(?:и|та|а|их|это|потом|сразу|на\s+свое\s+усмотрение|на\s+своё\s+усмотрение|любые|какие\s+угодно)\b/gu, ' ')
    .replace(/\b(?:запиши|записать|сохрани|сохранить|запомни|запомнить|добавь|добавить|оставь|оставить)\b/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseGenerateMemoryNotesCommand(prompt) {
  const normalized = normalizeCommandText(prompt);
  if (!/(заметк\p{L}*|notes?)/u.test(normalized)) return null;
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

const STREAM_TARGET_WORD_PATTERN = '(?:трансляц\\p{L}*|стрим\\p{L}*|демк\\p{L}*|демонстрац\\p{L}*|экран|шаринг|screen\\s*share|screenshare|stream(?:ing)?|video)';
const DISABLE_STREAM_VERB_PATTERN = '(?:выключи|отключи|выруби|убери|запрети|заблокируй|останови|прекрати|disable|stop|block)';
const ENABLE_STREAM_VERB_PATTERN = '(?:включи|разреши|верни|разблокируй|enable|allow)';

function streamCommandRegex(source) {
  return new RegExp(
    source
      .replaceAll('{{STREAM}}', STREAM_TARGET_WORD_PATTERN)
      .replaceAll('{{DISABLE}}', DISABLE_STREAM_VERB_PATTERN)
      .replaceAll('{{ENABLE}}', ENABLE_STREAM_VERB_PATTERN),
    'iu',
  );
}

function parseMemberStreamAction(prompt) {
  const normalized = normalizeCommandText(prompt);
  const patterns = [
    { action: 'disable_member_stream', re: streamCommandRegex('^{{DISABLE}}\\s+{{STREAM}}\\s+(?:пользователя|участника|юзера)\\s+(.+)$') },
    { action: 'disable_member_stream', re: streamCommandRegex('^{{DISABLE}}\\s+{{STREAM}}\\s+(?:у\\s+)?(.+)$') },
    { action: 'disable_member_stream', re: streamCommandRegex('^{{DISABLE}}\\s+(.+?)\\s+{{STREAM}}$') },
    { action: 'disable_member_stream', re: streamCommandRegex('^{{DISABLE}}\\s+(.+?)\\s+(?:стримить|транслировать|демонстрировать\\s+экран)$') },
    { action: 'enable_member_stream', re: streamCommandRegex('^{{ENABLE}}\\s+{{STREAM}}\\s+(?:пользователя|участника|юзера)\\s+(.+)$') },
    { action: 'enable_member_stream', re: streamCommandRegex('^{{ENABLE}}\\s+{{STREAM}}\\s+(?:у\\s+)?(.+)$') },
    { action: 'enable_member_stream', re: streamCommandRegex('^{{ENABLE}}\\s+(.+?)\\s+{{STREAM}}$') },
    { action: 'enable_member_stream', re: streamCommandRegex('^{{ENABLE}}\\s+(.+?)\\s+(?:стримить|транслировать|демонстрировать\\s+экран)$') },
  ];
  for (const { action, re } of patterns) {
    const match = normalized.match(re);
    const target = cleanMemberTargetText(match?.[1]);
    if (target) return { action, target };
  }
  return null;
}

function parseSimpleMemberAction(prompt) {
  const normalized = normalizeCommandText(prompt);
  const moveBackMatch = normalized.match(/^(?:верни|вернуть)\s+(.+?)?\s*(?:обратно|назад)(?:\s+(?:в|на)\s+(?:канал|войс|воис|voice))?$/u);
  if (moveBackMatch) {
    return {
      action: 'move_member_back',
      target: isPronounTarget(moveBackMatch[1]) ? '' : cleanMemberTargetText(moveBackMatch[1]),
    };
  }

  const moveMatch = normalized.match(/^(?:перемести|перенеси|перекинь|перетащи)\s+(.+?)\s+(?:в|на|до)\s+(.+)$/u);
  if (moveMatch?.[1]?.trim() && moveMatch?.[2]?.trim()) {
    return {
      action: 'move_member',
      target: cleanMemberTargetText(moveMatch[1]),
      channel: moveMatch[2].trim(),
    };
  }

  const memberStreamAction = parseMemberStreamAction(prompt);
  if (memberStreamAction) return memberStreamAction;

  const kickFromServerMatch = normalized.match(/^(?:исключи)\s+(.+?)\s+(?:с|со)\s+(?:сервера|server)$/u);
  if (kickFromServerMatch?.[1]?.trim()) {
    return { action: 'kick_member', target: cleanMemberTargetText(kickFromServerMatch[1]) };
  }

  const patterns = [
    { action: 'mute_member', re: /^(?:замуть|замут|зам ють|замють|мутни|заглуши|приглуши|выключи микрофон|отключи микрофон|mute)\s+(.+)$/u },
    { action: 'unmute_member', re: /^(?:размуть|размут|разглуши|верни микрофон|включи микрофон|unmute)\s+(.+)$/u },
    { action: 'disconnect_member', re: /^(?:отключи|отключить|выкинь|выкини|выкин|дисконнектни|дисконектни|дискон|disconnect)\s+(.+)$/u },
    { action: 'deafen_member', re: /^(?:оглуши|задефай|деафни)\s+(.+)$/u },
    { action: 'undeafen_member', re: /^(?:разоглуши|раздефай|андефни)\s+(.+)$/u },
    { action: 'kick_member', re: /^(?:кикни|кик|исключи|kick)\s+(.+)$/u },
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

  const discordChatAction = parseDiscordChatSendAction(prompt);
  if (discordChatAction) return discordChatAction;

  const generatedNotes = parseGenerateMemoryNotesCommand(prompt);
  if (generatedNotes) return generatedNotes;

  const userProfileAction = parseUserProfileCommand(prompt);
  if (userProfileAction) return userProfileAction;

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
  const listReminder = parseListRemindersCommand(prompt);
  if (listReminder) return listReminder;
  if (looksLikeReminderCreate(prompt)) {
    return {
      action: 'action_error',
      text: 'Похоже на напоминание, но я не понял дату или время. Пример: “бот напомни завтра в 10:00 проверить маршрут”.',
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
  const noteMatch = String(prompt || '').trim().match(/^(?:запиши\s+заметку|добавь\s+заметку|сделай\s+заметку|создай\s+заметку|оставь\s+заметку|сохрани\s+заметку|note|remember\s+note)\s*(?:что|:)?\s+(.+)$/iu);
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
  const fallbackListReminder = parseListRemindersCommand(prompt);
  if (fallbackListReminder) {
    return fallbackListReminder;
  }
  if (normalized.includes('отмени все напомин') || normalized.includes('очисти напомин') || normalized.includes('сбрось напомин')) {
    return { action: 'clear_reminders' };
  }
  const scheduledSound = parseSoundboardScheduleCommand(prompt);
  if (scheduledSound) return scheduledSound;
  const musicAction = parseMusicAction(prompt);
  if (musicAction) return musicAction;
  const thirdPartyBotCommand = parseThirdPartyBotCommand(prompt);
  if (thirdPartyBotCommand) return thirdPartyBotCommand;
  if ((normalized.includes('отключ') || normalized.includes('выкин') || normalized.includes('дискон')) && /(всех|all)/u.test(normalized)) {
    return { action: 'disconnect_all' };
  }
  if ((normalized.includes('замуть') || normalized.includes('зам ють') || normalized.includes('замут') || normalized.includes('мут')) && /(всех|all)/u.test(normalized)) {
    return { action: 'mute_all' };
  }
  if ((normalized.includes('размуть') || normalized.includes('размут')) && /(всех|all)/u.test(normalized)) {
    return { action: 'unmute_all' };
  }
  const moveAllMatch = normalized.match(/(?:перемести|перенеси|перекинь|перетащи)\s+(?:всех|all)\s+(?:в|на|до)\s+(.+)$/u);
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
  if (playSoundMatch?.[1]?.trim() && !/(?:микрофон|звука\s+(?:для|у))/.test(normalized)) {
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
  const createCategoryMatch = normalized.match(/^(?:создай|создать|create)\s+(?:(?:новую|new)\s+)?(?:категор\p{L}*|category)(?:\s+(.+))?$/u);
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
  const createVoiceMatch = normalized.match(/^(?:создай|создать|create)\s+(?:(?:новый|new)\s+)?(?:голосов\p{L}*\s+канал|войс\s+канал|воис\s+канал|voice\s+channel|войс|воис|voice)(?:\s+(.+))?$/u);
  if (createVoiceMatch) {
    return { action: 'create_voice_channel', text: cleanCreatedChannelName(createVoiceMatch[1], 'Новый voice') };
  }
  const createTextMatch = normalized.match(/^(?:создай|создать|create)\s+(?:(?:новый|new)\s+)?(?:текстов\p{L}*\s+канал|чат|text\s+channel)(?:\s+(.+))?$/u);
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
    || normalized.includes('ты здесь')
    || normalized.includes('ты на месте')
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
        + 'Схема: {"action":"...","target":"...","channel":"...","value":0,"text":"...","field":"...","dueAt":0,"repeatIntervalMs":0,"repeatLabel":"","range":"all|today|tomorrow|week|overdue","userOnly":false}. '
        + 'Доступные action: disconnect_member, disconnect_all, kick_member, ban_member, move_member, move_member_back, move_all_members, mute_member, unmute_member, mute_all, unmute_all, disable_member_stream, enable_member_stream, deafen_member, undeafen_member, timeout_member, untimeout_member, add_role, remove_role, create_role, delete_role, set_role_color, set_role_mentionable, set_role_hoist, set_nickname, lock_voice, unlock_voice, rename_voice, set_voice_limit, lock_text, unlock_text, rename_text, set_text_topic, pin_last_message, set_slowmode, clear_messages, send_message, web_search_send_message, create_text_channel, create_voice_channel, create_category, move_channel_to_category, create_thread, archive_thread, lock_thread, unlock_thread, delete_channel, create_invite, list_invites, delete_invite, list_members, list_roles, list_channels, play_soundboard_sound, schedule_soundboard_sound, list_soundboard_sounds, rename_soundboard_sound, delete_soundboard_sound, music_play, music_pause, music_resume, music_stop, music_skip, music_volume, music_queue, rename_server, telegram_send_message, telegram_send_note, telegram_search_and_send, telegram_send_last_answer, telegram_send_memory, telegram_send_reminders, telegram_list_chats, telegram_status, telegram_test, telegram_clear, remember_memory, remember_user_memory, generate_memory_notes, search_memory, delete_memory, list_reminders, update_user_profile, show_user_profile, show_status, show_limits, reset_memory, pause_listening, resume_listening, stop_speaking, delete_reminder, none. '
        + 'target это имя участника ровно как услышано, даже если ник смешанный русский/English/цифры или склонен: "досика" -> target "досика", "Dosikk" -> target "Dosikk". Если говорят "меня/мне", target="меня"; если говорят "себя/тебя/бота" в команде ассистенту, target="себя". channel это имя канала назначения или канала для действия. value это число: секунды для timeout/slowmode, лимит voice или количество сообщений. text это имя роли, новый ник, новое имя канала или текст сообщения. '
        + 'Основной язык команд русский; английский допустим только как отдельные слова, команды, ники или названия. Не подставляй команды на других языках. '
        + 'Если говорят "отключи/выкинь из войса" это disconnect_member, а "отключи всех" это disconnect_all. Если говорят "кикни/исключи" это kick_member. '
        + 'Если говорят "отключи микрофон/выключи микрофон/замуть" это mute_member, а не disconnect_member. "размуть/верни микрофон" это unmute_member. '
        + 'Если говорят "выключи/отключи/запрети трансляцию/стрим/демку/экран у пользователя X", это disable_member_stream, а не mute_member и не kick_member. "включи/разреши трансляцию/стрим/демку X" это enable_member_stream. '
        + 'Понимай разговорные и неточные варианты для всех команд: "выруби микрофон", "приглуши", "закинь/перекинь/перетащи в канал", "выкинь из войса", "почисти чат", "сделай комнату", "дай модерку", "сними роль", "поставь медленный режим", "поставь ограничение войса", "закрой комнату", "открой чат". '
        + 'Если говорят "замуть всех" это mute_all, а "таймаут на N" это timeout_member. Если говорят "перемести всех в канал" это move_all_members. "верни его/досика обратно" это move_member_back. '
        + '"проиграй/включи звук X", "саундборд X", "звук на звуковой панели X" это play_soundboard_sound и text=X. "проигрывай звук X каждую минуту" или "проиграй звук X через минуту" это schedule_soundboard_sound: text=X, dueAt не заполняй сам если не уверен; локальный parser обычно обработает. "покажи звуки" это list_soundboard_sounds. "переименуй/удали звук X" это rename_soundboard_sound/delete_soundboard_sound. '
        + '"включи/поставь песню/музыку/трек/радио X", "найди X на YouTube и включи", "play X" это music_play и text=X. "поставь музыку на паузу" это music_pause. "продолжи музыку" это music_resume. "выключи/останови музыку" это music_stop. "следующий трек/пропусти песню" это music_skip. "громкость музыки 50" это music_volume и value=50. "покажи очередь музыки" это music_queue. '
        + '"найди/поищи X и отправь в чат/текстовый канал Y" это web_search_send_message, text=X, channel=Y если назван. "отправь в чат ссылку на сайт X" это web_search_send_message. Обычное "напиши в чат X" это send_message. '
        + '"отправь/напиши/скинь/кинь/закинь/перекинь/продублируй X в телеграм/телегу/тг/telegram/telega", а также STT-варианты "телега", "тележка", это telegram_send_message и text=X. '
        + '"заметка/запиши заметку/сохрани заметку в телеграм X" это telegram_send_note и text=X. '
        + '"найди/поищи/загугли/пробей/узнай X и отправь/скинь/закинь в телеграм" это telegram_search_and_send и text=X. '
        + '"отправь/скинь/продублируй последний ответ/это/то что сказал в телеграм" это telegram_send_last_answer. "отправь память/напоминания в телеграм" это telegram_send_memory/telegram_send_reminders. "покажи телеграм чаты/айди/статус" это telegram_list_chats/telegram_status. '
        + '"создай инвайт" это create_invite. "покажи инвайты" это list_invites. "удали инвайт CODE" это delete_invite. "создай категорию X" это create_category. "перемести канал X в категорию Y" это move_channel_to_category. '
        + '"создай тред X" это create_thread. "архивируй/залочь/разлочь тред X" это archive_thread/lock_thread/unlock_thread. "покажи участников/роли/каналы" это list_members/list_roles/list_channels. '
        + '"переименуй сервер X" это rename_server. "покрась роль X в #ff0000" это set_role_color, role name в text, color в value или text. '
        + '"запомни/запиши заметку/сохрани X" это remember_memory и text=X. "придумай/сгенерируй N заметок и запиши/сохрани их" это generate_memory_notes, value=N, text=тема если названа. "запомни обо мне X" это remember_user_memory и text=X. "что ты помнишь про X/найди в памяти X/что я просил вчера" это search_memory и text=X. "удали заметку/память про X" это delete_memory и text=X. '
        + '"покажи мой профиль" это show_user_profile. "мой часовой пояс X" это update_user_profile field="timezone" text=X. "любимые темы X" это update_user_profile field="favoriteTopics" text=X. "стиль общения X" это update_user_profile field="communicationStyle" text=X. "частые задачи X" это update_user_profile field="frequentTasks" text=X. "привычные команды X" это update_user_profile field="habitualCommands" text=X. "персональная заметка X" это update_user_profile field="personalNotes" text=X. "предпочтения по шуткам X" это update_user_profile field="jokeTone" text=X. "называй меня X" это update_user_profile field="preferredName" text=X. '
        + '"какие/покажи/скажи/прочитай/назови/есть ли напоминания" это list_reminders. Если сказали "на сегодня", range="today"; "на завтра", range="tomorrow"; "на неделю", range="week"; "просроченные", range="overdue"; "мои/у меня/для меня/личные", userOnly=true. '
        + '"стоп/замолчи/хватит/остановись/харош" это stop_speaking. "удали напоминание про X" это delete_reminder и text=X. "сбрось диалог/новый диалог" это reset_memory. "покажи статус" это show_status. "покажи лимиты" это show_limits. '
        + 'Если команда не является действием Discord, action=none.',
    },
    { role: 'user', content: prompt },
  ];
  const modelsToTry = actionModelsToTry();
  let lastError = null;
  for (const model of modelsToTry) {
    try {
      const result = await createGroqChatCompletion({
        model,
        temperature: 0,
        max_completion_tokens: 220,
        messages,
      }, {
        queue: 'ai',
        label: 'action-parser',
        session: null,
        model,
      });
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
      field: parsed.field ? String(parsed.field) : '',
      value: Number.isFinite(Number(parsed.value)) && String(parsed.value ?? '').trim() !== ''
        ? Number(parsed.value)
        : (parsed.value === undefined || parsed.value === null ? 0 : String(parsed.value)),
      text: parsed.text ? String(parsed.text) : '',
      dueAt: Number.isFinite(Number(parsed.dueAt)) ? Number(parsed.dueAt) : 0,
      repeatIntervalMs: Number.isFinite(Number(parsed.repeatIntervalMs)) ? Number(parsed.repeatIntervalMs) : 0,
      repeatLabel: parsed.repeatLabel ? String(parsed.repeatLabel) : '',
      range: parsed.range ? String(parsed.range) : '',
      userOnly: Boolean(parsed.userOnly),
    };
  } catch (error) {
    console.error('action parse failed:', raw, error);
    return { action: 'none' };
  }
}

async function editEveryoneOverwrite(channel, overwrites, reason) {
  await channel.permissionOverwrites.edit(channel.guild.roles.everyone, overwrites, { reason });
}

async function waitForVerifiedState(check, { timeoutMs = 5000, intervalMs = 450 } = {}) {
  const startedAt = Date.now();
  let lastValue = null;
  let lastError = null;
  while (Date.now() - startedAt <= timeoutMs) {
    try {
      lastValue = await check();
      if (lastValue) return { ok: true, value: lastValue };
    } catch (error) {
      lastError = error;
    }
    await delay(intervalMs);
  }
  return { ok: false, value: lastValue, error: lastError };
}

async function fetchFreshMember(guild, memberId) {
  return guild.members.fetch({ user: memberId, force: true }).catch(() => null);
}

async function verifyMemberRole(member, roleId, expected) {
  return waitForVerifiedState(async () => {
    const fresh = await fetchFreshMember(member.guild, member.id);
    if (!fresh) return false;
    return fresh.roles.cache.has(roleId) === expected ? fresh : false;
  });
}

async function verifyRoleExists(guild, roleId, expected = true) {
  return waitForVerifiedState(async () => {
    const role = await guild.roles.fetch(roleId, { force: true }).catch(() => null);
    return Boolean(role) === expected ? (role || true) : false;
  });
}

async function verifyRoleProperty(guild, roleId, predicate) {
  return waitForVerifiedState(async () => {
    const role = await guild.roles.fetch(roleId, { force: true }).catch(() => null);
    if (!role) return false;
    return predicate(role) ? role : false;
  });
}

async function verifyVoiceMuteState(member, field, expected) {
  return waitForVerifiedState(async () => {
    const state = member.guild.voiceStates.cache.get(member.id);
    if (state && state[field] === expected) return state;
    const fresh = await fetchFreshMember(member.guild, member.id);
    return fresh?.voice?.[field] === expected ? fresh.voice : false;
  });
}

async function verifyMemberVoiceChannel(guild, memberId, channelId) {
  return waitForVerifiedState(async () => {
    const state = guild.voiceStates.cache.get(memberId);
    if (state?.channelId === channelId) return state;
    const fresh = await fetchFreshMember(guild, memberId);
    return fresh?.voice?.channelId === channelId ? fresh.voice : false;
  }, { timeoutMs: 8000, intervalMs: 500 });
}

async function verifyMemberDisconnected(guild, memberId) {
  return waitForVerifiedState(async () => {
    const state = guild.voiceStates.cache.get(memberId);
    if (state && !state.channelId) return state;
    const fresh = await fetchFreshMember(guild, memberId);
    return !fresh?.voice?.channelId ? (fresh || true) : false;
  }, { timeoutMs: 8000, intervalMs: 500 });
}

async function verifyMemberAbsent(guild, memberId) {
  return waitForVerifiedState(async () => {
    const fresh = await fetchFreshMember(guild, memberId);
    return fresh ? false : true;
  }, { timeoutMs: 8000, intervalMs: 500 });
}

async function verifyMemberTimeout(guild, memberId, expectedTimedOut) {
  return waitForVerifiedState(async () => {
    const fresh = await fetchFreshMember(guild, memberId);
    if (!fresh) return false;
    const timedOut = Boolean(fresh.communicationDisabledUntilTimestamp && fresh.communicationDisabledUntilTimestamp > Date.now());
    return timedOut === expectedTimedOut ? fresh : false;
  });
}

async function verifyGuildBan(guild, memberId, expected = true) {
  return waitForVerifiedState(async () => {
    const ban = await guild.bans.fetch(memberId).catch(() => null);
    return Boolean(ban) === expected ? (ban || true) : false;
  }, { timeoutMs: 8000, intervalMs: 500 });
}

async function verifyChannelExists(guild, channelId, expected = true) {
  return waitForVerifiedState(async () => {
    const channel = await guild.channels.fetch(channelId, { force: true }).catch(() => null);
    return Boolean(channel) === expected ? (channel || true) : false;
  });
}

async function verifyChannelName(guild, channelId, expectedName) {
  return waitForVerifiedState(async () => {
    const channel = await guild.channels.fetch(channelId, { force: true }).catch(() => null);
    if (!channel) return false;
    return channel.name === expectedName ? channel : false;
  });
}

async function verifyChannelUserLimit(guild, channelId, expectedLimit) {
  return waitForVerifiedState(async () => {
    const channel = await guild.channels.fetch(channelId, { force: true }).catch(() => null);
    if (!channel) return false;
    return Number(channel.userLimit || 0) === Number(expectedLimit || 0) ? channel : false;
  });
}

async function verifyTextSlowmode(guild, channelId, expectedSeconds) {
  return waitForVerifiedState(async () => {
    const channel = await guild.channels.fetch(channelId, { force: true }).catch(() => null);
    if (!channel) return false;
    return Number(channel.rateLimitPerUser || 0) === Number(expectedSeconds || 0) ? channel : false;
  });
}

async function verifyGuildName(guild, expectedName) {
  return waitForVerifiedState(async () => {
    const fresh = await client.guilds.fetch(guild.id).catch(() => null);
    return fresh?.name === expectedName ? fresh : false;
  });
}

function reminderStillExists(guildId, reminderId) {
  return getGuildState(guildId).reminders.some((item) => item.id === reminderId);
}

function verifyReminderStored(reminder) {
  return Boolean(reminder?.id && reminderStillExists(reminder.guildId, reminder.id));
}

function verifyReminderTimer(reminder) {
  return Boolean(reminder?.id && reminderTimers.has(reminder.id));
}

function verifyTelegramDelivery(sent) {
  const messages = Array.isArray(sent) ? sent : [];
  return messages.length > 0 && messages.every((message) => message?.message_id || message?.date);
}

function telegramDeliveryText(sent, actionText = 'сообщение') {
  const messages = Array.isArray(sent) ? sent : [];
  if (!verifyTelegramDelivery(messages)) {
    return `Telegram API не подтвердил доставку: ${actionText} могло не уйти.`;
  }
  return messages.length === 1
    ? `Telegram подтвердил доставку: ${actionText}.`
    : `Telegram подтвердил доставку: ${actionText}, частей: ${messages.length}.`;
}

function soundboardAcceptedText(soundName) {
  return `Discord принял запрос на soundboard-звук ${soundName}.`;
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
    const verified = await verifyMemberDisconnected(targetMember.guild, targetMember.id);
    if (!verified.ok) return `Отправил запрос на отключение ${targetMember.displayName}, но Discord не подтвердил выход из voice.`;
    return `Проверил: ${targetMember.displayName} отключен от голосового канала.`;
  } catch (error) {
    console.error('disconnect failed:', error);
    return `Не смог отключить ${targetMember.displayName}: ${error.message || error}`;
  }
}

function refreshSessionVoiceChannel(session, voiceChannel) {
  if (!session || !voiceChannel) return;
  session.voiceChannel = voiceChannel;
  session.knownVoiceMemberIds = new Set(getHumanVoiceMembers(session).map((member) => member.id));
}

function permissionOverwriteValue(overwrite, permission) {
  if (!overwrite) return null;
  if (overwrite.allow?.has(permission)) return true;
  if (overwrite.deny?.has(permission)) return false;
  return null;
}

function memberVoiceStreaming(session, member) {
  const state = session?.guild?.voiceStates?.cache?.get(member?.id);
  return Boolean(state?.streaming ?? member?.voice?.streaming);
}

function streamRestoreNotice(previousStreamValue) {
  if (!STREAM_DISABLE_RESTORE_MS || previousStreamValue === false) {
    return 'Право Stream уже было запрещено, автоматически не меняю.';
  }
  return `Право включить трансляцию вернется через ${Math.round(STREAM_DISABLE_RESTORE_MS / 1000)} секунд.`;
}

function scheduleStreamPermissionRestore(session, voiceChannel, targetMember, previousValue, reason) {
  if (!STREAM_DISABLE_RESTORE_MS || previousValue === false) return;
  const guildId = session.guild?.id;
  const channelId = voiceChannel.id;
  const memberId = targetMember.id;
  const memberName = targetMember.displayName;
  const channelName = voiceChannel.name;
  const timer = setTimeout(async () => {
    try {
      const channel = await session.guild.channels.fetch(channelId).catch(() => null);
      if (!channel || ![ChannelType.GuildVoice, ChannelType.GuildStageVoice].includes(channel.type)) return;
      const currentValue = permissionOverwriteValue(
        channel.permissionOverwrites.cache.get(memberId),
        PermissionFlagsBits.Stream,
      );
      if (currentValue !== false) return;
      await channel.permissionOverwrites.edit(
        memberId,
        { Stream: previousValue },
        { reason: `${reason}; auto-restore Stream permission` },
      );
      appendEvent('member_stream_permission_restored', {
        guildId,
        voiceChannelId: channelId,
        voiceChannelName: channelName,
        memberId,
        memberName,
        restoredValue: previousValue,
      });
      console.log(`restored stream permission member=${memberId} channel=${channelId} value=${previousValue}`);
    } catch (error) {
      console.error(`stream permission restore failed member=${memberId} channel=${channelId}:`, error);
      appendEvent('member_stream_permission_restore_failed', {
        guildId,
        voiceChannelId: channelId,
        memberId,
        message: error.message || String(error),
      });
    }
  }, STREAM_DISABLE_RESTORE_MS);
  timer.unref?.();
}

function waitForMemberVoiceChannel(guild, memberId, channelId, timeoutMs = 12_000) {
  const current = guild?.voiceStates?.cache?.get(memberId);
  if (current?.channelId === channelId) return Promise.resolve(current);

  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      client.off('voiceStateUpdate', onVoiceStateUpdate);
      resolve(null);
    }, timeoutMs);

    function onVoiceStateUpdate(oldState, newState) {
      const guildId = newState.guild?.id || oldState.guild?.id;
      const userId = newState.id || oldState.id;
      if (guildId !== guild.id || userId !== memberId) return;
      if (newState.channelId !== channelId) return;
      clearTimeout(timeout);
      client.off('voiceStateUpdate', onVoiceStateUpdate);
      resolve(newState);
    }

    client.on('voiceStateUpdate', onVoiceStateUpdate);
  });
}

async function moveVoiceMemberToChannel(session, targetMember, destination, reason) {
  const fromChannel = targetMember.voice.channel;
  if (targetMember.id !== client.user.id) {
    await targetMember.voice.setChannel(destination, reason);
    const verified = await verifyMemberVoiceChannel(session.guild, targetMember.id, destination.id);
    if (!verified.ok) {
      const actualChannelId = session.guild.voiceStates.cache.get(targetMember.id)?.channelId
        || targetMember.voice?.channelId
        || 'unknown';
      throw new Error(`Discord did not confirm moving ${targetMember.displayName} to ${destination.name}; current voice channel id: ${actualChannelId}`);
    }
    return fromChannel;
  }

  if (targetMember.voice.channelId !== destination.id) {
    await targetMember.voice.setChannel(destination, reason);
  }
  const movedState = await waitForMemberVoiceChannel(session.guild, targetMember.id, destination.id);
  if (!movedState) {
    const actualChannelId = session.guild.voiceStates.cache.get(targetMember.id)?.channelId
      || targetMember.voice?.channelId
      || 'unknown';
    throw new Error(`Discord did not move bot to ${destination.name}; current voice channel id: ${actualChannelId}`);
  }
  if (session.connection && session.connection.state.status !== VoiceConnectionStatus.Destroyed) {
    session.connection.rejoin({
      channelId: destination.id,
      selfDeaf: false,
      selfMute: false,
    });
    await entersState(session.connection, VoiceConnectionStatus.Ready, 20_000).catch((error) => {
      console.error('voice rejoin after bot move failed:', error);
    });
  }
  refreshSessionVoiceChannel(session, destination);
  appendEvent('bot_voice_moved', {
    guildId: session.guild?.id,
    fromChannelId: fromChannel?.id || null,
    fromChannelName: fromChannel?.name || null,
    toChannelId: destination.id,
    toChannelName: destination.name,
  });
  await writeStatusSnapshot();
  return fromChannel;
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
      .map((member) => profilePreferredName(member?.guild?.id, member) || member.displayName || member.user?.globalName || member.user?.username || '')
      .map((name) => String(name).replace(/\s+/g, ' ').trim())
      .filter(Boolean),
  )].slice(0, 12);
}

function displayMemberName(member) {
  return displayMemberNames([member])[0] || 'друг';
}

function shortenPresenceNameText(name) {
  const value = String(name || '').replace(/\s+/g, ' ').trim() || 'друг';
  if (charLength(value) <= 18) return value;
  return [...value].slice(0, 18).join('').replace(/\s+\S*$/u, '').trim() || [...value].slice(0, 18).join('').trim();
}

function presenceMemberName(member) {
  return shortenPresenceNameText(profilePreferredName(member?.guild?.id, member) || displayMemberName(member));
}

function shouldUsePresenceMemberNames(humanCount) {
  return Number(humanCount || 0) <= PRESENCE_NAME_ANNOUNCEMENT_MAX_MEMBERS;
}

function pickRandom(items) {
  return items[Math.floor(Math.random() * items.length)];
}

function shortenPresencePhrase(text) {
  const cleaned = sanitizeVoiceOutputText(stripMarkdownFormatting(text))
    .replace(/^(?:[-*]|\d+[.)])\s+/u, '')
    .replace(/^[«"“”']+|[»"“”']+$/gu, '')
    .replace(/\s+/g, ' ')
    .trim();
  if (charLength(cleaned) <= PRESENCE_ANNOUNCEMENT_MAX_CHARS) return cleaned;
  return [...cleaned]
    .slice(0, PRESENCE_ANNOUNCEMENT_MAX_CHARS)
    .join('')
    .replace(/\s+\S*$/u, '')
    .replace(/[,.!?;:]+$/u, '')
    .trim() || [...cleaned].slice(0, PRESENCE_ANNOUNCEMENT_MAX_CHARS).join('').trim();
}

function pickPresencePhrase(session, bucket, phrases) {
  const items = phrases.map(shortenPresencePhrase).filter(Boolean);
  if (!items.length) return '';
  session.presencePhraseHistory ||= new Map();
  const recent = session.presencePhraseHistory.get(bucket) || [];
  const available = items.filter((item) => !recent.includes(item));
  const phrase = pickRandom(available.length ? available : items);
  const historyLimit = Math.max(1, Math.min(6, items.length - 1));
  session.presencePhraseHistory.set(bucket, [phrase, ...recent.filter((item) => item !== phrase)].slice(0, historyLimit));
  return phrase;
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

function formatPresenceNameListForSpeech(names, limit = 2) {
  return names.slice(0, limit).map(shortenPresenceNameText).join(', ');
}

function formatShortList(items, limit = 20) {
  const list = items
    .map((item) => String(item || '').replace(/\s+/g, ' ').trim())
    .filter(Boolean);
  const shown = list.slice(0, limit);
  const tail = list.length > limit ? `\n...и еще ${list.length - limit}` : '';
  return shown.length ? `${shown.join('\n')}${tail}` : 'пусто';
}

function presenceGreetingStore() {
  const value = runtimeConfig.presenceGreetingLastSeen;
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    runtimeConfig.presenceGreetingLastSeen = {};
  }
  return runtimeConfig.presenceGreetingLastSeen;
}

function cleanupPresenceGreetingStore(now = Date.now()) {
  if (!PRESENCE_MEMBER_GREETING_COOLDOWN_MS) return;
  const store = presenceGreetingStore();
  const maxAge = Math.max(PRESENCE_MEMBER_GREETING_COOLDOWN_MS * 4, 48 * 60 * 60_000);
  let changed = false;
  for (const [key, timestamp] of Object.entries(store)) {
    const value = Number(timestamp);
    if (!Number.isFinite(value) || now - value > maxAge) {
      delete store[key];
      changed = true;
    }
  }
  if (changed) void saveRuntimeConfig();
}

function presenceGreetingKey(session, member) {
  return [
    session?.guild?.id || 'guild',
    session?.voiceChannel?.id || 'voice',
    member?.id || 'member',
  ].join(':');
}

function shouldGreetPresenceMember(session, member) {
  if (!PRESENCE_MEMBER_GREETING_COOLDOWN_MS) return true;
  const now = Date.now();
  cleanupPresenceGreetingStore(now);
  const key = presenceGreetingKey(session, member);
  const last = Number(presenceGreetingStore()[key] || 0);
  const allowed = !last || now - last >= PRESENCE_MEMBER_GREETING_COOLDOWN_MS;
  if (!allowed) {
    appendEvent('presence_greeting_skipped', {
      guildId: session?.guild?.id,
      voiceChannelId: session?.voiceChannel?.id,
      userId: member?.id,
      userName: displayMemberName(member),
      cooldownMs: PRESENCE_MEMBER_GREETING_COOLDOWN_MS,
      remainingMs: Math.max(0, PRESENCE_MEMBER_GREETING_COOLDOWN_MS - (now - last)),
    });
  }
  return allowed;
}

function rememberPresenceMemberGreeting(session, member) {
  if (!PRESENCE_MEMBER_GREETING_COOLDOWN_MS) return;
  const store = presenceGreetingStore();
  store[presenceGreetingKey(session, member)] = Date.now();
  cleanupPresenceGreetingStore();
  void saveRuntimeConfig();
}

function presenceMemberSearchText(member) {
  return [...new Set([
    displayMemberName(member),
    ...rawCandidateMemberNames(member),
    ...candidateMemberNames(member),
    ...candidateMemberSearchNames(member),
  ])]
    .filter(Boolean)
    .join(' ');
}

function cleanPresenceContextText(text, limit = 180) {
  const value = sanitizeVoiceOutputText(stripMarkdownFormatting(text))
    .replace(/\b(?:gsk|ghp|github_pat|MTQ)[A-Za-z0-9._-]{12,}\b/gu, '[секрет скрыт]')
    .replace(/\s+/g, ' ')
    .trim();
  if (charLength(value) <= limit) return value;
  return [...value].slice(0, limit).join('').replace(/\s+\S*$/u, '').trim();
}

function formatPresenceMemoryContext(session, members, perMemberLimit = 3) {
  const guildId = session?.guild?.id;
  if (!guildId) return '';
  const guildState = getGuildState(guildId);
  const memberList = (Array.isArray(members) ? members : [members]).filter(Boolean).slice(0, 4);
  const sections = [];

  for (const member of memberList) {
    const searchText = presenceMemberSearchText(member);
    const lines = [];
    const profile = getUserProfile(guildId, member.id, member);
    if (profile) {
      const profileLines = [
        profile.preferredName ? `обращение: ${profile.preferredName}` : '',
        profile.favoriteTopics?.length ? `темы: ${profile.favoriteTopics.slice(0, 4).join(', ')}` : '',
        profile.jokeTone ? `тон шуток: ${profile.jokeTone}` : '',
        profile.personalNotes?.length ? `профиль: ${profile.personalNotes.slice(-2).join('; ')}` : '',
      ].filter(Boolean);
      lines.push(...profileLines.map((text) => cleanPresenceContextText(text, 180)));
    }
    const userMemories = [...(guildState.userMemories?.[member.id] || [])]
      .slice(-perMemberLimit)
      .map((memory) => cleanPresenceContextText(memory.text))
      .filter(Boolean);
    if (userMemories.length) {
      lines.push(...userMemories.map((text) => `память: ${text}`));
    }

    const serverMemories = [...(guildState.memories || [])]
      .map((memory, index) => {
        const authorScore = memory.userId === member.id || similarity(memory.userName || '', displayMemberName(member)) >= 0.82 ? 0.45 : 0;
        const textScore = scoreTextRelevance(`${memory.userName || ''} ${memory.text || ''}`, searchText);
        return { memory, score: authorScore + textScore, index };
      })
      .filter((item) => item.score > 0.12)
      .sort((a, b) => b.score - a.score || b.index - a.index)
      .slice(0, perMemberLimit)
      .map(({ memory }) => cleanPresenceContextText(memory.text))
      .filter(Boolean);
    if (serverMemories.length) {
      lines.push(...serverMemories.map((text) => `заметка: ${text}`));
    }

    const reminders = [...(guildState.reminders || [])]
      .map((reminder, index) => {
        const ownerScore = reminder.userId === member.id || similarity(reminder.userName || '', displayMemberName(member)) >= 0.82 ? 0.45 : 0;
        const textScore = scoreTextRelevance(`${reminder.userName || ''} ${reminder.text || ''}`, searchText);
        return { reminder, score: ownerScore + textScore, index };
      })
      .filter((item) => item.score > 0.12)
      .sort((a, b) => b.score - a.score || b.index - a.index)
      .slice(0, 2)
      .map(({ reminder }) => {
        const text = cleanPresenceContextText(reminder.text, 140);
        return text ? `напоминание: ${text} (${formatDueTime(reminder.dueAt)})` : '';
      })
      .filter(Boolean);
    if (reminders.length) {
      lines.push(...reminders);
    }

    if (lines.length) {
      sections.push(`${presenceMemberName(member)}:\n${lines.slice(0, perMemberLimit + 2).join('\n')}`);
    }
  }

  return sections.join('\n\n').slice(0, 1200);
}

function buildFallbackMemberJoinAnnouncement(session, member) {
  const greeting = dayPartGreeting();
  const name = presenceMemberName(member);
  return pickPresencePhrase(session, 'member_join_named', [
    `${name}, ${greeting}.`,
    `${name}, привет.`,
    `${name}, рад слышать.`,
    `${name}, заходи.`,
    `${name}, вовремя.`,
    `${name}, войс бодрее.`,
    `${name}, подключение принято.`,
    `${name}, на связи.`,
  ]);
}

function buildFallbackBotJoinAnnouncement(session) {
  const names = displayMemberNames(getHumanVoiceMembers(session));
  if (!names.length) return '';
  if (names.length > PRESENCE_BOT_JOIN_NAMED_MAX_MEMBERS) {
    return pickPresencePhrase(session, 'bot_join_generic', [
      'Всем привет, я на месте.',
      'Подключился, слушаю вас.',
      'Я в голосовом.',
      'Зашел в войс.',
      'Я подключился.',
      'Ассистент в канале.',
      'Я на связи.',
      'Готов работать.',
    ]);
  }
  if (names.length === 1) {
    const name = shortenPresenceNameText(names[0]);
    return pickPresencePhrase(session, 'bot_join_single', [
      `${name}, я на месте.`,
      `${name}, привет.`,
      `${name}, я в войсе.`,
      `${name}, на связи.`,
    ]);
  }
  const namesText = formatPresenceNameListForSpeech(names, PRESENCE_BOT_JOIN_NAMED_MAX_MEMBERS);
  return pickPresencePhrase(session, 'bot_join_named', [
    `Всем привет. ${namesText}, я на месте.`,
    `${namesText}, привет, работаем.`,
    `${namesText}, я в голосовом.`,
    `${namesText}, ассистент на связи.`,
  ]);
}

async function generatePresenceAnnouncementFromAi(session, prompt, fallback, label) {
  const safeFallback = shortenPresencePhrase(fallback);
  if (!effectiveGroqApiKey()) return safeFallback;

  const modelsToTry = chatModelsToTry(getChatModel());
  let lastError = null;
  for (const [modelIndex, model] of modelsToTry.entries()) {
    try {
      const result = await createGroqChatCompletion({
        model,
        messages: [
          {
            role: 'system',
            content: [
              'Ты генерируешь короткие голосовые приветствия для закрытого Discord voice-чата.',
              'Русский язык по умолчанию, английские слова можно оставлять только как ники или термины.',
              'Стиль живой, дружеский, можно слегка смешно, но без длинных объяснений.',
              'Не произноси токены, API-ключи, пароли и длинные секретные строки.',
              'Без markdown, списков, кавычек и эмодзи. Верни только одну фразу.',
            ].join(' '),
          },
          { role: 'user', content: prompt },
        ],
        temperature: 0.9,
        max_completion_tokens: 80,
      }, {
        queue: 'ai',
        label,
        session,
        model,
      });
      trackGroqRateLimits(session.textChannel, label, result.response, model);
      const text = shortenPresencePhrase(trimAssistantReply(result.data?.choices?.[0]?.message?.content || '', PRESENCE_ANNOUNCEMENT_MAX_CHARS));
      if (text) return text;
    } catch (error) {
      lastError = error;
      trackGroqRateLimits(session.textChannel, label, error, model);
      if (isGroqRateLimitError(error)) markGroqModelOnCooldown(model, label, groqResetHeaderFromError(error, 'tokens'));
      if (GROQ_AUTO_MODEL_FALLBACK && shouldFallbackGroqModel(error) && modelIndex < modelsToTry.length - 1) {
        console.warn(`presence greeting model ${model} failed, trying fallback ${modelsToTry[modelIndex + 1]}:`, error.message || error);
        continue;
      }
      break;
    }
  }

  if (lastError) console.warn(`presence greeting generation failed (${label}), using fallback:`, lastError.message || lastError);
  return safeFallback;
}

async function buildMemberJoinAnnouncement(session, member, humanCount = 1) {
  const fallback = buildFallbackMemberJoinAnnouncement(session, member);
  const name = presenceMemberName(member);
  const context = formatPresenceMemoryContext(session, member);
  const prompt = [
    `Событие: пользователь ${name} присоединился к voice-каналу.`,
    `Сейчас в voice примерно ${humanCount} человек.`,
    context
      ? `Локальная память, заметки и напоминания про пользователя:\n${context}`
      : 'Локального контекста про пользователя нет.',
    `Сделай короткое приветствие до ${PRESENCE_ANNOUNCEMENT_MAX_CHARS} символов.`,
    `Обязательно назови ${name}. Если есть контекст, аккуратно зацепись за него. Если контекста нет, придумай разную дефолтную живую фразу.`,
    'Не говори, что ты смотрел память. Не выдумывай конкретные личные факты, которых нет в контексте.',
  ].join('\n');
  return generatePresenceAnnouncementFromAi(session, prompt, fallback, 'presence-member-join');
}

function buildMemberLeaveAnnouncement(session, member, humanCountBeforeLeave = 1) {
  if (!shouldUsePresenceMemberNames(humanCountBeforeLeave)) {
    return pickPresencePhrase(session, 'member_leave_generic', [
      'Кто-то вышел из войса.',
      'Один человек вышел.',
      'В войсе стало чуть тише.',
      'Минус один в голосовом.',
      'Состав чуть меньше.',
      'Кто-то отключился.',
      'Один слот освободился.',
      'Минус один в войсе.',
    ]);
  }

  const name = presenceMemberName(member);
  return pickPresencePhrase(session, 'member_leave_named', [
    `${name} вышел.`,
    `${name} покинул войс.`,
    `${name} ушел.`,
    `${name} исчез из войса.`,
    `${name} отключился.`,
    `${name} вышел из канала.`,
    `${name} микрофон отдыхает.`,
    `${name} минус в войсе.`,
  ]);
}

async function buildBotJoinAnnouncement(session) {
  const humanMembers = getHumanVoiceMembers(session);
  const names = displayMemberNames(humanMembers);
  if (!names.length) return '';
  const fallback = buildFallbackBotJoinAnnouncement(session);
  const namedGreeting = names.length <= PRESENCE_BOT_JOIN_NAMED_MAX_MEMBERS;
  const context = formatPresenceMemoryContext(session, humanMembers, namedGreeting ? 2 : 1);
  const namesText = formatPresenceNameListForSpeech(names, PRESENCE_BOT_JOIN_NAMED_MAX_MEMBERS);
  const prompt = [
    'Событие: ассистент сам подключился к voice-каналу.',
    namedGreeting
      ? `В канале ${names.length} человек: ${namesText}. Нужно коротко поздороваться с каждым по имени.`
      : `В канале больше ${PRESENCE_BOT_JOIN_NAMED_MAX_MEMBERS} человек. Нужно одно общее приветствие без перечисления всех имен.`,
    context
      ? `Локальная память, заметки и напоминания по участникам:\n${context}`
      : 'Полезного локального контекста по участникам нет.',
    `Сделай одну короткую фразу до ${PRESENCE_ANNOUNCEMENT_MAX_CHARS} символов.`,
    'Если есть контекст, можно сделать короткую живую отсылку. Не говори, что ты смотрел память.',
  ].join('\n');
  return generatePresenceAnnouncementFromAi(session, prompt, fallback, 'presence-bot-join');
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

function enqueuePresenceAnnouncement(session, textOrBuilder, key) {
  if (!isPresenceAnnouncementsEnabled() || !textOrBuilder || !isSessionVoiceReady(session)) return;
  if (!rememberPresenceEvent(session, key)) return;

  session.presenceQueue = (session.presenceQueue || Promise.resolve())
    .catch(() => {})
    .then(async () => {
      if (PRESENCE_ANNOUNCEMENT_DELAY_MS) await delay(PRESENCE_ANNOUNCEMENT_DELAY_MS);
      if (!(await waitForPresenceSpeechSlot(session))) return;
      const text = typeof textOrBuilder === 'function' ? await textOrBuilder() : textOrBuilder;
      if (!isPresenceAnnouncementsEnabled() || !text || !isSessionVoiceReady(session)) return;
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
  if (isMusicLoaded(session)) {
    const stoppedMusic = stopMusic(session, { clearQueue: true, reason: 'voice_stop' });
    return stoppedMusic || Boolean(session.busy || session.interruptBusy);
  }
  const stopped = session.player?.stop(true) || false;
  return stopped || Boolean(session.busy || session.interruptBusy);
}

function clampMusicVolume(value) {
  const normalized = Number(value);
  if (!Number.isFinite(normalized)) return MUSIC_DEFAULT_VOLUME;
  return Math.max(0, Math.min(1.5, normalized));
}

function createMusicState() {
  return {
    queue: [],
    current: null,
    playing: false,
    paused: false,
    volume: clampMusicVolume(MUSIC_DEFAULT_VOLUME),
    resource: null,
    ffmpeg: null,
    lastError: '',
    lastStartedAt: 0,
    lastUpdatedAt: 0,
    stopping: false,
    advancing: false,
  };
}

function isMusicLoaded(session) {
  return Boolean(session?.music?.current);
}

function musicStatus(session) {
  if (!session?.music?.current) return 'idle';
  if (session.music.paused || session.player?.state?.status === AudioPlayerStatus.Paused) return 'paused';
  if (session.player?.state?.status === AudioPlayerStatus.Playing) return 'playing';
  return session.music.playing ? 'buffering' : 'idle';
}

function summarizeMusic(session) {
  const music = session?.music || createMusicState();
  return {
    status: musicStatus(session),
    volume: music.volume,
    volumePercent: Math.round((music.volume || 0) * 100),
    current: music.current ? {
      title: music.current.title,
      url: music.current.webpageUrl || music.current.url || '',
      durationSec: music.current.durationSec || 0,
      requestedBy: music.current.requestedBy || '',
      requestedAt: music.current.requestedAt || 0,
      source: music.current.source || 'youtube',
    } : null,
    queue: (music.queue || []).map((track) => ({
      title: track.title,
      url: track.webpageUrl || track.url || '',
      durationSec: track.durationSec || 0,
      requestedBy: track.requestedBy || '',
      requestedAt: track.requestedAt || 0,
      source: track.source || 'youtube',
    })),
    queueLength: music.queue?.length || 0,
    lastError: music.lastError || '',
    lastStartedAt: music.lastStartedAt || 0,
    lastUpdatedAt: music.lastUpdatedAt || 0,
  };
}

function formatDuration(seconds) {
  const total = Math.round(Number(seconds || 0));
  if (!total) return '';
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  return h ? `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}` : `${m}:${String(s).padStart(2, '0')}`;
}

function formatTrackTitle(track) {
  const title = String(track?.title || track?.webpageUrl || track?.url || 'трек').replace(/\s+/g, ' ').trim();
  const duration = formatDuration(track?.durationSec);
  return duration ? `${title} (${duration})` : title;
}

function formatMusicQueue(session) {
  const music = session?.music;
  if (!music?.current && !music?.queue?.length) return 'Очередь музыки пустая.';
  const lines = [];
  if (music.current) lines.push(`Сейчас: ${formatTrackTitle(music.current)} · ${musicStatus(session)} · громкость ${Math.round(music.volume * 100)}%`);
  for (const [index, track] of (music.queue || []).entries()) {
    lines.push(`${index + 1}. ${formatTrackTitle(track)}`);
  }
  return lines.join('\n');
}

function isProbablyUrl(value) {
  return /^https?:\/\//iu.test(String(value || '').trim());
}

function normalizeMusicQuery(value) {
  return String(value || '')
    .replace(/[“”«»]/g, '"')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/^["']|["']$/g, '')
    .trim();
}

function trackFromYtDlpJson(item, query, requestedBy) {
  const entry = item?.entries?.find(Boolean) || item;
  const url = entry?.webpage_url || entry?.original_url || entry?.url || (isProbablyUrl(query) ? query : '');
  return {
    id: entry?.id || crypto.randomUUID?.() || `${Date.now()}`,
    title: String(entry?.title || query || url || 'YouTube audio').replace(/\s+/g, ' ').trim(),
    webpageUrl: url,
    url,
    durationSec: Number(entry?.duration || 0) || 0,
    source: entry?.extractor_key || entry?.extractor || 'youtube',
    requestedBy,
    requestedAt: Date.now(),
  };
}

function ytDlpCommandCandidates() {
  return [
    MUSIC_YT_DLP_COMMAND,
    '/opt/media-tools/bin/yt-dlp',
    path.join(__dirname, '.venv', 'bin', 'yt-dlp'),
    'yt-dlp',
  ].filter(Boolean);
}

async function resolveMusicTrack(query, requestedBy = '') {
  const cleanQuery = normalizeMusicQuery(query);
  if (!cleanQuery) throw new Error('Что включить? Назови песню, музыку, радио или ссылку.');
  const target = isProbablyUrl(cleanQuery) ? cleanQuery : `ytsearch1:${cleanQuery}`;
  const { stdout } = await runFirstAvailableCommandCapture(
    ytDlpCommandCandidates(),
    ['--dump-json', '--no-playlist', '--no-warnings', '--skip-download', target],
    'yt-dlp',
    { timeoutMs: MUSIC_SEARCH_TIMEOUT_MS },
  );
  const jsonLine = stdout.split(/\r?\n/u).map((line) => line.trim()).filter(Boolean).at(-1);
  if (!jsonLine) throw new Error(`Не нашел музыку по запросу “${cleanQuery}”.`);
  const parsed = JSON.parse(jsonLine);
  const track = trackFromYtDlpJson(parsed, cleanQuery, requestedBy);
  if (!track.webpageUrl && !track.url) throw new Error(`Не нашел ссылку для “${cleanQuery}”.`);
  return track;
}

async function resolveMusicStreamUrl(track) {
  const target = track?.webpageUrl || track?.url;
  if (!target) throw new Error('У трека нет ссылки для проигрывания.');
  const { stdout } = await runFirstAvailableCommandCapture(
    ytDlpCommandCandidates(),
    ['-g', '-f', 'bestaudio/best', '--no-playlist', '--no-warnings', target],
    'yt-dlp',
    { timeoutMs: MUSIC_SEARCH_TIMEOUT_MS },
  );
  const url = stdout.split(/\r?\n/u).map((line) => line.trim()).find(Boolean);
  if (!url) throw new Error(`Не получил audio stream для “${track.title || target}”.`);
  return url;
}

function stopMusicProcess(session) {
  const child = session?.music?.ffmpeg;
  if (child && !child.killed) {
    child.kill('SIGTERM');
    setTimeout(() => {
      if (!child.killed) child.kill('SIGKILL');
    }, 1200).unref();
  }
  if (session?.music) session.music.ffmpeg = null;
}

function createMusicAudioResource(session, streamUrl) {
  const args = [
    '-hide_banner',
    '-loglevel', 'warning',
    '-reconnect', '1',
    '-reconnect_streamed', '1',
    '-reconnect_delay_max', '5',
    '-i', streamUrl,
    '-vn',
    '-f', 's16le',
    '-ar', '48000',
    '-ac', '2',
    'pipe:1',
  ];
  const child = spawn('ffmpeg', args, { stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = '';
  child.stderr.on('data', (chunk) => {
    stderr = `${stderr}${chunk.toString()}`.slice(-MUSIC_FFMPEG_LOG_LIMIT);
  });
  child.on('close', (code) => {
    if (code && session.music?.current) {
      session.music.lastError = `ffmpeg exited with code ${code}: ${stderr}`.slice(0, MUSIC_FFMPEG_LOG_LIMIT);
      appendEvent('music_ffmpeg_closed', {
        guildId: session.guild?.id,
        voiceChannelId: session.voiceChannel?.id,
        code,
        stderr,
        track: session.music.current?.title,
      });
    }
  });
  child.on('error', (error) => {
    if (session.music) session.music.lastError = error.message || String(error);
    appendEvent('music_ffmpeg_error', {
      guildId: session.guild?.id,
      voiceChannelId: session.voiceChannel?.id,
      message: error.message || String(error),
    });
  });
  const resource = createAudioResource(child.stdout, { inputType: StreamType.Raw, inlineVolume: true });
  resource.volume?.setVolume(clampMusicVolume(session.music.volume));
  return { child, resource };
}

async function startMusicTrack(session, track) {
  if (!session?.connection || session.connection.state.status === VoiceConnectionStatus.Destroyed) {
    throw new Error('Я не подключен к голосовому каналу.');
  }
  session.music ||= createMusicState();
  stopMusicProcess(session);
  session.music.lastError = '';
  session.music.playing = false;
  session.music.paused = false;
  const streamUrl = await resolveMusicStreamUrl(track);
  const { child, resource } = createMusicAudioResource(session, streamUrl);
  session.music.current = track;
  session.music.ffmpeg = child;
  session.music.resource = resource;
  session.music.playing = true;
  session.music.paused = false;
  session.music.lastStartedAt = Date.now();
  session.music.lastUpdatedAt = Date.now();
  markAssistantInteraction(session, 'music');
  session.player.play(resource);
  appendEvent('music_started', {
    guildId: session.guild?.id,
    voiceChannelId: session.voiceChannel?.id,
    title: track.title,
    url: track.webpageUrl || track.url,
    queueLength: session.music.queue.length,
  });
}

async function advanceMusicQueue(session, reason = 'idle') {
  if (!session?.music || session.music.advancing || session.music.stopping) return;
  session.music.advancing = true;
  try {
    stopMusicProcess(session);
    const previous = session.music.current;
    session.music.current = null;
    session.music.resource = null;
    session.music.playing = false;
    session.music.paused = false;
    const next = session.music.queue.shift();
    session.music.lastUpdatedAt = Date.now();
    appendEvent('music_track_finished', {
      guildId: session.guild?.id,
      voiceChannelId: session.voiceChannel?.id,
      reason,
      title: previous?.title || '',
      next: next?.title || '',
      queueLength: session.music.queue.length,
    });
    if (next) await startMusicTrack(session, next);
  } catch (error) {
    if (session.music) session.music.lastError = error.message || String(error);
    appendEvent('music_advance_failed', {
      guildId: session.guild?.id,
      voiceChannelId: session.voiceChannel?.id,
      reason,
      message: error.message || String(error),
    });
    console.error('music advance failed:', error);
  } finally {
    if (session?.music) session.music.advancing = false;
  }
}

function stopMusic(session, { clearQueue = true, reason = 'stop' } = {}) {
  if (!session?.music) return false;
  const hadCurrent = Boolean(session.music.current);
  const queueLength = session.music.queue.length;
  session.music.stopping = true;
  stopMusicProcess(session);
  session.music.current = null;
  session.music.resource = null;
  session.music.playing = false;
  session.music.paused = false;
  session.music.lastUpdatedAt = Date.now();
  if (clearQueue) session.music.queue = [];
  const stopped = session.player?.stop(true) || false;
  appendEvent('music_stopped', {
    guildId: session.guild?.id,
    voiceChannelId: session.voiceChannel?.id,
    reason,
    hadCurrent,
    clearedQueue: clearQueue,
    queueLength,
  });
  setTimeout(() => {
    if (session?.music) session.music.stopping = false;
  }, 0).unref();
  return hadCurrent || queueLength > 0 || stopped;
}

async function queueOrPlayMusic(session, track) {
  session.music ||= createMusicState();
  if (session.music.current) {
    if (session.music.queue.length >= MUSIC_MAX_QUEUE) {
      throw new Error(`Очередь заполнена: максимум ${MUSIC_MAX_QUEUE} треков.`);
    }
    session.music.queue.push(track);
    session.music.lastUpdatedAt = Date.now();
    appendEvent('music_queued', {
      guildId: session.guild?.id,
      voiceChannelId: session.voiceChannel?.id,
      title: track.title,
      queueLength: session.music.queue.length,
    });
    return { queued: true, position: session.music.queue.length };
  }
  await startMusicTrack(session, track);
  return { queued: false, position: 0 };
}

async function executeMusicAction(session, actorMember, parsed, { source = 'voice' } = {}) {
  if (!session?.voiceChannel?.id || !session?.connection || session.connection.state.status === VoiceConnectionStatus.Destroyed) {
    return { text: 'Я не подключен к голосовому каналу.', speak: false };
  }
  session.music ||= createMusicState();
  const requestedBy = actorMember?.displayName || actorMember?.user?.username || source;

  switch (parsed.action) {
    case 'music_play': {
      const query = String(parsed.text || parsed.value || parsed.channel || '').trim();
      if (!query) return { text: 'Что включить? Назови песню, радио или ссылку.', speak: false };
      const track = await resolveMusicTrack(query, requestedBy);
      const result = await queueOrPlayMusic(session, track);
      return {
        text: result.queued
          ? `Добавил в очередь ${result.position}: ${formatTrackTitle(track)}.`
          : `Включаю: ${formatTrackTitle(track)}.`,
        speak: false,
      };
    }
    case 'music_pause': {
      if (!session.music.current) return { text: 'Музыка сейчас не играет.', speak: false };
      const ok = session.player.pause(true);
      session.music.paused = true;
      session.music.playing = false;
      session.music.lastUpdatedAt = Date.now();
      return { text: ok ? 'Поставил музыку на паузу.' : 'Попробовал поставить музыку на паузу.', speak: false };
    }
    case 'music_resume': {
      if (!session.music.current) return { text: 'Музыка сейчас не загружена.', speak: false };
      const ok = session.player.unpause();
      session.music.paused = false;
      session.music.playing = true;
      session.music.lastUpdatedAt = Date.now();
      return { text: ok ? 'Продолжаю музыку.' : 'Попробовал продолжить музыку.', speak: false };
    }
    case 'music_stop': {
      const stopped = stopMusic(session, { clearQueue: true, reason: source });
      return { text: stopped ? 'Выключил музыку и очистил очередь.' : 'Музыка сейчас не играет.', speak: false };
    }
    case 'music_skip': {
      if (!session.music.current) return { text: 'Сейчас нечего пропускать.', speak: false };
      const skipped = session.music.current;
      stopMusicProcess(session);
      session.music.current = null;
      session.music.resource = null;
      session.music.playing = false;
      session.music.paused = false;
      session.player.stop(true);
      await advanceMusicQueue(session, 'skip');
      return {
        text: session.music.current
          ? `Пропустил ${formatTrackTitle(skipped)}. Включаю следующий трек.`
          : `Пропустил ${formatTrackTitle(skipped)}. Очередь закончилась.`,
        speak: false,
      };
    }
    case 'music_volume': {
      let volume = Number(parsed.value);
      if (parsed.delta) volume = (session.music.volume || MUSIC_DEFAULT_VOLUME) + Number(parsed.delta);
      if (volume > 1.5) volume /= 100;
      volume = clampMusicVolume(volume);
      session.music.volume = volume;
      session.music.resource?.volume?.setVolume(volume);
      session.music.lastUpdatedAt = Date.now();
      return { text: `Громкость музыки: ${Math.round(volume * 100)}%.`, speak: false };
    }
    case 'music_queue': {
      await sendText(session.textChannel, `Музыка:\n${formatMusicQueue(session)}`);
      return { text: 'Отправил очередь музыки в чат.', speak: false };
    }
    default:
      return null;
  }
}

function attachMusicPlayerHandlers(session) {
  session.player.on(AudioPlayerStatus.Idle, () => {
    if (!session.music?.current || session.music.stopping) return;
    void advanceMusicQueue(session, 'idle');
  });
  session.player.on('error', (error) => {
    if (!session.music?.current) return;
    session.music.lastError = error.message || String(error);
    appendEvent('music_player_error', {
      guildId: session.guild?.id,
      voiceChannelId: session.voiceChannel?.id,
      message: error.message || String(error),
      track: session.music.current?.title,
    });
    void advanceMusicQueue(session, 'player_error');
  });
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
  const unverified = removed.filter((reminder) => reminderStillExists(session.guild.id, reminder.id) || reminderTimers.has(reminder.id));
  if (unverified.length) return `Отправил запрос на удаление напоминаний, но локальная проверка не подтвердила удаление: ${unverified.length}.`;
  const list = removed.map((reminder, index) => `${index + 1}. ${reminder.text}`).join('\n');
  return removed.length === 1
    ? `Проверил: напоминание удалено: ${removed[0].text}`
    : `Проверил: удалено напоминаний: ${removed.length}.\n${list}`;
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
    ? `Проверил: запись памяти удалена: ${removed[0].memory.text}`
    : `Проверил: удалено записей памяти: ${removed.length}.\n${list}`;
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
	  'disable_member_stream',
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
  'отключ', 'выкин', 'дискон',
  'замут', 'замуть', 'зам ють', 'размут', 'размуть',
  'перемест', 'перенеси', 'перекин', 'верни',
  'кик', 'забан', 'бан',
  'создай', 'создать', 'удали', 'убери',
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

  const callNamePreference = await handleCallNamePreferenceCommand(session, actorMember, prompt);
  if (callNamePreference) return callNamePreference;

  const musicInterrupt = parseMusicInterruptAction(prompt, session);
  if (musicInterrupt) {
    appendEvent('music_voice_interrupt', {
      guildId: session.guild?.id,
      voiceChannelId: session.voiceChannel?.id,
      actorId: actorMember?.id,
      action: musicInterrupt.action,
      prompt,
    });
    return executeMusicAction(session, actorMember, musicInterrupt, { source: 'voice_interrupt' });
  }

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
    const selfTarget = await resolveSelfMemberTarget(session, actorMember, parsed.target);
    if (selfTarget) return selfTarget.error ? selfTarget : selfTarget.member;
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
      case 'update_user_profile': {
        const field = normalizeProfileFieldName(parsed.field);
        const patch = setProfileFieldFromText(field, parsed.text || parsed.value || '');
        if (!patch) return 'Не понял, что записать в профиль.';
        const profile = updateUserProfile(session.guild.id, actorMember, patch, 'voice_command');
        appendEvent('user_profile_updated', {
          guildId: session.guild.id,
          userId: actorMember?.id,
          field,
          source: 'voice_command',
        });
        return `Обновил профиль: ${USER_PROFILE_FIELD_LABELS[field] || field}.`;
      }
      case 'show_user_profile': {
        const profile = getUserProfile(session.guild.id, actorMember?.id, actorMember, { create: true });
        await sendText(session.textChannel, `Профиль ${profile.preferredName || profile.userName || 'пользователя'}:\n${formatUserProfile(profile)}`);
        return { text: 'Отправил твой профиль в чат.', speak: false };
      }
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
        const profilePatch = profilePatchFromPersonalMemory(text);
        if (profilePatch) updateUserProfile(session.guild.id, actorMember, profilePatch, 'personal_memory');
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
          const sent = await sendTelegramMessage(`Сохраненные заметки:\n${list}`);
          return verifyTelegramDelivery(sent)
            ? `Проверил: придумал, сохранил и Telegram подтвердил доставку. Заметок: ${saved.length}.`
            : `Заметки сохранены, но Telegram не подтвердил доставку.`;
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
        if (!verifyReminderStored(reminder) || !verifyReminderTimer(reminder)) {
          return `Запрос на напоминание записан, но локальная проверка расписания не подтвердилась. Проверь список напоминаний.`;
        }
        const text = reminder.repeatIntervalMs
          ? `Проверил: напоминание сохранено и поставлено на повтор "${reminder.repeatLabel || 'периодически'}". Первый раз ${formatDueTime(reminder.dueAt)}.`
          : `Проверил: напоминание сохранено. Сработает ${formatDueTime(reminder.dueAt)}.`;
        const speechText = reminder.repeatIntervalMs
          ? `Напоминание сохранено с повтором. Первый раз ${formatDueTimeForSpeech(reminder.dueAt)}.`
          : `Напоминание сохранено ${formatDueTimeForSpeech(reminder.dueAt)}.`;
        return { text, speechText };
      }
      case 'schedule_soundboard_sound': {
        const denied = requirePermission(PermissionFlagsBits.UseSoundboard, 'Use Soundboard');
        if (denied) return denied;
        if (!session.voiceChannel?.id) return 'Я не подключен к голосовому каналу.';
        if (!parsed.dueAt || !parsed.text?.trim()) return 'Не понял расписание soundboard-звука.';
        const result = await findSoundboardSound(session, parsed.text);
        if (result.error) return result.error;
        const soundName = result.sound.name || result.sound.soundId;
        const reminder = addReminderItem(session, actorMember, `soundboard: ${soundName}`, parsed.dueAt, {
          kind: REMINDER_KIND_SOUNDBOARD,
          soundboardSoundName: parsed.text,
          soundboardSoundId: result.sound.soundId,
          soundboardSourceGuildId: result.sound.guildId || null,
          repeatIntervalMs: parsed.repeatIntervalMs,
          repeatLabel: parsed.repeatLabel,
        });
        appendEvent('soundboard_reminder_added', {
          guildId: session.guild.id,
          userId: actorMember?.id,
          sound: soundName,
          dueAt: reminder.dueAt,
          repeatLabel: reminder.repeatLabel,
          voiceChannelId: reminder.voiceChannelId,
        });
        if (!verifyReminderStored(reminder) || !verifyReminderTimer(reminder)) {
          return `Запрос на расписание soundboard-звука записан, но локальная проверка таймера не подтвердилась. Проверь список напоминаний.`;
        }
        const text = reminder.repeatIntervalMs
          ? `Проверил: soundboard-звук ${soundName} сохранен в расписании "${reminder.repeatLabel || 'периодически'}". Первый раз ${formatDueTime(reminder.dueAt)}.`
          : `Проверил: soundboard-звук ${soundName} запланирован. Сработает ${formatDueTime(reminder.dueAt)}.`;
        const speechText = reminder.repeatIntervalMs
          ? `Звук ${soundName} сохранен с повтором. Первый раз ${formatDueTimeForSpeech(reminder.dueAt)}.`
          : `Звук ${soundName} запланирован ${formatDueTimeForSpeech(reminder.dueAt)}.`;
        return { text, speechText };
      }
      case 'music_play':
      case 'music_pause':
      case 'music_resume':
      case 'music_stop':
      case 'music_skip':
      case 'music_volume':
      case 'music_queue':
        return executeMusicAction(session, actorMember, parsed, { source: 'voice' });
      case 'list_reminders': {
        const options = {
          range: parsed.range || parsed.value || 'all',
          userOnly: Boolean(parsed.userOnly),
          userId: parsed.userOnly ? actorMember?.id : null,
          limit: 15,
        };
        return {
          text: `${reminderListTitle(options)}:\n${formatReminderList(session.guild.id, options)}`,
          speak: false,
        };
      }
      case 'delete_reminder': {
        return handleDeleteReminderCommand(session, parsed);
      }
      case 'clear_reminders': {
        const count = clearReminderItems(session.guild.id);
        clearPendingAction(session);
        const remaining = getGuildState(session.guild.id).reminders.length;
        if (remaining) return `Отправил запрос на очистку напоминаний, но локальная проверка видит оставшиеся: ${remaining}.`;
        return `Проверил: активные напоминания отменены. Удалено: ${count}.`;
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
        const requested = members.filter((_, index) => results[index]?.status === 'fulfilled');
        const checks = await Promise.allSettled(requested.map((member) => verifyMemberDisconnected(session.guild, member.id)));
        const ok = checks.filter((result) => result.status === 'fulfilled' && result.value?.ok).length;
        return ok === members.length
          ? `Проверил: отключены участники voice channel: ${ok}/${members.length}.`
          : `Запрос на отключение отправлен, но Discord подтвердил выход только ${ok}/${members.length}.`;
      }
      case 'kick_member': {
        const denied = requirePermission(PermissionFlagsBits.KickMembers, 'Kick Members');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        if (target.id === client.user.id) return 'Я не буду кикать самого себя.';
        const targetName = target.displayName;
        const targetId = target.id;
        await target.kick(reason);
        const verified = await verifyMemberAbsent(session.guild, targetId);
        if (!verified.ok) return `Отправил запрос на kick ${targetName}, но Discord все еще показывает участника на сервере.`;
        return `Проверил: ${targetName} кикнут с сервера.`;
      }
      case 'ban_member': {
        const denied = requirePermission(PermissionFlagsBits.BanMembers, 'Ban Members');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        if (target.id === client.user.id) return 'Я не буду банить самого себя.';
        const targetName = target.displayName;
        const targetId = target.id;
        await target.ban({ reason });
        const verified = await verifyGuildBan(session.guild, targetId, true);
        if (!verified.ok) return `Отправил запрос на ban ${targetName}, но Discord не подтвердил бан.`;
        return `Проверил: ${targetName} в бан-листе.`;
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
        await moveVoiceMemberToChannel(session, target, destination, reason);
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
        return `Проверил: ${target.displayName} перемещен в ${destination.name}.`;
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
        await moveVoiceMemberToChannel(session, target, destination, reason);
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
        return `Проверил: ${target.displayName} вернулся в ${destination.name}.`;
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
        const requested = members.filter((_, index) => results[index]?.status === 'fulfilled');
        const checks = await Promise.allSettled(requested.map((member) => verifyMemberVoiceChannel(session.guild, member.id, destination.id)));
        const ok = checks.filter((result) => result.status === 'fulfilled' && result.value?.ok).length;
        return ok === members.length
          ? `Проверил: в ${destination.name} перемещено ${ok}/${members.length}.`
          : `Запрос на перемещение отправлен, но Discord подтвердил ${ok}/${members.length}.`;
      }
      case 'mute_member':
      case 'unmute_member': {
        const denied = requirePermission(PermissionFlagsBits.MuteMembers, 'Mute Members');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        if (!target.voice?.channel) return `${target.displayName} сейчас не в голосовом канале.`;
        const muted = parsed.action === 'mute_member';
        await target.voice.setMute(muted, reason);
        const verified = await verifyVoiceMuteState(target, 'serverMute', muted);
        if (!verified.ok) {
          return `Отправил запрос на ${muted ? 'mute' : 'unmute'} ${target.displayName}, но Discord не подтвердил состояние микрофона.`;
        }
        return muted
          ? `Проверил: ${target.displayName} замьючен.`
          : `Проверил: ${target.displayName} размьючен.`;
      }
      case 'disable_member_stream':
      case 'enable_member_stream': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        const voiceChannel = target.voice?.channel || session.voiceChannel;
        if (!voiceChannel || ![ChannelType.GuildVoice, ChannelType.GuildStageVoice].includes(voiceChannel.type)) {
          return `${target.displayName} сейчас не в голосовом канале.`;
        }
        const enabled = parsed.action === 'enable_member_stream';
        const previousStreamValue = permissionOverwriteValue(
          voiceChannel.permissionOverwrites.cache.get(target.id),
          PermissionFlagsBits.Stream,
        );
        const wasStreaming = memberVoiceStreaming(session, target);
        await voiceChannel.permissionOverwrites.edit(
          target,
          { Stream: enabled ? true : false },
          { reason },
        );
        if (enabled) return `Разрешил ${target.displayName} включать трансляцию в ${voiceChannel.name}.`;

        scheduleStreamPermissionRestore(session, voiceChannel, target, previousStreamValue, reason);
        await delay(STREAM_DISABLE_VERIFY_DELAY_MS);
        const stillStreaming = memberVoiceStreaming(session, target);
        appendEvent('member_stream_disable_checked', {
          guildId: session.guild?.id,
          voiceChannelId: voiceChannel.id,
          voiceChannelName: voiceChannel.name,
          memberId: target.id,
          memberName: target.displayName,
          wasStreaming,
          stillStreaming,
          previousStreamValue,
        });
        console.log(`stream disable checked member=${target.id} wasStreaming=${wasStreaming} stillStreaming=${stillStreaming} previous=${previousStreamValue}`);
        const restoreNotice = streamRestoreNotice(previousStreamValue);
        if (wasStreaming && !stillStreaming) {
          return `Трансляцию у ${target.displayName} выключил. ${restoreNotice}`;
        }
        if (stillStreaming) {
          return `Запретил ${target.displayName} повторно включать трансляцию, но текущую Discord не оборвал. ${restoreNotice}`;
        }
        return `Запретил ${target.displayName} включать трансляцию в ${voiceChannel.name}. ${restoreNotice}`;
      }
      case 'mute_all':
      case 'unmute_all': {
        const denied = requirePermission(PermissionFlagsBits.MuteMembers, 'Mute Members');
        if (denied) return denied;
        const members = getManagedVoiceMembers(session, actorMember);
        if (!members.length) return 'Некого менять в текущем voice channel.';
        const muted = parsed.action === 'mute_all';
        const results = await Promise.allSettled(members.map((member) => member.voice.setMute(muted, reason)));
        const requested = members.filter((_, index) => results[index]?.status === 'fulfilled');
        const checks = await Promise.allSettled(requested.map((member) => verifyVoiceMuteState(member, 'serverMute', muted)));
        const ok = checks.filter((result) => result.status === 'fulfilled' && result.value?.ok).length;
        return muted
          ? `Проверил mute участников: ${ok}/${members.length}.`
          : `Проверил unmute участников: ${ok}/${members.length}.`;
      }
      case 'deafen_member':
      case 'undeafen_member': {
        const denied = requirePermission(PermissionFlagsBits.DeafenMembers, 'Deafen Members');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        if (!target.voice?.channel) return `${target.displayName} сейчас не в голосовом канале.`;
        const deafened = parsed.action === 'deafen_member';
        await target.voice.setDeaf(deafened, reason);
        const verified = await verifyVoiceMuteState(target, 'serverDeaf', deafened);
        if (!verified.ok) {
          return `Отправил запрос на ${deafened ? 'deafen' : 'undeafen'} ${target.displayName}, но Discord не подтвердил состояние звука.`;
        }
        return deafened
          ? `Проверил: звук для ${target.displayName} заглушен.`
          : `Проверил: звук для ${target.displayName} возвращен.`;
      }
      case 'timeout_member':
      case 'untimeout_member': {
        const denied = requirePermission(PermissionFlagsBits.ModerateMembers, 'Moderate Members');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        if (parsed.action === 'untimeout_member') {
          await target.timeout(null, reason);
          const verified = await verifyMemberTimeout(session.guild, target.id, false);
          if (!verified.ok) return `Отправил запрос на снятие таймаута с ${target.displayName}, но Discord не подтвердил состояние.`;
          return `Проверил: таймаут с ${target.displayName} снят.`;
        }
        const seconds = Math.max(1, Math.min(28 * 24 * 60 * 60, Math.round(parsed.value || 300)));
        await target.timeout(seconds * 1000, reason);
        const verified = await verifyMemberTimeout(session.guild, target.id, true);
        if (!verified.ok) return `Отправил запрос на таймаут ${target.displayName}, но Discord не подтвердил состояние.`;
        return `Проверил: ${target.displayName} в таймауте на ${seconds} секунд.`;
      }
      case 'add_role':
      case 'remove_role': {
        const denied = requirePermission(PermissionFlagsBits.ManageRoles, 'Manage Roles');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        const roleResult = await findRole(session, roleText());
        if (roleResult.error) return roleResult.error;
        const manageError = await botRoleManageError(session, target, roleResult.role);
        if (manageError) return manageError;
        if (parsed.action === 'add_role') {
          await target.roles.add(roleResult.role, reason);
          const verified = await verifyMemberRole(target, roleResult.role.id, true);
          if (!verified.ok) return `Отправил запрос на выдачу роли ${roleResult.role.name}, но Discord не подтвердил роль у ${target.displayName}.`;
          return `Проверил: у ${target.displayName} есть роль ${roleResult.role.name}.`;
        }
        await target.roles.remove(roleResult.role, reason);
        const verified = await verifyMemberRole(target, roleResult.role.id, false);
        if (!verified.ok) return `Отправил запрос на снятие роли ${roleResult.role.name}, но Discord не подтвердил снятие у ${target.displayName}.`;
        return `Проверил: у ${target.displayName} больше нет роли ${roleResult.role.name}.`;
      }
      case 'create_role': {
        const denied = requirePermission(PermissionFlagsBits.ManageRoles, 'Manage Roles');
        if (denied) return denied;
        const name = roleText();
        if (!name) return 'Какую роль создать?';
        const role = await session.guild.roles.create({ name: name.slice(0, 100), reason });
        const verified = await verifyRoleExists(session.guild, role.id, true);
        if (!verified.ok) return `Discord вернул роль ${role.name}, но повторная проверка ее не нашла.`;
        return `Проверил: роль ${role.name} создана.`;
      }
      case 'delete_role': {
        const denied = requirePermission(PermissionFlagsBits.ManageRoles, 'Manage Roles');
        if (denied) return denied;
        const roleResult = await findRole(session, roleText());
        if (roleResult.error) return roleResult.error;
        const manageError = await botRoleManageError(session, null, roleResult.role);
        if (manageError) return manageError;
        const roleName = roleResult.role.name;
        const roleId = roleResult.role.id;
        await roleResult.role.delete(reason);
        const verified = await verifyRoleExists(session.guild, roleId, false);
        if (!verified.ok) return `Отправил запрос на удаление роли ${roleName}, но Discord все еще показывает эту роль.`;
        return `Проверил: роль ${roleName} удалена.`;
      }
      case 'set_role_color': {
        const denied = requirePermission(PermissionFlagsBits.ManageRoles, 'Manage Roles');
        if (denied) return denied;
        const roleResult = await findRole(session, parsed.target || parsed.text || parsed.channel);
        if (roleResult.error) return roleResult.error;
        const manageError = await botRoleManageError(session, null, roleResult.role);
        if (manageError) return manageError;
        const colorText = String(parsed.value || parsed.channel || '').trim();
        const color = parseColorValue(colorText);
        if (!color) return 'Не понял цвет роли. Скажи цвет словом или hex, например #ff0000.';
        await roleResult.role.setColor(color, reason);
        const verified = await verifyRoleProperty(session.guild, roleResult.role.id, (role) => role.hexColor?.toLowerCase() === color.toLowerCase());
        if (!verified.ok) return `Отправил запрос на цвет роли ${roleResult.role.name}, но Discord не подтвердил цвет ${color}.`;
        return `Проверил: роль ${roleResult.role.name} имеет цвет ${color}.`;
      }
      case 'set_role_mentionable':
      case 'set_role_hoist': {
        const denied = requirePermission(PermissionFlagsBits.ManageRoles, 'Manage Roles');
        if (denied) return denied;
        const roleResult = await findRole(session, parsed.target || parsed.text || parsed.channel);
        if (roleResult.error) return roleResult.error;
        const manageError = await botRoleManageError(session, null, roleResult.role);
        if (manageError) return manageError;
        const enabled = parseBooleanIntent(String(parsed.value || parsed.channel || ''), true);
        if (parsed.action === 'set_role_mentionable') {
          await roleResult.role.setMentionable(enabled, reason);
          const verified = await verifyRoleProperty(session.guild, roleResult.role.id, (role) => role.mentionable === enabled);
          if (!verified.ok) return `Отправил запрос на mentionable для роли ${roleResult.role.name}, но Discord не подтвердил состояние.`;
          return enabled ? `Проверил: роль ${roleResult.role.name} теперь можно упоминать.` : `Проверил: роль ${roleResult.role.name} больше нельзя упоминать.`;
        }
        await roleResult.role.setHoist(enabled, reason);
        const verified = await verifyRoleProperty(session.guild, roleResult.role.id, (role) => role.hoist === enabled);
        if (!verified.ok) return `Отправил запрос на отображение роли ${roleResult.role.name}, но Discord не подтвердил состояние.`;
        return enabled ? `Проверил: роль ${roleResult.role.name} теперь показывается отдельно.` : `Проверил: роль ${roleResult.role.name} больше не показывается отдельно.`;
      }
      case 'set_nickname': {
        const denied = requirePermission(PermissionFlagsBits.ManageNicknames, 'Manage Nicknames');
        if (denied) return denied;
        const target = await getTarget();
        if (target.error) return target.error;
        const nickname = parsed.text.trim();
        if (!nickname) return 'Какой ник поставить?';
        const nextNick = nickname.slice(0, 32);
        await target.setNickname(nextNick, reason);
        const verified = await waitForVerifiedState(async () => {
          const fresh = await fetchFreshMember(session.guild, target.id);
          return fresh?.displayName === nextNick || fresh?.nickname === nextNick ? fresh : false;
        });
        if (!verified.ok) return `Отправил запрос на ник ${nextNick}, но Discord не подтвердил переименование ${target.displayName}.`;
        return `Проверил: ${target.displayName} теперь ${nextNick}.`;
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
        const nextName = name.slice(0, 100);
        const channelId = session.voiceChannel.id;
        await session.voiceChannel.setName(nextName, reason);
        const verified = await verifyChannelName(session.guild, channelId, nextName);
        if (!verified.ok) return `Отправил запрос на переименование voice channel в ${nextName}, но Discord не подтвердил новое имя.`;
        if (verified.value?.id === session.voiceChannel?.id) session.voiceChannel = verified.value;
        return `Проверил: voice channel переименован в ${nextName}.`;
      }
      case 'set_voice_limit': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        if (!session.voiceChannel) return 'Я не подключен к голосовому каналу.';
        const limit = Math.max(0, Math.min(99, Math.round(parsed.value)));
        const channelId = session.voiceChannel.id;
        await session.voiceChannel.setUserLimit(limit, reason);
        const verified = await verifyChannelUserLimit(session.guild, channelId, limit);
        if (!verified.ok) return `Отправил запрос на лимит voice channel ${limit}, но Discord не подтвердил значение.`;
        return limit ? `Проверил: лимит voice channel ${limit}.` : 'Проверил: лимит voice channel убран.';
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
        const channelId = session.textChannel.id;
        await session.textChannel.setName(name, reason);
        const verified = await verifyChannelName(session.guild, channelId, name);
        if (!verified.ok) return `Отправил запрос на переименование текстового канала в ${name}, но Discord не подтвердил новое имя.`;
        if (verified.value?.id === session.textChannel?.id) session.textChannel = verified.value;
        return `Проверил: текстовый канал переименован в ${name}.`;
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
        const verified = await verifyTextSlowmode(session.guild, targetChannel.id, seconds);
        if (!verified.ok) return `Отправил запрос на slowmode ${seconds} секунд, но Discord не подтвердил значение.`;
        return seconds ? `Проверил: slowmode ${seconds} секунд.` : 'Проверил: slowmode выключен.';
      }
      case 'clear_messages': {
        const denied = requirePermission(PermissionFlagsBits.ManageMessages, 'Manage Messages');
        if (denied) return denied;
        const count = Math.max(1, Math.min(100, Math.round(parsed.value || 10)));
        const targetChannel = channelText() ? await findTextChannel(session, channelText()) : session.textChannel;
        if (!targetChannel?.bulkDelete) return 'Этот канал не поддерживает очистку сообщений.';
        const deleted = await targetChannel.bulkDelete(count, true);
        return `Проверил: Discord подтвердил удаление сообщений: ${deleted.size}.`;
      }
      case 'send_message': {
        const denied = requirePermission(PermissionFlagsBits.SendMessages, 'Send Messages');
        if (denied) return denied;
        const text = parsed.text.trim();
        if (!text) return 'Что написать в чат?';
        const targetChannel = parsed.channel ? await findTextChannel(session, parsed.channel) : session.textChannel;
        if (!targetChannel) return `Не нашел текстовый канал “${parsed.channel}”.`;
        const sent = await sendText(targetChannel, text.slice(0, 1800));
        if (!sent?.id) return `Не получил подтверждение отправки сообщения в #${targetChannel.name}.`;
        return targetChannel.id === session.textChannel.id ? 'Проверил: сообщение отправлено в чат.' : `Проверил: сообщение отправлено в #${targetChannel.name}.`;
      }
      case 'web_search_send_message': {
        const denied = requirePermission(PermissionFlagsBits.SendMessages, 'Send Messages');
        if (denied) return denied;
        const query = String(parsed.text || parsed.channel || '').trim();
        if (!query) return 'Что найти и отправить в чат?';
        const targetChannel = parsed.channel ? await findTextChannel(session, parsed.channel) : session.textChannel;
        if (!targetChannel) return `Не нашел текстовый канал “${parsed.channel}”.`;
        const message = await generateDiscordWebSearchMessage(session, actorMember, query);
        const sent = await sendText(targetChannel, message);
        if (!sent?.id) return { text: `Нашел информацию, но Discord не подтвердил отправку в #${targetChannel.name}.`, speak: true };
        return { text: targetChannel.id === session.textChannel.id ? 'Нашел информацию. Проверил: сообщение отправлено в чат.' : `Нашел информацию. Проверил: сообщение отправлено в #${targetChannel.name}.`, speak: true };
      }
      case 'create_text_channel': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        const name = normalizeTextChannelName(parsed.text || parsed.channel);
        const created = await session.guild.channels.create({ name, type: ChannelType.GuildText, reason });
        const verified = await verifyChannelExists(session.guild, created.id, true);
        if (!verified.ok) return `Discord вернул канал #${created.name}, но повторная проверка его не нашла.`;
        return `Проверил: текстовый канал #${created.name} создан.`;
      }
      case 'create_voice_channel': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        const name = normalizeVoiceChannelName(parsed.text || parsed.channel);
        const created = await session.guild.channels.create({ name, type: ChannelType.GuildVoice, reason });
        const verified = await verifyChannelExists(session.guild, created.id, true);
        if (!verified.ok) return `Discord вернул голосовой канал ${created.name}, но повторная проверка его не нашла.`;
        return `Проверил: голосовой канал ${created.name} создан.`;
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
        const targetId = targetChannel.id;
        await targetChannel.delete(reason);
        const verified = await verifyChannelExists(session.guild, targetId, false);
        if (!verified.ok) {
          return deletingCurrentTextChannel
            ? { text: `Отправил запрос на удаление канала ${targetName}, но Discord не подтвердил удаление.`, send: false }
            : `Отправил запрос на удаление канала ${targetName}, но Discord не подтвердил удаление.`;
        }
        if (deletingCurrentTextChannel) {
          return { text: `Проверил: канал ${targetName} удален.`, send: false };
        }
        return `Проверил: канал ${targetName} удален.`;
      }
      case 'create_category': {
        const denied = requirePermission(PermissionFlagsBits.ManageChannels, 'Manage Channels');
        if (denied) return denied;
        const name = normalizeCategoryName(parsed.text || parsed.channel);
        const created = await session.guild.channels.create({ name, type: ChannelType.GuildCategory, reason });
        const verified = await verifyChannelExists(session.guild, created.id, true);
        if (!verified.ok) return `Discord вернул категорию ${created.name}, но повторная проверка ее не нашла.`;
        return `Проверил: категория ${created.name} создана.`;
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
        const verified = await waitForVerifiedState(async () => {
          const fresh = await session.guild.channels.fetch(targetChannel.id).catch(() => null);
          return fresh?.parentId === category.id ? fresh : false;
        });
        if (!verified.ok) return `Отправил запрос на перенос канала ${targetChannel.name}, но Discord не подтвердил категорию ${category.name}.`;
        return `Проверил: канал ${targetChannel.name} в категории ${category.name}.`;
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
        const verified = await verifyChannelExists(session.guild, thread.id, true);
        if (!verified.ok) return `Discord вернул тред ${thread.name}, но повторная проверка его не нашла.`;
        return `Проверил: тред ${thread.name} создан.`;
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
          const verified = await waitForVerifiedState(async () => {
            const fresh = await session.guild.channels.fetch(thread.id, { force: true }).catch(() => null);
            return fresh?.archived === true ? fresh : false;
          });
          if (!verified.ok) return `Отправил запрос на архив треда ${thread.name}, но Discord не подтвердил состояние.`;
          return `Проверил: тред ${thread.name} архивирован.`;
        }
        await thread.setLocked(parsed.action === 'lock_thread', reason);
        const locked = parsed.action === 'lock_thread';
        const verified = await waitForVerifiedState(async () => {
          const fresh = await session.guild.channels.fetch(thread.id, { force: true }).catch(() => null);
          return fresh?.locked === locked ? fresh : false;
        });
        if (!verified.ok) return `Отправил запрос на ${locked ? 'lock' : 'unlock'} треда ${thread.name}, но Discord не подтвердил состояние.`;
        return locked
          ? `Проверил: тред ${thread.name} залочен.`
          : `Проверил: тред ${thread.name} разлочен.`;
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
        const verified = await waitForVerifiedState(async () => {
          const fetched = await session.guild.invites.fetch(invite.code).catch(() => null);
          return fetched?.code === invite.code ? fetched : false;
        });
        const sent = await sendText(session.textChannel, `Invite: ${invite.url}`);
        if (!verified.ok) return { text: 'Discord вернул invite, но повторная проверка его не нашла.', speak: false };
        return { text: sent?.id ? 'Проверил: invite создан, ссылка отправлена в чат.' : 'Проверил: invite создан, но отправка ссылки в чат не подтвердилась.', speak: false };
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
        const verified = await waitForVerifiedState(async () => {
          const fetched = await session.guild.invites.fetch(code).catch(() => null);
          return fetched ? false : true;
        });
        if (!verified.ok) return `Отправил запрос на удаление invite ${code}, но Discord все еще его показывает.`;
        return `Проверил: invite ${code} удален.`;
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
        await postSoundboardSound(session, result.sound);
        return soundboardAcceptedText(result.sound.name || result.sound.soundId);
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
        const verified = await waitForVerifiedState(async () => {
          const sounds = await fetchSoundboardSounds(session);
          const sound = sounds.find((item) => item.soundId === updated.soundId && item.guildId === session.guild.id);
          return sound?.name === newName ? sound : false;
        });
        if (!verified.ok) return `Отправил запрос на переименование soundboard-звука в ${newName}, но Discord не подтвердил новое имя.`;
        return `Проверил: soundboard-звук переименован в ${updated.name}.`;
      }
      case 'delete_soundboard_sound': {
        const denied = requirePermission(PermissionFlagsBits.ManageGuildExpressions, 'Manage Expressions');
        if (denied) return denied;
        const result = await findSoundboardSound(session, parsed.text || parsed.channel);
        if (result.error) return result.error;
        if (result.sound.guildId !== session.guild.id) return 'Этот звук стандартный или с другого сервера, его нельзя удалить здесь.';
        const name = result.sound.name || result.sound.soundId;
        const soundId = result.sound.soundId;
        await session.guild.soundboardSounds.delete(result.sound, reason);
        const verified = await waitForVerifiedState(async () => {
          const sounds = await fetchSoundboardSounds(session);
          return sounds.some((item) => item.soundId === soundId && item.guildId === session.guild.id) ? false : true;
        });
        if (!verified.ok) return `Отправил запрос на удаление soundboard-звука ${name}, но Discord все еще его показывает.`;
        return `Проверил: soundboard-звук ${name} удален.`;
      }
      case 'rename_server': {
        const denied = requirePermission(PermissionFlagsBits.ManageGuild, 'Manage Server');
        if (denied) return denied;
        const name = String(parsed.text || parsed.channel || '').replace(/\s+/g, ' ').trim().slice(0, 100);
        if (!name) return 'Как назвать сервер?';
        await session.guild.setName(name, reason);
        const verified = await verifyGuildName(session.guild, name);
        if (!verified.ok) return `Отправил запрос на переименование сервера в ${name}, но Discord не подтвердил имя.`;
        return `Проверил: сервер переименован в ${name}.`;
      }
      case 'telegram_send_message': {
        const text = String(parsed.text || parsed.channel || '').trim();
        if (!text) return 'Что отправить в Telegram?';
        const sent = await sendTelegramMessage(text);
        return telegramDeliveryText(sent, 'сообщение');
      }
      case 'telegram_send_note': {
        const text = String(parsed.text || parsed.channel || '').trim();
        if (!text) return 'Какую заметку отправить в Telegram?';
        const sent = await sendTelegramMessage(formatTelegramNote(actorMember, text));
        return telegramDeliveryText(sent, 'заметку');
      }
      case 'telegram_search_and_send': {
        const query = String(parsed.text || parsed.channel || '').trim();
        if (!query) return 'Что найти и отправить в Telegram?';
        const summary = await generateTelegramWebSearchSummary(session, actorMember, query);
        const sent = await sendTelegramMessage(summary);
        return verifyTelegramDelivery(sent)
          ? 'Нашел информацию. Telegram подтвердил доставку.'
          : 'Нашел информацию, но Telegram не подтвердил доставку.';
      }
      case 'telegram_send_last_answer': {
        const text = getLastAssistantReply(session);
        if (!text) return 'Пока нет последнего ответа, который можно отправить в Telegram.';
        const sent = await sendTelegramMessage(text);
        return telegramDeliveryText(sent, 'последний ответ');
      }
      case 'telegram_send_memory': {
        const sent = await sendTelegramMessage(`Память Discord:\n${formatMemoryList(session.guild.id, actorMember?.id)}`);
        return telegramDeliveryText(sent, 'память Discord');
      }
      case 'telegram_send_reminders': {
        const sent = await sendTelegramMessage(`Напоминания Discord:\n${formatReminderList(session.guild.id)}`);
        return telegramDeliveryText(sent, 'напоминания Discord');
      }
      case 'telegram_list_chats': {
        const chats = await getRecentTelegramChats();
        const lines = chats.map(formatTelegramChat);
        const text = `Telegram chats:\n${formatShortList(lines, 30)}\nЕсли списка нет, напиши боту в Telegram /start или добавь его в группу и отправь туда сообщение.`;
        if (parsed.toTelegram) {
          const sent = await sendTelegramMessage(text);
          return telegramDeliveryText(sent, 'список Telegram-чатов');
        }
        await sendText(session.textChannel, text);
        return { text: 'Отправил список Telegram-чатов в Discord.', speak: false };
      }
      case 'telegram_status': {
        const text = `Telegram status:\n${formatTelegramStatus()}`;
        if (parsed.toTelegram) {
          const sent = await sendTelegramMessage(text);
          return telegramDeliveryText(sent, 'статус Telegram');
        }
        await sendText(session.textChannel, text);
        return { text: 'Отправил статус Telegram в Discord.', speak: false };
      }
      case 'telegram_test': {
        const sent = await sendTelegramMessage(`Тест из Discord от ${actorMember?.displayName || actorMember?.user?.username || 'пользователя'}.`);
        return telegramDeliveryText(sent, 'тестовое сообщение');
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
  const sttStats = {
    startedAt: Date.now(),
    attempts: 0,
    promptAttempts: 0,
    noPromptAttempts: 0,
    alternateAttempts: 0,
    transientErrors: 0,
    promptLengthRetries: 0,
    modelFallbacks: 0,
    promptChars: charLength(prompt),
    promptBytes: Buffer.byteLength(prompt || '', 'utf8'),
    modelsPlanned: modelsToTry.slice(0, 5),
    modelsTried: [],
    finalModel: null,
    finalLabel: null,
    finalLanguage: null,
    success: false,
    textLength: 0,
    error: null,
    durationMs: 0,
  };

  const recordSttFinished = (text = '', error = null) => {
    if (sttStats.finished) return text;
    sttStats.finished = true;
    sttStats.durationMs = Date.now() - sttStats.startedAt;
    sttStats.success = Boolean(text) && !error;
    sttStats.textLength = String(text || '').length;
    sttStats.error = error ? (error.message || String(error)).slice(0, 300) : null;

    if (session?.diagnostics) {
      session.diagnostics.sttRequests = (session.diagnostics.sttRequests || 0) + sttStats.attempts;
      session.diagnostics.sttTransientErrors = (session.diagnostics.sttTransientErrors || 0) + sttStats.transientErrors;
      session.diagnostics.sttPromptLengthRetries = (session.diagnostics.sttPromptLengthRetries || 0) + sttStats.promptLengthRetries;
      session.diagnostics.sttModelFallbacks = (session.diagnostics.sttModelFallbacks || 0) + sttStats.modelFallbacks;
      session.diagnostics.lastSttStats = {
        startedAt: sttStats.startedAt,
        durationMs: sttStats.durationMs,
        attempts: sttStats.attempts,
        promptAttempts: sttStats.promptAttempts,
        noPromptAttempts: sttStats.noPromptAttempts,
        alternateAttempts: sttStats.alternateAttempts,
        transientErrors: sttStats.transientErrors,
        promptLengthRetries: sttStats.promptLengthRetries,
        modelFallbacks: sttStats.modelFallbacks,
        promptChars: sttStats.promptChars,
        promptBytes: sttStats.promptBytes,
        modelsPlanned: sttStats.modelsPlanned,
        modelsTried: sttStats.modelsTried,
        finalModel: sttStats.finalModel,
        finalLabel: sttStats.finalLabel,
        finalLanguage: sttStats.finalLanguage,
        success: sttStats.success,
        textLength: sttStats.textLength,
        error: sttStats.error,
      };
      if (error) session.diagnostics.lastError = sttStats.error;
    }

    if (sttStats.transientErrors || sttStats.promptLengthRetries || sttStats.modelFallbacks || sttStats.attempts > 1) {
      appendEvent('stt_diagnostics', {
        guildId: session?.guild?.id,
        voiceChannelId: session?.voiceChannel?.id,
        userId,
        attempts: sttStats.attempts,
        promptAttempts: sttStats.promptAttempts,
        noPromptAttempts: sttStats.noPromptAttempts,
        alternateAttempts: sttStats.alternateAttempts,
        transientErrors: sttStats.transientErrors,
        promptLengthRetries: sttStats.promptLengthRetries,
        modelFallbacks: sttStats.modelFallbacks,
        modelsTried: sttStats.modelsTried,
        finalModel: sttStats.finalModel,
        finalLabel: sttStats.finalLabel,
        success: sttStats.success,
        textLength: sttStats.textLength,
        durationMs: sttStats.durationMs,
        error: sttStats.error,
      });
    }
    return text;
  };

  const transcribe = async (model, language, label, usePrompt = true) => {
    sttStats.attempts += 1;
    if (usePrompt && prompt) sttStats.promptAttempts += 1;
    else sttStats.noPromptAttempts += 1;
    if (label !== 'speech-to-text') sttStats.alternateAttempts += 1;
    if (!sttStats.modelsTried.includes(model)) sttStats.modelsTried.push(model);
    sttStats.finalModel = model;
    sttStats.finalLabel = label;
    sttStats.finalLanguage = language || 'auto';
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
          sttStats.promptLengthRetries += 1;
          console.warn(`${label} prompt too long for provider, retrying without prompt`);
          return transcribe(model, language, `${label}-no-prompt`, false);
        }
        if (!isTransientGroqConnectionError(error) || attempt >= STT_TRANSIENT_RETRIES) throw error;
        sttStats.transientErrors += 1;
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
            const retryLooksUsable = !isSttBoilerplateTranscript(retry);
            const improved = retryLooksUsable && (
              hasWakeWord(retry)
              || (isWakeListenWindow(session, Date.now(), userId) && !isSttPromptEchoTranscript(retry))
              || (isSttPromptEchoTranscript(first) && !isSttPromptEchoTranscript(retry) && normalizeCommandText(retry).split(/\s+/u).length >= 3)
            );
            if (improved) {
              console.log(`stt fallback improved transcript user=${userId}: "${first}" -> "${retry}"`);
              return recordSttFinished(retry);
            }
            if (!retryLooksUsable) {
              console.log(`stt fallback rejected boilerplate user=${userId}: "${first}" -> "${retry}"`);
            }
          }
        }
        return recordSttFinished(first);
      }
      if (getSttLanguage()) {
        const retry = await transcribeWithRetry(model, '', 'speech-to-text-retry');
        if (retry) return recordSttFinished(retry);
      }
    } catch (error) {
      lastModelError = error;
      trackGroqRateLimits(channel, 'speech-to-text', error, model);
      if (isGroqRateLimitError(error)) markGroqModelOnCooldown(model, 'speech-to-text', groqResetHeaderFromError(error, 'requests'));
      if (GROQ_AUTO_MODEL_FALLBACK && shouldFallbackGroqModel(error) && modelIndex < modelsToTry.length - 1) {
        sttStats.modelFallbacks += 1;
        console.warn(`STT model ${model} failed, trying fallback ${modelsToTry[modelIndex + 1]}:`, error.message || error);
        continue;
      }
      recordSttFinished('', error);
      throw error;
    }
  }
  if (lastModelError) {
    recordSttFinished('', lastModelError);
    throw lastModelError;
  }
  return recordSttFinished('');
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

function wantsOwnProfileTime(prompt) {
  const normalized = normalizeCommandText(prompt);
  return /(^|\s)(у\s+меня|мой|мое|моё|моя|my|mine)(\s|$)/u.test(normalized)
    || normalized === 'который час'
    || normalized === 'сколько времени'
    || normalized === 'what time';
}

function timePlaceFromUserProfile(session, actorMember) {
  const profile = actorMember ? getUserProfile(session?.guild?.id, actorMember.id, actorMember) : null;
  if (!profile?.timezone) return null;
  try {
    new Intl.DateTimeFormat('ru-RU', { timeZone: profile.timezone }).format(new Date());
  } catch {
    return null;
  }
  return {
    name: profile.preferredName || profile.userName || 'твой профиль',
    country: 'профиль пользователя',
    timezone: profile.timezone,
  };
}

function weatherSearchNames(location) {
  const raw = cleanupWeatherLocation(location);
  if (!raw) return [];
  const lower = raw.toLocaleLowerCase('ru');
  const names = [raw];
  if (/черниг|chernihiv|chernigov/.test(lower)) names.unshift('Чернигов', 'Chernihiv');
  if (/киев|kyiv|kiev/.test(lower)) names.unshift('Киев', 'Kyiv');
  if (/львов|lviv|lvov/.test(lower)) names.unshift('Львов', 'Lviv');
  if (/одесс|одес|odesa|odessa/.test(lower)) names.unshift('Одесса', 'Odesa');
  if (/хар(ь|к)ов|kharkiv|kharkov/.test(lower)) names.unshift('Харьков', 'Kharkiv');
  if (/днепр|dnipro|dnepr/.test(lower)) names.unshift('Днепр', 'Dnipro');
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

const DIRECT_TIME_LOCATIONS = [
  { patterns: [/герман/iu, /\bgermany\b/iu, /\bdeutschland\b/iu], name: 'Берлин', country: 'Германия', timezone: 'Europe/Berlin' },
  { patterns: [/украин/iu, /\bukraine\b/iu], name: 'Киев', country: 'Украина', timezone: 'Europe/Kyiv' },
  { patterns: [/киев/iu, /\bkyiv\b/iu, /\bkiev\b/iu], name: 'Киев', country: 'Украина', timezone: 'Europe/Kyiv' },
  { patterns: [/польш/iu, /\bpoland\b/iu], name: 'Варшава', country: 'Польша', timezone: 'Europe/Warsaw' },
  { patterns: [/франц/iu, /\bfrance\b/iu], name: 'Париж', country: 'Франция', timezone: 'Europe/Paris' },
  { patterns: [/итал/iu, /\bitaly\b/iu], name: 'Рим', country: 'Италия', timezone: 'Europe/Rome' },
  { patterns: [/испан/iu, /\bspain\b/iu], name: 'Мадрид', country: 'Испания', timezone: 'Europe/Madrid' },
  { patterns: [/британ/iu, /англи/iu, /\buk\b/iu, /\bunited kingdom\b/iu, /\bengland\b/iu], name: 'Лондон', country: 'Великобритания', timezone: 'Europe/London' },
  { patterns: [/сша/iu, /америк/iu, /\busa\b/iu, /\bunited states\b/iu], name: 'Вашингтон', admin1: 'DC', country: 'США', timezone: 'America/New_York' },
  { patterns: [/япон/iu, /\bjapan\b/iu], name: 'Токио', country: 'Япония', timezone: 'Asia/Tokyo' },
  { patterns: [/турц/iu, /\bturkey\b/iu, /\bturkiye\b/iu], name: 'Стамбул', country: 'Турция', timezone: 'Europe/Istanbul' },
];

function directTimeLocation(location) {
  const raw = cleanupTimeLocation(location);
  if (!raw) return null;
  const normalized = normalizeCommandText(raw);
  return DIRECT_TIME_LOCATIONS.find((item) => item.patterns.some((pattern) => pattern.test(raw) || pattern.test(normalized))) || null;
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

function normalizeTelegramChatLookup(value) {
  return normalizeCommandText(String(value || '').replace(/^@/u, '').trim());
}

function normalizeTelegramKnownChats(value) {
  const list = Array.isArray(value) ? value : [];
  const seen = new Set();
  const normalized = [];
  for (const item of list) {
    const id = normalizeTelegramChatId(item?.id ?? item?.chatId);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    normalized.push({
      id,
      type: String(item?.type || 'chat').slice(0, 40),
      title: String(item?.title || '').slice(0, 120),
      username: String(item?.username || '').slice(0, 80),
      first_name: String(item?.first_name || '').slice(0, 80),
      last_name: String(item?.last_name || '').slice(0, 80),
      seenAt: Number(item?.seenAt || 0),
    });
  }
  return normalized
    .sort((a, b) => Number(b.seenAt || 0) - Number(a.seenAt || 0))
    .slice(0, 30);
}

function telegramChatIdOrDefault(chatId = '') {
  return normalizeTelegramChatId(chatId) || getTelegramDefaultChatId();
}

function isTelegramChatNotFoundError(error) {
  return /Telegram getChat:.*chat not found|Bad Request:\s*chat not found/i.test(error?.message || String(error || ''));
}

function telegramChatSetupHint(reference = '') {
  const suffix = reference ? `\nВведено: ${reference}` : '';
  return [
    'Telegram не видит этот чат.',
    'Что сделать:',
    '1. Если это личка: открой своего Telegram-бота и отправь ему /start.',
    '2. Если это группа: добавь Telegram-бота в группу и отправь там любое сообщение.',
    '3. Потом в Discord выполни /telegram_chats и выбери id из списка.',
    '4. Сохрани его через /telegram_chat chat_id:<id>.',
    'Для supergroup id обычно начинается с -100.',
    suffix,
  ].filter(Boolean).join('\n');
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

  return await runQueuedTask(
    'telegram',
    `send:${targetChatId}:${chunks.length} chunk`,
    async () => {
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
    },
    {
      chatId: targetChatId,
      chunks: chunks.length,
      chars: chunks.reduce((sum, chunk) => sum + chunk.length, 0),
    },
  );
}

async function validateTelegramBotToken(token) {
  return await callTelegramApi('getMe', {}, { token });
}

function telegramChatMatchesReference(chat, reference) {
  const ref = normalizeTelegramChatLookup(reference);
  if (!ref || !chat) return false;
  const candidates = [
    chat.id,
    chat.username,
    chat.title,
    [chat.first_name, chat.last_name].filter(Boolean).join(' '),
  ].map(normalizeTelegramChatLookup).filter(Boolean);
  return candidates.some((candidate) => candidate === ref || candidate.includes(ref) || ref.includes(candidate));
}

async function getRecentTelegramChats({ token = getTelegramBotToken(), includeKnown = true } = {}) {
  const updates = await callTelegramApi('getUpdates', { limit: 60, timeout: 0 }, { token });
  const chats = new Map();
  const effectiveToken = String(token || '').trim();
  const runtimeToken = getTelegramBotToken();
  if (includeKnown && effectiveToken && effectiveToken === runtimeToken) {
    for (const chat of runtimeConfig.telegramKnownChats || []) {
      if (chat?.id) chats.set(String(chat.id), chat);
    }
  }
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

async function resolveTelegramChatReference(reference, { token = getTelegramBotToken() } = {}) {
  const target = normalizeTelegramChatId(reference);
  if (!target) return { chat: null, chatId: '' };

  try {
    const chat = await callTelegramApi('getChat', { chat_id: target }, { token });
    return { chat, chatId: normalizeTelegramChatId(chat?.id || target) };
  } catch (error) {
    if (!isTelegramChatNotFoundError(error)) throw error;
  }

  const recentChats = await getRecentTelegramChats({
    token,
    includeKnown: String(token || '').trim() === getTelegramBotToken(),
  }).catch(() => []);
  const exact = recentChats.find((chat) => normalizeTelegramChatId(chat.id) === target);
  const fuzzy = exact || recentChats.find((chat) => telegramChatMatchesReference(chat, target));
  if (fuzzy?.id) {
    return { chat: fuzzy, chatId: normalizeTelegramChatId(fuzzy.id) };
  }

  const available = recentChats.length
    ? `\nПоследние видимые чаты:\n${formatShortList(recentChats.map(formatTelegramChat), 10)}`
    : '';
  throw new Error(`${telegramChatSetupHint(reference)}${available}`);
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
  const allowed = getTelegramInboundAllowedChatIds();
  return [
    `Telegram token: ${getTelegramBotToken() ? `set (${tokenSource})` : 'not set'}`,
    `Default chat_id: ${chatId || 'not set'}`,
    `Inbound bridge: ${isTelegramInboundEnabled() ? 'on' : 'off'}`,
    `Allowed inbound chats: ${allowed.length ? allowed.join(', ') : 'not set'}`,
    `Plain text forwarding: ${isTelegramInboundPlainForwardEnabled() ? 'on' : 'off'}`,
    'Для настройки: /telegram_setup, затем /telegram_chat или /telegram_chats.',
  ].join('\n');
}

function telegramUpdateMessage(update) {
  return update?.message || update?.edited_message || update?.channel_post || null;
}

function telegramSenderName(message) {
  const from = message?.from || {};
  const username = from.username ? `@${from.username}` : '';
  const fullName = [from.first_name, from.last_name].filter(Boolean).join(' ').trim();
  return username || fullName || message?.chat?.title || `chat ${message?.chat?.id || 'unknown'}`;
}

function telegramMessageText(message) {
  return String(message?.text || message?.caption || '').replace(/\r/g, '').trim();
}

function telegramKnownChatFromMessage(message) {
  const chat = message?.chat;
  if (!chat?.id) return null;
  return {
    id: normalizeTelegramChatId(chat.id),
    type: chat.type || 'chat',
    title: chat.title || '',
    username: chat.username || '',
    first_name: chat.first_name || '',
    last_name: chat.last_name || '',
    seenAt: Date.now(),
  };
}

function rememberTelegramKnownChat(message) {
  const known = telegramKnownChatFromMessage(message);
  if (!known?.id) return;
  const list = normalizeTelegramKnownChats([known, ...(runtimeConfig.telegramKnownChats || [])]);
  if (JSON.stringify(list) !== JSON.stringify(runtimeConfig.telegramKnownChats || [])) {
    updateRuntimeConfig({ telegramKnownChats: list });
  }
}

function isTelegramUpdateOlderThanCurrentBoot(update) {
  const message = telegramUpdateMessage(update);
  const messageDateMs = Number(message?.date || 0) * 1000;
  return Boolean(messageDateMs && messageDateMs < startedAt - 60_000);
}

function isTelegramChatAllowed(chatId) {
  const id = normalizeTelegramChatId(chatId);
  if (!id) return false;
  return getTelegramInboundAllowedChatIds().includes(id);
}

function telegramHelpText() {
  return [
    'Telegram -> Discord команды:',
    '/status - статус бота',
    '/logs 20 - последние события',
    '/reminders - активные напоминания',
    '/voice - активные voice-каналы',
    '/channels - текстовые каналы',
    '/send текст - отправить в основной Discord-чат',
    '/send #канал текст или /send канал: текст - отправить в конкретный канал',
    '/cmd команда - выполнить команду бота через Discord-парсер',
    '/ask вопрос - спросить ИИ и получить ответ сюда',
    '',
    'Обычный текст из Telegram по умолчанию не пересылается. Для отправки в Discord используй /send текст.',
  ].join('\n');
}

function canBotSendInChannel(channel) {
  if (!channel?.guild) return Boolean(channel?.send);
  const me = channel.guild.members.me;
  const permissions = me ? channel.permissionsFor(me) : channel.permissionsFor(client.user?.id);
  if (!permissions) return Boolean(channel?.send);
  return permissions.has(PermissionFlagsBits.ViewChannel) && permissions.has(PermissionFlagsBits.SendMessages);
}

async function fetchDefaultTelegramGuild() {
  if (!client.isReady()) return null;
  if (DISCORD_GUILD_ID) {
    const guild = await client.guilds.fetch(DISCORD_GUILD_ID).catch(() => null);
    if (guild) return guild;
  }
  if (AUTO_JOIN_GUILD_ID) {
    const guild = await client.guilds.fetch(AUTO_JOIN_GUILD_ID).catch(() => null);
    if (guild) return guild;
  }
  return client.guilds.cache.first() || null;
}

async function findDefaultTextChannelForGuild(guild) {
  if (!guild) return null;
  if (AUTO_JOIN_TEXT_CHANNEL_ID) {
    const configured = await guild.channels.fetch(AUTO_JOIN_TEXT_CHANNEL_ID).catch(() => null);
    if (configured?.isTextBased?.() && canBotSendInChannel(configured)) return configured;
  }
  if (monitorChannel?.guild?.id === guild.id && canBotSendInChannel(monitorChannel)) return monitorChannel;
  const channels = await guild.channels.fetch().catch(() => null);
  const candidates = [...(channels?.values?.() || [])]
    .filter((channel) => channel
      && [ChannelType.GuildText, ChannelType.GuildAnnouncement].includes(channel.type)
      && channel.isTextBased?.()
      && canBotSendInChannel(channel))
    .sort((a, b) => (a.rawPosition ?? 0) - (b.rawPosition ?? 0));
  return candidates[0] || null;
}

function makeTelegramSessionLike(guild, textChannel) {
  const existing = sessions.get(guild.id);
  if (existing) {
    if (!existing.textChannel && textChannel) existing.textChannel = textChannel;
    return existing;
  }
  if (!telegramSessionHistories.has(guild.id)) telegramSessionHistories.set(guild.id, []);
  return {
    guild,
    textChannel,
    voiceChannel: null,
    connection: null,
    player: null,
    activeUsers: new Set(),
    history: telegramSessionHistories.get(guild.id),
    queue: Promise.resolve(),
    busy: false,
    interruptBusy: false,
    paused: false,
    lastReplyAt: 0,
    listenAfter: 0,
  };
}

async function getTelegramDiscordContext(channelHint = '') {
  const active = [...sessions.values()].find((session) => session?.guild && session?.textChannel);
  const guild = active?.guild || await fetchDefaultTelegramGuild();
  if (!guild) throw new Error('Discord guild не найден. Проверь DISCORD_GUILD_ID или наличие сервера у бота.');

  let textChannel = active?.textChannel || await findDefaultTextChannelForGuild(guild);
  const session = makeTelegramSessionLike(guild, textChannel);
  if (channelHint) {
    const hinted = await findTextChannel(session, channelHint);
    if (!hinted) throw new Error(`Не нашел Discord-канал "${channelHint}".`);
    textChannel = hinted;
    session.textChannel ||= hinted;
  }
  if (!textChannel?.send) throw new Error('Не нашел доступный текстовый Discord-канал для Telegram bridge.');
  setMonitorChannel(textChannel);
  return { guild, textChannel, session };
}

async function getTelegramCommandActor(guild) {
  return guild.members.me || await guild.members.fetchMe().catch(() => null);
}

function parseTelegramSendTarget(args) {
  const value = String(args || '').trim();
  if (!value) return { text: '', channel: '' };

  const hashMatch = value.match(/^#([^\s:]{1,100})\s+([\s\S]+)$/u);
  if (hashMatch) return { channel: hashMatch[1].trim(), text: hashMatch[2].trim() };

  const colonMatch = value.match(/^#?([^:\n]{2,100})\s*:\s*([\s\S]+)$/u);
  if (colonMatch) return { channel: colonMatch[1].trim(), text: colonMatch[2].trim() };

  return { text: value, channel: '' };
}

function normalizeTelegramCommandText(text) {
  const raw = String(text || '').trim();
  const match = raw.match(/^\/([a-zA-Zа-яА-ЯёЁ0-9_]+)(?:@\w+)?(?:\s+([\s\S]*))?$/u);
  if (!match) return null;
  return {
    command: match[1].toLowerCase(),
    args: String(match[2] || '').trim(),
  };
}

async function sendDiscordMessageFromTelegram(chatId, args, authorName) {
  const { channel, text } = parseTelegramSendTarget(args);
  if (!text) {
    await sendTelegramMessage('Что отправить в Discord? Пример: /send #bot привет', { chatId, disableWebPagePreview: true });
    return;
  }

  const context = await getTelegramDiscordContext(channel);
  const targetChannel = channel ? await findTextChannel(context.session, channel) : context.textChannel;
  if (!targetChannel?.send) throw new Error(`Не нашел Discord-канал "${channel}".`);
  const sent = await sendText(targetChannel, `Telegram ${authorName}: ${text}`);
  await sendTelegramMessage(
    sent?.id ? `Discord подтвердил отправку в #${targetChannel.name}.` : `Discord не подтвердил отправку в #${targetChannel.name}.`,
    { chatId, disableWebPagePreview: true },
  );
  appendEvent('telegram_inbound_discord_message', {
    chatId,
    discordGuildId: context.guild.id,
    discordChannelId: targetChannel.id,
    author: authorName,
  });
}

function formatTelegramSessionSummary(item) {
  return `${item.guildName || item.guildId}: voice=${item.voiceChannelName || 'none'}, state=${item.connectionState}, paused=${item.paused}, busy=${item.busy}, humans=${item.humanVoiceMembers}, trigger="${getWakeWord() || 'off'}"`;
}

function formatTelegramBotStatus(context) {
  const sessionsText = summarizeSessions();
  const current = context?.session?.connection ? formatSessionStatus(context.session) : 'Voice: не подключен.';
  return [
    `Discord bot: ${client.user?.tag || 'not ready'}`,
    current,
    sessionsText.length ? `Активные сессии:\n${sessionsText.map(formatTelegramSessionSummary).join('\n')}` : 'Активных voice-сессий нет.',
    formatTelegramStatus(),
  ].join('\n\n');
}

async function formatTelegramEventLog(limit = 20) {
  const safeLimit = Math.max(1, Math.min(50, Number(limit) || 20));
  const events = await storage.readEvents(safeLimit).catch(() => []);
  if (!events.length) return 'Логи событий пустые.';
  return events.slice(0, safeLimit).map((event) => {
    const time = event.ts ? new Date(event.ts).toLocaleString('ru-RU', { dateStyle: 'short', timeStyle: 'medium' }) : 'no time';
    const payload = event.payload ? JSON.stringify(event.payload).slice(0, 220) : '';
    return `${time} ${event.type}${payload ? `: ${payload}` : ''}`;
  }).join('\n');
}

async function formatActiveVoiceChannels(guild) {
  const channels = await guild.channels.fetch();
  const rows = [...channels.values()]
    .filter((channel) => channel && [ChannelType.GuildVoice, ChannelType.GuildStageVoice].includes(channel.type))
    .map((channel) => {
      const members = [...(channel.members?.values?.() || [])].filter((member) => !member.user?.bot);
      const connected = sessions.get(guild.id)?.voiceChannel?.id === channel.id ? ' · бот здесь' : '';
      return members.length ? `${channel.name}: ${members.length} чел. (${members.map(displayMemberName).join(', ')})${connected}` : `${channel.name}: пусто${connected}`;
    })
    .filter(Boolean);
  return rows.length ? rows.join('\n') : 'Voice-каналы не найдены.';
}

async function formatTextChannels(guild) {
  const channels = await guild.channels.fetch();
  const rows = [...channels.values()]
    .filter((channel) => channel && [ChannelType.GuildText, ChannelType.GuildAnnouncement].includes(channel.type))
    .sort((a, b) => (a.rawPosition ?? 0) - (b.rawPosition ?? 0))
    .map((channel) => `#${channel.name}${canBotSendInChannel(channel) ? '' : ' · нет права писать'}`);
  return rows.length ? formatShortList(rows, 50) : 'Текстовые каналы не найдены.';
}

function formatTelegramActionResult(result) {
  if (typeof result === 'string') return result;
  if (!result || typeof result !== 'object') return 'Команда выполнена.';
  return result.text || result.message || 'Команда выполнена.';
}

async function executeTelegramBotCommand(chatId, args, authorName) {
  const prompt = String(args || '').trim();
  if (!prompt) {
    await sendTelegramMessage('Какую команду выполнить? Пример: /cmd покажи статус', { chatId, disableWebPagePreview: true });
    return;
  }

  const context = await getTelegramDiscordContext();
  const actor = await getTelegramCommandActor(context.guild);
  if (!actor) throw new Error('Не смог получить Discord member бота для выполнения команды.');
  const parsed = await parseAction(prompt, context.textChannel);
  parsed.originalPrompt = prompt;
  parsed.source = 'telegram';
  if (!parsed.action || parsed.action === 'none') {
    await sendTelegramMessage('Не понял команду. Для вопроса к ИИ используй /ask вопрос.', { chatId, disableWebPagePreview: true });
    appendEvent('telegram_inbound_command_unparsed', { chatId, author: authorName, prompt });
    return;
  }

  const result = await executeParsedAction(context.session, actor, parsed);
  await sendTelegramMessage(formatTelegramActionResult(result), { chatId, disableWebPagePreview: true });
  appendEvent('telegram_inbound_command_executed', {
    chatId,
    discordGuildId: context.guild.id,
    action: parsed.action,
    author: authorName,
  });
}

async function answerTelegramAsk(chatId, args, authorName) {
  const prompt = String(args || '').trim();
  if (!prompt) {
    await sendTelegramMessage('Какой вопрос задать ИИ? Пример: /ask какая погода в Чернигове?', { chatId, disableWebPagePreview: true });
    return;
  }
  const context = await getTelegramDiscordContext();
  const answer = await askGroq(context.session, `Telegram ${authorName}`, prompt, null);
  await sendTelegramMessage(answer, { chatId, disableWebPagePreview: false });
  appendEvent('telegram_inbound_ask_answered', {
    chatId,
    discordGuildId: context.guild.id,
    author: authorName,
  });
}

async function handleTelegramInboundCommand(chatId, text, message) {
  const authorName = telegramSenderName(message);
  const command = normalizeTelegramCommandText(text);
  if (!command) {
    if (!isTelegramInboundPlainForwardEnabled()) {
      appendEvent('telegram_inbound_plain_ignored', {
        chatId,
        author: authorName,
        chars: String(text || '').length,
      });
      return;
    }
    await sendDiscordMessageFromTelegram(chatId, text, authorName);
    return;
  }

  const name = command.command;
  const args = command.args;
  if (['start', 'help', 'помощь'].includes(name)) {
    await sendTelegramMessage(telegramHelpText(), { chatId, disableWebPagePreview: true });
    return;
  }
  if (['status', 'статус'].includes(name)) {
    const context = await getTelegramDiscordContext().catch(() => null);
    await sendTelegramMessage(formatTelegramBotStatus(context), { chatId, disableWebPagePreview: true });
    return;
  }
  if (['logs', 'log', 'логи'].includes(name)) {
    const limit = args.match(/\d+/u)?.[0] || 20;
    await sendTelegramMessage(await formatTelegramEventLog(limit), { chatId, disableWebPagePreview: true });
    return;
  }
  if (['reminders', 'reminder', 'напоминания'].includes(name)) {
    const context = await getTelegramDiscordContext();
    await sendTelegramMessage(`Напоминания:\n${formatReminderList(context.guild.id)}`, { chatId, disableWebPagePreview: true });
    return;
  }
  if (['voice', 'voices', 'войс', 'войсы'].includes(name)) {
    const context = await getTelegramDiscordContext();
    await sendTelegramMessage(`Voice-каналы:\n${await formatActiveVoiceChannels(context.guild)}`, { chatId, disableWebPagePreview: true });
    return;
  }
  if (['channels', 'каналы'].includes(name)) {
    const context = await getTelegramDiscordContext();
    await sendTelegramMessage(`Текстовые каналы:\n${await formatTextChannels(context.guild)}`, { chatId, disableWebPagePreview: true });
    return;
  }
  if (['send', 'discord', 'chat', 'to', 'написать', 'отправить'].includes(name)) {
    await sendDiscordMessageFromTelegram(chatId, args, authorName);
    return;
  }
  if (['cmd', 'command', 'команда'].includes(name)) {
    await executeTelegramBotCommand(chatId, args, authorName);
    return;
  }
  if (['ask', 'ai', 'ии', 'вопрос'].includes(name)) {
    await answerTelegramAsk(chatId, args, authorName);
    return;
  }

  await sendTelegramMessage(`Не знаю такую Telegram-команду: /${name}\n\n${telegramHelpText()}`, { chatId, disableWebPagePreview: true });
}

async function processTelegramUpdate(update) {
  const message = telegramUpdateMessage(update);
  if (!message?.chat?.id) return;
  const chatId = normalizeTelegramChatId(message.chat.id);
  const text = telegramMessageText(message);
  rememberTelegramKnownChat(message);
  if (!text) return;

  if (!isTelegramChatAllowed(chatId)) {
    appendEvent('telegram_inbound_unauthorized', {
      chatId,
      sender: telegramSenderName(message),
      updateId: update.update_id,
    });
    return;
  }

  try {
    await handleTelegramInboundCommand(chatId, text, message);
    updateRuntimeConfig({
      telegramInboundLastAt: Date.now(),
      telegramInboundLastError: '',
      telegramInboundLastErrorAt: 0,
    });
  } catch (error) {
    console.error('telegram inbound command failed:', error);
    updateRuntimeConfig({
      telegramInboundLastError: error.message || String(error),
      telegramInboundLastErrorAt: Date.now(),
    });
    await sendTelegramMessage(`Ошибка Telegram -> Discord: ${error.message || error}`, { chatId, disableWebPagePreview: true })
      .catch((replyError) => console.error('telegram inbound error reply failed:', replyError));
  }
}

async function bootstrapTelegramInboundOffset() {
  const updates = await callTelegramApi('getUpdates', { limit: 100, timeout: 0 }, { timeoutMs: 10_000 });
  const maxUpdateId = Math.max(0, ...(updates || []).map((update) => Number(update.update_id || 0)));
  if (maxUpdateId) {
    updateRuntimeConfig({ telegramUpdateOffset: maxUpdateId + 1 });
    appendEvent('telegram_inbound_bootstrapped', { offset: maxUpdateId + 1, skipped: updates.length });
  }
}

async function pollTelegramInbound() {
  if (telegramInboundPollInProgress) return;
  if (Date.now() < telegramInboundBackoffUntil) return;
  if (!client.isReady()) return;
  if (!isTelegramInboundEnabled()) return;
  if (!getTelegramBotToken()) return;
  if (!getTelegramInboundAllowedChatIds().length) return;

  telegramInboundPollInProgress = true;
  try {
    const offset = Number(runtimeConfig.telegramUpdateOffset || 0);
    const payload = {
      limit: TELEGRAM_INBOUND_LIMIT,
      timeout: 0,
      allowed_updates: ['message', 'edited_message', 'channel_post'],
    };
    if (offset) payload.offset = offset;
    const updates = await callTelegramApi('getUpdates', payload, { timeoutMs: 10_000 });
    let nextOffset = offset;
    let skippedOld = 0;
    for (const update of updates || []) {
      nextOffset = Math.max(nextOffset, Number(update.update_id || 0) + 1);
      if (!offset && isTelegramUpdateOlderThanCurrentBoot(update)) {
        skippedOld += 1;
        continue;
      }
      await processTelegramUpdate(update);
    }
    if (skippedOld) appendEvent('telegram_inbound_old_updates_skipped', { skipped: skippedOld, nextOffset });
    if (nextOffset !== offset) updateRuntimeConfig({ telegramUpdateOffset: nextOffset });
  } catch (error) {
    console.error('telegram inbound poll failed:', error);
    telegramInboundBackoffUntil = Date.now() + 15_000;
    updateRuntimeConfig({
      telegramInboundLastError: error.message || String(error),
      telegramInboundLastErrorAt: Date.now(),
    });
  } finally {
    telegramInboundPollInProgress = false;
  }
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
      const result = await createGroqChatCompletion({
        model,
        messages,
        temperature: 0.25,
        max_completion_tokens: 900,
        compound_custom: {
          tools: {
            enabled_tools: ['web_search', 'visit_website'],
          },
        },
      }, {
        queue: 'webSearch',
        label: 'telegram-web-search',
        session,
        model,
      });
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

async function generateDiscordWebSearchMessage(session, actorMember, query) {
  const cleanQuery = String(query || '').replace(/\s+/g, ' ').trim();
  if (!cleanQuery) throw new Error('Что искать для Discord-чата?');
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
        'Ты готовишь короткое сообщение для Discord-чата по запросу пользователя. '
        + 'Всегда используй web_search и visit_website. Отвечай на языке запроса. '
        + 'Если просят ссылку, дай прямой URL и коротко подпиши, что это. '
        + 'Не выдумывай источники и не вставляй длинные списки. Максимум 5 коротких строк. '
        + `Текущая дата: ${today}, timezone Europe/Kyiv.`,
    },
    { role: 'user', content: `${userName} просит найти и отправить в Discord-чат: ${cleanQuery}` },
  ];

  let completion;
  let usedModel = getWebSearchModel();
  let lastError = null;
  const modelsToTry = webSearchModelsToTry(getWebSearchModel());
  for (const [modelIndex, model] of modelsToTry.entries()) {
    usedModel = model;
    try {
      console.log(`discord web search model=${model} query=${cleanQuery.slice(0, 160)}`);
      const result = await createGroqChatCompletion({
        model,
        messages,
        temperature: 0.2,
        max_completion_tokens: 500,
        compound_custom: {
          tools: {
            enabled_tools: ['web_search', 'visit_website'],
          },
        },
      }, {
        queue: 'webSearch',
        label: 'discord-web-send',
        session,
        model,
      });
      completion = result.data;
      trackGroqRateLimits(session?.textChannel, 'discord-web-send', result.response, model);
      break;
    } catch (error) {
      lastError = error;
      trackGroqRateLimits(session?.textChannel, 'discord-web-send', error, model);
      if (isGroqRateLimitError(error)) markGroqModelOnCooldown(model, 'discord-web-send', groqResetHeaderFromError(error, 'tokens'));
      if (
        GROQ_AUTO_MODEL_FALLBACK
        && (shouldFallbackGroqModel(error) || isRequestTooLargeError(error))
        && modelIndex < modelsToTry.length - 1
      ) {
        console.warn(`discord web send model ${model} failed, trying fallback ${modelsToTry[modelIndex + 1]}:`, error.message || error);
        continue;
      }
      throw error;
    }
  }
  if (!completion) throw lastError || new Error(`No Discord web-search completion from ${usedModel}`);
  const text = trimTelegramReply(completion.choices[0]?.message?.content || '', 1800);
  return text || `Не нашел надежный результат по запросу: ${cleanQuery}`;
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
  const direct = directTimeLocation(location);
  if (direct) return direct;
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

async function tryAnswerTimeQuery(prompt, session = null, actorMember = null) {
  if (!isTimeQuery(prompt)) return '';
  const location = extractTimeLocation(prompt, session);
  const place = location
    ? await geocodeTimeLocation(location)
    : (wantsOwnProfileTime(prompt) ? timePlaceFromUserProfile(session, actorMember) : null);
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

async function tryAnswerDeterministicQuery(session, prompt, actorMember = null) {
  const mathReply = tryAnswerMathQuery(prompt);
  if (mathReply) return mathReply;

  const normalized = normalizeCommandText(prompt);
  if (/(иерарх\p{L}*\s+рол|рол\p{L}*.{0,30}иерарх|missing permissions|manage roles|права.{0,30}рол)/u.test(normalized)) {
    return 'В Discord роль выше управляет ролями ниже себя. Даже с Administrator бот не сможет выдать или забрать роль, если его верхняя роль ниже или на одном уровне с этой ролью либо с верхней ролью участника. Решение: в настройках сервера перетащи роль бота выше ролей, которыми он должен управлять.';
  }

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
      ? await tryAnswerTimeQuery(prompt, session, actorMember)
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

function sanitizeVoiceOutputText(text) {
  return String(text || '')
    .replace(/ð[\u0080-\u00bf]{1,5}/gu, '')
    .replace(/[\u0080-\u009f]/gu, '')
    .replace(/[\u200d\ufe0f]/gu, '')
    .replace(/\p{Extended_Pictographic}/gu, '')
    .replace(/[ \t]+/gu, ' ')
    .replace(/\s+([,.!?;:])/gu, '$1')
    .replace(/\n{3,}/gu, '\n\n')
    .trim();
}

function trimAssistantReply(text, limit = VOICE_REPLY_MAX_CHARS) {
  let replyText = sanitizeVoiceOutputText(stripMarkdownFormatting(removeOpenEndedHookSentences(text)));
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

function cleanWakeAckPhrase(text) {
  const cleaned = stripMarkdownFormatting(text)
    .replace(/[«»"']/gu, '')
    .replace(/[\r\n]+/gu, ' ')
    .replace(/\s+/gu, ' ')
    .replace(/[?.!…]+$/u, '')
    .trim();
  if (!cleaned) return '';
  const first = cleaned.split(/[.!?…]/u)[0]?.trim() || cleaned;
  if (first.length <= WAKE_ACK_MAX_CHARS) return first;
  return first.slice(0, WAKE_ACK_MAX_CHARS).replace(/\s+\S*$/u, '').trim() || first.slice(0, WAKE_ACK_MAX_CHARS).trim();
}

function isValidWakeAckPhrase(phrase) {
  const normalized = normalizeCommandText(phrase);
  if (!normalized) return false;
  if (normalized.split(/\s+/u).length > 4) return false;
  return /(слуш|говор|готов|связ|жду|давай|вниматель|тут|здесь|окей|okay|yes|да)/u.test(normalized);
}

async function generateWakeAckPhrase(session, actorMember = null) {
  const fallback = () => pickRandom(WAKE_ACK_FALLBACK_PHRASES.length ? WAKE_ACK_FALLBACK_PHRASES : ['Слушаю', 'Говори']);
  if (!WAKE_ACK_AI_ENABLED) return fallback();

  const userName = profilePreferredName(session?.guild?.id, actorMember) || actorMember?.displayName || actorMember?.user?.username || 'человек';
  const messages = [
    {
      role: 'system',
      content:
        'Ты придумываешь одну короткую голосовую фразу-подтверждение для Discord-ассистента. '
        + 'Верни только саму фразу, без markdown, без кавычек, без объяснений. '
        + 'Фраза должна означать: я слушаю, говори. 1-3 слова. '
        + 'Основной язык русский. Можно одно короткое English-слово, если оно естественно для голосового чата.',
    },
    {
      role: 'user',
      content: `Ассистента только что позвал ${userName}. Дай случайную короткую фразу-подтверждение.`,
    },
  ];

  const modelsToTry = actionModelsToTry();
  let lastError = null;
  for (const [index, model] of modelsToTry.entries()) {
    try {
      const result = await getGroqClient().chat.completions.create({
        model,
        messages,
        temperature: 1,
        max_completion_tokens: 16,
      }).withResponse();
      trackGroqRateLimits(session?.textChannel, 'wake-ack', result.response, model);
      const phrase = cleanWakeAckPhrase(result.data?.choices?.[0]?.message?.content || '');
      if (isValidWakeAckPhrase(phrase)) return phrase;
      if (phrase) console.log(`wake ack rejected phrase="${phrase}"`);
    } catch (error) {
      lastError = error;
      trackGroqRateLimits(session?.textChannel, 'wake-ack', error, model);
      if (isGroqRateLimitError(error)) markGroqModelOnCooldown(model, 'wake-ack', groqResetHeaderFromError(error, 'tokens'));
      if (GROQ_AUTO_MODEL_FALLBACK && shouldFallbackGroqModel(error) && index < modelsToTry.length - 1) {
        console.warn(`wake ack model ${model} failed, trying fallback ${modelsToTry[index + 1]}:`, error.message || error);
        continue;
      }
      break;
    }
  }
  if (lastError) console.warn('wake ack generation failed, using fallback:', lastError.message || lastError);
  return fallback();
}

async function openWakeListening(session, userId, actorMember, transcript, source = 'voice') {
  if (!session) return;
  const userIdText = userId ? String(userId) : null;
  markWakeListen(session, userIdText);
  session.wakeAckInProgress = true;
  session.wakeAckUserId = userIdText;
  if (source === 'voice') session.busy = false;
  try {
    const phrase = await generateWakeAckPhrase(session, actorMember);
    appendEvent('wake_ack', {
      guildId: session.guild?.id,
      voiceChannelId: session.voiceChannel?.id,
      userId: userIdText,
      transcript,
      phrase,
      source,
    });
    if (session.wakeAckInProgress && String(session.wakeAckUserId || '') === String(userIdText || '')) {
      await speak(session, phrase).catch((error) => {
        console.error('wake ack speak failed:', error);
        void sendText(session.textChannel, `🤖 ${phrase}`).catch(() => {});
      });
    } else {
      console.log(`wake ack skipped by user speech user=${userIdText}: ${transcript}`);
    }
    console.log(`wake listen opened user=${userIdText}: ${transcript}; ack="${phrase}"`);
  } finally {
    if (String(session.wakeAckUserId || '') === String(userIdText || '')) {
      session.wakeAckInProgress = false;
      session.wakeAckUserId = null;
    }
  }
}

async function askGroq(session, userName, prompt, actorMember = null) {
  const useWebSearch = shouldUseWebSearch(prompt);
  const userProfile = actorMember ? getUserProfile(session.guild?.id, actorMember.id, actorMember) : null;
  const effectiveUserName = userProfile?.preferredName || userName;
  try {
    const deterministicReply = await tryAnswerDeterministicQuery(session, prompt, actorMember);
    if (deterministicReply) {
      const replyText = trimAssistantReply(deterministicReply, VOICE_REPLY_MAX_CHARS);
      session.history.push({ role: 'user', content: `${effectiveUserName}: ${prompt}` });
      session.history.push({ role: 'assistant', content: replyText });
      session.history.splice(0, Math.max(0, session.history.length - 12));
      return replyText;
    }
  } catch (error) {
    console.warn(`deterministic query fallback failed: ${error.message || error}`);
  }
  const memoryContext = useWebSearch ? '' : formatMemoryContext(session.guild?.id, prompt, actorMember?.id || null);
  const profileContext = formatUserProfileContext(session.guild?.id, actorMember);
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
    ...(profileContext ? [{
      role: 'system',
      content:
        `${profileContext}\nИспользуй профиль как предпочтения текущего пользователя: обращение, темы, стиль, часовой пояс, привычные команды и тон шуток. `
        + 'Не выдумывай отсутствующие поля профиля.',
    }] : []),
    ...(useWebSearch ? [] : session.history.slice(-8)),
    { role: 'user', content: `${effectiveUserName}: ${prompt}` },
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
      const result = await createGroqChatCompletion(request, {
        queue: useWebSearch ? 'webSearch' : 'ai',
        label: useWebSearch ? 'web-search' : 'chat',
        session,
        model,
      });
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
      { role: 'user', content: `${effectiveUserName}: ${prompt}` },
    ];
    const fallbackModels = chatModelsToTry(getChatModel());
    for (const [modelIndex, model] of fallbackModels.entries()) {
      usedModel = model;
      try {
        const result = await createGroqChatCompletion({
          model,
          messages: fallbackMessages,
          temperature: 0.35,
          max_completion_tokens: 180,
        }, {
          queue: 'ai',
          label: 'chat-fallback',
          session,
          model,
        });
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

  let replyText = trimAssistantReply(completion.choices[0]?.message?.content || '');
  if (!replyText) {
    appendEvent('assistant_empty_model_answer', {
      guildId: session.guild?.id,
      voiceChannelId: session.voiceChannel?.id,
      prompt,
      model: usedModel,
      web: useWebSearch,
    });
    console.warn(`empty ${useWebSearch ? 'web' : 'chat'} completion model=${usedModel} prompt="${prompt}"`);
    const fallbackModels = chatModelsToTry(getChatModel()).filter((model) => model !== usedModel);
    for (const [modelIndex, model] of fallbackModels.entries()) {
      try {
        const result = await createGroqChatCompletion({
          model,
          messages: [
            messages[0],
            {
              role: 'system',
              content: 'Предыдущая модель вернула пустой ответ. Верни непустой короткий ответ обычным текстом, без markdown.',
            },
            { role: 'user', content: `${effectiveUserName}: ${prompt}` },
          ],
          temperature: 0.35,
          max_completion_tokens: 180,
        }, {
          queue: 'ai',
          label: 'chat-empty-retry',
          session,
          model,
        });
        trackGroqRateLimits(session.textChannel, 'chat-empty-retry', result.response, model);
        replyText = trimAssistantReply(result.data?.choices?.[0]?.message?.content || '');
        if (replyText) break;
      } catch (error) {
        trackGroqRateLimits(session.textChannel, 'chat-empty-retry', error, model);
        if (isGroqRateLimitError(error)) markGroqModelOnCooldown(model, 'chat-empty-retry', groqResetHeaderFromError(error, 'tokens'));
        if (GROQ_AUTO_MODEL_FALLBACK && shouldFallbackGroqModel(error) && modelIndex < fallbackModels.length - 1) continue;
        console.warn('chat empty retry failed:', error.message || error);
        break;
      }
    }
  }
  if (!replyText) {
    replyText = 'Модель вернула пустой ответ. Запрос не выполнен, попробуй повторить короче.';
  }

  session.history.push({ role: 'user', content: `${effectiveUserName}: ${prompt}` });
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
      const result = await createGroqChatCompletion(request, {
        queue: isWebMode ? 'webSearch' : 'ai',
        label: `idle-chatter-${mode}`,
        session,
        model,
      });
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
    if (isMusicLoaded(session)) continue;
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
    if (isMusicLoaded(session)) continue;
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
        stopMusic(session, { clearQueue: true, reason: 'idle_leave' });
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

let backupInProgress = false;

function isSessionIdleForBackup(session) {
  if (!session) return true;
  cleanupStaleActiveCaptures(session);
  if (session.busy || session.interruptBusy || session.wakeAckInProgress) return false;
  if (session.activeUsers?.size) return false;
  if (isMusicLoaded(session)) return false;
  if (session.player?.state?.status === AudioPlayerStatus.Playing) return false;
  return true;
}

function isBackupIdleNow() {
  return [...sessions.values()].every(isSessionIdleForBackup);
}

async function maybeRunScheduledBackup() {
  if (backupInProgress || !isBackupEnabled()) return;
  const targetPath = getBackupTargetPath();
  if (!targetPath) return;

  const now = Date.now();
  const dueAt = backupNextRunAt();
  if (dueAt && now < dueAt) return;

  if (isBackupIdleOnly() && !isBackupIdleNow()) {
    runtimeConfig.backupNextRunAt = dueAt || now + 15 * 60_000;
    return;
  }

  backupInProgress = true;
  try {
    await runQueuedTask('backup', 'scheduled-backup', async () => {
      await saveStoreQueue.catch(() => {});
      await saveRuntimeConfigQueue.catch(() => {});
      const backup = await storage.createBackup();
      const localPath = storage.backupPath(backup.file);
      const target = await syncBackupToTarget({
        localPath,
        targetPath,
        username: getBackupTargetUsername(),
        password: getBackupTargetPassword(),
        retention: getBackupRetention(),
        logger: console,
      });
      await syncBackupToTarget({
        localPath,
        targetPath: path.join(dataDir, 'backups'),
        retention: getBackupRetention(),
        logger: console,
      }).catch((error) => console.warn(`local backup prune skipped: ${error.message || error}`));
      const finishedAt = Date.now();
      updateRuntimeConfig({
        backupLastRunAt: finishedAt,
        backupNextRunAt: finishedAt + getBackupIntervalHours() * 60 * 60_000,
        backupLastFile: backup.file,
        backupLastTarget: target?.target || localPath,
        backupLastError: '',
        backupLastErrorAt: 0,
      });
      appendEvent('backup_created', {
        file: backup.file,
        size: backup.size,
        target: maskBackupTarget(target?.target || localPath),
        retention: getBackupRetention(),
        pruned: target?.pruned?.length || 0,
        scheduled: true,
      });
      console.log(`scheduled backup created file=${backup.file} target=${maskBackupTarget(target?.target || localPath)}`);
    }, {
      target: maskBackupTarget(targetPath),
      retention: getBackupRetention(),
      scheduled: true,
    });
  } catch (error) {
    const message = error.message || String(error);
    updateRuntimeConfig({
      backupLastError: message,
      backupLastErrorAt: Date.now(),
      backupNextRunAt: Date.now() + 30 * 60_000,
    });
    appendEvent('backup_failed', {
      error: message,
      target: maskBackupTarget(targetPath),
      scheduled: true,
    });
    console.error('scheduled backup failed:', error);
  } finally {
    backupInProgress = false;
    await writeStatusSnapshot();
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

async function runCommandCapture(command, args, label, { timeoutMs = 30_000 } = {}) {
  return await new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    let settled = false;
    const timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.kill('SIGKILL');
      reject(new Error(`${label || command} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    const finish = (fn, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      fn(value);
    };
    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });
    child.on('error', (error) => {
      if (error.code === 'ENOENT') {
        finish(reject, new Error(`${label || command} is not installed or not in PATH`));
      } else {
        finish(reject, error);
      }
    });
    child.on('close', (code) => {
      if (code === 0) finish(resolve, { stdout, stderr });
      else finish(reject, new Error(stderr || `${label || command} exited with code ${code}`));
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

async function runFirstAvailableCommandCapture(commands, args, label, options = {}) {
  let lastError = null;
  for (const command of commands) {
    try {
      return await runCommandCapture(command, args, label, options);
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
  if (isMusicLoaded(session)) {
    appendEvent('speech_skipped_music_active', {
      guildId: session.guild?.id,
      voiceChannelId: session.voiceChannel?.id,
      music: session.music?.current?.title || '',
      text: String(text || '').slice(0, 180),
    });
    return;
  }

  const spokenText = sanitizeVoiceOutputText(stripMarkdownFormatting(text));
  if (!spokenText) return;

  const speechVersion = beginSpeech(session);
  const wavPath = await runQueuedTask(
    'tts',
    `synthesize:${getTtsProvider()}:${spokenText.length} chars`,
    () => synthesizeSpeech(spokenText),
    queueMetaForSession(session, { provider: getTtsProvider(), chars: spokenText.length }),
  );
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
  const now = Date.now();
  if (session.wakeAckInProgress) {
    if (String(session.wakeAckUserId || '') !== String(userId)) {
      markIgnored(session, 'wake_ack_other_user');
      return;
    }
    session.wakeAckInProgress = false;
    session.wakeAckUserId = null;
    stopPlayback(session);
    appendEvent('wake_ack_barge_in', {
      guildId: session.guild?.id,
      voiceChannelId: session.voiceChannel?.id,
      userId,
    });
  }
  if (isWakeListenForOtherUser(session, userId, now)) {
    markIgnored(session, 'wake_listen_other_user');
    return;
  }
  const busyAtStart = session.busy;
  if (busyAtStart && session.interruptBusy) {
    markIgnored(session, 'busy_interrupt_in_progress');
    return;
  }
  const captureStartedAt = Date.now();
  const isPostWakeCapture = isWakeListenWindow(session, captureStartedAt, userId);
  const silenceMs = isPostWakeCapture ? POST_WAKE_SILENCE_MS : SILENCE_MS;
  const maxUtteranceMs = isPostWakeCapture ? POST_WAKE_MAX_UTTERANCE_MS : MAX_UTTERANCE_MS;
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
    end: { behavior: EndBehaviorType.AfterSilence, duration: silenceMs },
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
  }, maxUtteranceMs);

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
        markIgnored(session, 'wake_listening_interrupt', { lastTranscript: transcript });
        await openWakeListening(session, userId, member, transcript, 'voice_interrupt');
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
        markIgnored(session, 'wake_listening', { lastTranscript: transcript });
        await openWakeListening(session, userId, member, transcript, 'voice');
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
        const actionSpeechText = typeof actionResult === 'string'
          ? actionText
          : (actionResult.speechText || actionResult.speakText || actionText);
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
        if (shouldSpeak && actionSpeechText && !isTurnCancelled(session, turnId)) {
          const ttsStartedAt = Date.now();
          await speak(session, actionSpeechText);
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
      if (!answer) {
        const fallbackText = 'ИИ не вернул текст. Повтори запрос чуть короче.';
        console.warn(`empty assistant answer user=${userId} prompt="${prompt}"`);
        appendEvent('assistant_empty_answer', {
          guildId: session.guild?.id,
          voiceChannelId: session.voiceChannel?.id,
          userId,
          prompt,
        });
        await sendText(session.textChannel, `🤖 ${fallbackText}`);
        session.lastReplyAt = Date.now();
        if (session.diagnostics) {
          session.diagnostics.lastAnswerAt = session.lastReplyAt;
          session.diagnostics.lastError = 'empty_assistant_answer';
          session.diagnostics.lastTimingsMs = { ...timings, total: Date.now() - turnStartedAt };
        }
        return;
      }
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
  if (old?.connection && old.connection.state.status !== VoiceConnectionStatus.Destroyed) {
    stopMusic(old, { clearQueue: true, reason: 'reconnect' });
    old.connection.destroy();
  }

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
    presencePhraseHistory: new Map(),
    busy: false,
    interruptBusy: false,
    paused: false,
    pendingAction: null,
    lastReplyAt: 0,
    activeDialogueUntil: 0,
    wakeAckInProgress: false,
    wakeAckUserId: null,
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
    music: createMusicState(),
    diagnostics: createVoiceDiagnostics(),
  };
  attachMusicPlayerHandlers(session);
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
  enqueuePresenceAnnouncement(session, () => buildBotJoinAnnouncement(session), `bot_join:${voiceChannel.id}:${session.joinedAt}`);
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
    new SlashCommandBuilder().setName('profile').setDescription('Показать твой профиль ассистента'),
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
  if (!userId) return;
  if (userId === client.user.id) {
    if (newState.channel && newChannelId !== watchedChannelId) {
      refreshSessionVoiceChannel(session, newState.channel);
      await writeStatusSnapshot();
    }
    return;
  }

  let member = newState.member || oldState.member || session.guild.members.cache.get(userId);
  if (!member) member = await session.guild.members.fetch(userId).catch(() => null);
  if (!member || member.user?.bot) return;

  const joinedWatchedChannel = newChannelId === watchedChannelId && oldChannelId !== watchedChannelId;
  const leftWatchedChannel = oldChannelId === watchedChannelId && newChannelId !== watchedChannelId;
  if (!joinedWatchedChannel && !leftWatchedChannel) return;

  session.lastHumanActivityAt = Date.now();
  if (joinedWatchedChannel) {
    session.knownVoiceMemberIds?.add(member.id);
    const humanCount = getHumanVoiceMembers(session).length;
    if (shouldGreetPresenceMember(session, member)) {
      rememberPresenceMemberGreeting(session, member);
      enqueuePresenceAnnouncement(
        session,
        () => buildMemberJoinAnnouncement(session, member, humanCount),
        `member_join:${watchedChannelId}:${member.id}`,
      );
    }
  } else {
    session.knownVoiceMemberIds?.delete(member.id);
    const humanCountAfterLeave = getHumanVoiceMembers(session).length;
    if (humanCountAfterLeave) {
      enqueuePresenceAnnouncement(
        session,
        buildMemberLeaveAnnouncement(session, member, humanCountAfterLeave + 1),
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

      const bot = await validateTelegramBotToken(token);
      let chat = null;
      let resolvedChatId = '';
      let chatWarning = '';
      if (chatId) {
        try {
          const resolved = await resolveTelegramChatReference(chatId, { token });
          chat = resolved.chat;
          resolvedChatId = resolved.chatId;
        } catch (error) {
          chatWarning = error.message || String(error);
        }
      }
      updateRuntimeConfig({
        telegramBotToken: token,
        telegramDefaultChatId: resolvedChatId || getTelegramDefaultChatId(),
      });
      appendEvent('telegram_configured', {
        guildId: interaction.guildId,
        actorId: interaction.user?.id,
        botUsername: bot?.username || null,
        chatId: resolvedChatId || null,
        chatWarning: chatWarning || null,
      });
      await reply(
        interaction,
        [
          `Telegram подключен: @${bot?.username || bot?.first_name || 'bot'}.`,
          chat
            ? `Default chat: ${formatTelegramChat(chat)}.`
            : (chatWarning || 'Default chat_id пока не задан. Используй /telegram_chat или /telegram_chats.'),
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
      if (old?.connection) {
        stopMusic(old, { clearQueue: true, reason: 'slash_join' });
        old.connection.destroy();
      }

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
      if (session?.connection) {
        stopMusic(session, { clearQueue: true, reason: 'slash_leave' });
        session.connection.destroy();
      }
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

    if (interaction.commandName === 'profile') {
      await interaction.deferReply({ flags: MessageFlags.SuppressNotifications });
      const profile = getUserProfile(interaction.guildId, interaction.member?.id, interaction.member, { create: true });
      await reply(interaction, `Профиль ${profile.preferredName || profile.userName || interaction.user.username}:\n${formatUserProfile(profile)}`);
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
      await reply(
        interaction,
        verifyReminderStored(reminder) && verifyReminderTimer(reminder)
          ? `Проверил: напоминание сохранено на ${formatDueTime(reminder.dueAt)}.`
          : 'Напоминание записано, но локальная проверка таймера не подтвердилась.',
      );
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
      const chatRef = normalizeTelegramChatId(interaction.options.getString('chat_id', true));
      if (!getTelegramBotToken()) {
        await reply(interaction, 'Telegram token не задан. Сначала используй /telegram_setup.', { flags: MessageFlags.Ephemeral });
        return;
      }
      const resolved = await resolveTelegramChatReference(chatRef).catch((error) => ({ error: error.message || String(error) }));
      if (resolved.error) {
        await reply(interaction, resolved.error, { flags: MessageFlags.Ephemeral });
        return;
      }
      const { chat, chatId } = resolved;
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
      const sent = await sendTelegramMessage(text, { chatId });
      await reply(interaction, telegramDeliveryText(sent, 'сообщение'));
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

await initPanelCommandOffset().catch((error) => console.error('panel command offset init failed:', error));

setInterval(() => {
  void pollPanelCommands().catch((error) => console.error('panel command poll failed:', error));
}, 1_000).unref();

setInterval(() => {
  void pollTelegramInbound().catch((error) => console.error('telegram inbound tick failed:', error));
}, TELEGRAM_INBOUND_POLL_MS).unref();

setInterval(() => {
  void maybeRunIdleChatter().catch((error) => console.error('idle chatter tick failed:', error));
}, IDLE_CHATTER_CHECK_MS).unref();

setInterval(() => {
  void maybeRunIdleLeave().catch((error) => console.error('idle leave tick failed:', error));
}, IDLE_LEAVE_CHECK_MS).unref();

setTimeout(() => {
  void maybeRunScheduledBackup().catch((error) => console.error('backup startup tick failed:', error));
}, 60_000).unref();

setInterval(() => {
  void maybeRunScheduledBackup().catch((error) => console.error('backup tick failed:', error));
}, BACKUP_CHECK_MS).unref();

setInterval(() => {
  void runHealthCheck().catch((error) => console.error('healthcheck tick failed:', error));
}, HEALTHCHECK_INTERVAL_MS).unref();

setTimeout(() => {
  void refreshGroqModelDiscovery({ force: true, reason: 'startup' }).catch((error) => console.error('model discovery startup failed:', error));
}, GROQ_MODEL_DISCOVERY_INITIAL_DELAY_MS).unref();

setInterval(() => {
  void refreshGroqModelDiscovery({ reason: 'timer' }).catch((error) => console.error('model discovery tick failed:', error));
}, Math.min(GROQ_MODEL_DISCOVERY_INTERVAL_MS, 6 * 60 * 60_000)).unref();

await client.login(DISCORD_TOKEN);
