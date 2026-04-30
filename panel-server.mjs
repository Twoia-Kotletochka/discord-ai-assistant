import 'dotenv/config';

import { createReadStream, promises as fs } from 'node:fs';
import { execFile } from 'node:child_process';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import crypto from 'node:crypto';
import { promisify } from 'node:util';
import Groq from 'groq-sdk';

import { createStorage } from './storage.mjs';
import { maskBackupTarget, normalizeBackupTargetPath, splitBackupTargetCredentials, syncBackupToTarget } from './backup-targets.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const execFileAsync = promisify(execFile);
const publicDir = path.join(__dirname, 'panel');
const envPath = path.resolve(process.env.ENV_PATH || path.join(__dirname, '.env'));
const dataDir = path.resolve(process.env.DATA_DIR || path.join(__dirname, 'data'));
const runtimeConfigPath = path.join(dataDir, 'runtime-config.json');
const statusPath = path.join(dataDir, 'status.json');
const statePath = path.join(dataDir, 'state.json');
const eventLogPath = path.join(dataDir, 'events.jsonl');
const backupsDir = path.join(dataDir, 'backups');
const dockerSocketPath = process.env.DOCKER_SOCKET || '/var/run/docker.sock';
const dockerContainers = {
  bot: process.env.BOT_CONTAINER_NAME || 'discord-ai-assistant-bot',
  panel: process.env.PANEL_CONTAINER_NAME || 'discord-ai-assistant-panel',
};

await fs.mkdir(dataDir, { recursive: true });
await fs.mkdir(backupsDir, { recursive: true });

const storage = await createStorage({ dataDir, logger: console });
const envFile = await readEnvFile();
const panelPassword = process.env.PANEL_PASSWORD || envFile.PANEL_PASSWORD || '';
const panelHost = process.env.PANEL_HOST || '127.0.0.1';
const panelPort = Number(process.env.PANEL_PORT || 8787);
const panelSessionMaxAgeSec = Math.max(300, Number(process.env.PANEL_SESSION_MAX_AGE_SECONDS || envFile.PANEL_SESSION_MAX_AGE_SECONDS || 43_200));
const panelLoginMaxAttempts = Math.max(1, Number(process.env.PANEL_LOGIN_MAX_ATTEMPTS || envFile.PANEL_LOGIN_MAX_ATTEMPTS || 6));
const panelLoginWindowMs = Math.max(60_000, Number(process.env.PANEL_LOGIN_WINDOW_MS || envFile.PANEL_LOGIN_WINDOW_MS || 600_000));
const panelLoginLockMs = Math.max(60_000, Number(process.env.PANEL_LOGIN_LOCK_MS || envFile.PANEL_LOGIN_LOCK_MS || 300_000));
const sessionSecret = crypto.randomBytes(32).toString('hex');
const sessions = new Map();
const loginAttempts = new Map();
const panelGroqLimits = new Map();
let voicePresetCache = { at: 0, value: null };
let groqModelPresetCache = { at: 0, value: null };
let stateMutationQueue = Promise.resolve();

const modelPresets = {
  chat: [
    'llama-3.3-70b-versatile',
    'openai/gpt-oss-120b',
    'meta-llama/llama-4-scout-17b-16e-instruct',
    'qwen/qwen3-32b',
    'openai/gpt-oss-20b',
    'llama-3.1-8b-instant',
  ],
  stt: ['whisper-large-v3-turbo', 'whisper-large-v3'],
  web: ['groq/compound', 'groq/compound-mini'],
  macosVoices: ['Milena', 'Yuri', 'Alena', 'Katya', 'Daniel', 'Samantha'],
  espeakVoices: ['ru', 'ru+f3', 'ru+m3', 'en-us', 'en-gb'],
  edgeVoices: [
    'ru-RU-SvetlanaNeural',
    'ru-RU-DmitryNeural',
    'en-US-AvaMultilingualNeural',
    'en-US-EmmaMultilingualNeural',
    'en-US-AndrewMultilingualNeural',
    'en-US-BrianMultilingualNeural',
  ],
};

function defaultWakeAliasesFor(wakeWord) {
  const normalizedWake = String(wakeWord || '').toLowerCase().replaceAll('ё', 'е').trim();
  if (normalizedWake === 'бот') {
    return 'вот,от,робот,роботик,ботик,бота,боту,боте,боты,ботом,бод,бат,борт,вод,бо,ботт';
  }
  if (normalizedWake === 'железяка') {
    return 'железка,железяко,железяку,железяке,железякой,железяки,железякин';
  }
  if (normalizedWake === 'зеро' || normalizedWake === 'zero') {
    return 'zero,зеро,зэро,зиро,зера,зеру,зэру,зерро,зэрро,зер,зироу,зара,заро,зоро,зерно,зено,зена,зина,зэра,зэна,серо,сиро,сера,сэро,сено,церо,цено,геро,жеро,ксеро,zerro,zeroo,zeero,ziro,zera,zaro,zoro,zeno,zenu,zena,zina,zere,zerre,sero,seno,cero,ceno,xero,xeno,hero';
  }
  return '';
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

function trackGroqRateLimits(label, response, model = 'unknown') {
  const headers = response?.headers || response;
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
    panelGroqLimits.set(`${model}:${metric.name}`, { ...metric, label, model, checkedAt: Date.now() });
  }
}

function groqLimitsObject() {
  return Object.fromEntries(panelGroqLimits.entries());
}

async function probeGroqLimits() {
  const runtime = await readRuntimeConfig();
  const envValues = await readEnvFile();
  const apiKey = runtime.groqApiKey || envValues.GROQ_API_KEY || process.env.GROQ_API_KEY;
  if (!apiKey) throw new Error('Groq API key is not set');
  const client = new Groq({ apiKey });
  const model = runtime.groqChatModel || envValues.GROQ_CHAT_MODEL || 'llama-3.3-70b-versatile';
  const result = await client.chat.completions.create({
    model,
    messages: [{ role: 'user', content: 'ok' }],
    temperature: 0,
    max_completion_tokens: 1,
  }).withResponse();
  trackGroqRateLimits('panel-probe', result.response, model);
  return groqLimitsObject();
}

function isSttModel(id) {
  return /whisper|transcrib|speech-to-text|asr/u.test(id);
}

function isAssistantChatModel(id) {
  return !/(whisper|transcrib|speech-to-text|asr|orpheus|tts|speech|prompt-guard|safeguard)/u.test(id);
}

function isWebSearchModel(id) {
  return /^groq\/compound/u.test(id);
}

async function getGroqApiKey() {
  const runtime = await readRuntimeConfig();
  const envValues = await readEnvFile();
  return runtime.groqApiKey || envValues.GROQ_API_KEY || process.env.GROQ_API_KEY || '';
}

async function getGroqModelPresets(force = false) {
  if (!force && groqModelPresetCache.value && Date.now() - groqModelPresetCache.at < 300_000) {
    return groqModelPresetCache.value;
  }

  let value = {
    chat: modelPresets.chat,
    stt: modelPresets.stt,
    web: modelPresets.web,
    modelInfo: [],
    modelSource: 'fallback',
  };

  const apiKey = await getGroqApiKey();
  if (apiKey) {
    try {
      const client = new Groq({ apiKey });
      const models = await client.models.list();
      const active = (models.data || [])
        .filter((model) => model.active !== false && model.id)
        .sort((a, b) => a.id.localeCompare(b.id));
      const ids = active.map((model) => model.id);
      value = {
        chat: [...new Set([...modelPresets.chat, ...ids.filter(isAssistantChatModel)])],
        stt: [...new Set([...modelPresets.stt, ...ids.filter(isSttModel)])],
        web: [...new Set([...modelPresets.web, ...ids.filter(isWebSearchModel)])],
        modelInfo: active.map((model) => ({
          id: model.id,
          ownedBy: model.owned_by || '',
          contextWindow: model.context_window || null,
          maxCompletionTokens: model.max_completion_tokens || null,
        })),
        modelSource: 'groq',
        modelUpdatedAt: Date.now(),
      };
    } catch (error) {
      console.error('Groq models list failed:', error.message || error);
    }
  }

  groqModelPresetCache = { at: Date.now(), value };
  return value;
}

function parseSayVoices(output) {
  return output
    .split(/\r?\n/u)
    .map((line) => line.match(/^(.+?)\s{2,}[a-z]{2}_[A-Z]{2}\s+#/u)?.[1]?.trim())
    .filter(Boolean);
}

function parseEspeakVoices(output) {
  return output
    .split(/\r?\n/u)
    .slice(1)
    .map((line) => line.trim().split(/\s+/u)[3])
    .filter(Boolean);
}

function parseEdgeVoices(output) {
  return output
    .split(/\r?\n/u)
    .map((line) => line.trim().split(/\s+/u)[0])
    .filter((voice) => /^[a-z]{2}-[A-Z]{2}-.+Neural$/u.test(voice));
}

function edgeTtsCommandCandidates() {
  return [
    process.env.EDGE_TTS_COMMAND,
    path.join(__dirname, '.venv', 'bin', 'edge-tts'),
    '/opt/edge-tts/bin/edge-tts',
    'edge-tts',
  ].filter(Boolean);
}

async function execFirstAvailable(commands, args) {
  let lastError = null;
  for (const command of commands) {
    try {
      return await execFileAsync(command, args);
    } catch (error) {
      lastError = error;
      if (error.code !== 'ENOENT') throw error;
    }
  }
  throw lastError || new Error('No command candidates');
}

async function getVoicePresets() {
  if (voicePresetCache.value && Date.now() - voicePresetCache.at < 60_000) return voicePresetCache.value;

  const [sayResult, espeakResult, edgeResult] = await Promise.all([
    execFileAsync('say', ['-v', '?']).catch(() => null),
    execFileAsync('espeak-ng', ['--voices']).catch(() => null),
    execFirstAvailable(edgeTtsCommandCandidates(), ['--list-voices']).catch(() => null),
  ]);

  const macosVoices = sayResult?.stdout ? parseSayVoices(sayResult.stdout) : [];
  const espeakVoices = espeakResult?.stdout ? parseEspeakVoices(espeakResult.stdout) : [];
  const edgeVoices = edgeResult?.stdout ? parseEdgeVoices(edgeResult.stdout) : [];
  const value = {
    ...modelPresets,
    macosVoices: [...new Set([...modelPresets.macosVoices, ...macosVoices])],
    espeakVoices: [...new Set([...modelPresets.espeakVoices, ...espeakVoices])],
    edgeVoices: [...new Set([...modelPresets.edgeVoices, ...edgeVoices])],
  };
  voicePresetCache = { at: Date.now(), value };
  return value;
}

function defaultRuntimeConfig() {
  const assistantName = envFile.ASSISTANT_NAME || 'Бот';
  const wakeWord = envFile.BOT_WAKE_WORD || assistantName.toLowerCase();
  const backupTarget = splitBackupTargetCredentials(envFile.BACKUP_TARGET_PATH || path.join(dataDir, 'backups'));
  return {
    botEnabled: true,
    listeningPaused: false,
    assistantName,
    wakeWord,
    wakeAliases: envFile.BOT_WAKE_ALIASES || defaultWakeAliasesFor(wakeWord),
    wakeFuzzy: (envFile.BOT_WAKE_FUZZY || 'true') === 'true',
    groqApiKey: '',
    groqChatModel: envFile.GROQ_CHAT_MODEL || 'llama-3.3-70b-versatile',
    groqSttModel: envFile.GROQ_STT_MODEL || 'whisper-large-v3-turbo',
    actionParserModel: envFile.ACTION_PARSER_MODEL || 'llama-3.1-8b-instant',
    webSearchEnabled: (envFile.WEB_SEARCH_ENABLED || 'true') === 'true',
    webSearchModel: envFile.WEB_SEARCH_MODEL || 'groq/compound',
    idleChatterEnabled: (envFile.IDLE_CHATTER_ENABLED || 'false') === 'true',
    idleChatterMinutes: Math.max(1, Math.min(180, Number(envFile.IDLE_CHATTER_MINUTES || 5))),
    idleChatterUseWeb: (envFile.IDLE_CHATTER_USE_WEB || 'true') === 'true',
    idleChatterStyle: envFile.IDLE_CHATTER_STYLE || 'mixed',
    idleLeaveEnabled: (envFile.IDLE_LEAVE_ENABLED || 'true') === 'true',
    idleLeaveMinutes: Math.max(1, Math.min(1440, Number(envFile.IDLE_LEAVE_MINUTES || 60))),
    idleLeavePhrase: envFile.IDLE_LEAVE_PHRASE || '',
    presenceAnnouncementsEnabled: (envFile.PRESENCE_ANNOUNCEMENTS_ENABLED || 'true') === 'true',
    activeDialogueEnabled: (envFile.ACTIVE_DIALOGUE_ENABLED || 'false') === 'true',
    activeDialogueSeconds: Math.max(10, Math.min(300, Number(envFile.ACTIVE_DIALOGUE_SECONDS || 45))),
    confirmDangerousActions: false,
    assistantPersona: envFile.ASSISTANT_PERSONA || 'default',
    healthcheckEnabled: (envFile.HEALTHCHECK_ENABLED || 'true') === 'true',
    sttLanguage: envFile.STT_LANGUAGE || 'ru',
    ttsProvider: envFile.TTS_PROVIDER || (process.platform === 'darwin' ? 'macos' : 'espeak'),
    macosVoice: envFile.MACOS_TTS_VOICE || 'Milena',
    espeakVoice: envFile.ESPEAK_TTS_VOICE || 'ru',
    espeakSpeed: Number(envFile.ESPEAK_TTS_SPEED || 165),
    edgeVoice: envFile.EDGE_TTS_VOICE || 'ru-RU-SvetlanaNeural',
    edgeEnglishVoice: envFile.EDGE_TTS_ENGLISH_VOICE || 'en-US-AvaMultilingualNeural',
    edgeRate: envFile.EDGE_TTS_RATE || '+0%',
    edgePitch: envFile.EDGE_TTS_PITCH || '+0Hz',
    backupEnabled: (envFile.BACKUP_ENABLED || 'false') === 'true',
    backupTargetPath: backupTarget.targetPath,
    backupTargetUsername: envFile.BACKUP_TARGET_USERNAME || backupTarget.username || '',
    backupTargetPassword: envFile.BACKUP_TARGET_PASSWORD || backupTarget.password || '',
    backupIntervalHours: Math.max(1, Math.min(720, Number(envFile.BACKUP_INTERVAL_HOURS || 24))),
    backupRetention: Math.max(1, Math.min(20, Number(envFile.BACKUP_RETENTION || 2))),
    backupIdleOnly: (envFile.BACKUP_IDLE_ONLY || 'true') !== 'false',
    backupLastRunAt: 0,
    backupNextRunAt: 0,
    backupLastFile: '',
    backupLastTarget: '',
    backupLastError: '',
    backupLastErrorAt: 0,
    updatedAt: Date.now(),
  };
}

async function readJson(filePath, fallback = null) {
  const raw = await fs.readFile(filePath, 'utf8').catch(() => null);
  if (!raw) return fallback;
  try {
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

async function writeJson(filePath, value) {
  const tmpPath = `${filePath}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify(value, null, 2));
  await fs.rename(tmpPath, filePath);
}

async function readRuntimeConfig() {
  return { ...defaultRuntimeConfig(), ...(await storage.loadRuntimeConfig({})) };
}

async function writeRuntimeConfig(patch) {
  const current = await readRuntimeConfig();
  const targetInput = splitBackupTargetCredentials(patch.backupTargetPath ?? current.backupTargetPath ?? path.join(dataDir, 'backups'));
  const patchPassword = typeof patch.backupTargetPassword === 'string' ? patch.backupTargetPassword : null;
  const usernameSource = targetInput.username || (patch.backupTargetUsername !== undefined
    ? patch.backupTargetUsername
    : (current.backupTargetUsername || ''));
  const backupTargetUsername = patch.backupClearCredentials
    ? ''
    : String(usernameSource).trim().slice(0, 120);
  const backupTargetPassword = patch.backupClearCredentials
    ? ''
    : (patchPassword && patchPassword.trim()
      ? patchPassword.trim().slice(0, 240)
      : (targetInput.password || current.backupTargetPassword || ''));
  const next = {
    ...current,
    ...patch,
    botEnabled: patch.botEnabled === undefined ? current.botEnabled !== false : patch.botEnabled !== false,
    listeningPaused: patch.listeningPaused === undefined ? current.listeningPaused === true : patch.listeningPaused === true,
    assistantName: String(patch.assistantName ?? current.assistantName ?? 'Бот').replace(/\s+/g, ' ').trim().slice(0, 40) || 'Бот',
    wakeWord: String(patch.wakeWord ?? current.wakeWord ?? 'бот').replace(/\s+/g, ' ').trim().toLowerCase().slice(0, 40) || 'бот',
    wakeAliases: String(patch.wakeAliases ?? current.wakeAliases ?? ''),
    wakeFuzzy: patch.wakeFuzzy === undefined ? current.wakeFuzzy !== false : patch.wakeFuzzy !== false,
    groqChatModel: String(patch.groqChatModel ?? current.groqChatModel ?? 'llama-3.3-70b-versatile'),
    groqSttModel: String(patch.groqSttModel ?? current.groqSttModel ?? 'whisper-large-v3-turbo'),
    actionParserModel: String(patch.actionParserModel ?? current.actionParserModel ?? 'llama-3.1-8b-instant'),
    webSearchEnabled: patch.webSearchEnabled === undefined ? current.webSearchEnabled !== false : patch.webSearchEnabled !== false,
    webSearchModel: String(patch.webSearchModel ?? current.webSearchModel ?? 'groq/compound'),
    idleChatterEnabled: patch.idleChatterEnabled === undefined ? current.idleChatterEnabled === true : patch.idleChatterEnabled === true,
    idleChatterMinutes: Math.max(1, Math.min(180, Number(patch.idleChatterMinutes ?? current.idleChatterMinutes ?? 5))),
    idleChatterUseWeb: patch.idleChatterUseWeb === undefined ? current.idleChatterUseWeb !== false : patch.idleChatterUseWeb !== false,
    idleChatterStyle: String(patch.idleChatterStyle ?? current.idleChatterStyle ?? 'mixed'),
    idleLeaveEnabled: patch.idleLeaveEnabled === undefined ? current.idleLeaveEnabled === true : patch.idleLeaveEnabled === true,
    idleLeaveMinutes: Math.max(1, Math.min(1440, Number(patch.idleLeaveMinutes ?? current.idleLeaveMinutes ?? 60))),
    idleLeavePhrase: String(patch.idleLeavePhrase ?? current.idleLeavePhrase ?? '').replace(/\s+/g, ' ').trim().slice(0, 240),
    presenceAnnouncementsEnabled: patch.presenceAnnouncementsEnabled === undefined ? current.presenceAnnouncementsEnabled !== false : patch.presenceAnnouncementsEnabled === true,
    activeDialogueEnabled: patch.activeDialogueEnabled === undefined ? current.activeDialogueEnabled === true : patch.activeDialogueEnabled === true,
    activeDialogueSeconds: Math.max(10, Math.min(300, Number(patch.activeDialogueSeconds ?? current.activeDialogueSeconds ?? 45))),
    confirmDangerousActions: false,
    assistantPersona: String(patch.assistantPersona ?? current.assistantPersona ?? 'default'),
    healthcheckEnabled: patch.healthcheckEnabled === undefined ? current.healthcheckEnabled !== false : patch.healthcheckEnabled !== false,
    espeakSpeed: Math.max(80, Math.min(260, Number(patch.espeakSpeed ?? current.espeakSpeed ?? 165))),
    edgeVoice: String(patch.edgeVoice ?? current.edgeVoice ?? 'ru-RU-SvetlanaNeural'),
    edgeEnglishVoice: String(patch.edgeEnglishVoice ?? current.edgeEnglishVoice ?? 'en-US-AvaMultilingualNeural'),
    edgeRate: String(patch.edgeRate ?? current.edgeRate ?? '+0%'),
    edgePitch: String(patch.edgePitch ?? current.edgePitch ?? '+0Hz'),
    backupEnabled: patch.backupEnabled === undefined ? current.backupEnabled === true : patch.backupEnabled === true,
    backupTargetPath: normalizeBackupTargetPath(targetInput.targetPath).slice(0, 500),
    backupTargetUsername,
    backupTargetPassword,
    backupIntervalHours: Math.max(1, Math.min(720, Number(patch.backupIntervalHours ?? current.backupIntervalHours ?? 24))),
    backupRetention: Math.max(1, Math.min(20, Number(patch.backupRetention ?? current.backupRetention ?? 2))),
    backupIdleOnly: patch.backupIdleOnly === undefined ? current.backupIdleOnly !== false : patch.backupIdleOnly !== false,
    backupLastRunAt: Number(patch.backupLastRunAt ?? current.backupLastRunAt ?? 0),
    backupNextRunAt: Number(patch.backupNextRunAt ?? current.backupNextRunAt ?? 0),
    backupLastFile: String(patch.backupLastFile ?? current.backupLastFile ?? '').slice(0, 255),
    backupLastTarget: String(patch.backupLastTarget ?? current.backupLastTarget ?? '').slice(0, 500),
    backupLastError: String(patch.backupLastError ?? current.backupLastError ?? '').slice(0, 500),
    backupLastErrorAt: Number(patch.backupLastErrorAt ?? current.backupLastErrorAt ?? 0),
    updatedAt: Date.now(),
  };
  delete next.backupClearCredentials;
  await storage.saveRuntimeConfig(next);
  return next;
}

async function readEnvFile() {
  const raw = await fs.readFile(envPath, 'utf8').catch(() => '');
  const values = {};
  for (const line of raw.split(/\r?\n/)) {
    const match = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/);
    if (!match) continue;
    values[match[1]] = match[2].replace(/^["']|["']$/g, '');
  }
  return values;
}

async function updateEnvFile(patch) {
  const raw = await fs.readFile(envPath, 'utf8').catch(() => '');
  const lines = raw.split(/\r?\n/);
  const pending = new Map(Object.entries(patch).filter(([, value]) => value !== undefined));
  const next = lines.map((line) => {
    const match = line.match(/^(\s*)([A-Za-z_][A-Za-z0-9_]*)(\s*=\s*)(.*)$/);
    if (!match || !pending.has(match[2])) return line;
    const value = String(pending.get(match[2]) ?? '');
    pending.delete(match[2]);
    return `${match[1]}${match[2]}${match[3]}${value}`;
  });
  for (const [key, value] of pending) next.push(`${key}=${String(value ?? '')}`);
  await fs.writeFile(envPath, `${next.join('\n').replace(/\n+$/u, '')}\n`);
}

function mask(value) {
  if (!value) return '';
  if (value.length <= 10) return '••••';
  return `${value.slice(0, 4)}••••${value.slice(-4)}`;
}

function publicEnv(values) {
  return {
    discordTokenSet: Boolean(values.DISCORD_TOKEN),
    discordTokenMasked: mask(values.DISCORD_TOKEN),
    groqApiKeySet: Boolean(values.GROQ_API_KEY),
    groqApiKeyMasked: mask(values.GROQ_API_KEY),
    discordGuildId: values.DISCORD_GUILD_ID || '',
    panelPasswordSet: Boolean(panelPassword),
  };
}

function cookieValue(req, name) {
  const cookie = req.headers.cookie || '';
  for (const part of cookie.split(';')) {
    const [key, value] = part.trim().split('=');
    if (key === name) return decodeURIComponent(value || '');
  }
  return '';
}

function cleanupAuthState() {
  const now = Date.now();
  for (const [token, session] of sessions.entries()) {
    if (!session?.expiresAt || session.expiresAt <= now) sessions.delete(token);
  }
  for (const [key, attempt] of loginAttempts.entries()) {
    if (now - attempt.firstAt > panelLoginWindowMs && now > attempt.lockedUntil) {
      loginAttempts.delete(key);
    }
  }
}

function loginAttemptKey(req) {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return forwarded || req.socket.remoteAddress || 'unknown';
}

function loginAttempt(req) {
  const key = loginAttemptKey(req);
  const now = Date.now();
  let attempt = loginAttempts.get(key);
  if (!attempt || (now - attempt.firstAt > panelLoginWindowMs && now > attempt.lockedUntil)) {
    attempt = { count: 0, firstAt: now, lockedUntil: 0 };
    loginAttempts.set(key, attempt);
  }
  return { key, attempt };
}

function currentLoginBlock(req) {
  const { attempt } = loginAttempt(req);
  const now = Date.now();
  if (attempt.lockedUntil > now) {
    return { blocked: true, retryAfterSec: Math.ceil((attempt.lockedUntil - now) / 1000) };
  }
  return { blocked: false, retryAfterSec: 0 };
}

function recordBadLogin(req) {
  const { attempt } = loginAttempt(req);
  attempt.count += 1;
  if (attempt.count >= panelLoginMaxAttempts) {
    attempt.lockedUntil = Date.now() + panelLoginLockMs;
  }
  return Math.max(0, panelLoginMaxAttempts - attempt.count);
}

function clearBadLogins(req) {
  loginAttempts.delete(loginAttemptKey(req));
}

function timingSafePasswordEquals(candidate) {
  const hash = (value) => crypto.createHash('sha256').update(String(value || ''), 'utf8').digest();
  return crypto.timingSafeEqual(hash(candidate), hash(panelPassword));
}

function cookieAttributes(req) {
  const forwardedProto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim().toLowerCase();
  const secure = req.socket.encrypted || forwardedProto === 'https';
  return `HttpOnly; SameSite=Strict; Path=/${secure ? '; Secure' : ''}`;
}

function sessionCookie(token, req) {
  return `panel_session=${encodeURIComponent(token)}; ${cookieAttributes(req)}; Max-Age=${panelSessionMaxAgeSec}`;
}

function clearSessionCookie(req) {
  return `panel_session=; ${cookieAttributes(req)}; Max-Age=0`;
}

function isAuthed(req) {
  if (!panelPassword) return true;
  cleanupAuthState();
  const token = cookieValue(req, 'panel_session');
  const session = token ? sessions.get(token) : null;
  if (!session) return false;
  if (session.expiresAt <= Date.now()) {
    sessions.delete(token);
    return false;
  }
  session.lastSeenAt = Date.now();
  return true;
}

function send(res, status, body, headers = {}) {
  const payload = typeof body === 'string' ? body : JSON.stringify(body);
  res.writeHead(status, {
    'Content-Type': typeof body === 'string' ? 'text/plain; charset=utf-8' : 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    ...headers,
  });
  res.end(payload);
}

async function readBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (!chunks.length) return {};
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

async function listBackups() {
  return await storage.listBackups();
}

async function createBackupAndSync({ manual = true } = {}) {
  const runtime = await readRuntimeConfig();
  const backup = await storage.createBackup();
  const localPath = storage.backupPath(backup.file);
  let target = null;
  if (runtime.backupTargetPath) {
    try {
      target = await syncBackupToTarget({
        localPath,
        targetPath: runtime.backupTargetPath,
        username: runtime.backupTargetUsername,
        password: runtime.backupTargetPassword,
        retention: runtime.backupRetention || 2,
        logger: console,
      });
      await syncBackupToTarget({
        localPath,
        targetPath: backupsDir,
        retention: runtime.backupRetention || 2,
        logger: console,
      }).catch((error) => console.warn(`local backup prune skipped: ${error.message || error}`));
    } catch (error) {
      await writeRuntimeConfig({
        backupLastFile: backup.file,
        backupLastError: error.message || String(error),
        backupLastErrorAt: Date.now(),
      });
      await storage.appendEvent({
        ts: new Date().toISOString(),
        type: 'backup_failed',
        payload: {
          file: backup.file,
          target: maskBackupTarget(runtime.backupTargetPath),
          error: error.message || String(error),
          manual,
        },
      }).catch(() => {});
      throw error;
    }
  }
  const finishedAt = Date.now();
  await writeRuntimeConfig({
    backupLastRunAt: finishedAt,
    backupNextRunAt: finishedAt + Math.max(1, Math.min(720, Number(runtime.backupIntervalHours || 24))) * 60 * 60_000,
    backupLastFile: backup.file,
    backupLastTarget: target?.target || localPath,
    backupLastError: '',
    backupLastErrorAt: 0,
  });
  await storage.appendEvent({
    ts: new Date().toISOString(),
    type: 'backup_created',
    payload: {
      file: backup.file,
      size: backup.size,
      target: maskBackupTarget(target?.target || localPath),
      retention: runtime.backupRetention || 2,
      pruned: target?.pruned?.length || 0,
      manual,
    },
  }).catch(() => {});
  return { ...backup, target };
}

async function readEventLog(limit = 120) {
  return await storage.readEvents(limit);
}

function withStateMutation(mutator) {
  const work = stateMutationQueue
    .catch(() => {})
    .then(async () => {
      const state = await storage.loadState();
      const result = mutator(state);
      if (result) await storage.saveState(state);
      return { state, result };
    });
  stateMutationQueue = work.then(() => {}, () => {});
  return work;
}

function memoryStatsFromState(state) {
  const guilds = Object.entries(state?.guilds || {});
  const userMemories = (guildState) => Object.values(guildState.userMemories || {})
    .reduce((sum, memories) => sum + (Array.isArray(memories) ? memories.length : 0), 0);
  return {
    guilds: guilds.length,
    memories: guilds.reduce((sum, [, guildState]) => sum + (guildState.memories?.length || 0) + userMemories(guildState), 0),
    reminders: guilds.reduce((sum, [, guildState]) => sum + (guildState.reminders?.length || 0), 0),
  };
}

function memoryItemRow(guildId, scope, ownerId, memory, index) {
  return {
    key: `${guildId}:${scope}:${ownerId || ''}:${memory?.id || index}`,
    guildId,
    scope,
    ownerId: ownerId || '',
    id: memory?.id || '',
    index,
    text: String(memory?.text || ''),
    userId: memory?.userId || ownerId || '',
    userName: memory?.userName || '',
    createdAt: Number(memory?.createdAt || 0),
  };
}

function collectMemoryItems(state, limit = 250) {
  const items = [];
  for (const [guildId, guildState] of Object.entries(state?.guilds || {})) {
    for (const [index, memory] of (guildState.memories || []).entries()) {
      items.push(memoryItemRow(guildId, 'guild', '', memory, index));
    }
    for (const [ownerId, memories] of Object.entries(guildState.userMemories || {})) {
      if (!Array.isArray(memories)) continue;
      for (const [index, memory] of memories.entries()) {
        items.push(memoryItemRow(guildId, 'user', ownerId, memory, index));
      }
    }
  }
  return items
    .sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0))
    .slice(0, Math.max(1, Math.min(1000, Number(limit) || 250)));
}

function deleteMemoryFromState(state, body) {
  const guildId = String(body.guildId || '');
  const scope = String(body.scope || 'guild');
  const ownerId = String(body.ownerId || body.userId || '');
  const targetId = String(body.id || '');
  const targetIndex = Number(body.index);
  const guildState = state?.guilds?.[guildId];
  if (!guildState) return null;

  const matches = (memory, index) => {
    if (targetId) return String(memory?.id || '') === targetId;
    return Number.isInteger(targetIndex) && index === targetIndex;
  };

  if (scope === 'user') {
    const memories = guildState.userMemories?.[ownerId];
    if (!Array.isArray(memories)) return null;
    let removed = null;
    guildState.userMemories[ownerId] = memories.filter((memory, index) => {
      if (!matches(memory, index)) return true;
      removed = memory;
      return false;
    });
    if (!guildState.userMemories[ownerId].length) delete guildState.userMemories[ownerId];
    return removed ? memoryItemRow(guildId, 'user', ownerId, removed, targetIndex) : null;
  }

  if (!Array.isArray(guildState.memories)) return null;
  let removed = null;
  guildState.memories = guildState.memories.filter((memory, index) => {
    if (!matches(memory, index)) return true;
    removed = memory;
    return false;
  });
  return removed ? memoryItemRow(guildId, 'guild', '', removed, targetIndex) : null;
}

function reminderItemRow(guildId, reminder, index) {
  return {
    key: `${guildId}:${reminder?.id || index}`,
    guildId,
    id: reminder?.id || '',
    index,
    text: String(reminder?.text || ''),
    channelId: reminder?.channelId || '',
    voiceChannelId: reminder?.voiceChannelId || '',
    voiceChannelName: reminder?.voiceChannelName || '',
    userId: reminder?.userId || '',
    userName: reminder?.userName || '',
    dueAt: Number(reminder?.dueAt || 0),
    repeatIntervalMs: reminder?.repeatIntervalMs || null,
    repeatLabel: reminder?.repeatLabel || '',
    createdAt: Number(reminder?.createdAt || 0),
  };
}

function collectReminderItems(state, limit = 250) {
  const items = [];
  for (const [guildId, guildState] of Object.entries(state?.guilds || {})) {
    for (const [index, reminder] of (guildState.reminders || []).entries()) {
      items.push(reminderItemRow(guildId, reminder, index));
    }
  }
  return items
    .sort((a, b) => (a.dueAt || 0) - (b.dueAt || 0))
    .slice(0, Math.max(1, Math.min(1000, Number(limit) || 250)));
}

function deleteReminderFromState(state, body) {
  const guildId = String(body.guildId || '');
  const targetId = String(body.id || '');
  const targetIndex = Number(body.index);
  const guildState = state?.guilds?.[guildId];
  if (!guildState || !Array.isArray(guildState.reminders)) return null;
  let removed = null;
  guildState.reminders = guildState.reminders.filter((reminder, index) => {
    const match = targetId ? String(reminder?.id || '') === targetId : (Number.isInteger(targetIndex) && index === targetIndex);
    if (!match) return true;
    removed = reminder;
    return false;
  });
  return removed ? reminderItemRow(guildId, removed, targetIndex) : null;
}

function decodeDockerLogBuffer(buffer) {
  const chunks = [];
  let offset = 0;
  while (offset + 8 <= buffer.length) {
    const streamType = buffer[offset];
    const size = buffer.readUInt32BE(offset + 4);
    if (![1, 2].includes(streamType) || size < 0 || offset + 8 + size > buffer.length) {
      return buffer.toString('utf8').replace(/\u0000/g, '');
    }
    chunks.push(buffer.subarray(offset + 8, offset + 8 + size));
    offset += 8 + size;
  }
  if (offset !== buffer.length) chunks.push(buffer.subarray(offset));
  return Buffer.concat(chunks).toString('utf8');
}

function dockerApi(pathname, { method = 'GET', body = null, expectText = false } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const req = http.request({
      socketPath: dockerSocketPath,
      path: pathname,
      method,
      headers: payload ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) } : {},
    }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        const buffer = Buffer.concat(chunks);
        const text = expectText ? decodeDockerLogBuffer(buffer) : buffer.toString('utf8').replace(/\u0000/g, '');
        if (res.statusCode >= 400) {
          reject(new Error(text || `Docker API HTTP ${res.statusCode}`));
          return;
        }
        if (expectText) {
          resolve(text);
          return;
        }
        try {
          resolve(text ? JSON.parse(text) : {});
        } catch {
          resolve(text);
        }
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

async function dockerStatus() {
  const names = Object.values(dockerContainers);
  const filters = encodeURIComponent(JSON.stringify({ name: names }));
  const containers = await dockerApi(`/containers/json?all=1&filters=${filters}`).catch((error) => ({
    error: error.message || String(error),
    containers: [],
  }));
  if (containers.error) return containers;
  return {
    containers: containers.map((item) => ({
      id: item.Id,
      names: item.Names || [],
      image: item.Image,
      state: item.State,
      status: item.Status,
    })),
  };
}

async function dockerLogs(target, tail = 200) {
  const name = dockerContainers[target];
  if (!name) throw new Error('Unknown container target');
  return dockerApi(`/containers/${encodeURIComponent(name)}/logs?stdout=1&stderr=1&tail=${Math.max(10, Math.min(1000, Number(tail) || 200))}&timestamps=1`, {
    expectText: true,
  });
}

async function dockerRestart(target) {
  const name = dockerContainers[target];
  if (!name) throw new Error('Unknown container target');
  return dockerApi(`/containers/${encodeURIComponent(name)}/restart?t=10`, { method: 'POST' });
}

async function createVoicePreview(body) {
  const voice = String(body.voice || '').trim() || (await readRuntimeConfig()).edgeVoice || 'ru-RU-SvetlanaNeural';
  const text = String(body.text || 'Привет, я голосовой ассистент.').slice(0, 240);
  const rate = String(body.rate || '+0%');
  const pitch = String(body.pitch || '+0Hz');
  const id = crypto.randomBytes(8).toString('hex');
  const textPath = path.join(dataDir, `voice-preview-${id}.txt`);
  const mediaPath = path.join(dataDir, `voice-preview-${id}.mp3`);
  await fs.writeFile(textPath, text);
  try {
    await execFirstAvailable(edgeTtsCommandCandidates(), [
      '--voice', voice,
      '--rate', rate,
      '--pitch', pitch,
      '--file', textPath,
      '--write-media', mediaPath,
    ]);
    const audio = await fs.readFile(mediaPath);
    return audio;
  } finally {
    fs.unlink(textPath).catch(() => {});
    fs.unlink(mediaPath).catch(() => {});
  }
}

function safeBackupPath(file) {
  return storage.backupPath(file);
}

async function apiStatus() {
  const [runtime, status, envValues, backups, presets, docker] = await Promise.all([
    readRuntimeConfig(),
    readJson(statusPath, null),
    readEnvFile(),
    listBackups(),
    Promise.all([getVoicePresets(), getGroqModelPresets()]).then(([voice, models]) => ({ ...voice, ...models })),
    dockerStatus(),
  ]);
  return {
    panel: {
      ok: true,
      pid: process.pid,
      uptimeSec: Math.round(process.uptime()),
      authRequired: Boolean(panelPassword),
      sessionMaxAgeSec: panelSessionMaxAgeSec,
      loginMaxAttempts: panelLoginMaxAttempts,
      host: {
        platform: os.platform(),
        arch: os.arch(),
        loadavg: os.loadavg(),
        totalMem: os.totalmem(),
        freeMem: os.freemem(),
        uptimeSec: Math.round(os.uptime()),
      },
      process: { memory: process.memoryUsage(), cpu: process.cpuUsage() },
      groqLimits: groqLimitsObject(),
      docker,
      storage: storage.info(),
    },
    bot: status,
    runtime: {
      ...runtime,
      groqApiKey: runtime.groqApiKey ? mask(runtime.groqApiKey) : '',
      backupTargetPassword: '',
      backupTargetPasswordSet: Boolean(runtime.backupTargetPassword),
      backupTargetMasked: maskBackupTarget(runtime.backupTargetPath || ''),
      backupLastTargetMasked: maskBackupTarget(runtime.backupLastTarget || ''),
    },
    env: publicEnv(envValues),
    backups,
    presets,
  };
}

async function handleApi(req, res, url) {
  if (url.pathname === '/api/login' && req.method === 'POST') {
    cleanupAuthState();
    const block = currentLoginBlock(req);
    if (block.blocked) {
      send(res, 429, { ok: false, error: 'Too many login attempts', retryAfterSec: block.retryAfterSec }, {
        'Retry-After': String(block.retryAfterSec),
      });
      return;
    }

    const body = await readBody(req).catch(() => ({}));
    if (!panelPassword || timingSafePasswordEquals(body.password)) {
      const token = crypto.randomBytes(32).toString('base64url');
      sessions.set(token, { createdAt: Date.now(), lastSeenAt: Date.now(), expiresAt: Date.now() + panelSessionMaxAgeSec * 1000 });
      clearBadLogins(req);
      send(res, 200, { ok: true }, {
        'Set-Cookie': sessionCookie(token, req),
      });
      return;
    }
    const remainingAttempts = recordBadLogin(req);
    send(res, 401, { ok: false, error: 'Bad password', remainingAttempts });
    return;
  }

  if (url.pathname === '/api/logout' && req.method === 'POST') {
    const token = cookieValue(req, 'panel_session');
    if (token) sessions.delete(token);
    send(res, 200, { ok: true }, {
      'Set-Cookie': clearSessionCookie(req),
    });
    return;
  }

  if (!isAuthed(req)) {
    send(res, 401, { ok: false, authRequired: true }, {
      'Set-Cookie': clearSessionCookie(req),
    });
    return;
  }

  if (url.pathname === '/api/status' && req.method === 'GET') {
    send(res, 200, await apiStatus());
    return;
  }

  if (url.pathname === '/api/runtime' && req.method === 'POST') {
    const body = await readBody(req);
    const patch = {};
    for (const key of ['botEnabled', 'listeningPaused', 'assistantName', 'wakeWord', 'wakeAliases', 'wakeFuzzy', 'groqChatModel', 'groqSttModel', 'actionParserModel', 'webSearchEnabled', 'webSearchModel', 'idleChatterEnabled', 'idleChatterMinutes', 'idleChatterUseWeb', 'idleChatterStyle', 'idleLeaveEnabled', 'idleLeaveMinutes', 'idleLeavePhrase', 'presenceAnnouncementsEnabled', 'activeDialogueEnabled', 'activeDialogueSeconds', 'confirmDangerousActions', 'assistantPersona', 'healthcheckEnabled', 'sttLanguage', 'ttsProvider', 'macosVoice', 'espeakVoice', 'espeakSpeed', 'edgeVoice', 'edgeEnglishVoice', 'edgeRate', 'edgePitch', 'backupEnabled', 'backupTargetPath', 'backupTargetUsername', 'backupTargetPassword', 'backupClearCredentials', 'backupIntervalHours', 'backupRetention', 'backupIdleOnly']) {
      if (body[key] !== undefined) patch[key] = body[key];
    }
    const runtime = await writeRuntimeConfig(patch);
    send(res, 200, {
      ok: true,
      runtime: {
        ...runtime,
        groqApiKey: runtime.groqApiKey ? mask(runtime.groqApiKey) : '',
        backupTargetPassword: '',
        backupTargetPasswordSet: Boolean(runtime.backupTargetPassword),
      },
    });
    return;
  }

  if (url.pathname === '/api/events' && req.method === 'GET') {
    send(res, 200, { ok: true, events: await readEventLog(url.searchParams.get('limit') || 120) });
    return;
  }

  if (url.pathname === '/api/memory' && req.method === 'GET') {
    const state = await storage.loadState();
    send(res, 200, {
      ok: true,
      stats: memoryStatsFromState(state),
      memories: collectMemoryItems(state, url.searchParams.get('limit') || 250),
      storage: storage.info(),
    });
    return;
  }

  if (url.pathname === '/api/memory/delete' && req.method === 'POST') {
    const body = await readBody(req);
    const { state, result: removed } = await withStateMutation((currentState) => deleteMemoryFromState(currentState, body));
    if (!removed) {
      send(res, 404, { ok: false, error: 'Memory item not found' });
      return;
    }
    await storage.appendEvent({
      ts: new Date().toISOString(),
      type: 'panel_memory_deleted',
      payload: { guildId: removed.guildId, scope: removed.scope, id: removed.id, text: removed.text },
    }).catch(() => {});
    send(res, 200, {
      ok: true,
      deleted: removed,
      stats: memoryStatsFromState(state),
      memories: collectMemoryItems(state, url.searchParams.get('limit') || 250),
    });
    return;
  }

  if (url.pathname === '/api/reminders' && req.method === 'GET') {
    const state = await storage.loadState();
    send(res, 200, {
      ok: true,
      stats: memoryStatsFromState(state),
      reminders: collectReminderItems(state, url.searchParams.get('limit') || 250),
      storage: storage.info(),
    });
    return;
  }

  if (url.pathname === '/api/reminders/delete' && req.method === 'POST') {
    const body = await readBody(req);
    const { state, result: removed } = await withStateMutation((currentState) => deleteReminderFromState(currentState, body));
    if (!removed) {
      send(res, 404, { ok: false, error: 'Reminder not found' });
      return;
    }
    await storage.appendEvent({
      ts: new Date().toISOString(),
      type: 'panel_reminder_deleted',
      payload: { guildId: removed.guildId, id: removed.id, text: removed.text, dueAt: removed.dueAt },
    }).catch(() => {});
    send(res, 200, {
      ok: true,
      deleted: removed,
      stats: memoryStatsFromState(state),
      reminders: collectReminderItems(state, url.searchParams.get('limit') || 250),
    });
    return;
  }

  if (url.pathname === '/api/docker/status' && req.method === 'GET') {
    send(res, 200, { ok: true, docker: await dockerStatus() });
    return;
  }

  if (url.pathname === '/api/docker/logs' && req.method === 'GET') {
    send(res, 200, { ok: true, logs: await dockerLogs(url.searchParams.get('target') || 'bot', url.searchParams.get('tail') || 200) });
    return;
  }

  if (url.pathname === '/api/docker/restart' && req.method === 'POST') {
    const body = await readBody(req);
    const target = String(body.target || 'bot');
    if (target === 'panel') {
      send(res, 200, { ok: true, restarting: target });
      setTimeout(() => {
        dockerRestart(target).catch((error) => console.error('panel docker restart failed:', error));
      }, 250);
      return;
    }
    await dockerRestart(target);
    send(res, 200, { ok: true, restarted: target });
    return;
  }

  if (url.pathname === '/api/voice/preview' && req.method === 'POST') {
    const body = await readBody(req);
    const audio = await createVoicePreview(body);
    res.writeHead(200, {
      'Content-Type': 'audio/mpeg',
      'Cache-Control': 'no-store',
    });
    res.end(audio);
    return;
  }

  if (url.pathname === '/api/secrets' && req.method === 'POST') {
    const body = await readBody(req);
    const envPatch = {};
    if (body.discordToken) envPatch.DISCORD_TOKEN = String(body.discordToken).trim();
    if (body.groqApiKey) envPatch.GROQ_API_KEY = String(body.groqApiKey).trim();
    if (body.discordGuildId !== undefined) envPatch.DISCORD_GUILD_ID = String(body.discordGuildId).trim();
    await updateEnvFile(envPatch);
    if (body.groqApiKey) await writeRuntimeConfig({ groqApiKey: String(body.groqApiKey).trim() });
    send(res, 200, { ok: true, restartRequired: Boolean(body.discordToken || body.discordGuildId) });
    return;
  }

  if (url.pathname === '/api/backups' && req.method === 'GET') {
    send(res, 200, { ok: true, backups: await listBackups() });
    return;
  }

  if (url.pathname === '/api/limits/probe' && req.method === 'POST') {
    send(res, 200, { ok: true, limits: await probeGroqLimits() });
    return;
  }

  if (url.pathname === '/api/models/refresh' && req.method === 'POST') {
    send(res, 200, { ok: true, presets: await getGroqModelPresets(true) });
    return;
  }

  if (url.pathname === '/api/backups/create' && req.method === 'POST') {
    const backup = await createBackupAndSync({ manual: true });
    send(res, 200, { ok: true, file: backup.file, backup, backups: await listBackups() });
    return;
  }

  if (url.pathname === '/api/backups/restore' && req.method === 'POST') {
    const body = await readBody(req);
    if (!safeBackupPath(body.file)) {
      send(res, 400, { ok: false, error: 'Bad backup file' });
      return;
    }
    const restored = await storage.restoreBackup(body.file);
    send(res, 200, { ok: true, restartRecommended: true, restored });
    return;
  }

  if (url.pathname.startsWith('/api/backups/download/') && req.method === 'GET') {
    const file = decodeURIComponent(url.pathname.split('/').pop() || '');
    const backup = storage.createBackupReadStream(file);
    if (!backup) {
      send(res, 404, { ok: false, error: 'Not found' });
      return;
    }
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Content-Disposition': `attachment; filename="${path.basename(backup.path)}"`,
    });
    backup.stream.pipe(res);
    return;
  }

  send(res, 404, { ok: false, error: 'Not found' });
}

async function serveStatic(req, res, url) {
  const requested = url.pathname === '/' ? '/index.html' : url.pathname;
  if (['/index.html', '/'].includes(url.pathname) && !isAuthed(req)) {
    await serveFile(res, path.join(publicDir, 'login.html'));
    return;
  }
  if (requested === '/login.html' && isAuthed(req)) {
    res.writeHead(302, { Location: '/', 'Cache-Control': 'no-store' });
    res.end();
    return;
  }

  const fullPath = path.normalize(path.join(publicDir, requested));
  if (!fullPath.startsWith(publicDir)) {
    send(res, 403, 'Forbidden');
    return;
  }
  await serveFile(res, fullPath);
}

async function serveFile(res, fullPath) {
  const stat = await fs.stat(fullPath).catch(() => null);
  if (!stat?.isFile()) {
    send(res, 404, 'Not found');
    return;
  }
  const ext = path.extname(fullPath);
  const types = {
    '.html': 'text/html; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.svg': 'image/svg+xml',
  };
  res.writeHead(200, {
    'Content-Type': types[ext] || 'application/octet-stream',
    'Cache-Control': 'no-store',
  });
  createReadStream(fullPath).pipe(res);
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (url.pathname.startsWith('/api/')) {
    handleApi(req, res, url).catch((error) => {
      console.error('panel api failed:', error);
      send(res, 500, { ok: false, error: error.message || String(error) });
    });
    return;
  }
  serveStatic(req, res, url).catch((error) => {
    console.error('panel static failed:', error);
    send(res, 500, 'Internal error');
  });
});

server.listen(panelPort, panelHost, () => {
  console.log(`Panel listening on http://${panelHost}:${panelPort}`);
  if (!panelPassword) console.warn('PANEL_PASSWORD is not set. Keep the panel bound to localhost or protect it with a reverse proxy.');
});
