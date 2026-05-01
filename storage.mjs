import { createReadStream, promises as fs } from 'node:fs';
import path from 'node:path';

function emptyStateStore() {
  return { version: 1, guilds: {} };
}

function normalizeStateStore(value) {
  if (!value || typeof value !== 'object') return emptyStateStore();
  if (!value.guilds || typeof value.guilds !== 'object') value.guilds = {};
  value.version = 1;
  for (const guildState of Object.values(value.guilds)) {
    if (!guildState || typeof guildState !== 'object') continue;
    if (!Array.isArray(guildState.memories)) guildState.memories = [];
    if (!guildState.userMemories || typeof guildState.userMemories !== 'object') guildState.userMemories = {};
    if (!guildState.userProfiles || typeof guildState.userProfiles !== 'object') guildState.userProfiles = {};
    if (!Array.isArray(guildState.reminders)) guildState.reminders = [];
  }
  return value;
}

function parseJson(text, fallback = null) {
  if (!text) return fallback;
  if (typeof text === 'object') return text;
  try {
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}

async function readJson(filePath, fallback = null) {
  const raw = await fs.readFile(filePath, 'utf8').catch(() => null);
  return parseJson(raw, fallback);
}

async function writeJson(filePath, value) {
  await writeTextFile(filePath, JSON.stringify(value, null, 2));
}

async function writeJsonIfChanged(filePath, value) {
  const next = JSON.stringify(value, null, 2);
  const current = await fs.readFile(filePath, 'utf8').catch(() => null);
  if (current === next) return false;
  await writeTextFile(filePath, next);
  return true;
}

async function writeTextFile(filePath, text) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const tmpPath = `${filePath}.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2, 8)}.tmp`;
  await fs.writeFile(tmpPath, text);
  await fs.rename(tmpPath, filePath);
}

function safeEventValue(value) {
  if (value === null || value === undefined) return value;
  if (typeof value === 'string') return value.length > 2500 ? `${value.slice(0, 2500)}...` : value;
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (Array.isArray(value)) return value.slice(0, 40).map(safeEventValue);
  if (typeof value === 'object') {
    const result = {};
    for (const [key, item] of Object.entries(value).slice(0, 80)) {
      result[key] = /token|secret|password|apiKey|authorization/i.test(key) ? '[redacted]' : safeEventValue(item);
    }
    return result;
  }
  return String(value);
}

function sanitizeRuntimeConfigForBackup(config = {}) {
  const result = { ...(config || {}) };
  for (const key of Object.keys(result)) {
    if (/token|secret|password|apiKey|authorization/i.test(key)) {
      result[key] = result[key] ? '[redacted]' : '';
    }
  }
  if (typeof result.backupTargetPath === 'string') {
    try {
      const url = new URL(result.backupTargetPath);
      url.username = '';
      url.password = '';
      result.backupTargetPath = url.href.replace(/%20/gu, ' ');
    } catch {
      // Local filesystem paths are safe to keep as-is.
    }
  }
  if (typeof result.backupLastTarget === 'string') {
    try {
      const url = new URL(result.backupLastTarget);
      url.username = '';
      url.password = '';
      result.backupLastTarget = url.href.replace(/%20/gu, ' ');
    } catch {
      // Local filesystem paths are safe to keep as-is.
    }
  }
  return result;
}

function sanitizeRuntimeConfigFromBackup(config = {}) {
  const result = { ...(config || {}) };
  for (const [key, value] of Object.entries(result)) {
    if (/token|secret|password|apiKey|authorization/i.test(key) && value === '[redacted]') {
      result[key] = '';
    }
  }
  return result;
}

function hasStateContent(state) {
  return Object.values(state?.guilds || {}).some((guildState) => {
    if (!guildState || typeof guildState !== 'object') return false;
    if ((guildState.memories || []).length) return true;
    if ((guildState.reminders || []).length) return true;
    if (Object.keys(guildState.userProfiles || {}).length) return true;
    return Object.values(guildState.userMemories || {}).some((items) => Array.isArray(items) && items.length);
  });
}

function hasProfileContent(state) {
  return Object.values(state?.guilds || {}).some((guildState) => (
    guildState
    && typeof guildState === 'object'
    && Object.keys(guildState.userProfiles || {}).length > 0
  ));
}

function backupFileName() {
  const stamp = new Date().toISOString().replace(/[:.]/gu, '-');
  return `state-${stamp}.json`;
}

function backupEventLimit() {
  return Math.max(0, Math.min(20_000, Number(process.env.BACKUP_EVENT_LIMIT || 5000)));
}

function isSafeBackupName(file) {
  return /^state-\d{4}-\d{2}-\d{2}T.*\.json$/u.test(path.basename(String(file || '')));
}

function emptyAutonomyStore() {
  return { version: 1, conversationJournal: [], memoryFacts: [], assistantReflections: [] };
}

function normalizeAutonomyStore(value) {
  const store = value && typeof value === 'object' ? value : emptyAutonomyStore();
  store.version = 1;
  if (!Array.isArray(store.conversationJournal)) store.conversationJournal = [];
  if (!Array.isArray(store.memoryFacts)) store.memoryFacts = [];
  if (!Array.isArray(store.assistantReflections)) store.assistantReflections = [];
  return store;
}

function autonomyId(prefix) {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function autonomyLimit(value, fallback = 250) {
  return Math.max(1, Math.min(5000, Number(value) || fallback));
}

function jsonAutonomyMaxRows() {
  return Math.max(100, Math.min(50_000, Number(process.env.AUTONOMY_JSON_MAX_ROWS || 5000)));
}

function safeAutonomyText(value, limit = 2000) {
  return String(value || '')
    .replace(/\s+/gu, ' ')
    .trim()
    .slice(0, limit);
}

function normalizeJournalRow(row = {}) {
  return {
    id: String(row.id || autonomyId('cj')),
    guildId: String(row.guildId || 'global'),
    guildName: safeAutonomyText(row.guildName, 190),
    voiceChannelId: row.voiceChannelId ? String(row.voiceChannelId) : null,
    voiceChannelName: safeAutonomyText(row.voiceChannelName, 190),
    userId: row.userId ? String(row.userId) : null,
    userName: safeAutonomyText(row.userName, 190),
    transcript: safeAutonomyText(row.transcript, 2400),
    prompt: safeAutonomyText(row.prompt, 1600),
    wake: row.wake === true,
    wakeListen: row.wakeListen === true,
    usedForAnswer: row.usedForAnswer === true,
    source: safeAutonomyText(row.source || 'voice', 40),
    createdAt: Number(row.createdAt || Date.now()),
    processedAt: row.processedAt ? Number(row.processedAt) : null,
    meta: row.meta && typeof row.meta === 'object' ? safeEventValue(row.meta) : {},
  };
}

function normalizeMemoryFactRow(row = {}) {
  return {
    id: String(row.id || autonomyId('fact')),
    guildId: String(row.guildId || 'global'),
    userId: row.userId ? String(row.userId) : null,
    userName: safeAutonomyText(row.userName, 190),
    kind: safeAutonomyText(row.kind || 'general', 40),
    text: safeAutonomyText(row.text, 1400),
    confidence: Math.max(0, Math.min(1, Number(row.confidence ?? 0.6))),
    sourceJournalIds: Array.isArray(row.sourceJournalIds) ? row.sourceJournalIds.map(String).slice(0, 30) : [],
    createdAt: Number(row.createdAt || Date.now()),
    updatedAt: Number(row.updatedAt || Date.now()),
    meta: row.meta && typeof row.meta === 'object' ? safeEventValue(row.meta) : {},
  };
}

function normalizeReflectionRow(row = {}) {
  return {
    id: String(row.id || autonomyId('refl')),
    guildId: String(row.guildId || 'global'),
    guildName: safeAutonomyText(row.guildName, 190),
    voiceChannelId: row.voiceChannelId ? String(row.voiceChannelId) : null,
    voiceChannelName: safeAutonomyText(row.voiceChannelName, 190),
    text: safeAutonomyText(row.text, 800),
    spoken: row.spoken === true,
    sent: row.sent === true,
    reason: safeAutonomyText(row.reason, 240),
    createdAt: Number(row.createdAt || Date.now()),
    meta: row.meta && typeof row.meta === 'object' ? safeEventValue(row.meta) : {},
  };
}

class JsonStorage {
  constructor({ dataDir, logger = console }) {
    this.driver = 'json';
    this.connected = true;
    this.logger = logger;
    this.dataDir = dataDir;
    this.statePath = path.join(dataDir, 'state.json');
    this.runtimeConfigPath = path.join(dataDir, 'runtime-config.json');
    this.eventLogPath = path.join(dataDir, 'events.jsonl');
    this.autonomyPath = path.join(dataDir, 'autonomy.json');
    this.backupsDir = path.join(dataDir, 'backups');
  }

  async init() {
    await fs.mkdir(this.dataDir, { recursive: true });
    await fs.mkdir(this.backupsDir, { recursive: true });
    return this;
  }

  info() {
    return {
      driver: this.driver,
      connected: this.connected,
      dataDir: this.dataDir,
      statePath: this.statePath,
      runtimeConfigPath: this.runtimeConfigPath,
    };
  }

  async loadState() {
    const raw = await fs.readFile(this.statePath, 'utf8').catch((error) => {
      if (error.code === 'ENOENT') return null;
      throw error;
    });
    if (!raw) return emptyStateStore();
    try {
      return normalizeStateStore(JSON.parse(raw));
    } catch (error) {
      const brokenPath = `${this.statePath}.broken-${Date.now()}`;
      await fs.rename(this.statePath, brokenPath).catch(() => {});
      this.logger.error?.(`state store is corrupted, moved to ${brokenPath}:`, error);
      return emptyStateStore();
    }
  }

  async saveState(state) {
    await writeJson(this.statePath, normalizeStateStore(state));
  }

  async loadRuntimeConfig(fallback = {}) {
    return await readJson(this.runtimeConfigPath, fallback) || fallback;
  }

  async saveRuntimeConfig(config) {
    await writeJson(this.runtimeConfigPath, config || {});
  }

  async appendEvent(row) {
    await fs.appendFile(this.eventLogPath, `${JSON.stringify(row)}\n`);
  }

  async readEvents(limit = 120) {
    const raw = await fs.readFile(this.eventLogPath, 'utf8').catch(() => '');
    if (!raw) return [];
    return raw
      .trim()
      .split(/\r?\n/u)
      .filter(Boolean)
      .slice(-Math.max(1, Math.min(500, Number(limit) || 120)))
      .map((line) => parseJson(line, { ts: null, type: 'broken_log_line', payload: { line } }))
      .reverse();
  }

  async replaceEvents(events = []) {
    const rows = Array.isArray(events) ? events : [];
    const text = rows
      .map((row) => JSON.stringify({
        ts: row?.ts || new Date().toISOString(),
        type: String(row?.type || 'event').slice(0, 120),
        payload: safeEventValue(row?.payload || {}),
      }))
      .join('\n');
    await writeTextFile(this.eventLogPath, text ? `${text}\n` : '');
  }

  async loadAutonomyData() {
    return normalizeAutonomyStore(await readJson(this.autonomyPath, emptyAutonomyStore()));
  }

  async saveAutonomyData(value) {
    await writeJson(this.autonomyPath, normalizeAutonomyStore(value));
  }

  async exportAutonomyData() {
    return await this.loadAutonomyData();
  }

  async replaceAutonomyData(value = {}) {
    await this.saveAutonomyData(value);
  }

  async appendConversationJournal(row) {
    const store = await this.loadAutonomyData();
    const item = normalizeJournalRow(row);
    if (!item.transcript) return null;
    store.conversationJournal.push(item);
    store.conversationJournal.splice(0, Math.max(0, store.conversationJournal.length - jsonAutonomyMaxRows()));
    await this.saveAutonomyData(store);
    return item;
  }

  async listConversationJournal(options = {}) {
    const store = await this.loadAutonomyData();
    const guildId = options.guildId ? String(options.guildId) : '';
    const sinceMs = Number(options.sinceMs || 0);
    const processed = options.processed;
    const rows = store.conversationJournal
      .filter((row) => !guildId || row.guildId === guildId)
      .filter((row) => !sinceMs || Number(row.createdAt || 0) >= sinceMs)
      .filter((row) => processed === undefined ? true : (processed ? Boolean(row.processedAt) : !row.processedAt))
      .sort((a, b) => Number(a.createdAt || 0) - Number(b.createdAt || 0));
    return rows.slice(-autonomyLimit(options.limit, 250));
  }

  async listConversationJournalGuildIds(options = {}) {
    const store = await this.loadAutonomyData();
    const processed = options.processed;
    const ids = [];
    const seen = new Set();
    for (const row of store.conversationJournal) {
      if (processed !== undefined && (processed ? !row.processedAt : row.processedAt)) continue;
      const guildId = String(row.guildId || '').trim();
      if (!guildId || seen.has(guildId)) continue;
      seen.add(guildId);
      ids.push(guildId);
      if (ids.length >= autonomyLimit(options.limit, 250)) break;
    }
    return ids;
  }

  async markConversationJournalProcessed(ids = [], processedAt = Date.now()) {
    const idSet = new Set((Array.isArray(ids) ? ids : []).map(String));
    if (!idSet.size) return 0;
    const store = await this.loadAutonomyData();
    let changed = 0;
    for (const row of store.conversationJournal) {
      if (!idSet.has(String(row.id))) continue;
      row.processedAt = processedAt;
      changed += 1;
    }
    if (changed) await this.saveAutonomyData(store);
    return changed;
  }

  async upsertMemoryFact(row) {
    const item = normalizeMemoryFactRow(row);
    if (!item.text) return null;
    const store = await this.loadAutonomyData();
    const index = store.memoryFacts.findIndex((fact) => fact.id === item.id);
    if (index >= 0) store.memoryFacts[index] = { ...store.memoryFacts[index], ...item, updatedAt: Date.now() };
    else store.memoryFacts.push(item);
    store.memoryFacts.splice(0, Math.max(0, store.memoryFacts.length - jsonAutonomyMaxRows()));
    await this.saveAutonomyData(store);
    return item;
  }

  async listMemoryFacts(options = {}) {
    const store = await this.loadAutonomyData();
    const guildId = options.guildId ? String(options.guildId) : '';
    const userId = options.userId ? String(options.userId) : '';
    const rows = store.memoryFacts
      .filter((row) => !guildId || row.guildId === guildId)
      .filter((row) => !userId || row.userId === userId)
      .sort((a, b) => Number(b.updatedAt || b.createdAt || 0) - Number(a.updatedAt || a.createdAt || 0));
    return rows.slice(0, autonomyLimit(options.limit, 250));
  }

  async appendAssistantReflection(row) {
    const store = await this.loadAutonomyData();
    const item = normalizeReflectionRow(row);
    if (!item.text) return null;
    store.assistantReflections.push(item);
    store.assistantReflections.splice(0, Math.max(0, store.assistantReflections.length - jsonAutonomyMaxRows()));
    await this.saveAutonomyData(store);
    return item;
  }

  async listAssistantReflections(options = {}) {
    const store = await this.loadAutonomyData();
    const guildId = options.guildId ? String(options.guildId) : '';
    const rows = store.assistantReflections
      .filter((row) => !guildId || row.guildId === guildId)
      .sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0));
    return rows.slice(0, autonomyLimit(options.limit, 250));
  }

  async autonomyStats() {
    const store = await this.loadAutonomyData();
    return {
      journal: store.conversationJournal.length,
      unprocessedJournal: store.conversationJournal.filter((row) => !row.processedAt).length,
      facts: store.memoryFacts.length,
      reflections: store.assistantReflections.length,
    };
  }

  async listBackups() {
    const files = await fs.readdir(this.backupsDir).catch(() => []);
    const rows = [];
    for (const file of files.filter((item) => item.endsWith('.json'))) {
      const fullPath = path.join(this.backupsDir, file);
      const stat = await fs.stat(fullPath).catch(() => null);
      if (stat) rows.push({ file, size: stat.size, createdAt: stat.mtimeMs });
    }
    return rows.sort((a, b) => b.createdAt - a.createdAt);
  }

  backupPath(file) {
    const base = path.basename(String(file || ''));
    if (!isSafeBackupName(base)) return null;
    return path.join(this.backupsDir, base);
  }

  async createBackup() {
    const state = await this.loadState();
    const runtimeConfig = await this.loadRuntimeConfig({});
    const payload = {
      version: 2,
      createdAt: new Date().toISOString(),
      storageDriver: this.driver,
      state,
      runtimeConfig: sanitizeRuntimeConfigForBackup(runtimeConfig),
      events: backupEventLimit() ? await this.readEvents(backupEventLimit()) : [],
      autonomy: await this.exportAutonomyData(),
    };
    const file = backupFileName();
    const fullPath = path.join(this.backupsDir, file);
    await writeJson(fullPath, payload);
    const stat = await fs.stat(fullPath);
    return { file, size: stat.size, createdAt: stat.mtimeMs };
  }

  async restoreBackup(file) {
    const backupPath = this.backupPath(file);
    if (!backupPath) throw new Error('Bad backup file');
    const content = await fs.readFile(backupPath, 'utf8');
    const parsed = JSON.parse(content);
    const state = parsed?.state ? normalizeStateStore(parsed.state) : normalizeStateStore(parsed);
    const runtimeConfig = parsed?.runtimeConfig && typeof parsed.runtimeConfig === 'object'
      ? sanitizeRuntimeConfigFromBackup(parsed.runtimeConfig)
      : null;
    const events = Array.isArray(parsed?.events) ? parsed.events : null;
    const autonomy = parsed?.autonomy && typeof parsed.autonomy === 'object' ? parsed.autonomy : null;
    await this.saveState(state);
    if (runtimeConfig) await this.saveRuntimeConfig(runtimeConfig);
    if (events) await this.replaceEvents(events);
    if (autonomy) await this.replaceAutonomyData(autonomy);
    return {
      stateRestored: true,
      runtimeRestored: Boolean(runtimeConfig),
      eventsRestored: events?.length || 0,
      autonomyRestored: Boolean(autonomy),
    };
  }

  createBackupReadStream(file) {
    const backupPath = this.backupPath(file);
    if (!backupPath) return null;
    return { stream: createReadStream(backupPath), path: backupPath };
  }
}

class MySqlStorage extends JsonStorage {
  constructor(options) {
    super(options);
    this.driver = 'mysql';
    this.connected = false;
    this.pool = null;
    this.migrateFromJson = String(process.env.DB_MIGRATE_FROM_JSON || 'true') !== 'false';
  }

  async init() {
    await super.init();
    const mysql = await import('mysql2/promise');
    this.pool = mysql.createPool({
      host: process.env.DB_HOST || '127.0.0.1',
      port: Number(process.env.DB_PORT || 3306),
      user: process.env.DB_USER || 'assistant',
      password: process.env.DB_PASSWORD || '',
      database: process.env.DB_NAME || 'discord_ai_assistant',
      waitForConnections: true,
      connectionLimit: Math.max(1, Number(process.env.DB_CONNECTION_LIMIT || 5)),
      namedPlaceholders: false,
      charset: 'utf8mb4',
    });
    await this.waitForConnection();
    await this.ensureSchema();
    this.connected = true;
    return this;
  }

  info() {
    return {
      ...super.info(),
      driver: this.driver,
      connected: this.connected,
      host: process.env.DB_HOST || '127.0.0.1',
      database: process.env.DB_NAME || 'discord_ai_assistant',
    };
  }

  async waitForConnection() {
    const attempts = Math.max(1, Number(process.env.DB_CONNECT_ATTEMPTS || 45));
    let lastError = null;
    for (let index = 1; index <= attempts; index += 1) {
      try {
        const connection = await this.pool.getConnection();
        connection.release();
        return;
      } catch (error) {
        lastError = error;
        await new Promise((resolve) => setTimeout(resolve, Math.min(5000, 500 * index)));
      }
    }
    throw lastError || new Error('Database connection failed');
  }

  async ensureSchema() {
    await this.pool.execute(`
      CREATE TABLE IF NOT EXISTS runtime_config (
        config_key VARCHAR(80) PRIMARY KEY,
        value_json LONGTEXT NOT NULL,
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);
    await this.pool.execute(`
      CREATE TABLE IF NOT EXISTS guild_memories (
        id VARCHAR(80) PRIMARY KEY,
        guild_id VARCHAR(32) NOT NULL,
        scope VARCHAR(16) NOT NULL,
        user_id VARCHAR(32) NULL,
        user_name VARCHAR(190) NULL,
        text TEXT NOT NULL,
        created_at_ms BIGINT NOT NULL,
        raw_json LONGTEXT NOT NULL,
        INDEX idx_guild_scope (guild_id, scope, user_id),
        INDEX idx_created_at (created_at_ms)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);
    await this.pool.execute(`
      CREATE TABLE IF NOT EXISTS reminders (
        id VARCHAR(80) PRIMARY KEY,
        guild_id VARCHAR(32) NOT NULL,
        channel_id VARCHAR(32) NULL,
        voice_channel_id VARCHAR(32) NULL,
        voice_channel_name VARCHAR(190) NULL,
        user_id VARCHAR(32) NULL,
        user_name VARCHAR(190) NULL,
        text TEXT NOT NULL,
        due_at_ms BIGINT NOT NULL,
        repeat_interval_ms BIGINT NULL,
        repeat_label VARCHAR(255) NULL,
        raw_json LONGTEXT NOT NULL,
        INDEX idx_guild_due (guild_id, due_at_ms),
        INDEX idx_user_due (user_id, due_at_ms)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);
    await this.pool.execute(`
      CREATE TABLE IF NOT EXISTS user_profiles (
        guild_id VARCHAR(32) NOT NULL,
        user_id VARCHAR(32) NOT NULL,
        user_name VARCHAR(190) NULL,
        preferred_name VARCHAR(190) NULL,
        timezone VARCHAR(80) NULL,
        updated_at_ms BIGINT NOT NULL,
        raw_json LONGTEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id),
        INDEX idx_updated_at (updated_at_ms)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);
    await this.pool.execute(`
      CREATE TABLE IF NOT EXISTS event_logs (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        ts VARCHAR(40) NOT NULL,
        type VARCHAR(120) NOT NULL,
        payload_json LONGTEXT NOT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        INDEX idx_type_created (type, created_at),
        INDEX idx_created (created_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);
    await this.pool.execute(`
      CREATE TABLE IF NOT EXISTS conversation_journal (
        id VARCHAR(80) PRIMARY KEY,
        guild_id VARCHAR(32) NOT NULL,
        guild_name VARCHAR(190) NULL,
        voice_channel_id VARCHAR(32) NULL,
        voice_channel_name VARCHAR(190) NULL,
        user_id VARCHAR(32) NULL,
        user_name VARCHAR(190) NULL,
        transcript TEXT NOT NULL,
        prompt TEXT NULL,
        wake TINYINT(1) NOT NULL DEFAULT 0,
        wake_listen TINYINT(1) NOT NULL DEFAULT 0,
        used_for_answer TINYINT(1) NOT NULL DEFAULT 0,
        source VARCHAR(40) NOT NULL DEFAULT 'voice',
        created_at_ms BIGINT NOT NULL,
        processed_at_ms BIGINT NULL,
        raw_json LONGTEXT NOT NULL,
        INDEX idx_journal_guild_created (guild_id, created_at_ms),
        INDEX idx_journal_processed (processed_at_ms),
        INDEX idx_journal_user_created (user_id, created_at_ms)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);
    await this.pool.execute(`
      CREATE TABLE IF NOT EXISTS memory_facts (
        id VARCHAR(80) PRIMARY KEY,
        guild_id VARCHAR(32) NOT NULL,
        user_id VARCHAR(32) NULL,
        user_name VARCHAR(190) NULL,
        kind VARCHAR(40) NOT NULL DEFAULT 'general',
        text TEXT NOT NULL,
        confidence DOUBLE NOT NULL DEFAULT 0.6,
        created_at_ms BIGINT NOT NULL,
        updated_at_ms BIGINT NOT NULL,
        raw_json LONGTEXT NOT NULL,
        INDEX idx_fact_guild_updated (guild_id, updated_at_ms),
        INDEX idx_fact_user_updated (user_id, updated_at_ms),
        FULLTEXT INDEX ft_fact_text (text)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);
    await this.pool.execute(`
      CREATE TABLE IF NOT EXISTS assistant_reflections (
        id VARCHAR(80) PRIMARY KEY,
        guild_id VARCHAR(32) NOT NULL,
        guild_name VARCHAR(190) NULL,
        voice_channel_id VARCHAR(32) NULL,
        voice_channel_name VARCHAR(190) NULL,
        text TEXT NOT NULL,
        spoken TINYINT(1) NOT NULL DEFAULT 0,
        sent TINYINT(1) NOT NULL DEFAULT 0,
        reason VARCHAR(240) NULL,
        created_at_ms BIGINT NOT NULL,
        raw_json LONGTEXT NOT NULL,
        INDEX idx_reflection_guild_created (guild_id, created_at_ms)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);
  }

  async loadState() {
    const [memoryRows] = await this.pool.query('SELECT * FROM guild_memories ORDER BY created_at_ms ASC, id ASC');
    const [reminderRows] = await this.pool.query('SELECT * FROM reminders ORDER BY due_at_ms ASC, id ASC');
    const [profileRows] = await this.pool.query('SELECT * FROM user_profiles ORDER BY updated_at_ms ASC, user_id ASC');
    if (!memoryRows.length && !reminderRows.length && !profileRows.length && this.migrateFromJson) {
      const jsonState = await super.loadState();
      if (hasStateContent(jsonState)) {
        await this.saveState(jsonState);
        this.logger.log?.('Migrated state.json into MySQL storage.');
        return jsonState;
      }
    }

    const state = emptyStateStore();
    const guildState = (guildId) => {
      const key = String(guildId || 'global');
      if (!state.guilds[key]) state.guilds[key] = { memories: [], userMemories: {}, userProfiles: {}, reminders: [] };
      return state.guilds[key];
    };

    for (const row of memoryRows) {
      const raw = parseJson(row.raw_json, null);
      const item = raw && typeof raw === 'object'
        ? raw
        : {
          id: row.id,
          text: row.text,
          userId: row.user_id,
          userName: row.user_name,
          createdAt: Number(row.created_at_ms),
        };
      const current = guildState(row.guild_id);
      if (row.scope === 'user') {
        const userId = row.user_id || 'unknown';
        if (!Array.isArray(current.userMemories[userId])) current.userMemories[userId] = [];
        current.userMemories[userId].push(item);
      } else {
        current.memories.push(item);
      }
    }

    for (const row of reminderRows) {
      const raw = parseJson(row.raw_json, null);
      guildState(row.guild_id).reminders.push(raw && typeof raw === 'object'
        ? raw
        : {
          id: row.id,
          guildId: row.guild_id,
          channelId: row.channel_id,
          voiceChannelId: row.voice_channel_id,
          voiceChannelName: row.voice_channel_name,
          userId: row.user_id,
          userName: row.user_name,
          text: row.text,
          dueAt: Number(row.due_at_ms),
          repeatIntervalMs: row.repeat_interval_ms === null ? null : Number(row.repeat_interval_ms),
          repeatLabel: row.repeat_label,
        });
    }
    for (const row of profileRows) {
      const raw = parseJson(row.raw_json, null);
      const userId = String(row.user_id || raw?.userId || 'unknown');
      guildState(row.guild_id).userProfiles[userId] = raw && typeof raw === 'object'
        ? { ...raw, userId }
        : {
          userId,
          userName: row.user_name,
          preferredName: row.preferred_name,
          timezone: row.timezone,
          updatedAt: Number(row.updated_at_ms || Date.now()),
        };
    }

    if (!profileRows.length && this.migrateFromJson) {
      const jsonState = await super.loadState();
      if (hasProfileContent(jsonState)) {
        for (const [guildId, jsonGuildState] of Object.entries(jsonState.guilds || {})) {
          if (!jsonGuildState?.userProfiles || typeof jsonGuildState.userProfiles !== 'object') continue;
          guildState(guildId).userProfiles = {
            ...guildState(guildId).userProfiles,
            ...jsonGuildState.userProfiles,
          };
        }
        await this.saveState(state);
        this.logger.log?.('Migrated user profiles from state.json into MySQL storage.');
        return normalizeStateStore(state);
      }
    }
    await writeJsonIfChanged(this.statePath, normalizeStateStore(state)).catch(() => {});
    return normalizeStateStore(state);
  }

  async saveState(state) {
    const normalized = normalizeStateStore(state);
    const connection = await this.pool.getConnection();
    try {
      await connection.beginTransaction();
      await connection.query('DELETE FROM guild_memories');
      await connection.query('DELETE FROM reminders');
      await connection.query('DELETE FROM user_profiles');
      for (const [guildId, guildState] of Object.entries(normalized.guilds || {})) {
        for (const memory of guildState.memories || []) {
          await this.insertMemory(connection, guildId, 'guild', null, memory);
        }
        for (const [userId, memories] of Object.entries(guildState.userMemories || {})) {
          if (!Array.isArray(memories)) continue;
          for (const memory of memories) {
            await this.insertMemory(connection, guildId, 'user', userId, memory);
          }
        }
        for (const reminder of guildState.reminders || []) {
          await this.insertReminder(connection, guildId, reminder);
        }
        for (const [userId, profile] of Object.entries(guildState.userProfiles || {})) {
          if (!profile || typeof profile !== 'object') continue;
          await this.insertUserProfile(connection, guildId, userId, profile);
        }
      }
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
    await super.saveState(normalized).catch(() => {});
  }

  async insertMemory(connection, guildId, scope, userId, memory) {
    const id = String(memory.id || `${scope}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
    await connection.execute(
      `INSERT INTO guild_memories (id, guild_id, scope, user_id, user_name, text, created_at_ms, raw_json)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        id,
        String(guildId || 'global'),
        scope,
        userId || memory.userId || null,
        memory.userName || null,
        String(memory.text || ''),
        Number(memory.createdAt || Date.now()),
        JSON.stringify({ ...memory, id }),
      ],
    );
  }

  async insertReminder(connection, guildId, reminder) {
    const id = String(reminder.id || `rem-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
    await connection.execute(
      `INSERT INTO reminders
        (id, guild_id, channel_id, voice_channel_id, voice_channel_name, user_id, user_name, text, due_at_ms, repeat_interval_ms, repeat_label, raw_json)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        id,
        String(reminder.guildId || guildId || 'global'),
        reminder.channelId || null,
        reminder.voiceChannelId || null,
        reminder.voiceChannelName || null,
        reminder.userId || null,
        reminder.userName || null,
        String(reminder.text || ''),
        Number(reminder.dueAt || Date.now()),
        reminder.repeatIntervalMs === undefined || reminder.repeatIntervalMs === null ? null : Number(reminder.repeatIntervalMs),
        reminder.repeatLabel || null,
        JSON.stringify({ ...reminder, id }),
      ],
    );
  }

  async insertUserProfile(connection, guildId, userId, profile) {
    const id = String(profile.userId || userId || 'unknown');
    const payload = { ...profile, userId: id };
    await connection.execute(
      `INSERT INTO user_profiles
        (guild_id, user_id, user_name, preferred_name, timezone, updated_at_ms, raw_json)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        String(guildId || 'global'),
        id,
        profile.userName || null,
        profile.preferredName || null,
        profile.timezone || null,
        Number(profile.updatedAt || profile.createdAt || Date.now()),
        JSON.stringify(payload),
      ],
    );
  }

  async loadRuntimeConfig(fallback = {}) {
    const [rows] = await this.pool.execute('SELECT value_json FROM runtime_config WHERE config_key = ?', ['runtime']);
    if (rows[0]?.value_json) {
      const parsed = parseJson(rows[0].value_json, fallback);
      await writeJsonIfChanged(this.runtimeConfigPath, parsed || {}).catch(() => {});
      return parsed;
    }
    const jsonConfig = await super.loadRuntimeConfig(fallback);
    if (jsonConfig && Object.keys(jsonConfig).length) await this.saveRuntimeConfig(jsonConfig);
    return jsonConfig || fallback;
  }

  async saveRuntimeConfig(config) {
    const payload = JSON.stringify(config || {});
    await this.pool.execute(
      `INSERT INTO runtime_config (config_key, value_json)
       VALUES (?, ?)
       ON DUPLICATE KEY UPDATE value_json = VALUES(value_json), updated_at = CURRENT_TIMESTAMP(3)`,
      ['runtime', payload],
    );
    await super.saveRuntimeConfig(config || {}).catch(() => {});
  }

  async appendEvent(row) {
    const safe = {
      ts: row.ts || new Date().toISOString(),
      type: String(row.type || 'event').slice(0, 120),
      payload: safeEventValue(row.payload || {}),
    };
    await this.pool.execute(
      'INSERT INTO event_logs (ts, type, payload_json) VALUES (?, ?, ?)',
      [safe.ts, safe.type, JSON.stringify(safe.payload)],
    );
    await super.appendEvent(safe).catch(() => {});
  }

  async readEvents(limit = 120) {
    const count = Math.max(1, Math.min(500, Number(limit) || 120));
    const [rows] = await this.pool.query(
      `SELECT ts, type, payload_json FROM event_logs ORDER BY id DESC LIMIT ${count}`,
    );
    if (!rows.length && this.migrateFromJson) return super.readEvents(limit);
    return rows.map((row) => ({
      ts: row.ts,
      type: row.type,
      payload: parseJson(row.payload_json, {}),
    }));
  }

  async replaceEvents(events = []) {
    const rows = Array.isArray(events) ? [...events].reverse() : [];
    const connection = await this.pool.getConnection();
    try {
      await connection.beginTransaction();
      await connection.query('DELETE FROM event_logs');
      for (const row of rows) {
        const safe = {
          ts: row?.ts || new Date().toISOString(),
          type: String(row?.type || 'event').slice(0, 120),
          payload: safeEventValue(row?.payload || {}),
        };
        await connection.execute(
          'INSERT INTO event_logs (ts, type, payload_json) VALUES (?, ?, ?)',
          [safe.ts, safe.type, JSON.stringify(safe.payload)],
        );
      }
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
    await super.replaceEvents(rows).catch(() => {});
  }

  async exportAutonomyData() {
    const [journalRows] = await this.pool.query('SELECT * FROM conversation_journal ORDER BY created_at_ms ASC, id ASC');
    const [factRows] = await this.pool.query('SELECT * FROM memory_facts ORDER BY created_at_ms ASC, id ASC');
    const [reflectionRows] = await this.pool.query('SELECT * FROM assistant_reflections ORDER BY created_at_ms ASC, id ASC');
    return normalizeAutonomyStore({
      conversationJournal: journalRows.map((row) => parseJson(row.raw_json, null) || {
        id: row.id,
        guildId: row.guild_id,
        guildName: row.guild_name,
        voiceChannelId: row.voice_channel_id,
        voiceChannelName: row.voice_channel_name,
        userId: row.user_id,
        userName: row.user_name,
        transcript: row.transcript,
        prompt: row.prompt,
        wake: Boolean(row.wake),
        wakeListen: Boolean(row.wake_listen),
        usedForAnswer: Boolean(row.used_for_answer),
        source: row.source,
        createdAt: Number(row.created_at_ms),
        processedAt: row.processed_at_ms === null ? null : Number(row.processed_at_ms),
      }),
      memoryFacts: factRows.map((row) => parseJson(row.raw_json, null) || {
        id: row.id,
        guildId: row.guild_id,
        userId: row.user_id,
        userName: row.user_name,
        kind: row.kind,
        text: row.text,
        confidence: Number(row.confidence),
        createdAt: Number(row.created_at_ms),
        updatedAt: Number(row.updated_at_ms),
      }),
      assistantReflections: reflectionRows.map((row) => parseJson(row.raw_json, null) || {
        id: row.id,
        guildId: row.guild_id,
        guildName: row.guild_name,
        voiceChannelId: row.voice_channel_id,
        voiceChannelName: row.voice_channel_name,
        text: row.text,
        spoken: Boolean(row.spoken),
        sent: Boolean(row.sent),
        reason: row.reason,
        createdAt: Number(row.created_at_ms),
      }),
    });
  }

  async replaceAutonomyData(value = {}) {
    const store = normalizeAutonomyStore(value);
    const connection = await this.pool.getConnection();
    try {
      await connection.beginTransaction();
      await connection.query('DELETE FROM conversation_journal');
      await connection.query('DELETE FROM memory_facts');
      await connection.query('DELETE FROM assistant_reflections');
      for (const row of store.conversationJournal) await this.insertConversationJournal(connection, row);
      for (const row of store.memoryFacts) await this.insertMemoryFact(connection, row);
      for (const row of store.assistantReflections) await this.insertAssistantReflection(connection, row);
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
    await super.replaceAutonomyData(store).catch(() => {});
  }

  async insertConversationJournal(connection, row) {
    const item = normalizeJournalRow(row);
    await connection.execute(
      `INSERT INTO conversation_journal
        (id, guild_id, guild_name, voice_channel_id, voice_channel_name, user_id, user_name, transcript, prompt, wake, wake_listen, used_for_answer, source, created_at_ms, processed_at_ms, raw_json)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE processed_at_ms = VALUES(processed_at_ms), raw_json = VALUES(raw_json)`,
      [
        item.id,
        item.guildId,
        item.guildName || null,
        item.voiceChannelId || null,
        item.voiceChannelName || null,
        item.userId || null,
        item.userName || null,
        item.transcript,
        item.prompt || null,
        item.wake ? 1 : 0,
        item.wakeListen ? 1 : 0,
        item.usedForAnswer ? 1 : 0,
        item.source || 'voice',
        item.createdAt,
        item.processedAt,
        JSON.stringify(item),
      ],
    );
    return item;
  }

  async appendConversationJournal(row) {
    const connection = await this.pool.getConnection();
    try {
      const item = await this.insertConversationJournal(connection, row);
      await super.appendConversationJournal(item).catch(() => {});
      return item;
    } finally {
      connection.release();
    }
  }

  async listConversationJournal(options = {}) {
    const limit = autonomyLimit(options.limit, 250);
    const where = [];
    const params = [];
    if (options.guildId) {
      where.push('guild_id = ?');
      params.push(String(options.guildId));
    }
    if (options.sinceMs) {
      where.push('created_at_ms >= ?');
      params.push(Number(options.sinceMs));
    }
    if (options.processed !== undefined) {
      where.push(options.processed ? 'processed_at_ms IS NOT NULL' : 'processed_at_ms IS NULL');
    }
    const [rows] = await this.pool.query(
      `SELECT raw_json FROM conversation_journal${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY created_at_ms DESC LIMIT ${limit}`,
      params,
    );
    return rows
      .map((row) => normalizeJournalRow(parseJson(row.raw_json, {})))
      .reverse();
  }

  async listConversationJournalGuildIds(options = {}) {
    const limit = autonomyLimit(options.limit, 250);
    const where = [];
    if (options.processed !== undefined) {
      where.push(options.processed ? 'processed_at_ms IS NOT NULL' : 'processed_at_ms IS NULL');
    }
    const [rows] = await this.pool.query(
      `SELECT DISTINCT guild_id FROM conversation_journal${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY guild_id LIMIT ${limit}`,
    );
    return rows.map((row) => String(row.guild_id || '').trim()).filter(Boolean);
  }

  async markConversationJournalProcessed(ids = [], processedAt = Date.now()) {
    const list = (Array.isArray(ids) ? ids : []).map(String).filter(Boolean);
    if (!list.length) return 0;
    const placeholders = list.map(() => '?').join(',');
    const [result] = await this.pool.query(
      `UPDATE conversation_journal SET processed_at_ms = ? WHERE id IN (${placeholders})`,
      [processedAt, ...list],
    );
    await super.markConversationJournalProcessed(list, processedAt).catch(() => {});
    return Number(result.affectedRows || 0);
  }

  async insertMemoryFact(connection, row) {
    const item = normalizeMemoryFactRow(row);
    await connection.execute(
      `INSERT INTO memory_facts
        (id, guild_id, user_id, user_name, kind, text, confidence, created_at_ms, updated_at_ms, raw_json)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         user_id = VALUES(user_id),
         user_name = VALUES(user_name),
         kind = VALUES(kind),
         text = VALUES(text),
         confidence = VALUES(confidence),
         updated_at_ms = VALUES(updated_at_ms),
         raw_json = VALUES(raw_json)`,
      [
        item.id,
        item.guildId,
        item.userId || null,
        item.userName || null,
        item.kind || 'general',
        item.text,
        item.confidence,
        item.createdAt,
        item.updatedAt,
        JSON.stringify(item),
      ],
    );
    return item;
  }

  async upsertMemoryFact(row) {
    const connection = await this.pool.getConnection();
    try {
      const item = await this.insertMemoryFact(connection, row);
      await super.upsertMemoryFact(item).catch(() => {});
      return item;
    } finally {
      connection.release();
    }
  }

  async listMemoryFacts(options = {}) {
    const limit = autonomyLimit(options.limit, 250);
    const where = [];
    const params = [];
    if (options.guildId) {
      where.push('guild_id = ?');
      params.push(String(options.guildId));
    }
    if (options.userId) {
      where.push('user_id = ?');
      params.push(String(options.userId));
    }
    const [rows] = await this.pool.query(
      `SELECT raw_json FROM memory_facts${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY updated_at_ms DESC LIMIT ${limit}`,
      params,
    );
    return rows.map((row) => normalizeMemoryFactRow(parseJson(row.raw_json, {})));
  }

  async insertAssistantReflection(connection, row) {
    const item = normalizeReflectionRow(row);
    await connection.execute(
      `INSERT INTO assistant_reflections
        (id, guild_id, guild_name, voice_channel_id, voice_channel_name, text, spoken, sent, reason, created_at_ms, raw_json)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE raw_json = VALUES(raw_json)`,
      [
        item.id,
        item.guildId,
        item.guildName || null,
        item.voiceChannelId || null,
        item.voiceChannelName || null,
        item.text,
        item.spoken ? 1 : 0,
        item.sent ? 1 : 0,
        item.reason || null,
        item.createdAt,
        JSON.stringify(item),
      ],
    );
    return item;
  }

  async appendAssistantReflection(row) {
    const connection = await this.pool.getConnection();
    try {
      const item = await this.insertAssistantReflection(connection, row);
      await super.appendAssistantReflection(item).catch(() => {});
      return item;
    } finally {
      connection.release();
    }
  }

  async listAssistantReflections(options = {}) {
    const limit = autonomyLimit(options.limit, 250);
    const where = [];
    const params = [];
    if (options.guildId) {
      where.push('guild_id = ?');
      params.push(String(options.guildId));
    }
    const [rows] = await this.pool.query(
      `SELECT raw_json FROM assistant_reflections${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY created_at_ms DESC LIMIT ${limit}`,
      params,
    );
    return rows.map((row) => normalizeReflectionRow(parseJson(row.raw_json, {})));
  }

  async autonomyStats() {
    const [[journal], [unprocessed], [facts], [reflections]] = await Promise.all([
      this.pool.query('SELECT COUNT(*) AS count FROM conversation_journal'),
      this.pool.query('SELECT COUNT(*) AS count FROM conversation_journal WHERE processed_at_ms IS NULL'),
      this.pool.query('SELECT COUNT(*) AS count FROM memory_facts'),
      this.pool.query('SELECT COUNT(*) AS count FROM assistant_reflections'),
    ]);
    return {
      journal: Number(journal[0]?.count || 0),
      unprocessedJournal: Number(unprocessed[0]?.count || 0),
      facts: Number(facts[0]?.count || 0),
      reflections: Number(reflections[0]?.count || 0),
    };
  }
}

export async function createStorage(options = {}) {
  const driver = String(process.env.STORAGE_DRIVER || 'json').trim().toLowerCase();
  if (driver === 'mysql' || driver === 'mariadb') {
    try {
      return await new MySqlStorage(options).init();
    } catch (error) {
      if (String(process.env.STORAGE_FALLBACK_JSON || 'true') === 'false') throw error;
      options.logger?.error?.('MySQL storage failed, falling back to JSON:', error.message || error);
    }
  }
  return await new JsonStorage(options).init();
}
