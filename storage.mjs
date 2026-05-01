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

class JsonStorage {
  constructor({ dataDir, logger = console }) {
    this.driver = 'json';
    this.connected = true;
    this.logger = logger;
    this.dataDir = dataDir;
    this.statePath = path.join(dataDir, 'state.json');
    this.runtimeConfigPath = path.join(dataDir, 'runtime-config.json');
    this.eventLogPath = path.join(dataDir, 'events.jsonl');
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
    await this.saveState(state);
    if (runtimeConfig) await this.saveRuntimeConfig(runtimeConfig);
    if (events) await this.replaceEvents(events);
    return { stateRestored: true, runtimeRestored: Boolean(runtimeConfig), eventsRestored: events?.length || 0 };
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
