import { execFile } from 'node:child_process';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
const SAFE_BACKUP_RE = /^state-\d{4}-\d{2}-\d{2}T.*\.json$/u;

function safeBackupFiles(files) {
  return [...new Set((files || []).map((file) => path.basename(String(file || ''))))]
    .filter((file) => SAFE_BACKUP_RE.test(file))
    .sort()
    .reverse();
}

function decodePathPart(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function normalizeRetention(value) {
  return Math.max(1, Math.min(20, Number(value) || 2));
}

function stripTrailingSlash(value) {
  return String(value || '').replace(/[\\/]+$/u, '');
}

function ensureUrlDirectory(url) {
  const next = new URL(url.href);
  if (!next.pathname.endsWith('/')) next.pathname = `${next.pathname}/`;
  return next;
}

function stripUrlCredentials(value) {
  const url = new URL(value);
  url.username = '';
  url.password = '';
  return url.href;
}

function decodeCredential(value) {
  return decodePathPart(String(value || ''));
}

export function splitBackupTargetCredentials(value) {
  const targetPath = normalizeBackupTargetPath(value);
  if (!targetPath) return { targetPath: '', username: '', password: '' };
  try {
    const url = new URL(targetPath);
    const username = decodeCredential(url.username || '');
    const password = decodeCredential(url.password || '');
    url.username = '';
    url.password = '';
    return {
      targetPath: normalizeBackupTargetPath(url.href.replace(/%20/gu, ' ')),
      username,
      password,
    };
  } catch {
    return { targetPath, username: '', password: '' };
  }
}

export function applyBackupTargetCredentials(targetPath, username = '', password = '') {
  const target = normalizeBackupTargetPath(targetPath);
  if (!target) return '';
  try {
    const url = new URL(target);
    if (!['ftp:', 'smb:'].includes(url.protocol)) return target;
    const user = String(username || '').trim();
    if (user) {
      url.username = user;
      url.password = String(password || '');
    }
    return url.href;
  } catch {
    return target;
  }
}

function appendUrlFile(url, file) {
  const next = ensureUrlDirectory(url);
  next.pathname = `${next.pathname}${encodeURIComponent(file)}`;
  return next;
}

function executableError(name, error) {
  if (error?.code === 'ENOENT') {
    return new Error(`${name} is not installed in the container`);
  }
  return error;
}

async function runCommand(command, args, options = {}) {
  try {
    return await execFileAsync(command, args, {
      timeout: 120_000,
      maxBuffer: 5 * 1024 * 1024,
      ...options,
    });
  } catch (error) {
    throw executableError(command, error);
  }
}

async function pruneLocalBackups(dir, retention) {
  const files = safeBackupFiles(await fs.readdir(dir).catch(() => []));
  const stale = files.slice(normalizeRetention(retention));
  for (const file of stale) {
    await fs.unlink(path.join(dir, file)).catch(() => {});
  }
  return stale;
}

async function syncLocalBackup(localPath, targetPath, retention) {
  const targetDir = targetPath.startsWith('file://')
    ? decodePathPart(new URL(targetPath).pathname)
    : path.resolve(targetPath);
  await fs.mkdir(targetDir, { recursive: true });
  const targetFile = path.join(targetDir, path.basename(localPath));
  if (path.resolve(localPath) !== path.resolve(targetFile)) {
    await fs.copyFile(localPath, targetFile);
  }
  const pruned = await pruneLocalBackups(targetDir, retention);
  return {
    ok: true,
    type: 'local',
    target: targetDir,
    file: targetFile,
    pruned,
  };
}

async function syncFtpBackup(localPath, targetPath, retention, logger = console) {
  const targetUrl = ensureUrlDirectory(new URL(targetPath));
  const file = path.basename(localPath);
  const uploadUrl = appendUrlFile(targetUrl, file);

  await runCommand('curl', [
    '--silent',
    '--show-error',
    '--fail',
    '--ftp-create-dirs',
    '--upload-file',
    localPath,
    uploadUrl.href,
  ]);

  let pruned = [];
  try {
    const listed = await runCommand('curl', [
      '--silent',
      '--show-error',
      '--fail',
      '--list-only',
      targetUrl.href,
    ]);
    const files = safeBackupFiles(listed.stdout.split(/\r?\n/u).map((line) => line.trim()));
    pruned = files.slice(normalizeRetention(retention));
    if (pruned.length) {
      const rootUrl = new URL(targetUrl.href);
      const dirPath = decodePathPart(targetUrl.pathname).replace(/\/+$/u, '');
      rootUrl.pathname = '/';
      for (const stale of pruned) {
        await runCommand('curl', [
          '--silent',
          '--show-error',
          '--fail',
          '-Q',
          `DELE ${path.posix.join(dirPath, stale)}`,
          rootUrl.href,
        ]).catch((error) => logger.warn?.(`FTP backup prune failed for ${stale}: ${error.message || error}`));
      }
    }
  } catch (error) {
    logger.warn?.(`FTP backup prune skipped: ${error.message || error}`);
  }

  return {
    ok: true,
    type: 'ftp',
    target: stripUrlCredentials(targetUrl.href),
    file: stripUrlCredentials(uploadUrl.href),
    pruned,
  };
}

function parseSmbTarget(targetPath) {
  const url = new URL(targetPath);
  const parts = url.pathname.split('/').filter(Boolean).map(decodePathPart);
  const share = parts.shift();
  if (!url.hostname || !share) throw new Error('SMB path must look like smb://host/share/folder');
  return {
    service: `//${url.hostname}/${share}`,
    port: url.port || '',
    username: decodePathPart(url.username || ''),
    password: decodePathPart(url.password || ''),
    dirParts: parts,
    displayTarget: `smb://${url.host}/${[share, ...parts].map(encodeURIComponent).join('/')}`,
  };
}

function smbQuote(value) {
  return String(value || '').replace(/"/gu, '\\"');
}

function smbAuthArgs(target) {
  const args = [];
  if (target.username) {
    args.push('-U', `${target.username}%${target.password}`);
  } else {
    args.push('-N');
  }
  if (target.port) args.push('-p', target.port);
  return args;
}

function smbCdCommands(dirParts) {
  const commands = [];
  for (const part of dirParts) {
    commands.push(`mkdir "${smbQuote(part)}"`);
    commands.push(`cd "${smbQuote(part)}"`);
  }
  return commands;
}

async function runSmb(target, commands) {
  return await runCommand('smbclient', [
    target.service,
    ...smbAuthArgs(target),
    '-g',
    '-m',
    'SMB3',
    '-c',
    commands.join('; '),
  ]);
}

function parseSmbFiles(output) {
  const files = [];
  for (const rawLine of String(output || '').split(/\r?\n/u)) {
    const line = rawLine.trim();
    if (!line) continue;
    const grepable = line.match(/^(?:file|FILE)\|([^|]+)\|/u);
    if (grepable?.[1]) {
      files.push(grepable[1].trim());
      continue;
    }
    const classic = line.match(/^(.+?)\s+[A-Z]+\s+\d+\s+/u);
    if (classic?.[1]) files.push(classic[1].trim());
  }
  return safeBackupFiles(files.filter((file) => file !== '.' && file !== '..'));
}

async function syncSmbBackup(localPath, targetPath, retention, logger = console) {
  const target = parseSmbTarget(targetPath);
  const file = path.basename(localPath);
  const setup = smbCdCommands(target.dirParts);

  await runSmb(target, [
    ...setup,
    `put "${smbQuote(localPath)}" "${smbQuote(file)}"`,
  ]);

  let pruned = [];
  try {
    const listed = await runSmb(target, [...setup, 'ls']);
    const files = parseSmbFiles(listed.stdout);
    pruned = files.slice(normalizeRetention(retention));
    for (const stale of pruned) {
      await runSmb(target, [
        ...setup,
        `del "${smbQuote(stale)}"`,
      ]).catch((error) => logger.warn?.(`SMB backup prune failed for ${stale}: ${error.message || error}`));
    }
  } catch (error) {
    logger.warn?.(`SMB backup prune skipped: ${error.message || error}`);
  }

  return {
    ok: true,
    type: 'smb',
    target: target.displayTarget,
    file: `${target.displayTarget.replace(/\/$/u, '')}/${encodeURIComponent(file)}`,
    pruned,
  };
}

export function normalizeBackupTargetPath(value) {
  return stripTrailingSlash(String(value || '').replace(/\s+/gu, ' ').trim());
}

export function maskBackupTarget(value) {
  const raw = normalizeBackupTargetPath(value);
  if (!raw) return '';
  try {
    const url = new URL(raw);
    if (url.password) url.password = '***';
    if (url.username) url.username = `${url.username.slice(0, 2)}***`;
    return url.href.replace(/%20/gu, ' ');
  } catch {
    return raw;
  }
}

export async function syncBackupToTarget({
  localPath,
  targetPath,
  username = '',
  password = '',
  retention = 2,
  logger = console,
} = {}) {
  if (!localPath) throw new Error('localPath is required');
  const target = applyBackupTargetCredentials(targetPath, username, password);
  if (!target) return null;

  const protocol = (() => {
    try {
      return new URL(target).protocol;
    } catch {
      return '';
    }
  })();

  if (protocol === 'ftp:') return await syncFtpBackup(localPath, target, retention, logger);
  if (protocol === 'smb:') return await syncSmbBackup(localPath, target, retention, logger);
  if (protocol === 'file:' || !protocol) return await syncLocalBackup(localPath, target, retention);
  throw new Error(`Unsupported backup target protocol: ${protocol}`);
}
