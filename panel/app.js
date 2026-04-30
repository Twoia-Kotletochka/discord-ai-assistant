const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

let state = null;
let events = [];

function markDirty(form) {
  if (form) form.dataset.dirty = 'true';
}

function markClean(form) {
  if (form) form.dataset.dirty = 'false';
}

function shouldHydrateForm(form, force = false) {
  if (!form) return false;
  if (force) return true;
  if (form.dataset.dirty === 'true') return false;
  return !form.contains(document.activeElement);
}

function fmtBytes(value) {
  if (!Number.isFinite(value)) return '-';
  const units = ['B', 'KB', 'MB', 'GB'];
  let size = value;
  let unit = 0;
  while (size > 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${size.toFixed(unit ? 1 : 0)} ${units[unit]}`;
}

function fmtUptime(sec) {
  if (!Number.isFinite(sec)) return '-';
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = Math.floor(sec % 60);
  return h ? `${h}ч ${m}м` : `${m}м ${s}с`;
}

function toast(text) {
  const box = $('#toast');
  box.textContent = text;
  box.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { box.hidden = true; }, 2800);
}

function esc(value) {
  return String(value ?? '').replace(/[&<>"']/gu, (char) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  })[char]);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
    credentials: 'same-origin',
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(data.error || `HTTP ${response.status}`);
    error.status = response.status;
    throw error;
  }
  return data;
}

function fillSelect(selector, values = [], selected = '') {
  const select = $(selector);
  const options = [...new Set([selected, ...values].filter(Boolean))];
  select.innerHTML = options.map((value) => `<option value="${esc(value)}">${esc(value)}</option>`).join('');
  select.value = selected || options[0] || '';
}

function edgeVoiceGroups(values = []) {
  const all = [...new Set(values.filter(Boolean))];
  const russian = all.filter((voice) => /^ru-RU-/u.test(voice));
  const english = all.filter((voice) => /^en-/u.test(voice) || /Multilingual/u.test(voice));
  return {
    all,
    russian: russian.length ? russian : all,
    english: english.length ? english : all,
  };
}

function normalizeWakeText(value) {
  return String(value || '')
    .toLowerCase()
    .replaceAll('ё', 'е')
    .replace(/[^\p{L}\p{N}\s_-]/gu, ' ')
    .replace(/\s+/gu, ' ')
    .trim();
}

function defaultAliasesForWake(value) {
  const wake = normalizeWakeText(value);
  if (wake === 'бот') return 'вот, от, робот, роботик, ботик, бота, боту, боте, боты, ботом, бод, бат, борт, вод, бо, ботт';
  if (wake === 'железяка') return 'железка, железяко, железяку, железяке, железякой, железяки, железякин';
  return '';
}

function setForm(form, values) {
  for (const [key, value] of Object.entries(values || {})) {
    const input = form.elements[key];
    if (!input) continue;
    if (input.type === 'checkbox') input.checked = Boolean(value);
    else input.value = value ?? '';
  }
}

function render(forceHydrateForms = false) {
  const bot = state.bot;
  const runtime = state.runtime || {};
  const panel = state.panel || {};
  const memory = bot?.memory || {};
  const session = bot?.sessions?.[0];

  $('#globalStatus').textContent = bot?.ok ? 'Bot online' : 'Bot offline';
  $('#globalStatus').className = `status-pill ${bot?.ok ? 'ok' : 'bad'}`;
  $('#botState').textContent = runtime.botEnabled === false ? 'Off' : (bot?.ok ? 'On' : 'No status');
  $('#voiceState').textContent = session?.voiceChannelName || 'Не подключен';
  $('#memoryState').textContent = `${memory.memories || 0} / ${memory.reminders || 0}`;
  $('#botPid').textContent = bot?.pid || '-';
  $('#botUptime').textContent = fmtUptime(bot?.uptimeSec);
  $('#botStarted').textContent = bot?.startedAt ? new Date(bot.startedAt).toLocaleString('ru-RU') : '-';
  $('#memoryCount').textContent = memory.memories ?? '-';
  $('#reminderCount').textContent = memory.reminders ?? '-';

  $('#botEnabled').checked = runtime.botEnabled !== false;
  $('#listeningPaused').checked = runtime.listeningPaused === true;

  if (shouldHydrateForm($('#modelsForm'), forceHydrateForms)) setForm($('#modelsForm'), runtime);
  if (shouldHydrateForm($('#voiceForm'), forceHydrateForms)) setForm($('#voiceForm'), runtime);
  if (shouldHydrateForm($('#featuresForm'), forceHydrateForms)) setForm($('#featuresForm'), runtime);
  if (shouldHydrateForm($('#secretsForm'), forceHydrateForms)) {
    setForm($('#secretsForm'), {
      discordGuildId: state.env?.discordGuildId || '',
    });
  }
  $('#secretsHint').textContent = `Discord token: ${state.env?.discordTokenSet ? state.env.discordTokenMasked : 'не задан'}, Groq key: ${state.env?.groqApiKeySet ? state.env.groqApiKeyMasked : 'не задан'}`;
  $('#modelHint').textContent = `${state.presets?.modelSource === 'groq' ? 'Список получен из Groq API' : 'Fallback список моделей'} · ${state.presets?.modelInfo?.length || 0} active`;
  const edgeGroups = edgeVoiceGroups(state.presets?.edgeVoices || []);
  const ruText = edgeGroups.russian.filter((voice) => /^ru-RU-/u.test(voice)).join(', ') || 'нет ru-RU голосов';
  $('#voiceHint').textContent = `Edge free endpoint сейчас отдаёт RU: ${ruText}. ru-RU-DariyaNeural есть в Azure Speech, но не доступен через бесплатный edge-tts на этом сервере.`;

  const sessions = bot?.sessions || [];
  $('#sessionsList').innerHTML = sessions.length
    ? sessions.map((item) => {
      const diag = item.diagnostics || {};
      const diagText = `members=${item.humanVoiceMembers || 0}/${item.voiceMembers || 0}, events=${diag.voiceEvents || 0}, captures=${diag.captures || 0}, ignored=${diag.ignored || 0}, last=${diag.lastIgnoredReason || 'none'}`;
      const timings = diag.lastTimingsMs ? ` · ${Object.entries(diag.lastTimingsMs).map(([key, value]) => `${key}:${value}ms`).join(' ')}` : '';
      const transcript = diag.lastTranscript ? ` · "${diag.lastTranscript}"` : '';
      const idleLeave = item.idleLeaveDueAt ? ` · уйдет ${new Date(item.idleLeaveDueAt).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })}` : '';
      return `
      <div class="row">
        <div><b>${esc(item.voiceChannelName || 'Voice не подключен')}</b><small>${esc(item.guildName || item.guildId)} · ${esc(item.connectionState)} · paused=${esc(item.paused)}${esc(idleLeave)}</small><small>${esc(diagText)}${esc(transcript)}${esc(timings)}</small></div>
        <div class="row-actions"><small>${esc(item.activeCaptures)} active</small></div>
      </div>
    `;
    }).join('')
    : '<p class="muted">Активных voice-сессий нет.</p>';

  $('#backupList').innerHTML = state.backups?.length
    ? state.backups.map((item) => `
      <div class="row">
        <div><b>${esc(item.file)}</b><small>${esc(fmtBytes(item.size))} · ${esc(new Date(item.createdAt).toLocaleString('ru-RU'))}</small></div>
        <div class="row-actions">
          <a href="/api/backups/download/${encodeURIComponent(item.file)}">Скачать</a>
          <button class="ghost" data-restore="${esc(item.file)}">Восстановить</button>
        </div>
      </div>
    `).join('')
    : '<p class="muted">Бэкапов пока нет.</p>';

  const host = panel.host || {};
  $('#loadAvg').textContent = host.loadavg ? host.loadavg.map((v) => v.toFixed(2)).join(' / ') : '-';
  $('#ramState').textContent = host.totalMem ? `${fmtBytes(host.totalMem - host.freeMem)} / ${fmtBytes(host.totalMem)}` : '-';
  $('#panelUptime').textContent = fmtUptime(panel.uptimeSec);

  const limits = { ...(panel.groqLimits || {}), ...(bot?.groqLimits || {}) };
  $('#limitsList').innerHTML = Object.keys(limits).length
    ? Object.entries(limits).sort(([a], [b]) => a.localeCompare(b)).map(([, item]) => {
      const pct = item.limit ? Math.round(item.remaining / item.limit * 100) : 0;
      return `<div class="row"><div><b>${esc(item.model || 'unknown')} · ${esc(item.name)}: ${esc(pct)}%</b><small>${esc(item.remaining)}/${esc(item.limit)} · reset ${esc(item.reset || 'unknown')} · ${esc(item.label || 'source unknown')}</small></div></div>`;
    }).join('')
    : '<p class="muted">Данных пока нет. Нажми "Обновить лимиты" или задай боту вопрос через голос/чат.</p>';

  const docker = panel.docker || {};
  $('#dockerList').innerHTML = docker.error
    ? `<p class="muted">Docker API недоступен: ${esc(docker.error)}</p>`
    : (docker.containers || []).length
      ? docker.containers.map((item) => `
        <div class="row">
          <div><b>${esc((item.names || [])[0]?.replace(/^\//u, '') || item.id?.slice(0, 12) || 'container')}</b><small>${esc(item.state)} · ${esc(item.status)}</small></div>
        </div>
      `).join('')
      : '<p class="muted">Контейнеры не найдены или Docker socket не подключен.</p>';

  $('#eventsList').innerHTML = events.length
    ? events.map((item) => {
      const payload = item.payload ? JSON.stringify(item.payload) : '';
      return `<div class="row"><div><b>${esc(item.type || 'event')}</b><small>${esc(item.ts ? new Date(item.ts).toLocaleString('ru-RU') : '')}</small><small>${esc(payload)}</small></div></div>`;
    }).join('')
    : '<p class="muted">Журнал пока пустой.</p>';

  $$('[data-restore]').forEach((button) => {
    button.addEventListener('click', async () => {
      if (!confirm(`Восстановить ${button.dataset.restore}?`)) return;
      await api('/api/backups/restore', { method: 'POST', body: { file: button.dataset.restore } });
      toast('Backup восстановлен. Рекомендуется перезапустить бота.');
      await loadStatus();
    });
  });
}

async function loadEvents() {
  const data = await api('/api/events?limit=120');
  events = data.events || [];
  if (state) render();
}

async function loadStatus({ forceHydrateForms = false } = {}) {
  try {
    state = await api('/api/status');
    document.body.classList.remove('locked');
    $('#tabs').hidden = false;
    if (shouldHydrateForm($('#modelsForm'), forceHydrateForms)) {
      fillSelect('#chatModelSelect', state.presets?.chat, state.runtime?.groqChatModel);
      fillSelect('#sttModelSelect', state.presets?.stt, state.runtime?.groqSttModel);
    }
    if (shouldHydrateForm($('#featuresForm'), forceHydrateForms)) {
      fillSelect('#webSearchModelSelect', state.presets?.web, state.runtime?.webSearchModel);
      const featuresForm = $('#featuresForm');
      featuresForm.dataset.lastWakeDefaultAliases = defaultAliasesForWake(state.runtime?.wakeWord);
    }
    if (shouldHydrateForm($('#voiceForm'), forceHydrateForms)) {
      const edgeGroups = edgeVoiceGroups(state.presets?.edgeVoices || []);
      fillSelect('#macosVoiceSelect', state.presets?.macosVoices, state.runtime?.macosVoice);
      fillSelect('#espeakVoiceSelect', state.presets?.espeakVoices, state.runtime?.espeakVoice);
      fillSelect('#edgeVoiceSelect', edgeGroups.russian, state.runtime?.edgeVoice);
      fillSelect('#edgeEnglishVoiceSelect', edgeGroups.english, state.runtime?.edgeEnglishVoice);
    }
    render(forceHydrateForms);
    await loadEvents().catch(() => {});
  } catch (error) {
    if (error.status === 401) {
      window.location.replace('/login.html');
      return;
    }
    toast(error.message);
  }
}

async function saveRuntime(patch) {
  await api('/api/runtime', { method: 'POST', body: patch });
  toast('Настройки применены');
  await loadStatus({ forceHydrateForms: true });
}

$$('.tab').forEach((button) => {
  button.addEventListener('click', () => {
    $$('.tab').forEach((item) => item.classList.toggle('active', item === button));
    $$('.tabpage').forEach((page) => page.classList.toggle('active', page.id === `tab-${button.dataset.tab}`));
  });
});

$('#botEnabled').addEventListener('change', (event) => saveRuntime({ botEnabled: event.target.checked }));
$('#listeningPaused').addEventListener('change', (event) => saveRuntime({ listeningPaused: event.target.checked }));

$('#modelsForm').addEventListener('submit', async (event) => {
  event.preventDefault();
  const data = Object.fromEntries(new FormData(event.currentTarget));
  markClean(event.currentTarget);
  await saveRuntime(data);
});

$('#voiceForm').addEventListener('submit', async (event) => {
  event.preventDefault();
  const data = Object.fromEntries(new FormData(event.currentTarget));
  data.espeakSpeed = Number(data.espeakSpeed);
  markClean(event.currentTarget);
  await saveRuntime(data);
});

$('#featuresForm').addEventListener('submit', async (event) => {
  event.preventDefault();
  const form = event.currentTarget;
  const data = {
    assistantName: form.elements.assistantName.value,
    wakeWord: form.elements.wakeWord.value,
    wakeAliases: form.elements.wakeAliases.value,
    wakeFuzzy: form.elements.wakeFuzzy.checked,
    webSearchEnabled: form.elements.webSearchEnabled.checked,
    webSearchModel: form.elements.webSearchModel.value,
    idleChatterEnabled: form.elements.idleChatterEnabled.checked,
    idleChatterMinutes: Number(form.elements.idleChatterMinutes.value),
    idleChatterUseWeb: form.elements.idleChatterUseWeb.checked,
    idleChatterStyle: form.elements.idleChatterStyle.value,
    idleLeaveEnabled: form.elements.idleLeaveEnabled.checked,
    idleLeaveMinutes: Number(form.elements.idleLeaveMinutes.value),
    idleLeavePhrase: form.elements.idleLeavePhrase.value,
    activeDialogueEnabled: form.elements.activeDialogueEnabled.checked,
    activeDialogueSeconds: Number(form.elements.activeDialogueSeconds.value),
    confirmDangerousActions: false,
    assistantPersona: form.elements.assistantPersona.value,
    healthcheckEnabled: form.elements.healthcheckEnabled.checked,
  };
  markClean(form);
  await saveRuntime(data);
});

$('#featuresForm').elements.wakeWord.addEventListener('change', (event) => {
  const form = $('#featuresForm');
  const aliases = form.elements.wakeAliases;
  const previousDefault = form.dataset.lastWakeDefaultAliases || '';
  if (!aliases.value.trim() || aliases.value.trim() === previousDefault) {
    aliases.value = defaultAliasesForWake(event.target.value);
  }
  form.dataset.lastWakeDefaultAliases = defaultAliasesForWake(event.target.value);
  markDirty(form);
});

$('#secretsForm').addEventListener('submit', async (event) => {
  event.preventDefault();
  const data = Object.fromEntries(new FormData(event.currentTarget));
  await api('/api/secrets', { method: 'POST', body: data });
  event.currentTarget.elements.groqApiKey.value = '';
  event.currentTarget.elements.discordToken.value = '';
  markClean(event.currentTarget);
  toast('Ключи сохранены. Discord token требует перезапуск бота.');
  await loadStatus({ forceHydrateForms: true });
});

$('#createBackup').addEventListener('click', async () => {
  await api('/api/backups/create', { method: 'POST' });
  toast('Backup создан');
  await loadStatus();
});

$('#refreshModels').addEventListener('click', async () => {
  await api('/api/models/refresh', { method: 'POST' });
  toast('Список моделей обновлен');
  await loadStatus({ forceHydrateForms: $('#modelsForm').dataset.dirty !== 'true' });
});

$('#probeLimits').addEventListener('click', async () => {
  await api('/api/limits/probe', { method: 'POST' });
  toast('Лимиты Groq обновлены');
  await loadStatus();
});

$('#previewVoice').addEventListener('click', async () => {
  const form = $('#voiceForm');
  const voice = form.elements.ttsProvider.value === 'edge' ? form.elements.edgeVoice.value : form.elements.edgeVoice.value;
  const response = await fetch('/api/voice/preview', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'same-origin',
    body: JSON.stringify({
      voice,
      rate: form.elements.edgeRate.value,
      pitch: form.elements.edgePitch.value,
      text: $('#previewText').value,
    }),
  });
  if (!response.ok) throw new Error((await response.json().catch(() => ({}))).error || `HTTP ${response.status}`);
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const audio = new Audio(url);
  audio.addEventListener('ended', () => URL.revokeObjectURL(url), { once: true });
  await audio.play();
});

$('#refreshEvents').addEventListener('click', async () => {
  await loadEvents();
  toast('Журнал обновлен');
});

$('#refreshDocker').addEventListener('click', loadStatus);

$('#loadDockerLogs').addEventListener('click', async () => {
  const target = $('#dockerLogTarget').value;
  const data = await api(`/api/docker/logs?target=${encodeURIComponent(target)}&tail=200`);
  $('#dockerLogs').textContent = data.logs || 'Лог пустой.';
});

async function restartContainer(target) {
  if (!confirm(`Перезапустить ${target}?`)) return;
  await api('/api/docker/restart', { method: 'POST', body: { target } });
  toast(`${target} перезапускается`);
  setTimeout(() => loadStatus().catch(() => {}), 2500);
}

$('#restartBot').addEventListener('click', () => restartContainer('bot'));
$('#restartPanel').addEventListener('click', () => restartContainer('panel'));

$$('[data-refresh]').forEach((button) => button.addEventListener('click', loadStatus));
['modelsForm', 'voiceForm', 'featuresForm', 'secretsForm'].forEach((id) => {
  const form = $(`#${id}`);
  form.addEventListener('input', () => markDirty(form));
  form.addEventListener('change', () => markDirty(form));
});
setInterval(loadStatus, 5000);
loadStatus();
