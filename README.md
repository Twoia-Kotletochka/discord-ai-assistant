# Discord AI assistant

Голосовой Discord-ассистент на Node.js. Бот заходит в voice-канал, слушает речь, распознает ее через Groq Whisper, отвечает через Groq Chat/Compound и озвучивает ответ обратно в Discord.

Проект рассчитан на запуск в Docker: бот и веб-панель работают в отдельных контейнерах, а память/напоминания хранятся в Docker volume.

## Возможности

- Голосовой диалог в Discord voice channel.
- Настраиваемое имя ассистента и trigger word: например `бот`, `железяка`, `алиса`.
- Fuzzy wake word: бот умеет реагировать на похожие слова, если STT распознал триггер неточно.
- Русская, английская и смешанная речь.
- STT через Groq Whisper.
- Chat через Groq модели.
- Web search через Groq Compound для актуальных запросов.
- TTS через Microsoft Edge Neural voices (`edge-tts`) внутри Docker.
- Веб-панель с настройками ключей, моделей, голоса, памяти, логов и состояния Docker.
- Локальная память и персональная память пользователей.
- Напоминания, включая повторяющиеся.
- Голосовые Discord-действия: mute, move, disconnect, роли, каналы, slowmode, очистка сообщений и другие команды.
- Подтверждение опасных действий через `бот да` / `бот нет`.
- Idle-фразы после тишины в voice-канале.
- Авто-уход из voice после долгого игнора с короткой фразой перед выходом.
- Приветствия при входе/выходе пользователей из voice-канала.
- Healthcheck и auto recovery.

## Важно про безопасность

Никогда не публикуйте `.env`, `data/`, логи, токены Discord, Groq API keys, пароли панели и локальные backup-файлы.

В репозитории уже настроены `.gitignore` и `.dockerignore`, но все равно проверьте это перед публикацией:

```bash
git status --short
```

Если вы когда-либо отправляли Discord token или Groq API key в чат, считайте ключ скомпрометированным:

- Discord token: откройте Discord Developer Portal и нажмите **Reset Token**.
- Groq key: удалите старый ключ в Groq Console и создайте новый.

## Требования

- Docker Engine.
- Docker Compose plugin.
- Discord bot token.
- Groq API key.
- Сервер или ПК с нормальным доступом к Discord voice UDP.

Проверка Docker:

```bash
docker --version
docker compose version
```

## 1. Создание Discord-бота

1. Откройте [Discord Developer Portal](https://discord.com/developers/applications).
2. Нажмите **New Application**.
3. Задайте имя приложения.
4. Откройте раздел **Bot**.
5. Нажмите **Reset Token** или **Copy Token**.
6. Сохраните токен в `.env` как `DISCORD_TOKEN`.
7. Для базового voice-режима privileged intents не нужны: бот использует `Guilds` и `Guild Voice States`. Поиск людей надежнее всего работает по участникам текущего voice-канала.
8. Откройте **OAuth2 -> URL Generator**.
9. В `Scopes` выберите:
   - `bot`
   - `applications.commands`
10. В `Bot Permissions` выберите минимум для подключения к voice и ответов в текстовый канал:
   - `View Channels`
   - `Connect`
   - `Speak`
   - `Use Voice Activity`
   - `Send Messages`
   - `Read Message History`
11. Для голосовых админ-команд добавьте нужные права:
   - `Move Members`
   - `Mute Members`
   - `Deafen Members`
   - `Manage Channels`
   - `Manage Roles`
   - `Manage Messages`
   - `Kick Members`
   - `Ban Members`
   - `Moderate Members`
12. Откройте сгенерированную OAuth2-ссылку и пригласите бота на сервер.

Для тестов проще выдать боту `Administrator`, но для постоянного сервера лучше дать только реально нужные права.

## 2. Получение Groq API key

1. Откройте [Groq Console](https://console.groq.com/keys).
2. Создайте API key.
3. Сохраните его в `.env` как `GROQ_API_KEY`.

## 3. Установка проекта

```bash
git clone https://github.com/Twoia-Kotletochka/discord-ai-assistant.git
cd discord-ai-assistant
cp .env.example .env
```

Откройте `.env`:

```bash
nano .env
```

Минимально заполните:

```bash
DISCORD_TOKEN=ваш_discord_bot_token
GROQ_API_KEY=ваш_groq_api_key
DISCORD_GUILD_ID=id_вашего_discord_сервера
PANEL_PASSWORD=сложный_пароль_для_панели
```

`DISCORD_GUILD_ID` не обязателен, но с ним slash-команды регистрируются быстрее. Чтобы получить ID сервера, включите Developer Mode в Discord, нажмите правой кнопкой по серверу и выберите **Copy Server ID**.

## 4. Основные настройки `.env`

```bash
ASSISTANT_NAME=Бот
BOT_WAKE_WORD=бот
BOT_WAKE_ALIASES=вот,от,робот,роботик,ботик,бота,боту,боте,боты,ботом,бод,бат,борт,вод,бо,ботт
BOT_WAKE_FUZZY=true
LISTEN_WITHOUT_WAKE_WORD=false
```

Если хотите другое имя:

```bash
ASSISTANT_NAME=Железяка
BOT_WAKE_WORD=железяка
BOT_WAKE_ALIASES=железка,железяко,железяку,железяке,железякой
```

Эти параметры также можно менять в веб-панели без пересборки контейнеров.

Авто-уход, если к боту долго не обращались:

```bash
IDLE_LEAVE_ENABLED=true
IDLE_LEAVE_MINUTES=60
IDLE_LEAVE_PHRASE=
```

`IDLE_LEAVE_PHRASE` можно оставить пустым, тогда бот выберет случайную обиженную фразу. Таймер сбрасывается, когда пользователь реально обращается к боту голосом или slash-командой; обычный разговор людей в voice-канале таймер не сбрасывает.

Автоподключение после перезапуска контейнера:

```bash
AUTO_JOIN_ENABLED=false
AUTO_JOIN_GUILD_ID=
AUTO_JOIN_VOICE_CHANNEL_ID=
AUTO_JOIN_TEXT_CHANNEL_ID=
```

По умолчанию автоподключение выключено. Чтобы бот сам заходил после старта, нужно явно поставить `AUTO_JOIN_ENABLED=true` и заполнить все три ID. Если `AUTO_JOIN_ENABLED=false`, подключение выполняется только вручную через `/join`, даже если ID каналов случайно заполнены.

Модели по умолчанию:

```bash
GROQ_CHAT_MODEL=llama-3.1-8b-instant
GROQ_STT_MODEL=whisper-large-v3-turbo
WEB_SEARCH_MODEL=groq/compound-mini
```

Голос по умолчанию:

```bash
TTS_PROVIDER=edge
EDGE_TTS_VOICE=ru-RU-SvetlanaNeural
EDGE_TTS_ENGLISH_VOICE=en-US-AvaMultilingualNeural
EDGE_TTS_RATE=+0%
EDGE_TTS_PITCH=+0Hz
```

## 5. Запуск в Docker

```bash
docker compose up -d --build
```

Проверить контейнеры:

```bash
docker compose ps
```

Смотреть логи:

```bash
docker compose logs -f bot panel
```

Остановить:

```bash
docker compose down
```

Обновить после `git pull`:

```bash
git pull
docker compose up -d --build
docker compose logs -f bot panel
```

## 6. Веб-панель

По умолчанию панель слушает:

```text
http://127.0.0.1:8787
```

Если запускаете на удаленном VPS, безопасный вариант - SSH tunnel:

```bash
ssh -L 8787:127.0.0.1:8787 user@server_ip
```

После этого откройте на своем ПК:

```text
http://127.0.0.1:8787
```

Если нужно открыть панель в локальной сети напрямую, задайте в `.env`:

```bash
PANEL_BIND_HOST=0.0.0.0
PANEL_PORT=8787
PANEL_PASSWORD=сложный_пароль
```

Потом откройте:

```text
http://SERVER_IP:8787
```

`0.0.0.0` открывает панель на всех сетевых интерфейсах сервера. Не оставляйте панель без пароля, если она доступна не только с localhost.

В панели есть вкладки:

- **Обзор** - состояние бота и voice-сессии.
- **Управление** - имя ассистента, trigger word, интернет-поиск, idle-фразы, авто-уход без обращений, подтверждения действий, healthcheck.
- **Ключи и модели** - Groq key, Discord token, guild ID, chat/STT/web модели.
- **Голос** - выбор TTS provider, Edge voices, скорость, тон, preview голоса.
- **Память** - счетчики памяти/напоминаний, backup и restore `state.json`.
- **Журнал** - события бота и Docker logs.
- **Система** - RAM/load, Docker status, restart bot/panel, Groq limits.

Для Docker-кнопок панели используется Docker socket `/var/run/docker.sock`. Это удобно для локального Linux-сервера, но это высокий уровень доступа. Не выставляйте такую панель в интернет без VPN/reverse proxy/auth.

## 7. Подключение бота к voice

1. Запустите контейнеры.
2. Зайдите в Discord voice channel.
3. В текстовом канале сервера выполните:

```text
/join
```

Бот должен зайти в ваш voice channel.

Пример голосовой фразы:

```text
Бот, расскажи коротко что такое Groq.
```

После wake word бот некоторое время держит активный диалог и может отвечать без повторного `бот`, если включена настройка активного диалога.

Остановить текущую речь:

```text
Бот стоп
Бот хватит
Бот остановись
```

## 8. Slash-команды

- `/join` - подключить бота к вашему voice channel.
- `/leave` - отключить бота.
- `/ask` - текстовый вопрос модели.
- `/disconnect` - отключить выбранного участника от voice.
- `/actions` - показать примеры голосовых действий.
- `/limits` - показать последние Groq rate-limit headers.
- `/stop` - остановить текущую речь.
- `/reset` - сбросить историю текущего диалога.
- `/remember` - записать факт в локальную память.
- `/memories` - показать память.
- `/remind` - создать напоминание.
- `/reminders` - показать напоминания.
- `/pause` - поставить обработку голоса на паузу.
- `/resume` - снять паузу.
- `/status` - показать состояние voice-сессии.

## 9. Примеры голосовых команд

Обычный разговор:

```text
Бот, что ты умеешь?
Бот, explain this in English.
Бот, найди свежие новости про Groq.
Бот, какая сейчас погода в Киеве?
```

Память:

```text
Бот, запомни что наш сервер играет по вечерам.
Бот, запомни обо мне что я люблю короткие ответы.
Бот, что ты помнишь?
Бот, что ты помнишь обо мне?
Бот, забудь память.
```

Напоминания:

```text
Бот, напомни через 5 минут проверить чай.
Бот, напоминай каждые 2 часа размяться.
Бот, покажи напоминания.
Бот, удали напоминание про чай.
Бот, удали второе напоминание.
```

Discord-действия:

```text
Бот, отключи Иван от войса.
Бот, замуть Иван.
Бот, размуть Иван.
Бот, перемести Иван в Общий.
Бот, отключи всех от войса.
Бот, создай текстовый канал тест.
Бот, переименуй войс в Комната тестов.
Бот, очисти 20 сообщений.
```

Опасные действия требуют подтверждения:

```text
Бот, отключи всех от войса.
Бот, да.
```

Отмена:

```text
Бот, нет.
```

## 10. Где хранятся данные

В Docker данные лежат в volume `bot-data` и монтируются в контейнер как:

```text
/app/data
```

Главные файлы:

- `/app/data/runtime-config.json` - настройки из панели.
- `/app/data/state.json` - память и напоминания.
- `/app/data/status.json` - последний статус бота.
- `/app/data/events.jsonl` - журнал событий.
- `/app/data/backups/` - backup-файлы памяти.

Эти файлы не должны попадать в публичный git.

Создать backup памяти можно в панели во вкладке **Память**.

## 11. Диагностика

Бот не заходит в voice:

```bash
docker compose logs -f bot
```

Проверьте:

- бот приглашен на сервер;
- у бота есть `Connect`, `Speak`, `Use Voice Activity`;
- вы сами находитесь в voice channel перед `/join`;
- Discord token корректный.

Бот молчит:

- проверьте, что `LISTEN_WITHOUT_WAKE_WORD=false` и вы произносите wake word;
- проверьте вкладку **Журнал** в панели;
- проверьте Groq key;
- проверьте `GROQ_STT_MODEL`;
- попробуйте увеличить чувствительность через `MIN_RMS`;
- скажите фразу целиком: `Бот, скажи привет`.

Нет данных о лимитах:

- Groq rate-limit headers появляются только после запросов к Groq;
- нажмите в панели **Обновить лимиты**;
- задайте боту голосовой или текстовый вопрос.

Панель не открывается:

```bash
docker compose ps
docker compose logs -f panel
```

Проверьте `PANEL_BIND_HOST`, `PANEL_PORT`, firewall и SSH tunnel.

## 12. Структура проекта

```text
voice-bot.mjs       основной Discord voice bot
panel-server.mjs    backend веб-панели
panel/              статический frontend панели
Dockerfile          образ для bot/panel
docker-compose.yml  два контейнера: bot и panel
.env.example        шаблон настроек без секретов
```
