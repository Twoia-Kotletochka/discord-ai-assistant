# Discord AI assistant

Голосовой Discord-ассистент на Node.js. Бот заходит в voice-канал, слушает речь, распознает ее через Groq Whisper, отвечает через Groq Chat/Compound и озвучивает ответ обратно в Discord.

Проект рассчитан на запуск в Docker: бот, веб-панель и MariaDB работают в отдельных контейнерах, а память/напоминания хранятся в базе с JSON-зеркалом в Docker volume.

## Возможности

- Голосовой диалог в Discord voice channel.
- Настраиваемое имя ассистента и trigger word: например `бот`, `железяка`, `алиса`.
- Fuzzy wake word: бот умеет реагировать на похожие слова, если STT распознал триггер неточно.
- Русская, английская и смешанная речь.
- STT через Groq Whisper.
- Chat через Groq модели.
- Web search через Groq Compound для актуальных запросов.
- Локальный калькулятор для простых голосовых примеров без обращения к ИИ.
- Telegram bridge: отправка сообщений, заметок, памяти, напоминаний и результатов интернет-поиска из Discord voice в Telegram.
- TTS через Microsoft Edge Neural voices (`edge-tts`) внутри Docker.
- Веб-панель с настройками ключей, моделей, голоса, памяти, логов и состояния Docker.
- Локальная память и персональная память пользователей.
- Напоминания, включая повторяющиеся.
- MariaDB/MySQL storage layer для памяти, персональной памяти, напоминаний, runtime-настроек и журнала событий.
- Голосовые Discord-действия: mute, move, disconnect, роли, каналы, категории, треды, invite-ссылки, soundboard, slowmode, очистка сообщений и другие команды.
- Fuzzy-поиск пользователей по voice/server списку: `Досик`, `досика`, `Dosik`, `Dosikk` могут сопоставляться с одним участником.
- Voice-действия выполняются сразу после команды, без отдельного подтверждения.
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
- Git.
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
WAKE_ACK_AI_ENABLED=true
WAKE_ACK_FALLBACK_PHRASES=Слушаю,Говори,На связи,Да, я тут,Внимательно,Давай,Жду вопрос
ACTIVE_DIALOGUE_ENABLED=false
ACTIVE_DIALOGUE_SECONDS=45
WAKE_LISTEN_WINDOW_MS=15000
SILENCE_MS=900
MAX_UTTERANCE_MS=8000
POST_WAKE_SILENCE_MS=1200
POST_WAKE_MAX_UTTERANCE_MS=20000
STREAM_DISABLE_RESTORE_MS=8000
STREAM_DISABLE_VERIFY_DELAY_MS=1500
PRESENCE_ANNOUNCEMENTS_ENABLED=true
PRESENCE_NAME_ANNOUNCEMENT_MAX_MEMBERS=2
PRESENCE_BOT_JOIN_NAMED_MAX_MEMBERS=3
PRESENCE_MEMBER_GREETING_COOLDOWN_MS=43200000
PRESENCE_ANNOUNCEMENT_MAX_CHARS=60
VOICE_REPLY_MAX_CHARS=450
```

Если хотите другое имя:

```bash
ASSISTANT_NAME=Железяка
BOT_WAKE_WORD=железяка
BOT_WAKE_ALIASES=железка,железяко,железяку,железяке,железякой
```

Эти параметры также можно менять в веб-панели без пересборки контейнеров.

Если пользователь сказал только wake word, например “Зеро”, бот голосом отвечает короткой AI-фразой вроде “Слушаю” или “Говори”, затем на `WAKE_LISTEN_WINDOW_MS` слушает только того же спикера. Следующая фраза этого спикера идет в ИИ без повторного триггера. `POST_WAKE_SILENCE_MS` и `POST_WAKE_MAX_UTTERANCE_MS` задают, сколько ждать паузу и какой максимум у вопроса после вызова.

`PRESENCE_MEMBER_GREETING_COOLDOWN_MS=43200000` значит, что одного и того же пользователя в одном voice-канале бот приветствует не чаще одного раза за 12 часов. Join-приветствие генерируется коротко и может использовать локальную память, заметки и напоминания про этого пользователя.

`PRESENCE_BOT_JOIN_NAMED_MAX_MEMBERS=3` управляет входом самого бота: если в voice 1-3 человека, бот коротко здоровается с каждым по имени; если людей больше 3, говорит одно общее приветствие без перечисления всех.

`PRESENCE_NAME_ANNOUNCEMENT_MAX_MEMBERS=2` оставлен для фраз выхода: при большем количестве участников бот использует короткую общую фразу без имени.

`PRESENCE_ANNOUNCEMENT_MAX_CHARS=60` ограничивает длину приветствий и фраз выхода, чтобы голос не затягивался.

`STREAM_DISABLE_VERIFY_DELAY_MS=1500` задает паузу перед проверкой, оборвалась ли трансляция после запрета Stream. Бот больше не пишет, что трансляция остановлена, пока не увидит это по voice state.

`VOICE_REPLY_MAX_CHARS=450` ограничивает обычные голосовые ответы, чтобы бот не читал длинные полотна. Для Telegram и сохраненной памяти это ограничение не режет текст.

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
GROQ_CHAT_MODEL=llama-3.3-70b-versatile
GROQ_STT_MODEL=whisper-large-v3-turbo
ACTION_PARSER_MODEL=llama-3.1-8b-instant
WEB_SEARCH_MODEL=groq/compound
GROQ_AUTO_MODEL_FALLBACK=true
GROQ_MODEL_DISCOVERY_ENABLED=true
GROQ_MODEL_DISCOVERY_INTERVAL_MS=172800000
GROQ_AUTO_SELECT_DISCOVERED_MODELS=true
GROQ_CHAT_FALLBACK_MODELS=llama-3.3-70b-versatile,openai/gpt-oss-120b,meta-llama/llama-4-scout-17b-16e-instruct,qwen/qwen3-32b,openai/gpt-oss-20b,llama-3.1-8b-instant
GROQ_ACTION_FALLBACK_MODELS=llama-3.1-8b-instant,openai/gpt-oss-20b,qwen/qwen3-32b,llama-3.3-70b-versatile
GROQ_STT_FALLBACK_MODELS=whisper-large-v3-turbo,whisper-large-v3
GROQ_WEB_FALLBACK_MODELS=groq/compound,groq/compound-mini
```

`ACTION_PARSER_MODEL` лучше держать лёгкой и быстрой: она используется только для распознавания команд Discord/Telegram, а не для длинных ответов ассистента.

Если Groq вернул `429`, модель недоступна или в rate-limit headers осталось `0` requests/tokens, бот временно пропускает эту модель и пробует следующую из fallback-списка. Когда provider присылает reset-время, cooldown берется из него; иначе используется `GROQ_MODEL_LIMIT_COOLDOWN_MS` (по умолчанию 10 минут). Поэтому основной ответ начинается с более сильной `llama-3.3-70b-versatile`, а при исчерпании лимита уходит на модели слабее.

`GROQ_MODEL_DISCOVERY_ENABLED=true` включает фоновую проверку `Groq /models`: первая проверка запускается после старта, дальше по умолчанию раз в 48 часов (`GROQ_MODEL_DISCOVERY_INTERVAL_MS`). Если у провайдера появится новая сильная chat/STT/web-модель, бот добавит ее в auto-fallback и при `GROQ_AUTO_SELECT_DISCOVERED_MODELS=true` будет пробовать ее раньше статического списка. Если лимит новой модели закончится, сработает обычный cooldown и бот уйдет на следующую доступную.

Для Groq Whisper prompt ограничен лимитом провайдера, поэтому бот по умолчанию держит запас:

```bash
STT_LANGUAGE=ru
STT_ALLOWED_LANGUAGES=ru,en
STT_LANGUAGE_HINT=Основная речь на русском. Английские слова допускаются только как короткие термины, команды, ники или названия.
STT_LANGUAGE_GUARD_ENABLED=true
STT_PROMPT_MAX_CHARS=420
STT_PROMPT_MAX_BYTES=780
```

`STT_LANGUAGE=ru` фиксирует русский как основной язык распознавания. Языковой guard работает после распознавания и отбрасывает явный мусор на других языках, но оставляет короткие английские команды, ники и технические названия.

Telegram можно настроить через Discord-команду `/telegram_setup`. Через `.env` тоже можно, но не рекомендуется для публичных машин:

```bash
TELEGRAM_BOT_TOKEN=
TELEGRAM_DEFAULT_CHAT_ID=
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

`docker-compose.yml` поднимает три сервиса:

- `db` - MariaDB для памяти, напоминаний, runtime-настроек и журнала событий.
- `bot` - Discord voice assistant.
- `panel` - веб-панель управления.

Постоянные данные лежат в отдельных Docker volumes:

- `bot-data` - `/app/data`, JSON-зеркало, runtime-config, backup-файлы панели, статус.
- `db-data` - `/var/lib/mysql`, основная MariaDB-база.
- `bot-tmp` - временные аудиофайлы.

Обычный запуск на локальной машине или VPS:

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

`docker compose down` не удаляет память. Не используйте `docker compose down -v`, если не хотите стереть `bot-data` и `db-data`.

Для VPS рекомендуется держать панель закрытой на localhost и открывать ее через SSH tunnel. Если сервер стоит в домашней локальной сети и вы осознанно открываете панель в LAN, задайте `PANEL_BIND_HOST=0.0.0.0` и сложный `PANEL_PASSWORD`.

## 6. Обновление без потери памяти

Самый безопасный способ обновления:

```bash
./scripts/update.sh
```

Скрипт делает backup, подтягивает свежий `main`, пересобирает `bot`/`panel` и запускает контейнеры через `docker compose up -d --remove-orphans`. Он не удаляет Docker volumes.

Полезные варианты:

```bash
./scripts/update.sh --logs
./scripts/update.sh --skip-pull
./scripts/update.sh --skip-backup
```

Ручной вариант обновления:

```bash
./scripts/backup.sh
git pull --ff-only
docker compose pull db
docker compose build bot panel
docker compose up -d --remove-orphans
docker compose logs -f bot panel
```

Перед любым переносом или обновлением проверьте, что backup создан:

```bash
ls -lah backups/
```

Что нельзя делать при обычном обновлении:

```bash
docker compose down -v
docker volume rm ...
rm -rf data backups
```

Эти команды удаляют постоянные данные или локальные backup-файлы.

## 7. Backup и перенос на другой сервер

Создать backup:

```bash
./scripts/backup.sh
```

По умолчанию backup сохраняется в:

```text
backups/YYYYMMDD-HHMMSS/
backups/YYYYMMDD-HHMMSS.tar.gz
```

`scripts/backup.sh` и `scripts/update.sh` держат только два последних системных backup-комплекта: последний и предпоследний. Все более старые папки `backups/YYYYMMDD-HHMMSS/` и архивы `backups/YYYYMMDD-HHMMSS.tar.gz` удаляются автоматически. Количество можно изменить через `BACKUP_ARCHIVE_RETENTION`, по умолчанию `2`.

Внутри:

- `db.sql.gz` - дамп MariaDB, если контейнер `db` запущен.
- `bot-data.tgz` - архив volume `/app/data`.
- `manifest.txt` - дата, git commit, состояние compose.
- `env.redacted` - `.env` без токенов и паролей.

Если нужно сохранить и `.env` с секретами, выполните:

```bash
INCLUDE_ENV=1 ./scripts/backup.sh
```

Такой backup содержит токены и пароли. Храните его как секрет.

Сохранить backup в другую папку:

```bash
BACKUP_DIR=/mnt/backups/discord-ai ./scripts/backup.sh
```

Автоматические переносимые backup-файлы можно включить в веб-панели:

1. Откройте вкладку **Память**.
2. В блоке **Настройки backup** включите **Автоbackup**.
3. Укажите путь назначения.
4. Если SMB/FTP требует авторизацию, заполните `Логин хранилища` и `Пароль хранилища`.
5. Оставьте `Интервал` = `24`, `Хранить копий` = `2`, если нужны только две последние копии.
6. Сохраните настройки.

Поддерживаемые пути:

```text
/mnt/backups/discord-ai
file:///mnt/backups/discord-ai
ftp://192.168.0.1:21/G/Discord AI Bot Backups
smb://192.168.0.1/G/Discord AI Bot Backups
```

Для SMB первая часть пути после хоста - это имя сетевого раздела. В примере выше `G` является share name. Логин и пароль лучше сохранять отдельными полями в панели: пароль не показывается в статусе и не попадает в portable JSON backup. Также поддерживается старый формат с учёткой прямо в URL:

```text
smb://user:password@192.168.0.1/G/Discord AI Bot Backups
ftp://user:password@192.168.0.1:21/G/Discord AI Bot Backups
```

Автоbackup запускается раз в сутки, когда бот свободен: он не говорит, не обрабатывает голос и не занят командой. Кнопка **Создать backup** делает backup сразу. Если SMB/FTP не отвечает, transport обрывается по timeout `BACKUP_TRANSPORT_TIMEOUT_SECONDS`, чтобы бот не зависал на сетевом хранилище. Эти backup-файлы являются переносимым JSON-снимком активного хранилища: память сервера, персональная память, заметки, напоминания, runtime-настройки и последние события. Restore такого файла доступен из панели и подходит для восстановления на чистом проекте из репозитория. Для полного системного dump с SQL и `/app/data` используйте `./scripts/backup.sh`.

Перенос на новый VPS:

1. Установите Docker и Docker Compose plugin.
2. Склонируйте репозиторий.
3. Скопируйте `.env` или создайте новый из `.env.example`.
4. Перенесите нужный backup в папку `backups/`.
5. Запустите `docker compose up -d db`.
6. Восстановите базу и `/app/data`.
7. Запустите `docker compose up -d --build`.

Пример restore из backup-папки:

```bash
STAMP=YYYYMMDD-HHMMSS
docker compose up -d db
gunzip -c "backups/$STAMP/db.sql.gz" | docker compose exec -T db sh -lc 'mariadb -u"$MARIADB_USER" -p"$MARIADB_PASSWORD" "$MARIADB_DATABASE"'
docker compose run --rm --no-deps --user 0:0 -v "$PWD/backups/$STAMP:/backup:ro" --entrypoint sh bot -lc 'mkdir -p /app/data && find /app/data -mindepth 1 -maxdepth 1 -exec rm -rf {} + && tar -xzf /backup/bot-data.tgz -C /app/data && chown -R 10001:10001 /app/data'
docker compose up -d --build
```

Если проект был на старом JSON storage, первый запуск с `STORAGE_DRIVER=mysql` автоматически мигрирует `/app/data/state.json` и `/app/data/runtime-config.json` в MariaDB.

## 8. GitHub Actions

В репозитории есть workflow `.github/workflows/ci.yml`. Он запускается на push, pull request и вручную:

- `npm ci`;
- `npm run check`;
- `docker compose config`;
- `docker build`.

Это проверяет синтаксис Node.js-файлов и то, что Docker image собирается из чистого репозитория.

## 9. Веб-панель

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
- **Управление** - имя ассистента, trigger word, интернет-поиск, idle-фразы, приветствия в voice, авто-уход без обращений, healthcheck.
- **Ключи и модели** - Groq key, Discord token, guild ID, chat/STT/web модели.
- **Голос** - выбор TTS provider, Edge voices, скорость, тон, preview голоса.
- **Память** - счетчики, активное хранилище, backup/restore, просмотр и удаление записей памяти.
- **Напоминания** - просмотр и удаление активных одноразовых и повторяющихся напоминаний.
- **Журнал** - события бота и Docker logs.
- **Система** - RAM/load, Docker status, restart bot/panel, Groq limits.

Для Docker-кнопок панели используется Docker socket `/var/run/docker.sock`. Это удобно для локального Linux-сервера, но это высокий уровень доступа. Не выставляйте такую панель в интернет без VPN/reverse proxy/auth.

## 10. Подключение бота к voice

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

По умолчанию каждое новое обращение должно начинаться с trigger word: `Бот, ...`. Если включить `ACTIVE_DIALOGUE_ENABLED=true`, бот сможет некоторое время отвечать без повторного `бот`.

Остановить текущую речь:

```text
Бот стоп
Бот хватит
Бот остановись
```

## 11. Slash-команды

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
- `/telegram_setup` - безопасно сохранить Telegram bot token через приватное окно Discord.
- `/telegram_chat` - установить default Telegram `chat_id`.
- `/telegram_chats` - показать последние Telegram-чаты из `getUpdates`.
- `/telegram_status` - проверить Telegram-интеграцию.
- `/telegram_clear` - удалить Telegram-настройки из runtime-config.
- `/telegram_send` - отправить текст в Telegram.

Telegram-сообщения поддерживают простую Markdown-разметку: `**жирный текст**`, `*курсив*`, `` `code` `` и `[текст](https://example.com)`. Бот перед отправкой конвертирует ее в безопасный Telegram HTML; если Telegram отклонит разметку, сообщение будет автоматически отправлено обычным текстом.

## 12. Telegram bridge

1. Создайте Telegram-бота через [@BotFather](https://t.me/BotFather).
2. Скопируйте token.
3. В Discord выполните `/telegram_setup`.
4. В открывшемся приватном окне вставьте token. Токен не публикуется в канал Discord.
5. Напишите вашему Telegram-боту `/start`, либо добавьте его в группу и отправьте туда любое сообщение.
6. В Discord выполните `/telegram_chats`.
7. Скопируйте нужный `chat_id`.
8. Выполните `/telegram_chat chat_id:...`.
9. Проверьте `/telegram_send text:Тест из Discord`.

Telegram token хранится в `/app/data/runtime-config.json`, а не в git. Если вы укажете token через `.env`, он будет жить в `.env`, который тоже нельзя публиковать.

Голосовые примеры:

```text
Бот, напиши в телеграм всем привет.
Бот, скинь в телегу всем привет.
Бот, в тг чиркани всем привет.
Бот, закинь всем привет в телегу.
Бот, напиши заметку в телеграм что завтра созвон в 20:00.
Бот, сохрани в телеге заметку завтра созвон в 20:00.
Бот, найди свежие новости про Groq и отправь в телеграм.
Бот, пробей новости Groq и закинь в тг.
Бот, поищи инфу про Groq и скинь в телегу.
Бот, поищи в интернете погоду в Чернигове и отправь в телеграм.
Бот, отправь последний ответ в телеграм.
Бот, продублируй это в тг.
Бот, отправь память в телеграм.
Бот, отправь напоминания в телеграм.
Бот, покажи телеграм чаты.
Бот, покажи телеграм айди.
```

Бот также пытается понимать разговорные варианты: `телега`, `телегу`, `тг`, `telegram`, `telega`, а также глаголы `скинь`, `кинь`, `закинь`, `перекинь`, `передай`, `продублируй`, `сохрани`, `запиши`, `пробей`, `узнай`.

## 13. Примеры голосовых команд

Обычный разговор:

```text
Бот, что ты умеешь?
Бот, объясни термин webhook.
Бот, найди свежие новости про Groq.
Бот, какая сейчас погода в Киеве?
```

Память:

```text
Бот, запомни что наш сервер играет по вечерам.
Бот, запомни обо мне что я люблю короткие ответы.
Бот, запиши заметку что завтра созвон в 20:00.
Бот, сохрани что проект переносим на VPS.
Бот, что ты помнишь?
Бот, что ты помнишь обо мне?
Бот, что ты помнишь про VPS?
Бот, найди в памяти созвон.
Бот, что я просил вчера?
Бот, удали заметку про созвон.
Бот, удали память про VPS.
Бот, забудь память.
```

Напоминания:

```text
Бот, напомни через 5 минут проверить чай.
Бот, напомнить через 5 минут проверить слайды.
Бот, поставь напоминание через 10 минут перезвонить.
Бот, напомни завтра в 10:00 проверить маршрут.
Бот, напомни завтра в 10 часов дня проверить маршрут ROVEX.
Бот, напоминай каждые 2 часа размяться.
Бот, покажи напоминания.
Бот, удали напоминание про чай.
Бот, удали второе напоминание.
```

Относительные слова вроде `сегодня`, `завтра`, `послезавтра` и дни недели считаются в timezone `REMINDER_TIME_ZONE`, по умолчанию `Europe/Kyiv`. В ответе бот показывает точное время с timezone, чтобы было видно, когда напоминание реально сработает.

Discord-действия:

```text
Бот, отключи Иван от войса.
Бот, замуть Иван.
Бот, размуть Иван.
Бот, перемести Иван в Общий.
Бот, отключи всех от войса.
Бот, выключи трансляцию Ивану.
Бот, разреши трансляцию Ивану.
Бот, создай текстовый канал тест.
Бот, переименуй войс в Комната тестов.
Бот, очисти 20 сообщений.
```

Команда `выключи трансляцию` временно снимает право `Stream`, чтобы сбить активную демонстрацию экрана, и возвращает прежнее значение через `STREAM_DISABLE_RESTORE_MS`. Discord API не дает прямой команды остановить чужой stream без такого временного permission-переключения.

Админ-действия выполняются сразу после распознавания команды. Для тестового сервера это удобно, но на публичном сервере выдавайте боту только те Discord-права, которые реально готовы доверить голосовым командам.

Если локальные правила не поняли формулировку, бот подключает AI-parser действий для всех командных фраз: Discord, память, напоминания, soundboard и Telegram. Поэтому разговорные варианты вроде `выруби микрофон Досику`, `перекинь Досика в общий`, `почисти чат на 20 сообщений`, `дай Досику модерку`, `сделай комнату для рейда` тоже должны распознаваться, если действие и цель сказаны достаточно явно.

## 14. Где хранятся данные

В Docker используются два постоянных volume:

```text
bot-data -> /app/data
db-data  -> /var/lib/mysql
```

По умолчанию `docker-compose.yml` запускает MariaDB и задает:

```bash
STORAGE_DRIVER=mysql
DB_HOST=db
DB_NAME=discord_ai_assistant
DB_USER=assistant
```

Главные таблицы MariaDB:

- `guild_memories` - общая и персональная память.
- `reminders` - активные напоминания.
- `runtime_config` - настройки панели, Telegram token/chat_id, выбранные модели и голос.
- `event_logs` - журнал событий бота.

При первом запуске с MySQL бот автоматически мигрирует старые файлы из `/app/data/state.json` и `/app/data/runtime-config.json`, если они есть. После этого JSON-файлы остаются зеркалом/fallback для аварийного восстановления.

Главные файлы в `/app/data`:

- `/app/data/runtime-config.json` - настройки из панели.
- Telegram token, если сохранен через `/telegram_setup`, тоже лежит в `/app/data/runtime-config.json`.
- `/app/data/state.json` - JSON-зеркало памяти и напоминаний.
- `/app/data/status.json` - последний статус бота.
- `/app/data/events.jsonl` - JSONL-зеркало журнала событий.
- `/app/data/backups/` - переносимые backup-файлы активного хранилища: память, напоминания, runtime-config и последние события.

Эти файлы не должны попадать в публичный git.

Создать backup базы/памяти можно в панели во вкладке **Память**. Restore восстанавливает состояние в активное хранилище. После restore рекомендуется перезапустить бота.

## 15. Диагностика

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
- предупреждения о лимитах отправляются в Discord только один раз при пересечении порогов `15%` и `5%`; меняется через `API_LIMIT_ALERT_THRESHOLDS=15,5`. Повторные уведомления включатся только после восстановления лимита выше `API_LIMIT_ALERT_RESET_PERCENT=50`.

Telegram не отправляет:

- проверьте `/telegram_status`;
- напишите Telegram-боту `/start`;
- для группы добавьте бота в группу и отправьте туда любое сообщение;
- проверьте `/telegram_chats` и сохраните правильный `chat_id` через `/telegram_chat`;
- если token случайно попал в чат или git, сразу перевыпустите его через @BotFather.

Панель не открывается:

```bash
docker compose ps
docker compose logs -f panel
```

Проверьте `PANEL_BIND_HOST`, `PANEL_PORT`, firewall и SSH tunnel.

## 16. Структура проекта

```text
voice-bot.mjs       основной Discord voice bot
panel-server.mjs    backend веб-панели
panel/              статический frontend панели
Dockerfile          образ для bot/panel
docker-compose.yml  сервисы bot, panel, db и постоянные volumes
scripts/backup.sh   backup MariaDB и /app/data
scripts/update.sh   безопасное обновление сервера без удаления volumes
.github/workflows/  CI: syntax check, compose config, Docker build
.env.example        шаблон настроек без секретов
```
