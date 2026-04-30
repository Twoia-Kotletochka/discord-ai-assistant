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
ACTIVE_DIALOGUE_ENABLED=false
ACTIVE_DIALOGUE_SECONDS=45
WAKE_LISTEN_WINDOW_MS=9000
SILENCE_MS=900
MAX_UTTERANCE_MS=8000
```

Если хотите другое имя:

```bash
ASSISTANT_NAME=Железяка
BOT_WAKE_WORD=железяка
BOT_WAKE_ALIASES=железка,железяко,железяку,железяке,железякой
```

Эти параметры также можно менять в веб-панели без пересборки контейнеров.

Если пользователь сказал только wake word, например “Зеро”, бот на `WAKE_LISTEN_WINDOW_MS` включает короткое окно ожидания следующей фразы без повторного триггера. `SILENCE_MS` и `MAX_UTTERANCE_MS` задают, как быстро Discord-аудио чанк закрывается после паузы или фонового шума.

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
ACTION_PARSER_MODEL=llama-3.1-8b-instant
WEB_SEARCH_MODEL=groq/compound
```

`ACTION_PARSER_MODEL` лучше держать лёгкой и быстрой: она используется только для распознавания команд Discord/Telegram, а не для длинных ответов ассистента.

Для Groq Whisper prompt ограничен лимитом провайдера, поэтому бот по умолчанию держит запас:

```bash
STT_PROMPT_MAX_CHARS=420
STT_PROMPT_MAX_BYTES=780
```

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
- **Управление** - имя ассистента, trigger word, интернет-поиск, idle-фразы, приветствия в voice, авто-уход без обращений, healthcheck.
- **Ключи и модели** - Groq key, Discord token, guild ID, chat/STT/web модели.
- **Голос** - выбор TTS provider, Edge voices, скорость, тон, preview голоса.
- **Память** - счетчики, активное хранилище, backup/restore, просмотр и удаление записей памяти.
- **Напоминания** - просмотр и удаление активных одноразовых и повторяющихся напоминаний.
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

По умолчанию каждое новое обращение должно начинаться с trigger word: `Бот, ...`. Если включить `ACTIVE_DIALOGUE_ENABLED=true`, бот сможет некоторое время отвечать без повторного `бот`.

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
- `/telegram_setup` - безопасно сохранить Telegram bot token через приватное окно Discord.
- `/telegram_chat` - установить default Telegram `chat_id`.
- `/telegram_chats` - показать последние Telegram-чаты из `getUpdates`.
- `/telegram_status` - проверить Telegram-интеграцию.
- `/telegram_clear` - удалить Telegram-настройки из runtime-config.
- `/telegram_send` - отправить текст в Telegram.

## 9. Telegram bridge

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

## 10. Примеры голосовых команд

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

Админ-действия выполняются сразу после распознавания команды. Для тестового сервера это удобно, но на публичном сервере выдавайте боту только те Discord-права, которые реально готовы доверить голосовым командам.

Если локальные правила не поняли формулировку, бот подключает AI-parser действий для всех командных фраз: Discord, память, напоминания, soundboard и Telegram. Поэтому разговорные варианты вроде `выруби микрофон Досику`, `перекинь Досика в общий`, `почисти чат на 20 сообщений`, `дай Досику модерку`, `сделай комнату для рейда` тоже должны распознаваться, если действие и цель сказаны достаточно явно.

## 11. Где хранятся данные

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
- `/app/data/backups/` - backup-файлы памяти/runtime-config из активного хранилища.

Эти файлы не должны попадать в публичный git.

Создать backup базы/памяти можно в панели во вкладке **Память**. Restore восстанавливает состояние в активное хранилище. После restore рекомендуется перезапустить бота.

## 12. Диагностика

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

## 13. Структура проекта

```text
voice-bot.mjs       основной Discord voice bot
panel-server.mjs    backend веб-панели
panel/              статический frontend панели
Dockerfile          образ для bot/panel
docker-compose.yml  два контейнера: bot и panel
.env.example        шаблон настроек без секретов
```
