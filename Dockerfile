FROM node:24-bookworm-slim

ENV NODE_ENV=production \
    TTS_PROVIDER=edge \
    EDGE_TTS_COMMAND=/opt/edge-tts/bin/edge-tts

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      ca-certificates \
      espeak-ng \
      ffmpeg \
      python3 \
      python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/edge-tts \
    && /opt/edge-tts/bin/pip install --no-cache-dir edge-tts==7.2.8

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY voice-bot.mjs panel-server.mjs ./
COPY panel ./panel

RUN mkdir -p /app/tmp /app/data \
    && useradd --system --create-home --uid 10001 botuser \
    && chown -R botuser:botuser /app

USER botuser

CMD ["node", "voice-bot.mjs"]
