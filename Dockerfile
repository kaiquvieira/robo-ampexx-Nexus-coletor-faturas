# =========================================
# Dockerfile (Opção A) — Base Node + Python em venv
# Resolve PEP 668 (externally-managed-environment)
# =========================================

FROM node:20-bookworm-slim

WORKDIR /app

# -------------------------
# Dependências do sistema
# - python3 + venv para instalar libs do seu leitor (pdfplumber etc)
# - libs comuns que o pdfplumber/pillow podem precisar
# -------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-venv \
    python3-pip \
    ca-certificates \
    curl \
    git \
    build-essential \
    libmagic1 \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    libpango-1.0-0 \
    libcairo2 \
    libjpeg62-turbo \
    libpng16-16 \
    libfreetype6 \
    fonts-liberation \
  && rm -rf /var/lib/apt/lists/*

# -------------------------
# Python venv (PEP 668 safe)
# -------------------------
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# -------------------------
# Instala dependências Python no venv
# (mantém cache eficiente: requirements antes do código)
# -------------------------
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# -------------------------
# Instala dependências Node
# -------------------------
COPY package*.json ./
RUN npm ci --omit=dev

# Playwright browsers (se você usa Playwright no Node)
# Importante: isso baixa os browsers dentro da imagem
RUN npx playwright install --with-deps chromium

# -------------------------
# Copia o restante do projeto
# -------------------------
COPY . .

# -------------------------
# Variáveis padrão (opcional)
# -------------------------
ENV NODE_ENV=production
ENV PYTHON_BIN=/opt/venv/bin/python

# -------------------------
# Comando principal
# -------------------------
CMD ["node", "index.js"]
