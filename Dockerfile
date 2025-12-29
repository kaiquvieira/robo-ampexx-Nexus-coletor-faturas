FROM node:20-bookworm

# Dependências do Playwright (Chromium)
RUN npx playwright install-deps chromium

# Python + pip
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Node deps
COPY package*.json ./
RUN npm ci

# Python deps
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

# App
COPY . .

# Playwright browsers (instala o Chromium no container)
RUN npx playwright install chromium

# Rode o job (node chama o python no final)
CMD ["node", "index.js"]
