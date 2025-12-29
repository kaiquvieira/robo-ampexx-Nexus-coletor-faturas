FROM node:20-bullseye

# ===============================
# Dependências de sistema
# ===============================
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-venv \
    python3-pip \
    chromium \
    chromium-driver \
    fonts-liberation \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libxkbcommon0 \
    libgtk-3-0 \
    libasound2 \
    libgbm1 \
    libxshmfence1 \
 && rm -rf /var/lib/apt/lists/*

# ===============================
# Python virtualenv
# ===============================
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# ===============================
# Python deps
# ===============================
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ===============================
# Node deps
# ===============================
WORKDIR /app
COPY package*.json ./
RUN npm install

# ===============================
# Código
# ===============================
COPY . .

# ===============================
# Playwright
# ===============================
RUN npx playwright install chromium

# ===============================
# Start
# ===============================
CMD ["node", "index.js"]
