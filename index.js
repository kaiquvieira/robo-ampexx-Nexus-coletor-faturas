import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { chromium } from 'playwright';
import { spawn } from 'child_process';
import { createClient } from '@supabase/supabase-js';

// ===================== ENV =====================
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || '';

const SUPABASE_KEY = SUPABASE_SERVICE_ROLE_KEY || SUPABASE_ANON_KEY || '';
if (!SUPABASE_URL) throw new Error('SUPABASE_URL não definido no .env');
if (!SUPABASE_KEY) throw new Error('Defina SUPABASE_SERVICE_ROLE_KEY (recomendado) ou SUPABASE_ANON_KEY no .env');

const SUPABASE_ACCOUNTS_TABLE = process.env.SUPABASE_ACCOUNTS_TABLE || 'cadastros_acesso';
const SUPABASE_TARGETS_TABLE = process.env.SUPABASE_TARGETS_TABLE || 'targets_uc';

const SUPABASE_STORAGE_BUCKET = process.env.SUPABASE_STORAGE_BUCKET || 'faturas-elektro';
const SUPABASE_STORAGE_PUBLIC = (process.env.SUPABASE_STORAGE_PUBLIC || 'false').toLowerCase() === 'true';

// ===================== Supabase client =====================
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false },
});

// ===================== Config =====================
const BASE_URL = process.env.NEOENERGIA_BASE_URL || 'https://agenciavirtual.neoenergia.com/';
const MEUS_IMOVEIS_URL = `${BASE_URL}#/home/meus-imoveis`;

// Em container (Railway), TEM que ser headless.
// Mesmo que você set HEADLESS=false por engano, forçamos true se não tiver DISPLAY.
const HEADLESS_ENV = (process.env.HEADLESS || 'true').toLowerCase() === 'true';
const RUNNING_IN_CONTAINER = !!process.env.RAILWAY_ENVIRONMENT || !!process.env.RAILWAY_PROJECT_ID || fs.existsSync('/.dockerenv');
const HAS_DISPLAY = !!process.env.DISPLAY;

const HEADLESS = (RUNNING_IN_CONTAINER && !HAS_DISPLAY) ? true : HEADLESS_ENV;

const KEEP_OPEN = (process.env.KEEP_OPEN || 'false').toLowerCase() === 'true';
const KEEP_OPEN_MS = Number(process.env.KEEP_OPEN_MS || '0');

const DEBUG_SAVE_AUX = (process.env.DEBUG_SAVE_AUX || 'false').toLowerCase() === 'true';
const CLEAN_CLIENT_PDFS_BEFORE_SAVE = (process.env.CLEAN_CLIENT_PDFS_BEFORE_SAVE || 'true').toLowerCase() === 'true';

const DOWNLOADS_ROOT = process.env.DOWNLOADS_ROOT || path.join(process.cwd(), 'downloads');
const DEBUG_ROOT = process.env.DEBUG_ROOT || path.join(process.cwd(), 'debug');

const SLOW_MO = Number(process.env.SLOW_MO || '0');

// ===================== Utils =====================
function safeFileName(s) { return String(s || '').replace(/[^\w.-]+/g, '_'); }
function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }
function onlyDigits(s) { return String(s || '').replace(/\D+/g, ''); }
async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function getMonthKey(d = new Date()) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  return `${y}-${m}`;
}

function waitForTerminalEnter(message) {
  return new Promise((resolve) => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    rl.question(message, () => { rl.close(); resolve(); });
  });
}

// ===================== Python runner =====================
function runPythonScript(scriptPath, args = [], options = {}) {
  return new Promise((resolve, reject) => {
    const pyBin = process.env.PYTHON_BIN || 'python';

    const p = spawn(pyBin, [scriptPath, ...args], {
      stdio: ['ignore', 'pipe', 'pipe'],
      ...options,
    });

    let stdout = '';
    let stderr = '';

    p.stdout.on('data', (d) => (stdout += d.toString()));
    p.stderr.on('data', (d) => (stderr += d.toString()));

    p.on('error', (err) => reject(err));

    p.on('close', (code) => {
      if (code === 0) return resolve({ code, stdout, stderr });
      const e = new Error(`Python saiu com code=${code}\n${stderr || stdout}`);
      e.code = code;
      e.stdout = stdout;
      e.stderr = stderr;
      reject(e);
    });
  });
}

// ===================== Execution window rules (dia_fatura) =====================
function clampDay(d) {
  const n = Number(d);
  if (!Number.isFinite(n)) return null;
  if (n < 1) return 1;
  if (n > 31) return 31;
  return n;
}

function shouldRunUcToday(target, today = new Date()) {
  const dia = clampDay(target?.dia_fatura);
  if (!dia) return true;

  const tol = clampDay(target?.tolerancia_dias) ?? 2;
  const nowDay = today.getDate();

  const start = clampDay(dia - tol);
  const end = clampDay(dia + tol);

  return nowDay >= start && nowDay <= end;
}

// ===================== Load accounts from Supabase =====================
async function loadAccountsFromSupabase() {
  const { data: cadastros, error: err1 } = await supabase
    .from(SUPABASE_ACCOUNTS_TABLE)
    .select('id, nome, cpf_cnpj, senha, ativo, created_at')
    .eq('ativo', true)
    .order('created_at', { ascending: true });

  if (err1) throw new Error(`Supabase ${SUPABASE_ACCOUNTS_TABLE}: ${err1.message}`);
  if (!cadastros?.length) return [];

  const ids = cadastros.map(c => c.id);

  const { data: targets, error: err2 } = await supabase
    .from(SUPABASE_TARGETS_TABLE)
    .select('cadastro_id, uc_code, ativo, dia_fatura, tolerancia_dias')
    .in('cadastro_id', ids);

  if (err2) throw new Error(`Supabase ${SUPABASE_TARGETS_TABLE}: ${err2.message}`);

  const mapTargets = new Map();
  for (const t of (targets || [])) {
    if (t?.ativo === false) continue;

    const k = t.cadastro_id;
    const arr = mapTargets.get(k) || [];

    arr.push({
      uc_code: String(t.uc_code || '').trim(),
      dia_fatura: t.dia_fatura ?? null,
      tolerancia_dias: t.tolerancia_dias ?? 2,
    });

    mapTargets.set(k, arr);
  }

  return cadastros.map(c => ({
    id: c.id,
    name: c.nome,
    document: c.cpf_cnpj,
    password: c.senha,
    targets: (mapTargets.get(c.id) || []).filter(t => t?.uc_code),
  }));
}

// ===================== Playwright stability helpers =====================
async function safeScreenshot(page, filePath) {
  try {
    if (page && !page.isClosed()) {
      await page.screenshot({ path: filePath, fullPage: true });
      return true;
    }
  } catch {}
  return false;
}

async function waitForUiStable(page, timeoutMs = 45000) {
  await page.waitForLoadState('domcontentloaded', { timeout: timeoutMs }).catch(() => {});
  const start = Date.now();

  let lastH = -1;
  let stableCount = 0;

  while (Date.now() - start < timeoutMs) {
    const h = await page.evaluate(() => document.body ? document.body.scrollHeight : 0).catch(() => 0);
    if (h === lastH) stableCount += 1;
    else stableCount = 0;
    lastH = h;

    const hasSpinner = await page.evaluate(() => {
      const sel = [
        'mat-progress-spinner',
        '.mat-progress-spinner',
        '.mat-mdc-progress-spinner',
        '.loading',
        '.spinner',
        '.ngx-spinner',
        '.cdk-overlay-backdrop.showing',
      ].join(',');
      return !!document.querySelector(sel);
    }).catch(() => false);

    if (!hasSpinner && stableCount >= 2) return true;
    await sleep(400);
  }
  return true;
}

// ===================== Supabase Storage helpers =====================
function isPdfBytes(buf) {
  if (!buf || buf.length < 5) return false;
  return buf[0] === 0x25 && buf[1] === 0x50 && buf[2] === 0x44 && buf[3] === 0x46;
}

function buildStorageKey({ monthKey, accountName, clientCode }) {
  return [
    safeFileName(monthKey),
    safeFileName(accountName),
    `${safeFileName(clientCode)}.pdf`
  ].join('/');
}

async function uploadBytesToStorage(objectKey, buf, contentType = 'application/octet-stream') {
  const { error } = await supabase
    .storage
    .from(SUPABASE_STORAGE_BUCKET)
    .upload(objectKey, buf, {
      upsert: true,
      contentType,
      cacheControl: '3600',
    });

  if (error) throw new Error(`Supabase Storage upload falhou: ${error.message}`);

  let publicUrl = null;
  if (SUPABASE_STORAGE_PUBLIC) {
    const { data } = supabase.storage.from(SUPABASE_STORAGE_BUCKET).getPublicUrl(objectKey);
    publicUrl = data?.publicUrl || null;
  }
  return { bucket: SUPABASE_STORAGE_BUCKET, objectKey, publicUrl };
}

async function uploadPdfToSupabaseStorage(pdfBuf, { monthKey, accountName, clientCode }) {
  if (!isPdfBytes(pdfBuf)) throw new Error('Buffer não é PDF válido (%PDF não encontrado).');
  const objectKey = buildStorageKey({ monthKey, accountName, clientCode });
  return uploadBytesToStorage(objectKey, pdfBuf, 'application/pdf');
}

async function storagePdfAlreadyExists({ monthKey, accountName, clientCode }) {
  const objectKey = buildStorageKey({ monthKey, accountName, clientCode });

  const prefix = objectKey.split('/').slice(0, -1).join('/') + '/';
  const fileName = objectKey.split('/').pop();

  const { data, error } = await supabase
    .storage
    .from(SUPABASE_STORAGE_BUCKET)
    .list(prefix, { limit: 1000 });

  if (error) return false;
  return (data || []).some((it) => it?.name === fileName);
}

// ===================== DEBUG: salva local + sobe no Storage em debug/ =====================
async function dumpDebug(page, accountName, prefix) {
  const monthKey = getMonthKey(new Date());
  const baseLocal = path.join(DEBUG_ROOT, safeFileName(monthKey), safeFileName(accountName || 'SEM_CONTA'));
  ensureDir(baseLocal);

  const ts = Date.now();
  const shotLocal = path.join(baseLocal, `${prefix}_${ts}.png`);
  const htmlLocal = path.join(baseLocal, `${prefix}_${ts}.html`);

  await safeScreenshot(page, shotLocal);

  let html = '';
  try {
    if (page && !page.isClosed()) {
      html = await page.content();
      fs.writeFileSync(htmlLocal, html, 'utf-8');
    }
  } catch {}

  // Sobe pro Storage em debug/<YYYY-MM>/<CONTA>/
  let shotRemote = null;
  let htmlRemote = null;

  try {
    const shotKey = ['debug', safeFileName(monthKey), safeFileName(accountName || 'SEM_CONTA'), `${prefix}_${ts}.png`].join('/');
    const htmlKey = ['debug', safeFileName(monthKey), safeFileName(accountName || 'SEM_CONTA'), `${prefix}_${ts}.html`].join('/');

    if (fs.existsSync(shotLocal)) {
      const buf = fs.readFileSync(shotLocal);
      const up = await uploadBytesToStorage(shotKey, buf, 'image/png');
      shotRemote = `storage://${up.bucket}/${up.objectKey}`;
    }
    if (html && html.length) {
      const buf = Buffer.from(html, 'utf-8');
      const up = await uploadBytesToStorage(htmlKey, buf, 'text/html');
      htmlRemote = `storage://${up.bucket}/${up.objectKey}`;
    }
  } catch (e) {
    console.log(`[WARN][DEBUG UPLOAD] Falhou subir debug pro storage: ${e?.message || e}`);
  }

  if (shotRemote || htmlRemote) {
    console.log(`[DEBUG] Dump salvo no Storage: ${shotRemote || ''} ${htmlRemote || ''}`.trim());
  } else {
    console.log(`[DEBUG] Dump salvo local: ${shotLocal} ${htmlLocal}`);
  }

  return { shotLocal, htmlLocal, shotRemote, htmlRemote };
}

// ===================== Cookies/UI stable =====================
async function maybeAcceptCookies(page, accountName) {
  const candidates = [
    page.getByRole('button', { name: /aceitar|concordar|accept|ok|entendi/i }).first(),
    page.locator('button:has-text("Aceitar")').first(),
    page.locator('button:has-text("Concordo")').first(),
    page.locator('button:has-text("Entendi")').first(),
    page.locator('[id*="cookie" i] button:has-text("Aceitar")').first(),
    page.locator('button[aria-label*="aceitar" i]').first(),
  ];

  for (const b of candidates) {
    try {
      if (await b.count()) {
        await b.click({ timeout: 2500 }).catch(() => b.click({ timeout: 2500, force: true }));
        await sleep(200);
        break;
      }
    } catch {}
  }
}

// ===================== Swal OK =====================
async function clickSwalOkIfVisible(page, timeoutMs = 20000) {
  const btn = page.locator('button.swal2-confirm.swal2-styled').first()
    .or(page.getByRole('button', { name: /^ok$/i }).first());

  try {
    await btn.waitFor({ state: 'visible', timeout: timeoutMs });
    await btn.click({ timeout: 8000 }).catch(() => btn.click({ timeout: 8000, force: true }));
    await page.waitForTimeout(250);
    return true;
  } catch {
    return false;
  }
}

// ===================== Login helpers (ROBUSTO) =====================
async function clickLoginAndCapturePopup(page) {
  const loginCandidate = page
    .getByRole('link', { name: /login|entrar|acessar/i }).first()
    .or(page.getByRole('button', { name: /login|entrar|acessar/i }).first())
    .or(page.locator('a:has-text("Entrar"), button:has-text("Entrar")').first());

  if (!(await loginCandidate.count())) return page;

  const popupPromise = page.waitForEvent('popup', { timeout: 10000 }).catch(() => null);
  await loginCandidate.click({ timeout: 20000 }).catch(() => {});
  const popup = await popupPromise;

  if (popup) {
    await popup.waitForLoadState('domcontentloaded').catch(() => {});
    return popup;
  }

  await page.waitForLoadState('domcontentloaded').catch(() => {});
  return page;
}

// procura input CPF/CNPJ em qualquer frame
async function findCpfCnpjLocatorInAnyFrame(page) {
  const selectors = [
    'input[formcontrolname*="cpf" i]',
    'input[formcontrolname*="cnpj" i]',
    'input[name*="cpf" i]',
    'input[id*="cpf" i]',
    'input[name*="cnpj" i]',
    'input[id*="cnpj" i]',
    'input[placeholder*="CPF" i]',
    'input[placeholder*="CNPJ" i]',
    'input[aria-label*="CPF" i]',
    'input[aria-label*="CNPJ" i]',
    // fallback genérico:
    'input[type="tel"]',
    'input[type="text"]',
  ];

  // 1) tenta no main frame
  for (const sel of selectors) {
    const loc = page.locator(sel).first();
    try { if (await loc.count()) return { scope: page, locator: loc, selector: sel }; } catch {}
  }

  // 2) tenta em frames
  for (const f of page.frames()) {
    for (const sel of selectors) {
      const loc = f.locator(sel).first();
      try { if (await loc.count()) return { scope: f, locator: loc, selector: sel }; } catch {}
    }
  }

  // 3) tenta getByLabel / getByPlaceholder (às vezes é mais certeiro)
  const alt1 = page.getByLabel(/cpf|cnpj/i).first();
  try { if (await alt1.count()) return { scope: page, locator: alt1, selector: 'getByLabel(cpf|cnpj)' }; } catch {}

  const alt2 = page.getByPlaceholder(/cpf|cnpj/i).first();
  try { if (await alt2.count()) return { scope: page, locator: alt2, selector: 'getByPlaceholder(cpf|cnpj)' }; } catch {}

  return null;
}

async function findPasswordLocatorInAnyFrame(page) {
  // password quase sempre é estável
  const main = page.locator('input[type="password"]').first();
  try { if (await main.count()) return { scope: page, locator: main }; } catch {}

  for (const f of page.frames()) {
    const loc = f.locator('input[type="password"]').first();
    try { if (await loc.count()) return { scope: f, locator: loc }; } catch {}
  }
  return null;
}

async function fillDocumentRobust(page, rawDocument, accountName) {
  const doc = onlyDigits(rawDocument);

  // tenta por até 60s (em headless pode demorar carregar)
  const start = Date.now();
  while (Date.now() - start < 60000) {
    const found = await findCpfCnpjLocatorInAnyFrame(page);
    if (found?.locator) {
      try {
        await found.locator.waitFor({ state: 'visible', timeout: 5000 });
        await found.locator.scrollIntoViewIfNeeded().catch(() => {});
        await found.locator.click({ clickCount: 3, timeout: 5000 }).catch(() => found.locator.click({ timeout: 5000, force: true }));
        await found.locator.fill('').catch(() => {});
        await found.locator.type(doc, { delay: 60 });
        await page.keyboard.press('Tab').catch(() => {});
        return true;
      } catch (e) {
        // pode estar coberto por overlay; tenta aceitar cookies de novo
        await maybeAcceptCookies(page, accountName);
      }
    }
    await sleep(450);
  }

  // debug rico
  console.log(`[ERRO] Não encontrei/preenchi CPF/CNPJ. url=${page.url()}`);
  await dumpDebug(page, accountName, 'doc_input_fail');
  throw new Error('Não consegui preencher o campo CPF/CNPJ.');
}

async function fillPasswordRobust(page, password, accountName) {
  const start = Date.now();
  while (Date.now() - start < 45000) {
    const found = await findPasswordLocatorInAnyFrame(page);
    if (found?.locator) {
      try {
        await found.locator.waitFor({ state: 'visible', timeout: 5000 });
        await found.locator.scrollIntoViewIfNeeded().catch(() => {});
        await found.locator.click({ clickCount: 3, timeout: 5000 }).catch(() => found.locator.click({ timeout: 5000, force: true }));
        await found.locator.fill('').catch(() => {});
        await found.locator.type(String(password), { delay: 50 });
        await page.keyboard.press('Tab').catch(() => {});
        return true;
      } catch {
        await maybeAcceptCookies(page, accountName);
      }
    }
    await sleep(350);
  }

  await dumpDebug(page, accountName, 'pass_input_fail');
  throw new Error('Não consegui preencher a senha.');
}

async function clickEntrarRobust(page, accountName) {
  const btnCandidates = [
    page.locator('button.btn-neoprimary.w-50[title="Entrar"][type="button"]').first(),
    page.locator('button:has-text("ENTRAR")').first(),
    page.locator('button[title="Entrar"]').first(),
    page.getByRole('button', { name: /entrar/i }).first(),
  ];

  let btn = null;
  for (const b of btnCandidates) {
    try {
      if (await b.count()) { btn = b; break; }
    } catch {}
  }

  if (!btn) {
    await dumpDebug(page, accountName, 'no_enter_button');
    throw new Error('Não encontrei o botão ENTRAR.');
  }

  await btn.scrollIntoViewIfNeeded().catch(() => {});
  await btn.waitFor({ state: 'visible', timeout: 20000 }).catch(async () => {
    await dumpDebug(page, accountName, 'enter_not_visible');
    throw new Error('Botão ENTRAR não ficou visível.');
  });

  const disabled = await btn.evaluate(el => el.disabled === true).catch(() => false);
  if (disabled) {
    await dumpDebug(page, accountName, 'enter_disabled');
    throw new Error('Botão ENTRAR está desabilitado.');
  }

  await btn.click({ timeout: 20000 }).catch(() => btn.click({ timeout: 20000, force: true }));
  await page.waitForLoadState('domcontentloaded', { timeout: 30000 }).catch(() => {});
  await sleep(600);
}

async function captchaVisible(page) {
  const iframe = page.locator('iframe[src*="recaptcha"], iframe[title*="reCAPTCHA" i]').first();
  try { return await iframe.isVisible({ timeout: 1500 }); } catch { return false; }
}

// ===================== Estado + filtro (mantive o seu comportamento) =====================
async function findFrameWithStateCards(page) {
  for (const f of page.frames()) {
    try { if ((await f.locator('mat-card.card-estado').count()) > 0) return f; } catch {}
  }
  return page.mainFrame();
}

async function waitForStateSelection(page, accountName) {
  const timeoutMs = 45000;
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const f = await findFrameWithStateCards(page);
    try {
      const count = await f.locator('mat-card.card-estado').count();
      if (count > 0) return f;
    } catch {}
    await sleep(500);
  }

  await dumpDebug(page, accountName, 'state_selection_not_loaded');
  throw new Error('Tela de seleção (cards de estado) não detectada.');
}

async function findFrameWithFilterSelect(page) {
  for (const f of page.frames()) {
    try {
      const c1 = await f.locator('mat-select[placeholder="Filtrar"]').count();
      const c2 = await f.locator('mat-form-field:has-text("Filtrar") mat-select').count();
      if ((c1 + c2) > 0) return f;
    } catch {}
  }
  return page.mainFrame();
}

async function waitForFilterSelect(page, accountName, timeoutMs = 60000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const f = await findFrameWithFilterSelect(page);
    const sel = f.locator('mat-select[placeholder="Filtrar"]').first()
      .or(f.locator('mat-form-field:has-text("Filtrar") mat-select').first());

    try {
      if (await sel.count()) {
        await sel.waitFor({ state: 'visible', timeout: 1500 });
        return { frame: f, matSelect: sel };
      }
    } catch {}
    await sleep(350);
  }

  await dumpDebug(page, accountName, 'filter_not_found');
  throw new Error('Não encontrei o filtro "Filtrar".');
}

async function selectSaoPauloInFrame(frame, page, accountName) {
  const spRegex = /São\s*Paulo/i;
  const spCard = frame.locator('mat-card.card-estado').filter({ hasText: spRegex }).first();

  try {
    await spCard.waitFor({ state: 'visible', timeout: 30000 });
    await spCard.scrollIntoViewIfNeeded().catch(() => {});
    await spCard.click({ timeout: 20000 }).catch(() => spCard.click({ timeout: 20000, force: true }));

    await waitForFilterSelect(page, accountName, 60000);
  } catch {
    await dumpDebug(page, accountName, 'select_sao_paulo_fail');
    throw new Error('Não consegui clicar no card "São Paulo".');
  }
}

async function filterSelectLigada(page, accountName) {
  const { matSelect } = await waitForFilterSelect(page, accountName, 60000);

  try {
    const trigger = matSelect.locator('.mat-select-trigger').first();
    await matSelect.scrollIntoViewIfNeeded().catch(() => {});

    if (await trigger.count()) {
      await trigger.click({ timeout: 20000 }).catch(() => trigger.click({ timeout: 20000, force: true }));
    } else {
      await matSelect.click({ timeout: 20000 }).catch(() => matSelect.click({ timeout: 20000, force: true }));
    }

    const overlayPane = page.locator('div.cdk-overlay-pane').first();
    await overlayPane.waitFor({ state: 'visible', timeout: 20000 });

    const option = page.locator('mat-option').filter({ hasText: /^\s*Ligada\s*$/i }).first();
    if ((await option.count()) === 0) {
      await dumpDebug(page, accountName, 'ligada_option_not_found');
      throw new Error('Não encontrei a opção "Ligada" no overlay.');
    }

    await option.waitFor({ state: 'visible', timeout: 20000 });
    await option.click({ timeout: 15000 }).catch(() => option.click({ timeout: 15000, force: true }));

    await page.waitForTimeout(400);
  } catch (e) {
    await dumpDebug(page, accountName, 'filter_ligada_fail');
    throw new Error(`Falha ao selecionar Filtrar -> Ligada. ${e?.message || ''}`.trim());
  }
}

// ===================== Lista de imóveis =====================
async function findFrameWithImoveisList(page) {
  for (const f of page.frames()) {
    try { if ((await f.locator('div.box-imoveis').count()) > 0) return f; } catch {}
  }
  return page.mainFrame();
}

async function waitForImoveisList(page, accountName, timeoutMs = 60000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const f = await findFrameWithImoveisList(page);
    try {
      const n = await f.locator('div.box-imoveis').count();
      if (n > 0) return f;
    } catch {}
    await sleep(350);
  }
  await dumpDebug(page, accountName, 'imoveis_list_not_loaded');
  throw new Error('Lista de imóveis (box-imoveis) não carregou.');
}

async function getVisibleCodesOnPage(page, accountName) {
  const frame = await waitForImoveisList(page, accountName, 60000);
  const codes = await frame.$$eval('div.unidade-consumidora-value', els =>
    els.map(e => (e.textContent || '').trim())
  );
  return (codes || []).filter(Boolean);
}

async function clickImovelByCodigo(page, accountName, codigo) {
  const code = String(codigo).trim();
  const frame = await waitForImoveisList(page, accountName, 60000);

  const card = frame.locator('div.box-imoveis').filter({
    has: frame.locator('div.unidade-consumidora-value', { hasText: new RegExp(`^\\s*${code}\\s*$`) })
  }).first();

  if ((await card.count()) === 0) {
    // Não derruba o fluxo: deixa quem chamou decidir
    return null;
  }

  await waitForUiStable(page, 45000);

  await card.scrollIntoViewIfNeeded().catch(() => {});
  await card.waitFor({ state: 'visible', timeout: 30000 });

  const arrow = card.locator('mat-icon[svgicon="arrow_forward"]').first();
  const urlBefore = page.url();

  if (await arrow.count()) {
    await arrow.click({ timeout: 15000 }).catch(() => arrow.click({ timeout: 15000, force: true }));
  } else {
    await card.click({ timeout: 15000 }).catch(() => card.click({ timeout: 15000, force: true }));
  }

  const start = Date.now();
  while (Date.now() - start < 60000) {
    if (page.url() !== urlBefore) return true;
    await sleep(300);
  }

  return null;
}

// ===================== Faturas / Visualizar =====================
function cleanPdfBase64(s) {
  if (!s) return null;
  let x = String(s).trim();
  const prefix = 'data:application/pdf;base64,';
  if (x.toLowerCase().startsWith(prefix)) x = x.slice(prefix.length);
  x = x.replace(/\s+/g, '');
  if (x.startsWith('JVBER') && x.length > 2000) return x;
  return null;
}

function findPdfBase64InJson(obj) {
  const seen = new Set();
  function walk(v) {
    if (v == null) return null;
    if (typeof v === 'string') return cleanPdfBase64(v);
    if (typeof v !== 'object') return null;
    if (seen.has(v)) return null;
    seen.add(v);
    if (Array.isArray(v)) {
      for (const it of v) {
        const r = walk(it);
        if (r) return r;
      }
      return null;
    }
    for (const k of Object.keys(v)) {
      const r = walk(v[k]);
      if (r) return r;
    }
    return null;
  }
  return walk(obj);
}

function findPdfUrlInJson(obj) {
  const seen = new Set();
  function walk(v) {
    if (v == null) return null;
    if (typeof v === 'string') {
      const x = v.trim();
      if ((x.startsWith('http://') || x.startsWith('https://')) &&
          (x.toLowerCase().includes('pdf') || x.toLowerCase().includes('/servicos/faturas/'))) {
        return x;
      }
      return null;
    }
    if (typeof v !== 'object') return null;
    if (seen.has(v)) return null;
    seen.add(v);
    if (Array.isArray(v)) {
      for (const it of v) {
        const r = walk(it);
        if (r) return r;
      }
      return null;
    }
    for (const k of Object.keys(v)) {
      const r = walk(v[k]);
      if (r) return r;
    }
    return null;
  }
  return walk(obj);
}

function looksRelevantResponse(resp) {
  try {
    const h = resp.headers();
    const ct = String(h['content-type'] || '').toLowerCase();
    const url = String(resp.url() || '').toLowerCase();

    if (url.startsWith('blob:')) return true;
    if (url.includes('apiseprd.neoenergia.com') && url.includes('/servicos/faturas/') && url.includes('/pdf')) return true;
    if (ct.includes('application/pdf') && !url.startsWith('blob:')) return true;
    if (ct.includes('application/json')) return true;

    return false;
  } catch {
    return false;
  }
}

async function hasNoInvoicesMessage(page) {
  const selectors = ['mat-card#sem-fatura', 'mat-card.sem-fatura', '#sem-fatura'];

  for (const sel of selectors) {
    try {
      const el = page.locator(sel).first();
      if (await el.isVisible({ timeout: 800 })) return true;
    } catch {}
  }

  try {
    for (const f of page.frames()) {
      for (const sel of selectors) {
        try {
          const el = f.locator(sel).first();
          if (await el.isVisible({ timeout: 800 })) return true;
        } catch {}
      }
    }
  } catch {}

  return false;
}

async function openFaturasE2Via(page, accountName) {
  const timeoutMs = 60000;
  const start = Date.now();
  let card = null;

  while (Date.now() - start < timeoutMs) {
    card = page.locator('mat-card:has(span.card-text:has-text("Faturas e 2ª via de faturas"))').first()
      .or(page.locator('mat-card:has-text("Faturas e 2ª via de faturas")').first());
    try {
      if (await card.count()) {
        await card.waitFor({ state: 'visible', timeout: 1500 });
        break;
      }
    } catch {}
    await sleep(350);
  }

  if (!card || !(await card.count())) {
    await dumpDebug(page, accountName, 'faturas_card_not_found');
    throw new Error('Não encontrei o card "Faturas e 2ª via de faturas".');
  }

  await waitForUiStable(page, 45000);
  await card.scrollIntoViewIfNeeded().catch(() => {});
  await card.click({ timeout: 20000 }).catch(() => card.click({ timeout: 20000, force: true }));

  await waitForUiStable(page, 45000);

  const btnMaisOpcoes = page.getByRole('button', { name: /mais opções/i }).first()
    .or(page.locator('button:has-text("MAIS OPÇÕES")').first());

  const waitBtn = btnMaisOpcoes.waitFor({ state: 'visible', timeout: 25000 }).then(() => 'HAS_BTN').catch(() => null);

  const waitSemFatura = (async () => {
    const t0 = Date.now();
    while (Date.now() - t0 < 25000) {
      if (await hasNoInvoicesMessage(page)) return 'SEM_FATURA';
      await sleep(300);
    }
    return null;
  })();

  const result = await Promise.race([waitBtn, waitSemFatura]);

  if (result === 'SEM_FATURA') return false;

  if (!result) {
    if (await hasNoInvoicesMessage(page)) return false;
    await dumpDebug(page, accountName, 'faturas_page_not_loaded');
    throw new Error('Cliquei em "Faturas e 2ª via", mas não detectei a tela de faturas.');
  }

  return true;
}

async function openMenuOpcoesFaturaVisualizar(page, accountName) {
  await waitForUiStable(page, 45000);

  const btnMaisOpcoes = page.getByRole('button', { name: /mais opções/i }).first()
    .or(page.locator('button:has-text("MAIS OPÇÕES")').first());

  await btnMaisOpcoes.waitFor({ state: 'visible', timeout: 60000 });
  await btnMaisOpcoes.scrollIntoViewIfNeeded().catch(() => {});
  await btnMaisOpcoes.click({ timeout: 20000 }).catch(() => btnMaisOpcoes.click({ timeout: 20000, force: true }));

  const anyPanel = page.locator('div.mat-menu-panel[role="menu"]').filter({
    has: page.locator('button[role="menuitem"]')
  }).last();

  await anyPanel.waitFor({ state: 'visible', timeout: 20000 }).catch(async () => {
    await dumpDebug(page, accountName, 'menu_panel_not_visible');
    throw new Error('Menu do "MAIS OPÇÕES" não abriu.');
  });

  const optFatura = anyPanel.locator('button[role="menuitem"]').filter({ hasText: /opções de fatura/i }).first();
  await optFatura.waitFor({ state: 'visible', timeout: 20000 });
  await optFatura.click({ timeout: 20000 }).catch(() => optFatura.click({ timeout: 20000, force: true }));

  const subPanel = page.locator('div.mat-menu-panel[role="menu"]').filter({
    has: page.locator('button[role="menuitem"]')
  }).last();

  await subPanel.waitFor({ state: 'visible', timeout: 20000 }).catch(async () => {
    await dumpDebug(page, accountName, 'submenu_panel_not_visible');
    throw new Error('Submenu de "Opções de fatura" não ficou visível.');
  });

  const visualizarItem = subPanel.locator('button[role="menuitem"]').filter({ hasText: /^\s*visualizar\s*$/i }).first()
    .or(subPanel.locator('button[role="menuitem"]').filter({ hasText: /visualizar/i }).first());

  await visualizarItem.waitFor({ state: 'visible', timeout: 20000 });
  await visualizarItem.click({ timeout: 20000 }).catch(() => visualizarItem.click({ timeout: 20000, force: true }));

  await clickSwalOkIfVisible(page, 15000);
  await waitForUiStable(page, 45000);
  return true;
}

async function selectRadioNaoEstouComFatura(page, accountName) {
  await waitForUiStable(page, 60000);

  const rb = page.locator('mat-radio-button').filter({ hasText: /Não\s*Estou\s*Com\s*A\s*Fatura\s*Em\s*Mãos/i }).first();
  const input = rb.locator('input.mat-radio-input[type="radio"]').first();

  if ((await rb.count()) === 0) {
    await dumpDebug(page, accountName, 'radio_label_not_found');
    throw new Error('Não encontrei o radio "Não estou com a fatura em mãos".');
  }

  await rb.scrollIntoViewIfNeeded().catch(() => {});
  await rb.click({ timeout: 15000 }).catch(() => rb.click({ timeout: 15000, force: true }));

  if (!(await input.isChecked().catch(() => false))) {
    await rb.evaluate((el) => {
      const inp = el.querySelector('input.mat-radio-input[type="radio"]');
      if (inp) {
        inp.checked = true;
        inp.dispatchEvent(new Event('input', { bubbles: true }));
        inp.dispatchEvent(new Event('change', { bubbles: true }));
        inp.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      }
    }).catch(() => {});
  }

  if (!(await input.isChecked().catch(() => false))) {
    await dumpDebug(page, accountName, 'radio_select_failed');
    throw new Error('Não consegui marcar o radio desejado.');
  }

  return true;
}

async function clickVisualizarAndUploadPdf(page, accountName, account, clientCode) {
  await waitForUiStable(page, 60000);

  const btn = page.locator('button[title="Visualizar"]').first()
    .or(page.locator('button:has-text("VISUALIZAR")').first());

  if ((await btn.count()) === 0) {
    await dumpDebug(page, accountName, 'btn_visualizar_not_found');
    throw new Error('Não encontrei o botão VISUALIZAR.');
  }

  const candidates = [];
  const onResponse = (resp) => {
    try {
      if (!resp.ok()) return;
      if (!looksRelevantResponse(resp)) return;
      candidates.push(resp);
    } catch {}
  };
  page.on('response', onResponse);

  const popupPromise = page.waitForEvent('popup', { timeout: 45000 }).catch(() => null);

  await btn.scrollIntoViewIfNeeded().catch(() => {});
  await btn.click({ timeout: 20000 }).catch(() => btn.click({ timeout: 20000, force: true }));

  const popup = await popupPromise;
  if (popup) await popup.waitForLoadState('domcontentloaded').catch(() => {});

  await sleep(4500);
  page.off('response', onResponse);

  const apiPdfJson = candidates.find((r) => {
    const url = String(r.url() || '').toLowerCase();
    const ct = String(r.headers()?.['content-type'] || '').toLowerCase();
    return url.includes('apiseprd.neoenergia.com') && url.includes('/servicos/faturas/') && url.includes('/pdf') && ct.includes('application/json');
  });

  if (!apiPdfJson) {
    if (popup && !popup.isClosed()) await popup.close().catch(() => {});
    await page.bringToFront().catch(() => {});
    await dumpDebug(page, accountName, `no_api_pdf_json_${clientCode}`);
    throw new Error('Não encontrei a resposta JSON do endpoint de PDF.');
  }

  const rawBuf = await apiPdfJson.body().catch(() => null);
  if (!rawBuf || rawBuf.length < 10) {
    if (popup && !popup.isClosed()) await popup.close().catch(() => {});
    await page.bringToFront().catch(() => {});
    throw new Error('Resposta JSON do endpoint veio vazia.');
  }

  const raw = rawBuf.toString('utf-8');
  let json = null;
  try { json = JSON.parse(raw); } catch { json = null; }
  if (!json) {
    if (popup && !popup.isClosed()) await popup.close().catch(() => {});
    await page.bringToFront().catch(() => {});
    throw new Error('Não foi possível parsear o JSON do endpoint de PDF.');
  }

  const monthKey = getMonthKey(new Date());

  const b64 = findPdfBase64InJson(json);
  if (b64) {
    const pdfBuf = Buffer.from(b64, 'base64');
    if (isPdfBytes(pdfBuf)) {
      const uploaded = await uploadPdfToSupabaseStorage(pdfBuf, {
        monthKey,
        accountName: account.name,
        clientCode,
      });

      if (popup && !popup.isClosed()) await popup.close().catch(() => {});
      await page.bringToFront().catch(() => {});
      await clickSwalOkIfVisible(page, 15000).catch(() => {});
      return uploaded;
    }
  }

  const pdfUrl = findPdfUrlInJson(json);
  if (pdfUrl) {
    const resp2 = await page.request.get(pdfUrl, {
      headers: { 'Accept': 'application/pdf,application/octet-stream,*/*' },
      timeout: 120000
    }).catch(() => null);

    if (resp2 && resp2.ok()) {
      const pdfBuf = await resp2.body().catch(() => null);
      if (pdfBuf && isPdfBytes(pdfBuf)) {
        const uploaded = await uploadPdfToSupabaseStorage(pdfBuf, {
          monthKey,
          accountName: account.name,
          clientCode,
        });

        if (popup && !popup.isClosed()) await popup.close().catch(() => {});
        await page.bringToFront().catch(() => {});
        await clickSwalOkIfVisible(page, 15000).catch(() => {});
        return uploaded;
      }
    }
  }

  if (popup && !popup.isClosed()) await popup.close().catch(() => {});
  await page.bringToFront().catch(() => {});
  await dumpDebug(page, accountName, `pdf_not_resolved_${clientCode}`);
  throw new Error('Não consegui extrair PDF (base64/url) do JSON.');
}

// ===================== Paginação =====================
function getNextPageAnchor(page) {
  return page.locator('a.page-link[aria-label="Next"]').first()
    .or(page.locator('a[aria-label="Next"].page-link').first())
    .or(page.locator('a.page-link:has-text("Próximo")').first())
    .or(page.locator('a[aria-label="Next"]').first());
}

async function hasNextPage(page) {
  const a = getNextPageAnchor(page);
  if (!(await a.count())) return false;

  const disabled = await a.evaluate((el) => {
    const aria = (el.getAttribute('aria-disabled') || '').toLowerCase();
    const cls = (el.getAttribute('class') || '').toLowerCase();
    const parentCls = (el.parentElement?.getAttribute('class') || '').toLowerCase();
    const pe = (getComputedStyle(el).pointerEvents || '').toLowerCase();

    if (aria === 'true') return true;
    if (cls.includes('disabled')) return true;
    if (parentCls.includes('disabled')) return true;
    if (pe === 'none') return true;

    return false;
  }).catch(() => true);

  return !disabled;
}

async function goToNextPage(page, accountName) {
  const a = getNextPageAnchor(page);
  if (!(await a.count())) return false;

  let beforeFirst = '';
  try {
    const before = await getVisibleCodesOnPage(page, accountName);
    beforeFirst = before[0] || '';
  } catch {}

  await a.scrollIntoViewIfNeeded().catch(() => {});
  await a.click({ timeout: 15000 }).catch(() => a.click({ force: true }));

  await waitForUiStable(page, 60000);

  const start = Date.now();
  while (Date.now() - start < 45000) {
    try {
      const after = await getVisibleCodesOnPage(page, accountName);
      const afterFirst = after[0] || '';
      if (!beforeFirst && after.length > 0) break;
      if (afterFirst && afterFirst !== beforeFirst) break;
    } catch {}
    await sleep(400);
  }

  return true;
}

// ===================== Orquestração principal =====================
async function runFlow(page, account) {
  const accountName = account?.name || 'SEM_CONTA';

  await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' });
  await maybeAcceptCookies(page, accountName);
  await waitForUiStable(page, 45000);

  page = await clickLoginAndCapturePopup(page);
  await maybeAcceptCookies(page, accountName);
  await waitForUiStable(page, 45000);

  // Preenche login de forma robusta
  await fillDocumentRobust(page, account.document, accountName);
  await fillPasswordRobust(page, account.password, accountName);
  await clickEntrarRobust(page, accountName);

  if (await captchaVisible(page)) {
    console.log(`[${accountName}] reCAPTCHA visível. Resolva manualmente (se estiver com UI).`);
    if (!HEADLESS) {
      await waitForTerminalEnter('Depois de resolver e ver a tela avançar, pressione ENTER para continuar... ');
    } else {
      await dumpDebug(page, accountName, 'captcha_visible_headless');
      throw new Error('reCAPTCHA visível em headless. Necessário ajustar estratégia/conta ou rodar com solução alternativa.');
    }
  }

  await maybeAcceptCookies(page, accountName);
  await waitForUiStable(page, 45000);

  // Seleção de estado + filtro ligada
  const stateFrame = await waitForStateSelection(page, accountName);
  await selectSaoPauloInFrame(stateFrame, page, accountName);

  await filterSelectLigada(page, accountName);
  await waitForImoveisList(page, accountName, 60000);

  // Filtra UCs elegíveis hoje
  const targetsAll = (account.targets || []).filter(t => t?.uc_code);
  const today = new Date();

  const targetsToday = targetsAll
    .filter(t => shouldRunUcToday(t, today))
    .map(t => String(t.uc_code).trim())
    .filter(Boolean);

  if (targetsToday.length === 0) {
    console.log(`[SKIP] "${accountName}": sem UCs elegíveis hoje (dia_fatura/tolerância).`);
    return true;
  }

  const targetsSet = new Set(targetsToday);
  const doneSet = new Set();

  // Skip se já tem no storage
  for (const code of targetsSet) {
    const monthKey = getMonthKey(new Date());
    const exists = await storagePdfAlreadyExists({
      monthKey,
      accountName: account.name,
      clientCode: code,
    });
    if (exists) {
      doneSet.add(code);
      console.log(`[SKIP][JÁ TEM NO STORAGE] ${code}`);
    }
  }

  let pageIndex = 1;

  while (true) {
    if (doneSet.size >= targetsSet.size) {
      console.log(`[${accountName}] Todos os targets já processados. Encerrando.`);
      break;
    }

    console.log(`\n[debug] Página ${pageIndex} de imóveis...`);

    const visibleCodes = await getVisibleCodesOnPage(page, accountName);
    const toProcessHere = visibleCodes.filter(c => targetsSet.has(c) && !doneSet.has(c));

    // Processa somente os codes que estão visíveis nessa página
    for (const clientCode of toProcessHere) {
      try {
        const monthKey = getMonthKey(new Date());

        const exists = await storagePdfAlreadyExists({
          monthKey,
          accountName: account.name,
          clientCode,
        });

        if (exists) {
          doneSet.add(clientCode);
          console.log(`[SKIP][JÁ TEM NO STORAGE] ${clientCode}`);
          continue;
        }

        console.log(`\n[UC] ${clientCode} abrindo...`);

        const opened = await clickImovelByCodigo(page, accountName, clientCode);

        if (!opened) {
          console.log(`[WARN][NÃO ACHEI NO UI] ${clientCode} (página ${pageIndex}). Vou seguir para as próximas páginas.`);
          continue;
        }

        await waitForUiStable(page, 45000);

        // Abre "Faturas e 2ª via"
        const hasFaturas = await openFaturasE2Via(page, accountName);
        if (!hasFaturas) {
          console.log(`[WARN][SEM FATURA] ${clientCode}: não há fatura emitida`);
          doneSet.add(clientCode); // considera "processado" para não travar
          await page.goBack({ waitUntil: 'domcontentloaded' }).catch(() => {});
          await waitForUiStable(page, 45000);
          await filterSelectLigada(page, accountName).catch(() => {});
          continue;
        }

        // Menu -> Opções de fatura -> Visualizar
        await openMenuOpcoesFaturaVisualizar(page, accountName);
        await selectRadioNaoEstouComFatura(page, accountName);

        // Visualizar e subir
        const uploaded = await clickVisualizarAndUploadPdf(page, accountName, account, clientCode);
        console.log(`[OK] ${clientCode} -> storage://${uploaded.bucket}/${uploaded.objectKey}`);

        doneSet.add(clientCode);

        // Volta para lista (melhor esforço)
        await page.goBack({ waitUntil: 'domcontentloaded' }).catch(() => {});
        await waitForUiStable(page, 45000);
        await filterSelectLigada(page, accountName).catch(() => {});
        await waitForImoveisList(page, accountName, 60000);
      } catch (e) {
        console.log(`[WARN][ERRO UC] ${clientCode}: ${e?.message || e}`);
        await dumpDebug(page, accountName, `uc_fail_${clientCode}`).catch(() => {});
        // Importante: NÃO derruba a conta inteira; segue para o próximo code visível
        try {
          await page.goto(MEUS_IMOVEIS_URL, { waitUntil: 'domcontentloaded' }).catch(() => {});
          await waitForUiStable(page, 45000);
          await filterSelectLigada(page, accountName).catch(() => {});
          await waitForImoveisList(page, accountName, 60000);
        } catch {}
      }
    }

    if (doneSet.size >= targetsSet.size) {
      console.log(`[${accountName}] Todos os targets foram processados. Encerrando.`);
      break;
    }

    const next = await hasNextPage(page).catch(() => false);
    console.log(`[debug] Próxima página disponível? ${next ? 'SIM' : 'NÃO'}`);

    if (!next) {
      const missing = [...targetsSet].filter(x => !doneSet.has(x));
      console.log(`[debug] Última página atingida. Targets pendentes (${missing.length}): ${missing.join(', ')}`);
      break;
    }

    await goToNextPage(page, accountName);
    pageIndex += 1;
    await filterSelectLigada(page, accountName).catch(() => {});
  }

  return true;
}

// ===================== MAIN =====================
(async () => {
  const accounts = await loadAccountsFromSupabase();

  if (!accounts.length) {
    console.log('[debug] Nenhum cadastro encontrado no Supabase. Encerrando.');
    process.exit(0);
  }

  const browser = await chromium.launch({ headless: HEADLESS, slowMo: SLOW_MO });
  const openContexts = [];

  const monthKey = getMonthKey(new Date());
  ensureDir(DOWNLOADS_ROOT);
  ensureDir(DEBUG_ROOT);

  for (const acc of accounts) {
    if (!acc?.name || !acc?.document || !acc?.password) {
      console.log(`[SKIP] Cadastro inválido (faltando nome/cpf_cnpj/senha). id=${acc?.id || 'N/A'}`);
      continue;
    }

    const targetsAll = (acc.targets || []).filter(t => t?.uc_code);
    const targetsToday = targetsAll.filter(t => shouldRunUcToday(t, new Date()));
    if (targetsAll.length > 0 && targetsToday.length === 0) {
      console.log(`[SKIP] "${acc.name}": sem UCs elegíveis hoje (dia_fatura/tolerância).`);
      continue;
    }

    const context = await browser.newContext({ acceptDownloads: true });
    const page = await context.newPage();

    await context.addInitScript(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    });

    try {
      console.log(`\n[${acc.name}] iniciando...`);
      await runFlow(page, acc);
      console.log(`\n[${acc.name}] OK: fluxo executado.`);

      if (KEEP_OPEN) openContexts.push(context);
      else await context.close().catch(() => {});
    } catch (e) {
      console.error(`\n[${acc.name}] ERRO FATAL:`, e?.message || e);
      await dumpDebug(page, acc.name, 'fatal').catch(() => {});
      if (KEEP_OPEN) openContexts.push(context);
      else await context.close().catch(() => {});
    }
  }

  if (KEEP_OPEN) {
    if (KEEP_OPEN_MS > 0) {
      console.log(`[debug] KEEP_OPEN=true. Mantendo navegador aberto por ${KEEP_OPEN_MS} ms...`);
      await sleep(KEEP_OPEN_MS);
    } else {
      console.log('[debug] KEEP_OPEN=true. Navegador ficará aberto até você pressionar ENTER no terminal.');
      await waitForTerminalEnter('Pressione ENTER para encerrar e fechar o navegador... ');
    }

    for (const ctx of openContexts) {
      await ctx.close().catch(() => {});
    }
  }

  await browser.close().catch(() => {});

  // ===================== Rodar PYTHON ao final =====================
  try {
    const mesRef = monthKey;
    const bucket = SUPABASE_STORAGE_BUCKET;

    console.log(`\n[PY] Rodando /app/leitor_fatura_elektroneoenergia.py ao final...`);
    console.log(`[PY] args: ${JSON.stringify(['--mes-ref', mesRef, '--bucket', bucket])}`);

    const r = await runPythonScript(
      './leitor_fatura_elektroneoenergia.py',
      ['--mes-ref', mesRef, '--bucket', bucket],
      { env: process.env }
    );

    if (r.stdout?.trim()) console.log('[PY][STDOUT]\n' + r.stdout.trim());
    if (r.stderr?.trim()) console.error('[PY][STDERR]\n' + r.stderr.trim());
    console.log('[PY][OK] Finalizado.');
  } catch (e) {
    console.error('\n[PY][ERRO]', e?.message || e);
  }
})();
