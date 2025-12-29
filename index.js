import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { chromium } from 'playwright';
import { spawn } from 'child_process';
import { createClient } from '@supabase/supabase-js';

// ===================== ENV obrigatórios =====================
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || '';

const SUPABASE_ACCOUNTS_TABLE = process.env.SUPABASE_ACCOUNTS_TABLE || 'cadastros';
const SUPABASE_TARGETS_TABLE = process.env.SUPABASE_TARGETS_TABLE || 'cadastro_targets';

const SUPABASE_STORAGE_BUCKET = process.env.SUPABASE_STORAGE_BUCKET || 'faturas-elektro';
const SUPABASE_STORAGE_PUBLIC = (process.env.SUPABASE_STORAGE_PUBLIC || 'false').toLowerCase() === 'true';

const BASE_URL = process.env.NEOENERGIA_BASE_URL || 'https://agenciavirtual.neoenergia.com/';
const HEADLESS = (process.env.HEADLESS || 'false').toLowerCase() === 'true';

const KEEP_OPEN = (process.env.KEEP_OPEN || 'false').toLowerCase() === 'true';
const KEEP_OPEN_MS = Number(process.env.KEEP_OPEN_MS || '0');

const DEBUG_DIR = process.env.DEBUG_DIR || path.join(process.cwd(), 'debug');
const MEUS_IMOVEIS_URL = `${BASE_URL}#/home/meus-imoveis`;

if (!SUPABASE_URL) throw new Error('Defina SUPABASE_URL no .env');
const SUPABASE_KEY = SUPABASE_SERVICE_ROLE_KEY || SUPABASE_ANON_KEY;
if (!SUPABASE_KEY) throw new Error('Defina SUPABASE_SERVICE_ROLE_KEY (recomendado) ou SUPABASE_ANON_KEY no .env');

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false },
});

// ===================== Helpers =====================
function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }
function safeFileName(s) { return String(s || '').replace(/[^\w.-]+/g, '_'); }
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

// ===================== Supabase: leitura de contas/targets =====================
function clampDay(d) {
  const n = Number(d);
  if (!Number.isFinite(n)) return null;
  if (n < 1) return 1;
  if (n > 31) return 31;
  return n;
}

/**
 * Se dia_fatura NULL => processa sempre
 * Se dia_fatura preenchido => processa apenas se hoje estiver em:
 * [dia_fatura - tolerancia_dias, dia_fatura + tolerancia_dias]
 */
function shouldRunUcToday(target, today = new Date()) {
  const dia = clampDay(target?.dia_fatura);
  if (!dia) return true;

  const tol = clampDay(target?.tolerancia_dias) ?? 2;
  const nowDay = today.getDate();

  const start = clampDay(dia - tol);
  const end = clampDay(dia + tol);

  return nowDay >= start && nowDay <= end;
}

async function loadAccountsFromSupabase() {
  const { data: cadastros, error: err1 } = await supabase
    .from(SUPABASE_ACCOUNTS_TABLE)
    .select('id, nome, cpf_cnpj, senha, ativo')
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
      uc_code: onlyDigits(t.uc_code || ''),
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

// ===================== Debug =====================
async function safeScreenshot(page, filePath) {
  try {
    if (page && !page.isClosed()) {
      await page.screenshot({ path: filePath, fullPage: true });
      return true;
    }
  } catch {}
  return false;
}

async function dumpDebug(page, outDir, prefix) {
  ensureDir(outDir);
  const ts = Date.now();
  const shot = path.join(outDir, `${prefix}_${ts}.png`);
  const html = path.join(outDir, `${prefix}_${ts}.html`);

  await safeScreenshot(page, shot);
  try { if (page && !page.isClosed()) fs.writeFileSync(html, await page.content(), 'utf-8'); } catch {}
  return { shot, html };
}

// ===================== UI helpers =====================
async function maybeAcceptCookies(page) {
  const btns = [
    page.getByRole('button', { name: /aceitar|concordar|accept|ok/i }).first(),
    page.locator('button:has-text("Aceitar")').first(),
    page.locator('button:has-text("Concordo")').first()
  ];
  for (const b of btns) {
    try {
      if (await b.count()) { await b.click({ timeout: 2000 }); break; }
    } catch {}
  }
}

async function waitForUiStable(page, timeoutMs = 45000) {
  await page.waitForLoadState('domcontentloaded', { timeout: timeoutMs }).catch(() => {});
  const start = Date.now();
  let lastH = -1;
  let stableCount = 0;

  while (Date.now() - start < timeoutMs) {
    const h = await page.evaluate(() => document.body ? document.body.scrollHeight : 0).catch(() => 0);
    if (h === lastH) stableCount += 1; else stableCount = 0;
    lastH = h;

    const hasSpinner = await page.evaluate(() => {
      const sel = [
        'mat-progress-spinner',
        '.mat-progress-spinner',
        '.mat-mdc-progress-spinner',
        '.loading',
        '.spinner',
        '.ngx-spinner'
      ].join(',');
      return !!document.querySelector(sel);
    }).catch(() => false);

    if (!hasSpinner && stableCount >= 2) return true;
    await sleep(400);
  }
  return true;
}

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

async function captchaVisible(page) {
  const iframe = page.locator('iframe[src*="recaptcha"], iframe[title*="reCAPTCHA" i]').first();
  try { return await iframe.isVisible({ timeout: 1500 }); } catch { return false; }
}

// ===================== Login flow =====================
async function clickLoginAndCapturePopup(page) {
  const loginCandidate = page
    .getByRole('link', { name: /login|entrar|acessar/i }).first()
    .or(page.getByRole('button', { name: /login|entrar|acessar/i }).first())
    .or(page.locator('a:has-text("Entrar"), button:has-text("Entrar")').first());

  if (!(await loginCandidate.count())) return page;

  const popupPromise = page.waitForEvent('popup', { timeout: 7000 }).catch(() => null);
  await loginCandidate.click({ timeout: 15000 }).catch(() => {});
  const popup = await popupPromise;

  if (popup) {
    await popup.waitForLoadState('domcontentloaded').catch(() => {});
    return popup;
  }
  await page.waitForLoadState('domcontentloaded').catch(() => {});
  return page;
}

async function findLoginScope(page) {
  for (const f of page.frames()) {
    try {
      const hasPass = (await f.locator('input[type="password"]').count()) > 0;
      const hasDoc = (await f.locator(
        'input[name*="cpf" i],input[id*="cpf" i],input[name*="cnpj" i],input[id*="cnpj" i],' +
        'input[placeholder*="CPF" i],input[placeholder*="CNPJ" i],input[aria-label*="CPF" i],input[aria-label*="CNPJ" i],' +
        'input[type="tel"],input[type="text"]'
      ).count()) > 0;
      if (hasPass && hasDoc) return f;
    } catch {}
  }
  return page.mainFrame();
}

async function fillDocument(scope, rawDocument, outDir, pageForDebug) {
  const doc = onlyDigits(rawDocument);
  const docInput = scope.locator(
    'input[name*="cpf" i],input[id*="cpf" i],input[name*="cnpj" i],input[id*="cnpj" i],' +
    'input[placeholder*="CPF" i],input[placeholder*="CNPJ" i],input[aria-label*="CPF" i],input[aria-label*="CNPJ" i],' +
    'input[type="tel"],input[type="text"]'
  ).first();

  try {
    await docInput.waitFor({ state: 'visible', timeout: 30000 });
    await docInput.click({ clickCount: 3 }).catch(() => {});
    await docInput.fill('');
    await docInput.type(doc, { delay: 80 });
    await docInput.press('Tab').catch(() => {});
  } catch {
    await dumpDebug(pageForDebug, outDir, 'doc_input_fail');
    throw new Error('Não consegui preencher o campo CPF/CNPJ.');
  }
}

async function fillPassword(scope, password, outDir, pageForDebug) {
  const passInput = scope.locator('input[type="password"]').first();
  try {
    await passInput.waitFor({ state: 'visible', timeout: 30000 });
    await passInput.click({ clickCount: 3 }).catch(() => {});
    await passInput.fill('');
    await passInput.type(String(password), { delay: 70 });
    await passInput.press('Tab').catch(() => {});
  } catch {
    await dumpDebug(pageForDebug, outDir, 'pass_input_fail');
    throw new Error('Não consegui preencher a senha.');
  }
}

async function clickEntrar(scope, page, outDir) {
  const btn = scope
    .locator('button.btn-neoprimary.w-50[title="Entrar"][type="button"]')
    .first()
    .or(scope.locator('button:has-text("ENTRAR")').first())
    .or(scope.locator('button[title="Entrar"]').first());

  if (!(await btn.count())) {
    await dumpDebug(page, outDir, 'no_enter_button');
    throw new Error('Não encontrei o botão ENTRAR.');
  }

  await btn.scrollIntoViewIfNeeded().catch(() => {});
  await btn.waitFor({ state: 'visible', timeout: 15000 });

  const disabled = await btn.evaluate(el => el.disabled === true).catch(() => false);
  if (disabled) {
    await dumpDebug(page, outDir, 'enter_disabled');
    throw new Error('Botão ENTRAR está desabilitado.');
  }

  await btn.click({ timeout: 15000 }).catch(async () => {
    await btn.dispatchEvent('click').catch(() => {});
  });

  await page.waitForLoadState('domcontentloaded', { timeout: 25000 }).catch(() => {});
  await page.waitForTimeout(500);
}

// ===================== Estado / Filtro =====================
async function findFrameWithStateCards(page) {
  for (const f of page.frames()) {
    try { if ((await f.locator('mat-card.card-estado').count()) > 0) return f; } catch {}
  }
  return page.mainFrame();
}

async function waitForStateSelection(page, outDir) {
  const timeoutMs = 30000;
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const f = await findFrameWithStateCards(page);
    try {
      const count = await f.locator('mat-card.card-estado').count();
      if (count > 0) return f;
    } catch {}
    await sleep(500);
  }
  await dumpDebug(page, outDir, 'state_selection_not_loaded');
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

async function waitForFilterSelect(page, outDir, timeoutMs = 45000) {
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
  await dumpDebug(page, outDir, 'filter_not_found');
  throw new Error('Não encontrei o filtro "Filtrar".');
}

async function selectSaoPauloInFrame(frame, page, outDir) {
  const spRegex = /São\s*Paulo/i;
  const spCard = frame.locator('mat-card.card-estado').filter({ hasText: spRegex }).first();

  try {
    await spCard.waitFor({ state: 'visible', timeout: 30000 });
    await spCard.scrollIntoViewIfNeeded().catch(() => {});
    await spCard.click({ timeout: 20000 }).catch(async () => {
      await spCard.dispatchEvent('click').catch(() => {});
    });
    await waitForFilterSelect(page, outDir, 60000);
  } catch {
    await dumpDebug(page, outDir, 'select_sao_paulo_fail');
    throw new Error('Não consegui clicar no card "São Paulo".');
  }
}

async function filterSelectLigada(page, outDir) {
  const { matSelect } = await waitForFilterSelect(page, outDir, 60000);

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
      await dumpDebug(page, outDir, 'ligada_option_not_found');
      throw new Error('Não encontrei a opção "Ligada" no overlay.');
    }

    await option.waitFor({ state: 'visible', timeout: 20000 });
    await option.click({ timeout: 15000 }).catch(() => option.click({ timeout: 15000, force: true }));

    await page.waitForTimeout(400);
  } catch (e) {
    await dumpDebug(page, outDir, 'filter_ligada_fail');
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

async function waitForImoveisList(page, outDir, timeoutMs = 60000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const f = await findFrameWithImoveisList(page);
    try {
      const n = await f.locator('div.box-imoveis').count();
      if (n > 0) return f;
    } catch {}
    await sleep(350);
  }
  await dumpDebug(page, outDir, 'imoveis_list_not_loaded');
  throw new Error('Lista de imóveis (box-imoveis) não carregou.');
}

async function getVisibleCodesOnPage(page, outDir) {
  const frame = await waitForImoveisList(page, outDir, 60000);

  // pega texto e normaliza para só dígitos
  const rawCodes = await frame.$$eval(
    'div.unidade-consumidora-value',
    els => els.map(e => (e.textContent || '').trim())
  );

  const codes = rawCodes
    .map(x => x.replace(/\s+/g, ' ').trim())
    .map(x => x.replace(/[^\d]/g, '')) // remove máscara/pontos/espaços
    .filter(Boolean);

  return codes;
}

async function clickImovelByCodigo(page, outDir, codigo) {
  const code = onlyDigits(codigo);
  const frame = await waitForImoveisList(page, outDir, 60000);

  // locator sempre (nada de ElementHandle), para não cair em "cardEl.locator is not a function"
  const codeLocator = frame.locator('div.unidade-consumidora-value').filter({
    hasText: new RegExp(`\\b${code}\\b`)
  }).first();

  if ((await codeLocator.count()) === 0) {
    await dumpDebug(page, outDir, `codigo_${code}_nao_encontrado`);
    throw new Error(`Código do cliente não encontrado na lista: ${code}`);
  }

  const card = codeLocator.locator('xpath=ancestor::div[contains(@class,"box-imoveis")]').first();

  if ((await card.count()) === 0) {
    await dumpDebug(page, outDir, `card_${code}_nao_encontrado`);
    throw new Error(`Card da UC não encontrado para: ${code}`);
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

  await dumpDebug(page, outDir, `codigo_${code}_enter_timeout`);
  throw new Error(`Cliquei no código ${code}, mas não detectei avanço de tela em tempo hábil.`);
}

// ===================== Storage =====================
function buildStorageKey({ monthKey, accountName, clientCode }) {
  return [
    safeFileName(monthKey),
    safeFileName(accountName),
    `${safeFileName(clientCode)}.pdf`
  ].join('/');
}

function isPdfBytes(buf) {
  if (!buf || buf.length < 5) return false;
  return buf[0] === 0x25 && buf[1] === 0x50 && buf[2] === 0x44 && buf[3] === 0x46; // %PDF
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

async function uploadPdfToSupabaseStorage(pdfBuf, { monthKey, accountName, clientCode }) {
  if (!isPdfBytes(pdfBuf)) throw new Error('Buffer não é PDF válido (%PDF não encontrado).');

  const objectKey = buildStorageKey({ monthKey, accountName, clientCode });

  const { error } = await supabase
    .storage
    .from(SUPABASE_STORAGE_BUCKET)
    .upload(objectKey, pdfBuf, {
      upsert: true,
      contentType: 'application/pdf',
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

// ===================== Faturas UI (Visualizar) =====================
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
  return false;
}

async function openFaturasE2Via(page, outDir) {
  const card = page.locator('mat-card:has(span.card-text:has-text("Faturas e 2ª via de faturas"))').first()
    .or(page.locator('mat-card:has-text("Faturas e 2ª via de faturas")').first());

  await waitForUiStable(page, 45000);
  await card.waitFor({ state: 'visible', timeout: 60000 }).catch(async () => {
    await dumpDebug(page, outDir, 'faturas_card_not_found');
    throw new Error('Não encontrei o card "Faturas e 2ª via de faturas".');
  });

  await card.scrollIntoViewIfNeeded().catch(() => {});
  await card.click({ timeout: 20000 }).catch(() => card.click({ timeout: 20000, force: true }));
  await waitForUiStable(page, 45000);

  // se não tem fatura, não quebra o fluxo da conta
  const btnMaisOpcoes = page.getByRole('button', { name: /mais opções/i }).first()
    .or(page.locator('button:has-text("MAIS OPÇÕES")').first());

  const gotBtn = await btnMaisOpcoes.waitFor({ state: 'visible', timeout: 25000 }).then(() => true).catch(() => false);
  if (!gotBtn) {
    if (await hasNoInvoicesMessage(page)) return false;
    await dumpDebug(page, outDir, 'faturas_page_not_loaded');
    throw new Error('Cliquei em "Faturas e 2ª via", mas não detectei a tela de faturas.');
  }
  return true;
}

async function openMenuOpcoesFaturaVisualizar(page, outDir) {
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
    await dumpDebug(page, outDir, 'menu_panel_not_visible');
    throw new Error('Menu do "MAIS OPÇÕES" não abriu.');
  });

  const optFatura = anyPanel.locator('button[role="menuitem"]').filter({ hasText: /opções de fatura/i }).first();
  await optFatura.waitFor({ state: 'visible', timeout: 20000 });
  await optFatura.click({ timeout: 20000 }).catch(() => optFatura.click({ timeout: 20000, force: true }));

  const subPanel = page.locator('div.mat-menu-panel[role="menu"]').filter({
    has: page.locator('button[role="menuitem"]')
  }).last();

  await subPanel.waitFor({ state: 'visible', timeout: 20000 }).catch(async () => {
    await dumpDebug(page, outDir, 'submenu_panel_not_visible');
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

async function selectRadioByLabelText(page, outDir, clientCode, labelTextRegex = /Não\s*Estou\s*Com\s*A\s*Fatura\s*Em\s*Mãos/i) {
  await waitForUiStable(page, 60000);

  const rb = page.locator('mat-radio-button').filter({ hasText: labelTextRegex }).first();
  const input = rb.locator('input.mat-radio-input[type="radio"]').first();

  if ((await rb.count()) === 0) {
    await dumpDebug(page, outDir, `radio_label_not_found_${clientCode}`);
    throw new Error('Não encontrei o mat-radio-button com o texto desejado.');
  }

  await rb.scrollIntoViewIfNeeded().catch(() => {});
  await rb.click({ timeout: 15000 }).catch(() => rb.click({ timeout: 15000, force: true }));

  const ok = await input.isChecked().catch(() => false);
  if (!ok) {
    await dumpDebug(page, outDir, `radio_select_failed_${clientCode}`);
    throw new Error('Não consegui marcar a opção de rádio.');
  }
  return true;
}

async function clickVisualizarAndUploadPdf(page, outDir, clientCode, accountName) {
  await waitForUiStable(page, 60000);

  const btn = page.locator('button[title="Visualizar"]').first()
    .or(page.locator('button:has-text("VISUALIZAR")').first());

  if ((await btn.count()) === 0) {
    await dumpDebug(page, outDir, 'btn_visualizar_not_found');
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
    await dumpDebug(page, outDir, `no_api_pdf_json_${clientCode}`);
    throw new Error('Não encontrei a resposta JSON do endpoint de PDF.');
  }

  const rawBuf = await apiPdfJson.body().catch(() => null);
  if (!rawBuf || rawBuf.length < 10) throw new Error('Resposta JSON do endpoint veio vazia.');

  let json = null;
  try { json = JSON.parse(rawBuf.toString('utf-8')); } catch { json = null; }
  if (!json) throw new Error('Não foi possível parsear o JSON do endpoint de PDF.');

  const b64 = findPdfBase64InJson(json);
  if (!b64) throw new Error('Não consegui extrair base64 do PDF do JSON.');

  const pdfBuf = Buffer.from(b64, 'base64');
  if (!isPdfBytes(pdfBuf)) throw new Error('Base64 extraído não gerou um PDF válido.');

  const monthKey = getMonthKey(new Date());
  const uploaded = await uploadPdfToSupabaseStorage(pdfBuf, {
    monthKey,
    accountName,
    clientCode,
  });

  if (popup && !popup.isClosed()) await popup.close().catch(() => {});
  await page.bringToFront().catch(() => {});
  await clickSwalOkIfVisible(page, 15000).catch(() => {});

  return uploaded;
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

async function goToNextPage(page, outDir) {
  const a = getNextPageAnchor(page);
  if (!(await a.count())) return false;

  let beforeFirst = '';
  try {
    const before = await getVisibleCodesOnPage(page, outDir);
    beforeFirst = before[0] || '';
  } catch {}

  await a.scrollIntoViewIfNeeded().catch(() => {});
  await a.click({ timeout: 15000 }).catch(() => a.click({ force: true }));
  await waitForUiStable(page, 60000);

  const start = Date.now();
  while (Date.now() - start < 45000) {
    const after = await getVisibleCodesOnPage(page, outDir).catch(() => []);
    const afterFirst = after[0] || '';
    if (!beforeFirst && after.length > 0) break;
    if (afterFirst && afterFirst !== beforeFirst) break;
    await sleep(400);
  }

  return true;
}

// ===================== Orquestração por conta =====================
async function runFlow(page, account, outDir) {
  await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' });
  await maybeAcceptCookies(page);

  page = await clickLoginAndCapturePopup(page);
  await maybeAcceptCookies(page);

  const scope = await findLoginScope(page);
  await fillDocument(scope, account.document, outDir, page);
  await fillPassword(scope, account.password, outDir, page);
  await clickEntrar(scope, page, outDir);

  if (await captchaVisible(page)) {
    console.log('[Cliente] reCAPTCHA visível. Resolva manualmente no navegador.');
    await waitForTerminalEnter('Depois de resolver e ver a tela avançar, pressione ENTER para continuar... ');
  }

  await maybeAcceptCookies(page);

  const stateFrame = await waitForStateSelection(page, outDir);
  await selectSaoPauloInFrame(stateFrame, page, outDir);

  await filterSelectLigada(page, outDir);
  await waitForImoveisList(page, outDir, 60000);

  // targets elegíveis hoje
  const today = new Date();
  const targetsAll = (account.targets || []).filter(t => t?.uc_code);
  const targetsToday = targetsAll.filter(t => shouldRunUcToday(t, today)).map(t => onlyDigits(t.uc_code)).filter(Boolean);

  if (targetsToday.length === 0) {
    console.log(`[SKIP] Conta "${account.name}" sem UCs elegíveis hoje (dia_fatura/tolerância).`);
    return true;
  }

  const targetsSet = new Set(targetsToday);
  const doneSet = new Set();

  let pageIndex = 1;
  while (true) {
    if (doneSet.size >= targetsSet.size) break;

    console.log(`\n[debug] Página ${pageIndex} de imóveis...`);
    const visibleCodes = await getVisibleCodesOnPage(page, outDir);

    const toProcessHere = visibleCodes.filter(c => targetsSet.has(c) && !doneSet.has(c));

    // Processa UC por UC com try/catch individual => não derruba conta
    for (const clientCode of toProcessHere) {
      if (doneSet.has(clientCode)) continue;

      // se já existe no Storage, pula
      const monthKey = getMonthKey(new Date());
      const exists = await storagePdfAlreadyExists({
        monthKey,
        accountName: account.name,
        clientCode,
      }).catch(() => false);

      if (exists) {
        doneSet.add(clientCode);
        console.log(`[SKIP][JÁ TEM NO STORAGE] ${clientCode}`);
        continue;
      }

      try {
        console.log(`[UC] ${clientCode} abrindo...`);
        await clickImovelByCodigo(page, outDir, clientCode);

        const okFaturas = await openFaturasE2Via(page, outDir);
        if (!okFaturas) {
          console.log(`[WARN][SEM FATURA] ${clientCode}`);
          doneSet.add(clientCode);
          await page.goto(MEUS_IMOVEIS_URL, { waitUntil: 'domcontentloaded' }).catch(() => {});
          await waitForUiStable(page, 45000);
          await filterSelectLigada(page, outDir).catch(() => {});
          await waitForImoveisList(page, outDir, 60000);
          continue;
        }

        await openMenuOpcoesFaturaVisualizar(page, outDir);
        await selectRadioByLabelText(page, outDir, clientCode);
        const uploaded = await clickVisualizarAndUploadPdf(page, outDir, clientCode, account.name);

        console.log(`[OK] ${clientCode} -> storage://${uploaded.bucket}/${uploaded.objectKey}`);
        doneSet.add(clientCode);

        // volta para lista
        await page.goto(MEUS_IMOVEIS_URL, { waitUntil: 'domcontentloaded' }).catch(() => {});
        await waitForUiStable(page, 45000);
        await filterSelectLigada(page, outDir).catch(() => {});
        await waitForImoveisList(page, outDir, 60000);
      } catch (e) {
        console.log(`[WARN][ERRO UC] ${clientCode}: ${e?.message || e}`);
        await dumpDebug(page, outDir, `uc_${clientCode}_erro`).catch(() => {});
        // tenta voltar para lista e seguir com a próxima UC
        await page.goto(MEUS_IMOVEIS_URL, { waitUntil: 'domcontentloaded' }).catch(() => {});
        await waitForUiStable(page, 45000);
        await filterSelectLigada(page, outDir).catch(() => {});
        await waitForImoveisList(page, outDir, 60000);
        continue;
      }
    }

    if (doneSet.size >= targetsSet.size) break;

    const next = await hasNextPage(page).catch(() => false);
    if (!next) {
      // não achou na lista até o fim
      const missing = [...targetsSet].filter(x => !doneSet.has(x));
      if (missing.length) {
        console.log(`[WARN] Última página. UCs não encontradas no UI (${missing.length}): ${missing.join(', ')}`);
      }
      break;
    }

    await goToNextPage(page, outDir);
    pageIndex += 1;

    // garante filtro (site às vezes reseta)
    await filterSelectLigada(page, outDir).catch(() => {});
  }

  return true;
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

// ===================== Main =====================
(async () => {
  ensureDir(DEBUG_DIR);

  const accounts = await loadAccountsFromSupabase();
  if (!accounts.length) {
    console.log('[debug] Nenhum cadastro encontrado no Supabase. Encerrando.');
    process.exit(0);
  }

  const browser = await chromium.launch({ headless: HEADLESS, slowMo: 60 });
  const openContexts = [];
  const monthKey = getMonthKey(new Date());

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

    const context = await browser.newContext({ acceptDownloads: false });
    const page = await context.newPage();

    await context.addInitScript(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    });

    const outDir = path.join(DEBUG_DIR, safeFileName(monthKey), safeFileName(acc.name));
    ensureDir(outDir);

    try {
      console.log(`\n[${acc.name}] iniciando...`);
      await runFlow(page, acc, outDir);
      console.log(`[${acc.name}] OK: fluxo executado.`);

      if (KEEP_OPEN) openContexts.push(context);
      else await context.close().catch(() => {});
    } catch (e) {
      console.error(`\n[${acc.name}] ERRO FATAL:`, e?.message || e);
      await dumpDebug(page, outDir, 'fatal').catch(() => {});
      console.error(`[${acc.name}] Debug salvo em: ${outDir}`);

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

  // ===================== Rodar Python ao final (lendo PDFs do Storage) =====================
  try {
    const pyScript = path.resolve('./leitor_fatura_elektroneoenergia.py');
    const args = ['--mes-ref', monthKey, '--bucket', SUPABASE_STORAGE_BUCKET];

    console.log(`\n[PY] Rodando ${pyScript} ao final...`);
    console.log(`[PY] args: ${JSON.stringify(args)}`);

    const r = await runPythonScript(pyScript, args, { env: process.env });

    if (r.stdout?.trim()) console.log('[PY][STDOUT]\n' + r.stdout.trim());
    if (r.stderr?.trim()) console.error('[PY][STDERR]\n' + r.stderr.trim());
    console.log('[PY][OK] Finalizado.');
  } catch (e) {
    console.error('\n[PY][ERRO]', e?.message || e);
  }
})();
