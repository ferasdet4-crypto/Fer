/**
 * Cloudflare Worker Telegram Bot (Bezsvitla / DTEK schedules) — FINAL FIXED
 * ✅ Multi-bot via /webhook/<TOKEN> + env.BOT_TOKENS
 * ✅ Persistent per-user state in KV (STATE_KV)
 * ✅ Global bot config in KV (ads/whitelist) — admin changes affect ALL users
 * ✅ Admin panel: ads (add/list/delete), ad timer (cooldown), whitelist, stats
 * ✅ Saved queues, alerts toggle, cron alerts
 * ✅ Fix: /start, /my, /admin work in groups (/start@botname)
 * ✅ Fix: safer editMessage with fallback sendMessage
 * ✅ Fix: retries for fetching city queues & schedule pages
 *
 * REQUIRED BINDINGS (Cloudflare):
 *  - KV namespace binding: STATE_KV
 *  - BOT_TOKENS: "token1,token2" OR JSON '["token1","token2"]'
 *  - ADMIN_ID: Telegram numeric chat id (string ok)
 *  - SPONSOR_LINK: https://...
 *
 * OPTIONAL ENV:
 *  - AD_INFO_LINK: https://... (button “📢 Тут може бути ваша реклама”)
 *  - UA_TZ_OFFSET_MIN (default 120)
 *  - STATE_TTL_SEC (default 60*60*24*45) 45 days
 *  - ALERT_MIN_BEFORE (default 20)
 *  - ALERT_WINDOW_MIN (default 6)      // cron window tolerance
 *  - ALERT_MAX_PER_CRON (default 300)  // safety cap
 *  - FETCH_TIMEOUT_MS (default 12000)
 *  - FETCH_RETRY_COUNT (default 3)
 *  - FETCH_RETRY_DELAY_MS (default 600)
 *
 * CRON:
 *  - Configure cron in Cloudflare (e.g. every 5 minutes). scheduled() sends alerts.
 */

const VERSION = "kv-cron-final-1.1.2";

// ===================== UTIL: SAFE JSON =====================
function jparse(s, fallback) { try { return JSON.parse(s); } catch { return fallback; } }
function jstringify(obj) { try { return JSON.stringify(obj); } catch { return "{}"; } }

function envNum(env, name, def) {
  const v = env?.[name];
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

function ttlSec(env) { return envNum(env, "STATE_TTL_SEC", 60 * 60 * 24 * 45); }
function uaOffsetMin(env) { return envNum(env, "UA_TZ_OFFSET_MIN", 120); }
function alertMinutesBefore(env) { return envNum(env, "ALERT_MIN_BEFORE", 20); }
function alertWindowMin(env) { return envNum(env, "ALERT_WINDOW_MIN", 6); }
function fetchTimeoutMs(env) { return envNum(env, "FETCH_TIMEOUT_MS", 12000); }
function fetchRetryCount(env) { return envNum(env, "FETCH_RETRY_COUNT", 3); }
function fetchRetryDelayMs(env) { return envNum(env, "FETCH_RETRY_DELAY_MS", 600); }
function alertMaxPerCron(env) { return envNum(env, "ALERT_MAX_PER_CRON", 300); }

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ===================== TOKENS / COMMANDS =====================
function parseTokens(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw.map(String);
  const s = String(raw).trim();
  if (s.startsWith("[") && s.endsWith("]")) {
    const arr = jparse(s, []);
    return Array.isArray(arr) ? arr.map(String) : [];
  }
  return s.split(",").map(x => x.trim()).filter(Boolean);
}

// For groups: "/start@MyBot arg" -> "/start"
function extractCommand(text) {
  const t = String(text || "").trim();
  if (!t.startsWith("/")) return null;
  const first = t.split(/\s+/)[0];
  const cmd = first.split("@")[0];
  return cmd.toLowerCase();
}

// ===================== KV KEYS =====================
function stKey(token, chatId) { return `${token}:${chatId}`; }
function stateKvKey(key) { return `st:${key}`; }

function cfgKey(token) { return `cfg:${token}`; }              // global config per bot token
function userRegKey(token, chatId) { return `user:${token}:${chatId}`; }
function usersCountKey(token) { return `users_count:${token}`; }

async function sha1Hex(text) {
  const data = new TextEncoder().encode(text);
  const buf = await crypto.subtle.digest("SHA-1", data);
  const arr = Array.from(new Uint8Array(buf));
  return arr.map(b => b.toString(16).padStart(2, "0")).join("");
}
function subKey(token, chatId, urlHash) { return `sub:${token}:${chatId}:${urlHash}`; }

// ===================== DEFAULTS =====================
function initDefaultAdItem() {
  return { id: "default", type: "text", text: "📢 Тут може бути твоя реклама. Натисни «Стати спонсором» 👇", createdAt: Date.now() };
}

function defaultUserState() {
  return {
    v: 2,
    cities: [],
    queues: [],
    selectedCity: null,
    selected: null,
    saved: [],
    // per-user ad pacing
    ad: { counter: 0, nextAtMs: 0 },
    admin: { mode: null, tmp: null },
    stats: { actions: 0 },
    alerts: { enabled: true }
  };
}

function defaultGlobalConfig(env) {
  return {
    v: 2,
    ads: {
      enabled: true,
      frequency: 3,            // every N actions (per-user counter)
      cooldownSec: 0           // minimum time between ads per user (0 = off)
    },
    items: [initDefaultAdItem()], // ad items list (0 = active)
    whitelist: [],                // array of chatIds (strings)
    updatedAt: Date.now(),
    adInfoLink: String(env?.AD_INFO_LINK || "").trim() || null
  };
}

function wlSet(cfg) { return new Set((cfg?.whitelist || []).map(String)); }
function setWhitelistFromSet(cfg, s) { cfg.whitelist = [...s].map(String); }

// ===================== KV IO =====================
async function loadUserState(env, key) {
  const raw = await env.STATE_KV.get(stateKvKey(key));
  if (!raw) return defaultUserState();
  const obj = jparse(raw, null);
  if (!obj || typeof obj !== "object") return defaultUserState();

  // fill
  if (!obj.cities) obj.cities = [];
  if (!obj.queues) obj.queues = [];
  if (!("selectedCity" in obj)) obj.selectedCity = null;
  if (!("selected" in obj)) obj.selected = null;
  if (!obj.saved) obj.saved = [];
  if (!obj.ad) obj.ad = { counter: 0, nextAtMs: 0 };
  if (!obj.admin) obj.admin = { mode: null, tmp: null };
  if (!obj.stats) obj.stats = { actions: 0 };
  if (!obj.alerts) obj.alerts = { enabled: true };

  return obj;
}

async function saveUserState(env, key, st) {
  await env.STATE_KV.put(stateKvKey(key), jstringify(st), { expirationTtl: ttlSec(env) });
}

async function loadGlobalConfig(env, token) {
  const raw = await env.STATE_KV.get(cfgKey(token));
  if (!raw) {
    const cfg = defaultGlobalConfig(env);
    await env.STATE_KV.put(cfgKey(token), jstringify(cfg), { expirationTtl: ttlSec(env) });
    return cfg;
  }
  const cfg = jparse(raw, null);
  if (!cfg || typeof cfg !== "object") {
    const def = defaultGlobalConfig(env);
    await env.STATE_KV.put(cfgKey(token), jstringify(def), { expirationTtl: ttlSec(env) });
    return def;
  }

  // migrations / fills
  if (!cfg.ads) cfg.ads = { enabled: true, frequency: 3, cooldownSec: 0 };
  if (typeof cfg.ads.enabled !== "boolean") cfg.ads.enabled = true;
  if (!Number.isFinite(Number(cfg.ads.frequency))) cfg.ads.frequency = 3;
  if (!Number.isFinite(Number(cfg.ads.cooldownSec))) cfg.ads.cooldownSec = 0;
  cfg.ads.frequency = Math.max(1, Math.min(20, Number(cfg.ads.frequency)));
  cfg.ads.cooldownSec = Math.max(0, Number(cfg.ads.cooldownSec));

  if (!Array.isArray(cfg.items) || !cfg.items.length) cfg.items = [initDefaultAdItem()];
  if (!Array.isArray(cfg.whitelist)) cfg.whitelist = [];
  if (!("adInfoLink" in cfg)) cfg.adInfoLink = String(env?.AD_INFO_LINK || "").trim() || null;

  return cfg;
}

async function saveGlobalConfig(env, token, cfg) {
  cfg.updatedAt = Date.now();
  await env.STATE_KV.put(cfgKey(token), jstringify(cfg), { expirationTtl: ttlSec(env) });
}

// ===================== USER COUNTER =====================
async function ensureUserRegistered(env, token, chatId) {
  const uKey = userRegKey(token, chatId);
  const exists = await env.STATE_KV.get(uKey);
  if (exists) return;

  await env.STATE_KV.put(uKey, "1", { expirationTtl: ttlSec(env) });

  const cKey = usersCountKey(token);
  const raw = await env.STATE_KV.get(cKey);
  const cur = Number(raw || 0);
  const next = Number.isFinite(cur) ? cur + 1 : 1;
  await env.STATE_KV.put(cKey, String(next), { expirationTtl: ttlSec(env) });
}

// ===================== TIME =====================
function uaLocalNow(env) {
  const offMin = uaOffsetMin(env);
  const nowUtcMs = Date.now();
  const localMs = nowUtcMs + offMin * 60_000;
  const d = new Date(localMs);
  const minutes = d.getUTCHours() * 60 + d.getUTCMinutes();
  return { nowUtcMs, localMs, offMin, minutes, dLocal: d };
}
function localDayStartMs(localMs) { return Math.floor(localMs / 86_400_000) * 86_400_000; }
function fmtDelta(min) {
  const m = Math.max(0, Math.floor(min));
  const h = Math.floor(m / 60);
  const r = m % 60;
  return `${h} год ${r} хв`;
}

// ===================== FETCH WITH RETRY =====================
async function fetchWithRetry(url, init, env) {
  const tries = fetchRetryCount(env);
  const delay = fetchRetryDelayMs(env);
  let lastErr = null;

  for (let i = 0; i < tries; i++) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), fetchTimeoutMs(env));
    try {
      const res = await fetch(url, { ...(init || {}), signal: ctrl.signal });
      if (res.ok) return res;
      lastErr = new Error(`HTTP ${res.status} for ${url}`);
      // small retry on 429/5xx
      if (res.status < 500 && res.status !== 429) return res;
    } catch (e) {
      lastErr = e;
    } finally {
      clearTimeout(t);
    }
    if (i < tries - 1) await sleep(delay);
  }
  throw lastErr || new Error("fetch failed");
}

// ===================== TELEGRAM API =====================
async function tgCall(token, method, payload, env) {
  try {
    const res = await fetchWithRetry(`https://api.telegram.org/bot${token}/${method}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload || {})
    }, env);
    const json = await res.json().catch(() => ({}));
    return json;
  } catch (e) {
    console.log("tgCall error", method, e);
    return { ok: false, description: String(e?.message || e) };
  }
}

async function sendMessage(token, chatId, text, replyMarkup, env) {
  const payload = { chat_id: chatId, text };
  if (replyMarkup) payload.reply_markup = replyMarkup;
  return tgCall(token, "sendMessage", payload, env);
}

async function editMessage(token, chatId, messageId, text, replyMarkup, env) {
  const payload = { chat_id: chatId, message_id: messageId, text };
  if (replyMarkup) payload.reply_markup = replyMarkup;
  return tgCall(token, "editMessageText", payload, env);
}

async function answerCallback(token, callbackQueryId, env) {
  return tgCall(token, "answerCallbackQuery", { callback_query_id: callbackQueryId }, env);
}

function tgOk(r) { return Boolean(r && r.ok === true); }

async function safeEditOrSend(token, chatId, messageId, text, replyMarkup, env) {
  const r = await editMessage(token, chatId, messageId, text, replyMarkup, env);
  if (tgOk(r)) return r;

  const desc = String(r?.description || "");
  // harmless: no changes
  if (desc.toLowerCase().includes("message is not modified")) return r;

  // fallback: send new message
  await sendMessage(token, chatId, text, replyMarkup, env);
  return r;
}

// ===================== UI KEYBOARDS =====================
function startKeyboard(env) {
  return {
    inline_keyboard: [
      [{ text: "⭐ Мої черги", callback_data: "my" }],
      [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]
    ]
  };
}

function mainQueueKeyboard(env, st) {
  const alertOn = st?.alerts?.enabled !== false;
  return {
    inline_keyboard: [
      [{ text: "🔄 Оновити", callback_data: "refresh" }],
      [{ text: "⭐ Зберегти", callback_data: "save" }],
      [{ text: alertOn ? "🔔 Алерти: увімкн." : "🔕 Алерти: вимкн.", callback_data: "alerts_toggle" }],
      [{ text: "📋 Мої черги", callback_data: "my" }],
      [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]
    ]
  };
}

function adButtons(env, cfg) {
  const sponsor = [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }];
  const link = cfg?.adInfoLink || String(env?.AD_INFO_LINK || "").trim() || env.SPONSOR_LINK;
  const info = [{ text: "📢 Тут може бути ваша реклама", url: link }];
  return { inline_keyboard: [sponsor, info] };
}

// ===================== BEZSVITLA: SEARCH / PARSE =====================
function normalizeBezUrl(u) {
  const s = String(u || "").trim();
  if (!s) return "https://bezsvitla.com.ua/";
  if (/^https?:\/\//i.test(s)) return s;
  if (s.startsWith("/")) return "https://bezsvitla.com.ua" + s;
  return "https://bezsvitla.com.ua/" + s;
}

function canonicalizeBezUrl(u) {
  try {
    const url = new URL(normalizeBezUrl(u));
    url.hash = "";
    url.search = "";
    // trim trailing slash (except root)
    let out = url.toString();
    if (out.endsWith("/") && url.pathname !== "/") out = out.slice(0, -1);
    return out;
  } catch {
    return normalizeBezUrl(u);
  }
}

function queueNameFromUrl(u) {
  const m = String(u).match(/cherha-(\d+)-(\d+)/i);
  if (!m) return "Черга";
  return `Черга ${m[1]}.${m[2]}`;
}

function extractQueueUrlsFromText(text, urlSet) {
  const html = String(text || "");
  // capture both absolute and relative URLs that contain cherha-<d>-<d>
  const re = /(?:https?:\/\/bezsvitla\.com\.ua)?(\/[^"'\s<>]*?cherha-(\d+)-(\d+)[^"'\s<>]*)/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const relOrFull = m[1];
    const abs = canonicalizeBezUrl(relOrFull);
    urlSet.add(abs);
  }
}

function uniqueQueuesFromUrlSet(urlSet) {
  const out = [];
  for (const u of urlSet) out.push({ name: queueNameFromUrl(u), url: u });
  // stable-ish sort by queue number
  out.sort((a, b) => {
    const ma = a.name.match(/(\d+)\.(\d+)/);
    const mb = b.name.match(/(\d+)\.(\d+)/);
    const a1 = ma ? Number(ma[1]) : 999, a2 = ma ? Number(ma[2]) : 999;
    const b1 = mb ? Number(mb[1]) : 999, b2 = mb ? Number(mb[2]) : 999;
    if (a1 !== b1) return a1 - b1;
    return a2 - b2;
  });
  return out;
}

function detectOnOff(fragment) {
  const s = String(fragment || "").toLowerCase();
  // OFF first (more specific)
  if (/icon[-_ ]?off\b|status[-_ ]?off\b|light[-_ ]?off\b|no[-_ ]?light\b|svitlo[-_ ]?off\b|bolt[-_ ]?slash\b|fa-bolt-slash\b|power[-_ ]?off\b|blackout\b/.test(s)) return false;
  if (/icon[-_ ]?on\b|status[-_ ]?on\b|light[-_ ]?on\b|has[-_ ]?light\b|svitlo[-_ ]?on\b|fa-lightbulb\b|lightbulb\b/.test(s)) return true;
  return null;
}

async function searchCities(query, env) {
  const url = "https://bezsvitla.com.ua/search-locality?q=" + encodeURIComponent(query);
  const res = await fetchWithRetry(url, {
    headers: {
      "Accept": "application/json",
      "X-Requested-With": "XMLHttpRequest",
      "User-Agent": "Mozilla/5.0",
      "Referer": "https://bezsvitla.com.ua/"
    }
  }, env);
  if (!res.ok) return [];
  const data = await res.json().catch(() => null);
  if (!Array.isArray(data)) return [];
  return data
    .filter(x => x && x.name && x.url)
    .map(x => ({ name: x.name, url: canonicalizeBezUrl(x.url) }))
    .slice(0, 8);
}
async function getQueuesFromCityUrl(cityUrl, env) {
  const absCityUrl = canonicalizeBezUrl(cityUrl);

  // 0) Fetch city page
  let html = "";
  try {
    const res = await fetchWithRetry(absCityUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "uk-UA,uk;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
      }
    }, env);
    if (!res.ok) return [];
    html = await res.text();
  } catch (e) {
    console.log("getQueuesFromCityUrl fetch city error:", e);
    return [];
  }

  const urlSet = new Set();

  // 1) Best effort: find queue URLs anywhere in city HTML (Kyiv часто ховає в script/data)
  extractQueueUrlsFromText(html, urlSet);
  if (urlSet.size) return uniqueQueuesFromUrlSet(urlSet);

  // 2) Fallback: crawl a limited number of subpages under the same city path (works for some cities / districts)
  let basePath = "";
  try { basePath = new URL(absCityUrl).pathname.replace(/\/$/, ""); } catch {}
  if (!basePath) return [];

  const hrefs = [...html.matchAll(/href="([^"]+)"/g)].map(m => m[1]);

  const subLinks = hrefs
    .filter(h => h && h.startsWith(basePath + "/") && !h.includes("#") && !/cherha-\d+-\d+/i.test(h))
    .slice(0, 40);

  const subUrls = [...new Set(subLinks.map(h => canonicalizeBezUrl(h)))];

  for (const u of subUrls) {
    try {
      const r = await fetchWithRetry(u, {
        headers: {
          "User-Agent": "Mozilla/5.0",
          "Accept-Language": "uk-UA,uk;q=0.9",
          "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
      }, env);
      if (!r.ok) continue;
      const h = await r.text();
      extractQueueUrlsFromText(h, urlSet);
      if (urlSet.size) break; // found something — stop crawling
    } catch {
      // ignore per-subpage errors
    }
  }

  if (!urlSet.size) return [];
  return uniqueQueuesFromUrlSet(urlSet);
}
function extractLiItems(html) {
  const blocks = [];
  const s = String(html || "");

  // 1) Try to parse explicit <li ...>time–time...</li> blocks (older layout)
  const liRe = /<li[^>]*>([\s\S]*?)<\/li>/gi;
  let m;
  while ((m = liRe.exec(s)) !== null) {
    const inner = m[1] || "";
    const tm = inner.match(/(\d{2}:\d{2})\s*[–-]\s*(\d{2}:\d{2})/);
    if (!tm) continue;

    const start = tm[1];
    const end = tm[2];

    // detect on/off by nearby classes/icons/text
    const on = detectOnOff(inner);

    blocks.push({ start, end, on });
  }

  if (blocks.length) return blocks;

  // 2) Newer layout: cards/table rows — find ALL time ranges, then infer on/off from nearby markup
  const timeRe = /(\d{2}:\d{2})\s*[–-]\s*(\d{2}:\d{2})/g;
  let lastKey = null;
  let lastPos = -9999;

  let t;
  while ((t = timeRe.exec(s)) !== null) {
    const start = t[1];
    const end = t[2];
    const pos = t.index;

    const fragStart = Math.max(0, pos - 160);
    const fragEnd = Math.min(s.length, pos + 260);
    const frag = s.slice(fragStart, fragEnd);

    const on = detectOnOff(frag);

    const key = `${start}-${end}-${on}`;
    if (key === lastKey && (pos - lastPos) < 40) continue; // skip near-duplicates
    lastKey = key;
    lastPos = pos;

    blocks.push({ start, end, on });
  }

  return blocks;
}
function extractTimeOnly(html) {
  const ms = [...html.matchAll(/(\d{2}:\d{2})\s*[–-]\s*(\d{2}:\d{2})/g)];
  return ms.map(m => ({ start: m[1], end: m[2], on: null }));
}
function toMin(t) {
  const [h, m] = String(t).split(":").map(Number);
  return h * 60 + m;
}
function normalizeBlock(b) {
  const startMin = toMin(b.start);
  let endMin = toMin(b.end);
  if (endMin < startMin) endMin += 24 * 60;
  return { ...b, startMin, endMin };
}
function findCurrentBlock(blocks, nowMin) {
  for (const b of blocks) if (nowMin >= b.startMin && nowMin < b.endMin) return b;
  return null;
}
function findNextBlock(blocks, nowMin) {
  for (const b of blocks) if (b.startMin > nowMin) return b;
  return null;
}
function fmtBlocks(arr) {
  if (!arr.length) return "❌ Нема даних";
  return arr.map(b => `${b.on === true ? "🟢" : b.on === false ? "🔴" : "🟡"} ${b.start} – ${b.end}`).join("\n");
}

async function fetchScheduleBlocks(url, env) {
  const res = await fetchWithRetry(url, {
    headers: { "User-Agent": "Mozilla/5.0", "Accept-Language": "uk-UA,uk;q=0.9" }
  }, env);
  if (!res.ok) return { ok: false, blocks: [] };
  const html = await res.text();
  const items = extractLiItems(html);
  const blocks = (items.length ? items : extractTimeOnly(html)).map(x => normalizeBlock(x));
  return { ok: true, blocks };
}

function computeStatus(blocks, nowMin) {
  let statusLine = "❓ Нема даних";
  let nextLine = "";
  let nextChangeMin = null;
  let nextChangeType = null;
  let nextChangeAtText = null;

  const current = findCurrentBlock(blocks, nowMin);

  if (current) {
    nextChangeMin = current.endMin;
    nextChangeAtText = current.end;
    if (current.on === true) {
      statusLine = "🟢 ЗАРАЗ Є СВІТЛО";
      nextLine = `⏰ Вимкнуть о ${current.end}\n⏳ Через ${fmtDelta(current.endMin - nowMin)}`;
      nextChangeType = "off";
    } else if (current.on === false) {
      statusLine = "🔴 ЗАРАЗ НЕМАЄ СВІТЛА";
      nextLine = `⏰ Увімкнуть о ${current.end}\n⏳ Через ${fmtDelta(current.endMin - nowMin)}`;
      nextChangeType = "on";
    } else {
      statusLine = "🟡 ЗАРАЗ: невідомо";
      nextLine = `⏳ До ${current.end}: ${fmtDelta(current.endMin - nowMin)}`;
      nextChangeType = "unknown";
    }
    return { statusLine, nextLine, nextChangeMin, nextChangeType, nextChangeAtText };
  }

  const next = findNextBlock(blocks, nowMin);
  if (next) {
    statusLine = "🔴 ЗАРАЗ НЕМА СВІТЛА";
    nextLine = `⏰ Наступна зміна о ${next.start}\n⏳ Через ${fmtDelta(next.startMin - nowMin)}`;
    nextChangeMin = next.startMin;
    nextChangeAtText = next.start;
    if (next.on === true) nextChangeType = "on";
    else if (next.on === false) nextChangeType = "off";
    else nextChangeType = "unknown";
    return { statusLine, nextLine, nextChangeMin, nextChangeType, nextChangeAtText };
  }

  return { statusLine, nextLine, nextChangeMin, nextChangeType, nextChangeAtText };
}

async function buildInfo(sel, env) {
  const { ok, blocks } = await fetchScheduleBlocks(sel.url, env);
  if (!ok || !blocks.length) {
    return { text: `📍 ${sel.cityName}\n🔌 ${sel.queueName}\n\n❌ Не вдалося завантажити сторінку черги.\nСпробуй ще раз (🔄 Оновити).`, meta: null };
  }

  const { minutes: nowMin } = uaLocalNow(env);
  const meta = computeStatus(blocks, nowMin);

  const today = blocks.slice(0, 12);
  const tomorrow = blocks.slice(12, 24);

  let text =
`📍 ${sel.cityName}
🔌 ${sel.queueName}
${meta.statusLine}
${meta.nextLine}

📊 СЬОГОДНІ:
${fmtBlocks(today)}`;

  if (tomorrow.length) text += `\n\n📅 ЗАВТРА:\n${fmtBlocks(tomorrow)}`;

  return { text: text.trim(), meta: { ...meta, nowMin } };
}

// ===================== ADS (GLOBAL CONFIG) =====================
function isAdmin(chatId, env) { return String(chatId) === String(env.ADMIN_ID); }

function hasAnyMedia(message) { return Boolean(message.photo || message.video || message.document); }

function mediaToAdItem(message, extraText) {
  const id = String(Date.now());
  const createdAt = Date.now();
  if (message.photo && message.photo.length) {
    const best = message.photo[message.photo.length - 1];
    return { id, type: "photo", file_id: best.file_id, text: extraText || "", createdAt };
  }
  if (message.video) {
    return { id, type: "video", file_id: message.video.file_id, text: extraText || "", createdAt };
  }
  if (message.document) {
    return { id, type: "document", file_id: message.document.file_id, text: extraText || "", createdAt };
  }
  return null;
}

async function sendAdItem(token, chatId, item, env, cfg) {
  const markup = adButtons(env, cfg);

  if (item.type === "text") {
    await sendMessage(token, chatId, item.text, markup, env);
    return;
  }

  const caption = item.text || "📢 Реклама";
  if (item.type === "photo") {
    await tgCall(token, "sendPhoto", { chat_id: chatId, photo: item.file_id, caption, reply_markup: markup }, env);
    return;
  }
  if (item.type === "video") {
    await tgCall(token, "sendVideo", { chat_id: chatId, video: item.file_id, caption, reply_markup: markup }, env);
    return;
  }
  if (item.type === "document") {
    await tgCall(token, "sendDocument", { chat_id: chatId, document: item.file_id, caption, reply_markup: markup }, env);
    return;
  }

  await sendMessage(token, chatId, "📢 Реклама", markup, env);
}

async function maybeShowAd(token, chatId, st, env, cfg) {
  if (isAdmin(chatId, env)) return;

  const wl = wlSet(cfg);
  if (wl.has(String(chatId))) return;

  if (!cfg?.ads?.enabled) return;

  st.ad = st.ad || { counter: 0, nextAtMs: 0 };

  // cooldown (timer)
  const now = Date.now();
  if (Number(st.ad.nextAtMs || 0) > now) return;

  // frequency (actions)
  st.ad.counter = (st.ad.counter || 0) + 1;
  const freq = Math.max(1, Number(cfg.ads.frequency || 3));
  if (st.ad.counter % freq !== 0) return;

  const item = (cfg.items && cfg.items.length) ? cfg.items[0] : initDefaultAdItem();
  await sendAdItem(token, chatId, item, env, cfg);

  const cd = Math.max(0, Number(cfg.ads.cooldownSec || 0));
  if (cd > 0) st.ad.nextAtMs = now + cd * 1000;
}

// ===================== QUEUES UI =====================
async function showMyQueues(token, chatId, st, env, editMessageId) {
  if (!st.saved || !st.saved.length) {
    const text = "📭 У тебе ще немає збережених черг";
    if (editMessageId) await safeEditOrSend(token, chatId, editMessageId, text, null, env);
    else await sendMessage(token, chatId, text, null, env);
    return;
  }

  const keyboard = st.saved.map((q, i) => ([{
    text: `${q.cityName} | ${q.queueName}`,
    callback_data: `show|${i}`
  }]));

  const kb = { inline_keyboard: keyboard.concat([[{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]]) };

  if (editMessageId) await safeEditOrSend(token, chatId, editMessageId, "⭐ Мої черги:", kb, env);
  else await sendMessage(token, chatId, "⭐ Мої черги:", kb, env);
}

// ===================== ADMIN PANEL (GLOBAL CONFIG) =====================
function adminKeyboard(cfg) {
  const adsEnabled = cfg?.ads?.enabled !== false;
  return {
    inline_keyboard: [
      [{ text: adsEnabled ? "⛔ Вимкнути рекламу" : "✅ Увімкнути рекламу", callback_data: "admin|toggle_ad" }],
      [{ text: "➖ Частота рідше", callback_data: "admin|freq_down" }, { text: "➕ Частота частіше", callback_data: "admin|freq_up" }],
      [{ text: "⏱ Таймер реклами", callback_data: "admin|ad_timer" }],
      [{ text: "➕ Додати текст реклами", callback_data: "admin|add_ad_text" }],
      [{ text: "➕ Додати медіа реклами", callback_data: "admin|add_ad_media" }],
      [{ text: "🗂 Список реклами", callback_data: "admin|ads_list" }],
      [{ text: "🚫 Whitelist меню", callback_data: "admin|wl_menu" }],
      [{ text: "📈 Статистика", callback_data: "admin|stats" }],
      [{ text: "🔙 Закрити", callback_data: "admin|close" }]
    ]
  };
}

async function showAdminMenu(token, chatId, st, env, cfg, editMessageId) {
  const wl = wlSet(cfg);
  const status = cfg.ads.enabled ? "✅ увімкнена" : "⛔ вимкнена";
  const freq = cfg.ads.frequency || 3;
  const cd = Number(cfg.ads.cooldownSec || 0);
  const cdText = cd > 0 ? humanizeSeconds(cd) : "вимкнено";

  const usersCount = await env.STATE_KV.get(usersCountKey(token)).catch(() => null);
  const users = usersCount ? Number(usersCount) : 0;

  const text =
`👑 Адмін-панель

🧩 Версія: ${VERSION}
📊 Юзерів (оцінка): ${users}

📢 Реклама: ${status}
🔁 Частота: раз на ${freq} дій
⏱ Таймер: ${cdText}
🧾 Оголошень: ${(cfg.items || []).length}

🚫 Whitelist: ${wl.size} юзерів`;

  const kb = adminKeyboard(cfg);

  if (editMessageId) await safeEditOrSend(token, chatId, editMessageId, text, kb, env);
  else await sendMessage(token, chatId, text, kb, env);
}

async function showWhitelistMenu(token, chatId, env, cfg, messageId) {
  const wl = wlSet(cfg);
  const list = [...wl].slice(0, 30);
  const text =
`🚫 Whitelist (без реклами)

Кнопки:
— «➕ Додати ID» (надішли наступним повідомленням ID)
— «➖ Видалити ID» (надішли ID)

Зараз у списку: ${wl.size}

${list.length ? list.map(x => `• ${x}`).join("\n") : "— порожньо —"}`;

  const kb = {
    inline_keyboard: [
      [{ text: "➕ Додати ID", callback_data: "admin|wl_add" }, { text: "➖ Видалити ID", callback_data: "admin|wl_del" }],
      [{ text: "🔙 Назад", callback_data: "admin" }]
    ]
  };

  await safeEditOrSend(token, chatId, messageId, text, kb, env);
}

async function showStats(token, chatId, st, env, cfg, messageId) {
  const usersCount = await env.STATE_KV.get(usersCountKey(token)).catch(() => null);
  const users = usersCount ? Number(usersCount) : 0;

  const wl = wlSet(cfg);
  const ads = (cfg.items || []).length;
  const saved = st.saved?.length || 0;

  const text =
`📈 Статистика

👥 Юзерів (оцінка): ${users}
⭐ Збережено у тебе: ${saved}
📢 Оголошень: ${ads}
🚫 Whitelist: ${wl.size}

⚙️ Дій (у тебе): ${st.stats?.actions || 0}`;

  const kb = { inline_keyboard: [[{ text: "🔙 Назад", callback_data: "admin" }]] };
  await safeEditOrSend(token, chatId, messageId, text, kb, env);
}

function adsListText(cfg) {
  const items = (cfg.items || []);
  const lines = items.slice(0, 20).map((a, i) => {
    const kind = a.type || "text";
    const t = (a.text || "").replace(/\s+/g, " ").trim();
    const preview = t ? (t.length > 40 ? t.slice(0, 40) + "…" : t) : "(без тексту)";
    return `${i === 0 ? "✅ " : "• "}#${a.id} [${kind}] ${preview}`;
  });
  return `🗂 Реклама (перші 20)\n\n${lines.length ? lines.join("\n") : "— порожньо —"}\n\nНатисни кнопку, щоб видалити.`;
}

function adsListKeyboard(cfg) {
  const items = (cfg.items || []).slice(0, 10); // buttons limit
  const rows = items.map(a => ([{ text: `❌ Видалити #${a.id}`, callback_data: `admin|ad_del|${a.id}` }]));
  rows.push([{ text: "🔙 Назад", callback_data: "admin" }]);
  return { inline_keyboard: rows };
}

// duration: "7d 1h 4s", "2h30m", "90m", "0" (disable), "3600"
function parseDurationToSec(s) {
  const txt = String(s || "").trim().toLowerCase();
  if (!txt) return null;

  // plain number => seconds
  if (/^\d+(\.\d+)?$/.test(txt)) {
    const n = Number(txt);
    if (!Number.isFinite(n)) return null;
    return Math.max(0, Math.floor(n));
  }

  // tokenized units
  const re = /(\d+(?:\.\d+)?)\s*(d|day|days|h|hr|hrs|hour|hours|m|min|mins|minute|minutes|s|sec|secs|second|seconds)/g;
  let total = 0;
  let matched = false;
  let m;
  while ((m = re.exec(txt)) !== null) {
    matched = true;
    const v = Number(m[1]);
    const u = m[2];
    if (!Number.isFinite(v)) continue;
    if (u.startsWith("d")) total += v * 86400;
    else if (u.startsWith("h") || u.startsWith("hr") || u.startsWith("hour")) total += v * 3600;
    else if (u.startsWith("m")) total += v * 60;
    else total += v;
  }
  if (!matched) return null;
  return Math.max(0, Math.floor(total));
}

function humanizeSeconds(sec) {
  sec = Math.max(0, Math.floor(Number(sec || 0)));
  const d = Math.floor(sec / 86400); sec -= d * 86400;
  const h = Math.floor(sec / 3600); sec -= h * 3600;
  const m = Math.floor(sec / 60); sec -= m * 60;
  const parts = [];
  if (d) parts.push(`${d} д`);
  if (h) parts.push(`${h} год`);
  if (m) parts.push(`${m} хв`);
  if (sec || !parts.length) parts.push(`${sec} с`);
  return parts.join(" ");
}

async function handleAdminAction(token, chatId, st, env, cfg, data, messageId) {
  st.admin = st.admin || { mode: null, tmp: null };
  const parts = data.split("|");
  const act = parts[1];

  if (act === "toggle_ad") {
    cfg.ads.enabled = !cfg.ads.enabled;
    await saveGlobalConfig(env, token, cfg);
    await showAdminMenu(token, chatId, st, env, cfg, messageId);
    return;
  }

  if (act === "freq_up") {
    cfg.ads.frequency = Math.max(1, Number(cfg.ads.frequency || 3) - 1);
    await saveGlobalConfig(env, token, cfg);
    await showAdminMenu(token, chatId, st, env, cfg, messageId);
    return;
  }

  if (act === "freq_down") {
    cfg.ads.frequency = Math.min(20, Number(cfg.ads.frequency || 3) + 1);
    await saveGlobalConfig(env, token, cfg);
    await showAdminMenu(token, chatId, st, env, cfg, messageId);
    return;
  }

  if (act === "ad_timer") {
    st.admin.mode = "await_ad_timer";
    await safeEditOrSend(
      token,
      chatId,
      messageId,
      "⏱ Введи таймер реклами (cooldown).\n\nПриклади:\n• 7d 1h 4s\n• 90m\n• 3600 (сек)\n• 0 (вимкнути)\n\n(або /cancel)",
      null,
      env
    );
    return;
  }

  if (act === "add_ad_text") {
    st.admin.mode = "await_ad_text";
    await safeEditOrSend(token, chatId, messageId, "✍️ Надішли наступним повідомленням текст реклами.\n(Або /cancel)", null, env);
    return;
  }

  if (act === "add_ad_media") {
    st.admin.mode = "await_ad_media";
    await safeEditOrSend(token, chatId, messageId, "📎 Надішли фото/відео/документ.\nCaption буде текстом реклами.\n(Або /cancel)", null, env);
    return;
  }

  if (act === "ads_list") {
    await safeEditOrSend(token, chatId, messageId, adsListText(cfg), adsListKeyboard(cfg), env);
    return;
  }

  if (act === "ad_del") {
    const id = parts[2];
    if (id) {
      cfg.items = (cfg.items || []).filter(a => String(a.id) !== String(id));
      if (!cfg.items.length) cfg.items = [initDefaultAdItem()];
      await saveGlobalConfig(env, token, cfg);
    }
    await safeEditOrSend(token, chatId, messageId, "🗑 Видалено. Оновлено список.", adsListKeyboard(cfg), env);
    return;
  }

  if (act === "wl_menu") {
    await showWhitelistMenu(token, chatId, env, cfg, messageId);
    return;
  }

  if (act === "wl_add") {
    st.admin.mode = "await_wl_add";
    await safeEditOrSend(token, chatId, messageId, "➕ Надішли ID юзера (число) наступним повідомленням.\n(Або /cancel)", null, env);
    return;
  }

  if (act === "wl_del") {
    st.admin.mode = "await_wl_del";
    await safeEditOrSend(token, chatId, messageId, "➖ Надішли ID юзера для видалення.\n(Або /cancel)", null, env);
    return;
  }

  if (act === "stats") {
    await showStats(token, chatId, st, env, cfg, messageId);
    return;
  }

  if (act === "close") {
    await safeEditOrSend(token, chatId, messageId, "✅ Закрито.", null, env);
    return;
  }
}

// ===================== ALERT SUBSCRIPTIONS =====================
async function upsertSubscription(env, token, chatId, sel, enabled) {
  const minutesBefore = alertMinutesBefore(env);
  const h = await sha1Hex(sel.url);
  const key = subKey(token, chatId, h);

  if (!enabled) {
    await env.STATE_KV.delete(key);
    return { key, enabled: false };
  }

  const rec = {
    token,
    chatId: String(chatId),
    url: sel.url,
    cityName: sel.cityName,
    queueName: sel.queueName,
    minutesBefore,
    enabled: true,
    lastNotifiedEventUtcMs: 0,
    updatedAt: Date.now()
  };

  await env.STATE_KV.put(key, jstringify(rec), { expirationTtl: ttlSec(env) });
  return { key, enabled: true };
}

async function listSubscriptions(env) {
  const prefix = "sub:";
  let cursor = undefined;
  const out = [];
  while (true) {
    const res = await env.STATE_KV.list({ prefix, cursor, limit: 1000 });
    if (res?.keys?.length) out.push(...res.keys.map(k => k.name));
    if (res?.list_complete) break;
    cursor = res.cursor;
    if (!cursor) break;
    if (out.length > 50_000) break;
  }
  return out;
}

// ===================== ROUTERS =====================
async function handleUpdate(update, token, env) {
  if (update.callback_query) return handleCallback(update.callback_query, token, env);
  if (update.message) return handleMessage(update.message, token, env);
}

// ===================== MESSAGE HANDLER =====================
async function handleMessage(message, token, env) {
  const chatId = message.chat.id;
  await ensureUserRegistered(env, token, chatId).catch(() => {});

  const key = stKey(token, chatId);
  const st = await loadUserState(env, key);
  const cfg = await loadGlobalConfig(env, token);

  st.stats = st.stats || { actions: 0 };
  st.stats.actions = (st.stats.actions || 0) + 1;

  const text = (message.text || "").trim();
  const cap = (message.caption || "").trim();
  const cmd = extractCommand(text);

  // /cancel for admin modes
  if (isAdmin(chatId, env) && (cmd === "/cancel" || text.toLowerCase() === "cancel")) {
    st.admin = st.admin || { mode: null, tmp: null };
    st.admin.mode = null;
    st.admin.tmp = null;
    await sendMessage(token, chatId, "✅ Скасовано.", null, env);
    await saveUserState(env, key, st);
    return;
  }

  // admin modes (text)
  if (isAdmin(chatId, env) && st.admin?.mode === "await_ad_text" && text && !cmd) {
    cfg.items = cfg.items || [];
    cfg.items.unshift({ id: String(Date.now()), type: "text", text, createdAt: Date.now() });
    st.admin.mode = null;
    await saveGlobalConfig(env, token, cfg);
    await sendMessage(token, chatId, "✅ Текст реклами додано.", null, env);
    await saveUserState(env, key, st);
    return;
  }

  // admin mode (timer)
  if (isAdmin(chatId, env) && st.admin?.mode === "await_ad_timer" && text) {
    const sec = parseDurationToSec(text);
    if (sec === null) {
      await sendMessage(token, chatId, "⚠️ Не зрозумів формат. Приклад: 7d 1h 4s або 90m або 3600 або 0.", null, env);
      await saveUserState(env, key, st);
      return;
    }
    cfg.ads.cooldownSec = Math.max(0, sec);
    st.admin.mode = null;
    await saveGlobalConfig(env, token, cfg);
    await sendMessage(token, chatId, `✅ Таймер встановлено: ${cfg.ads.cooldownSec ? humanizeSeconds(cfg.ads.cooldownSec) : "вимкнено"}`, null, env);
    await saveUserState(env, key, st);
    return;
  }

  // admin mode (media)
  if (isAdmin(chatId, env) && st.admin?.mode === "await_ad_media" && hasAnyMedia(message)) {
    const item = mediaToAdItem(message, cap);
    if (!item) {
      await sendMessage(token, chatId, "❌ Не зміг додати рекламу. Спробуй інше медіа.", null, env);
      await saveUserState(env, key, st);
      return;
    }
    cfg.items = cfg.items || [];
    cfg.items.unshift(item);
    st.admin.mode = null;
    await saveGlobalConfig(env, token, cfg);
    await sendMessage(token, chatId, `✅ Рекламу додано (#${item.id}).`, null, env);
    await saveUserState(env, key, st);
    return;
  }

  // admin mode whitelist add/del
  if (isAdmin(chatId, env) && (st.admin?.mode === "await_wl_add" || st.admin?.mode === "await_wl_del") && text) {
    const id = text.replace(/[^\d-]/g, "").trim();
    if (!id) {
      await sendMessage(token, chatId, "⚠️ Надішли тільки числовий ID.", null, env);
      await saveUserState(env, key, st);
      return;
    }
    const wl = wlSet(cfg);
    if (st.admin.mode === "await_wl_add") wl.add(String(id));
    if (st.admin.mode === "await_wl_del") wl.delete(String(id));
    setWhitelistFromSet(cfg, wl);
    st.admin.mode = null;
    await saveGlobalConfig(env, token, cfg);
    await sendMessage(token, chatId, st.admin.mode === "await_wl_add" ? `✅ Додано: ${id}` : `🗑 Видалено: ${id}`, null, env);
    await saveUserState(env, key, st);
    return;
  }

  // legacy: /ad in caption with media (still works)
  if (isAdmin(chatId, env) && hasAnyMedia(message) && cap.startsWith("/ad")) {
    const extraText = cap.replace(/^\/ad\s*/i, "").trim();
    const item = mediaToAdItem(message, extraText);
    if (!item) {
      await sendMessage(token, chatId, "❌ Не зміг додати рекламу. Спробуй інше медіа.", null, env);
      await saveUserState(env, key, st);
      return;
    }
    cfg.items = cfg.items || [];
    cfg.items.unshift(item);
    await saveGlobalConfig(env, token, cfg);
    await sendMessage(token, chatId, `✅ Рекламу додано (#${item.id}).`, null, env);
    await saveUserState(env, key, st);
    return;
  }

  // /start
  if (cmd === "/start") {
    st.cities = [];
    st.queues = [];
    st.selectedCity = null;
    st.selected = null;

    let msg = "⚡ ДТЕК • Світло Графік\n\n✍️ Напиши назву міста";
    if (isAdmin(chatId, env)) msg += "\n\n👑 Адмін: /admin";
    await sendMessage(token, chatId, msg, startKeyboard(env), env);

    await maybeShowAd(token, chatId, st, env, cfg);
    await saveUserState(env, key, st);
    return;
  }

  // /my
  if (cmd === "/my") {
    await showMyQueues(token, chatId, st, env, null);
    await maybeShowAd(token, chatId, st, env, cfg);
    await saveUserState(env, key, st);
    return;
  }

  // /admin
  if (cmd === "/admin" && isAdmin(chatId, env)) {
    await showAdminMenu(token, chatId, st, env, cfg, null);
    await saveUserState(env, key, st);
    return;
  }

  // City search: need plain text, not command
  if (!text || cmd) {
    await saveUserState(env, key, st);
    return;
  }

  if (text.length < 2) {
    await sendMessage(token, chatId, "✍️ Напиши назву міста (мінімум 2 символи)", null, env);
    await saveUserState(env, key, st);
    return;
  }

  const cities = await searchCities(text, env).catch(() => []);
  if (!cities.length) {
    await sendMessage(token, chatId, "❌ Місто не знайдено", null, env);
    await maybeShowAd(token, chatId, st, env, cfg);
    await saveUserState(env, key, st);
    return;
  }

  st.cities = cities;
  st.queues = [];
  st.selectedCity = null;
  st.selected = null;

  const keyboard = cities.slice(0, 8).map((c, i) => ([{ text: c.name, callback_data: `city|${i}` }]));
  await sendMessage(token, chatId, "📍 Обери місто:", { inline_keyboard: keyboard }, env);
  await maybeShowAd(token, chatId, st, env, cfg);

  await saveUserState(env, key, st);
}

// ===================== CALLBACK HANDLER =====================
async function handleCallback(q, token, env) {
  const chatId = q.message.chat.id;
  const messageId = q.message.message_id;
  const data = q.data || "";

  await ensureUserRegistered(env, token, chatId).catch(() => {});
  await answerCallback(token, q.id, env).catch(() => {});

  const key = stKey(token, chatId);
  const st = await loadUserState(env, key);
  const cfg = await loadGlobalConfig(env, token);

  st.stats = st.stats || { actions: 0 };
  st.stats.actions = (st.stats.actions || 0) + 1;

  if (data === "noop") { await saveUserState(env, key, st); return; }

  if (data === "admin") {
    if (!isAdmin(chatId, env)) { await saveUserState(env, key, st); return; }
    await showAdminMenu(token, chatId, st, env, cfg, messageId);
    await saveUserState(env, key, st);
    return;
  }

  if (data.startsWith("admin|")) {
    if (!isAdmin(chatId, env)) { await saveUserState(env, key, st); return; }
    await handleAdminAction(token, chatId, st, env, cfg, data, messageId);
    await saveUserState(env, key, st);
    return;
  }

  if (data.startsWith("city|")) {
    const idx = Number(data.split("|")[1]);
    const city = st.cities?.[idx];
    if (!city) {
      await safeEditOrSend(token, chatId, messageId, "⚠️ Стан втрачено. Напиши місто ще раз.", null, env);
      await saveUserState(env, key, st);
      return;
    }

    st.selectedCity = city;

    const queues = await getQueuesFromCityUrl(city.url, env).catch(() => []);
    if (!queues.length) {
      await safeEditOrSend(
        token,
        chatId,
        messageId,
        "❌ Черги не завантажились.\nСпробуй ще раз: натисни місто повторно або напиши назву міста знову.",
        null,
        env
      );
      await saveUserState(env, key, st);
      return;
    }

    st.queues = queues;
    st.selected = null;

    const keyboard = queues.map((qq, i) => ([{ text: qq.name, callback_data: `queue|${i}` }]));
    await safeEditOrSend(token, chatId, messageId, `🔌 Обери чергу:\n\n📍 ${city.name}`, { inline_keyboard: keyboard }, env);
    await maybeShowAd(token, chatId, st, env, cfg);
    await saveUserState(env, key, st);
    return;
  }

  if (data.startsWith("queue|")) {
    const idx = Number(data.split("|")[1]);
    const qq = st.queues?.[idx];
    if (!qq) {
      await safeEditOrSend(token, chatId, messageId, "⚠️ Стан втрачено. Напиши місто ще раз.", null, env);
      await saveUserState(env, key, st);
      return;
    }

    const cityName = st.selectedCity?.name || "Обране місто";
    st.selected = { cityName, queueName: qq.name, url: qq.url };

    const { text } = await buildInfo(st.selected, env);
    await safeEditOrSend(token, chatId, messageId, text, mainQueueKeyboard(env, st), env);
    await maybeShowAd(token, chatId, st, env, cfg);

    if (st.alerts?.enabled !== false) {
      await upsertSubscription(env, token, chatId, st.selected, true).catch(() => {});
    }

    await saveUserState(env, key, st);
    return;
  }

  if (data === "refresh") {
    if (!st.selected) {
      await safeEditOrSend(token, chatId, messageId, "⚠️ Спочатку обери місто та чергу.", null, env);
      await saveUserState(env, key, st);
      return;
    }
    const { text } = await buildInfo(st.selected, env);
    await safeEditOrSend(token, chatId, messageId, text, mainQueueKeyboard(env, st), env);
    await maybeShowAd(token, chatId, st, env, cfg);
    await saveUserState(env, key, st);
    return;
  }

  if (data === "save") {
    if (!st.selected) {
      await sendMessage(token, chatId, "⚠️ Немає вибраної черги.", null, env);
      await saveUserState(env, key, st);
      return;
    }
    if (!st.saved.find(x => x.url === st.selected.url)) {
      st.saved.push({ ...st.selected });
      await sendMessage(token, chatId, "⭐ Чергу збережено!", null, env);
    } else {
      await sendMessage(token, chatId, "✅ Вже збережено", null, env);
    }
    await maybeShowAd(token, chatId, st, env, cfg);
    await saveUserState(env, key, st);
    return;
  }

  if (data === "alerts_toggle") {
    st.alerts = st.alerts || { enabled: true };
    st.alerts.enabled = !st.alerts.enabled;

    if (st.alerts.enabled === false) {
      const prefix = `sub:${token}:${chatId}:`;
      let cursor = undefined;
      while (true) {
        const res = await env.STATE_KV.list({ prefix, cursor, limit: 500 });
        for (const k of (res?.keys || [])) await env.STATE_KV.delete(k.name);
        if (res?.list_complete) break;
        cursor = res.cursor;
        if (!cursor) break;
      }
      await sendMessage(token, chatId, "🔕 Алерти вимкнено.", null, env);
    } else {
      if (st.selected) await upsertSubscription(env, token, chatId, st.selected, true).catch(() => {});
      await sendMessage(token, chatId, "🔔 Алерти увімкнено.", null, env);
    }

    try {
      if (st.selected) {
        const { text } = await buildInfo(st.selected, env);
        await safeEditOrSend(token, chatId, messageId, text, mainQueueKeyboard(env, st), env);
      }
    } catch {}

    await saveUserState(env, key, st);
    return;
  }

  if (data === "my") {
    await showMyQueues(token, chatId, st, env, messageId);
    await maybeShowAd(token, chatId, st, env, cfg);
    await saveUserState(env, key, st);
    return;
  }

  if (data.startsWith("show|")) {
    const idx = Number(data.split("|")[1]);
    const item = st.saved[idx];
    if (!item) {
      await sendMessage(token, chatId, "⚠️ Не знайдено.", null, env);
      await saveUserState(env, key, st);
      return;
    }

    st.selected = { ...item };
    const { text } = await buildInfo(st.selected, env);

    const kb = {
      inline_keyboard: [
        [{ text: "🔄 Оновити", callback_data: "refresh" }],
        [{ text: "❌ Видалити", callback_data: `del|${idx}` }],
        [{ text: st.alerts?.enabled !== false ? "🔔 Алерти: увімкн." : "🔕 Алерти: вимкн.", callback_data: "alerts_toggle" }],
        [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]
      ]
    };

    await sendMessage(token, chatId, text, kb, env);
    await maybeShowAd(token, chatId, st, env, cfg);

    if (st.alerts?.enabled !== false) {
      await upsertSubscription(env, token, chatId, st.selected, true).catch(() => {});
    }

    await saveUserState(env, key, st);
    return;
  }

  if (data.startsWith("del|")) {
    const idx = Number(data.split("|")[1]);
    if (Number.isInteger(idx) && st.saved[idx]) {
      st.saved.splice(idx, 1);
      await sendMessage(token, chatId, "❌ Видалено", null, env);
    }
    await maybeShowAd(token, chatId, st, env, cfg);
    await saveUserState(env, key, st);
    return;
  }

  await saveUserState(env, key, st);
}

// ===================== CRON ALERTS =====================
async function processOneSubscription(env, rec, now) {
  if (!rec || rec.enabled !== true) return { sent: false };

  const { ok, blocks } = await fetchScheduleBlocks(rec.url, env);
  if (!ok || !blocks.length) return { sent: false };

  const nowMin = now.minutes;
  const meta = computeStatus(blocks, nowMin);
  if (!Number.isFinite(meta.nextChangeMin)) return { sent: false };

  const eventLocalMs = localDayStartMs(now.localMs) + meta.nextChangeMin * 60_000;
  const eventUtcMs = eventLocalMs - now.offMin * 60_000;

  const minsBefore = Number(rec.minutesBefore || alertMinutesBefore(env));
  const alertUtcMs = eventUtcMs - minsBefore * 60_000;

  const windowMs = alertWindowMin(env) * 60_000;

  const last = Number(rec.lastNotifiedEventUtcMs || 0);
  if (last && last >= eventUtcMs) return { sent: false };

  const within = (now.nowUtcMs >= alertUtcMs) && (now.nowUtcMs < alertUtcMs + windowMs);
  if (!within) return { sent: false };

  let what = "зміна";
  if (meta.nextChangeType === "on") what = "увімкнення";
  if (meta.nextChangeType === "off") what = "вимкнення";

  const atLocal = new Date(eventUtcMs + now.offMin * 60_000);
  const hh = String(atLocal.getUTCHours()).padStart(2, "0");
  const mm = String(atLocal.getUTCMinutes()).padStart(2, "0");
  const at = `${hh}:${mm}`;

  const msg =
`🔔 АЛЕРТ: через ${minsBefore} хв

📍 ${rec.cityName}
🔌 ${rec.queueName}

⏰ ${what} о ${at}`;

  await sendMessage(rec.token, rec.chatId, msg, null, env);

  rec.lastNotifiedEventUtcMs = eventUtcMs;
  rec.updatedAt = Date.now();

  const urlHash = await sha1Hex(rec.url);
  const key = subKey(rec.token, rec.chatId, urlHash);
  await env.STATE_KV.put(key, jstringify(rec), { expirationTtl: ttlSec(env) });

  return { sent: true };
}

async function runCronAlerts(env) {
  const keys = await listSubscriptions(env);
  if (!keys.length) return { processed: 0, sent: 0 };

  let processed = 0;
  let sent = 0;

  const now = uaLocalNow(env);
  const max = alertMaxPerCron(env);

  for (const k of keys) {
    if (processed >= max) break;
    const raw = await env.STATE_KV.get(k);
    if (!raw) continue;
    const rec = jparse(raw, null);
    processed++;
    try {
      const r = await processOneSubscription(env, rec, now);
      if (r.sent) sent++;
    } catch (e) {
      console.log("CRON sub error", k, e);
    }
  }

  return { processed, sent };
}

// ===================== WORKER EXPORT =====================
export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);

      if (url.pathname === "/") {
        return new Response(`Bot worker is running. VERSION=${VERSION}`, { status: 200 });
      }

      if (url.pathname === "/version") {
        return new Response(VERSION, { status: 200 });
      }

      if (url.pathname.startsWith("/webhook/")) {
        const token = url.pathname.split("/")[2];

        const tokens = parseTokens(env.BOT_TOKENS);
        if (!tokens.includes(token)) return new Response("Invalid token", { status: 403 });

        const update = await request.json();
        await handleUpdate(update, token, env);
        return new Response("OK", { status: 200 });
      }

      return new Response("Not found", { status: 404 });
    } catch (e) {
      console.log("FATAL ERROR:", e);
      return new Response("Worker error", { status: 500 });
    }
  },

  async scheduled(event, env, ctx) {
    try {
      const r = await runCronAlerts(env);
      console.log("CRON DONE", r);
    } catch (e) {
      console.log("CRON FATAL", e);
    }
  }
};
