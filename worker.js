/**
 * Cloudflare Worker Telegram Bot (Bezsvitla / DTEK schedules) — FULL VERSION (FINAL FIX)
 * ✅ Multi-bot via /webhook/<TOKEN> and env.BOT_TOKENS
 * ✅ Persistent per-user state in KV
 * ✅ GLOBAL ads + GLOBAL whitelist in KV (admin changes apply to everyone)
 * ✅ Ads: add text/media, frequency, TTL per ad item (e.g. 7d1h4s), delete specific ad
 * ✅ Always show button “📢 Тут може бути ваша реклама” under ANY ad
 * ✅ Saved queues
 * ✅ Alerts via Cron — notify N minutes before next change (default 20)
 * ✅ FIX: city queues sometimes fail — retries + KV cache + retry button
 *
 * REQUIRED BINDINGS (Cloudflare):
 *  - KV namespace binding: STATE_KV
 *  - BOT_TOKENS: "token1,token2" OR JSON '["token1","token2"]'
 *  - ADMIN_ID: Telegram numeric chat id (string ok)
 *  - SPONSOR_LINK: https://...
 *
 * OPTIONAL ENV:
 *  - AD_INFO_LINK (optional) where “Тут може бути ваша реклама” button leads (fallback SPONSOR_LINK)
 *  - UA_TZ_OFFSET_MIN (default 120)
 *  - STATE_TTL_SEC (default 60*60*24*45) per-user state + subscriptions TTL
 *  - ALERT_MIN_BEFORE (default 20)
 *  - ALERT_WINDOW_MIN (default 6)
 *  - ALERT_MAX_PER_CRON (default 300)
 *  - FETCH_TIMEOUT_MS (default 12000)
 *  - USERS_COUNT_CACHE_SEC (default 300)
 *  - CITY_QUEUES_CACHE_TTL_SEC (default 60*60*24*3)  // 3 days
 *  - FETCH_RETRY_COUNT (default 3)
 *  - FETCH_RETRY_DELAY_MS (default 600)
 */

const VERSION = "kv-cron-full-1.0.2-final";

// ===================== UTIL: SAFE JSON =====================
function jparse(s, fallback) {
  try { return JSON.parse(s); } catch { return fallback; }
}
function jstringify(obj) {
  try { return JSON.stringify(obj); } catch { return "{}"; }
}

function envNum(env, name, def) {
  const v = env?.[name];
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

function ttlSec(env) {
  return envNum(env, "STATE_TTL_SEC", 60 * 60 * 24 * 45);
}

function uaOffsetMin(env) {
  return envNum(env, "UA_TZ_OFFSET_MIN", 120);
}

function alertMinutesBefore(env) {
  return envNum(env, "ALERT_MIN_BEFORE", 20);
}

function alertWindowMin(env) {
  return envNum(env, "ALERT_WINDOW_MIN", 6);
}

function fetchTimeoutMs(env) {
  return envNum(env, "FETCH_TIMEOUT_MS", 12000);
}

function alertMaxPerCron(env) {
  return envNum(env, "ALERT_MAX_PER_CRON", 300);
}

function usersCountCacheSec(env) {
  return envNum(env, "USERS_COUNT_CACHE_SEC", 300);
}

function cityQueuesCacheTtlSec(env) {
  return envNum(env, "CITY_QUEUES_CACHE_TTL_SEC", 60 * 60 * 24 * 3);
}

function fetchRetryCount(env) {
  return envNum(env, "FETCH_RETRY_COUNT", 3);
}

function fetchRetryDelayMs(env) {
  return envNum(env, "FETCH_RETRY_DELAY_MS", 600);
}

function adInfoLink(env) {
  return env.AD_INFO_LINK || env.SPONSOR_LINK || "https://t.me/";
}

function stKey(token, chatId) {
  return `${token}:${chatId}`;
}

function isAdmin(chatId, env) {
  return String(chatId) === String(env.ADMIN_ID);
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

function localDayStartMs(localMs) {
  return Math.floor(localMs / 86_400_000) * 86_400_000;
}

function fmtDelta(min) {
  const m = Math.max(0, Math.floor(min));
  const h = Math.floor(m / 60);
  const r = m % 60;
  return `${h} год ${r} хв`;
}

// ===================== TELEGRAM API =====================
async function tgCall(token, method, payload, env) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), fetchTimeoutMs(env));
  try {
    const res = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: ctrl.signal
    });
    const json = await res.json().catch(() => ({}));
    return json;
  } finally {
    clearTimeout(t);
  }
}

async function sendMessage(token, chatId, text, replyMarkup = null, env) {
  const payload = { chat_id: chatId, text };
  if (replyMarkup) payload.reply_markup = replyMarkup;
  return tgCall(token, "sendMessage", payload, env);
}

async function editMessage(token, chatId, messageId, text, replyMarkup = null, env) {
  const payload = { chat_id: chatId, message_id: messageId, text };
  if (replyMarkup) payload.reply_markup = replyMarkup;
  return tgCall(token, "editMessageText", payload, env);
}

async function answerCallback(token, callbackQueryId, env) {
  return tgCall(token, "answerCallbackQuery", { callback_query_id: callbackQueryId }, env);
}

// ===================== KEYBOARDS =====================
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

function startKeyboard(env) {
  return {
    inline_keyboard: [
      [{ text: "⭐ Мої черги", callback_data: "my" }],
      [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]
    ]
  };
}

// ===================== TOKENS PARSER =====================
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

// ===================== FETCH WITH RETRY (fix glitch) =====================
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function fetchWithRetry(url, init, env) {
  const tries = Math.max(1, fetchRetryCount(env));
  const delay = Math.max(0, fetchRetryDelayMs(env));

  let lastErr = null;
  for (let i = 0; i < tries; i++) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), fetchTimeoutMs(env));
    try {
      const res = await fetch(url, { ...init, signal: ctrl.signal });
      clearTimeout(t);
      if (res.ok) return res;
      lastErr = new Error(`HTTP ${res.status}`);
    } catch (e) {
      clearTimeout(t);
      lastErr = e;
    }
    if (i < tries - 1) await sleep(delay);
  }
  throw lastErr || new Error("fetch failed");
}

// ===================== PER-USER STATE =====================
function defaultState() {
  return {
    v: 3,
    cities: [],
    queues: [],
    selectedCity: null,
    selected: null,
    saved: [],
    admin: { mode: null, tmp: null },
    stats: { actions: 0 },
    alerts: { enabled: true },
    adCounter: 0
  };
}

async function loadState(env, key) {
  const raw = await env.STATE_KV.get(`st:${key}`);
  if (!raw) return defaultState();
  const obj = jparse(raw, null);
  if (!obj || typeof obj !== "object") return defaultState();

  if (!obj.saved) obj.saved = [];
  if (!obj.cities) obj.cities = [];
  if (!obj.queues) obj.queues = [];
  if (!obj.admin) obj.admin = { mode: null, tmp: null };
  if (!obj.stats) obj.stats = { actions: 0 };
  if (!obj.alerts) obj.alerts = { enabled: true };
  if (typeof obj.adCounter !== "number") obj.adCounter = 0;

  return obj;
}

async function saveState(env, key, st) {
  await env.STATE_KV.put(`st:${key}`, jstringify(st), { expirationTtl: ttlSec(env) });
}

// ===================== GLOBAL ADS + WHITELIST =====================
function initAdConfig() {
  return {
    enabled: true,
    frequency: 3,
    items: [
      { id: "default", type: "text", text: "📢 Тут може бути твоя реклама. Натисни «Стати спонсором» 👇", createdAt: Date.now(), expiresAt: 0 }
    ]
  };
}

function adsKey(token) { return `cfg:ads:${token}`; }
function wlKey(token) { return `cfg:wl:${token}`; }

async function loadAdConfig(env, token) {
  const raw = await env.STATE_KV.get(adsKey(token));
  const cfg = raw ? jparse(raw, null) : null;
  if (!cfg || typeof cfg !== "object") return initAdConfig();
  if (!Array.isArray(cfg.items)) cfg.items = initAdConfig().items;
  if (typeof cfg.enabled !== "boolean") cfg.enabled = true;
  if (!Number.isFinite(Number(cfg.frequency)) || Number(cfg.frequency) < 1) cfg.frequency = 3;

  cfg.items = cfg.items.map(x => ({
    id: String(x?.id ?? Date.now()),
    type: x?.type || "text",
    text: x?.text || "",
    file_id: x?.file_id,
    createdAt: Number(x?.createdAt || Date.now()),
    expiresAt: Number(x?.expiresAt || 0)
  }));

  if (!cfg.items.length) cfg.items = initAdConfig().items;
  return cfg;
}

async function saveAdConfig(env, token, cfg) {
  await env.STATE_KV.put(adsKey(token), jstringify(cfg));
}

async function loadWhitelist(env, token) {
  const raw = await env.STATE_KV.get(wlKey(token));
  const arr = raw ? jparse(raw, []) : [];
  return new Set(Array.isArray(arr) ? arr.map(String) : []);
}

async function saveWhitelist(env, token, wlSet) {
  await env.STATE_KV.put(wlKey(token), jstringify([...wlSet].map(String)));
}

function pruneExpiredAds(cfg) {
  const now = Date.now();
  cfg.items = (cfg.items || []).filter(it => {
    if (!it) return false;
    const exp = Number(it.expiresAt || 0);
    return exp === 0 || exp > now;
  });
  if (!cfg.items.length) cfg.items = initAdConfig().items;
  return cfg;
}

// ===================== USERS COUNT (accurate) =====================
async function ensureUserRegistered(env, token, chatId) {
  const uKey = `user:${token}:${chatId}`;
  const exists = await env.STATE_KV.get(uKey);
  if (exists) return;
  await env.STATE_KV.put(uKey, "1");
  await env.STATE_KV.delete(`cache:users_count:${token}`).catch(() => {});
}

async function getUsersCount(env, token) {
  const cacheKey = `cache:users_count:${token}`;
  const cachedRaw = await env.STATE_KV.get(cacheKey);
  if (cachedRaw) {
    const c = jparse(cachedRaw, null);
    if (c && Number.isFinite(c.count) && Number.isFinite(c.ts)) {
      if (Date.now() - c.ts < usersCountCacheSec(env) * 1000) return c.count;
    }
  }

  const prefix = `user:${token}:`;
  let cursor = undefined;
  let count = 0;
  while (true) {
    const res = await env.STATE_KV.list({ prefix, cursor, limit: 1000 });
    count += (res?.keys?.length || 0);
    if (res?.list_complete) break;
    cursor = res.cursor;
    if (!cursor) break;
    if (count > 500_000) break;
  }
  await env.STATE_KV.put(cacheKey, jstringify({ count, ts: Date.now() }), { expirationTtl: usersCountCacheSec(env) });
  return count;
}

// ===================== TTL PARSER =====================
function parseDurationToSec(s) {
  if (!s) return 0;
  const str = String(s).trim().toLowerCase().replace(/\s+/g, "");
  if (!/^\d/.test(str)) return 0;
  const re = /(\d+)([wdhms])/g;
  let m;
  let total = 0;
  let matched = false;
  while ((m = re.exec(str))) {
    matched = true;
    const n = Number(m[1]);
    const u = m[2];
    if (!Number.isFinite(n)) continue;
    if (u === "w") total += n * 7 * 24 * 3600;
    if (u === "d") total += n * 24 * 3600;
    if (u === "h") total += n * 3600;
    if (u === "m") total += n * 60;
    if (u === "s") total += n;
  }
  return matched ? total : 0;
}

function splitTtlPrefix(text) {
  const t = String(text || "").trim();
  const first = t.split(/\s+/, 1)[0] || "";
  const ttl = parseDurationToSec(first);
  if (ttl > 0) {
    const rest = t.slice(first.length).trim();
    return { ttlSec: ttl, restText: rest };
  }
  return { ttlSec: 0, restText: t };
}

// ===================== ADS =====================
function hasAnyMedia(message) {
  return Boolean(message.photo || message.video || message.document);
}

function mediaToAdItem(message, extraText, ttlSec) {
  const id = String(Date.now());
  const createdAt = Date.now();
  const expiresAt = ttlSec > 0 ? (createdAt + ttlSec * 1000) : 0;

  if (message.photo && message.photo.length) {
    const best = message.photo[message.photo.length - 1];
    return { id, type: "photo", file_id: best.file_id, text: extraText || "", createdAt, expiresAt };
  }
  if (message.video) {
    return { id, type: "video", file_id: message.video.file_id, text: extraText || "", createdAt, expiresAt };
  }
  if (message.document) {
    return { id, type: "document", file_id: message.document.file_id, text: extraText || "", createdAt, expiresAt };
  }
  return null;
}

function adButtons(env) {
  return [
    { text: "🤝 Стати спонсором", url: env.SPONSOR_LINK },
    { text: "📢 Тут може бути ваша реклама", url: adInfoLink(env) }
  ];
}

async function sendAdItem(token, chatId, item, env) {
  const kb = { inline_keyboard: [adButtons(env)] };

  if (item.type === "text") {
    const text = item.text || "📢 Реклама";
    await sendMessage(token, chatId, text, kb, env);
    return;
  }

  const caption = item.text || "📢 Реклама";

  if (item.type === "photo") {
    await tgCall(token, "sendPhoto", { chat_id: chatId, photo: item.file_id, caption, reply_markup: kb }, env);
    return;
  }
  if (item.type === "video") {
    await tgCall(token, "sendVideo", { chat_id: chatId, video: item.file_id, caption, reply_markup: kb }, env);
    return;
  }
  if (item.type === "document") {
    await tgCall(token, "sendDocument", { chat_id: chatId, document: item.file_id, caption, reply_markup: kb }, env);
    return;
  }

  await sendMessage(token, chatId, caption, kb, env);
}

async function maybeShowAd(token, chatId, st, env) {
  if (isAdmin(chatId, env)) return;

  const wl = await loadWhitelist(env, token);
  if (wl.has(String(chatId))) return;

  let cfg = await loadAdConfig(env, token);
  cfg = pruneExpiredAds(cfg);
  if (!cfg.enabled) return;

  st.adCounter = (st.adCounter || 0) + 1;
  if ((st.adCounter % (cfg.frequency || 3)) !== 0) return;

  const item = (cfg.items && cfg.items.length) ? cfg.items[0] : null;
  if (!item) return;

  await sendAdItem(token, chatId, item, env);
}

// ===================== ADMIN MENUS =====================
async function showAdminMenu(token, chatId, st, env, editMessageId = null) {
  let ads = await loadAdConfig(env, token);
  ads = pruneExpiredAds(ads);
  const wl = await loadWhitelist(env, token);

  const status = ads.enabled ? "✅ увімкнена" : "⛔ вимкнена";
  const freq = ads.frequency || 3;
  const adsCount = ads.items?.length || 0;

  const users = await getUsersCount(env, token).catch(() => 0);
  const alertsOn = st.alerts?.enabled !== false;

  const text =
`👑 Адмін-панель

🧩 Версія: ${VERSION}
📊 Юзерів: ${users}

📢 Реклама: ${status}
🔁 Частота: раз на ${freq} дій
🧾 Оголошень (активні): ${adsCount}

🔔 Алерти для тебе: ${alertsOn ? "✅ увімкнені" : "⛔ вимкнені"}
🚫 Whitelist: ${wl.size} юзерів`;

  const kb = {
    inline_keyboard: [
      [{ text: ads.enabled ? "⛔ Вимкнути рекламу" : "✅ Увімкнути рекламу", callback_data: "admin|toggle_ad" }],
      [{ text: "➖ Частота рідше", callback_data: "admin|freq_down" }, { text: "➕ Частота частіше", callback_data: "admin|freq_up" }],
      [{ text: "🗂 Керування рекламою", callback_data: "admin|ads_menu" }],
      [{ text: "🚫 Whitelist меню", callback_data: "admin|wl_menu" }],
      [{ text: "📈 Статистика", callback_data: "admin|stats" }],
      [{ text: "🔙 Закрити", callback_data: "admin|close" }]
    ]
  };

  if (editMessageId) await editMessage(token, chatId, editMessageId, text, kb, env);
  else await sendMessage(token, chatId, text, kb, env);
}

async function showAdsMenu(token, chatId, st, env, messageId) {
  let ads = await loadAdConfig(env, token);
  ads = pruneExpiredAds(ads);
  await saveAdConfig(env, token, ads).catch(() => {});

  const lines = (ads.items || []).slice(0, 25).map((it, idx) => {
    const exp = Number(it.expiresAt || 0);
    const expTxt = exp ? new Date(exp).toISOString().replace("T"," ").slice(0,19) + "Z" : "∞";
    const title = it.type === "text"
      ? (it.text || "").slice(0, 32).replace(/\n/g, " ")
      : `${it.type} (${it.file_id ? "file" : "?"})`;
    return `${idx + 1}. #${it.id} — ${it.type} — exp: ${expTxt}\n   ${title}`;
  });

  const text =
`🗂 Реклама: керування

Додати:
— «➕ Текст» → надішли текст (можна з TTL: 7d1h4s Текст)
— «➕ Медіа» → надішли фото/відео/док (caption може починатись з TTL)

Видалити:
— натисни «❌» біля потрібного оголошення

Список (до 25):
${lines.length ? lines.join("\n") : "— порожньо —"}`;

  const delButtons = [];
  const items = (ads.items || []).slice(0, 12);
  for (let i = 0; i < items.length; i += 2) {
    const row = [];
    row.push({ text: `❌ ${i + 1}`, callback_data: `admin|ad_del|${items[i].id}` });
    if (items[i + 1]) row.push({ text: `❌ ${i + 2}`, callback_data: `admin|ad_del|${items[i + 1].id}` });
    delButtons.push(row);
  }

  const kb = {
    inline_keyboard: [
      [{ text: "➕ Додати текст", callback_data: "admin|add_ad_text" }],
      [{ text: "➕ Додати медіа", callback_data: "admin|add_ad_media" }],
      [{ text: "🗑 Очистити все", callback_data: "admin|clear_ads" }],
      ...delButtons,
      [{ text: "🔙 Назад", callback_data: "admin" }]
    ]
  };

  await editMessage(token, chatId, messageId, text, kb, env);
}

async function showWhitelistMenu(token, chatId, st, env, messageId) {
  const wl = await loadWhitelist(env, token);
  const list = [...wl].slice(0, 30);

  const text =
`🚫 Whitelist (без реклами) — ГЛОБАЛЬНИЙ ✅

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

  await editMessage(token, chatId, messageId, text, kb, env);
}

async function showStats(token, chatId, st, env, messageId) {
  const users = await getUsersCount(env, token).catch(() => 0);

  const wl = await loadWhitelist(env, token);
  let ads = await loadAdConfig(env, token);
  ads = pruneExpiredAds(ads);

  const saved = st.saved?.length || 0;

  const text =
`📈 Статистика

👥 Юзерів: ${users}
⭐ Збережено у тебе: ${saved}
📢 Оголошень (активні): ${ads.items?.length || 0}
🚫 Whitelist: ${wl.size}

⚙️ Дій (у тебе): ${st.stats?.actions || 0}`;

  const kb = { inline_keyboard: [[{ text: "🔙 Назад", callback_data: "admin" }]] };
  await editMessage(token, chatId, messageId, text, kb, env);
}

async function handleAdminAction(token, chatId, st, env, data, messageId) {
  if (!st.admin) st.admin = { mode: null, tmp: null };

  const parts = data.split("|");
  const act = parts[1];

  if (act === "toggle_ad") {
    let ads = await loadAdConfig(env, token);
    ads.enabled = !ads.enabled;
    await saveAdConfig(env, token, ads);
    await showAdminMenu(token, chatId, st, env, messageId);
    return;
  }

  if (act === "freq_up") {
    let ads = await loadAdConfig(env, token);
    ads.frequency = Math.max(1, (Number(ads.frequency || 3)) - 1);
    await saveAdConfig(env, token, ads);
    await showAdminMenu(token, chatId, st, env, messageId);
    return;
  }

  if (act === "freq_down") {
    let ads = await loadAdConfig(env, token);
    ads.frequency = Math.min(50, (Number(ads.frequency || 3)) + 1);
    await saveAdConfig(env, token, ads);
    await showAdminMenu(token, chatId, st, env, messageId);
    return;
  }

  if (act === "ads_menu") {
    await showAdsMenu(token, chatId, st, env, messageId);
    return;
  }

  if (act === "ad_del") {
    const id = parts[2];
    let ads = await loadAdConfig(env, token);
    ads = pruneExpiredAds(ads);
    const before = ads.items.length;
    ads.items = ads.items.filter(x => String(x.id) !== String(id));
    if (!ads.items.length) ads.items = initAdConfig().items;
    await saveAdConfig(env, token, ads);
    await showAdsMenu(token, chatId, st, env, messageId);
    return;
  }

  if (act === "clear_ads") {
    const ads = initAdConfig();
    await saveAdConfig(env, token, ads);
    await showAdsMenu(token, chatId, st, env, messageId);
    return;
  }

  if (act === "add_ad_text") {
    st.admin.mode = "await_ad_text";
    await editMessage(
      token, chatId, messageId,
      "✍️ Надішли наступним повідомленням текст реклами.\nМожеш додати TTL на початку: 7d1h4s Текст\n(Або /cancel щоб скасувати)",
      null, env
    );
    return;
  }

  if (act === "add_ad_media") {
    st.admin.mode = "await_ad_media";
    await editMessage(
      token, chatId, messageId,
      "📎 Надішли фото/відео/документ.\nCaption може починатися з TTL: 7d1h4s Текст\n(Або /cancel щоб скасувати)",
      null, env
    );
    return;
  }

  if (act === "wl_menu") {
    await showWhitelistMenu(token, chatId, st, env, messageId);
    return;
  }

  if (act === "wl_add") {
    st.admin.mode = "await_wl_add";
    await editMessage(token, chatId, messageId, "➕ Надішли ID юзера (число) наступним повідомленням.\n(Або /cancel)", null, env);
    return;
  }

  if (act === "wl_del") {
    st.admin.mode = "await_wl_del";
    await editMessage(token, chatId, messageId, "➖ Надішли ID юзера для видалення.\n(Або /cancel)", null, env);
    return;
  }

  if (act === "stats") {
    await showStats(token, chatId, st, env, messageId);
    return;
  }

  if (act === "close") {
    await editMessage(token, chatId, messageId, "✅ Закрито.", null, env);
    return;
  }
}

// ===================== MY QUEUES UI =====================
async function showMyQueues(token, chatId, st, env, editMessageId = null) {
  if (!st.saved || !st.saved.length) {
    const text = "📭 У тебе ще немає збережених черг";
    if (editMessageId) await editMessage(token, chatId, editMessageId, text, null, env);
    else await sendMessage(token, chatId, text, null, env);
    return;
  }

  const keyboard = st.saved.map((q, i) => ([{
    text: `${q.cityName} | ${q.queueName}`,
    callback_data: `show|${i}`
  }]));

  const kb = { inline_keyboard: keyboard.concat([[{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]]) };

  if (editMessageId) {
    await editMessage(token, chatId, editMessageId, "⭐ Мої черги:", kb, env);
  } else {
    await sendMessage(token, chatId, "⭐ Мої черги:", kb, env);
  }
}

// ===================== BEZSVITLA: SEARCH CITIES =====================
async function searchCities(query, env) {
  const url = "https://bezsvitla.com.ua/search-locality?q=" + encodeURIComponent(query);
  try {
    const res = await fetchWithRetry(url, {
      headers: {
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://bezsvitla.com.ua/"
      }
    }, env);
    const data = await res.json().catch(() => null);
    if (!Array.isArray(data)) return [];
    return data
      .filter(x => x && x.name && x.url)
      .map(x => ({ name: x.name, url: x.url }))
      .slice(0, 8);
  } catch {
    return [];
  }
}

// ===================== CITY QUEUES (FIX: retry + KV cache) =====================
async function sha1Hex(text) {
  const data = new TextEncoder().encode(text);
  const buf = await crypto.subtle.digest("SHA-1", data);
  const arr = Array.from(new Uint8Array(buf));
  return arr.map(b => b.toString(16).padStart(2, "0")).join("");
}

function cityQueuesCacheKey(token, cityUrlHash) {
  return `cache:city_queues:${token}:${cityUrlHash}`;
}

function parseQueuesFromCityHtml(html) {
  const matches = [...html.matchAll(/href="([^"]*\/cherha-[^"]+)"/g)];
  const urls = [...new Set(matches.map(m => m[1]).map(u => u.startsWith("http") ? u : ("https://bezsvitla.com.ua" + u)))];
  return urls.map(u => {
    const code = u.split("cherha-")[1].replace(/-/g, ".");
    return { name: `Черга ${code}`, url: u };
  });
}

async function getQueuesFromCityUrl(cityUrl, env, token) {
  const h = await sha1Hex(cityUrl);
  const cKey = cityQueuesCacheKey(token, h);

  // try cache first
  const cached = await env.STATE_KV.get(cKey);
  if (cached) {
    const obj = jparse(cached, null);
    if (obj && Array.isArray(obj.queues) && obj.queues.length) {
      return { queues: obj.queues, fromCache: true };
    }
  }

  // fetch with retry
  try {
    const res = await fetchWithRetry(cityUrl, {
      headers: { "User-Agent": "Mozilla/5.0", "Accept-Language": "uk-UA,uk;q=0.9" }
    }, env);

    const html = await res.text();
    const queues = parseQueuesFromCityHtml(html);

    if (queues.length) {
      await env.STATE_KV.put(cKey, jstringify({ queues, ts: Date.now() }), { expirationTtl: cityQueuesCacheTtlSec(env) });
      return { queues, fromCache: false };
    }

    // If fetch ok but parsing empty: try cache fallback (maybe old)
    if (cached) {
      const obj = jparse(cached, null);
      if (obj && Array.isArray(obj.queues) && obj.queues.length) {
        return { queues: obj.queues, fromCache: true };
      }
    }

    return { queues: [], fromCache: false };
  } catch {
    // fetch failed: fallback cache
    if (cached) {
      const obj = jparse(cached, null);
      if (obj && Array.isArray(obj.queues) && obj.queues.length) {
        return { queues: obj.queues, fromCache: true };
      }
    }
    return { queues: [], fromCache: false };
  }
}

// ===================== SCHEDULE PARSERS =====================
function extractLiItems(html) {
  const lis = [...html.matchAll(/<li[^>]*>([\s\S]*?)<\/li>/g)].map(m => m[0]);
  const out = [];
  for (const li of lis) {
    const tm = li.match(/(\d{2}:\d{2})\s*[–-]\s*(\d{2}:\d{2})/);
    if (!tm) continue;
    const start = tm[1];
    const end = tm[2];
    const on = li.includes("icon-on") ? true : (li.includes("icon-off") ? false : null);
    out.push({ start, end, on });
  }
  return out;
}

function extractTimeOnly(html) {
  const ms = [...html.matchAll(/(\d{2}:\d{2})\s*[–-]\s*(\d{2}:\d{2})/g)];
  return ms.map(m => ({ start: m[1], end: m[2], on: null }));
}

function toMin(t) {
  const [h, m] = t.split(":").map(Number);
  return h * 60 + m;
}

function normalizeBlock(b) {
  const startMin = toMin(b.start);
  let endMin = toMin(b.end);
  if (endMin < startMin) endMin += 24 * 60;
  return { ...b, startMin, endMin };
}

function findCurrentBlock(blocks, nowMin) {
  for (const b of blocks) {
    if (nowMin >= b.startMin && nowMin < b.endMin) return b;
  }
  return null;
}

function findNextBlock(blocks, nowMin) {
  for (const b of blocks) {
    if (b.startMin > nowMin) return b;
  }
  return null;
}

function fmtBlocks(arr) {
  if (!arr.length) return "❌ Нема даних";
  return arr
    .map(b => `${b.on === true ? "🟢" : b.on === false ? "🔴" : "🟡"} ${b.start} – ${b.end}`)
    .join("\n");
}

async function fetchScheduleBlocks(url, env) {
  try {
    const res = await fetchWithRetry(url, {
      headers: { "User-Agent": "Mozilla/5.0", "Accept-Language": "uk-UA,uk;q=0.9" }
    }, env);

    const html = await res.text();
    const items = extractLiItems(html);
    const blocks = (items.length ? items : extractTimeOnly(html)).map(x => normalizeBlock(x));
    return { ok: true, blocks, html };
  } catch {
    return { ok: false, blocks: [], html: "" };
  }
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
      statusLine = "🔴 ЗАРАЗ НЕМА СВІТЛА";
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
  if (!ok) {
    return { text: `📍 ${sel.cityName}\n🔌 ${sel.queueName}\n\n❌ Не вдалося завантажити сторінку черги`, meta: null };
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

  if (tomorrow.length) {
    text += `\n\n📅 ЗАВТРА:\n${fmtBlocks(tomorrow)}`;
  }

  return { text: text.trim(), meta: { ...meta, nowMin } };
}

// ===================== ALERT SUBSCRIPTIONS =====================
function subKey(token, chatId, urlHash) {
  return `sub:${token}:${chatId}:${urlHash}`;
}

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

// ===================== ROUTER =====================
async function handleUpdate(update, token, env) {
  if (update.callback_query) {
    await handleCallback(update.callback_query, token, env);
    return;
  }
  if (update.message) {
    await handleMessage(update.message, token, env);
    return;
  }
}

// ===================== MESSAGE HANDLER =====================
async function handleMessage(message, token, env) {
  const chatId = message.chat.id;
  await ensureUserRegistered(env, token, chatId).catch(() => {});

  const key = stKey(token, chatId);
  const st = await loadState(env, key);

  st.stats.actions = (st.stats.actions || 0) + 1;

  const text = (message.text || "").trim();
  const capRaw = (message.caption || "").trim();

  // cancel admin modes
  if (isAdmin(chatId, env) && (text === "/cancel" || text === "cancel")) {
    st.admin.mode = null;
    st.admin.tmp = null;
    await sendMessage(token, chatId, "✅ Скасовано.", null, env);
    await saveState(env, key, st);
    return;
  }

  // admin add ad text
  if (isAdmin(chatId, env) && st.admin?.mode === "await_ad_text" && text) {
    const { ttlSec: tsec, restText } = splitTtlPrefix(text);
    const createdAt = Date.now();
    const expiresAt = tsec > 0 ? (createdAt + tsec * 1000) : 0;

    let ads = await loadAdConfig(env, token);
    ads = pruneExpiredAds(ads);
    ads.items.unshift({ id: String(createdAt), type: "text", text: restText || text, createdAt, expiresAt });
    await saveAdConfig(env, token, ads);

    st.admin.mode = null;
    await sendMessage(token, chatId, "✅ Текст реклами додано (глобально).", null, env);
    await saveState(env, key, st);
    return;
  }

  // admin add ad media
  if (isAdmin(chatId, env) && st.admin?.mode === "await_ad_media" && hasAnyMedia(message)) {
    const { ttlSec: tsec, restText } = splitTtlPrefix(capRaw);
    const item = mediaToAdItem(message, restText, tsec);
    if (!item) {
      await sendMessage(token, chatId, "❌ Не зміг додати рекламу.", null, env);
      await saveState(env, key, st);
      return;
    }
    let ads = await loadAdConfig(env, token);
    ads = pruneExpiredAds(ads);
    ads.items.unshift(item);
    await saveAdConfig(env, token, ads);

    st.admin.mode = null;
    await sendMessage(token, chatId, `✅ Рекламу додано (глобально) (#${item.id}).`, null, env);
    await saveState(env, key, st);
    return;
  }

  // admin whitelist add/del
  if (isAdmin(chatId, env) && (st.admin?.mode === "await_wl_add" || st.admin?.mode === "await_wl_del") && text) {
    const id = text.replace(/[^\d-]/g, "").trim();
    if (!id) {
      await sendMessage(token, chatId, "⚠️ Надішли тільки числовий ID.", null, env);
      await saveState(env, key, st);
      return;
    }

    const wl = await loadWhitelist(env, token);
    if (st.admin.mode === "await_wl_add") {
      wl.add(String(id));
      await saveWhitelist(env, token, wl);
      st.admin.mode = null;
      await sendMessage(token, chatId, `✅ Додано в whitelist (глобально): ${id}`, null, env);
      await saveState(env, key, st);
      return;
    } else {
      wl.delete(String(id));
      await saveWhitelist(env, token, wl);
      st.admin.mode = null;
      await sendMessage(token, chatId, `🗑 Видалено з whitelist (глобально): ${id}`, null, env);
      await saveState(env, key, st);
      return;
    }
  }

  // /start
  if (text === "/start") {
    st.cities = [];
    st.queues = [];
    st.selectedCity = null;
    st.selected = null;

    let msg = "⚡ ДТЕК • Світло Графік\n\n✍️ Напиши назву міста";
    if (isAdmin(chatId, env)) msg += "\n\n👑 Адмін: /admin";
    await sendMessage(token, chatId, msg, startKeyboard(env), env);
    await maybeShowAd(token, chatId, st, env);
    await saveState(env, key, st);
    return;
  }

  // /my
  if (text === "/my") {
    await showMyQueues(token, chatId, st, env);
    await maybeShowAd(token, chatId, st, env);
    await saveState(env, key, st);
    return;
  }

  // /admin
  if (text === "/admin" && isAdmin(chatId, env)) {
    await showAdminMenu(token, chatId, st, env);
    await saveState(env, key, st);
    return;
  }

  if (!text) {
    await saveState(env, key, st);
    return;
  }

  if (text.length < 2) {
    await sendMessage(token, chatId, "✍️ Напиши назву міста (мінімум 2 символи)", null, env);
    await saveState(env, key, st);
    return;
  }

  // search cities
  const cities = await searchCities(text, env);
  if (!cities.length) {
    await sendMessage(token, chatId, "❌ Місто не знайдено", null, env);
    await maybeShowAd(token, chatId, st, env);
    await saveState(env, key, st);
    return;
  }

  st.cities = cities;
  st.queues = [];
  st.selectedCity = null;
  st.selected = null;

  const keyboard = cities.slice(0, 8).map((c, i) => ([
    { text: c.name, callback_data: `city|${i}` }
  ]));

  await sendMessage(token, chatId, "📍 Обери місто:", { inline_keyboard: keyboard }, env);
  await maybeShowAd(token, chatId, st, env);

  await saveState(env, key, st);
}

// ===================== CALLBACK HANDLER =====================
async function handleCallback(q, token, env) {
  const chatId = q.message.chat.id;
  const messageId = q.message.message_id;
  const data = q.data || "";

  await ensureUserRegistered(env, token, chatId).catch(() => {});
  await answerCallback(token, q.id, env).catch(() => {});

  const key = stKey(token, chatId);
  const st = await loadState(env, key);

  st.stats.actions = (st.stats.actions || 0) + 1;

  if (data === "admin") {
    if (!isAdmin(chatId, env)) { await saveState(env, key, st); return; }
    await showAdminMenu(token, chatId, st, env, messageId);
    await saveState(env, key, st);
    return;
  }

  if (data.startsWith("admin|")) {
    if (!isAdmin(chatId, env)) { await saveState(env, key, st); return; }
    await handleAdminAction(token, chatId, st, env, data, messageId);
    await saveState(env, key, st);
    return;
  }

  // FIX: retry loading queues for selected city
  if (data === "retry_city_queues") {
    if (!st.selectedCity?.url) {
      await editMessage(token, chatId, messageId, "⚠️ Стан втрачено. Напиши місто ще раз.", null, env);
      await saveState(env, key, st);
      return;
    }

    const city = st.selectedCity;
    const r = await getQueuesFromCityUrl(city.url, env, token);
    const queues = r.queues;

    if (!queues.length) {
      const kb = { inline_keyboard: [[{ text: "🔁 Спробувати ще раз", callback_data: "retry_city_queues" }]] };
      await editMessage(token, chatId, messageId, "❌ Черги не завантажились. Спробуй ще раз.", kb, env);
      await saveState(env, key, st);
      return;
    }

    st.queues = queues;
    st.selected = null;

    const keyboard = queues.map((qq, i) => ([
      { text: qq.name, callback_data: `queue|${i}` }
    ]));

    const note = r.fromCache ? "\n\n(✅ Показую з кешу — сайт може лагати)" : "";
    await editMessage(token, chatId, messageId, `🔌 Обери чергу:\n\n📍 ${city.name}${note}`, { inline_keyboard: keyboard }, env);
    await maybeShowAd(token, chatId, st, env);
    await saveState(env, key, st);
    return;
  }

  if (data.startsWith("city|")) {
    const idx = Number(data.split("|")[1]);
    const city = st.cities[idx];
    if (!city) {
      await editMessage(token, chatId, messageId, "⚠️ Стан втрачено. Напиши місто ще раз.", null, env);
      await saveState(env, key, st);
      return;
    }

    st.selectedCity = city;
    st.queues = [];
    st.selected = null;

    // show loading
    await editMessage(token, chatId, messageId, `⏳ Завантажую черги...\n\n📍 ${city.name}`, null, env);

    const r = await getQueuesFromCityUrl(city.url, env, token);
    const queues = r.queues;

    if (!queues.length) {
      const kb = { inline_keyboard: [[{ text: "🔁 Спробувати ще раз", callback_data: "retry_city_queues" }]] };
      await editMessage(token, chatId, messageId, "❌ Не вдалося завантажити черги для цього міста.\nСайт іноді лагає — натисни «Спробувати ще раз».", kb, env);
      await saveState(env, key, st);
      return;
    }

    st.queues = queues;

    const keyboard = queues.map((qq, i) => ([
      { text: qq.name, callback_data: `queue|${i}` }
    ]));

    const note = r.fromCache ? "\n\n(✅ Показую з кешу — сайт може лагати)" : "";
    await editMessage(token, chatId, messageId, `🔌 Обери чергу:\n\n📍 ${city.name}${note}`, { inline_keyboard: keyboard }, env);
    await maybeShowAd(token, chatId, st, env);

    await saveState(env, key, st);
    return;
  }

  if (data.startsWith("queue|")) {
    const idx = Number(data.split("|")[1]);
    const qq = st.queues[idx];
    if (!qq) {
      await editMessage(token, chatId, messageId, "⚠️ Стан втрачено. Напиши місто ще раз.", null, env);
      await saveState(env, key, st);
      return;
    }

    const cityName = st.selectedCity?.name || "Обране місто";
    st.selected = { cityName, queueName: qq.name, url: qq.url };

    const { text } = await buildInfo(st.selected, env);
    await editMessage(token, chatId, messageId, text, mainQueueKeyboard(env, st), env);
    await maybeShowAd(token, chatId, st, env);

    if (st.alerts?.enabled !== false) {
      await upsertSubscription(env, token, chatId, st.selected, true).catch(() => {});
    }

    await saveState(env, key, st);
    return;
  }

  if (data === "refresh") {
    if (!st.selected) {
      await editMessage(token, chatId, messageId, "⚠️ Спочатку обери місто та чергу.", null, env);
      await saveState(env, key, st);
      return;
    }
    const { text } = await buildInfo(st.selected, env);
    await editMessage(token, chatId, messageId, text, mainQueueKeyboard(env, st), env);
    await maybeShowAd(token, chatId, st, env);
    await saveState(env, key, st);
    return;
  }

  if (data === "save") {
    if (!st.selected) {
      await sendMessage(token, chatId, "⚠️ Немає вибраної черги.", null, env);
      await saveState(env, key, st);
      return;
    }
    if (!st.saved.find(x => x.url === st.selected.url)) {
      st.saved.push({ ...st.selected });
      await sendMessage(token, chatId, "⭐ Чергу збережено!", null, env);
    } else {
      await sendMessage(token, chatId, "✅ Вже збережено", null, env);
    }
    await maybeShowAd(token, chatId, st, env);
    await saveState(env, key, st);
    return;
  }

  if (data === "alerts_toggle") {
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
        await editMessage(token, chatId, messageId, text, mainQueueKeyboard(env, st), env);
      }
    } catch {}

    await saveState(env, key, st);
    return;
  }

  if (data === "my") {
    await showMyQueues(token, chatId, st, env, messageId);
    await maybeShowAd(token, chatId, st, env);
    await saveState(env, key, st);
    return;
  }

  if (data.startsWith("show|")) {
    const idx = Number(data.split("|")[1]);
    const item = st.saved[idx];
    if (!item) {
      await sendMessage(token, chatId, "⚠️ Не знайдено.", null, env);
      await saveState(env, key, st);
      return;
    }

    st.selected = { ...item };
    const { text } = await buildInfo(st.selected, env);

    const kb = {
      inline_keyboard: [
        [{ text: "🔄 Оновити", callback_data: "refresh" }],
        [{ text: "❌ Видалити", callback_data: `del|${idx}` }],
        [{ text: st.alerts.enabled ? "🔔 Алерти: увімкн." : "🔕 Алерти: вимкн.", callback_data: "alerts_toggle" }],
        [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]
      ]
    };

    await sendMessage(token, chatId, text, kb, env);
    await maybeShowAd(token, chatId, st, env);

    if (st.alerts.enabled) {
      await upsertSubscription(env, token, chatId, st.selected, true).catch(() => {});
    }

    await saveState(env, key, st);
    return;
  }

  if (data.startsWith("del|")) {
    const idx = Number(data.split("|")[1]);
    if (Number.isInteger(idx) && st.saved[idx]) {
      st.saved.splice(idx, 1);
      await sendMessage(token, chatId, "❌ Видалено", null, env);
    }
    await maybeShowAd(token, chatId, st, env);
    await saveState(env, key, st);
    return;
  }

  await saveState(env, key, st);
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
        console.log("UPDATE:", JSON.stringify(update));

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