/**
 * Cloudflare Worker Telegram Bot (Bezsvitla / DTEK schedules) — FULL VERSION
 * ✅ Multi-bot via /webhook/<TOKEN> and env.BOT_TOKENS
 * ✅ Persistent state in KV (no in-memory Map issues)
 * ✅ Admin panel with buttons (ads, whitelist, alerts, stats)
 * ✅ Ads (text or media), frequency control, sponsor link
 * ✅ Saved queues
 * ✅ Alerts via Cron (scheduled) — notify N minutes before next change (default 20)
 *
 * REQUIRED BINDINGS (Cloudflare):
 *  - KV namespace binding: STATE_KV
 *  - (Optional) UA_TZ_OFFSET_MIN (default 120)
 *  - BOT_TOKENS: "token1,token2" OR JSON '["token1","token2"]'
 *  - ADMIN_ID: Telegram numeric chat id (string ok)
 *  - SPONSOR_LINK: https://...
 *
 * OPTIONAL ENV:
 *  - STATE_TTL_SEC (default 60*60*24*45) 45 days
 *  - ALERT_MIN_BEFORE (default 20)
 *  - ALERT_WINDOW_MIN (default 6)  // cron window tolerance
 *  - ALERT_MAX_PER_CRON (default 300) // safety cap
 *  - FETCH_TIMEOUT_MS (default 12000)
 *
 * CRON:
 *  - Set cron in Cloudflare (e.g. every 5 minutes). The scheduled() handler will send alerts.
 */

const VERSION = "kv-cron-full-1.0.0";

// ===================== UTIL: SAFE JSON =====================
function jparse(s, fallback) {
  try { return JSON.parse(s); } catch { return fallback; }
}
function jstringify(obj) {
  try { return JSON.stringify(obj); } catch { return "{}"; }
}

// ===================== ENV PARSER =====================
function parseTokens(raw) {
  // supports:
  // 1) "token1,token2"
  // 2) ["token1","token2"]
  if (!raw) return [];
  if (Array.isArray(raw)) return raw.map(String);
  const s = String(raw).trim();
  if (s.startsWith("[") && s.endsWith("]")) {
    const arr = jparse(s, []);
    return Array.isArray(arr) ? arr.map(String) : [];
  }
  return s.split(",").map(x => x.trim()).filter(Boolean);
}

function envNum(env, name, def) {
  const v = env?.[name];
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

function stKey(token, chatId) {
  return `${token}:${chatId}`;
}

function ttlSec(env) {
  return envNum(env, "STATE_TTL_SEC", 60 * 60 * 24 * 45);
}

function uaOffsetMin(env) {
  return envNum(env, "UA_TZ_OFFSET_MIN", 120); // UTC+2 default
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

// ===================== STATE DEFAULTS =====================
function initAdState() {
  return {
    enabled: true,
    frequency: 3,  // show ad every N actions
    counter: 0,
    items: [
      { id: "default", type: "text", text: "📢 Тут може бути твоя реклама. Натисни «Стати спонсором» 👇" }
    ]
  };
}

function defaultState(env) {
  return {
    v: 1,
    cities: [],
    queues: [],
    selectedCity: null,
    selected: null,
    saved: [],
    ad: initAdState(),
    whitelist: [], // store as array; convert to Set when needed
    admin: { mode: null, tmp: null }, // mode: "await_ad_text" | "await_ad_media" | "await_wl_add" | "await_wl_del"
    stats: { actions: 0 },
    alerts: { enabled: true } // per-user toggle
  };
}

// ===================== KV STATE IO =====================
async function loadState(env, key) {
  const raw = await env.STATE_KV.get(`st:${key}`);
  if (!raw) return defaultState(env);
  const obj = jparse(raw, null);
  if (!obj || typeof obj !== "object") return defaultState(env);

  // migrations / fills
  if (!obj.ad) obj.ad = initAdState();
  if (!obj.saved) obj.saved = [];
  if (!obj.cities) obj.cities = [];
  if (!obj.queues) obj.queues = [];
  if (!obj.admin) obj.admin = { mode: null, tmp: null };
  if (!obj.whitelist) obj.whitelist = [];
  if (!obj.stats) obj.stats = { actions: 0 };
  if (!obj.alerts) obj.alerts = { enabled: true };

  return obj;
}

async function saveState(env, key, st) {
  const ttl = ttlSec(env);
  await env.STATE_KV.put(`st:${key}`, jstringify(st), { expirationTtl: ttl });
}

function wlSet(st) {
  return new Set((st.whitelist || []).map(String));
}
function setWhitelistFromSet(st, s) {
  st.whitelist = [...s].map(String);
}

// ===================== USER COUNTER (best-effort) =====================
async function ensureUserRegistered(env, token, chatId) {
  // best-effort unique registration; not strictly atomic
  const uKey = `user:${token}:${chatId}`;
  const exists = await env.STATE_KV.get(uKey);
  if (exists) return;

  await env.STATE_KV.put(uKey, "1", { expirationTtl: ttlSec(env) });

  const cKey = `users_count:${token}`;
  const raw = await env.STATE_KV.get(cKey);
  const cur = Number(raw || 0);
  const next = Number.isFinite(cur) ? cur + 1 : 1;
  await env.STATE_KV.put(cKey, String(next), { expirationTtl: ttlSec(env) });
}

// ===================== TIME (UA LOCAL derived from offset) =====================
function uaLocalNow(env) {
  const offMin = uaOffsetMin(env);
  const nowUtcMs = Date.now();
  const localMs = nowUtcMs + offMin * 60_000;
  const d = new Date(localMs);
  const minutes = d.getUTCHours() * 60 + d.getUTCMinutes(); // since d is shifted, use getUTC*
  return { nowUtcMs, localMs, offMin, minutes, dLocal: d };
}

function localDayStartMs(localMs) {
  // localMs is "UTC ms + offset", so its "UTC day" corresponds to local day.
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

// ===================== BEZSVITLA: SEARCH CITIES =====================
async function searchCities(query, env) {
  const url = "https://bezsvitla.com.ua/search-locality?q=" + encodeURIComponent(query);
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), fetchTimeoutMs(env));
  try {
    const res = await fetch(url, {
      headers: {
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://bezsvitla.com.ua/"
      },
      signal: ctrl.signal
    });
    if (!res.ok) return [];
    const data = await res.json().catch(() => null);
    if (!Array.isArray(data)) return [];
    return data
      .filter(x => x && x.name && x.url)
      .map(x => ({ name: x.name, url: x.url }))
      .slice(0, 8);
  } finally {
    clearTimeout(t);
  }
}

// ===================== BEZSVITLA: QUEUES FROM CITY PAGE =====================
async function getQueuesFromCityUrl(cityUrl, env) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), fetchTimeoutMs(env));
  try {
    const res = await fetch(cityUrl, {
      headers: { "User-Agent": "Mozilla/5.0", "Accept-Language": "uk-UA,uk;q=0.9" },
      signal: ctrl.signal
    });
    if (!res.ok) return [];
    const html = await res.text();

    // links to /cherha-
    const matches = [...html.matchAll(/href="([^"]*\/cherha-[^"]+)"/g)];
    const urls = [...new Set(matches.map(m => m[1]).map(u => u.startsWith("http") ? u : ("https://bezsvitla.com.ua" + u)))];

    return urls.map(u => {
      const code = u.split("cherha-")[1].replace(/-/g, ".");
      return { name: `Черга ${code}`, url: u };
    });
  } finally {
    clearTimeout(t);
  }
}

// ===================== PARSERS =====================
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

// ===================== BUILD INFO + NEXT CHANGE META =====================
async function fetchScheduleBlocks(url, env) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), fetchTimeoutMs(env));
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0", "Accept-Language": "uk-UA,uk;q=0.9" },
      signal: ctrl.signal
    });
    if (!res.ok) return { ok: false, blocks: [], html: "" };
    const html = await res.text();
    const items = extractLiItems(html);
    const blocks = (items.length ? items : extractTimeOnly(html)).map(x => normalizeBlock(x));
    return { ok: true, blocks, html };
  } finally {
    clearTimeout(t);
  }
}

function computeStatus(blocks, nowMin) {
  let statusLine = "❓ Нема даних";
  let nextLine = "";
  let nextChangeMin = null;
  let nextChangeType = null; // "on" | "off" | "unknown"
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
    return { statusLine, nextLine, nextChangeMin, nextChangeType, nextChangeAtText, current, nextBlock: null };
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
    return { statusLine, nextLine, nextChangeMin, nextChangeType, nextChangeAtText, current: null, nextBlock: next };
  }

  return { statusLine, nextLine, nextChangeMin, nextChangeType, nextChangeAtText, current: null, nextBlock: null };
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

// ===================== ADS =====================
function isAdmin(chatId, env) {
  return String(chatId) === String(env.ADMIN_ID);
}

function hasAnyMedia(message) {
  return Boolean(message.photo || message.video || message.document);
}

function mediaToAdItem(message, extraText) {
  const id = String(Date.now());

  if (message.photo && message.photo.length) {
    const best = message.photo[message.photo.length - 1];
    return { id, type: "photo", file_id: best.file_id, text: extraText || "" };
  }
  if (message.video) {
    return { id, type: "video", file_id: message.video.file_id, text: extraText || "" };
  }
  if (message.document) {
    return { id, type: "document", file_id: message.document.file_id, text: extraText || "" };
  }
  return null;
}

async function sendAdItem(token, chatId, item, env) {
  const sponsorBtn = [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }];

  if (item.type === "text") {
    await sendMessage(token, chatId, item.text, { inline_keyboard: [sponsorBtn] }, env);
    return;
  }

  const caption = item.text || "📢 Реклама";
  const markup = { inline_keyboard: [sponsorBtn] };

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

  await sendMessage(token, chatId, "📢 Реклама", { inline_keyboard: [sponsorBtn] }, env);
}

async function maybeShowAd(token, chatId, st, env, reason) {
  if (isAdmin(chatId, env)) return;

  const wl = wlSet(st);
  if (wl.has(String(chatId))) return;

  if (!st.ad) st.ad = initAdState();
  if (!st.ad.enabled) return;

  st.ad.counter = (st.ad.counter || 0) + 1;
  if ((st.ad.counter % (st.ad.frequency || 3)) !== 0) return;

  const item = (st.ad.items && st.ad.items.length) ? st.ad.items[0] : null;
  if (!item) return;

  await sendAdItem(token, chatId, item, env);
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

// ===================== ADMIN PANEL (buttons) =====================
async function showAdminMenu(token, chatId, st, env, editMessageId = null) {
  if (!st.ad) st.ad = initAdState();

  const wl = wlSet(st);

  const status = st.ad.enabled ? "✅ увімкнена" : "⛔ вимкнена";
  const freq = st.ad.frequency || 3;
  const adsCount = st.ad.items?.length || 0;

  const usersCount = await env.STATE_KV.get(`users_count:${token}`).catch(() => null);
  const users = usersCount ? Number(usersCount) : 0;

  const alertsOn = st.alerts?.enabled !== false;

  const text =
`👑 Адмін-панель

🧩 Версія: ${VERSION}
📊 Юзерів (оцінка): ${users}

📢 Реклама: ${status}
🔁 Частота: раз на ${freq} дій
🧾 Оголошень: ${adsCount}

🔔 Алерти для тебе: ${alertsOn ? "✅ увімкнені" : "⛔ вимкнені"}
🚫 Whitelist: ${wl.size} юзерів`;

  const kb = {
    inline_keyboard: [
      [{ text: st.ad.enabled ? "⛔ Вимкнути рекламу" : "✅ Увімкнути рекламу", callback_data: "admin|toggle_ad" }],
      [{ text: "➖ Частота рідше", callback_data: "admin|freq_down" }, { text: "➕ Частота частіше", callback_data: "admin|freq_up" }],
      [{ text: "➕ Додати текст реклами", callback_data: "admin|add_ad_text" }],
      [{ text: "➕ Додати медіа реклами", callback_data: "admin|add_ad_media" }],
      [{ text: "🗑 Очистити рекламу", callback_data: "admin|clear_ads" }],
      [{ text: "🚫 Whitelist меню", callback_data: "admin|wl_menu" }],
      [{ text: "📈 Статистика", callback_data: "admin|stats" }],
      [{ text: "🔙 Закрити", callback_data: "admin|close" }]
    ]
  };

  if (editMessageId) await editMessage(token, chatId, editMessageId, text, kb, env);
  else await sendMessage(token, chatId, text, kb, env);
}

async function showWhitelistMenu(token, chatId, st, env, messageId) {
  const wl = wlSet(st);
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

  await editMessage(token, chatId, messageId, text, kb, env);
}

async function showStats(token, chatId, st, env, messageId) {
  const usersCount = await env.STATE_KV.get(`users_count:${token}`).catch(() => null);
  const users = usersCount ? Number(usersCount) : 0;

  const wl = wlSet(st);
  const ads = st.ad?.items?.length || 0;
  const saved = st.saved?.length || 0;

  const text =
`📈 Статистика

👥 Юзерів (оцінка): ${users}
⭐ Збережено у тебе: ${saved}
📢 Оголошень: ${ads}
🚫 Whitelist: ${wl.size}

⚙️ Дій (у тебе): ${st.stats?.actions || 0}`;

  const kb = { inline_keyboard: [[{ text: "🔙 Назад", callback_data: "admin" }]] };
  await editMessage(token, chatId, messageId, text, kb, env);
}

async function handleAdminAction(token, chatId, st, env, data, messageId) {
  if (!st.ad) st.ad = initAdState();
  if (!st.admin) st.admin = { mode: null, tmp: null };

  const act = data.split("|")[1];

  if (act === "toggle_ad") {
    st.ad.enabled = !st.ad.enabled;
    await showAdminMenu(token, chatId, st, env, messageId);
    return;
  }

  if (act === "freq_up") {
    st.ad.frequency = Math.max(1, (st.ad.frequency || 3) - 1);
    await showAdminMenu(token, chatId, st, env, messageId);
    return;
  }

  if (act === "freq_down") {
    st.ad.frequency = Math.min(20, (st.ad.frequency || 3) + 1);
    await showAdminMenu(token, chatId, st, env, messageId);
    return;
  }

  if (act === "clear_ads") {
    st.ad.items = initAdState().items;
    await editMessage(token, chatId, messageId, "🗑 Рекламу очищено. Залишено дефолтне оголошення.", null, env);
    await showAdminMenu(token, chatId, st, env);
    return;
  }

  if (act === "add_ad_text") {
    st.admin.mode = "await_ad_text";
    await editMessage(token, chatId, messageId, "✍️ Надішли наступним повідомленням текст реклами.\n(Або /cancel щоб скасувати)", null, env);
    return;
  }

  if (act === "add_ad_media") {
    st.admin.mode = "await_ad_media";
    await editMessage(token, chatId, messageId, "📎 Надішли фото/відео/документ.\nПідпис (caption) буде текстом реклами.\n(Або /cancel щоб скасувати)", null, env);
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

// ===================== ALERTS SUBSCRIPTIONS (KV indexed) =====================
async function sha1Hex(text) {
  const data = new TextEncoder().encode(text);
  const buf = await crypto.subtle.digest("SHA-1", data);
  const arr = Array.from(new Uint8Array(buf));
  return arr.map(b => b.toString(16).padStart(2, "0")).join("");
}

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

// ===================== ROUTERS =====================
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

  st.stats = st.stats || { actions: 0 };
  st.stats.actions = (st.stats.actions || 0) + 1;

  const text = (message.text || "").trim();
  const cap = (message.caption || "").trim();

  // Global cancel for admin modes
  if (isAdmin(chatId, env) && (text === "/cancel" || text === "cancel")) {
    st.admin = st.admin || { mode: null, tmp: null };
    st.admin.mode = null;
    st.admin.tmp = null;
    await sendMessage(token, chatId, "✅ Скасовано.", null, env);
    await saveState(env, key, st);
    return;
  }

  // Admin flow: waiting for ad text
  if (isAdmin(chatId, env) && st.admin?.mode === "await_ad_text" && text) {
    st.ad.items.unshift({ id: String(Date.now()), type: "text", text });
    st.admin.mode = null;
    await sendMessage(token, chatId, "✅ Текст реклами додано.", null, env);
    await saveState(env, key, st);
    return;
  }

  // Admin flow: waiting for ad media
  if (isAdmin(chatId, env) && st.admin?.mode === "await_ad_media" && hasAnyMedia(message)) {
    const item = mediaToAdItem(message, cap);
    if (!item) {
      await sendMessage(token, chatId, "❌ Не зміг додати рекламу. Спробуй інше медіа.", null, env);
      await saveState(env, key, st);
      return;
    }
    st.ad.items.unshift(item);
    st.admin.mode = null;
    await sendMessage(token, chatId, `✅ Рекламу додано (#${item.id}).`, null, env);
    await saveState(env, key, st);
    return;
  }

  // Admin whitelist add/del modes
  if (isAdmin(chatId, env) && (st.admin?.mode === "await_wl_add" || st.admin?.mode === "await_wl_del") && text) {
    const id = text.replace(/[^\d-]/g, "").trim();
    const wl = wlSet(st);
    if (!id) {
      await sendMessage(token, chatId, "⚠️ Надішли тільки числовий ID.", null, env);
      await saveState(env, key, st);
      return;
    }
    if (st.admin.mode === "await_wl_add") {
      wl.add(String(id));
      setWhitelistFromSet(st, wl);
      st.admin.mode = null;
      await sendMessage(token, chatId, `✅ Додано в whitelist: ${id}`, null, env);
      await saveState(env, key, st);
      return;
    }
    if (st.admin.mode === "await_wl_del") {
      wl.delete(String(id));
      setWhitelistFromSet(st, wl);
      st.admin.mode = null;
      await sendMessage(token, chatId, `🗑 Видалено з whitelist: ${id}`, null, env);
      await saveState(env, key, st);
      return;
    }
  }

  // legacy admin media ingestion via "/ad" caption
  if (isAdmin(chatId, env) && hasAnyMedia(message) && cap.startsWith("/ad")) {
    const extraText = cap.replace(/^\/ad\s*/i, "").trim();
    const item = mediaToAdItem(message, extraText);
    if (!item) {
      await sendMessage(token, chatId, "❌ Не зміг додати рекламу. Спробуй інше медіа.", null, env);
      await saveState(env, key, st);
      return;
    }
    st.ad.items.unshift(item);
    await sendMessage(token, chatId, `✅ Рекламу додано (#${item.id}).`, null, env);
    await saveState(env, key, st);
    return;
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
    await maybeShowAd(token, chatId, st, env, "start");

    await saveState(env, key, st);
    return;
  }

  // /my
  if (text === "/my") {
    await showMyQueues(token, chatId, st, env);
    await maybeShowAd(token, chatId, st, env, "my");
    await saveState(env, key, st);
    return;
  }

  // /admin
  if (text === "/admin" && isAdmin(chatId, env)) {
    await showAdminMenu(token, chatId, st, env);
    await saveState(env, key, st);
    return;
  }

  // minimal length
  if (!text) {
    await saveState(env, key, st);
    return;
  }
  if (text.length < 2) {
    await sendMessage(token, chatId, "✍️ Напиши назву міста (мінімум 2 символи)", null, env);
    await saveState(env, key, st);
    return;
  }

  // Search cities
  const cities = await searchCities(text, env);
  if (!cities.length) {
    await sendMessage(token, chatId, "❌ Місто не знайдено", null, env);
    await maybeShowAd(token, chatId, st, env, "no_city");
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
  await maybeShowAd(token, chatId, st, env, "cities");

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

  st.stats = st.stats || { actions: 0 };
  st.stats.actions = (st.stats.actions || 0) + 1;

  if (data === "noop") { await saveState(env, key, st); return; }

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

  if (data.startsWith("city|")) {
    const idx = Number(data.split("|")[1]);
    const city = st.cities[idx];
    if (!city) {
      await editMessage(token, chatId, messageId, "⚠️ Стан втрачено. Напиши місто ще раз.", null, env);
      await saveState(env, key, st);
      return;
    }

    st.selectedCity = city;

    const queues = await getQueuesFromCityUrl(city.url, env);
    if (!queues.length) {
      await editMessage(token, chatId, messageId, "❌ Черги не знайдені на сторінці міста", null, env);
      await saveState(env, key, st);
      return;
    }

    st.queues = queues;
    st.selected = null;

    const keyboard = queues.map((qq, i) => ([
      { text: qq.name, callback_data: `queue|${i}` }
    ]));

    await editMessage(token, chatId, messageId, `🔌 Обери чергу:\n\n📍 ${city.name}`, { inline_keyboard: keyboard }, env);
    await maybeShowAd(token, chatId, st, env, "queues");

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
    await maybeShowAd(token, chatId, st, env, "picked");

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
    await maybeShowAd(token, chatId, st, env, "refresh");
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
    await maybeShowAd(token, chatId, st, env, "save");
    await saveState(env, key, st);
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
        await editMessage(token, chatId, messageId, text, mainQueueKeyboard(env, st), env);
      }
    } catch {}

    await saveState(env, key, st);
    return;
  }

  if (data === "my") {
    await showMyQueues(token, chatId, st, env, messageId);
    await maybeShowAd(token, chatId, st, env, "my_btn");
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
        [{ text: st.alerts?.enabled !== false ? "🔔 Алерти: увімкн." : "🔕 Алерти: вимкн.", callback_data: "alerts_toggle" }],
        [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]
      ]
    };

    await sendMessage(token, chatId, text, kb, env);
    await maybeShowAd(token, chatId, st, env, "show_saved");

    if (st.alerts?.enabled !== false) {
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
    await maybeShowAd(token, chatId, st, env, "del");
    await saveState(env, key, st);
    return;
  }

  await saveState(env, key, st);
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
