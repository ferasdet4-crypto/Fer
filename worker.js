// ===================== IN-MEMORY STATE =====================
// key: `${token}:${chatId}`
const S = new Map();

/**
 * State shape:
 * {
 *   cities: [{name,url}],
 *   queues: [{name,url}],
 *   selectedCity: {name,url} | null,
 *   selected: {cityName, queueName, url} | null,
 *   saved: [{cityName, queueName, url}],
 *   ad: { enabled: true, frequency: 3, counter: 0, items: [{id,type,text,media}] },
 *   whitelist: Set<string>
 * }
 */

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);

      // health
      if (url.pathname === "/") {
        return new Response("Bot worker is running", { status: 200 });
      }

      // webhook /webhook/<TOKEN>
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
  }
};

// ===================== UPDATE ROUTER =====================
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

  // ignore all without text for search flow, but allow admin media ingestion
  const text = (message.text || "").trim();

  const key = stKey(token, chatId);
  const st = getState(key);
  S.set(key, st);

  console.log("TEXT:", text);

  // ADMIN: add ad by sending any media with caption "/ad"
  if (isAdmin(chatId, env) && !text && hasAnyMedia(message)) {
    // if admin sends media without caption, ignore
    return;
  }

  if (isAdmin(chatId, env) && hasAnyMedia(message)) {
    // allow: caption "/ad <optional text>"
    const cap = (message.caption || "").trim();
    if (cap.startsWith("/ad")) {
      const extraText = cap.replace(/^\/ad\s*/i, "").trim();
      const item = mediaToAdItem(message, extraText);
      if (!item) {
        await sendMessage(token, chatId, "❌ Не зміг додати рекламу. Спробуй інше медіа.");
        return;
      }
      st.ad.items.unshift(item);
      await sendMessage(token, chatId, `✅ Рекламу додано (#${item.id}).`);
      return;
    }
  }

  // /start
  if (text === "/start") {
    st.cities = [];
    st.queues = [];
    st.selectedCity = null;
    st.selected = null;

    const kb = {
      inline_keyboard: [
        [{ text: "⭐ Мої черги", callback_data: "my" }],
        [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]
      ]
    };

    let msg = "⚡ ДТЕК • Світло Графік\n\n✍️ Напиши назву міста";
    if (isAdmin(chatId, env)) msg += "\n\n👑 Адмін: /admin";
    await sendMessage(token, chatId, msg, kb);

    await maybeShowAd(token, chatId, st, env, "start");
    return;
  }

  // /my
  if (text === "/my") {
    await showMyQueues(token, chatId, st, env);
    await maybeShowAd(token, chatId, st, env, "my");
    return;
  }

  // /admin (admin only)
  if (text === "/admin" && isAdmin(chatId, env)) {
    await showAdminMenu(token, chatId, st, env);
    return;
  }

  // whitelist quick cmd for admin: /wl add 123 /wl del 123 /wl list
  if (isAdmin(chatId, env) && text.startsWith("/wl")) {
    await handleWhitelistCmd(token, chatId, st, env, text);
    return;
  }

  // minimal length
  if (!text) return;
  if (text.length < 2) {
    await sendMessage(token, chatId, "✍️ Напиши назву міста (мінімум 2 символи)");
    return;
  }

  // Search cities
  const cities = await searchCities(text);
  if (!cities.length) {
    await sendMessage(token, chatId, "❌ Місто не знайдено");
    await maybeShowAd(token, chatId, st, env, "no_city");
    return;
  }

  st.cities = cities;
  st.queues = [];
  st.selectedCity = null;
  st.selected = null;

  const keyboard = cities.slice(0, 8).map((c, i) => ([
    { text: c.name, callback_data: `city|${i}` }
  ]));

  await sendMessage(token, chatId, "📍 Обери місто:", { inline_keyboard: keyboard });
  await maybeShowAd(token, chatId, st, env, "cities");
}

// ===================== CALLBACK HANDLER =====================
async function handleCallback(q, token, env) {
  const chatId = q.message.chat.id;
  const messageId = q.message.message_id;
  const data = q.data || "";

  // always answer callback to avoid "Query is too old..."
  await answerCallback(token, q.id).catch(() => {});

  const key = stKey(token, chatId);
  const st = getState(key);
  S.set(key, st);

  // NOOP
  if (data === "noop") return;

  // ADMIN MENU
  if (data === "admin") {
    if (!isAdmin(chatId, env)) return;
    await showAdminMenu(token, chatId, st, env, messageId);
    return;
  }

  if (data.startsWith("admin|")) {
    if (!isAdmin(chatId, env)) return;
    await handleAdminAction(token, chatId, st, env, data, messageId);
    return;
  }

  // CITY PICK
  if (data.startsWith("city|")) {
    const idx = Number(data.split("|")[1]);
    const city = st.cities[idx];
    if (!city) {
      await editMessage(token, chatId, messageId, "⚠️ Стан втрачено. Напиши місто ще раз.");
      return;
    }

    st.selectedCity = city; // ✅ FIX: remember chosen city

    const queues = await getQueuesFromCityUrl(city.url);
    if (!queues.length) {
      await editMessage(token, chatId, messageId, "❌ Черги не знайдені на сторінці міста");
      return;
    }

    st.queues = queues;
    st.selected = null;

    const keyboard = queues.map((qq, i) => ([
      { text: qq.name, callback_data: `queue|${i}` }
    ]));

    await editMessage(token, chatId, messageId, `🔌 Обери чергу:\n\n📍 ${city.name}`, {
      inline_keyboard: keyboard
    });
    await maybeShowAd(token, chatId, st, env, "queues");
    return;
  }

  // QUEUE PICK
  if (data.startsWith("queue|")) {
    const idx = Number(data.split("|")[1]);
    const qq = st.queues[idx];
    if (!qq) {
      await editMessage(token, chatId, messageId, "⚠️ Стан втрачено. Напиши місто ще раз.");
      return;
    }

    const cityName = st.selectedCity?.name || "Обране місто"; // ✅ FIX: correct city/район
    st.selected = { cityName, queueName: qq.name, url: qq.url };

    const info = await buildInfo(st.selected, env);

    const kb = mainQueueKeyboard(env);
    await editMessage(token, chatId, messageId, info, kb);

    await maybeShowAd(token, chatId, st, env, "picked");
    return;
  }

  // REFRESH
  if (data === "refresh") {
    if (!st.selected) {
      await editMessage(token, chatId, messageId, "⚠️ Спочатку обери місто та чергу.");
      return;
    }
    const info = await buildInfo(st.selected, env);
    await editMessage(token, chatId, messageId, info, mainQueueKeyboard(env));
    await maybeShowAd(token, chatId, st, env, "refresh");
    return;
  }

  // SAVE
  if (data === "save") {
    if (!st.selected) {
      await sendMessage(token, chatId, "⚠️ Немає вибраної черги.");
      return;
    }
    if (!st.saved.find(x => x.url === st.selected.url)) {
      st.saved.push({ ...st.selected });
      await sendMessage(token, chatId, "⭐ Чергу збережено!");
    } else {
      await sendMessage(token, chatId, "✅ Вже збережено");
    }
    await maybeShowAd(token, chatId, st, env, "save");
    return;
  }

  // MY
  if (data === "my") {
    await showMyQueues(token, chatId, st, env, messageId);
    await maybeShowAd(token, chatId, st, env, "my_btn");
    return;
  }

  // SHOW SAVED
  if (data.startsWith("show|")) {
    const idx = Number(data.split("|")[1]);
    const item = st.saved[idx];
    if (!item) {
      await sendMessage(token, chatId, "⚠️ Не знайдено.");
      return;
    }

    st.selected = { ...item };
    const info = await buildInfo(st.selected, env);

    const kb = {
      inline_keyboard: [
        [{ text: "🔄 Оновити", callback_data: "refresh" }],
        [{ text: "❌ Видалити", callback_data: `del|${idx}` }],
        [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]
      ]
    };

    await sendMessage(token, chatId, info, kb);
    await maybeShowAd(token, chatId, st, env, "show_saved");
    return;
  }

  // DELETE SAVED
  if (data.startsWith("del|")) {
    const idx = Number(data.split("|")[1]);
    if (Number.isInteger(idx) && st.saved[idx]) {
      st.saved.splice(idx, 1);
      await sendMessage(token, chatId, "❌ Видалено");
    }
    await maybeShowAd(token, chatId, st, env, "del");
    return;
  }
}

// ===================== KEYBOARDS =====================
function mainQueueKeyboard(env) {
  return {
    inline_keyboard: [
      [{ text: "🔄 Оновити", callback_data: "refresh" }],
      [{ text: "⭐ Зберегти", callback_data: "save" }],
      [{ text: "📋 Мої черги", callback_data: "my" }],
      [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]
    ]
  };
}

// ===================== BEZSVITLA: SEARCH CITIES =====================
async function searchCities(query) {
  const url = "https://bezsvitla.com.ua/search-locality?q=" + encodeURIComponent(query);
  const res = await fetch(url, {
    headers: {
      "Accept": "application/json",
      "X-Requested-With": "XMLHttpRequest",
      "User-Agent": "Mozilla/5.0",
      "Referer": "https://bezsvitla.com.ua/"
    }
  });
  if (!res.ok) return [];
  let data;
  try { data = await res.json(); } catch { return []; }
  if (!Array.isArray(data)) return [];
  return data
    .filter(x => x.name && x.url)
    .map(x => ({ name: x.name, url: x.url }))
    .slice(0, 8);
}

// ===================== BEZSVITLA: QUEUES FROM CITY PAGE =====================
async function getQueuesFromCityUrl(cityUrl) {
  const res = await fetch(cityUrl, {
    headers: {
      "User-Agent": "Mozilla/5.0",
      "Accept-Language": "uk-UA,uk;q=0.9"
    }
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
}

// ===================== PARSE + STATUS (🟢/🔴) + TODAY/TOMORROW =====================
async function buildInfo(sel, env) {
  const res = await fetch(sel.url, {
    headers: {
      "User-Agent": "Mozilla/5.0",
      "Accept-Language": "uk-UA,uk;q=0.9"
    }
  });
  if (!res.ok) {
    return `📍 ${sel.cityName}\n🔌 ${sel.queueName}\n\n❌ Не вдалося завантажити сторінку черги`;
  }
  const html = await res.text();

  const items = extractLiItems(html);
  const blocks = (items.length ? items : extractTimeOnly(html)).map(x => normalizeBlock(x));

  const nowMin = nowMinutesUA(env);

  let statusLine = "❓ Нема даних";
  let nextLine = "";

  const current = findCurrentBlock(blocks, nowMin);
  if (current) {
    if (current.on === true) {
      statusLine = "🟢 ЗАРАЗ Є СВІТЛО";
      nextLine = `⏰ Вимкнуть о ${current.end}\n⏳ Через ${fmtDelta(current.endMin - nowMin)}`;
    } else if (current.on === false) {
      statusLine = "🔴 ЗАРАЗ НЕМА СВІТЛА";
      nextLine = `⏰ Увімкнуть о ${current.end}\n⏳ Через ${fmtDelta(current.endMin - nowMin)}`;
    } else {
      statusLine = "🟡 ЗАРАЗ: невідомо";
      nextLine = `⏳ До ${current.end}: ${fmtDelta(current.endMin - nowMin)}`;
    }
  } else {
    const next = findNextBlock(blocks, nowMin);
    if (next) {
      // we don't know current status reliably if not inside a block; assume "нема"
      statusLine = "🔴 ЗАРАЗ НЕМА СВІТЛА";
      nextLine = `⏰ Наступна зміна о ${next.start}\n⏳ Через ${fmtDelta(next.startMin - nowMin)}`;
    }
  }

  // Heuristic split: many pages have 12 blocks per day. If > 12, show 12/12.
  const today = blocks.slice(0, 12);
  const tomorrow = blocks.slice(12, 24);

  let text =
`📍 ${sel.cityName}
🔌 ${sel.queueName}
${statusLine}
${nextLine}

📊 СЬОГОДНІ:
${fmtBlocks(today)}`;

  if (tomorrow.length) {
    text += `\n\n📅 ЗАВТРА:\n${fmtBlocks(tomorrow)}`;
  }

  return text.trim();
}

function fmtBlocks(arr) {
  if (!arr.length) return "❌ Нема даних";
  return arr
    .map(b => `${b.on === true ? "🟢" : b.on === false ? "🔴" : "🟡"} ${b.start} – ${b.end}`)
    .join("\n");
}

function fmtDelta(min) {
  const m = Math.max(0, min);
  const h = Math.floor(m / 60);
  const r = m % 60;
  return `${h} год ${r} хв`;
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

function toMin(t) {
  const [h, m] = t.split(":").map(Number);
  return h * 60 + m;
}

// ===================== MY QUEUES UI =====================
async function showMyQueues(token, chatId, st, env, editMessageId = null) {
  if (!st.saved || !st.saved.length) {
    if (editMessageId) {
      await editMessage(token, chatId, editMessageId, "📭 У тебе ще немає збережених черг");
    } else {
      await sendMessage(token, chatId, "📭 У тебе ще немає збережених черг");
    }
    return;
  }

  const keyboard = st.saved.map((q, i) => ([{
    text: `${q.cityName} | ${q.queueName}`,
    callback_data: `show|${i}`
  }]));

  const kb = { inline_keyboard: keyboard.concat([[{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]]) };

  if (editMessageId) {
    await editMessage(token, chatId, editMessageId, "⭐ Мої черги:", kb);
  } else {
    await sendMessage(token, chatId, "⭐ Мої черги:", kb);
  }
}

// ===================== ADS + SPONSOR =====================
function initAdState() {
  return {
    enabled: true,
    frequency: 3,   // show ad every N "actions"
    counter: 0,
    items: [
      // default ad (text)
      {
        id: "default",
        type: "text",
        text: "📢 Тут може бути твоя реклама. Натисни «Стати спонсором» 👇"
      }
    ]
  };
}

async function maybeShowAd(token, chatId, st, env, reason) {
  // no ads for admin or whitelist
  if (isAdmin(chatId, env)) return;
  if (st.whitelist && st.whitelist.has(String(chatId))) return;

  if (!st.ad) st.ad = initAdState();
  if (!st.ad.enabled) return;

  st.ad.counter = (st.ad.counter || 0) + 1;

  // show ad every N actions only
  if (st.ad.counter % st.ad.frequency !== 0) return;

  const item = (st.ad.items && st.ad.items.length) ? st.ad.items[0] : null;
  if (!item) return;

  // show ad
  await sendAdItem(token, chatId, item, env);
}

async function sendAdItem(token, chatId, item, env) {
  const sponsorBtn = [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }];

  if (item.type === "text") {
    await sendMessage(token, chatId, item.text, { inline_keyboard: [sponsorBtn] });
    return;
  }

  // media types (photo/video/document)
  // We send with caption and sponsor button
  const caption = item.text || "📢 Реклама";
  const markup = { inline_keyboard: [sponsorBtn] };

  if (item.type === "photo") {
    await tgCall(token, "sendPhoto", {
      chat_id: chatId,
      photo: item.file_id,
      caption,
      reply_markup: markup
    });
    return;
  }
  if (item.type === "video") {
    await tgCall(token, "sendVideo", {
      chat_id: chatId,
      video: item.file_id,
      caption,
      reply_markup: markup
    });
    return;
  }
  if (item.type === "document") {
    await tgCall(token, "sendDocument", {
      chat_id: chatId,
      document: item.file_id,
      caption,
      reply_markup: markup
    });
    return;
  }

  // fallback
  await sendMessage(token, chatId, "📢 Реклама", { inline_keyboard: [sponsorBtn] });
}

// ===================== ADMIN PANEL =====================
function isAdmin(chatId, env) {
  return String(chatId) === String(env.ADMIN_ID);
}

async function showAdminMenu(token, chatId, st, env, editMessageId = null) {
  if (!st.ad) st.ad = initAdState();
  if (!st.whitelist) st.whitelist = new Set();

  const status = st.ad.enabled ? "✅ увімкнена" : "⛔ вимкнена";
  const freq = st.ad.frequency || 3;
  const adsCount = st.ad.items?.length || 0;
  const wlCount = st.whitelist.size || 0;

  const text =
`👑 Адмін-панель

📢 Реклама: ${status}
🔁 Частота: раз на ${freq} дій
🧾 Оголошень: ${adsCount}
🚫 Без реклами: ${wlCount} юзерів

➕ Додати рекламу:
— надішли будь-яке медіа з підписом:
   /ad Текст під медіа
або
   /ad   (без тексту)`;

  const kb = {
    inline_keyboard: [
      [{ text: st.ad.enabled ? "⛔ Вимкнути рекламу" : "✅ Увімкнути рекламу", callback_data: "admin|toggle_ad" }],
      [{ text: "➖ Частота рідше", callback_data: "admin|freq_down" }, { text: "➕ Частота частіше", callback_data: "admin|freq_up" }],
      [{ text: "🗑 Очистити рекламу", callback_data: "admin|clear_ads" }],
      [{ text: "🚫 Whitelist меню", callback_data: "admin|wl_menu" }],
      [{ text: "🔙 Закрити", callback_data: "admin|close" }]
    ]
  };

  if (editMessageId) {
    await editMessage(token, chatId, editMessageId, text, kb);
  } else {
    await sendMessage(token, chatId, text, kb);
  }
}

async function handleAdminAction(token, chatId, st, env, data, messageId) {
  if (!st.ad) st.ad = initAdState();
  if (!st.whitelist) st.whitelist = new Set();

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
    await editMessage(token, chatId, messageId, "🗑 Рекламу очищено. Залишено дефолтне оголошення.");
    await showAdminMenu(token, chatId, st, env);
    return;
  }

  if (act === "wl_menu") {
    await showWhitelistMenu(token, chatId, st, env, messageId);
    return;
  }

  if (act === "close") {
    await editMessage(token, chatId, messageId, "✅ Закрито.");
    return;
  }
}

async function showWhitelistMenu(token, chatId, st, env, messageId) {
  const list = [...(st.whitelist || new Set())].slice(0, 30);
  const text =
`🚫 Whitelist (без реклами)

Додати/видалити командою:
 /wl add <id>
 /wl del <id>
 /wl list

Зараз у списку: ${st.whitelist.size}

${list.length ? list.map(x => `• ${x}`).join("\n") : "— порожньо —"}`;

  const kb = {
    inline_keyboard: [
      [{ text: "🔙 Назад", callback_data: "admin" }]
    ]
  };

  await editMessage(token, chatId, messageId, text, kb);
}

async function handleWhitelistCmd(token, chatId, st, env, text) {
  if (!st.whitelist) st.whitelist = new Set();

  const parts = text.split(/\s+/).filter(Boolean);
  const sub = (parts[1] || "").toLowerCase();
  const id = parts[2];

  if (sub === "add" && id) {
    st.whitelist.add(String(id));
    await sendMessage(token, chatId, `✅ Додано в whitelist: ${id}`);
    return;
  }
  if (sub === "del" && id) {
    st.whitelist.delete(String(id));
    await sendMessage(token, chatId, `🗑 Видалено з whitelist: ${id}`);
    return;
  }
  if (sub === "list") {
    const list = [...st.whitelist].slice(0, 60);
    await sendMessage(token, chatId, `🚫 Whitelist (${st.whitelist.size})\n\n${list.length ? list.join("\n") : "— порожньо —"}`);
    return;
  }

  await sendMessage(token, chatId, "Команди:\n/wl add <id>\n/wl del <id>\n/wl list");
}

// ===================== MEDIA -> AD ITEM =====================
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

// ===================== TELEGRAM API =====================
async function sendMessage(token, chatId, text, replyMarkup = null) {
  const payload = { chat_id: chatId, text };
  if (replyMarkup) payload.reply_markup = replyMarkup;

  const out = await tgCall(token, "sendMessage", payload);
  console.log("sendMessage:", out);
}

async function editMessage(token, chatId, messageId, text, replyMarkup = null) {
  const payload = { chat_id: chatId, message_id: messageId, text };
  if (replyMarkup) payload.reply_markup = replyMarkup;

  const out = await tgCall(token, "editMessageText", payload);
  console.log("editMessageText:", out);
}

async function answerCallback(token, callbackQueryId) {
  return tgCall(token, "answerCallbackQuery", { callback_query_id: callbackQueryId });
}

async function tgCall(token, method, payload) {
  const res = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const json = await res.json().catch(() => ({}));
  return json;
}

// ===================== ENV PARSER =====================
function parseTokens(raw) {
  // supports:
  // 1) "token1,token2"
  // 2) ["token1","token2"]
  if (!raw) return [];
  if (Array.isArray(raw)) return raw;
  const s = String(raw).trim();
  if (s.startsWith("[") && s.endsWith("]")) {
    try {
      const arr = JSON.parse(s);
      return Array.isArray(arr) ? arr.map(String) : [];
    } catch {
      return [];
    }
  }
  return s.split(",").map(x => x.trim()).filter(Boolean);
}

function stKey(token, chatId) {
  return `${token}:${chatId}`;
}

function getState(key) {
  const st = S.get(key);
  if (st) return st;
  return {
    cities: [],
    queues: [],
    selectedCity: null,
    selected: null,
    saved: [],
    ad: initAdState(),
    whitelist: new Set()
  };
}

// ===================== UA TIME =====================
function nowMinutesUA(env) {
  const offset = Number(env.UA_TZ_OFFSET_MIN || 120); // UTC+2 default
  const d = new Date();
  const utcMin = d.getUTCHours() * 60 + d.getUTCMinutes();
  return (utcMin + offset + 1440) % 1440;
}