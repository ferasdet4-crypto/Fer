// ===================== IN-MEMORY STATE =====================
const S = new Map();

/**
 * State shape:
 * {
 *   cities: [{name,url}],
 *   queues: [{name,url}],
 *   selectedCity: {name,url} | null,
 *   selected: {cityName, queueName, url} | null,
 *   saved: [{cityName, queueName, url}],
 *   ad: { enabled, frequency, counter, items },
 *   whitelist: Set<string>
 * }
 */

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);

      if (url.pathname === "/") {
        return new Response("Bot worker is running", { status: 200 });
      }

      if (url.pathname.startsWith("/webhook/")) {
        const token = url.pathname.split("/")[2];
        const tokens = parseTokens(env.BOT_TOKENS);
        if (!tokens.includes(token)) return new Response("Invalid token", { status: 403 });

        const update = await request.json();
        await handleUpdate(update, token, env);
        return new Response("OK");
      }

      return new Response("Not found", { status: 404 });
    } catch (e) {
      console.log("FATAL", e);
      return new Response("Worker error", { status: 500 });
    }
  },

  // ===================== CRON =====================
  async scheduled(event, env) {
    await processAlerts(env);
  }
};

// ===================== UPDATE ROUTER =====================
async function handleUpdate(update, token, env) {
  if (update.message) await handleMessage(update.message, token, env);
  if (update.callback_query) await handleCallback(update.callback_query, token, env);
}

// ===================== MESSAGE HANDLER =====================
async function handleMessage(message, token, env) {
  const chatId = message.chat.id;
  const text = (message.text || "").trim();

  // статистика
  await env.STATS_KV.put(`u:${chatId}`, Date.now().toString(), { expirationTtl: 60 * 60 * 24 * 30 });

  const key = stKey(token, chatId);
  const st = getState(key);
  S.set(key, st);

  if (text === "/start") {
    await sendMessage(token, chatId,
      "⚡ ДТЕК • Світло Графік\n\n✍️ Напиши назву міста",
      {
        inline_keyboard: [
          [{ text: "⭐ Мої черги", callback_data: "my" }],
          [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]
        ]
      }
    );
    return;
  }

  if (text === "/admin" && isAdmin(chatId, env)) {
    await showAdminMenu(token, chatId, st, env);
    return;
  }

  if (text.length < 2) {
    await sendMessage(token, chatId, "✍️ Введи назву міста");
    return;
  }

  const cities = await searchCities(text);
  if (!cities.length) {
    await sendMessage(token, chatId, "❌ Місто не знайдено");
    return;
  }

  st.cities = cities;
  st.selectedCity = null;
  st.queues = [];

  await sendMessage(token, chatId, "📍 Обери місто:", {
    inline_keyboard: cities.map((c, i) => [{ text: c.name, callback_data: `city|${i}` }])
  });
}

// ===================== CALLBACK HANDLER =====================
async function handleCallback(q, token, env) {
  const chatId = q.message.chat.id;
  const messageId = q.message.message_id;
  const data = q.data;

  await answerCallback(token, q.id);

  const key = stKey(token, chatId);
  const st = getState(key);
  S.set(key, st);

  if (data.startsWith("city|")) {
    const city = st.cities[Number(data.split("|")[1])];
    if (!city) return;

    st.selectedCity = city;
    const queues = await getQueuesFromCityUrl(city.url);
    st.queues = queues;

    await editMessage(token, chatId, messageId,
      `🔌 Обери чергу\n📍 ${city.name}`,
      { inline_keyboard: queues.map((q, i) => [{ text: q.name, callback_data: `queue|${i}` }]) }
    );
    return;
  }

  if (data.startsWith("queue|")) {
    const qq = st.queues[Number(data.split("|")[1])];
    if (!qq) return;

    st.selected = {
      cityName: st.selectedCity.name,
      queueName: qq.name,
      url: qq.url
    };

    // 🔔 ПІДПИСКА НА АЛЕРТИ
    await env.ALERTS_KV.put(
      `${token}:${chatId}`,
      JSON.stringify({
        chatId,
        token,
        url: qq.url,
        lastAlert: {}
      })
    );

    const info = await buildInfo(st.selected, env);
    await editMessage(token, chatId, messageId, info, mainQueueKeyboard(env));
    return;
  }

  if (data === "refresh" && st.selected) {
    const info = await buildInfo(st.selected, env);
    await editMessage(token, chatId, messageId, info, mainQueueKeyboard(env));
  }
}

// ===================== ALERTS (CRON) =====================
async function processAlerts(env) {
  const list = await env.ALERTS_KV.list();
  for (const k of list.keys) {
    const sub = await env.ALERTS_KV.get(k.name, { type: "json" });
    if (!sub) continue;

    const blocks = await loadBlocks(sub.url);
    const now = nowMinutesUA(env);

    for (const b of blocks) {
      if (b.endMin - now === 20 && sub.lastAlert?.off !== b.end) {
        await sendMessage(sub.token, sub.chatId, `🔴 Через 20 хв ВИМКНУТЬ світло\n⏰ ${b.end}`);
        sub.lastAlert.off = b.end;
      }
      if (b.startMin - now === 20 && sub.lastAlert?.on !== b.start) {
        await sendMessage(sub.token, sub.chatId, `🟢 Через 20 хв УВІМКНУТЬ світло\n⏰ ${b.start}`);
        sub.lastAlert.on = b.start;
      }
    }

    await env.ALERTS_KV.put(k.name, JSON.stringify(sub));
  }
}

async function loadBlocks(url) {
  const res = await fetch(url);
  const html = await res.text();
  return extractLiItems(html).map(normalizeBlock);
}

// ===================== UTIL / PARSERS =====================
function extractLiItems(html) {
  return [...html.matchAll(/(\d{2}:\d{2})\s*[–-]\s*(\d{2}:\d{2}).*(icon-(on|off))/g)]
    .map(m => ({
      start: m[1],
      end: m[2],
      on: m[4] === "on"
    }));
}

function normalizeBlock(b) {
  const s = toMin(b.start);
  let e = toMin(b.end);
  if (e < s) e += 1440;
  return { ...b, startMin: s, endMin: e };
}

function toMin(t) {
  const [h, m] = t.split(":").map(Number);
  return h * 60 + m;
}

function nowMinutesUA(env) {
  const offset = Number(env.UA_TZ_OFFSET_MIN || 120);
  const d = new Date();
  return (d.getUTCHours() * 60 + d.getUTCMinutes() + offset) % 1440;
}

// ===================== TELEGRAM =====================
async function sendMessage(token, chatId, text, replyMarkup) {
  await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text, reply_markup: replyMarkup })
  });
}

async function editMessage(token, chatId, messageId, text, replyMarkup) {
  await fetch(`https://api.telegram.org/bot${token}/editMessageText`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, message_id: messageId, text, reply_markup: replyMarkup })
  });
}

async function answerCallback(token, id) {
  await fetch(`https://api.telegram.org/bot${token}/answerCallbackQuery`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ callback_query_id: id })
  });
}

// ===================== HELPERS =====================
function parseTokens(raw) {
  return String(raw).split(",").map(x => x.trim());
}
function stKey(token, chatId) { return `${token}:${chatId}`; }
function getState(key) {
  return S.get(key) || {
    cities: [], queues: [], selectedCity: null, selected: null, saved: [],
    ad: { enabled: true, frequency: 3, counter: 0, items: [] },
    whitelist: new Set()
  };
}
function isAdmin(chatId, env) { return String(chatId) === String(env.ADMIN_ID); }
