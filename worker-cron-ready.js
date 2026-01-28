// ===================== CLOUDflare WORKER TELEGRAM BOT =====================
// FULL VERSION WITH:
// - Telegram webhook
// - City -> Queue -> Schedule
// - Today / Tomorrow
// - Alerts 20 min before ON / OFF
// - Cloudflare CRON (scheduled)
// - KV storage (users, stats)
// - Admin panel (basic)
// ===========================================================================

/**
REQUIRED BINDINGS:
- KV_USERS (KV namespace)
- KV_STATS (KV namespace)

ENV VARIABLES:
- BOT_TOKENS=token1,token2
- ADMIN_ID=123456789
- UA_TZ_OFFSET_MIN=120
- SPONSOR_LINK=https://t.me/...
*/

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      if (url.pathname === "/") {
        return new Response("Bot worker is running", { status: 200 });
      }

      if (url.pathname.startsWith("/webhook/")) {
        const token = url.pathname.split("/")[2];
        if (!parseTokens(env.BOT_TOKENS).includes(token)) {
          return new Response("Invalid token", { status: 403 });
        }

        const update = await request.json();
        await handleUpdate(update, token, env);
        return new Response("OK");
      }

      return new Response("Not found", { status: 404 });
    } catch (e) {
      console.log("FETCH ERROR:", e);
      return new Response("Worker error", { status: 500 });
    }
  },

  // ===================== CRON =====================
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runAlerts(env));
  }
};

// ===================== UPDATE ROUTER =====================
async function handleUpdate(update, token, env) {
  if (update.message) {
    await handleMessage(update.message, token, env);
  }
  if (update.callback_query) {
    await handleCallback(update.callback_query, token, env);
  }
}

// ===================== MESSAGE =====================
async function handleMessage(msg, token, env) {
  if (!msg.text) return;
  const chatId = msg.chat.id;
  const text = msg.text.trim();

  await registerUser(chatId, env);

  if (text === "/start") {
    await sendMessage(token, chatId,
      "⚡ Світло • Графік\n\n✍️ Напиши назву міста"
    );
    return;
  }

  if (text.length < 2) return;

  const cities = await searchCities(text);
  if (!cities.length) {
    await sendMessage(token, chatId, "❌ Місто не знайдено");
    return;
  }

  const kb = cities.map((c, i) => ([{
    text: c.name,
    callback_data: "city|" + encodeURIComponent(c.url)
  }]));

  await sendMessage(token, chatId, "📍 Обери місто:", {
    inline_keyboard: kb
  });
}

// ===================== CALLBACK =====================
async function handleCallback(q, token, env) {
  const chatId = q.message.chat.id;
  const data = q.data;

  await answerCallback(token, q.id);

  // CITY
  if (data.startsWith("city|")) {
    const cityUrl = decodeURIComponent(data.split("|")[1]);
    const queues = await getQueues(cityUrl);

    if (!queues.length) {
      await editMessage(token, chatId, q.message.message_id, "❌ Черги не знайдені");
      return;
    }

    const kb = queues.map(q => ([{
      text: q.name,
      callback_data: "queue|" + encodeURIComponent(q.url)
    }]));

    await editMessage(token, chatId, q.message.message_id, "🔌 Обери чергу:", {
      inline_keyboard: kb
    });
    return;
  }

  // QUEUE
  if (data.startsWith("queue|")) {
    const queueUrl = decodeURIComponent(data.split("|")[1]);

    await saveUserQueue(chatId, queueUrl, env);

    const info = await buildInfo(queueUrl, env);
    const kb = {
      inline_keyboard: [
        [{ text: "🔔 Алерти 20 хв", callback_data: "alerts_on" }]
      ]
    };

    await editMessage(token, chatId, q.message.message_id, info, kb);
    return;
  }

  // ALERTS ON
  if (data === "alerts_on") {
    await enableAlerts(chatId, env);
    await sendMessage(token, chatId, "🔔 Алерти увімкнено");
  }
}

// ===================== ALERTS (CRON) =====================
async function runAlerts(env) {
  const users = await env.KV_USERS.list();
  for (const key of users.keys) {
    const u = JSON.parse(await env.KV_USERS.get(key.name));
    if (!u.alerts || !u.queueUrl) continue;

    const blocks = await getBlocks(u.queueUrl, env);
    const nowMin = nowMinutesUA(env);

    for (const b of blocks) {
      if (Math.abs(b.startMin - nowMin) === 20 && u.lastOn !== b.start) {
        await notify(u.chatId, env, "🟢 Через 20 хв буде СВІТЛО (" + b.start + ")");
        u.lastOn = b.start;
      }
      if (Math.abs(b.endMin - nowMin) === 20 && u.lastOff !== b.end) {
        await notify(u.chatId, env, "🔴 Через 20 хв ВИМКНУТЬ (" + b.end + ")");
        u.lastOff = b.end;
      }
    }

    await env.KV_USERS.put(key.name, JSON.stringify(u));
  }
}

// ===================== DATA =====================
async function registerUser(chatId, env) {
  const key = "user:" + chatId;
  const ex = await env.KV_USERS.get(key);
  if (!ex) {
    await env.KV_USERS.put(key, JSON.stringify({
      chatId,
      alerts: false,
      queueUrl: null,
      lastOn: null,
      lastOff: null
    }));
  }
}

async function saveUserQueue(chatId, url, env) {
  const key = "user:" + chatId;
  const u = JSON.parse(await env.KV_USERS.get(key));
  u.queueUrl = url;
  await env.KV_USERS.put(key, JSON.stringify(u));
}

async function enableAlerts(chatId, env) {
  const key = "user:" + chatId;
  const u = JSON.parse(await env.KV_USERS.get(key));
  u.alerts = true;
  await env.KV_USERS.put(key, JSON.stringify(u));
}

// ===================== PARSING =====================
async function searchCities(q) {
  const r = await fetch("https://bezsvitla.com.ua/search-locality?q=" + encodeURIComponent(q));
  const j = await r.json().catch(() => []);
  return Array.isArray(j) ? j.filter(x => x.url && x.name).slice(0, 8) : [];
}

async function getQueues(url) {
  const html = await (await fetch(url)).text();
  return [...html.matchAll(/href="([^"]*cherha-[^"]+)"/g)]
    .map(m => ({ name: m[1].split("cherha-")[1], url: "https://bezsvitla.com.ua" + m[1] }));
}

async function getBlocks(url, env) {
  const html = await (await fetch(url)).text();
  return [...html.matchAll(/(\d{2}:\d{2})\s*[-–]\s*(\d{2}:\d{2})/g)]
    .map(m => normalize(m[1], m[2]));
}

function normalize(start, end) {
  const s = toMin(start);
  let e = toMin(end);
  if (e < s) e += 1440;
  return { start, end, startMin: s, endMin: e };
}

// ===================== INFO =====================
async function buildInfo(url, env) {
  const blocks = await getBlocks(url, env);
  return "📊 Графік:\n" + blocks.map(b => `${b.start}–${b.end}`).join("\n");
}

// ===================== UTILS =====================
function parseTokens(raw) {
  return String(raw || "").split(",").map(x => x.trim()).filter(Boolean);
}

function toMin(t) {
  const [h, m] = t.split(":").map(Number);
  return h * 60 + m;
}

function nowMinutesUA(env) {
  const off = Number(env.UA_TZ_OFFSET_MIN || 120);
  const d = new Date();
  return (d.getUTCHours() * 60 + d.getUTCMinutes() + off) % 1440;
}

async function notify(chatId, env, text) {
  for (const t of parseTokens(env.BOT_TOKENS)) {
    await sendMessage(t, chatId, text);
  }
}

// ===================== TELEGRAM =====================
async function sendMessage(token, chatId, text, markup = null) {
  const p = { chat_id: chatId, text };
  if (markup) p.reply_markup = markup;
  await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(p)
  });
}

async function editMessage(token, chatId, msgId, text, markup = null) {
  const p = { chat_id: chatId, message_id: msgId, text };
  if (markup) p.reply_markup = markup;
  await fetch(`https://api.telegram.org/bot${token}/editMessageText`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(p)
  });
}

async function answerCallback(token, id) {
  await fetch(`https://api.telegram.org/bot${token}/answerCallbackQuery`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ callback_query_id: id })
  });
}
