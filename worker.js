// =====================================================
// TELEGRAM LIGHT BOT — FULL VERSION
// City → Queue → Schedule → Alerts → Admin Panel
// =====================================================

// ===================== MEMORY CACHE =====================
const S = new Map();

// ===================== WORKER =====================
export default {
  async fetch(request, env) {
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
  async scheduled(event, env) {
    await processAlerts(env);
  }
};

// =====================================================
// UPDATE ROUTER
// =====================================================
async function handleUpdate(update, token, env) {
  if (update.callback_query) {
    await handleCallback(update.callback_query, token, env);
    await answerCallback(token, update.callback_query.id);
    return;
  }
  if (update.message) {
    await handleMessage(update.message, token, env);
  }
}

// =====================================================
// MESSAGE HANDLER
// =====================================================
async function handleMessage(message, token, env) {
  const chatId = message.chat.id;
  const text = (message.text || "").trim();

  await registerUser(chatId, env);

  if (text === "/start") {
    await sendMessage(
      token,
      chatId,
`⚡ Графік відключень світла

✍️ Напиши назву міста`,
      {
        inline_keyboard: [
          [{ text: "📋 Мої черги", callback_data: "my" }],
          [{ text: "🤝 Стати спонсором", url: env.SPONSOR_LINK }]
        ]
      }
    );
    return;
  }

  if (text === "/admin" && isAdmin(chatId, env)) {
    await showAdminPanel(token, chatId, env);
    return;
  }

  if (text.length < 2) return;

  const cities = await searchCities(text);
  if (!cities.length) {
    await sendMessage(token, chatId, "❌ Місто не знайдено");
    return;
  }

  S.set(chatId, { cities });

  await sendMessage(
    token,
    chatId,
    "📍 Обери місто:",
    {
      inline_keyboard: cities.map((c, i) => [
        { text: c.name, callback_data: `city|${i}` }
      ])
    }
  );
}

// =====================================================
// CALLBACK HANDLER
// =====================================================
async function handleCallback(q, token, env) {
  const chatId = q.message.chat.id;
  const data = q.data;
  const msgId = q.message.message_id;
  const st = S.get(chatId) || {};

  // ---------- ADMIN ----------
  if (data === "admin_stats" && isAdmin(chatId, env)) {
    const stats = await env.STATS_KV.list();
    await editMessage(
      token,
      chatId,
      msgId,
      `📊 Користувачів у боті: ${stats.keys.length}`,
      adminKeyboard()
    );
    return;
  }

  // ---------- MY ----------
  if (data === "my") {
    const sub = await env.SUBS_KV.get(String(chatId), { type: "json" });
    if (!sub) {
      await sendMessage(token, chatId, "📭 У тебе немає збережених черг");
      return;
    }
    await sendMessage(
      token,
      chatId,
      `📍 ${sub.city}\n🔌 ${sub.queue}`,
      {
        inline_keyboard: [
          [{ text: "🔄 Оновити", callback_data: "refresh" }],
          [{ text: "📅 Завтра", callback_data: "tomorrow" }]
        ]
      }
    );
    return;
  }

  // ---------- CITY ----------
  if (data.startsWith("city|")) {
    const idx = Number(data.split("|")[1]);
    const city = st.cities?.[idx];
    if (!city) return;

    const queues = await getQueuesFromCityUrl(city.url);
    S.set(chatId, { city, queues });

    await editMessage(
      token,
      chatId,
      msgId,
      `📍 ${city.name}\n🔌 Обери чергу:`,
      {
        inline_keyboard: queues.map((q, i) => [
          { text: q.name, callback_data: `queue|${i}` }
        ])
      }
    );
    return;
  }

  // ---------- QUEUE ----------
  if (data.startsWith("queue|")) {
    const idx = Number(data.split("|")[1]);
    const queue = st.queues?.[idx];
    if (!queue) return;

    await env.SUBS_KV.put(
      String(chatId),
      JSON.stringify({
        chatId,
        token,
        city: st.city.name,
        queue: queue.name,
        url: queue.url,
        lastAlert: {}
      })
    );

    const info = await buildInfo(queue.url, st.city.name, queue.name, env);

    await editMessage(
      token,
      chatId,
      msgId,
      info,
      {
        inline_keyboard: [
          [{ text: "🔄 Оновити", callback_data: "refresh" }],
          [{ text: "📅 Завтра", callback_data: "tomorrow" }]
        ]
      }
    );
  }
}

// =====================================================
// CRON ALERTS (20 MINUTES)
// =====================================================
async function processAlerts(env) {
  const list = await env.SUBS_KV.list();

  for (const k of list.keys) {
    const sub = await env.SUBS_KV.get(k.name, { type: "json" });
    if (!sub) continue;

    const blocks = await loadBlocks(sub.url);
    const now = nowMinutesUA(env);

    for (const b of blocks) {
      if (Math.abs(b.startMin - now) === 20 && sub.lastAlert?.on !== b.start) {
        await sendMessage(
          sub.token,
          sub.chatId,
          `🟢 Через 20 хв УВІМКНУТЬ світло\n⏰ ${b.start}`
        );
        sub.lastAlert.on = b.start;
      }

      if (Math.abs(b.endMin - now) === 20 && sub.lastAlert?.off !== b.end) {
        await sendMessage(
          sub.token,
          sub.chatId,
          `🔴 Через 20 хв ВИМКНУТЬ світло\n⏰ ${b.end}`
        );
        sub.lastAlert.off = b.end;
      }
    }

    await env.SUBS_KV.put(k.name, JSON.stringify(sub));
  }
}

// =====================================================
// ADMIN PANEL
// =====================================================
function adminKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "📊 Статистика", callback_data: "admin_stats" }],
      [{ text: "❌ Закрити", callback_data: "noop" }]
    ]
  };
}

async function showAdminPanel(token, chatId, env) {
  await sendMessage(
    token,
    chatId,
    "👑 Адмін-панель",
    adminKeyboard()
  );
}

// =====================================================
// PARSING & HELPERS
// =====================================================
async function searchCities(query) {
  const r = await fetch(
    "https://bezsvitla.com.ua/search-locality?q=" + encodeURIComponent(query),
    { headers: { "Accept": "application/json" } }
  );
  if (!r.ok) return [];
  return (await r.json()).slice(0, 8);
}

async function getQueuesFromCityUrl(url) {
  const html = await (await fetch(url)).text();
  return [...new Set(
    [...html.matchAll(/href="([^"]*cherha-[^"]+)"/g)]
      .map(m => m[1])
  )].map(u => ({
    name: "Черга " + u.split("cherha-")[1].replace(/-/g, "."),
    url: u.startsWith("http") ? u : "https://bezsvitla.com.ua" + u
  }));
}

async function loadBlocks(url) {
  const html = await (await fetch(url)).text();
  const out = [];
  for (const m of html.matchAll(/(\d{2}:\d{2})\s*[–-]\s*(\d{2}:\d{2})/g)) {
    const s = toMin(m[1]);
    let e = toMin(m[2]);
    if (e < s) e += 1440;
    out.push({ start: m[1], end: m[2], startMin: s, endMin: e });
  }
  return out;
}

async function buildInfo(url, city, queue, env) {
  const blocks = await loadBlocks(url);
  const now = nowMinutesUA(env);

  let status = "❓";
  for (const b of blocks) {
    if (now >= b.startMin && now < b.endMin) {
      status = "🟢 Є світло до " + b.end;
    }
  }

  const today = blocks.slice(0, 12);
  const tomorrow = blocks.slice(12, 24);

  return `📍 ${city}
🔌 ${queue}

${status}

📊 Сьогодні:
${today.map(b => `• ${b.start} – ${b.end}`).join("\n")}

📅 Завтра:
${tomorrow.map(b => `• ${b.start} – ${b.end}`).join("\n")}`;
}

// =====================================================
// TELEGRAM API
// =====================================================
async function sendMessage(token, chatId, text, reply_markup) {
  await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text, reply_markup })
  });
}

async function editMessage(token, chatId, message_id, text, reply_markup) {
  await fetch(`https://api.telegram.org/bot${token}/editMessageText`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, message_id, text, reply_markup })
  });
}

async function answerCallback(token, id) {
  await fetch(`https://api.telegram.org/bot${token}/answerCallbackQuery`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ callback_query_id: id })
  });
}

// =====================================================
// UTILS
// =====================================================
function parseTokens(raw) {
  return String(raw).split(",").map(x => x.trim());
}

function toMin(t) {
  const [h, m] = t.split(":").map(Number);
  return h * 60 + m;
}

function nowMinutesUA(env) {
  const off = Number(env.UA_TZ_OFFSET_MIN || 120);
  const d = new Date();
  return (d.getUTCHours() * 60 + d.getUTCMinutes() + off + 1440) % 1440;
}

async function registerUser(chatId, env) {
  const key = "u:" + chatId;
  if (!(await env.STATS_KV.get(key))) {
    await env.STATS_KV.put(key, "1");
  }
}

function isAdmin(chatId, env) {
  return String(chatId) === String(env.ADMIN_ID);
          }
