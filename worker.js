// ===================== GLOBAL =====================
const S = new Map();
const ALERT_BEFORE_MIN = 20;

// ===================== EXPORT =====================
export default {
  async fetch(request, env) {
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
  },

  // 🔔 CRON
  async scheduled(event, env) {
    await processAlerts(env);
  }
};

// ===================== UPDATE ROUTER =====================
async function handleUpdate(update, token, env) {
  if (update.message) await handleMessage(update.message, token, env);
  if (update.callback_query) await handleCallback(update.callback_query, token, env);
}

// ===================== MESSAGE =====================
async function handleMessage(message, token, env) {
  const chatId = message.chat.id;
  const text = (message.text || "").trim();

  await incStats(chatId, env);

  const st = getState(token, chatId);

  if (text === "/start") {
    st.selected = null;
    await sendMessage(token, chatId,
`⚡ Графік світла
✍️ Напиши назву міста`,
      {
        inline_keyboard: [
          [{ text: "⭐ Мої черги", callback_data: "my" }],
          [{ text: "🤝 Спонсор", url: env.SPONSOR_LINK }]
        ]
      }
    );
    return;
  }

  if (text === "/admin" && isAdmin(chatId, env)) {
    await showAdmin(token, chatId, env);
    return;
  }

  if (!text || text.length < 2) return;

  const cities = await searchCities(text);
  if (!cities.length) {
    await sendMessage(token, chatId, "❌ Міст не знайдено");
    return;
  }

  st.cities = cities;
  await sendMessage(token, chatId, "📍 Обери місто", {
    inline_keyboard: cities.map((c, i) => [
      { text: c.name, callback_data: `city|${i}` }
    ])
  });
}

// ===================== CALLBACK =====================
async function handleCallback(q, token, env) {
  const chatId = q.message.chat.id;
  const data = q.data;
  const st = getState(token, chatId);

  await answerCallback(token, q.id);

  if (data === "my") {
    await showSaved(token, chatId, st, env);
    return;
  }

  if (data.startsWith("city|")) {
    const city = st.cities[+data.split("|")[1]];
    st.selectedCity = city;

    const queues = await getQueuesFromCityUrl(city.url);
    st.queues = queues;

    await editMessage(token, chatId, q.message.message_id,
      `🔌 ${city.name}\nОбери чергу`,
      {
        inline_keyboard: queues.map((q, i) => [
          { text: q.name, callback_data: `queue|${i}` }
        ])
      }
    );
    return;
  }

  if (data.startsWith("queue|")) {
    const qq = st.queues[+data.split("|")[1]];
    st.selected = { ...qq, cityName: st.selectedCity.name };

    await saveAlert(chatId, token, st.selected, env);

    const info = await buildInfo(st.selected, env);
    await editMessage(token, chatId, q.message.message_id, info, mainKeyboard(env));
  }
}

// ===================== ALERTS =====================
async function saveAlert(chatId, token, sel, env) {
  const html = await (await fetch(sel.url)).text();
  const blocks = extractBlocks(html);

  await env.ALERTS_KV.put(
    `${token}:${chatId}:${sel.url}`,
    JSON.stringify({ chatId, token, sel, blocks })
  );
}

async function processAlerts(env) {
  const list = await env.ALERTS_KV.list();
  const now = nowMinutesUA(env);

  for (const k of list.keys) {
    const data = JSON.parse(await env.ALERTS_KV.get(k.name));
    const { chatId, token, sel, blocks } = data;

    for (const b of blocks) {
      if (b.startMin - ALERT_BEFORE_MIN === now) {
        await sendMessage(token, chatId,
          `⏰ Через 20 хв ${b.on ? "УВІМКНУТЬ" : "ВИМКНУТЬ"} світло\n📍 ${sel.cityName}\n🔌 ${sel.queueName}`
        );
      }
    }
  }
}

// ===================== BUILD INFO =====================
async function buildInfo(sel, env) {
  const html = await (await fetch(sel.url)).text();
  const blocks = extractBlocks(html);
  const now = nowMinutesUA(env);

  const cur = blocks.find(b => now >= b.startMin && now < b.endMin);

  let text =
`📍 ${sel.cityName}
🔌 ${sel.queueName}

${cur
  ? cur.on ? "🟢 ЗАРАЗ Є СВІТЛО" : "🔴 ЗАРАЗ НЕМА СВІТЛА"
  : "❓ Невідомо"}

📊 СЬОГОДНІ:
${blocks.slice(0, 12).map(b =>
  `${b.on ? "🟢" : "🔴"} ${b.start}–${b.end}`
).join("\n")}`;

  if (blocks.length > 12) {
    text += `

📅 ЗАВТРА:
${blocks.slice(12, 24).map(b =>
  `${b.on ? "🟢" : "🔴"} ${b.start}–${b.end}`
).join("\n")}`;
  }

  return text;
}

// ===================== HELPERS =====================
function extractBlocks(html) {
  const m = [...html.matchAll(/(\d{2}:\d{2})\s*[–-]\s*(\d{2}:\d{2})/g)];
  return m.map((x, i) => {
    const s = toMin(x[1]);
    let e = toMin(x[2]);
    if (e < s) e += 1440;
    return { start: x[1], end: x[2], startMin: s, endMin: e, on: i % 2 === 0 };
  });
}

function mainKeyboard(env) {
  return {
    inline_keyboard: [
      [{ text: "🔄 Оновити", callback_data: "refresh" }],
      [{ text: "⭐ Мої черги", callback_data: "my" }],
      [{ text: "🤝 Спонсор", url: env.SPONSOR_LINK }]
    ]
  };
}

function getState(token, chatId) {
  const k = `${token}:${chatId}`;
  if (!S.has(k)) S.set(k, {});
  return S.get(k);
}

async function incStats(chatId, env) {
  const k = `u:${chatId}`;
  if (!(await env.STATS_KV.get(k))) {
    await env.STATS_KV.put(k, "1");
  }
}

// ===================== UTILS =====================
function nowMinutesUA(env) {
  const off = +env.UA_TZ_OFFSET_MIN || 120;
  const d = new Date();
  return (d.getUTCHours() * 60 + d.getUTCMinutes() + off) % 1440;
}
function toMin(t) {
  const [h, m] = t.split(":").map(Number);
  return h * 60 + m;
}
function parseTokens(s) {
  return String(s || "").split(",").map(x => x.trim());
}
function isAdmin(id, env) {
  return String(id) === String(env.ADMIN_ID);
}

// ===================== TG =====================
async function sendMessage(token, chatId, text, kb) {
  return fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text, reply_markup: kb })
  });
}
async function editMessage(token, chatId, id, text, kb) {
  return fetch(`https://api.telegram.org/bot${token}/editMessageText`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, message_id: id, text, reply_markup: kb })
  });
}
async function answerCallback(token, id) {
  return fetch(`https://api.telegram.org/bot${token}/answerCallbackQuery`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ callback_query_id: id })
  });
}