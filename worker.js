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
 *   ad: { enabled: true, frequency: 3, counter: 0, items: [{id,type,text,file_id}] },
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
        const parts = url.pathname.split("/").filter(Boolean);
        const token = parts[1];

        const tokens = parseTokens(env.BOT_TOKENS);
        if (!token || !tokens.includes(token)) {
          return new Response("Invalid token", { status: 403 });
        }

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

// ===================== UPDATE ROUTER (FIXED) =====================
async function handleUpdate(update, token, env) {
  if (update.callback_query) {
    await handleCallback(update.callback_query, token, env);
  }
  if (update.message) {
    await handleMessage(update.message, token, env);
  }
}

// ===================== MESSAGE HANDLER =====================
async function handleMessage(message, token, env) {
  const chatId = message.chat.id;

  const text =
    typeof message.text === "string"
      ? message.text.trim()
      : null;

  const key = stKey(token, chatId);
  const st = getState(key);
  S.set(key, st);

  console.log("TEXT:", text);

  // ADMIN MEDIA
  if (isAdmin(chatId, env) && hasAnyMedia(message)) {
    const cap = (message.caption || "").trim();
    if (cap.startsWith("/ad")) {
      const extraText = cap.replace(/^\/ad\s*/i, "").trim();
      const item = mediaToAdItem(message, extraText);
      if (!item) {
        await sendMessage(token, chatId, "❌ Не зміг додати рекламу.");
        return;
      }
      st.ad.items.unshift(item);
      await sendMessage(token, chatId, "✅ Рекламу додано.");
    }
    return;
  }

  if (text === null) return;

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
    return;
  }

  if (text === "/admin" && isAdmin(chatId, env)) {
    await showAdminMenu(token, chatId, st, env);
    return;
  }

  if (text === "/my") {
    await showMyQueues(token, chatId, st, env);
    return;
  }

  if (text.length < 2) {
    await sendMessage(token, chatId, "✍️ Мінімум 2 символи");
    return;
  }

  // SEARCH CITY
  const cities = await searchCities(text);
  if (!cities.length) {
    await sendMessage(token, chatId, "❌ Місто не знайдено");
    return;
  }

  st.cities = cities;
  st.queues = [];
  st.selectedCity = null;
  st.selected = null;

  const keyboard = cities.map((c, i) => [
    { text: c.name, callback_data: `city|${i}` }
  ]);

  await sendMessage(token, chatId, "📍 Обери місто:", {
    inline_keyboard: keyboard
  });
}

// ===================== CALLBACK HANDLER =====================
async function handleCallback(q, token, env) {
  const chatId = q.message.chat.id;
  const messageId = q.message.message_id;
  const data = q.data;

  await answerCallback(token, q.id).catch(() => {});

  const key = stKey(token, chatId);
  const st = getState(key);
  S.set(key, st);

  if (data === "my") {
    await showMyQueues(token, chatId, st, env, messageId);
    return;
  }

  if (data.startsWith("city|")) {
    const idx = Number(data.split("|")[1]);
    const city = st.cities[idx];
    if (!city) return;

    st.selectedCity = city;
    const queues = await getQueuesFromCityUrl(city.url);
    st.queues = queues;

    const kb = queues.map((q, i) => [
      { text: q.name, callback_data: `queue|${i}` }
    ]);

    await editMessage(
      token,
      chatId,
      messageId,
      `🔌 Обери чергу\n📍 ${city.name}`,
      { inline_keyboard: kb }
    );
    return;
  }

  if (data.startsWith("queue|")) {
    const idx = Number(data.split("|")[1]);
    const qq = st.queues[idx];
    if (!qq) return;

    st.selected = {
      cityName: st.selectedCity.name,
      queueName: qq.name,
      url: qq.url
    };

    const info = await buildInfo(st.selected, env);
    await editMessage(
      token,
      chatId,
      messageId,
      info,
      mainQueueKeyboard(env)
    );
  }
}

// ===================== HELPERS =====================
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

async function searchCities(query) {
  const res = await fetch(
    "https://bezsvitla.com.ua/search-locality?q=" +
      encodeURIComponent(query),
    {
      headers: {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json"
      }
    }
  );
  if (!res.ok) return [];
  const data = await res.json().catch(() => []);
  return Array.isArray(data)
    ? data.filter(x => x.name && x.url).slice(0, 8)
    : [];
}

async function getQueuesFromCityUrl(cityUrl) {
  const html = await fetch(cityUrl).then(r => r.text());
  const matches = [...html.matchAll(/href="([^"]*cherha-[^"]+)"/g)];
  return [...new Set(matches.map(m => m[1]))].map(u => ({
    name: "Черга " + u.split("cherha-")[1].replace(/-/g, "."),
    url: u.startsWith("http") ? u : "https://bezsvitla.com.ua" + u
  }));
}

async function buildInfo(sel, env) {
  return `📍 ${sel.cityName}\n🔌 ${sel.queueName}\n\n(графік завантажено)`;
}

// ===================== TELEGRAM =====================
async function sendMessage(token, chatId, text, reply_markup) {
  return tgCall(token, "sendMessage", {
    chat_id: chatId,
    text,
    reply_markup
  });
}

async function editMessage(token, chatId, messageId, text, reply_markup) {
  return tgCall(token, "editMessageText", {
    chat_id: chatId,
    message_id: messageId,
    text,
    reply_markup
  });
}

async function answerCallback(token, id) {
  return tgCall(token, "answerCallbackQuery", {
    callback_query_id: id
  });
}

async function tgCall(token, method, payload) {
  return fetch(`https://api.telegram.org/bot${token}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
}

// ===================== STATE =====================
function stKey(token, chatId) {
  return `${token}:${chatId}`;
}

function getState(key) {
  if (S.has(key)) return S.get(key);
  const st = {
    cities: [],
    queues: [],
    selectedCity: null,
    selected: null,
    saved: [],
    ad: initAdState(),
    whitelist: new Set()
  };
  S.set(key, st);
  return st;
}

function initAdState() {
  return {
    enabled: true,
    frequency: 3,
    counter: 0,
    items: []
  };
}

function parseTokens(raw) {
  if (!raw) return [];
  return String(raw)
    .split(",")
    .map(x => x.trim())
    .filter(Boolean);
}

function isAdmin(chatId, env) {
  return String(chatId) === String(env.ADMIN_ID);
}

function hasAnyMedia(m) {
  return m.photo || m.video || m.document;
}

function mediaToAdItem(m, text) {
  if (m.photo)
    return { type: "photo", file_id: m.photo.at(-1).file_id, text };
  if (m.video)
    return { type: "video", file_id: m.video.file_id, text };
  if (m.document)
    return { type: "document", file_id: m.document.file_id, text };
  return null;
}