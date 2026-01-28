// ===================== SIMPLE STABLE TELEGRAM WORKER =====================
// GUARANTEED TO RESPOND

const S = new Map(); // in-memory state

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);

      // health check
      if (url.pathname === "/") {
        return new Response("Bot worker is running", { status: 200 });
      }

      // webhook
      if (url.pathname.startsWith("/webhook/")) {
        const token = url.pathname.split("/")[2];
        if (!token || token !== env.BOT_TOKEN) {
          return new Response("Invalid token", { status: 403 });
        }

        const update = await request.json();
        await handleUpdate(update, token, env);
        return new Response("OK");
      }

      return new Response("Not found", { status: 404 });
    } catch (e) {
      console.log("ERROR:", e);
      return new Response("Worker error", { status: 500 });
    }
  }
};

// ===================== UPDATE ROUTER =====================
async function handleUpdate(update, token, env) {
  if (update.callback_query) {
    await handleCallback(update.callback_query, token);
    return;
  }
  if (update.message) {
    await handleMessage(update.message, token);
    return;
  }
}

// ===================== MESSAGE =====================
async function handleMessage(message, token) {
  const chatId = message.chat.id;
  const text = typeof message.text === "string" ? message.text.trim() : "";

  if (text === "/start") {
    S.delete(chatId);
    await sendMessage(token, chatId,
      "⚡ Світло ДТЕК\n\n✍️ Напиши назву міста"
    );
    return;
  }

  if (text.length < 2) return;

  const cities = await searchCities(text);
  if (!cities.length) {
    await sendMessage(token, chatId, "❌ Місто не знайдено");
    return;
  }

  S.set(chatId, { cities });

  const keyboard = cities.map((c, i) => ([
    { text: c.name, callback_data: `city|${i}` }
  ]));

  await sendMessage(token, chatId, "📍 Обери місто:", {
    inline_keyboard: keyboard
  });
}

// ===================== CALLBACK =====================
async function handleCallback(q, token) {
  const chatId = q.message.chat.id;
  const msgId = q.message.message_id;
  const data = q.data;

  await answerCallback(token, q.id);

  const st = S.get(chatId);
  if (!st) return;

  // CITY
  if (data.startsWith("city|")) {
    const idx = Number(data.split("|")[1]);
    const city = st.cities[idx];
    if (!city) return;

    const queues = await getQueues(city.url);
    if (!queues.length) {
      await editMessage(token, chatId, msgId, "❌ Черги не знайдені");
      return;
    }

    st.city = city;
    st.queues = queues;

    const kb = queues.map((q, i) => ([
      { text: q.name, callback_data: `queue|${i}` }
    ]));

    await editMessage(token, chatId, msgId,
      `📍 ${city.name}\n🔌 Обери чергу:`,
      { inline_keyboard: kb }
    );
    return;
  }

  // QUEUE
  if (data.startsWith("queue|")) {
    const idx = Number(data.split("|")[1]);
    const qv = st.queues[idx];
    if (!qv) return;

    const info = await buildInfo(st.city.name, qv);
    await editMessage(token, chatId, msgId, info);
  }
}

// ===================== DATA =====================
async function searchCities(query) {
  const res = await fetch(
    "https://bezsvitla.com.ua/search-locality?q=" + encodeURIComponent(query),
    { headers: { "X-Requested-With": "XMLHttpRequest" } }
  );
  if (!res.ok) return [];
  const data = await res.json();
  return data.slice(0, 6).map(x => ({ name: x.name, url: x.url }));
}

async function getQueues(url) {
  const res = await fetch(url);
  if (!res.ok) return [];
  const html = await res.text();
  const matches = [...html.matchAll(/href="([^"]*cherha-[^"]+)"/g)];
  return matches.map(m => {
    const u = m[1].startsWith("http") ? m[1] : "https://bezsvitla.com.ua" + m[1];
    return { name: "Черга " + u.split("cherha-")[1], url: u };
  });
}

async function buildInfo(city, queue) {
  const res = await fetch(queue.url);
  if (!res.ok) return "❌ Помилка";
  const html = await res.text();

  const times = [...html.matchAll(/(\d{2}:\d{2})\s*–\s*(\d{2}:\d{2})/g)]
    .slice(0, 12)
    .map(m => `• ${m[1]} – ${m[2]}`)
    .join("\n");

  return `📍 ${city}\n🔌 ${queue.name}\n\n📊 СЬОГОДНІ:\n${times || "Нема даних"}`;
}

// ===================== TELEGRAM =====================
async function sendMessage(token, chatId, text, markup) {
  return fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text, reply_markup: markup })
  });
}

async function editMessage(token, chatId, msgId, text, markup) {
  return fetch(`https://api.telegram.org/bot${token}/editMessageText`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, message_id: msgId, text, reply_markup: markup })
  });
}

async function answerCallback(token, id) {
  return fetch(`https://api.telegram.org/bot${token}/answerCallbackQuery`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ callback_query_id: id })
  });
}