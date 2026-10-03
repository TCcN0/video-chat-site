// ========== Durable Object: 聊天室房间 ==========
export class ChatRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Map();
    this.nextId = 1;
    this.initialized = false;
  }

  async init() {
    if (this.initialized) return;
    this.initialized = true;
    // 从 D1 加载最近 100 条消息到内存作为缓存
    try {
      const result = await this.env.DB.prepare(
        `SELECT * FROM messages WHERE room = 'main' ORDER BY ts DESC LIMIT 100`
      ).all();
      // 反转顺序，最早的在前面
      this.history = (result.results || []).reverse();
    } catch (e) {
      console.error('加载历史失败:', e);
      this.history = [];
    }
  }

  async fetch(request) {
    await this.init();

    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader !== 'websocket') {
      return new Response('Expected WebSocket', { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);

    const userId = this.nextId++;
    const userName = `游客${userId.toString().padStart(3, '0')}`;

    this.sessions.set(server, { id: userId, name: userName, color: `hsl(${(userId * 137) % 360}, 65%, 50%)` });

    // 发送历史消息给新用户
    for (const msg of this.history) {
      server.send(JSON.stringify(msg));
    }

    // 广播加入 + 在线人数
    const sysMsg = {
      type: 'system',
      text: `${userName} 加入了聊天室`,
      ts: Date.now()
    };
    await this.saveToDB(sysMsg);
    this.broadcast(sysMsg);
    this.broadcastOnlineCount();

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    let data;
    try {
      data = JSON.parse(message);
    } catch {
      return;
    }

    const session = this.sessions.get(ws);
    if (!session) return;

    if (data.type === 'chat') {
      const text = String(data.text || '').slice(0, 200).trim();
      if (!text) return;

      const msg = {
        type: 'chat',
        id: session.id,
        name: session.name,
        color: session.color,
        text,
        ts: Date.now()
      };

      await this.saveToDB(msg);
      this.broadcast(msg);
    }

    if (data.type === 'rename') {
      const newName = String(data.name || '').slice(0, 20).trim();
      if (newName) {
        session.name = newName;
        this.sessions.set(ws, session);
        const sysMsg = {
          type: 'system',
          text: `${newName} 修改了昵称`,
          ts: Date.now()
        };
        await this.saveToDB(sysMsg);
        this.broadcast(sysMsg);
        this.broadcastOnlineCount();
      }
    }
  }

  async webSocketClose(ws) {
    const session = this.sessions.get(ws);
    if (session) {
      this.sessions.delete(ws);
      const sysMsg = {
        type: 'system',
        text: `${session.name} 离开了聊天室`,
        ts: Date.now()
      };
      await this.saveToDB(sysMsg);
      this.broadcast(sysMsg);
      this.broadcastOnlineCount();
    }
  }

  async webSocketError(ws) {
    const session = this.sessions.get(ws);
    if (session) {
      this.sessions.delete(ws);
      this.broadcastOnlineCount();
    }
  }

  // 保存到 D1
  async saveToDB(msg) {
    try {
      await this.env.DB.prepare(
        `INSERT INTO messages (room, user_id, name, color, text, type, ts) 
         VALUES ('main', ?, ?, ?, ?, ?, ?)`
      ).bind(
        msg.id || null,
        msg.name || null,
        msg.color || null,
        msg.text || null,
        msg.type || 'chat',
        msg.ts || Date.now()
      ).run();

      // 只保留最近 500 条
      await this.env.DB.prepare(
        `DELETE FROM messages WHERE room = 'main' AND id NOT IN (
           SELECT id FROM messages WHERE room = 'main' ORDER BY ts DESC LIMIT 500
         )`
      ).run();
    } catch (e) {
      console.error('保存消息失败:', e);
    }
  }

  broadcast(msg) {
    const payload = JSON.stringify(msg);
    for (const ws of this.sessions.keys()) {
      try {
        ws.send(payload);
      } catch {
        this.sessions.delete(ws);
      }
    }
  }

  broadcastOnlineCount() {
    this.broadcast({
      type: 'online',
      count: this.sessions.size,
      ts: Date.now()
    });
  }
}

// ========== Worker 入口 ==========
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === '/ws') {
      const roomId = env.CHAT_ROOM.idFromName('main');
      const room = env.CHAT_ROOM.get(roomId);
      return room.fetch(request);
    }

    return env.ASSETS.fetch(request);
  }
};
