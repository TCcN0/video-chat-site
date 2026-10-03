// ========== Durable Object: 聊天室房间 ==========
export class ChatRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Map(); // WebSocket -> { id, name }
    this.history = [];         // 最近 100 条消息
    this.nextId = 1;
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader !== 'websocket') {
      return new Response('Expected WebSocket', { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);

    const userId = this.nextId++;
    const userName = `游客${userId.toString().padStart(3, '0')}`;

    this.sessions.set(server, { id: userId, name: userName });

    // 发送历史消息给新用户
    for (const msg of this.history) {
      server.send(JSON.stringify(msg));
    }

    // 广播加入 + 在线人数
    this.broadcast({
      type: 'system',
      text: `${userName} 加入了聊天室`,
      ts: Date.now()
    });
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
        text,
        ts: Date.now()
      };

      this.history.push(msg);
      if (this.history.length > 100) this.history.shift();

      this.broadcast(msg);
    }

    if (data.type === 'rename') {
      const newName = String(data.name || '').slice(0, 20).trim();
      if (newName) {
        session.name = newName;
        this.sessions.set(ws, session);
        this.broadcast({
          type: 'system',
          text: `${newName} 修改了昵称`,
          ts: Date.now()
        });
        this.broadcastOnlineCount();
      }
    }
  }

  async webSocketClose(ws) {
    const session = this.sessions.get(ws);
    if (session) {
      this.sessions.delete(ws);
      this.broadcast({
        type: 'system',
        text: `${session.name} 离开了聊天室`,
        ts: Date.now()
      });
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