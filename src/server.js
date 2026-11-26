import Fastify from 'fastify';
import cors from '@fastify/cors';
import websocket from '@fastify/websocket';
import { PrismaClient } from '@prisma/client';
import Redis from 'ioredis';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import crypto from 'crypto';

const app = Fastify({ logger: true });
const prisma = new PrismaClient();

// Conexão Redis (Upstash) - Se não configurar no .env, usa memória local como fallback
const redis = process.env.REDIS_URL ? new Redis(process.env.REDIS_URL) : null;
const JWT_SECRET = process.env.JWT_SECRET || 'nomade_super_secret_key_2026';
const MEDIA_HLS_BASE = process.env.MEDIA_HLS_BASE || 'http://localhost:8888';

await app.register(cors, { origin: '*' });
await app.register(websocket);

// Mapa de salas de chat conectadas: Map<channelUsername, Set<WebSocket>>
const chatRooms = new Map();

// Middleware de Autenticação JWT
async function authenticate(request, reply) {
  try {
    const authHeader = request.headers.authorization;
    if (!authHeader) throw new Error('Token ausente');
    const token = authHeader.replace('Bearer ', '');
    request.user = jwt.verify(token, JWT_SECRET);
  } catch (err) {
    return reply.code(401).send({ error: 'Não autorizado' });
  }
}

// ==========================================
// 1. ROTAS DE AUTENTICAÇÃO (CADASTRO E LOGIN)
// ==========================================

app.post('/api/v1/auth/register', async (request, reply) => {
  const { username, email, password } = request.body;
  const cleanUsername = username.toLowerCase().replace(/[^a-z0-9_]/g, '');

  if (!cleanUsername || !email || !password) {
    return reply.code(400).send({ error: 'Preencha todos os campos.' });
  }

  const passwordHash = await bcrypt.hash(password, 10);

  try {
    const user = await prisma.user.create({
      data: {
        username: cleanUsername,
        email,
        passwordHash,
        channel: {
          create: {
            streamKey: `live_${crypto.randomBytes(16).toString('hex')}`,
            title: `Live de ${cleanUsername}`
          }
        }
      },
      include: { channel: true }
    });

    const token = jwt.sign({ id: user.id, username: user.username }, JWT_SECRET, { expiresIn: '7d' });
    return { token, user: { id: user.id, username: user.username, channel: user.channel } };
  } catch (err) {
    return reply.code(409).send({ error: 'Usuário ou e-mail já cadastrado.' });
  }
});

app.post('/api/v1/auth/login', async (request, reply) => {
  const { email, password } = request.body;
  const user = await prisma.user.findUnique({ where: { email }, include: { channel: true } });

  if (!user || !(await bcrypt.compare(password, user.passwordHash))) {
    return reply.code(401).send({ error: 'Credenciais inválidas.' });
  }

  const token = jwt.sign({ id: user.id, username: user.username }, JWT_SECRET, { expiresIn: '7d' });
  return { token, user: { id: user.id, username: user.username, channel: user.channel } };
});

// ==========================================
// 2. ROTAS PÚBLICAS DE STREAMING
// ==========================================

app.get('/api/v1/streams/live', async () => {
  const channels = await prisma.channel.findMany({
    where: { isLive: true },
    include: { user: { select: { username: true } } },
    orderBy: { updatedAt: 'desc' }
  });

  return channels.map((ch) => ({
    id: ch.id,
    username: ch.user.username,
    title: ch.title,
    category: ch.category,
    playbackUrl: ch.playbackUrl,
    viewers: chatRooms.get(ch.user.username)?.size || 0
  }));
});

app.get('/api/v1/channels/:username', async (request, reply) => {
  const { username } = request.params;
  const user = await prisma.user.findUnique({
    where: { username: username.toLowerCase() },
    include: { channel: true }
  });

  if (!user || !user.channel) return reply.code(404).send({ error: 'Canal não encontrado.' });

  return {
    username: user.username,
    title: user.channel.title,
    category: user.channel.category,
    isLive: user.channel.isLive,
    playbackUrl: user.channel.playbackUrl,
    viewers: chatRooms.get(user.username)?.size || 0
  };
});

// ==========================================
// 3. ÁREA DE GERENCIAMENTO DO CRIADOR
// ==========================================

app.get('/api/v1/creator/me', { preHandler: [authenticate] }, async (request) => {
  const channel = await prisma.channel.findUnique({ where: { userId: request.user.id } });
  return {
    ingestUrl: 'rtmp://seu-dominio-ou-ip:1935/live',
    ...channel
  };
});

app.patch('/api/v1/creator/channel', { preHandler: [authenticate] }, async (request) => {
  const { title, category } = request.body;
  const updated = await prisma.channel.update({
    where: { userId: request.user.id },
    data: { title, category }
  });
  return updated;
});

app.post('/api/v1/creator/stream-key/reset', { preHandler: [authenticate] }, async (request) => {
  const newKey = `live_${crypto.randomBytes(16).toString('hex')}`;
  const updated = await prisma.channel.update({
    where: { userId: request.user.id },
    data: { streamKey: newKey }
  });
  return { streamKey: updated.streamKey };
});

// ==========================================
// 4. WEBHOOK DO SERVIDOR DE VÍDEO (MEDIAMTX)
// ==========================================

app.post('/api/v1/internal/media/auth', async (request, reply) => {
  // MediaMTX envia o path no formato "live/live_chave_secreta"
  const { path, action } = request.body;
  const streamKey = path?.split('/').pop();

  if (!streamKey) return reply.code(400).send({ error: 'Chave inválida' });

  const channel = await prisma.channel.findUnique({
    where: { streamKey },
    include: { user: true }
  });

  if (!channel) return reply.code(403).send({ error: 'Stream Key não autorizada' });

  const isStarting = action === 'publish';
  const playbackUrl = isStarting ? `${MEDIA_HLS_BASE}/live/${streamKey}/index.m3u8` : null;

  await prisma.channel.update({
    where: { id: channel.id },
    data: { isLive: isStarting, playbackUrl }
  });

  return { ok: true, status: isStarting ? 'LIVE' : 'OFFLINE' };
});

// ==========================================
// 5. CHAT EM TEMPO REAL (WEBSOCKET)
// ==========================================

app.get('/ws/chat/:username', { websocket: true }, (socket, request) => {
  const username = request.params.username.toLowerCase();

  if (!chatRooms.has(username)) chatRooms.set(username, new Set());
  chatRooms.get(username).add(socket);

  socket.on('message', async (rawMessage) => {
    try {
      const payload = JSON.parse(rawMessage.toString());
      if (!payload.text || !payload.token) return;

      const decoded = jwt.verify(payload.token, JWT_SECRET);
      const chatEvent = JSON.stringify({
        id: crypto.randomUUID(),
        sender: decoded.username,
        text: payload.text.slice(0, 300),
        timestamp: new Date().toISOString()
      });

      // Transmite para todos os espectadores conectados naquela live
      for (const client of chatRooms.get(username)) {
        if (client.readyState === 1) client.send(chatEvent);
      }
    } catch (e) {
      socket.send(JSON.stringify({ error: 'Faça login para enviar mensagens.' }));
    }
  });

  socket.on('close', () => {
    chatRooms.get(username)?.delete(socket);
  });
});

const PORT = process.env.PORT || 3001;
app.listen({ port: PORT, host: '0.0.0.0' });
