import express from 'express';
import bodyParser from 'body-parser';
import jwt from 'jsonwebtoken';
import { Kafka, Partitioners } from 'kafkajs';
import dotenv from 'dotenv';
import { Server } from 'socket.io';
import http from 'http';
import { v4 as uuidv4 } from 'uuid';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

dotenv.config();

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
  },
});

const port = process.env.PORT || 3000;
const jwtSecret = process.env.JWT_SECRET || 'secret';

// Kafka setup
const kafka = new Kafka({
  clientId: 'auth-ai-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});
const admin = kafka.admin();
const consumer = kafka.consumer({ groupId: 'notification-group' });

app.use(bodyParser.json());

async function createTopicsIfNeeded() {
  try {
    await admin.connect();
    const topics = ['ai-image-tasks', 'ai-image-status'];
    // Listando todos os tópicos existentes
    const existingTopics = await admin.listTopics();
    console.log('Tópicos existentes:', existingTopics);

    // Verificando se os tópicos já existem e criando os que não existem
    const topicsToCreate = topics.filter(
      (topic) => !existingTopics.includes(topic)
    );

    if (topicsToCreate.length > 0) {
      console.log('Criando tópicos:', topicsToCreate);

      await admin.createTopics({
        topics: topicsToCreate.map((topic) => ({ topic })),
      });
    } else {
    }
  } catch (error) {
    console.error('Erro ao verificar/criar tópicos Kafka:', error);
  } finally {
    await admin.disconnect();
  }
}

createTopicsIfNeeded();
// Middleware para verificar token JWT
function authenticateToken(req: any, res: any, next: any) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) return res.sendStatus(401);

  jwt.verify(token, jwtSecret, (err: any, user: any) => {
    if (err) return res.sendStatus(403);
    req.user = user;
    next();
  });
}

// Registro de usuários
app.post('/register', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password)
    return res.status(400).json({ error: 'Usuário ou senha inválidos' });

  const existing = await prisma.user.findUnique({
    where: {
      username,
    },
  });

  if (existing) return res.status(409).json({ error: 'Usuário já existe' });

  const user = await prisma.user.create({
    data: {
      username,
      password,
    },
  });

  res.status(201).json({ message: 'Usuário registrado com sucesso' });
});

// Login
app.post('/login', async (req, res) => {
  const { username, password } = req.body;
  const user = await prisma.user.findUnique({
    where: { username },
  });

  if (!user || user.password !== password) return res.sendStatus(403);

  const accessToken = jwt.sign(
    { id: user.id, username: user.username },
    jwtSecret
  );
  res.json({ accessToken });
});

// Envia imagem para processamento pela IA
app.post('/ai/process-image', authenticateToken, async (req, res) => {
  const { imageBase64, operation } = req.body;
  if (!imageBase64 || !operation)
    return res.status(400).json({ error: 'Dados incompletos' });

  const userId = req.user?.id;
  if (!userId) {
    return res.status(401).json({ error: 'Usuário não autenticado' });
  }
  const imageId = uuidv4();
  const message = {
    imageId,
    userId: req.user?.id,
    operation,
    image: imageBase64,
    timestamp: new Date().toISOString(),
  };

  // Salva imagem no banco de dados usando Prisma
  await prisma.image.create({
    data: {
      id: imageId,
      userId: userId,
      original: imageBase64,
      status: 'pending',
    },
  });

  try {
    await producer.connect();
    await producer.send({
      topic: 'ai-image-tasks',
      messages: [{ value: JSON.stringify(message) }],
    });
    await producer.disconnect();
    res.status(200).json({ status: 'Solicitação enviada', imageId });
  } catch (error) {
    console.error('Erro ao enviar para Kafka', error);
    res.status(500).json({ error: 'Erro interno ao processar imagem' });
  }
});

// Consumidor para resposta da IA
async function startConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'ai-image-status', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const statusUpdate = JSON.parse(message.value!.toString());
      console.log('Status recebido:', statusUpdate);

      // Atualiza o status da imagem no banco de dados usando Prisma
      await prisma.image.update({
        where: { id: statusUpdate.imageId },
        data: {
          status: 'done',
          edited: statusUpdate.editedImageBase64,
        },
      });

      // Notifica client via websocket
      io.emit('image-processing-status', statusUpdate);
    },
  });
}

startConsumer().catch(console.error);

// Socket.IO
io.on('connection', (socket) => {
  console.log('Cliente conectado via WebSocket');
  socket.on('disconnect', () => {
    console.log('Cliente desconectado');
  });
});

server.listen(port, () => {
  console.log(`Servidor rodando em http://localhost:${port}`);
});
