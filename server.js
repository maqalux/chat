const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { initializeApp } = require('firebase/app');
const { getDatabase, ref, push, onChildAdded, get, set } = require('firebase/database');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    credentials: true
  }
});

const PORT = process.env.PORT || 3000;

app.use(express.static('public'));

const firebaseConfig = {
  apiKey: process.env.FIREBASE_API_KEY,
  authDomain: process.env.FIREBASE_AUTH_DOMAIN,
  projectId: process.env.FIREBASE_PROJECT_ID,
  databaseURL: process.env.FIREBASE_DATABASE_URL,
  storageBucket: process.env.FIREBASE_STORAGE_BUCKET,
  messagingSenderId: process.env.FIREBASE_MESSAGING_SENDER_ID,
  appId: process.env.FIREBASE_APP_ID
};

const firebaseApp = initializeApp(firebaseConfig);
const db = getDatabase(firebaseApp);
const messagesRef = ref(db, 'messages');
const privateMessagesRef = ref(db, 'private_messages'); // Firebase-də şəxsi mesajlar üçün referans

const activeUsers = {};

onChildAdded(messagesRef, (snapshot) => {
  const newMsg = snapshot.val();
  io.emit('receiveMessage', newMsg);
});

io.on('connection', (socket) => {
  console.log('Yeni istifadəçi qoşuldu:', socket.id);

  get(messagesRef).then((snapshot) => {
    if (snapshot.exists()) {
      const allMessages = Object.values(snapshot.val());
      socket.emit('loadAllMessages', allMessages);
    }
  }).catch(err => console.error("Köhnə mesajlar çəkilərkən xəta:", err));

  socket.on('login', async (data, callback) => {
    try {
      const { nick, pass } = data;
      const userRef = ref(db, 'users/' + nick);
      const snapshot = await get(userRef);

      if (snapshot.exists()) {
        if (snapshot.val().password !== pass) {
          return callback({ success: false, message: "Şifrə yanlışdır!" });
        }
      } else {
        await set(userRef, { password: pass });
      }

      socket.nick = nick;
      activeUsers[nick] = socket.id;
      
      callback({ success: true });

      // === YENİ: İstifadəçi loqin olduqda Firebase-dən ONUN KÖHNƏ ŞƏXSİ MESAJLARINI yükləyirik ===
      get(privateMessagesRef).then((pSnapshot) => {
        if (pSnapshot.exists()) {
          const allPrivate = Object.values(pSnapshot.val());
          // Yalnız bu istifadəçinin göndərdiyi və ya ona gələn gizli mesajları süzürük
          const myPrivateMessages = allPrivate.filter(msg => msg.sender === nick || msg.recipient === nick);
          socket.emit('loadPrivateMessages', myPrivateMessages);
        }
      }).catch(err => console.error("Köhnə şəxsi mesajlar yüklənərkən xəta:", err));

    } catch (err) {
      console.error("Login zamanı xəta:", err);
      callback({ success: false, message: "Sistemdə xəta baş verdi, yenidən cəhd edin." });
    }
  });

  // === YENİLƏNDİ: ŞƏXSİ MESAJLAR ARTIQ FİREBASE-Ə YAZILIR ===
  socket.on('sendPrivateMessage', (data) => {
    const { sender, recipient, text } = data;

    // Mesajı Firebase bazasına push edirik (Yadda qalsın deyə)
    push(privateMessagesRef, { sender, recipient, text }).catch(err => console.error("Şəxsi mesaj bazaya yazılarkən xəta:", err));

    const targetSocketId = activeUsers[recipient];
    if (targetSocketId) {
      io.to(targetSocketId).emit('receivePrivateMessage', { sender, recipient, text });
    } else {
      // İstifadəçi onlayn olmasa belə mesaj bazaya yazıldı, sadəcə anlıq çatmadığı üçün məlumat veririk
      socket.emit('receivePrivateMessage', { 
        sender: 'Sistem', 
        text: `⚠️ ${recipient} hazırda onlayn deyil, lakin mesajınız qeydə alındı. Daxil olduqda görəcək.` 
      });
    }
  });

  socket.on('sendMessage', (data) => {
    push(messagesRef, data).catch(err => console.error("Mesaj yazılarkən xəta:", err));
  });

  socket.on('disconnect', () => {
    if (socket.nick) {
      delete activeUsers[socket.nick];
    }
  });
});

server.listen(PORT, () => {
  console.log(`Server ${PORT} portunda fəaliyyətə başladı...`);
});
