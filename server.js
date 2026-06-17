const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { initializeApp } = require('firebase/app');
const { getDatabase, ref, push, onChildAdded, onChildChanged, get, set, update } = require('firebase/database');

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
const privateMessagesRef = ref(db, 'private_messages');

const activeUsers = {};

// Real-time: Yeni mesaj əlavə olunanda hamıya göndər
onChildAdded(messagesRef, (snapshot) => {
  io.emit('receiveMessage', snapshot.val());
});

// Real-time: Mesaj silinəndə/dəyişəndə hamıya xəbər et
onChildChanged(messagesRef, (snapshot) => {
  io.emit('messageUpdated', snapshot.val());
});

io.on('connection', (socket) => {
  console.log('Yeni istifadəçi qoşuldu:', socket.id);

  // Köhnə ümumi mesajları yüklə
  get(messagesRef).then((snapshot) => {
    if (snapshot.exists()) {
      const allMessages = Object.values(snapshot.val());
      socket.emit('loadAllMessages', allMessages);
    }
  }).catch(err => console.error("Köhnə mesaj xətası:", err));

  // Giriş və Qeydiyyat Sistemi
  socket.on('login', async (data, callback) => {
    try {
      const { nick, pass } = data;
      const userRef = ref(db, 'users/' + nick);
      const snapshot = await get(userRef);

      let userRole = "user";

      if (snapshot.exists()) {
        if (snapshot.val().password !== pass) {
          return callback({ success: false, message: "Şifrə yanlışdır!" });
        }
        userRole = snapshot.val().role || "user";
      } else {
        // Yeni istifadəçi avtomatik qeydiyyatdan keçir
        await set(userRef, { password: pass, role: "user" });
      }

      socket.nick = nick;
      socket.role = userRole;
      activeUsers[nick] = socket.id;
      
      callback({ success: true, role: userRole });

      // Aktiv istifadəçi siyahısını yenilə və hamıya bəyan et
      io.emit('updateActiveUsers', Object.keys(activeUsers));

      // Şəxsi mesajları yüklə
      get(privateMessagesRef).then((pSnapshot) => {
        if (pSnapshot.exists()) {
          const allPrivate = Object.values(pSnapshot.val());
          const myPrivateMessages = allPrivate.filter(msg => msg.sender === nick || msg.recipient === nick);
          socket.emit('loadPrivateMessages', myPrivateMessages);
        }
      }).catch(err => console.error("Şəxsi mesaj yükləmə xətası:", err));

    } catch (err) {
      console.error("Login xətası:", err);
      callback({ success: false, message: "Sistem xətası baş verdi." });
    }
  });

  // Ümumi Mesaj Göndərilməsi (Mətn və ya Şəkil)
  socket.on('sendMessage', (data) => {
    const newMsgRef = push(messagesRef);
    const msgData = {
      id: newMsgRef.key,
      sender: data.sender,
      text: data.text || "",
      mediaType: data.mediaType || "text", // "text" və ya "image"
      mediaUrl: data.mediaUrl || "",
      isDeleted: false
    };
    set(newMsgRef, msgData).catch(err => console.error("Mesaj yazılma xətası:", err));
  });

  // Şəxsi Mesaj Göndərilməsi
  socket.on('sendPrivateMessage', (data) => {
    const { sender, recipient, text, mediaType, mediaUrl } = data;
    const newPrivRef = push(privateMessagesRef);
    
    const privData = {
      id: newPrivRef.key,
      sender,
      recipient,
      text: text || "",
      mediaType: mediaType || "text",
      mediaUrl: mediaUrl || "",
      isDeleted: false
    };

    set(newPrivRef, privData).catch(err => console.error("Şəxsi mesaj yazılma xətası:", err));

    const targetSocketId = activeUsers[recipient];
    if (targetSocketId) {
      io.to(targetSocketId).emit('receivePrivateMessage', privData);
    } else {
      socket.emit('receivePrivateMessage', { 
        sender: 'Sistem', 
        text: `⚠️ ${recipient} onlayn deyil, mesaj qeydə alındı.`,
        mediaType: 'text'
      });
    }
  });

  // Admin Mesaj Silmə Sistemi
  socket.on('deleteMessage', async (msgId) => {
    if (socket.role === 'admin') {
      const specificMsgRef = ref(db, `messages/${msgId}`);
      update(specificMsgRef, {
        text: "🗑️ Bu mesaj admin tərəfindən silinib.",
        mediaType: "text",
        mediaUrl: "",
        isDeleted: true
      }).catch(err => console.error("Silinmə xətası:", err));
    }
  });

  // Bağlantı kəsiləndə
  socket.on('disconnect', () => {
    if (socket.nick) {
      delete activeUsers[socket.nick];
      io.emit('updateActiveUsers', Object.keys(activeUsers));
    }
  });
});

server.listen(PORT, () => {
  console.log(`Server ${PORT} portunda aktivdir...`);
});
