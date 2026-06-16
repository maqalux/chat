require('dotenv').config(); // Lokal mühitdə .env faylındakı gizli açarları oxumaq üçün

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { initializeApp } = require('firebase/app');
const { getDatabase, ref, push, onChildAdded, get, set } = require('firebase/database');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Render avtomatik olaraq PORT təyin edir, yoxdursa 3000 istifadə olunur
const PORT = process.env.PORT || 3000;

// İçi HTML ilə dolu olan "public" qovluğunu istifadəçiyə təqdim edirik
app.use(express.static('public'));

// Bütün Firebase konfiqurasiyaları process.env vasitəsilə gizlədilib
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

// Yeni mesaj Firebase-ə düşəndə bütün istifadəçilərə anında göndər
onChildAdded(messagesRef, (snapshot) => {
  const newMsg = snapshot.val();
  io.emit('receiveMessage', newMsg);
});

io.on('connection', (socket) => {
  console.log('Yeni istifadəçi qoşuldu:', socket.id);

  // İstifadəçi qoşulanda köhnə mesajları bazadan çəkib ona göndəririk
  get(messagesRef).then((snapshot) => {
    if (snapshot.exists()) {
      const allMessages = Object.values(snapshot.val());
      socket.emit('loadAllMessages', allMessages);
    }
  });

  // Login yoxlaması
  socket.on('login', async (data, callback) => {
    const { nick, pass } = data;
    const userRef = ref(db, 'users/' + nick);
    const snapshot = await get(userRef);

    if (snapshot.exists()) {
      if (snapshot.val().password !== pass) {
        callback({ success: false, message: "Şifrə yanlışdır!" });
      } else {
        callback({ success: true });
      }
    } else {
      await set(userRef, { password: pass });
      callback({ success: true });
    }
  });

  // İstifadəçi yeni mesaj yazanda Firebase-ə əlavə et
  socket.on('sendMessage', (data) => {
    push(messagesRef, data);
  });
});

server.listen(PORT, () => {
  console.log(`Server ${PORT} portunda fəaliyyətə başladı...`);
});
