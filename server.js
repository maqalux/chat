const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { initializeApp } = require('firebase/app');
const { getDatabase, ref, push, onChildAdded, get, set } = require('firebase/database');

const app = express();
const server = http.createServer(app);

// ÇATIN SƏHİFƏDƏ İŞLƏMƏSİ ÜÇÜN BU HİSSƏYƏ CORS İCAZƏSİ ƏLAVƏ EDİLDİ
const io = new Server(server, {
  cors: {
    origin: "*", // Bütün kənar saytlardan (məsələn GitHub Pages-dən) gələn qoşulmalara icazə verir
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
          callback({ success: false, message: "Şifrə yanlışdır!" });
        } else {
          callback({ success: true });
        }
      } else {
        await set(userRef, { password: pass });
        callback({ success: true });
      }
    } catch (err) {
      console.error("Login zamanı xəta:", err);
      callback({ success: false, message: "Sistemdə xəta baş verdi, yenidən cəhd edin." });
    }
  });

  socket.on('sendMessage', (data) => {
    push(messagesRef, data).catch(err => console.error("Mesaj yazılarkən xəta:", err));
  });
});

server.listen(PORT, () => {
  console.log(`Server ${PORT} portunda fəaliyyətə başladı...`);
});
