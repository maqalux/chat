const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { initializeApp } = require('firebase/app');
const { getDatabase, ref, push, onChildAdded, onChildChanged, get, set, update, remove } = require('firebase/database');

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
app.use(express.json());

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
const usersRef = ref(db, 'users');

// nick -> socket.id (yalnız aktiv/onlayn istifadəçilər)
const activeUsers = {};

// ---------------------------------------------------------------------------
// ROL İERARXİYASI
// ---------------------------------------------------------------------------
const ROLE_RANK = { admin: 3, moderator: 2, user: 1 };
function roleRank(role) {
  return ROLE_RANK[role] || 1;
}

// ---------------------------------------------------------------------------
// REAL-TIME: Yeni ümumi mesaj əlavə olunanda hamıya göndər
// ---------------------------------------------------------------------------
onChildAdded(messagesRef, (snapshot) => {
  io.emit('receiveMessage', snapshot.val());
});

onChildChanged(messagesRef, (snapshot) => {
  io.emit('messageUpdated', snapshot.val());
});

onChildChanged(privateMessagesRef, (snapshot) => {
  const msg = snapshot.val();
  if (!msg) return;
  const targets = new Set([msg.sender, msg.recipient]);
  targets.forEach((nick) => {
    const sId = activeUsers[nick];
    if (sId) io.to(sId).emit('privateMessageUpdated', msg);
  });
});

// ---------------------------------------------------------------------------
// KÖMƏKÇİ FUNKSİYALAR
// ---------------------------------------------------------------------------
async function getUserData(nick) {
  const snapshot = await get(ref(db, 'users/' + nick));
  return snapshot.exists() ? snapshot.val() : null;
}

async function getAllUsersList() {
  const snapshot = await get(usersRef);
  if (!snapshot.exists()) return [];
  const usersObj = snapshot.val();
  return Object.keys(usersObj).map((nick) => ({
    nick,
    role: usersObj[nick].role || 'user',
    avatarUrl: usersObj[nick].avatarUrl || '',
    isBanned: !!usersObj[nick].isBanned,
    online: !!activeUsers[nick]
  }));
}

async function broadcastUserList() {
  const list = await getAllUsersList();
  io.emit('updateActiveUsers', list);
}

function generateResetCode() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}

// ---------------------------------------------------------------------------
// SOCKET BAĞLANTISI
// ---------------------------------------------------------------------------
io.on('connection', (socket) => {
  console.log('Yeni istifadəçi qoşuldu:', socket.id);

  // Köhnə ümumi mesajları yüklə
  get(messagesRef).then((snapshot) => {
    if (snapshot.exists()) {
      const allMessages = Object.values(snapshot.val());
      socket.emit('loadAllMessages', allMessages);
    }
  }).catch(err => console.error("Köhnə mesaj xətası:", err));

  // -------------------------------------------------------------------------
  // GİRİŞ VƏ QEYDİYYAT SİSTEMİ
  // -------------------------------------------------------------------------
  socket.on('login', async (data, callback) => {
    try {
      const { nick, pass, email } = data;
      const userRef = ref(db, 'users/' + nick);
      const snapshot = await get(userRef);

      let userRole = "user";
      let userAvatar = "";

      if (snapshot.exists()) {
        const existing = snapshot.val();

        if (existing.isBanned) {
          return callback({ success: false, message: "Hesabınız qara siyahıya alınıb. Mesajlaşma və giriş bloklanıb." });
        }

        if (existing.password !== pass) {
          return callback({ success: false, message: "Şifrə yanlışdır!" });
        }
        userRole = existing.role || "user";
        userAvatar = existing.avatarUrl || "";
      } else {
        // Yeni istifadəçi avtomatik qeydiyyatdan keçir
        await set(userRef, {
          password: pass,
          role: "user",
          avatarUrl: "",
          email: email || "",
          isBanned: false,
          createdAt: Date.now()
        });
      }

      socket.nick = nick;
      socket.role = userRole;
      activeUsers[nick] = socket.id;

      callback({ success: true, role: userRole, avatarUrl: userAvatar });

      await broadcastUserList();

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

  // -------------------------------------------------------------------------
  // GOOGLE İLƏ GİRİŞ
  // Frontend Firebase client SDK ilə Google popup/redirect girişini özü edir,
  // alınan istifadəçi məlumatlarını (uid, e-poçt, ad, şəkil) bura göndərir.
  // Biz həmin uid əsasında istifadəçi qeydini yaradır/varsa gətiririk.
  // -------------------------------------------------------------------------
  socket.on('googleLogin', async (data, callback) => {
    try {
      const { displayName, photoUrl, googleEmail, googleUid } = data;

      if (!googleUid || !googleEmail) {
        return callback({ success: false, message: "Google hesabından məlumat alına bilmədi." });
      }

      const baseNick = (displayName || googleEmail.split('@')[0]).trim().replace(/\s+/g, '_');

      // Əvvəlcə bu googleUid ilə qeydiyyatlı istifadəçi axtarırıq
      const allUsersSnap = await get(usersRef);
      let existingNick = null;

      if (allUsersSnap.exists()) {
        const allUsers = allUsersSnap.val();
        for (const nickKey of Object.keys(allUsers)) {
          if (allUsers[nickKey].googleUid === googleUid) {
            existingNick = nickKey;
            break;
          }
        }
      }

      let finalNick = existingNick;
      let userRole = "user";
      let userAvatar = "";

      if (existingNick) {
        const existing = (await get(ref(db, 'users/' + existingNick))).val();
        if (existing.isBanned) {
          return callback({ success: false, message: "Hesabınız qara siyahıya alınıb. Mesajlaşma və giriş bloklanıb." });
        }
        userRole = existing.role || "user";
        userAvatar = existing.avatarUrl || photoUrl || "";
      } else {
        // Yeni nick tap (toqquşma yoxlanışı)
        let candidate = baseNick;
        let counter = 1;
        while ((await get(ref(db, 'users/' + candidate))).exists()) {
          candidate = `${baseNick}${counter}`;
          counter++;
        }
        finalNick = candidate;
        userAvatar = photoUrl || "";

        await set(ref(db, 'users/' + finalNick), {
          password: null,
          role: "user",
          avatarUrl: userAvatar,
          email: googleEmail,
          googleUid: googleUid,
          isBanned: false,
          createdAt: Date.now()
        });
      }

      socket.nick = finalNick;
      socket.role = userRole;
      activeUsers[finalNick] = socket.id;

      callback({ success: true, nick: finalNick, role: userRole, avatarUrl: userAvatar });

      await broadcastUserList();

      get(privateMessagesRef).then((pSnapshot) => {
        if (pSnapshot.exists()) {
          const allPrivate = Object.values(pSnapshot.val());
          const myPrivateMessages = allPrivate.filter(msg => msg.sender === finalNick || msg.recipient === finalNick);
          socket.emit('loadPrivateMessages', myPrivateMessages);
        }
      }).catch(err => console.error("Şəxsi mesaj yükləmə xətası:", err));

    } catch (err) {
      console.error("Google login xətası:", err);
      callback({ success: false, message: "Google ilə giriş zamanı sistem xətası baş verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // ŞİFRƏNİ UNUTDUM — KOD GÖNDƏRMƏ VƏ TƏSDİQ
  // -------------------------------------------------------------------------
  socket.on('requestPasswordReset', async (data, callback) => {
    try {
      const { nick } = data;
      const userData = await getUserData(nick);

      if (!userData) {
        return callback({ success: false, message: "Bu istifadəçi adı ilə qeydiyyat tapılmadı." });
      }
      if (!userData.email) {
        return callback({ success: false, message: "Bu hesaba e-poçt ünvanı bağlanmayıb, şifrəni bərpa etmək mümkün deyil." });
      }

      const code = generateResetCode();
      await set(ref(db, 'passwordResets/' + nick), {
        code,
        expiresAt: Date.now() + 15 * 60 * 1000 // 15 dəqiqə
      });

      // Qeyd: real e-poçt göndərmə inteqrasiyası (məs. nodemailer) burada
      // qoşulmalıdır. Hazırkı mərhələdə kodu cavab olaraq qaytarırıq ki,
      // frontend "e-poçtunuza göndərildi" simulyasiyasını göstərə bilsin.
      console.log(`Şifrə bərpa kodu (${nick}): ${code}`);

      callback({ success: true, message: "Bərpa kodu e-poçt ünvanınıza göndərildi.", debugCode: code });
    } catch (err) {
      console.error("Şifrə bərpa tələbi xətası:", err);
      callback({ success: false, message: "Sistem xətası baş verdi." });
    }
  });

  socket.on('confirmPasswordReset', async (data, callback) => {
    try {
      const { nick, code, newPassword } = data;
      const resetSnap = await get(ref(db, 'passwordResets/' + nick));

      if (!resetSnap.exists()) {
        return callback({ success: false, message: "Bərpa tələbi tapılmadı, yenidən cəhd edin." });
      }

      const resetData = resetSnap.val();
      if (Date.now() > resetData.expiresAt) {
        await remove(ref(db, 'passwordResets/' + nick));
        return callback({ success: false, message: "Bərpa kodunun vaxtı bitib, yenidən tələb edin." });
      }

      if (resetData.code !== code) {
        return callback({ success: false, message: "Daxil etdiyiniz kod yanlışdır." });
      }

      if (!newPassword || newPassword.length < 4) {
        return callback({ success: false, message: "Yeni şifrə ən azı 4 simvol olmalıdır." });
      }

      await update(ref(db, 'users/' + nick), { password: newPassword });
      await remove(ref(db, 'passwordResets/' + nick));

      callback({ success: true, message: "Şifrəniz uğurla yeniləndi! İndi yeni şifrə ilə daxil ola bilərsiniz." });
    } catch (err) {
      console.error("Şifrə bərpa təsdiq xətası:", err);
      callback({ success: false, message: "Sistem xətası baş verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // PROFİL AYARLARI: ŞİFRƏ YENİLƏMƏ (köhnə şifrə məcburi)
  // -------------------------------------------------------------------------
  socket.on('updatePassword', async (data, callback) => {
    try {
      const { nick, oldPassword, newPassword } = data;
      if (!socket.nick || socket.nick !== nick) {
        return callback({ success: false, message: "Bu əməliyyat üçün icazəniz yoxdur." });
      }

      const userData = await getUserData(nick);
      if (!userData) {
        return callback({ success: false, message: "İstifadəçi tapılmadı." });
      }
      if (userData.password !== oldPassword) {
        return callback({ success: false, message: "Köhnə şifrə yanlışdır." });
      }
      if (!newPassword || newPassword.length < 4) {
        return callback({ success: false, message: "Yeni şifrə ən azı 4 simvol olmalıdır." });
      }

      await update(ref(db, 'users/' + nick), { password: newPassword });
      callback({ success: true, message: "Şifrəniz uğurla yeniləndi!" });
    } catch (err) {
      console.error("Şifrə yeniləmə xətası:", err);
      callback({ success: false, message: "Sistem xətası baş verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // PROFİL AYARLARI: E-POÇT YENİLƏMƏ (təsdiq tələb olunmur)
  // -------------------------------------------------------------------------
  socket.on('updateEmail', async (data, callback) => {
    try {
      const { nick, newEmail } = data;
      if (!socket.nick || socket.nick !== nick) {
        return callback({ success: false, message: "Bu əməliyyat üçün icazəniz yoxdur." });
      }
      if (!newEmail || !newEmail.includes('@')) {
        return callback({ success: false, message: "Düzgün e-poçt ünvanı daxil edin." });
      }

      await update(ref(db, 'users/' + nick), { email: newEmail });
      callback({ success: true, message: "E-poçt ünvanınız uğurla yeniləndi!" });
    } catch (err) {
      console.error("E-poçt yeniləmə xətası:", err);
      callback({ success: false, message: "Sistem xətası baş verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // PROFİL ŞƏKLİNİ FIREBASE BAZASINDA YENİLƏYƏN HİSSƏ
  // -------------------------------------------------------------------------
  socket.on('updateAvatar', async (data) => {
    try {
      const { nick, avatarUrl } = data;
      const userRef = ref(db, 'users/' + nick);
      await update(userRef, { avatarUrl: avatarUrl });
      console.log(`Profil şəkli Firebase-də yeniləndi: ${nick}`);
      await broadcastUserList();
    } catch (err) {
      console.error("Firebase avatar yeniləmə xətası:", err);
    }
  });

  // -------------------------------------------------------------------------
  // ÜMUMİ MESAJ GÖNDƏRİLMƏSİ (Mətn, Şəkil, GIF, Stiker, Reply, Forward)
  // -------------------------------------------------------------------------
  socket.on('sendMessage', async (data) => {
    try {
      const senderData = await getUserData(data.sender);
      if (senderData && senderData.isBanned) {
        socket.emit('actionBlocked', { message: "Qara siyahıda olduğunuz üçün mesaj göndərə bilməzsiniz." });
        return;
      }

      const newMsgRef = push(messagesRef);
      const msgData = {
        id: newMsgRef.key,
        sender: data.sender,
        senderAvatar: (senderData && senderData.avatarUrl) || "",
        text: data.text || "",
        mediaType: data.mediaType || "text", // text | image | gif | sticker
        mediaUrl: data.mediaUrl || "",
        replyTo: data.replyTo || null, // { id, sender, text } qısa snapshot
        forwardedFrom: data.forwardedFrom || null, // orijinal göndərənin adı
        reactions: {},
        readBy: [],
        deliveredTo: [],
        isDeleted: false,
        timestamp: Date.now()
      };
      await set(newMsgRef, msgData);
    } catch (err) {
      console.error("Mesaj yazılma xətası:", err);
    }
  });

  // -------------------------------------------------------------------------
  // ŞƏXSİ MESAJ GÖNDƏRİLMƏSİ
  // -------------------------------------------------------------------------
  socket.on('sendPrivateMessage', async (data) => {
    try {
      const { sender, recipient, text, mediaType, mediaUrl, replyTo, forwardedFrom } = data;

      const senderData = await getUserData(sender);
      if (senderData && senderData.isBanned) {
        socket.emit('actionBlocked', { message: "Qara siyahıda olduğunuz üçün mesaj göndərə bilməzsiniz." });
        return;
      }

      const newPrivRef = push(privateMessagesRef);

      const privData = {
        id: newPrivRef.key,
        sender,
        senderAvatar: (senderData && senderData.avatarUrl) || "",
        recipient,
        text: text || "",
        mediaType: mediaType || "text",
        mediaUrl: mediaUrl || "",
        replyTo: replyTo || null,
        forwardedFrom: forwardedFrom || null,
        reactions: {},
        readBy: [],
        deliveredTo: [],
        isDeleted: false,
        timestamp: Date.now()
      };

      await set(newPrivRef, privData);

      const targetSocketId = activeUsers[recipient];
      if (targetSocketId) {
        privData.deliveredTo = [recipient];
        await update(ref(db, `private_messages/${privData.id}`), { deliveredTo: [recipient] });
        io.to(targetSocketId).emit('receivePrivateMessage', privData);
        socket.emit('receivePrivateMessage', privData); // göndərənə də əks-sədası
      } else {
        socket.emit('receivePrivateMessage', privData);
        socket.emit('receivePrivateMessage', {
          sender: 'Sistem',
          recipient: sender,
          text: `⚠️ ${recipient} onlayn deyil, mesaj qeydə alındı.`,
          mediaType: 'text',
          reactions: {},
          isDeleted: false
        });
      }
    } catch (err) {
      console.error("Şəxsi mesaj yazılma xətası:", err);
    }
  });

  // -------------------------------------------------------------------------
  // MESAJ OXUNDU / ÇATDIRILDI STATUSU (WhatsApp tipli tək/cüt xətt)
  // -------------------------------------------------------------------------
  socket.on('markDelivered', async (data) => {
    try {
      const { msgId, isPrivate, reader } = data;
      const path = isPrivate ? `private_messages/${msgId}` : `messages/${msgId}`;
      const snap = await get(ref(db, path));
      if (!snap.exists()) return;
      const msg = snap.val();
      const deliveredTo = new Set(msg.deliveredTo || []);
      deliveredTo.add(reader);
      await update(ref(db, path), { deliveredTo: Array.from(deliveredTo) });
    } catch (err) {
      console.error("Çatdırılma statusu xətası:", err);
    }
  });

  socket.on('markRead', async (data) => {
    try {
      const { msgId, isPrivate, reader } = data;
      const path = isPrivate ? `private_messages/${msgId}` : `messages/${msgId}`;
      const snap = await get(ref(db, path));
      if (!snap.exists()) return;
      const msg = snap.val();
      const readBy = new Set(msg.readBy || []);
      const deliveredTo = new Set(msg.deliveredTo || []);
      readBy.add(reader);
      deliveredTo.add(reader);
      await update(ref(db, path), { readBy: Array.from(readBy), deliveredTo: Array.from(deliveredTo) });
    } catch (err) {
      console.error("Oxunma statusu xətası:", err);
    }
  });

  // -------------------------------------------------------------------------
  // MESAJA REAKSİYA VERMƏ
  // -------------------------------------------------------------------------
  socket.on('addReaction', async (data) => {
    try {
      const { msgId, isPrivate, nick, emoji } = data;
      const path = isPrivate ? `private_messages/${msgId}` : `messages/${msgId}`;
      const snap = await get(ref(db, path));
      if (!snap.exists()) return;
      const msg = snap.val();
      const reactions = msg.reactions || {};

      // Eyni emojini yenidən basarsa, reaksiyanı geri götürürük (toggle)
      if (reactions[nick] === emoji) {
        delete reactions[nick];
      } else {
        reactions[nick] = emoji;
      }

      await update(ref(db, path), { reactions });

      if (isPrivate) {
        const targets = new Set([msg.sender, msg.recipient]);
        targets.forEach((n) => {
          const sId = activeUsers[n];
          if (sId) io.to(sId).emit('privateMessageUpdated', { ...msg, reactions });
        });
      }
      // Ümumi mesajlar onChildChanged vasitəsilə avtomatik yayımlanır
    } catch (err) {
      console.error("Reaksiya xətası:", err);
    }
  });

  // -------------------------------------------------------------------------
  // ADMİN / MODERATOR MESAJ SİLMƏ SİSTEMİ
  // -------------------------------------------------------------------------
  socket.on('deleteMessage', async (data, callback) => {
    const cb = typeof callback === 'function' ? callback : () => {};
    try {
      const { msgId, isPrivate } = typeof data === 'string' ? { msgId: data, isPrivate: false } : data;

      if (socket.role !== 'admin' && socket.role !== 'moderator') {
        socket.emit('actionBlocked', { message: "Mesaj silmək üçün icazəniz yoxdur." });
        return cb({ success: false });
      }

      const path = isPrivate ? `private_messages/${msgId}` : `messages/${msgId}`;
      const snap = await get(ref(db, path));
      if (!snap.exists()) return cb({ success: false });
      const msg = snap.val();

      // Moderator admin-in mesajını silə bilməz
      if (socket.role === 'moderator') {
        const authorData = await getUserData(msg.sender);
        if (authorData && authorData.role === 'admin') {
          socket.emit('actionBlocked', { message: "Admin-in mesajını silmək icazəniz yoxdur." });
          return cb({ success: false });
        }
      }

      await update(ref(db, path), {
        text: "🗑️ Bu mesaj silinib.",
        mediaType: "text",
        mediaUrl: "",
        isDeleted: true
      });

      if (isPrivate) {
        const targets = new Set([msg.sender, msg.recipient]);
        targets.forEach((n) => {
          const sId = activeUsers[n];
          if (sId) io.to(sId).emit('privateMessageUpdated', { ...msg, text: "🗑️ Bu mesaj silinib.", mediaType: "text", mediaUrl: "", isDeleted: true });
        });
      }
      cb({ success: true });
    } catch (err) {
      console.error("Silinmə xətası:", err);
      cb({ success: false });
    }
  });

  // -------------------------------------------------------------------------
  // İSTİFADƏÇİ ÖZ MESAJINI SİLMƏ
  // -------------------------------------------------------------------------
  socket.on('deleteOwnMessage', async (data, callback) => {
    const cb = typeof callback === 'function' ? callback : () => {};
    try {
      const { msgId, isPrivate } = data;
      const path = isPrivate ? `private_messages/${msgId}` : `messages/${msgId}`;
      const snap = await get(ref(db, path));
      if (!snap.exists()) return cb({ success: false });
      const msg = snap.val();

      if (msg.sender !== socket.nick) {
        socket.emit('actionBlocked', { message: "Yalnız öz mesajınızı silə bilərsiniz." });
        return cb({ success: false });
      }

      await update(ref(db, path), {
        text: "🗑️ Bu mesaj silinib.",
        mediaType: "text",
        mediaUrl: "",
        isDeleted: true
      });

      if (isPrivate) {
        const targets = new Set([msg.sender, msg.recipient]);
        targets.forEach((n) => {
          const sId = activeUsers[n];
          if (sId) io.to(sId).emit('privateMessageUpdated', { ...msg, text: "🗑️ Bu mesaj silinib.", mediaType: "text", mediaUrl: "", isDeleted: true });
        });
      }
      cb({ success: true });
    } catch (err) {
      console.error("Öz mesajını silmə xətası:", err);
      cb({ success: false });
    }
  });

  // -------------------------------------------------------------------------
  // QARA SİYAHI SİSTEMİ
  // -------------------------------------------------------------------------
  socket.on('banUser', async (data, callback) => {
    try {
      const { targetNick } = data;

      if (!socket.nick) {
        return callback({ success: false, message: "Bu əməliyyat üçün daxil olmalısınız." });
      }

      if (targetNick === socket.nick) {
        return callback({ success: false, message: "Özünüzü qara siyahıya əlavə edə bilməzsiniz." });
      }

      const targetData = await getUserData(targetNick);
      if (!targetData) {
        return callback({ success: false, message: "İstifadəçi tapılmadı." });
      }

      // İCAZƏ QAYDALARI:
      // Admin -> admin istisna hamını banlaya bilər
      // Moderator -> yalnız adi istifadəçiləri banlaya bilər (admin və digər moderatoru banlaya bilməz)
      // Adi istifadəçi -> yalnız digər adi istifadəçini banlaya bilər (admin/moderatoru banlaya bilməz)
      if (socket.role === 'admin') {
        if (targetData.role === 'admin') {
          return callback({ success: false, message: "Admin-i qara siyahıya əlavə etmək olmaz." });
        }
      } else if (socket.role === 'moderator') {
        if (roleRank(targetData.role) >= roleRank('moderator')) {
          return callback({ success: false, message: "Bu istifadəçini qara siyahıya əlavə etmək icazəniz yoxdur." });
        }
      } else {
        // adi istifadəçi
        if (roleRank(targetData.role) > roleRank('user')) {
          return callback({ success: false, message: "Admin və ya moderatoru qara siyahıya əlavə edə bilməzsiniz." });
        }
      }

      await update(ref(db, 'users/' + targetNick), { isBanned: true });

      // Onlayndırsa, bağlantısını kəs və siyahıdan çıxar
      const targetSocketId = activeUsers[targetNick];
      if (targetSocketId) {
        io.to(targetSocketId).emit('youAreBanned');
        delete activeUsers[targetNick];
      }

      await broadcastUserList();
      callback({ success: true, message: `${targetNick} qara siyahıya əlavə edildi.` });
    } catch (err) {
      console.error("Ban xətası:", err);
      callback({ success: false, message: "Sistem xətası baş verdi." });
    }
  });

  socket.on('unbanUser', async (data, callback) => {
    try {
      const { targetNick } = data;
      if (socket.role !== 'admin' && socket.role !== 'moderator') {
        return callback({ success: false, message: "Bu əməliyyat üçün icazəniz yoxdur." });
      }

      const targetData = await getUserData(targetNick);
      if (!targetData) {
        return callback({ success: false, message: "İstifadəçi tapılmadı." });
      }

      if (socket.role === 'moderator' && roleRank(targetData.role) >= roleRank('moderator')) {
        return callback({ success: false, message: "Bu istifadəçini qara siyahıdan çıxarmaq icazəniz yoxdur." });
      }

      await update(ref(db, 'users/' + targetNick), { isBanned: false });
      await broadcastUserList();
      callback({ success: true, message: `${targetNick} qara siyahıdan çıxarıldı.` });
    } catch (err) {
      console.error("Unban xətası:", err);
      callback({ success: false, message: "Sistem xətası baş verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // ROL DƏYİŞMƏ: MODERATOR TƏYİN ETMƏ / GERİ ALMA (yalnız admin)
  // -------------------------------------------------------------------------
  socket.on('promoteToModerator', async (data, callback) => {
    try {
      const { targetNick } = data;
      if (socket.role !== 'admin') {
        return callback({ success: false, message: "Yalnız admin moderator təyin edə bilər." });
      }

      const targetData = await getUserData(targetNick);
      if (!targetData) {
        return callback({ success: false, message: "İstifadəçi tapılmadı." });
      }
      if (targetData.role === 'admin') {
        return callback({ success: false, message: "Admin-in rolunu dəyişmək olmaz." });
      }

      await update(ref(db, 'users/' + targetNick), { role: 'moderator' });

      const targetSocketId = activeUsers[targetNick];
      if (targetSocketId) {
        const targetSocket = io.sockets.sockets.get(targetSocketId);
        if (targetSocket) targetSocket.role = 'moderator';
        io.to(targetSocketId).emit('roleChanged', { role: 'moderator' });
      }

      await broadcastUserList();
      callback({ success: true, message: `${targetNick} moderator təyin edildi.` });
    } catch (err) {
      console.error("Moderator təyin etmə xətası:", err);
      callback({ success: false, message: "Sistem xətası baş verdi." });
    }
  });

  socket.on('demoteToUser', async (data, callback) => {
    try {
      const { targetNick } = data;
      if (socket.role !== 'admin') {
        return callback({ success: false, message: "Yalnız admin rol geri ala bilər." });
      }

      const targetData = await getUserData(targetNick);
      if (!targetData) {
        return callback({ success: false, message: "İstifadəçi tapılmadı." });
      }
      if (targetData.role === 'admin') {
        return callback({ success: false, message: "Admin-in rolunu dəyişmək olmaz." });
      }

      await update(ref(db, 'users/' + targetNick), { role: 'user' });

      const targetSocketId = activeUsers[targetNick];
      if (targetSocketId) {
        const targetSocket = io.sockets.sockets.get(targetSocketId);
        if (targetSocket) targetSocket.role = 'user';
        io.to(targetSocketId).emit('roleChanged', { role: 'user' });
      }

      await broadcastUserList();
      callback({ success: true, message: `${targetNick} adi istifadəçi statusuna qaytarıldı.` });
    } catch (err) {
      console.error("Rol geri alma xətası:", err);
      callback({ success: false, message: "Sistem xətası baş verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // HESAB SİLMƏ: ADMİN TƏRƏFİNDƏN İSTİFADƏÇİNİ SİLMƏ
  // -------------------------------------------------------------------------
  socket.on('deleteUserAccount', async (data, callback) => {
    try {
      const { targetNick } = data;
      if (socket.role !== 'admin') {
        return callback({ success: false, message: "Yalnız admin istifadəçi hesabını silə bilər." });
      }

      const targetData = await getUserData(targetNick);
      if (!targetData) {
        return callback({ success: false, message: "İstifadəçi tapılmadı." });
      }

      await remove(ref(db, 'users/' + targetNick));

      const targetSocketId = activeUsers[targetNick];
      if (targetSocketId) {
        io.to(targetSocketId).emit('accountDeleted');
        delete activeUsers[targetNick];
      }

      await broadcastUserList();
      callback({ success: true, message: `${targetNick} hesabı silindi.` });
    } catch (err) {
      console.error("Hesab silmə xətası:", err);
      callback({ success: false, message: "Sistem xətası baş verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // HESAB SİLMƏ: İSTİFADƏÇİ ÖZ HESABINI SİLİR
  // -------------------------------------------------------------------------
  socket.on('deleteOwnAccount', async (data, callback) => {
    try {
      const { nick, password } = data;
      if (!socket.nick || socket.nick !== nick) {
        return callback({ success: false, message: "Bu əməliyyat üçün icazəniz yoxdur." });
      }

      const userData = await getUserData(nick);
      if (!userData) {
        return callback({ success: false, message: "İstifadəçi tapılmadı." });
      }

      // Google ilə qoşulan hesablarda şifrə olmaya bilər
      if (userData.password !== null && userData.password !== password) {
        return callback({ success: false, message: "Şifrə yanlışdır." });
      }

      await remove(ref(db, 'users/' + nick));
      delete activeUsers[nick];
      await broadcastUserList();

      callback({ success: true, message: "Hesabınız uğurla silindi." });
    } catch (err) {
      console.error("Öz hesabını silmə xətası:", err);
      callback({ success: false, message: "Sistem xətası baş verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // ADMİN: BİR İSTİFADƏÇİNİN BÜTÜN MESAJLARINI SİLMƏ
  // -------------------------------------------------------------------------
  socket.on('deleteAllMessagesOfUser', async (data, callback) => {
    try {
      const { targetNick } = data;
      if (socket.role !== 'admin') {
        return callback({ success: false, message: "Yalnız admin bütün mesajları silə bilər." });
      }

      const targetData = await getUserData(targetNick);
      if (!targetData) {
        return callback({ success: false, message: "İstifadəçi tapılmadı." });
      }

      // Ümumi mesajlar
      const msgsSnap = await get(messagesRef);
      if (msgsSnap.exists()) {
        const allMsgs = msgsSnap.val();
        const updates = {};
        Object.keys(allMsgs).forEach((key) => {
          if (allMsgs[key].sender === targetNick && !allMsgs[key].isDeleted) {
            updates[`messages/${key}/text`] = "🗑️ Bu mesaj silinib.";
            updates[`messages/${key}/mediaType`] = "text";
            updates[`messages/${key}/mediaUrl`] = "";
            updates[`messages/${key}/isDeleted`] = true;
          }
        });
        if (Object.keys(updates).length) await update(ref(db), updates);
      }

      // Şəxsi mesajlar
      const privSnap = await get(privateMessagesRef);
      if (privSnap.exists()) {
        const allPriv = privSnap.val();
        for (const key of Object.keys(allPriv)) {
          const m = allPriv[key];
          if (m.sender === targetNick && !m.isDeleted) {
            await update(ref(db, `private_messages/${key}`), {
              text: "🗑️ Bu mesaj silinib.",
              mediaType: "text",
              mediaUrl: "",
              isDeleted: true
            });
            const targets = new Set([m.sender, m.recipient]);
            targets.forEach((n) => {
              const sId = activeUsers[n];
              if (sId) io.to(sId).emit('privateMessageUpdated', { ...m, text: "🗑️ Bu mesaj silinib.", mediaType: "text", mediaUrl: "", isDeleted: true });
            });
          }
        }
      }

      callback({ success: true, message: `${targetNick} istifadəçisinin bütün mesajları silindi.` });
    } catch (err) {
      console.error("Bütün mesajları silmə xətası:", err);
      callback({ success: false, message: "Sistem xətası baş verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // BAĞLANTI KƏSİLƏNDƏ
  // -------------------------------------------------------------------------
  socket.on('disconnect', () => {
    if (socket.nick) {
      delete activeUsers[socket.nick];
      broadcastUserList();
    }
  });
});

server.listen(PORT, () => {
  console.log(`Server ${PORT} portunda aktivdir...`);
});