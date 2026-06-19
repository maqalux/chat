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

// nick -> socket.id (yalnÄ±z aktiv/onlayn istifadÉ™Ã§ilÉ™r)
const activeUsers = {};

// ---------------------------------------------------------------------------
// ROL Ä°ERARXÄ°YASI
// ---------------------------------------------------------------------------
const ROLE_RANK = { admin: 3, moderator: 2, user: 1 };
function roleRank(role) {
  return ROLE_RANK[role] || 1;
}

// ---------------------------------------------------------------------------
// REAL-TIME: Yeni Ã¼mumi mesaj É™lavÉ™ olunanda hamÄ±ya gÃ¶ndÉ™r
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
// KÃ–MÆKÃ‡Ä° FUNKSÄ°YALAR
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
// SOCKET BAÄžLANTISI
// ---------------------------------------------------------------------------
io.on('connection', (socket) => {
  console.log('Yeni istifadÉ™Ã§i qoÅŸuldu:', socket.id);

  // KÃ¶hnÉ™ Ã¼mumi mesajlarÄ± yÃ¼klÉ™
  get(messagesRef).then((snapshot) => {
    if (snapshot.exists()) {
      const allMessages = Object.values(snapshot.val());
      socket.emit('loadAllMessages', allMessages);
    }
  }).catch(err => console.error("KÃ¶hnÉ™ mesaj xÉ™tasÄ±:", err));

  // -------------------------------------------------------------------------
  // GÄ°RÄ°Åž VÆ QEYDÄ°YYAT SÄ°STEMÄ°
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
          return callback({ success: false, message: "HesabÄ±nÄ±z qara siyahÄ±ya alÄ±nÄ±b. MesajlaÅŸma vÉ™ giriÅŸ bloklanÄ±b." });
        }

        if (existing.password !== pass) {
          return callback({ success: false, message: "ÅžifrÉ™ yanlÄ±ÅŸdÄ±r!" });
        }
        userRole = existing.role || "user";
        userAvatar = existing.avatarUrl || "";
      } else {
        // Yeni istifadÉ™Ã§i avtomatik qeydiyyatdan keÃ§ir
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

      // ÅžÉ™xsi mesajlarÄ± yÃ¼klÉ™
      get(privateMessagesRef).then((pSnapshot) => {
        if (pSnapshot.exists()) {
          const allPrivate = Object.values(pSnapshot.val());
          const myPrivateMessages = allPrivate.filter(msg => msg.sender === nick || msg.recipient === nick);
          socket.emit('loadPrivateMessages', myPrivateMessages);
        }
      }).catch(err => console.error("ÅžÉ™xsi mesaj yÃ¼klÉ™mÉ™ xÉ™tasÄ±:", err));

    } catch (err) {
      console.error("Login xÉ™tasÄ±:", err);
      callback({ success: false, message: "Sistem xÉ™tasÄ± baÅŸ verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // GOOGLE Ä°LÆ GÄ°RÄ°Åž
  // Frontend Firebase client SDK ilÉ™ Google popup/redirect giriÅŸini Ã¶zÃ¼ edir,
  // alÄ±nan istifadÉ™Ã§i mÉ™lumatlarÄ±nÄ± (uid, e-poÃ§t, ad, ÅŸÉ™kil) bura gÃ¶ndÉ™rir.
  // Biz hÉ™min uid É™sasÄ±nda istifadÉ™Ã§i qeydini yaradÄ±r/varsa gÉ™tiririk.
  // -------------------------------------------------------------------------
  socket.on('googleLogin', async (data, callback) => {
    try {
      const { displayName, photoUrl, googleEmail, googleUid } = data;

      if (!googleUid || !googleEmail) {
        return callback({ success: false, message: "Google hesabÄ±ndan mÉ™lumat alÄ±na bilmÉ™di." });
      }

      const baseNick = (displayName || googleEmail.split('@')[0]).trim().replace(/\s+/g, '_');

      // ÆvvÉ™lcÉ™ bu googleUid ilÉ™ qeydiyyatlÄ± istifadÉ™Ã§i axtarÄ±rÄ±q
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
          return callback({ success: false, message: "HesabÄ±nÄ±z qara siyahÄ±ya alÄ±nÄ±b. MesajlaÅŸma vÉ™ giriÅŸ bloklanÄ±b." });
        }
        userRole = existing.role || "user";
        userAvatar = existing.avatarUrl || photoUrl || "";
      } else {
        // Yeni nick tap (toqquÅŸma yoxlanÄ±ÅŸÄ±)
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
      }).catch(err => console.error("ÅžÉ™xsi mesaj yÃ¼klÉ™mÉ™ xÉ™tasÄ±:", err));

    } catch (err) {
      console.error("Google login xÉ™tasÄ±:", err);
      callback({ success: false, message: "Google ilÉ™ giriÅŸ zamanÄ± sistem xÉ™tasÄ± baÅŸ verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // ÅžÄ°FRÆNÄ° UNUTDUM â€” KOD GÃ–NDÆRMÆ VÆ TÆSDÄ°Q
  // -------------------------------------------------------------------------
  socket.on('requestPasswordReset', async (data, callback) => {
    try {
      const { nick } = data;
      const userData = await getUserData(nick);

      if (!userData) {
        return callback({ success: false, message: "Bu istifadÉ™Ã§i adÄ± ilÉ™ qeydiyyat tapÄ±lmadÄ±." });
      }
      if (!userData.email) {
        return callback({ success: false, message: "Bu hesaba e-poÃ§t Ã¼nvanÄ± baÄŸlanmayÄ±b, ÅŸifrÉ™ni bÉ™rpa etmÉ™k mÃ¼mkÃ¼n deyil." });
      }

      const code = generateResetCode();
      await set(ref(db, 'passwordResets/' + nick), {
        code,
        expiresAt: Date.now() + 15 * 60 * 1000 // 15 dÉ™qiqÉ™
      });

      // Qeyd: real e-poÃ§t gÃ¶ndÉ™rmÉ™ inteqrasiyasÄ± (mÉ™s. nodemailer) burada
      // qoÅŸulmalÄ±dÄ±r. HazÄ±rkÄ± mÉ™rhÉ™lÉ™dÉ™ kodu cavab olaraq qaytarÄ±rÄ±q ki,
      // frontend "e-poÃ§tunuza gÃ¶ndÉ™rildi" simulyasiyasÄ±nÄ± gÃ¶stÉ™rÉ™ bilsin.
      console.log(`ÅžifrÉ™ bÉ™rpa kodu (${nick}): ${code}`);

      callback({ success: true, message: "BÉ™rpa kodu e-poÃ§t Ã¼nvanÄ±nÄ±za gÃ¶ndÉ™rildi.", debugCode: code });
    } catch (err) {
      console.error("ÅžifrÉ™ bÉ™rpa tÉ™lÉ™bi xÉ™tasÄ±:", err);
      callback({ success: false, message: "Sistem xÉ™tasÄ± baÅŸ verdi." });
    }
  });

  socket.on('confirmPasswordReset', async (data, callback) => {
    try {
      const { nick, code, newPassword } = data;
      const resetSnap = await get(ref(db, 'passwordResets/' + nick));

      if (!resetSnap.exists()) {
        return callback({ success: false, message: "BÉ™rpa tÉ™lÉ™bi tapÄ±lmadÄ±, yenidÉ™n cÉ™hd edin." });
      }

      const resetData = resetSnap.val();
      if (Date.now() > resetData.expiresAt) {
        await remove(ref(db, 'passwordResets/' + nick));
        return callback({ success: false, message: "BÉ™rpa kodunun vaxtÄ± bitib, yenidÉ™n tÉ™lÉ™b edin." });
      }

      if (resetData.code !== code) {
        return callback({ success: false, message: "Daxil etdiyiniz kod yanlÄ±ÅŸdÄ±r." });
      }

      if (!newPassword || newPassword.length < 4) {
        return callback({ success: false, message: "Yeni ÅŸifrÉ™ É™n azÄ± 4 simvol olmalÄ±dÄ±r." });
      }

      await update(ref(db, 'users/' + nick), { password: newPassword });
      await remove(ref(db, 'passwordResets/' + nick));

      callback({ success: true, message: "ÅžifrÉ™niz uÄŸurla yenilÉ™ndi! Ä°ndi yeni ÅŸifrÉ™ ilÉ™ daxil ola bilÉ™rsiniz." });
    } catch (err) {
      console.error("ÅžifrÉ™ bÉ™rpa tÉ™sdiq xÉ™tasÄ±:", err);
      callback({ success: false, message: "Sistem xÉ™tasÄ± baÅŸ verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // PROFÄ°L AYARLARI: ÅžÄ°FRÆ YENÄ°LÆMÆ (kÃ¶hnÉ™ ÅŸifrÉ™ mÉ™cburi)
  // -------------------------------------------------------------------------
  socket.on('updatePassword', async (data, callback) => {
    try {
      const { nick, oldPassword, newPassword } = data;
      if (!socket.nick || socket.nick !== nick) {
        return callback({ success: false, message: "Bu É™mÉ™liyyat Ã¼Ã§Ã¼n icazÉ™niz yoxdur." });
      }

      const userData = await getUserData(nick);
      if (!userData) {
        return callback({ success: false, message: "Ä°stifadÉ™Ã§i tapÄ±lmadÄ±." });
      }
      if (userData.password !== oldPassword) {
        return callback({ success: false, message: "KÃ¶hnÉ™ ÅŸifrÉ™ yanlÄ±ÅŸdÄ±r." });
      }
      if (!newPassword || newPassword.length < 4) {
        return callback({ success: false, message: "Yeni ÅŸifrÉ™ É™n azÄ± 4 simvol olmalÄ±dÄ±r." });
      }

      await update(ref(db, 'users/' + nick), { password: newPassword });
      callback({ success: true, message: "ÅžifrÉ™niz uÄŸurla yenilÉ™ndi!" });
    } catch (err) {
      console.error("ÅžifrÉ™ yenilÉ™mÉ™ xÉ™tasÄ±:", err);
      callback({ success: false, message: "Sistem xÉ™tasÄ± baÅŸ verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // PROFÄ°L AYARLARI: E-POÃ‡T YENÄ°LÆMÆ (tÉ™sdiq tÉ™lÉ™b olunmur)
  // -------------------------------------------------------------------------
  socket.on('updateEmail', async (data, callback) => {
    try {
      const { nick, newEmail } = data;
      if (!socket.nick || socket.nick !== nick) {
        return callback({ success: false, message: "Bu É™mÉ™liyyat Ã¼Ã§Ã¼n icazÉ™niz yoxdur." });
      }
      if (!newEmail || !newEmail.includes('@')) {
        return callback({ success: false, message: "DÃ¼zgÃ¼n e-poÃ§t Ã¼nvanÄ± daxil edin." });
      }

      await update(ref(db, 'users/' + nick), { email: newEmail });
      callback({ success: true, message: "E-poÃ§t Ã¼nvanÄ±nÄ±z uÄŸurla yenilÉ™ndi!" });
    } catch (err) {
      console.error("E-poÃ§t yenilÉ™mÉ™ xÉ™tasÄ±:", err);
      callback({ success: false, message: "Sistem xÉ™tasÄ± baÅŸ verdi." });
    }
  });

  // -------------------------------------------------------------------------
  // PROFÄ°L ÅžÆKLÄ°NÄ° FIREBASE BAZASINDA YENÄ°LÆYÆN HÄ°SSÆ
  // -------------------------------------------------------------------------
  socket.on('updateAvatar', async (data) => {
    try {
      const { nick, avatarUrl } = data;
      const userRef = ref(db, 'users/' + nick);
      await update(userRef, { avatarUrl: avatarUrl });
      console.log(`Profil ÅŸÉ™kli Firebase-dÉ™ yenilÉ™ndi: ${nick}`);
      await broadcastUserList();
    } catch (err) {
      console.error("Firebase avatar yenilÉ™mÉ™ xÉ™tasÄ±:", err);
    }
  });

  // -------------------------------------------------------------------------
  // ÃœMUMÄ° MESAJ GÃ–NDÆRÄ°LMÆSÄ° (MÉ™tn, ÅžÉ™kil, GIF, Stiker, Reply, Forward)
  // -------------------------------------------------------------------------
  socket.on('sendMessage', async (data) => {
    try {
      const senderData = await getUserData(data.sender);
      if (senderData && senderData.isBanned) {
        socket.emit('actionBlocked', { message: "Qara siyahÄ±da olduÄŸunuz Ã¼Ã§Ã¼n mesaj gÃ¶ndÉ™rÉ™ bilmÉ™zsiniz." });
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
        replyTo: data.replyTo || null, // { id, sender, text } qÄ±sa snapshot
        forwardedFrom: data.forwardedFrom || null, // orijinal gÃ¶ndÉ™rÉ™nin adÄ±
        reactions: {},
        readBy: [],
        deliveredTo: [],
        isDeleted: false,
        timestamp: Date.now()
      };
      await set(newMsgRef, msgData);
    } catch (err) {
      console.error("Mesaj yazÄ±lma xÉ™tasÄ±:", err);
    }
  });

  // -------------------------------------------------------------------------
  // ÅžÆXSÄ° MESAJ GÃ–NDÆRÄ°LMÆSÄ°
  // -------------------------------------------------------------------------
  socket.on('sendPrivateMessage', async (data) => {
    try {
      const { sender, recipient, text, mediaType, mediaUrl, replyTo, forwardedFrom } = data;

      const senderData = await getUserData(sender);
      if (senderData && senderData.isBanned) {
        socket.emit('actionBlocked', { message: "Qara siyahÄ±da olduÄŸunuz Ã¼Ã§Ã¼n mesaj gÃ¶ndÉ™rÉ™ bilmÉ™zsiniz." });
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
        socket.emit('receivePrivateMessage', privData); // gÃ¶ndÉ™rÉ™nÉ™ dÉ™ É™ks-sÉ™dasÄ±
      } else {
        socket.emit('receivePrivateMessage', privData);
        socket.emit('receivePrivateMessage', {
          sender: 'Sistem',
          recipient: sender,
          text: `âš ï¸ ${recipient} onlayn deyil, mesaj qeydÉ™ alÄ±ndÄ±.`,
          mediaType: 'text',
          reactions: {},
          isDeleted: false
        });
      }
    } catch (err) {
      console.error("ÅžÉ™xsi mesaj yazÄ±lma xÉ™tasÄ±:", err);
    }
  });

  // -------------------------------------------------------------------------
  // MESAJ OXUNDU / Ã‡ATDIRILDI STATUSU (WhatsApp tipli tÉ™k/cÃ¼t xÉ™tt)
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
      console.error("Ã‡atdÄ±rÄ±lma statusu xÉ™tasÄ±:", err);
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
      console.error("Oxunma statusu xÉ™tasÄ±:", err);
    }
  });

  // -------------------------------------------------------------------------
  // MESAJA REAKSÄ°YA VERMÆ
  // -------------------------------------------------------------------------
  socket.on('addReaction', async (data) => {
    try {
      const { msgId, isPrivate, nick, emoji } = data;
      const path = isPrivate ? `private_messages/${msgId}` : `messages/${msgId}`;
      const snap = await get(ref(db, path));
      if (!snap.exists()) return;
      const msg = snap.val();
      const reactions = msg.reactions || {};

      // Eyni emojini yenidÉ™n basarsa, reaksiyanÄ± geri gÃ¶tÃ¼rÃ¼rÃ¼k (toggle)
      if (reactions[nick] === emoji) {
        delete reactions[nick];
      } else 
