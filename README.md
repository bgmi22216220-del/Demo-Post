# 🤖 Telegram Video Bot

Private channel se videos fetch karke users ko bhejne wala bot.
Auto-delete, 7-day cycle, admin panel — sab included.

---

## 📁 Project Structure

```
telegram_bot/
├── bot.py            ← Poora bot (single file)
├── requirements.txt  ← Dependencies
├── Procfile          ← Railway worker config
├── railway.toml      ← Railway deploy config
└── README.md
```

---

## ⚙️ Railway Variables

Railway dashboard mein **Settings → Variables** mein yeh 5 variables daalo:

| Variable Name | Example Value | Kahan Se Milega |
|---|---|---|
| `BOT_TOKEN` | `7412345678:AAFxxx...` | @BotFather se |
| `ADMIN_ID` | `987654321` | @userinfobot se |
| `DATABASE_URL` | `postgresql://user:pass@ep-xxx.neon.tech/neondb?sslmode=require` | Neon Console se |
| `CHANNEL_ID` | `-1001234567890` | Channel message forward karo @userinfobot ko |
| `CONTACT_ADMIN` | `@yourusername` | Apna Telegram username |

---

## 🚀 Setup Guide (Step by Step)

### Step 1 — Bot Banao
1. Telegram mein [@BotFather](https://t.me/BotFather) ko `/newbot` bhejo
2. Naam aur username do
3. Jo **Token** mile use copy karo → `BOT_TOKEN`

### Step 2 — Apna Telegram ID Pata Karo
1. [@userinfobot](https://t.me/userinfobot) ko `/start` bhejo
2. Jo **ID** mile use copy karo → `ADMIN_ID`

### Step 3 — Channel Setup
1. Apna private channel kholo
2. Bot ko channel ka **Admin** banao (post karne ki permission chahiye)
3. Channel se koi bhi message **forward karo** [@userinfobot](https://t.me/userinfobot) ko
4. Jo **Forwarded from ID** mile (e.g. `-1001234567890`) → `CHANNEL_ID`

### Step 4 — Neon Database Banao
1. [neon.tech](https://neon.tech) par free account banao
2. **New Project** → **New Database** banao
3. Dashboard mein **Connection String** copy karo
4. End mein `?sslmode=require` lagao → `DATABASE_URL`

```
postgresql://neondb_owner:password@ep-xxx.us-east-2.aws.neon.tech/neondb?sslmode=require
```

### Step 5 — Railway Par Deploy Karo
1. Yeh saari files **GitHub repo** mein push karo
2. [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub**
3. Repo select karo
4. **Variables** tab mein upar wale 5 variables daalo
5. **Deploy** button dabao ✅

---

## 📋 Commands

### 👤 User Commands

| Command | Kya karta hai |
|---|---|
| `/start` | Latest 20 videos milenge (5 min mein auto-delete) |

### 👑 Admin Commands

| Command | Usage | Kya karta hai |
|---|---|---|
| `/index` | `/index` | Channel auto-scan karta hai, saare videos index karta hai |
| `/index` | `/index 1 300` | Manual range se scan karta hai |
| `/reset` | `/reset` | Sab users ka cache delete karta hai (fresh videos milenge) |
| `/setcaption` | `/setcaption Aapka text` | Final message ka caption change karta hai |
| `/broadcast` | `/broadcast Message` | Sabko text message bhejta hai |
| `/broadcast` | Image reply + `/broadcast Caption` | Sabko image + caption bhejta hai |

---

## 🔄 Pehli Baar Setup Flow

```
Bot deploy karo Railway par
        ↓
Admin se /index chalao  →  Channel scan hoga (2-3 min lagenge)
        ↓
/setcaption se caption set karo
        ↓
Koi bhi user /start kare  →  Latest 20 videos milenge ✅
```

---

## 📢 Broadcast Usage

### Text only (link ke saath):
```
/broadcast Hamare channel mein aao! <a href="https://t.me/yourchannel">👉 Join Karo</a>
```

### Image + Caption + Link:
```
1. Pehle apne chat mein ek image bhejo
2. Us image ko REPLY karo
3. Phir likho:
   /broadcast 🔥 Dekho yeh! <a href="https://t.me/ch">Channel Join Karo</a>
```

### Supported HTML Tags:
| Tag | Result |
|---|---|
| `<b>text</b>` | **Bold** |
| `<i>text</i>` | *Italic* |
| `<a href="URL">text</a>` | Clickable Link |
| `<code>text</code>` | Monospace |

---

## ⚡ Bot Kaise Kaam Karta Hai

```
User /start kare
    │
    ├─ Purane session messages delete
    ├─ Warning bhejo: "5 min mein delete ho jayenge"
    │
    ├─ DB check: 7-day cache hai?
    │     ├─ YES → Same 20 videos dobara bhejo
    │     └─ NO  → Channel se latest 20 fetch karo + DB mein save karo
    │
    ├─ Videos bhejo (protect_content=True — forward/download band)
    ├─ Final caption + Contact Admin button (hamesha rahega)
    └─ Timer set: 5 min baad videos + warning auto-delete
```

---

## 🗄️ Database Tables

| Table | Kaam |
|---|---|
| `users` | Registered users |
| `settings` | Caption store |
| `fetched_content` | Har user ke 7-day cached video IDs |
| `broadcast_jobs` | Broadcast 24h auto-delete tracker |
| `channel_videos` | /index se scanned video message IDs |

---

## 🔒 Security

- `protect_content=True` — videos forward ya download nahi ho sakte
- Admin commands sirf `ADMIN_ID` wala use kar sakta hai
- Sab credentials Railway environment variables mein — code mein kuch hardcoded nahi
- Neon PostgreSQL SSL encrypted connection

---

## ❓ Common Problems

**Bot respond nahi kar raha?**
→ Railway → Deployments → View Logs check karo

**Videos nahi aa rahe?**
→ Admin se `/index` chalwao pehle

**Database error?**
→ `DATABASE_URL` mein `?sslmode=require` zaroori hai

**Bot channel se forward nahi kar pa raha?**
→ Bot ko channel ka Admin banao aur "Post Messages" permission do

**`/index` bohot slow hai?**
→ `/index 50 150` — range chhota rakho jitne messages channel mein hain
