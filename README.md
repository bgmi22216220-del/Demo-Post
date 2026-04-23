# 🤖 Advanced Telegram Video Bot

Private channel se videos fetch karke users ko bhejne wala bot.
Auto-delete, 7-day cycle, admin panel — sab kuch included.

---

## 📁 Project Structure

```
telegram_bot/
├── bot.py                      # Main entry point
├── config.py                   # Environment variable loader
├── database.py                 # PostgreSQL (Neon) — all DB operations
├── requirements.txt
├── Procfile                    # Railway.app worker
├── railway.toml                # Railway deployment config
├── .env.example                # Environment variable template
├── handlers/
│   ├── __init__.py
│   ├── user_handlers.py        # /start command
│   └── admin_handlers.py       # /reset /setcaption /setcontact /broadcast
└── utils/
    ├── __init__.py
    └── channel_indexer.py      # /index command — channel video scanner
```

---

## ⚙️ Setup Guide

### Step 1 — Bot Token
1. [@BotFather](https://t.me/BotFather) se `/newbot` karein
2. Token copy karein

### Step 2 — Channel Setup
1. Bot ko apne private channel ka **Admin** banao
2. Channel ID pane ke liye koi bhi channel message forward karo [@userinfobot](https://t.me/userinfobot) ko
3. ID `-100xxxxxxxxxx` format mein hogi

### Step 3 — Neon PostgreSQL
1. [neon.tech](https://neon.tech) par free account banao
2. New Project → New Database
3. Connection string copy karo (`postgresql://...`)

### Step 4 — Environment Variables

`.env.example` ko `.env` mein copy karo aur fill karo:

```bash
cp .env.example .env
```

```env
BOT_TOKEN=your_bot_token
ADMIN_ID=your_telegram_id
DATABASE_URL=postgresql://user:pass@host/db?sslmode=require
CHANNEL_ID=-1001234567890
CHANNEL_LAST_MSG_ID=500
```

### Step 5 — Install & Run

```bash
pip install -r requirements.txt
python bot.py
```

---

## 🚀 Railway.app Deployment

1. GitHub par code push karo
2. [railway.app](https://railway.app) → New Project → Deploy from GitHub
3. **Variables** section mein ye sab add karo:
   - `BOT_TOKEN`
   - `ADMIN_ID`
   - `DATABASE_URL`
   - `CHANNEL_ID`
   - `CHANNEL_LAST_MSG_ID`
4. Deploy button dabao ✅

---

## 📋 Commands Reference

### 👤 User Commands
| Command | Description |
|---------|-------------|
| `/start` | 20 random videos fetch karo (5 min auto-delete) |

### 👑 Admin Commands
| Command | Usage | Description |
|---------|-------|-------------|
| `/index` | `/index 1 500` | Channel scan karke video IDs store karo |
| `/reset` | `/reset` | Sabke video cache delete karo (fresh fetch hoga) |
| `/setcaption` | `/setcaption Naye videos ke liye...` | Final message ka caption set karo |
| `/setcontact` | `/setcontact @yourusername` | Contact button ka link set karo |
| `/broadcast` | Image reply karo + `/broadcast Caption` | Sabko broadcast karo |

---

## 🔄 First-Time Setup Flow

```
1. Bot deploy karo Railway par
2. Admin se /index 1 500  →  channel scan hoga
3. Koi bhi user /start kare  →  videos milenge
4. /setcaption aur /setcontact set karo
```

---

## 🗄️ Database Tables

| Table | Purpose |
|-------|---------|
| `users` | Registered users |
| `settings` | caption / contact key-value |
| `fetched_content` | User ke 7-day cached video IDs |
| `broadcast_jobs` | Broadcast auto-delete tracker |
| `channel_videos` | Indexed channel video message IDs |

---

## ⚡ Feature Flow

```
User /start
    │
    ├─ Delete old session messages
    ├─ Send warning: "5 min mein delete ho jayenge"
    │
    ├─ DB check: 7-day cache hai?
    │       ├─ YES → Same 20 videos resend
    │       └─ NO  → Channel se fresh 20 fetch + DB save
    │
    ├─ Videos send (protect_content=True)
    ├─ Final caption + Contact button (permanent)
    └─ JobQueue: 5 min baad videos + warning delete
```

---

## 🔒 Security Notes

- `protect_content=True` — forward/download disabled on videos
- Admin commands sirf `ADMIN_ID` wala user use kar sakta hai
- Sab credentials environment variables mein — code mein kuch hardcoded nahi
- Neon PostgreSQL SSL required connection

---

## ❓ Troubleshooting

**Videos nahi aa rahe?**
→ `/index 1 500` run karo pehle (range apni channel ke hisaab se adjust karo)

**Bot respond nahi kar raha?**
→ Railway logs check karo → Settings → Deployments → View Logs

**Database error?**
→ `DATABASE_URL` mein `?sslmode=require` zaroori hai Neon ke liye
