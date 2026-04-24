"""
Single-file Telegram Bot — Railway.app compatible
Fixes:
  - /broadcast: image reply + caption + HTML links working
  - Contact Admin button: CONTACT_ADMIN env variable se
"""

import json
import logging
import random
import urllib.parse
from datetime import datetime, timedelta

import pg8000.dbapi
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError
from dotenv import load_dotenv
import os

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN            = os.environ["BOT_TOKEN"]
ADMIN_ID             = int(os.environ["ADMIN_ID"])
DATABASE_URL         = os.environ["DATABASE_URL"]
CHANNEL_ID           = int(os.environ["CHANNEL_ID"])
CONTACT_ADMIN        = os.environ.get("CONTACT_ADMIN", "https://t.me/youradmin")
VIDEOS_PER_SESSION   = 20
VIDEO_DELETE_SECONDS = 5 * 60
CYCLE_DAYS           = 7

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ── DB connection ─────────────────────────────────────────────────────────────
def get_conn():
    r = urllib.parse.urlparse(DATABASE_URL)
    return pg8000.dbapi.connect(
        host=r.hostname,
        port=r.port or 5432,
        database=r.path.lstrip("/"),
        user=r.username,
        password=r.password,
        ssl_context=True,
    )

def _to_dict(cur, row):
    cols = [desc[0] for desc in cur.description]
    return dict(zip(cols, row))


# ── DB init ───────────────────────────────────────────────────────────────────
def init_db():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY, username TEXT,
                first_name TEXT, joined_at TIMESTAMPTZ DEFAULT NOW(),
                last_fetch TIMESTAMPTZ
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
        """)
        cur.execute("INSERT INTO settings (key,value) VALUES ('caption','Aur videos ke liye admin se contact karein.') ON CONFLICT (key) DO NOTHING;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fetched_content (
                id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL,
                message_ids TEXT NOT NULL DEFAULT '[]',
                warning_msg_id BIGINT, fetched_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_jobs (
                id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL, delete_at TIMESTAMPTZ NOT NULL,
                deleted BOOLEAN DEFAULT FALSE
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS channel_videos (
                message_id BIGINT PRIMARY KEY, media_type TEXT,
                indexed_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()
        logger.info("✅ DB tables ready.")
    finally:
        conn.close()


# ── DB helpers ────────────────────────────────────────────────────────────────
def upsert_user(user_id, username, first_name):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (user_id, username, first_name) VALUES (%s,%s,%s)
            ON CONFLICT (user_id) DO UPDATE
                SET username=EXCLUDED.username, first_name=EXCLUDED.first_name;
        """, (user_id, username, first_name))
        conn.commit()
    finally:
        conn.close()

def get_all_user_ids():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users;")
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

def update_last_fetch(user_id):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE users SET last_fetch=NOW() WHERE user_id=%s;", (user_id,))
        conn.commit()
    finally:
        conn.close()

def get_setting(key):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key=%s;", (key,))
        row = cur.fetchone()
        return row[0] if row else ""
    finally:
        conn.close()

def set_setting(key, value):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO settings (key,value) VALUES (%s,%s)
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value;
        """, (key, value))
        conn.commit()
    finally:
        conn.close()

def get_user_content(user_id):
    cutoff = datetime.utcnow() - timedelta(days=CYCLE_DAYS)
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, message_ids, warning_msg_id, fetched_at
            FROM fetched_content WHERE user_id=%s AND fetched_at>%s
            ORDER BY fetched_at DESC LIMIT 1;
        """, (user_id, cutoff))
        row = cur.fetchone()
        return _to_dict(cur, row) if row else None
    finally:
        conn.close()

def save_user_content(user_id, message_ids, warning_msg_id):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM fetched_content WHERE user_id=%s;", (user_id,))
        cur.execute("""
            INSERT INTO fetched_content (user_id, message_ids, warning_msg_id)
            VALUES (%s,%s,%s);
        """, (user_id, json.dumps(message_ids), warning_msg_id))
        conn.commit()
    finally:
        conn.close()

def reset_all_content():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM fetched_content;")
        conn.commit()
    finally:
        conn.close()

def save_broadcast_job(user_id, message_id):
    delete_at = datetime.utcnow() + timedelta(hours=24)
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO broadcast_jobs (user_id, message_id, delete_at)
            VALUES (%s,%s,%s);
        """, (user_id, message_id, delete_at))
        conn.commit()
    finally:
        conn.close()

def get_pending_broadcast_deletes():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, user_id, message_id FROM broadcast_jobs
            WHERE deleted=FALSE AND delete_at<=NOW();
        """)
        rows = cur.fetchall()
        return [_to_dict(cur, r) for r in rows]
    finally:
        conn.close()

def mark_broadcast_deleted(job_id):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE broadcast_jobs SET deleted=TRUE WHERE id=%s;", (job_id,))
        conn.commit()
    finally:
        conn.close()

def save_channel_video(message_id, media_type="video"):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO channel_videos (message_id, media_type)
            VALUES (%s,%s) ON CONFLICT (message_id) DO NOTHING;
        """, (message_id, media_type))
        conn.commit()
    finally:
        conn.close()

def get_latest_channel_video_ids(count):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT message_id FROM channel_videos
            ORDER BY message_id DESC LIMIT %s;
        """, (count,))
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

def clear_channel_videos():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM channel_videos;")
        conn.commit()
    finally:
        conn.close()


# ── Helpers ───────────────────────────────────────────────────────────────────
async def _safe_delete(bot, chat_id, msg_ids):
    for mid in msg_ids:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=mid)
        except TelegramError:
            pass


def _contact_url():
    """Build contact URL from CONTACT_ADMIN env var."""
    raw = CONTACT_ADMIN.strip()
    if raw.startswith("@"):
        return f"https://t.me/{raw[1:]}"
    if raw.lstrip("-").isdigit():
        # Numeric ID — open telegram user by id (tg://user?id=...)
        return f"tg://user?id={raw}"
    if raw.startswith("http"):
        return raw
    return f"https://t.me/{raw}"


# ── Auto-find latest message ID ───────────────────────────────────────────────
async def _find_latest_message_id(bot) -> int:
    probe = 1
    # Exponential probe upward
    while True:
        try:
            msg = await bot.forward_message(
                chat_id=ADMIN_ID, from_chat_id=CHANNEL_ID,
                message_id=probe, disable_notification=True,
            )
            await bot.delete_message(chat_id=ADMIN_ID, message_id=msg.message_id)
            probe *= 2
        except TelegramError:
            break

    upper = probe
    lower = probe // 2
    latest = lower

    for msg_id in range(upper, lower, -1):
        try:
            msg = await bot.forward_message(
                chat_id=ADMIN_ID, from_chat_id=CHANNEL_ID,
                message_id=msg_id, disable_notification=True,
            )
            await bot.delete_message(chat_id=ADMIN_ID, message_id=msg.message_id)
            latest = msg_id
            break
        except TelegramError:
            continue

    return latest


# ── Scheduled jobs ────────────────────────────────────────────────────────────
async def _delete_videos_job(context: ContextTypes.DEFAULT_TYPE):
    d = context.job.data
    await _safe_delete(context.bot, d["chat_id"], d["msg_ids"])
    logger.info(f"Auto-deleted {len(d['msg_ids'])} msgs for {d['chat_id']}")


async def _delete_broadcast_job(context: ContextTypes.DEFAULT_TYPE):
    jobs = get_pending_broadcast_deletes()
    for job in jobs:
        try:
            await context.bot.delete_message(
                chat_id=job["user_id"], message_id=job["message_id"]
            )
        except TelegramError:
            pass
        finally:
            mark_broadcast_deleted(job["id"])
    if jobs:
        logger.info(f"Cleaned {len(jobs)} broadcast msgs.")


# ═══════════════════════════════════════════════════════════════════════════════
# /start
# ═══════════════════════════════════════════════════════════════════════════════
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    chat_id = update.effective_chat.id
    bot     = context.bot

    upsert_user(user.id, user.username, user.first_name)

    # Delete old session
    prev_key = f"session_{user.id}"
    if prev_key in context.bot_data:
        await _safe_delete(bot, chat_id, context.bot_data[prev_key])
        del context.bot_data[prev_key]
    for job in context.job_queue.get_jobs_by_name(f"del_{user.id}"):
        job.schedule_removal()

    # Warning
    warn = await bot.send_message(
        chat_id=chat_id,
        text="⚠️ *Yeh videos 5 minute baad auto-delete ho jayenge.*\n\n📥 Download ya Forward disabled hai.",
        parse_mode="Markdown",
    )
    all_del = [warn.message_id]

    cached = get_user_content(user.id)
    if cached:
        ids = json.loads(cached["message_ids"]) if isinstance(cached["message_ids"], str) else cached["message_ids"]
        for ch_id in ids:
            try:
                sent = await bot.copy_message(
                    chat_id=chat_id, from_chat_id=CHANNEL_ID,
                    message_id=ch_id, protect_content=True, disable_notification=True,
                )
                all_del.append(sent.message_id)
            except TelegramError as e:
                logger.warning(f"Cached video {ch_id}: {e}")
    else:
        ch_ids = get_latest_channel_video_ids(VIDEOS_PER_SESSION)
        if not ch_ids:
            await bot.send_message(
                chat_id=chat_id,
                text="❌ Koi video nahi mili.\nAdmin pehle /index command chalaye.",
            )
            return
        for ch_id in ch_ids:
            try:
                sent = await bot.copy_message(
                    chat_id=chat_id, from_chat_id=CHANNEL_ID,
                    message_id=ch_id, protect_content=True, disable_notification=True,
                )
                all_del.append(sent.message_id)
            except TelegramError as e:
                logger.warning(f"Video {ch_id}: {e}")
        save_user_content(user.id, ch_ids, warn.message_id)
        update_last_fetch(user.id)

    context.bot_data[prev_key] = all_del

    # Final caption + Contact button — CONTACT_ADMIN variable se
    caption = get_setting("caption")
    contact_url = _contact_url()

    await bot.send_message(
        chat_id=chat_id,
        text=f"📌 *{caption}*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("📩 Contact Admin", url=contact_url)]]
        ),
    )

    context.job_queue.run_once(
        _delete_videos_job,
        when=VIDEO_DELETE_SECONDS,
        data={"chat_id": chat_id, "msg_ids": all_del},
        name=f"del_{user.id}",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# /index
# ═══════════════════════════════════════════════════════════════════════════════
async def index_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("🚫 Unauthorized.")

    if len(context.args) >= 2:
        try:
            start_id = int(context.args[0])
            end_id   = int(context.args[1])
        except ValueError:
            return await update.message.reply_text("❌ Example: `/index 1 300`", parse_mode="Markdown")
        status = await update.message.reply_text(
            f"🔍 Scanning `{start_id}` – `{end_id}`...", parse_mode="Markdown"
        )
    else:
        status = await update.message.reply_text("🔍 Channel ka latest message dhundh raha hoon...")
        latest   = await _find_latest_message_id(context.bot)
        end_id   = latest
        start_id = max(1, end_id - 499)
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=status.message_id,
            text=f"🔍 Latest ID: `{end_id}` — Scanning `{start_id}` to `{end_id}`...",
            parse_mode="Markdown",
        )

    clear_channel_videos()
    found = 0

    for msg_id in range(end_id, start_id - 1, -1):
        try:
            fwd = await context.bot.forward_message(
                chat_id=ADMIN_ID, from_chat_id=CHANNEL_ID,
                message_id=msg_id, disable_notification=True,
            )
            mtype = (
                "video"    if fwd.video    else
                "document" if fwd.document else
                "photo"    if fwd.photo    else None
            )
            if mtype:
                save_channel_video(msg_id, mtype)
                found += 1
            try:
                await context.bot.delete_message(chat_id=ADMIN_ID, message_id=fwd.message_id)
            except TelegramError:
                pass
        except TelegramError:
            continue

    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=status.message_id,
        text=(
            f"✅ *Index complete!*\n"
            f"📹 Found: *{found}* videos\n"
            f"📊 Range: `{start_id}` – `{end_id}`\n\n"
            f"Ab /start karne par latest {VIDEOS_PER_SESSION} videos milenge."
        ),
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# /reset  /setcaption  /setcontact
# ═══════════════════════════════════════════════════════════════════════════════
async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("🚫 Unauthorized.")
    reset_all_content()
    await update.message.reply_text("✅ *Reset! Sabko fresh videos milenge.*", parse_mode="Markdown")


async def setcaption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("🚫 Unauthorized.")
    # Raw text lena zaroori hai — context.args HTML tags tod deta hai
    raw = update.message.text or ""
    # "/setcaption " ke baad ka poora text lo
    if " " not in raw.strip():
        return await update.message.reply_text(
            "Usage: /setcaption Aapka caption yahan likhein\n\n"
            "Example:\n/setcaption 🔥 Daily videos ke liye join karo!",
        )
    text = raw.split(" ", 1)[1].strip()
    if not text:
        return await update.message.reply_text("❌ Caption empty hai. Kuch text do.")
    set_setting("caption", text)
    await update.message.reply_text(
        f"✅ Caption update ho gaya!\n\n📌 New caption:\n{text}"
    )


async def setcontact_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("🚫 Unauthorized.")
    await update.message.reply_text(
        "ℹ️ Contact button ke liye Railway mein `CONTACT_ADMIN` variable update karo.\n\n"
        "Example values:\n"
        "• `@yourusername`\n"
        "• `https://t.me/yourusername`\n"
        "• `123456789` (Telegram user ID)",
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# /broadcast  — FIXED: image reply + caption + HTML links
#
# Usage:
#   Text only:   /broadcast Hello <a href="https://t.me/ch">Join</a>
#   Image+text:  Kisi image ko reply karo → /broadcast Caption <a href="URL">Link</a>
# ═══════════════════════════════════════════════════════════════════════════════
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("🚫 Unauthorized.")

    msg   = update.message
    reply = msg.reply_to_message

    # Caption = sab kuch /broadcast ke baad
    # Support karta hai: plain text, HTML tags, <a href> links
    raw_text = msg.text or msg.caption or ""
    # Remove the /broadcast command prefix
    if raw_text.lower().startswith("/broadcast"):
        caption = raw_text[len("/broadcast"):].strip()
    else:
        caption = raw_text.strip()

    # Agar reply mein caption ho aur command mein nahi
    if not caption and reply and reply.caption:
        caption = reply.caption

    user_ids = get_all_user_ids()
    if not user_ids:
        return await msg.reply_text("⚠️ Koi registered user nahi mila.")

    if not reply and not caption:
        return await msg.reply_text(
            "❌ *Broadcast Usage:*\n\n"
            "📝 Text only:\n`/broadcast Aapka message`\n\n"
            "🔗 Text with link:\n`/broadcast Visit <a href='https://t.me/ch'>Channel</a>`\n\n"
            "🖼 Image + caption + link:\n"
            "Image ko reply karo phir likho:\n`/broadcast Caption <a href='URL'>Link Text</a>`",
            parse_mode="Markdown",
        )

    sending_msg = await msg.reply_text(
        f"📢 *Broadcasting to {len(user_ids)} users...*",
        parse_mode="Markdown",
    )

    ok = fail = 0

    for uid in user_ids:
        try:
            sent = None

            # ── Image broadcast ──────────────────────────────────────────────
            if reply and reply.photo:
                sent = await context.bot.send_photo(
                    chat_id=uid,
                    photo=reply.photo[-1].file_id,
                    caption=caption if caption else None,
                    parse_mode="HTML",
                )
            # ── Video broadcast ──────────────────────────────────────────────
            elif reply and reply.video:
                sent = await context.bot.send_video(
                    chat_id=uid,
                    video=reply.video.file_id,
                    caption=caption if caption else None,
                    parse_mode="HTML",
                )
            # ── Document broadcast ───────────────────────────────────────────
            elif reply and reply.document:
                sent = await context.bot.send_document(
                    chat_id=uid,
                    document=reply.document.file_id,
                    caption=caption if caption else None,
                    parse_mode="HTML",
                )
            # ── GIF/Animation broadcast ──────────────────────────────────────
            elif reply and reply.animation:
                sent = await context.bot.send_animation(
                    chat_id=uid,
                    animation=reply.animation.file_id,
                    caption=caption if caption else None,
                    parse_mode="HTML",
                )
            # ── Text only broadcast ──────────────────────────────────────────
            else:
                if not caption:
                    await sending_msg.edit_text("❌ Kuch toh do broadcast ke liye.")
                    return
                sent = await context.bot.send_message(
                    chat_id=uid,
                    text=caption,
                    parse_mode="HTML",
                    disable_web_page_preview=False,
                )

            if sent:
                save_broadcast_job(uid, sent.message_id)
                ok += 1

        except TelegramError as e:
            logger.warning(f"Broadcast uid {uid}: {e}")
            fail += 1

    await sending_msg.edit_text(
        f"✅ *Broadcast Complete!*\n\n"
        f"📨 Sent: *{ok}*\n"
        f"❌ Failed: *{fail}*\n"
        f"⏳ 24 ghante baad auto-delete.",
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP & MAIN
# ═══════════════════════════════════════════════════════════════════════════════
async def _on_startup(app: Application):
    app.job_queue.run_repeating(
        _delete_broadcast_job, interval=300, first=10, name="broadcast_cleaner"
    )
    # Bot command menu set karo (Telegram menu mein sirf yahi commands dikhenge)
    await app.bot.set_my_commands([
        ("start", "Videos dekho"),
    ])
    # Admin commands menu (private)
    from telegram import BotCommandScopeChat
    try:
        await app.bot.set_my_commands(
            [
                ("start",      "Videos dekho"),
                ("index",      "Channel index karo"),
                ("reset",      "Cache reset karo"),
                ("setcaption", "Caption set karo"),
                ("broadcast",  "Sabko message bhejo"),
            ],
            scope=BotCommandScopeChat(chat_id=ADMIN_ID),
        )
    except Exception:
        pass
    logger.info("✅ Broadcast cleaner scheduled. Bot commands set.")


def main():
    init_db()
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(_on_startup)
        .build()
    )
    app.add_handler(CommandHandler("start",      start_command))
    app.add_handler(CommandHandler("index",      index_command))
    app.add_handler(CommandHandler("reset",      reset_command))
    app.add_handler(CommandHandler("setcaption", setcaption_command))
    app.add_handler(CommandHandler("setcontact", setcontact_command))
    app.add_handler(CommandHandler("broadcast",  broadcast_command))
    logger.info("🤖 Bot started.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
