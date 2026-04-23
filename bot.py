"""
Single-file Telegram Bot — Railway.app compatible
NO external folder imports. Everything in one file.
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
BOT_TOKEN              = os.environ["BOT_TOKEN"]
ADMIN_ID               = int(os.environ["ADMIN_ID"])
DATABASE_URL           = os.environ["DATABASE_URL"]
CHANNEL_ID             = int(os.environ["CHANNEL_ID"])
VIDEOS_PER_SESSION     = 20
VIDEO_DELETE_SECONDS   = 5 * 60        # 5 minutes
CYCLE_DAYS             = 7

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
        cur.execute("INSERT INTO settings (key,value) VALUES ('contact','https://t.me/youradmin') ON CONFLICT (key) DO NOTHING;")
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
            ON CONFLICT (user_id) DO UPDATE SET username=EXCLUDED.username, first_name=EXCLUDED.first_name;
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
        cur.execute("INSERT INTO settings (key,value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value;", (key, value))
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
        cur.execute("INSERT INTO fetched_content (user_id,message_ids,warning_msg_id) VALUES (%s,%s,%s);",
                    (user_id, json.dumps(message_ids), warning_msg_id))
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
        cur.execute("INSERT INTO broadcast_jobs (user_id,message_id,delete_at) VALUES (%s,%s,%s);",
                    (user_id, message_id, delete_at))
        conn.commit()
    finally:
        conn.close()

def get_pending_broadcast_deletes():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id,user_id,message_id FROM broadcast_jobs WHERE deleted=FALSE AND delete_at<=NOW();")
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
        cur.execute("INSERT INTO channel_videos (message_id,media_type) VALUES (%s,%s) ON CONFLICT (message_id) DO NOTHING;",
                    (message_id, media_type))
        conn.commit()
    finally:
        conn.close()

def get_all_channel_video_ids():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT message_id FROM channel_videos ORDER BY RANDOM() LIMIT 100;")
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


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

async def _safe_delete(bot, chat_id, msg_ids):
    for mid in msg_ids:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=mid)
        except TelegramError:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULED JOBS
# ═══════════════════════════════════════════════════════════════════════════════

async def _delete_videos_job(context: ContextTypes.DEFAULT_TYPE):
    d = context.job.data
    await _safe_delete(context.bot, d["chat_id"], d["msg_ids"])
    logger.info(f"Auto-deleted {len(d['msg_ids'])} msgs for {d['chat_id']}")


async def _delete_broadcast_job(context: ContextTypes.DEFAULT_TYPE):
    jobs = get_pending_broadcast_deletes()
    for job in jobs:
        try:
            await context.bot.delete_message(chat_id=job["user_id"], message_id=job["message_id"])
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

    # Warning message
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
                sent = await bot.copy_message(chat_id=chat_id, from_chat_id=CHANNEL_ID,
                                               message_id=ch_id, protect_content=True, disable_notification=True)
                all_del.append(sent.message_id)
            except TelegramError as e:
                logger.warning(f"Cached video {ch_id}: {e}")
    else:
        ch_ids = get_all_channel_video_ids()
        if not ch_ids:
            await bot.send_message(chat_id=chat_id, text="❌ Koi video nahi mili. Admin se /index chalwao.")
            return
        sample = random.sample(ch_ids, min(VIDEOS_PER_SESSION, len(ch_ids)))
        for ch_id in sample:
            try:
                sent = await bot.copy_message(chat_id=chat_id, from_chat_id=CHANNEL_ID,
                                               message_id=ch_id, protect_content=True, disable_notification=True)
                all_del.append(sent.message_id)
            except TelegramError as e:
                logger.warning(f"Video {ch_id}: {e}")
        save_user_content(user.id, sample, warn.message_id)
        update_last_fetch(user.id)

    context.bot_data[prev_key] = all_del

    # Final caption + button (permanent)
    caption = get_setting("caption")
    contact = get_setting("contact")
    await bot.send_message(
        chat_id=chat_id,
        text=f"📌 *{caption}*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📩 Contact Admin", url=contact)]]),
    )

    context.job_queue.run_once(
        _delete_videos_job,
        when=VIDEO_DELETE_SECONDS,
        data={"chat_id": chat_id, "msg_ids": all_del},
        name=f"del_{user.id}",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════

async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("🚫 Unauthorized.")
    reset_all_content()
    await update.message.reply_text("✅ *Reset ho gaya!* Sabko fresh videos milenge.", parse_mode="Markdown")


async def setcaption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("🚫 Unauthorized.")
    if not context.args:
        return await update.message.reply_text("Usage: `/setcaption Your text`", parse_mode="Markdown")
    text = " ".join(context.args)
    set_setting("caption", text)
    await update.message.reply_text(f"✅ Caption set:\n_{text}_", parse_mode="Markdown")


async def setcontact_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("🚫 Unauthorized.")
    if not context.args:
        return await update.message.reply_text("Usage: `/setcontact @username`", parse_mode="Markdown")
    raw = context.args[0].strip()
    url = f"https://t.me/{raw[1:]}" if raw.startswith("@") else raw
    set_setting("contact", url)
    await update.message.reply_text(f"✅ Contact set: {url}")


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("🚫 Unauthorized.")
    msg      = update.message
    user_ids = get_all_user_ids()
    caption  = " ".join(context.args) if context.args else None
    if not user_ids:
        return await msg.reply_text("⚠️ Koi user nahi mila.")
    reply    = msg.reply_to_message
    ok = fail = 0
    await msg.reply_text(f"📢 Sending to *{len(user_ids)}* users...", parse_mode="Markdown")
    for uid in user_ids:
        try:
            if reply and reply.photo:
                sent = await context.bot.send_photo(chat_id=uid, photo=reply.photo[-1].file_id, caption=caption, parse_mode="HTML" if caption else None)
            elif reply and reply.video:
                sent = await context.bot.send_video(chat_id=uid, video=reply.video.file_id, caption=caption, parse_mode="HTML" if caption else None)
            elif reply and reply.document:
                sent = await context.bot.send_document(chat_id=uid, document=reply.document.file_id, caption=caption, parse_mode="HTML" if caption else None)
            else:
                text = caption or (msg.text.split(None,1)[1] if len(msg.text.split())>1 else "")
                if not text:
                    return await msg.reply_text("❌ Text ya image do.")
                sent = await context.bot.send_message(chat_id=uid, text=text, parse_mode="HTML")
            save_broadcast_job(uid, sent.message_id)
            ok += 1
        except TelegramError as e:
            logger.warning(f"Broadcast uid {uid}: {e}")
            fail += 1
    await msg.reply_text(f"✅ Done!\n📨 Sent: {ok}\n❌ Failed: {fail}\n⏳ 24h baad auto-delete.", parse_mode="Markdown")


async def index_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("🚫 Unauthorized.")
    if len(context.args) < 2:
        return await update.message.reply_text("Usage: `/index 1 300`", parse_mode="Markdown")
    try:
        start_id, end_id = int(context.args[0]), int(context.args[1])
    except ValueError:
        return await update.message.reply_text("❌ Valid numbers do.")
    clear_channel_videos()
    status = await update.message.reply_text(f"🔍 Scanning {start_id}–{end_id}...")
    found = 0
    for msg_id in range(start_id, end_id + 1):
        try:
            fwd = await context.bot.forward_message(
                chat_id=ADMIN_ID, from_chat_id=CHANNEL_ID,
                message_id=msg_id, disable_notification=True,
            )
            mtype = "video" if fwd.video else "document" if fwd.document else "photo" if fwd.photo else None
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
        text=f"✅ *Index complete!*\n📹 Found: *{found}*\n📊 Range: `{start_id}`–`{end_id}`",
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def _on_startup(app: Application):
    app.job_queue.run_repeating(_delete_broadcast_job, interval=300, first=10, name="broadcast_cleaner")
    logger.info("✅ Broadcast cleaner scheduled.")


def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).post_init(_on_startup).build()
    app.add_handler(CommandHandler("start",      start_command))
    app.add_handler(CommandHandler("reset",      reset_command))
    app.add_handler(CommandHandler("setcaption", setcaption_command))
    app.add_handler(CommandHandler("setcontact", setcontact_command))
    app.add_handler(CommandHandler("broadcast",  broadcast_command))
    app.add_handler(CommandHandler("index",      index_command))
    logger.info("🤖 Bot started.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
