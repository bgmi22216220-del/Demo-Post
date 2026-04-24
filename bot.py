"""
Single-file Telegram Bot — Railway.app compatible
- Auto-fetch: channel mein naya video aane par bot automatically index karta hai
- /index command removed
- /reset se naye videos aate hain, warna 7 din tak same videos
"""

import json
import logging
import urllib.parse
from datetime import datetime, timedelta

import pg8000.dbapi
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
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
                user_id    BIGINT PRIMARY KEY,
                username   TEXT,
                first_name TEXT,
                joined_at  TIMESTAMPTZ DEFAULT NOW(),
                last_fetch TIMESTAMPTZ
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
        """)
        cur.execute("""
            INSERT INTO settings (key, value)
            VALUES ('caption', 'Aur videos ke liye admin se contact karein.')
            ON CONFLICT (key) DO NOTHING;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fetched_content (
                id             SERIAL PRIMARY KEY,
                user_id        BIGINT NOT NULL,
                message_ids    TEXT   NOT NULL DEFAULT '[]',
                warning_msg_id BIGINT,
                fetched_at     TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_jobs (
                id         SERIAL PRIMARY KEY,
                user_id    BIGINT  NOT NULL,
                message_id BIGINT  NOT NULL,
                delete_at  TIMESTAMPTZ NOT NULL,
                deleted    BOOLEAN DEFAULT FALSE
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS channel_videos (
                message_id BIGINT PRIMARY KEY,
                media_type TEXT,
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
                SET username=EXCLUDED.username,
                    first_name=EXCLUDED.first_name;
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
            INSERT INTO settings (key, value) VALUES (%s,%s)
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
            FROM   fetched_content
            WHERE  user_id=%s AND fetched_at>%s
            ORDER  BY fetched_at DESC LIMIT 1;
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
            WHERE  deleted=FALSE AND delete_at<=NOW();
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

# ── Channel videos DB ─────────────────────────────────────────────────────────
def save_channel_video(message_id, media_type="video"):
    """Ek naya channel video ID save karo."""
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
    """Latest N videos lo (highest message_id = newest post)."""
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

def get_channel_video_count():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM channel_videos;")
        return cur.fetchone()[0]
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
    raw = CONTACT_ADMIN.strip()
    if raw.startswith("@"):
        return f"https://t.me/{raw[1:]}"
    if raw.startswith("http"):
        return raw
    return f"https://t.me/{raw}"


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
# AUTO-FETCH: Channel mein naya video aane par automatically index ho
# Bot ko channel ka admin banao — yeh handler channel ke har naye post ko
# sun-ta hai aur DB mein save karta hai
# ═══════════════════════════════════════════════════════════════════════════════
async def channel_post_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Jab bhi channel mein koi naya post aaye — video, document, photo —
    us message_id ko channel_videos table mein save karo.
    """
    post = update.channel_post
    if not post:
        return

    # Sirf apne channel ke posts handle karo
    if post.chat.id != CHANNEL_ID:
        return

    media_type = None
    if post.video:
        media_type = "video"
    elif post.document:
        media_type = "document"
    elif post.photo:
        media_type = "photo"

    if media_type:
        save_channel_video(post.message_id, media_type)
        total = get_channel_video_count()
        logger.info(
            f"✅ Auto-indexed {media_type} | msg_id={post.message_id} | total={total}"
        )


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
        text=(
            "⚠️ *Yeh videos 5 minute baad auto-delete ho jayenge.*\n\n"
            "📥 Download ya Forward disabled hai."
        ),
        parse_mode="Markdown",
    )
    all_del = [warn.message_id]

    # 7-day cache check
    cached = get_user_content(user.id)

    if cached:
        # Same videos 7 din tak
        ids = json.loads(cached["message_ids"]) \
            if isinstance(cached["message_ids"], str) else cached["message_ids"]
        for ch_id in ids:
            try:
                sent = await bot.copy_message(
                    chat_id=chat_id, from_chat_id=CHANNEL_ID,
                    message_id=ch_id, protect_content=True,
                    disable_notification=True,
                )
                all_del.append(sent.message_id)
            except TelegramError as e:
                logger.warning(f"Cached video {ch_id}: {e}")
    else:
        # Fresh fetch — latest videos
        ch_ids = get_latest_channel_video_ids(VIDEOS_PER_SESSION)
        if not ch_ids:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    "❌ Abhi koi video available nahi hai.\n"
                    "Thodi der baad /start karein."
                ),
            )
            return

        for ch_id in ch_ids:
            try:
                sent = await bot.copy_message(
                    chat_id=chat_id, from_chat_id=CHANNEL_ID,
                    message_id=ch_id, protect_content=True,
                    disable_notification=True,
                )
                all_del.append(sent.message_id)
            except TelegramError as e:
                logger.warning(f"Video {ch_id}: {e}")

        save_user_content(user.id, ch_ids, warn.message_id)
        update_last_fetch(user.id)

    context.bot_data[prev_key] = all_del

    # Final caption + Contact button (permanent — delete nahi hoga)
    caption     = get_setting("caption")
    contact_url = _contact_url()

    await bot.send_message(
        chat_id=chat_id,
        text=f"📌 *{caption}*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("📩 Contact Admin", url=contact_url)]]
        ),
    )

    # 5 min auto-delete schedule
    context.job_queue.run_once(
        _delete_videos_job,
        when=VIDEO_DELETE_SECONDS,
        data={"chat_id": chat_id, "msg_ids": all_del},
        name=f"del_{user.id}",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# /reset
# ═══════════════════════════════════════════════════════════════════════════════
async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("🚫 Unauthorized.")
    reset_all_content()
    total = get_channel_video_count()
    await update.message.reply_text(
        f"✅ *Reset ho gaya!*\n\n"
        f"📹 Channel mein indexed videos: *{total}*\n\n"
        f"Ab sabko next /start par latest videos milenge.",
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# /setcaption
# ═══════════════════════════════════════════════════════════════════════════════
async def setcaption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("🚫 Unauthorized.")
    raw = update.message.text or ""
    if " " not in raw.strip():
        return await update.message.reply_text(
            "Usage: /setcaption Aapka caption yahan likhein\n\n"
            "Example:\n/setcaption 🔥 Daily videos ke liye join karo!"
        )
    text = raw.split(" ", 1)[1].strip()
    if not text:
        return await update.message.reply_text("❌ Caption empty hai.")
    set_setting("caption", text)
    await update.message.reply_text(
        f"✅ *Caption update ho gaya!*\n\n📌 {text}",
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# /broadcast
# ═══════════════════════════════════════════════════════════════════════════════
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return await update.message.reply_text("🚫 Unauthorized.")

    msg   = update.message
    reply = msg.reply_to_message

    # Caption = /broadcast ke baad ka poora text (HTML support)
    raw_text = msg.text or msg.caption or ""
    caption  = raw_text.split(" ", 1)[1].strip() if " " in raw_text else ""
    if not caption and reply and reply.caption:
        caption = reply.caption

    user_ids = get_all_user_ids()
    if not user_ids:
        return await msg.reply_text("⚠️ Koi registered user nahi mila.")

    if not reply and not caption:
        return await msg.reply_text(
            "❌ *Broadcast Usage:*\n\n"
            "📝 Text + link:\n"
            "<code>/broadcast Hello! &lt;a href='https://t.me/ch'&gt;Join&lt;/a&gt;</code>\n\n"
            "🖼 Image + caption + link:\n"
            "Image ko reply karo phir:\n"
            "<code>/broadcast Caption &lt;a href='URL'&gt;Link&lt;/a&gt;</code>",
            parse_mode="HTML",
        )

    sending_msg = await msg.reply_text(
        f"📢 *Broadcasting to {len(user_ids)} users...*",
        parse_mode="Markdown",
    )

    ok = fail = 0
    for uid in user_ids:
        try:
            sent = None
            if reply and reply.photo:
                sent = await context.bot.send_photo(
                    chat_id=uid, photo=reply.photo[-1].file_id,
                    caption=caption or None, parse_mode="HTML",
                )
            elif reply and reply.video:
                sent = await context.bot.send_video(
                    chat_id=uid, video=reply.video.file_id,
                    caption=caption or None, parse_mode="HTML",
                )
            elif reply and reply.document:
                sent = await context.bot.send_document(
                    chat_id=uid, document=reply.document.file_id,
                    caption=caption or None, parse_mode="HTML",
                )
            elif reply and reply.animation:
                sent = await context.bot.send_animation(
                    chat_id=uid, animation=reply.animation.file_id,
                    caption=caption or None, parse_mode="HTML",
                )
            else:
                if not caption:
                    await sending_msg.edit_text("❌ Kuch toh do broadcast ke liye.")
                    return
                sent = await context.bot.send_message(
                    chat_id=uid, text=caption,
                    parse_mode="HTML", disable_web_page_preview=False,
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
    # Broadcast cleaner
    app.job_queue.run_repeating(
        _delete_broadcast_job, interval=300, first=10, name="broadcast_cleaner"
    )
    # Bot menu set karo
    from telegram import BotCommandScopeChat, BotCommand
    await app.bot.set_my_commands([
        BotCommand("start", "Videos dekho"),
    ])
    try:
        await app.bot.set_my_commands(
            [
                BotCommand("start",      "Videos dekho"),
                BotCommand("reset",      "Naye videos fetch karo"),
                BotCommand("setcaption", "Caption set karo"),
                BotCommand("broadcast",  "Sabko message bhejo"),
            ],
            scope=BotCommandScopeChat(chat_id=ADMIN_ID),
        )
    except Exception:
        pass
    logger.info("✅ Bot ready. Auto-fetch active.")


def main():
    init_db()
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(_on_startup)
        .build()
    )

    # User commands
    app.add_handler(CommandHandler("start",      start_command))

    # Admin commands
    app.add_handler(CommandHandler("reset",      reset_command))
    app.add_handler(CommandHandler("setcaption", setcaption_command))
    app.add_handler(CommandHandler("broadcast",  broadcast_command))

    # ✅ Auto-fetch: channel ke naye posts sun-o
    app.add_handler(MessageHandler(
        filters.ChatType.CHANNEL & filters.Chat(CHANNEL_ID),
        channel_post_handler,
    ))

    logger.info("🤖 Bot started — listening for channel posts...")
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "channel_post"],  # channel posts bhi sun-o
    )


if __name__ == "__main__":
    main()
