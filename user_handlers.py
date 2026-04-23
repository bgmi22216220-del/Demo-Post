"""
handlers/user_handlers.py
/start command flow:
  1. Delete old messages
  2. Send warning
  3. Send 20 videos (protect_content=True) — cached or fresh
  4. Send final caption + contact button
  5. Schedule auto-delete of videos + warning after 5 min
"""

import json
import logging
import random
from datetime import datetime

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
from telegram.error import TelegramError

from config import (
    CHANNEL_ID,
    VIDEOS_PER_SESSION,
    VIDEO_DELETE_SECONDS,
    ADMIN_ID,
)
from database import (
    upsert_user,
    update_last_fetch,
    get_user_content,
    save_user_content,
    get_setting,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helper — delete a list of message-ids silently
# ─────────────────────────────────────────────────────────────────────────────

async def _safe_delete(bot, chat_id: int, msg_ids: list):
    for mid in msg_ids:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=mid)
        except TelegramError:
            pass  # already deleted or never sent — ignore


# ─────────────────────────────────────────────────────────────────────────────
# Scheduled job: delete videos + warning after 5 min
# ─────────────────────────────────────────────────────────────────────────────

async def _delete_videos_job(context: ContextTypes.DEFAULT_TYPE):
    """Called by JobQueue after VIDEO_DELETE_SECONDS."""
    data      = context.job.data
    chat_id   = data["chat_id"]
    msg_ids   = data["msg_ids"]      # 20 video message-ids + warning id
    bot       = context.bot

    await _safe_delete(bot, chat_id, msg_ids)
    logger.info(f"Auto-deleted {len(msg_ids)} messages for chat {chat_id}")


# ─────────────────────────────────────────────────────────────────────────────
# Helper — fetch up to N video message-ids from the channel
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_channel_videos(bot, count: int) -> list[int]:
    """
    Forward-walks channel messages to collect video message IDs.
    Returns a shuffled sample of `count` IDs.

    Strategy:
      - Get the latest message from the channel (via getUpdates / forwardMessage trick)
      - Walk backwards from a high message_id to collect videos
    NOTE: Bots cannot call getChatHistory directly. We use copyMessage
          in dry-run style by reading the channel's message IDs via
          forwarding and catching the message_id. Instead, we rely on
          the admin having stored channel message IDs, OR we iterate
          a known range. 

    Practical approach used here:
      - Admin pre-populates CHANNEL_LAST_MSG_ID env var OR
      - We attempt IDs in reverse from a large range and collect hits.
    
    We use bot.forward_message with protect_content to a private chat
    but that requires knowing IDs. The cleanest Railway-friendly approach:
    store candidate IDs in DB via /index command. Here we implement
    a range-scan fallback that works for most channels.
    """
    from config import CHANNEL_ID
    import os

    # Try to get last known message id from env, default high range
    max_id = int(os.environ.get("CHANNEL_LAST_MSG_ID", 500))
    min_id = max(1, max_id - 1000)

    candidates = list(range(min_id, max_id + 1))
    random.shuffle(candidates)

    collected = []
    for msg_id in candidates:
        if len(collected) >= count * 3:   # collect 3x pool then sample
            break
        try:
            # copy_message to a dummy that we immediately delete is expensive.
            # Instead we use get_chat to probe the channel and forward to ADMIN
            # silently. The cleaner pattern: use bot.copy_message to self
            # with disable_notification=True, then track IDs.
            msg = await bot.forward_message(
                chat_id=ADMIN_ID,        # temp forward to admin
                from_chat_id=CHANNEL_ID,
                message_id=msg_id,
                disable_notification=True,
            )
            # Check if the forwarded message contains video
            if msg.video or msg.document:
                collected.append(msg_id)
            # Delete the temp forward
            await bot.delete_message(chat_id=ADMIN_ID, message_id=msg.message_id)
        except TelegramError:
            continue

    if not collected:
        return []

    sample = random.sample(collected, min(count, len(collected)))
    return sample


# ─────────────────────────────────────────────────────────────────────────────
# /start  command handler
# ─────────────────────────────────────────────────────────────────────────────

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    chat_id = update.effective_chat.id
    bot     = context.bot

    # ── 1. Register user ────────────────────────────────────────────────────
    upsert_user(user.id, user.username, user.first_name)

    # ── 2. Delete previous session messages stored in bot_data ──────────────
    prev_key = f"session_{user.id}"
    if prev_key in context.bot_data:
        old_ids = context.bot_data[prev_key]
        await _safe_delete(bot, chat_id, old_ids)
        del context.bot_data[prev_key]

    # Also cancel any old scheduled delete job for this user
    current_jobs = context.job_queue.get_jobs_by_name(f"del_{user.id}")
    for job in current_jobs:
        job.schedule_removal()

    # ── 3. Send warning message ──────────────────────────────────────────────
    warning_msg = await bot.send_message(
        chat_id=chat_id,
        text=(
            "⚠️ *Yeh videos 5 minute baad auto-delete ho jayenge.*\n\n"
            "📥 Download ya Forward karna disabled hai."
        ),
        parse_mode="Markdown",
    )

    # ── 4. Check 7-day cache ─────────────────────────────────────────────────
    cached = get_user_content(user.id)
    all_msg_ids_to_delete = [warning_msg.message_id]

    if cached:
        # ── 4a. Resend cached videos ─────────────────────────────────────────
        stored_ids: list[int] = json.loads(cached["message_ids"]) \
            if isinstance(cached["message_ids"], str) \
            else cached["message_ids"]

        sent_video_ids = []
        for ch_msg_id in stored_ids:
            try:
                sent = await bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=CHANNEL_ID,
                    message_id=ch_msg_id,
                    protect_content=True,
                    disable_notification=True,
                )
                sent_video_ids.append(sent.message_id)
            except TelegramError as e:
                logger.warning(f"Could not send cached video {ch_msg_id}: {e}")

        all_msg_ids_to_delete.extend(sent_video_ids)
        logger.info(f"Sent {len(sent_video_ids)} cached videos to {user.id}")

    else:
        # ── 4b. Fetch fresh videos from channel ──────────────────────────────
        channel_video_ids = await _fetch_channel_videos(bot, VIDEOS_PER_SESSION)

        if not channel_video_ids:
            await bot.send_message(
                chat_id=chat_id,
                text="❌ Abhi channel mein koi video available nahi hai. Baad mein try karein.",
            )
            return

        sent_video_ids = []
        for ch_msg_id in channel_video_ids:
            try:
                sent = await bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=CHANNEL_ID,
                    message_id=ch_msg_id,
                    protect_content=True,
                    disable_notification=True,
                )
                sent_video_ids.append(sent.message_id)
            except TelegramError as e:
                logger.warning(f"Could not send video {ch_msg_id}: {e}")

        all_msg_ids_to_delete.extend(sent_video_ids)

        # Save to DB for 7-day cycle
        save_user_content(user.id, channel_video_ids, warning_msg.message_id)
        update_last_fetch(user.id)
        logger.info(f"Fetched & sent {len(sent_video_ids)} fresh videos to {user.id}")

    # ── 5. Save session IDs in bot_data for next /start cleanup ─────────────
    context.bot_data[prev_key] = all_msg_ids_to_delete

    # ── 6. Send final caption + Contact Admin button (permanent) ────────────
    caption = get_setting("caption")
    contact = get_setting("contact")

    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📩 Contact Admin", url=contact)]]
    )
    await bot.send_message(
        chat_id=chat_id,
        text=f"📌 *{caption}*",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )

    # ── 7. Schedule auto-delete of videos + warning after 5 min ─────────────
    context.job_queue.run_once(
        _delete_videos_job,
        when=VIDEO_DELETE_SECONDS,
        data={"chat_id": chat_id, "msg_ids": all_msg_ids_to_delete},
        name=f"del_{user.id}",
    )
    logger.info(
        f"Scheduled deletion of {len(all_msg_ids_to_delete)} msgs "
        f"in {VIDEO_DELETE_SECONDS}s for user {user.id}"
    )
