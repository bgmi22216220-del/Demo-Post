"""
utils/channel_indexer.py
Only init_channel_videos_table is needed here now.
Full index logic is in bot.py
"""
from database import get_conn
import logging
logger = logging.getLogger(__name__)

def init_channel_videos_table():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS channel_videos (
                message_id BIGINT PRIMARY KEY,
                media_type TEXT,
                indexed_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()
    finally:
        conn.close()
