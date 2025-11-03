import os
import asyncio
import aiohttp
import aiofiles
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ChatAction
import yt_dlp
from flask import Flask
from threading import Thread
import motor.motor_asyncio
from datetime import datetime, timedelta
from urllib.parse import urlparse

DB_USER = "lakicalinuur"
DB_PASSWORD = "DjReFoWZGbwjry8K"
DB_APPNAME = "SpeechBot"
MONGO_URI = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@cluster0.n4hdlxk.mongodb.net/?retryWrites=true&w=majority&appName={DB_APPNAME}"

COOKIES_TXT_PATH = "cookies.txt"
if not os.path.exists(COOKIES_TXT_PATH):
    print(f"ERROR: Faylka '{COOKIES_TXT_PATH}' lama helin. Fadlan hubi inuu jiro.")

API_ID = 29169428
API_HASH = "55742b16a85aac494c7944568b5507e5"
BOT_TOKEN = "8303813448:AAEy5txrGzcK8o_0AhX-40YudvdEa0hpgNY"

DOWNLOAD_PATH = "downloads"
os.makedirs(DOWNLOAD_PATH, exist_ok=True)

MAX_CONCURRENT_DOWNLOADS = 5
MAX_VIDEO_DURATION = 2400

YDL_OPTS_PIN = {
    "format": "bestvideo+bestaudio/best",
    "outtmpl": os.path.join(DOWNLOAD_PATH, "%(title)s.%(ext)s"),
    "noplaylist": True,
    "quiet": True,
    "cookiefile": COOKIES_TXT_PATH
}

YDL_OPTS_YOUTUBE = {
    "format": "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]",
    "outtmpl": os.path.join(DOWNLOAD_PATH, "%(title)s.%(ext)s"),
    "noplaylist": True,
    "quiet": True,
    "cookiefile": COOKIES_TXT_PATH
}

YDL_OPTS_DEFAULT = {
    "format": "best",
    "outtmpl": os.path.join(DOWNLOAD_PATH, "%(title)s.%(ext)s"),
    "noplaylist": True,
    "quiet": True,
    "cookiefile": COOKIES_TXT_PATH
}

SUPPORTED_DOMAINS = [
    "youtube.com", "youtu.be", "facebook.com", "fb.watch", "pin.it",
    "x.com", "tiktok.com", "snapchat.com", "instagram.com"
]

mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db = mongo_client.get_default_database() or mongo_client[DB_APPNAME]
users_col = db["users"]
stats_col = db["stats"]

app = Client("video_downloader_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
flask_app = Flask(__name__)

active_downloads = 0
queue = asyncio.Queue()
lock = asyncio.Lock()

async def ensure_stats_doc():
    await stats_col.update_one(
        {"_id": "global"},
        {"$setOnInsert": {"total_downloads": 0, "platforms": {}}},
        upsert=True
    )

def get_platform_from_url(url: str):
    try:
        parsed = urlparse(url.lower())
        host = parsed.netloc
    except:
        host = url.lower()
    if "tiktok.com" in host:
        return "tiktok"
    if "youtube.com" in host or "youtu.be" in host:
        return "youtube"
    if "facebook.com" in host or "fb.watch" in host:
        return "facebook"
    if "pin.it" in host or "pinterest.com" in host:
        return "pinterest"
    if "x.com" in host or "twitter.com" in host:
        return "x"
    if "instagram.com" in host:
        return "instagram"
    if "snapchat.com" in host:
        return "snapchat"
    return "other"

async def record_user_activity(user):
    if not user:
        return
    now = datetime.utcnow()
    await users_col.update_one(
        {"_id": user.id},
        {"$set": {
            "username": getattr(user, "username", None),
            "first_name": getattr(user, "first_name", None),
            "last_name": getattr(user, "last_name", None),
            "last_active": now
        },
         "$setOnInsert": {"joined_at": now}},
        upsert=True
    )

async def increment_download_stats(platform):
    await stats_col.update_one(
        {"_id": "global"},
        {"$inc": {"total_downloads": 1, f"platforms.{platform}": 1}},
        upsert=True
    )

async def download_thumbnail(url, target_path):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    f = await aiofiles.open(target_path, mode='wb')
                    await f.write(await resp.read())
                    await f.close()
                    if os.path.exists(target_path):
                        return target_path
    except:
        pass
    return None

def extract_metadata_from_info(info):
    width = info.get("width")
    height = info.get("height")
    duration = info.get("duration")
    if not width or not height:
        formats = info.get("formats") or []
        best = None
        for f in formats:
            if f.get("width") and f.get("height"):
                best = f
                break
        if best:
            if not width:
                width = best.get("width")
            if not height:
                height = best.get("height")
            if not duration:
                dms = best.get("duration_ms")
                duration = info.get("duration") or (dms / 1000 if dms else None)
    return width, height, duration

async def download_video(url: str):
    loop = asyncio.get_running_loop()
    try:
        lowered = url.lower()
        is_pin = "pin.it" in lowered
        is_youtube = "youtube.com" in lowered or "youtu.be" in lowered
        if is_pin:
            ydl_opts = YDL_OPTS_PIN.copy()
        elif is_youtube:
            ydl_opts = YDL_OPTS_YOUTUBE.copy()
        else:
            ydl_opts = YDL_OPTS_DEFAULT.copy()

        def extract_info_sync():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=False)
        info = await loop.run_in_executor(None, extract_info_sync)
        width, height, duration = extract_metadata_from_info(info)
        if duration and duration > MAX_VIDEO_DURATION:
            return None

        def download_sync():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dl = ydl.extract_info(url, download=True)
                filename = ydl.prepare_filename(info_dl)
                return info_dl, filename
        info, filename = await loop.run_in_executor(None, download_sync)

        title = info.get("title") or ""
        desc = info.get("description") or ""
        is_youtube_flag = "youtube.com" in url.lower() or "youtu.be" in url.lower()
        if is_youtube_flag:
            caption = title or "@SooDajiye_Bot"
            if len(caption) > 1024:
                caption = caption[:1024]
        else:
            caption = desc.strip() or "@SooDajiye_Bot"
            if len(caption) > 1024:
                caption = caption[:1021] + "..."

        thumb = None
        thumb_url = info.get("thumbnail")
        if thumb_url:
            thumb_path = os.path.splitext(filename)[0] + ".jpg"
            thumb = await download_thumbnail(thumb_url, thumb_path)
        return caption, filename, width, height, duration, thumb
    except Exception as e:
        print(f"[ERROR] Failed to download {url}: {e}")
        return "ERROR"

async def download_audio_only(url: str):
    loop = asyncio.get_running_loop()
    lowered_url = url.lower()
    is_supported = any(domain in lowered_url for domain in ["youtube.com", "youtu.be", "facebook.com", "fb.watch"])
    if not is_supported:
        return None
    try:
        ydl_opts_info = {
            "skip_download": True,
            "quiet": True,
            "cookiefile": COOKIES_TXT_PATH
        }
        def extract_info_sync():
            with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
                return ydl.extract_info(url, download=False)
        info = await loop.run_in_executor(None, extract_info_sync)
        duration = info.get("duration")
        if not duration or duration <= 120:
            return None
        ydl_opts_audio = {
            "format": "bestaudio[ext=m4a]/bestaudio/best",
            "outtmpl": os.path.join(DOWNLOAD_PATH, "%(title)s.m4a"),
            "noplaylist": True,
            "quiet": True,
            "cookiefile": COOKIES_TXT_PATH
        }
        def download_sync():
            with yt_dlp.YoutubeDL(ydl_opts_audio) as ydl:
                info_dl = ydl.extract_info(url, download=True)
                filename = ydl.prepare_filename(info_dl)
                return info_dl, filename
        info_dl, filename = await loop.run_in_executor(None, download_sync)
        title = info_dl.get("title") or "Audio"
        caption = f"🎧 Muuqaalka Codkiisa oo kaliya\n\n{title}"
        return caption, filename
    except Exception as e:
        print(f"[ERROR] Audio download failed for {url}: {e}")
        return None

async def process_download(client, message, url):
    global active_downloads
    async with lock:
        active_downloads += 1
    try:
        await record_user_activity(message.from_user)
        await client.send_chat_action(message.chat.id, ChatAction.TYPING)
        result = await download_video(url)
        if result is None:
            await message.reply("Masoo dajin kari video ka dheer 40 minute 👍")
        elif result == "ERROR":
            await message.reply("Qalad ayaa dhacay, fadlan isku day mar kale 😓")
        else:
            caption, file_path, width, height, duration, thumb = result
            await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_VIDEO)
            kwargs = {"video": file_path, "caption": caption, "supports_streaming": True}
            if width:
                kwargs["width"] = int(width)
            if height:
                kwargs["height"] = int(height)
            if duration:
                kwargs["duration"] = int(float(duration))
            if thumb and os.path.exists(thumb):
                kwargs["thumb"] = thumb
            await client.send_video(message.chat.id, **kwargs)
            platform = get_platform_from_url(url)
            await increment_download_stats(platform)
            audio_result = await download_audio_only(url)
            if audio_result:
                audio_caption, audio_path = audio_result
                try:
                    await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_AUDIO)
                except:
                    try:
                        await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_DOCUMENT)
                    except:
                        pass
                try:
                    await client.send_audio(
                        message.chat.id,
                        audio=audio_path,
                        caption=audio_caption,
                        title=os.path.splitext(os.path.basename(audio_path))[0],
                        performer="Powered by SooDajiye Bot.m4a"
                    )
                except Exception as e:
                    print(f"[ERROR] Sending audio failed: {e}")
                if audio_path and os.path.exists(audio_path):
                    try:
                        os.remove(audio_path)
                    except:
                        pass
            for f in [file_path, thumb]:
                if f and os.path.exists(f):
                    try:
                        os.remove(f)
                    except:
                        pass
    finally:
        async with lock:
            active_downloads -= 1
        await start_next_download()

async def start_next_download():
    async with lock:
        while not queue.empty() and active_downloads < MAX_CONCURRENT_DOWNLOADS:
            client, message, url = await queue.get()
            asyncio.create_task(process_download(client, message, url))

@app.on_message(filters.private & filters.command("start"))
async def start(client, message: Message):
    await record_user_activity(message.from_user)
    await message.reply(
        "👋 Salaam!\n"
        "Iisoodir link Video kasocdo baraha hoos kuxusan si aan kuugu soo dajiyo.\n\n"
        "Supported sites:\n"
        "• YouTube\n"
        "• Facebook\n"
        "• Pinterest\n"
        "• X (Twitter)\n"
        "• TikTok\n"
        "• Instagram"
    )

@app.on_message(filters.private & filters.text)
async def handle_link(client, message: Message):
    await record_user_activity(message.from_user)
    url = message.text.strip()
    if not any(domain in url.lower() for domain in SUPPORTED_DOMAINS):
        await message.reply("kaliya Soodir link video saxa 👍")
        return
    async with lock:
        if active_downloads < MAX_CONCURRENT_DOWNLOADS:
            asyncio.create_task(process_download(client, message, url))
        else:
            await queue.put((client, message, url))

@app.on_message(filters.private & filters.command("status"))
async def status_handler(client, message: Message):
    await record_user_activity(message.from_user)
    await ensure_stats_doc()
    now = datetime.utcnow()
    day_ago = now - timedelta(days=1)
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)
    total_users = await users_col.count_documents({})
    active_last_day = await users_col.count_documents({"last_active": {"$gte": day_ago}})
    active_last_week = await users_col.count_documents({"last_active": {"$gte": week_ago}})
    active_last_month = await users_col.count_documents({"last_active": {"$gte": month_ago}})
    stat = await stats_col.find_one({"_id": "global"})
    if not stat:
        total_downloads = 0
        platforms = {}
    else:
        total_downloads = int(stat.get("total_downloads", 0))
        platforms = stat.get("platforms", {}) or {}
    platform_list = sorted(platforms.items(), key=lambda x: x[1], reverse=True)
    most_platform = platform_list[0][0] if platform_list else "N/A"
    most_count = platform_list[0][1] if platform_list else 0
    msg = (
        f"Bot Statistics:\n\n"
        f"Total users: {total_users}\n"
        f"Total active users last month: {active_last_month}\n"
        f"Total active users last week: {active_last_week}\n"
        f"Total active users last day: {active_last_day}\n"
        f"Total downloads: {total_downloads}\n"
        f"Platform with most downloads: {most_platform} ({most_count})\n\n"
        f"Breakdown per platform:\n"
    )
    for k, v in platform_list:
        msg += f"• {k}: {v}\n"
    await message.reply(msg)

@flask_app.route("/", methods=["GET", "POST", "HEAD"])
def keep_alive():
    return "Bot is alive ✅", 200

def run_flask():
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

def run_bot():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ensure_stats_doc())
    app.run()

Thread(target=run_flask).start()
run_bot()
