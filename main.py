import os
import re
import asyncio
import logging
import aiohttp
import aiofiles
from threading import Thread
from flask import Flask, request, abort
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ChatAction, ChatMemberStatus
import yt_dlp
import telebot
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
from urllib.parse import urlparse

COOKIES_TXT_PATH = "cookies.txt"
if not os.path.exists(COOKIES_TXT_PATH):
    open(COOKIES_TXT_PATH, "a").close()

API_ID = int(os.environ.get("API_ID", "29169428"))
API_HASH = os.environ.get("API_HASH", "55742b16a85aac494c7944568b5507e5")
BOT1_TOKEN = os.environ.get("BOT1_TOKEN", "8362188818:AAFLE14YbizBu1v7on1tQAOuqsFsIw1_7HY")
BOT2_TOKEN = os.environ.get("BOT2_TOKEN", "7522782488:AAH1OLPWbaiDaPiKJSQJ6n1Z1FeTOdtub8o")
WEBHOOK_BASE = os.environ.get("WEBHOOK_BASE", "https://repository-gayga-ugu-horee-yay.onrender.com")
PORT = int(os.environ.get("PORT", 8080))

DB_USER = "lakicalinuur"
DB_PASSWORD = "DjReFoWZGbwjry8K"
DB_APPNAME = "SpeechBot"
MONGO_URI = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@cluster0.n4hdlxk.mongodb.net/?retryWrites=true&w=majority&appName={DB_APPNAME}"

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client.speechbot_db

USERS_COLLECTION = db.users
DOWNLOADS_COLLECTION = db.downloads

REQUIRED_CHANNEL = "@ok_fans"

DOWNLOAD_PATH = "downloads"
os.makedirs(DOWNLOAD_PATH, exist_ok=True)

MAX_CONCURRENT_DOWNLOADS = 5
MAX_VIDEO_DURATION = 2400
MAX_VIDEO_DURATION_YOUTUBE = 600

BOT_USERNAME = None

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

pyro_client = Client("video_downloader_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT1_TOKEN)
flask_app = Flask(__name__)
telebot_bot = telebot.TeleBot(BOT2_TOKEN)

active_downloads = 0
queue = asyncio.Queue()
lock = asyncio.Lock()

async def get_bot_username(client):
    global BOT_USERNAME
    if BOT_USERNAME is None:
        try:
            me = await client.get_me()
            BOT_USERNAME = me.username
        except Exception as e:
            logging.exception(e)
            return "Bot"
    return BOT_USERNAME

async def is_user_in_channel(client, user_id: int) -> bool:
    try:
        member = await client.get_chat_member(REQUIRED_CHANNEL, user_id)
        return member.status in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
            ChatMemberStatus.RESTRICTED
        )
    except Exception:
        return False

async def ensure_joined(client, obj) -> bool:
    if isinstance(obj, CallbackQuery):
        uid = obj.from_user.id
        reply_target = obj.message
    else:
        uid = obj.from_user.id
        reply_target = obj
    try:
        if await is_user_in_channel(client, uid):
            return True
    except Exception:
        pass
    clean_channel_username = REQUIRED_CHANNEL.replace("@", "")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Join Channel", url=f"https://t.me/{clean_channel_username}")]
    ])
    text = f"🚫 Please join our channel {REQUIRED_CHANNEL} to use this bot.\n\nAfter joining, send the link again."
    try:
        if isinstance(obj, CallbackQuery):
            try:
                await obj.answer("🚫 First join the channel", show_alert=True)
            except Exception:
                pass
        await reply_target.reply_text(text, reply_markup=kb)
    except Exception:
        try:
            await client.send_message(uid, text, reply_markup=kb)
        except Exception:
            pass
    return False

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

async def download_video(url: str, bot_username: str):
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
        limit = MAX_VIDEO_DURATION_YOUTUBE if is_youtube else MAX_VIDEO_DURATION
        if duration and duration > limit:
            return ("TOO_LONG", limit)
        def download_sync():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dl = ydl.extract_info(url, download=True)
                filename = ydl.prepare_filename(info_dl)
                return info_dl, filename
        info, filename = await loop.run_in_executor(None, download_sync)
        title = info.get("title") or ""
        desc = info.get("description") or ""
        bot_tag = f"@{bot_username}"
        is_youtube_flag = "youtube.com" in url.lower() or "youtu.be" in url.lower()
        if is_youtube_flag:
            caption = title or bot_tag
            if len(caption) > 1024:
                caption = caption[:1024]
        else:
            caption = desc.strip() or bot_tag
            if len(caption) > 1024:
                caption = caption[:1021] + "..."
        thumb = None
        thumb_url = info.get("thumbnail")
        if thumb_url:
            thumb_path = os.path.splitext(filename)[0] + ".jpg"
            thumb = await download_thumbnail(thumb_url, thumb_path)
        return caption, filename, width, height, duration, thumb
    except Exception as e:
        logging.exception(e)
        return "ERROR"

async def download_audio_only(url: str, bot_username: str):
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
        caption = f"🎧 Audio only\n\n{title}"
        return caption, filename
    except Exception as e:
        logging.exception(e)
        return None

async def register_user(user_id, username, first_name):
    await USERS_COLLECTION.update_one(
        {"_id": user_id},
        {"$set": {"username": username, "first_name": first_name, "last_active": datetime.utcnow()},
         "$setOnInsert": {"join_date": datetime.utcnow()}},
        upsert=True
    )

def get_domain(url):
    try:
        hostname = urlparse(url).netloc
        if hostname.startswith("www."):
            hostname = hostname[4:]
        return hostname.replace("youtu.be", "youtube.com").replace("fb.watch", "facebook.com").replace("pin.it", "pinterest.com").replace("x.com", "twitter.com")
    except:
        return "Unknown"

async def record_download(user_id, url):
    domain = get_domain(url)
    await DOWNLOADS_COLLECTION.insert_one({
        "user_id": user_id,
        "url": url,
        "domain": domain,
        "timestamp": datetime.utcnow()
    })

async def process_download(client, message, url):
    global active_downloads
    await register_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    bot_username = await get_bot_username(client)
    async with lock:
        active_downloads += 1
    try:
        await client.send_chat_action(message.chat.id, ChatAction.TYPING)
        result = await download_video(url, bot_username)
        if isinstance(result, tuple) and result[0] == "TOO_LONG":
            limit = result[1]
            minutes = int(limit / 60)
            await message.reply(f"Cannot download videos longer than {minutes} minutes 👍")
        elif result is None:
            await message.reply("Cannot download this video 👍")
        elif result == "ERROR":
            await message.reply("An error occurred, please try again 😓")
        else:
            caption, file_path, width, height, duration, thumb = result
            await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_VIDEO)
            kwargs = {"video": file_path, "caption": caption, "supports_streaming": True}
            if width: kwargs["width"] = int(width)
            if height: kwargs["height"] = int(height)
            if duration: kwargs["duration"] = int(float(duration))
            if thumb and os.path.exists(thumb): kwargs["thumb"] = thumb
            await client.send_video(message.chat.id, **kwargs)
            await record_download(message.from_user.id, url)
            audio_result = await download_audio_only(url, bot_username)
            if audio_result:
                audio_caption, audio_path = audio_result
                try:
                    await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_AUDIO)
                except:
                    pass
                try:
                    await client.send_audio(
                        message.chat.id,
                        audio=audio_path,
                        caption=audio_caption,
                        title=os.path.splitext(os.path.basename(audio_path))[0],
                        performer=f"Powered by @{bot_username}"
                    )
                except Exception:
                    logging.exception("Sending audio failed")
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

async def get_bot_statistics():
    now = datetime.utcnow()
    last_day = now - timedelta(days=1)
    last_week = now - timedelta(weeks=1)
    last_month = now - timedelta(days=30)
    total_users = await USERS_COLLECTION.count_documents({})
    total_downloads = await DOWNLOADS_COLLECTION.count_documents({})
    active_users_last_day = await USERS_COLLECTION.count_documents({"last_active": {"$gte": last_day}})
    active_users_last_week = await USERS_COLLECTION.count_documents({"last_active": {"$gte": last_week}})
    active_users_last_month = await USERS_COLLECTION.count_documents({"last_active": {"$gte": last_month}})
    pipeline = [
        {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    most_downloaded_sites_cursor = DOWNLOADS_COLLECTION.aggregate(pipeline)
    most_downloaded_sites = await most_downloaded_sites_cursor.to_list(length=5)
    most_downloads_text = ""
    for item in most_downloaded_sites:
        most_downloads_text += f"• {item['_id']}: {item['count']} times\n"
    if not most_downloads_text:
        most_downloads_text = "No downloads recorded yet."
    return (
        f"📊 **Bot Statistics** 📊\n\n"
        f"**USERS:**\n"
        f"• Total Users: **{total_users:,}**\n"
        f"• Active (Last Month): **{active_users_last_month:,}**\n"
        f"• Active (Last Week): **{active_users_last_week:,}**\n"
        f"• Active (Last Day): **{active_users_last_day:,}**\n\n"
        f"**DOWNLOADS:**\n"
        f"• Total Downloads: **{total_downloads:,}**\n\n"
        f"**Most downloaded sites:**\n"
        f"{most_downloads_text}"
    )

@pyro_client.on_message(filters.private & filters.command("start"))
async def start(client, message: Message):
    if not await ensure_joined(client, message):
        return
    await register_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    await message.reply(
        "👋 Hello!\n"
        "Send me a video link from the supported sites.\n\n"
        "Supported sites:\n"
        "• YouTube\n"
        "• Facebook\n"
        "• Pinterest\n"
        "• X (Twitter)\n"
        "• TikTok\n"
        "• Instagram"
    )

@pyro_client.on_message(filters.private & filters.command("status"))
async def status_command(client, message: Message):
    if not await ensure_joined(client, message):
        return
    stats = await get_bot_statistics()
    await message.reply(stats)

@pyro_client.on_message(filters.private & filters.text)
async def handle_link(client, message: Message):
    if not await ensure_joined(client, message):
        return
    url = message.text.strip()
    if not any(domain in url.lower() for domain in SUPPORTED_DOMAINS):
        await message.reply("Please send a valid video link 👍")
        return
    await register_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    async with lock:
        if active_downloads < MAX_CONCURRENT_DOWNLOADS:
            asyncio.create_task(process_download(client, message, url))
        else:
            await queue.put((client, message, url))

def set_bot2_info():
    cmds = [
        telebot.types.BotCommand("start", "Start the bot")
    ]
    try:
        telebot_bot.set_my_commands(cmds)
    except Exception:
        logging.exception("Failed to set bot info")

@telebot_bot.message_handler(
    func=lambda m: m.chat.type in ["group", "supergroup"] and m.content_type == 'text'
)
def anti_spam_filter(message):
    try:
        bot_member = telebot_bot.get_chat_member(message.chat.id, telebot_bot.get_me().id)
        if bot_member.status not in ['administrator', 'creator']:
            return
        user_member = telebot_bot.get_chat_member(message.chat.id, message.from_user.id)
        if user_member.status in ['administrator', 'creator']:
            return
        text = message.text or ""
        if (
            len(text) > 1000
            or re.search(r"https?://", text)
            or "t.me/" in text
            or re.search(r"@\w+", text)
        ):
            telebot_bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
    except Exception:
        logging.exception("Anti-spam check failed")

@telebot_bot.message_handler(commands=['start'])
def handle_start(message):
    telebot_bot.send_message(
        message.chat.id,
        "👋 Welcome! Add me to your group and make me an admin to remove URls and @tags."
    )

@telebot_bot.message_handler(commands=['help'])
def handle_help(message):
    telebot_bot.send_message(
        message.chat.id,
        "Commands:\n"
        "/start - Start bot\n"
        "/help - This help message\n\n"
        "This bot only removes spam from groups when it is an admin.",
        parse_mode="Markdown"
    )

WEBHOOK_PATH = "/bot2"
WEBHOOK_URL = WEBHOOK_BASE.rstrip("/") + WEBHOOK_PATH

@flask_app.route("/", methods=["GET", "POST", "HEAD"])
def keep_alive():
    return "Bot is alive ✅", 200

@flask_app.route(WEBHOOK_PATH, methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        update = telebot.types.Update.de_json(request.data.decode('utf-8'))
        telebot_bot.process_new_updates([update])
        return '', 200
    else:
        abort(403)

@flask_app.route('/set_webhook', methods=['GET'])
def set_wh():
    try:
        telebot_bot.set_webhook(url=WEBHOOK_URL)
        return f"ok {WEBHOOK_URL}", 200
    except Exception as e:
        logging.exception(e)
        return "error", 500

@flask_app.route('/delete_webhook', methods=['GET'])
def del_wh():
    try:
        telebot_bot.delete_webhook()
        return "deleted", 200
    except Exception as e:
        logging.exception(e)
        return "error", 500

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT)

def run_bot():
    pyro_client.run()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    set_bot2_info()
    try:
        telebot_bot.set_webhook(url=WEBHOOK_URL)
    except Exception:
        logging.exception("Failed to set webhook on startup")
    Thread(target=run_flask, daemon=True).start()
    run_bot()
