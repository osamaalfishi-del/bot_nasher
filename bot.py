#!/usr/bin/env python3
"""
Telegram Auto-Publisher
- user client (Telethon): reads public source channels AND publishes to the target
  (media is re-sent by server-side reference: no download, no 'forwarded from' tag)
- bot client: admin control panel only
"""
import asyncio
import html as pyhtml
import logging
import os
import re
import sqlite3
import threading
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient, events, utils
from telethon.errors import FloodWaitError, RPCError
from telethon.extensions import html as tg_html
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.types import Channel, MessageMediaPoll, MessageMediaWebPage

load_dotenv()

# ───────────────────────── Config ─────────────────────────
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = {int(x) for x in os.environ["ADMIN_IDS"].split(",") if x.strip()}
SEND_DELAY = float(os.getenv("SEND_DELAY", "2"))
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DEFAULT_FOOTER = "📢 {channel}"

DATA_DIR.mkdir(mode=0o700, exist_ok=True)
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO
)
log = logging.getLogger("publisher")
logging.getLogger("telethon").setLevel(logging.WARNING)


# ───────────────────────── Storage ─────────────────────────
class DB:
    def __init__(self, path: Path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        os.chmod(path, 0o600)
        self.run(
            """CREATE TABLE IF NOT EXISTS sources(
                   channel_id INTEGER PRIMARY KEY, username TEXT, title TEXT)"""
        )
        self.run("CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT)")
        self.run("CREATE TABLE IF NOT EXISTS bans(word TEXT PRIMARY KEY)")
        self.run(
            """CREATE TABLE IF NOT EXISTS posted(
                   source_id INTEGER, msg_id INTEGER, PRIMARY KEY(source_id, msg_id))"""
        )

    def run(self, sql, params=()):
        with self.lock:
            cur = self.conn.execute(sql, params)
            self.conn.commit()
            return cur.fetchall()

    def get(self, key, default=""):
        rows = self.run("SELECT v FROM settings WHERE k=?", (key,))
        return rows[0][0] if rows else default

    def set(self, key, value):
        self.run("INSERT OR REPLACE INTO settings(k, v) VALUES(?, ?)", (key, str(value)))

    def sources(self):
        return self.run("SELECT channel_id, username, title FROM sources")

    def bans(self):
        return [r[0] for r in self.run("SELECT word FROM bans")]

    def seen(self, sid, mid):
        return bool(self.run("SELECT 1 FROM posted WHERE source_id=? AND msg_id=?", (sid, mid)))

    def mark(self, sid, mid):
        self.run("INSERT OR IGNORE INTO posted VALUES(?, ?)", (sid, mid))


db = DB(DATA_DIR / "state.db")
sources: set[int] = {r[0] for r in db.sources()}
send_lock = asyncio.Lock()

user = TelegramClient(str(DATA_DIR / "user"), API_ID, API_HASH)
bot = TelegramClient(str(DATA_DIR / "bot"), API_ID, API_HASH)
bot.parse_mode = "html"  # event.text on the bot returns HTML (keeps footer formatting)

# ───────────────────────── Cleaning ─────────────────────────
ANCHOR_RE = re.compile(r"<a\s[^>]*>(.*?)</a>", re.S | re.I)
URL_RE = re.compile(
    r"(?i)(?:https?://|ftp://|tg://|www\.|(?:t|telegram)\.me/)[^\s<>\"]+"
)
DOMAIN_RE = re.compile(
    r"(?i)\b(?:[a-z0-9-]+\.)+(?:com|net|org|io|me|ly|co|info|biz|app|xyz|site|online|link|gl|be)\b(?:/[^\s<>\"]*)?"
)
MENTION_RE = re.compile(r"(?<![\w@])@[A-Za-z][A-Za-z0-9_]{3,31}\b")


def clean_html(text: str) -> str:
    text = ANCHOR_RE.sub(r"\1", text)  # keep anchor text, drop the link
    text = URL_RE.sub("", text)
    if db.get("domains", "1") == "1":
        text = DOMAIN_RE.sub("", text)
    if db.get("mentions", "1") == "1":
        text = MENTION_RE.sub("", text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def render_footer() -> str:
    footer = db.get("footer", DEFAULT_FOOTER)
    if not footer:
        return ""
    uname = db.get("target_username")
    title = pyhtml.escape(db.get("target_title"))
    if uname:
        url = f"https://t.me/{uname}"
        link = f'<a href="{url}">{title or "@" + uname}</a>'
    else:
        url, link = "", title
    return footer.replace("{channel}", link).replace("{link}", url)


# ───────────────────────── Publishing ─────────────────────────
def has_media(m) -> bool:
    if m.media is None or isinstance(m.media, (MessageMediaWebPage, MessageMediaPoll)):
        return False
    return bool(m.photo or m.document)


async def deliver(target_id: int, caption: str, media: list) -> bool:
    """Send via the user account. Existing Telegram media is re-used by reference."""
    plain, _ = tg_html.parse(caption)
    entity = await user.get_input_entity(target_id)
    for _attempt in range(2):
        try:
            if media:
                payload = media if len(media) > 1 else media[0]
                if len(plain) <= 1024:
                    await user.send_file(entity, payload, caption=caption, parse_mode="html")
                else:  # caption limit: media first, text after
                    await user.send_file(entity, payload)
                    await user.send_message(entity, caption, parse_mode="html", link_preview=False)
            else:
                if len(plain) > 4096:
                    log.warning("Post exceeds 4096 chars, skipped")
                    return False
                await user.send_message(entity, caption, parse_mode="html", link_preview=False)
            return True
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
    return False


async def publish(source_id: int, msgs: list) -> None:
    msgs = [m for m in msgs if not m.action]
    target_id = db.get("target_id")
    if not msgs or not target_id or db.get("paused") == "1":
        return
    first = msgs[0]
    if db.seen(source_id, first.id):
        return

    text_msg = next((m for m in msgs if m.raw_text), msgs[0])
    raw = text_msg.raw_text or ""
    low = raw.lower()
    if any(w in low for w in db.bans()):
        log.info("Post %s/%s skipped (banned word)", source_id, first.id)
        return

    body = clean_html(tg_html.unparse(raw, text_msg.entities or []))
    media_msgs = [m for m in msgs if has_media(m)][:10]
    if any(getattr(m, "noforwards", False) for m in media_msgs):
        log.info("Post %s/%s skipped (protected content)", source_id, first.id)
        return
    if not body and not media_msgs:
        return
    caption = "\n\n".join(p for p in (body, render_footer()) if p)

    async with send_lock:
        try:
            if await deliver(int(target_id), caption, [m.media for m in media_msgs]):
                db.mark(source_id, first.id)
                log.info("Published %s/%s", source_id, first.id)
        except Exception:
            log.exception("Publish failed for %s/%s", source_id, first.id)
        await asyncio.sleep(SEND_DELAY)


@user.on(events.NewMessage(incoming=True))
async def on_message(event):
    if not event.is_channel or event.is_group or event.grouped_id:
        return
    if event.chat_id in sources:
        await publish(event.chat_id, [event.message])


@user.on(events.Album)
async def on_album(event):
    if event.is_channel and not event.is_group and event.chat_id in sources:
        await publish(event.chat_id, list(event.messages))


# ───────────────────────── Admin commands ─────────────────────────
HELP = (
    "<b>الأوامر</b>\n"
    "/add <code>@channel</code> — إضافة قناة مصدر\n"
    "/remove <code>@channel</code> — حذف قناة مصدر\n"
    "/list — القنوات المصدر\n"
    "/target <code>@channel</code> — قناة النشر (حساب المستخدم مشرف فيها)\n"
    "/footer <code>نص</code> — التذييل (يدعم تنسيق تيليجرام، "
    "<code>{channel}</code> = رابط قناتك)\n"
    "/footer_off — إلغاء التذييل\n"
    "/ban <code>كلمة</code> · /unban <code>كلمة</code> · /bans — كلمات تمنع النشر\n"
    "/mentions on|off — حذف @المعرّفات\n"
    "/domains on|off — حذف النطاقات المجردة\n"
    "/pause · /resume · /status"
)

REF_RE = re.compile(r"(?:https?://)?(?:t(?:elegram)?\.me/)?@?([A-Za-z][A-Za-z0-9_]{3,31})/?")


def parse_ref(s: str):
    s = s.strip()
    m = REF_RE.fullmatch(s)
    if m:
        return m.group(1)
    if re.fullmatch(r"-100\d{5,15}", s):
        return int(s)
    return None


def esc(s) -> str:
    return pyhtml.escape(str(s))


async def reply(event, text):
    await event.reply(text, parse_mode="html", link_preview=False)


async def cmd_help(event, arg):
    await reply(event, HELP)


async def cmd_add(event, arg):
    ref = parse_ref(arg)
    if not isinstance(ref, str):
        return await reply(event, "❌ أرسل معرّف قناة عامة: <code>/add @channel</code>")
    try:
        ent = await user.get_entity(ref)
    except (ValueError, RPCError):
        return await reply(event, "❌ لم أجد القناة.")
    if not isinstance(ent, Channel) or not ent.broadcast:
        return await reply(event, "❌ هذا ليس قناة.")
    cid = utils.get_peer_id(ent)
    if str(cid) == db.get("target_id"):
        return await reply(event, "❌ لا يمكن أن تكون قناة المصدر هي قناة النشر.")
    try:
        await user(JoinChannelRequest(ent))
    except RPCError as e:
        return await reply(event, f"❌ تعذّر الانضمام: {esc(type(e).__name__)}")
    db.run(
        "INSERT OR REPLACE INTO sources VALUES(?, ?, ?)",
        (cid, (ent.username or "").lower(), ent.title),
    )
    sources.add(cid)
    await reply(event, f"✅ أُضيفت: <b>{esc(ent.title)}</b>")


async def cmd_remove(event, arg):
    ref = parse_ref(arg)
    if not isinstance(ref, str):
        return await reply(event, "❌ مثال: <code>/remove @channel</code>")
    rows = db.run("SELECT channel_id FROM sources WHERE username=?", (ref.lower(),))
    if not rows:
        return await reply(event, "❌ القناة غير موجودة في القائمة.")
    cid = rows[0][0]
    db.run("DELETE FROM sources WHERE channel_id=?", (cid,))
    sources.discard(cid)
    try:
        await user(LeaveChannelRequest(await user.get_input_entity(cid)))
    except Exception:
        pass
    await reply(event, "🗑 تم الحذف.")


async def cmd_list(event, arg):
    rows = db.sources()
    if not rows:
        return await reply(event, "لا توجد قنوات مصدر.")
    lines = [f"• {esc(t)} (@{esc(u)})" if u else f"• {esc(t)}" for _, u, t in rows]
    await reply(event, "<b>القنوات المصدر:</b>\n" + "\n".join(lines))


async def cmd_target(event, arg):
    ref = parse_ref(arg)
    if ref is None:
        return await reply(event, "❌ مثال: <code>/target @mychannel</code>")
    ent = None
    for attempt in range(2):
        try:
            ent = await user.get_entity(ref)
            break
        except (ValueError, RPCError):
            if attempt == 0:
                await user.get_dialogs()  # refresh entity cache, then retry once
    if ent is None:
        return await reply(event, "❌ لم أصل للقناة. تأكد أن حساب المستخدم مشرف فيها.")
    if not isinstance(ent, Channel) or not ent.broadcast:
        return await reply(event, "❌ هذا ليس قناة.")
    perms = await user.get_permissions(ent, await user.get_me())
    if not (perms.is_creator or (perms.is_admin and perms.post_messages)):
        return await reply(event, "❌ حساب المستخدم ليس مشرفاً بصلاحية نشر الرسائل في هذه القناة.")
    cid = utils.get_peer_id(ent)
    if cid in sources:
        return await reply(event, "❌ هذه القناة مُضافة كمصدر.")
    db.set("target_id", cid)
    db.set("target_username", ent.username or "")
    db.set("target_title", ent.title)
    await reply(event, f"✅ قناة النشر: <b>{esc(ent.title)}</b>")


async def cmd_footer(event, arg):
    parts = (event.text or "").split(None, 1)
    new = parts[1].strip() if len(parts) > 1 else ""
    if not new:
        cur = render_footer()
        return await reply(event, "التذييل الحالي:\n\n" + (cur or "— لا يوجد —"))
    if len(new) > 600:
        return await reply(event, "❌ التذييل طويل جداً.")
    db.set("footer", new)
    await reply(event, "✅ تم الحفظ. المعاينة:\n\n" + render_footer())


async def cmd_footer_off(event, arg):
    db.set("footer", "")
    await reply(event, "✅ أُلغي التذييل.")


async def cmd_ban(event, arg):
    word = arg.strip().lower()
    if not word or len(word) > 64:
        return await reply(event, "❌ مثال: <code>/ban كلمة</code>")
    db.run("INSERT OR IGNORE INTO bans VALUES(?)", (word,))
    await reply(event, "✅ أُضيفت.")


async def cmd_unban(event, arg):
    db.run("DELETE FROM bans WHERE word=?", (arg.strip().lower(),))
    await reply(event, "✅ تم.")


async def cmd_bans(event, arg):
    words = db.bans()
    await reply(event, "الكلمات المحظورة:\n" + ("\n".join(f"• {esc(w)}" for w in words) or "— لا يوجد —"))


def toggle_cmd(key, label):
    async def handler(event, arg):
        val = arg.strip().lower()
        if val not in ("on", "off"):
            return await reply(event, f"❌ مثال: <code>/{key} on</code>")
        db.set(key, "1" if val == "on" else "0")
        await reply(event, f"✅ {label}: {'مفعّل' if val == 'on' else 'معطّل'}")
    return handler


async def cmd_pause(event, arg):
    db.set("paused", "1")
    await reply(event, "⏸ أُوقف النشر.")


async def cmd_resume(event, arg):
    db.set("paused", "0")
    await reply(event, "▶️ استُؤنف النشر.")


async def cmd_status(event, arg):
    await reply(
        event,
        "<b>الحالة</b>\n"
        f"النشر: {'⏸ متوقف' if db.get('paused') == '1' else '▶️ يعمل'}\n"
        f"قناة النشر: {esc(db.get('target_title') or '— غير محددة —')}\n"
        f"عدد المصادر: {len(sources)}\n"
        f"حذف المعرّفات: {'نعم' if db.get('mentions', '1') == '1' else 'لا'}\n"
        f"حذف النطاقات: {'نعم' if db.get('domains', '1') == '1' else 'لا'}\n"
        f"كلمات محظورة: {len(db.bans())}",
    )


COMMANDS = {
    "start": cmd_help, "help": cmd_help, "add": cmd_add, "remove": cmd_remove,
    "list": cmd_list, "target": cmd_target, "footer": cmd_footer,
    "footer_off": cmd_footer_off, "ban": cmd_ban, "unban": cmd_unban, "bans": cmd_bans,
    "mentions": toggle_cmd("mentions", "حذف المعرّفات"),
    "domains": toggle_cmd("domains", "حذف النطاقات"),
    "pause": cmd_pause, "resume": cmd_resume, "status": cmd_status,
}


@bot.on(
    events.NewMessage(
        incoming=True,
        pattern=re.compile(r"^/(\w+)(?:@\w+)?(?:\s+(.*))?$", re.S | re.I),
    )
)
async def dispatcher(event):
    if not event.is_private or event.sender_id not in ADMIN_IDS:
        return  # silently ignore everyone else
    handler = COMMANDS.get(event.pattern_match.group(1).lower())
    if not handler:
        return
    try:
        await handler(event, (event.pattern_match.group(2) or "").strip())
    except Exception:
        log.exception("Command failed")
        await reply(event, "⚠️ خطأ داخلي، راجع السجلات.")


# ───────────────────────── Entrypoint ─────────────────────────
async def main():
    await user.start()  # first run: interactive login (phone + code)
    await bot.start(bot_token=BOT_TOKEN)
    for f in DATA_DIR.glob("*.session"):
        os.chmod(f, 0o600)
    log.info("Running. Sources: %d", len(sources))
    await asyncio.gather(user.run_until_disconnected(), bot.run_until_disconnected())


if __name__ == "__main__":
    asyncio.run(main())
