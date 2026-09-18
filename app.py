import hashlib
import json
import os
import secrets
import sqlite3
import time
import urllib.parse
import urllib.request
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

from flask import Flask, Response, render_template, request, jsonify, session, stream_with_context, redirect, url_for
from groq import Groq
from dotenv import load_dotenv
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import generate_password_hash, check_password_hash

load_dotenv()

app = Flask(__name__)

app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

FREE_GUEST_CHATS = 1     
DAILY_CHAT_LIMIT = 0     


def _get_secret_key():
    key = os.getenv("SECRET_KEY")
    if key:
        return key

    key_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".secret_key")

    try:
        if os.path.exists(key_file):
            with open(key_file, "r") as f:
                return f.read().strip()

        key = secrets.token_hex(32)
        with open(key_file, "w") as f:
            f.write(key)
        return key
    except OSError:
        
        return secrets.token_hex(32)


app.secret_key = _get_secret_key()

app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=30)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"


DB_PATH = os.environ.get("DATABASE_PATH") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "users.db"
)
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
USE_POSTGRES = bool(DATABASE_URL)


def _sqlite_to_pg(sql):
    """Translate SQLite `?` placeholders to psycopg `%s`, skipping quoted text."""
    out = []
    i, n = 0, len(sql)
    quote = None
    while i < n:
        ch = sql[i]
        if quote:
            out.append(ch)
            if ch == quote:
                if i + 1 < n and sql[i + 1] == quote:  # 
                    out.append(sql[i + 1])
                    i += 1
                else:
                    quote = None
        else:
            if ch in ("'", '"'):
                quote = ch
                out.append(ch)
            elif ch == "?":
                out.append("%s")
            else:
                out.append(ch)
        i += 1
    return "".join(out)


class _PgConnection:
    """Tiny adapter so psycopg behaves like the sqlite3 code already written."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=()):
        if params is None:
            params = ()
        elif not isinstance(params, (tuple, list, dict)):
            params = (params,)
        return self._conn.execute(_sqlite_to_pg(sql), params)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


def get_db():
    if USE_POSTGRES:
        import psycopg
        from psycopg.rows import dict_row

        conn = psycopg.connect(DATABASE_URL)
        conn.row_factory = dict_row
        return _PgConnection(conn)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _add_column(conn, table, name, ddl):
    if USE_POSTGRES:
        conn.execute("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s" % (table, ddl))
        return
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(%s)" % table)]
    if name not in cols:
        conn.execute("ALTER TABLE %s ADD COLUMN %s" % (table, ddl))


# Engine-specific DDL fragments.
_ID_COL = "id SERIAL PRIMARY KEY" if USE_POSTGRES else "id INTEGER PRIMARY KEY AUTOINCREMENT"
_NOW_EXPR = (
    "to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')"
    if USE_POSTGRES
    else "datetime('now')"
)


def init_db():
    conn = get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            %s,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            chats_used INTEGER NOT NULL DEFAULT 0,
            chat_date TEXT NOT NULL DEFAULT '',
            custom_instructions TEXT NOT NULL DEFAULT '',
            memory TEXT NOT NULL DEFAULT '',
            verified INTEGER NOT NULL DEFAULT 1,
            verify_token TEXT NOT NULL DEFAULT '',
            verify_expires TEXT NOT NULL DEFAULT '',
            reset_token TEXT NOT NULL DEFAULT '',
            reset_expires TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (%s)
        )
        """ % (_ID_COL, _NOW_EXPR)
    )
    
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users(email)")
    except Exception:
        pass
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS shares (
            token TEXT PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (%s)
        )
        """ % (_NOW_EXPR,)
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS conversations (
            %s,
            user_id INTEGER NOT NULL,
            title TEXT NOT NULL DEFAULT 'New chat',
            pinned INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (%s),
            updated_at TEXT NOT NULL DEFAULT (%s)
        )
        """ % (_ID_COL, _NOW_EXPR, _NOW_EXPR)
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            %s,
            conversation_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            tokens INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (%s)
        )
        """ % (_ID_COL, _NOW_EXPR)
    )

    # Migrations for databases created by older versions.
    _add_column(conn, "users", "custom_instructions", "custom_instructions TEXT NOT NULL DEFAULT ''")
    _add_column(conn, "users", "memory", "memory TEXT NOT NULL DEFAULT ''")
    _add_column(conn, "users", "project_name", "project_name TEXT NOT NULL DEFAULT ''")
    _add_column(conn, "users", "project_desc", "project_desc TEXT NOT NULL DEFAULT ''")
    _add_column(conn, "users", "verified", "verified INTEGER NOT NULL DEFAULT 1")
    _add_column(conn, "users", "verify_token", "verify_token TEXT NOT NULL DEFAULT ''")
    _add_column(conn, "users", "verify_expires", "verify_expires TEXT NOT NULL DEFAULT ''")
    _add_column(conn, "users", "reset_token", "reset_token TEXT NOT NULL DEFAULT ''")
    _add_column(conn, "users", "reset_expires", "reset_expires TEXT NOT NULL DEFAULT ''")
    _add_column(conn, "conversations", "pinned", "pinned INTEGER NOT NULL DEFAULT 0")
    _add_column(conn, "messages", "tokens", "tokens INTEGER NOT NULL DEFAULT 0")

    conn.commit()
    conn.close()


init_db()


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def guest_remaining():
    used = session.get("guest_chats", 0)
    return max(0, FREE_GUEST_CHATS - used)


def daily_remaining(user):
    """Chats left today for a signed-in user. Returns None when unlimited."""
    if DAILY_CHAT_LIMIT <= 0:
        return None
    if user["chat_date"] != today_str():
        return DAILY_CHAT_LIMIT
    return max(0, DAILY_CHAT_LIMIT - user["chats_used"])


def remaining_after(used):
    """Chats left after one more chat; None when unlimited."""
    if DAILY_CHAT_LIMIT <= 0:
        return None
    return max(0, DAILY_CHAT_LIMIT - used - 1)


def get_current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    conn.close()

    if not user:
        session.pop("user_id", None)
        return None

    return user


def sanitize_conversation(conversation):
    """Keep only valid user/assistant text messages from client input."""
    out = []
    if not isinstance(conversation, list):
        return out

    for message in conversation:
        if not isinstance(message, dict):
            continue
        role = message.get("role")
        content = message.get("content")
        if role not in ("user", "assistant"):
            continue
        if not isinstance(content, str) or not content.strip():
            continue
        out.append({"role": role, "content": content})
        
    if out and out[-1]["role"] == "user":
        out = out[:-1]

    return out[-40:]  

_attempts = defaultdict(deque)


def _rate_limited(key, limit, window_seconds):
    now = time.time()
    dq = _attempts[key]
    while dq and now - dq[0] > window_seconds:
        dq.popleft()
    if len(dq) >= limit:
        return True
    dq.append(now)
    return False


def _rate_key():
    user = get_current_user()
    if user:
        return "u" + str(user["id"])
    return "ip:" + (request.remote_addr or "?")


groq_api_key = os.getenv("GROQ_API_KEY")

client = Groq(api_key=groq_api_key) if groq_api_key else None

MODEL = "openai/gpt-oss-20b"


SYSTEM_PROMPT = """
You are AadidevGPT-0, a friendly, helpful, and conversational AI assistant created by Aadidev Prasanth.

TONE & PERSONALITY:
- Speak naturally, warmly, and directly.
- Be concise, supportive, and engaging.
- Understand the user's question before answering.
- Give useful explanations when needed.
- Do not be unnecessarily repetitive.

ABOUT YOU (share these facts in your own words only when they're relevant — e.g. the user asks who you are, who made you, how you were built, or what model you use):
- You were created by Aadidev Prasanth.
- The AadidevGPT application was written by hand, line by line — no AI was used to write its code.
- You run on AadidevGPT's own model, called Model 04aadi.
- Never recite these bullet points verbatim. Weave the facts into one short, natural, confident answer.

FORMATTING RULES:
1. DO NOT introduce yourself or repeat your name unless the user explicitly asks "Who are you?" or gives an initial greeting.
2. Use Markdown formatting when it makes the answer clearer: bullet/numbered lists, **bold** for key terms, and fenced code blocks (```) for code. Keep paragraphs readable and avoid over-formatting.
3. Answer the user's question directly; do not restate it back to them.
4. If the user asks for contact or support, tell them to email aadidevprasanth12@yahoo.com.
5. If the user asks for something but hasn't given enough information, ask for the missing details before giving a finalized answer.
6. If the user seems stuck or is having difficulty, ask what's wrong and offer to help.
7. If user asks for specific output type such as PDF,Doc, or code, you should ouput it formatted to what the user needs. This can be asked by you if user specifically hasent gaven enough information about what they want or if youser specifically asks the ai to give awnser in their wanted format.
8. If user wants to export as something to the ai cannot do tell user "Sorry, we do not export that type of file" and instead give them a copy and pastable finish just in the chat itself.
9. if user exploits user terms of service the ai should explicity say "Sorry, thats against our policy. If you think that AadidevGPT is rong please contact constomur servecis

CONVERSATION MEMORY:
- Pay attention to previous messages in the current conversation.
- Use previous messages to understand references such as "it", "that", "the code", or "what I said earlier".
- Do not treat every user message as a completely new conversation.
- Maintain continuity throughout the current conversation.
"""


AI_MODES = {
    "general": "",
    "coding": "You are in Coding mode. Focus on writing clean, correct, and well-commented code. Explain your approach briefly before or after the code, show concrete examples, and point out edge cases.",
    "study": "You are in Study mode. Act as a patient tutor: explain concepts step by step, use simple analogies, check understanding, and suggest practice questions. Encourage the learner.",
    "writing": "You are in Writing mode. Help craft polished, clear, and engaging writing in a natural human voice — avoid robotic, formulaic, or overly structured phrasing, and vary sentence length. Improve tone, structure, and flow, and offer suggestions rather than rewriting everything unless asked.",
    "school": "You are in School mode. Act like a helpful tutor and writing coach: explain the material clearly first, then help the student draft well-structured paragraphs in their OWN words and natural voice (varied sentence length, plain words, no formulaic AI-sounding phrasing). Focus on helping them learn and write, not doing the assignment for them.",
    "research": "You are in Research mode. Give thorough, well-organized, and factual answers. Break complex topics into sections, cite reasoning and caveats, and clearly note when information may be uncertain or outdated.",
    "business": "You are in Business mode. Be concise, professional, and results-oriented. Give practical, actionable advice with clear priorities, and use bullet points and summaries where helpful.",
    "build": "You are in Build mode. Act as a senior engineer who builds complete, working projects. When the user asks to build something, respond with a concrete plan and then the actual code — organized file by file with clear file names (e.g. `### index.html`), runnable code, and short setup instructions. Prefer minimal, working solutions over abstractions, and note what the user still needs to add (API keys, etc.).",
}


def build_model_messages(user_message, history, custom_instructions="", memory="", mode="general", project_name="", project_desc=""):
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    persona = AI_MODES.get(mode or "general") or ""
    if persona:
        messages.append({"role": "system", "content": persona})
    if (project_name or "").strip():
        project_text = "ACTIVE PROJECT — the user is currently working on this project. Keep your answers relevant to it and remember it across messages:"
        if project_name.strip():
            project_text += "\nProject name: " + project_name.strip()
        if project_desc.strip():
            project_text += "\nProject details: " + project_desc.strip()
        messages.append({"role": "system", "content": project_text})
    if memory and memory.strip():
        messages.append({
            "role": "system",
            "content": "USER MEMORY — facts the user asked you to remember. Use them when relevant:\n" + memory.strip(),
        })
    if custom_instructions and custom_instructions.strip():
        messages.append({"role": "system", "content": custom_instructions.strip()})
    messages.extend(history[-40:])
    messages.append({"role": "user", "content": user_message})
    return messages


def make_title(text):
    title = " ".join(text.split())[:40]
    if len(text) > 40:
        title += "..."
    return title or "New chat"


def _serialize_conversation(r):
    return {
        "id": r["id"],
        "title": r["title"],
        "pinned": bool(r["pinned"]),
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }


def _resolve_context(user, data):
    """Applies the requested chat action for a logged-in user.

    Returns (True, (conn, conversation_id, prompt, model_history, used)) on
    success, or (False, (status, json_response)) on failure (conn closed).
    On success the caller owns `conn` and must close it.
    """

    
    conn = get_db()
    if user["chat_date"] != today_str():
        conn.execute(
            "UPDATE users SET chats_used = 0, chat_date = ? WHERE id = ?",
            (today_str(), user["id"]),
        )
        conn.commit()

    row = conn.execute(
        "SELECT chats_used, chat_date FROM users WHERE id = ?", (user["id"],)
    ).fetchone()
    used = row["chats_used"] if row["chat_date"] == today_str() else 0

    if DAILY_CHAT_LIMIT > 0 and used >= DAILY_CHAT_LIMIT:
        conn.close()
        return False, (
            403,
            {
                "error": "limit_reached",
                "message": "You've used all " + str(DAILY_CHAT_LIMIT) + " of your chats for today. Come back tomorrow!",
            },
        )

    user_message = (data.get("message") or "").strip()
    conversation_id = data.get("conversation_id")
    edit_message_id = data.get("edit_message_id")
    regenerate = bool(data.get("regenerate"))

    if not conversation_id:
        conversation_id = conn.execute(
            "INSERT INTO conversations (user_id, title) VALUES (?, ?) RETURNING id",
            (user["id"], make_title(user_message)),
        ).fetchone()["id"]
    else:
        conv = conn.execute(
            "SELECT id, title FROM conversations WHERE id = ? AND user_id = ?",
            (conversation_id, user["id"]),
        ).fetchone()
        if not conv:
            conn.close()
            return False, (404, {"error": "conversation_not_found", "message": "That conversation could not be found."})

 
    if edit_message_id:
        msg = conn.execute(
            "SELECT role FROM messages WHERE id = ? AND conversation_id = ?",
            (edit_message_id, conversation_id),
        ).fetchone()
        if not msg or msg["role"] != "user":
            conn.close()
            return False, (400, {"error": "invalid_edit", "message": "That message cannot be edited."})
        if not user_message:
            conn.close()
            return False, (400, {"error": "Please enter a message."})
        if len(user_message) > 12000:
            user_message = user_message[:12000]
        conn.execute(
            "UPDATE messages SET content = ? WHERE id = ?",
            (user_message, edit_message_id),
        )
        conn.execute("DELETE FROM messages WHERE id > ?", (edit_message_id,))


    elif regenerate:
        last = conn.execute(
            "SELECT id, role, content FROM messages WHERE conversation_id = ? "
            "ORDER BY id DESC LIMIT 1",
            (conversation_id,),
        ).fetchone()
        if not last or last["role"] != "assistant":
            conn.close()
            return False, (400, {"error": "nothing_to_regenerate", "message": "There's nothing to regenerate."})
        conn.execute("DELETE FROM messages WHERE id >= ?", (last["id"],))


    else:
        if not user_message:
            conn.close()
            return False, (400, {"error": "Please enter a message."})
        if len(user_message) > 12000:
            user_message = user_message[:12000]
        conn.execute(
            "INSERT INTO messages (conversation_id, role, content) VALUES (?, ?, ?)",
            (conversation_id, "user", user_message),
        )

    rows = conn.execute(
        "SELECT id, role, content FROM messages WHERE conversation_id = ? ORDER BY id ASC",
        (conversation_id,),
    ).fetchall()
    if not rows or rows[-1]["role"] != "user":
        conn.close()
        return False, (400, {"error": "invalid_history", "message": "This conversation has no active message."})

    prompt = rows[-1]["content"]
    model_history = [{"role": r["role"], "content": r["content"]} for r in rows[:-1]]
    title = conn.execute(
        "SELECT title FROM conversations WHERE id = ?", (conversation_id,)
    ).fetchone()["title"]

    return True, (conn, conversation_id, prompt, model_history, title, used)


def _insert_assistant(conn, conversation_id, text, tokens):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    message_id = conn.execute(
        "INSERT INTO messages (conversation_id, role, content, tokens) VALUES (?, ?, ?, ?) RETURNING id",
        (conversation_id, "assistant", text, tokens),
    ).fetchone()["id"]
    conn.execute(
        "UPDATE conversations SET updated_at = ? WHERE id = ?",
        (now, conversation_id),
    )
    return message_id, now


def _consume_chat(conn, user_id):
    conn.execute(
        "UPDATE users SET chats_used = chats_used + 1, chat_date = ? WHERE id = ?",
        (today_str(), user_id),
    )


WELCOME_COOKIE = "aadev_welcomed"


@app.route("/")
def home():
    
    if not request.cookies.get(WELCOME_COOKIE) and not request.args:
        return redirect(url_for("welcome"))
    return render_template("index.html")


@app.route("/welcome")
def welcome():
    return render_template("landing.html")


@app.route("/welcome/enter")
def welcome_enter():
    resp = redirect(url_for("home"))
    
    resp.set_cookie(WELCOME_COOKIE, "1", max_age=60 * 60 * 24 * 365, samesite="Lax")
    return resp


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.route("/me")
def me():
    user = get_current_user()

    if user:
        return jsonify({
            "logged_in": True,
            "email": user["email"],
            "remaining": daily_remaining(user),
            "daily_limit": DAILY_CHAT_LIMIT,
            "custom_instructions": user["custom_instructions"] or "",
            "memory": user["memory"] or "",
            "project_name": user["project_name"] or "",
            "project_desc": user["project_desc"] or "",
            "verified": bool(user["verified"]),
            "google_available": google_configured(),
            "email_configured": email_configured(),
        })

    return jsonify({
        "logged_in": False,
        "guest_remaining": guest_remaining(),
        "daily_limit": DAILY_CHAT_LIMIT,
        "google_available": google_configured(),
        "email_configured": email_configured(),
    })


@app.route("/signup", methods=["POST"])
def signup():
    if _rate_limited(_rate_key() + ":signup", 50, 300):
        return jsonify({"error": "Too many attempts. Please try again in a few minutes."}), 429

    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if "@" not in email or "." not in email:
        return jsonify({"error": "Please enter a valid email address."}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters."}), 400

    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        conn.close()
        return jsonify({"error": "An account with this email already exists. Please sign in."}), 409

    conn.execute(
        "INSERT INTO users (email, password_hash, verified) VALUES (?, ?, ?)",
        (email, generate_password_hash(password), 0 if email_configured() else 1),
    )


    verified = 1
    if email_configured():
        verified = 0
        token = secrets.token_urlsafe(32)
        conn.execute(
            "UPDATE users SET verify_token = ?, verify_expires = ? WHERE email = ?",
            (_hash_token(token), _token_expiry(), email),
        )
        conn.commit()
        link = request.url_root.rstrip("/") + "/verify-email?token=" + token
        send_email(
            email,
            "Verify your AadidevGPT-0 email",
            "Hi,\n\nPlease confirm this email address for your AadidevGPT-0 account.\n"
            "Open this link within 1 hour:\n" + link +
            "\n\nIf you didn't create an account, you can ignore this email.\n\n— AadidevGPT-0",
        )
    else:
        conn.commit()

    user = conn.execute(
        "SELECT id, email, chats_used, chat_date, custom_instructions, verified FROM users WHERE email = ?",
        (email,),
    ).fetchone()
    conn.close()

    session["user_id"] = user["id"]
    session.permanent = bool(data.get("remember", True))
    return jsonify({
        "ok": True,
        "logged_in": True,
        "email": email,
        "remaining": daily_remaining(user),
        "daily_limit": DAILY_CHAT_LIMIT,
        "custom_instructions": user["custom_instructions"] or "",
        "verified": bool(user["verified"]),
        "google_available": google_configured(),
        "email_configured": email_configured(),
    })


@app.route("/login", methods=["POST"])
def login():
    if _rate_limited(_rate_key() + ":login", 20, 300):
        return jsonify({"error": "Too many attempts. Please try again in a few minutes."}), 429

    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    conn.close()

    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify({"error": "Incorrect email or password."}), 401

    session["user_id"] = user["id"]
    session.permanent = bool(data.get("remember", True))
    return jsonify({
        "ok": True,
        "logged_in": True,
        "email": email,
        "remaining": daily_remaining(user),
        "daily_limit": DAILY_CHAT_LIMIT,
        "custom_instructions": user["custom_instructions"] or "",
        "verified": bool(user["verified"]),
        "google_available": google_configured(),
        "email_configured": email_configured(),
    })


@app.route("/logout", methods=["POST"])
def logout():
    session.pop("user_id", None)
    return jsonify({"ok": True})


@app.route("/settings", methods=["GET", "POST"])
def settings():
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401

    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        instructions = (data.get("custom_instructions") or "").strip()[:2000]
        project_name = (data.get("project_name") or "").strip()[:200]
        project_desc = (data.get("project_desc") or "").strip()[:3000]

        conn = get_db()
        conn.execute(
            "UPDATE users SET custom_instructions = ?, project_name = ?, project_desc = ? WHERE id = ?",
            (instructions, project_name, project_desc, user["id"]),
        )
        conn.commit()
        conn.close()

        return jsonify({"ok": True, "custom_instructions": instructions, "project_name": project_name, "project_desc": project_desc})

 
    conn = get_db()
    base = conn.execute(
        "SELECT id, email, verified, created_at, chats_used, chat_date FROM users WHERE id = ?",
        (user["id"],),
    ).fetchone()
    total_tokens = conn.execute(
        "SELECT COALESCE(SUM(m.tokens), 0) AS n FROM messages m "
        "JOIN conversations c ON m.conversation_id = c.id WHERE c.user_id = ?",
        (user["id"],),
    ).fetchone()["n"]
    conv_count = conn.execute(
        "SELECT COUNT(*) AS n FROM conversations WHERE user_id = ?", (user["id"],)
    ).fetchone()["n"]
    recent = conn.execute(
        "SELECT m.content, m.created_at, c.id AS conversation_id FROM messages m "
        "JOIN conversations c ON m.conversation_id = c.id "
        "WHERE c.user_id = ? AND m.role = 'user' ORDER BY m.id DESC LIMIT 10",
        (user["id"],),
    ).fetchall()
    conn.close()

    used_today = base["chats_used"] if base["chat_date"] == today_str() else 0

    return jsonify({
        "user": {
            "id": base["id"],
            "email": base["email"],
            "verified": bool(base["verified"]),
            "created_at": base["created_at"],
        },
        "custom_instructions": user["custom_instructions"] or "",
        "project": {
            "name": user["project_name"] or "",
            "desc": user["project_desc"] or "",
        },
        "usage": {
            "used_today": used_today,
            "remaining": None if DAILY_CHAT_LIMIT <= 0 else max(0, DAILY_CHAT_LIMIT - used_today),
            "daily_limit": DAILY_CHAT_LIMIT,
            "total_tokens": total_tokens,
            "conversation_count": conv_count,
        },
        "model": MODEL,
        "server": request.url_root.rstrip("/"),
        "recent_queries": [
            {
                "content": r["content"],
                "created_at": r["created_at"],
                "conversation_id": r["conversation_id"],
            }
            for r in recent
        ],
    })


@app.route("/change-password", methods=["POST"])
def change_password():
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401

    if _rate_limited(_rate_key() + ":pass", 10, 300):
        return jsonify({"error": "Too many attempts. Please try again in a few minutes."}), 429

    data = request.get_json(silent=True) or {}
    current_password = data.get("current_password") or ""
    new_password = data.get("new_password") or ""

    if len(new_password) < 6:
        return jsonify({"error": "New password must be at least 6 characters."}), 400

    conn = get_db()
    row = conn.execute("SELECT password_hash FROM users WHERE id = ?", (user["id"],)).fetchone()
    conn.close()

    if not row or not check_password_hash(row["password_hash"], current_password):
        return jsonify({"error": "Your current password is incorrect."}), 401

    conn = get_db()
    conn.execute(
        "UPDATE users SET password_hash = ? WHERE id = ?",
        (generate_password_hash(new_password), user["id"]),
    )
    conn.commit()
    conn.close()

    return jsonify({"ok": True})


@app.route("/delete-account", methods=["POST"])
def delete_account():
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401

    data = request.get_json(silent=True) or {}
    password = data.get("password") or ""

    conn = get_db()
    row = conn.execute("SELECT password_hash FROM users WHERE id = ?", (user["id"],)).fetchone()
    if not row or not check_password_hash(row["password_hash"], password):
        conn.close()
        return jsonify({"error": "Your password is incorrect."}), 400

    # Cascade delete: messages -> conversations -> user.
    conn.execute(
        "DELETE FROM messages WHERE conversation_id IN "
        "(SELECT id FROM conversations WHERE user_id = ?)",
        (user["id"],),
    )
    conn.execute("DELETE FROM conversations WHERE user_id = ?", (user["id"],))
    conn.execute("DELETE FROM users WHERE id = ?", (user["id"],))
    conn.commit()
    conn.close()

    session.pop("user_id", None)
    return jsonify({"ok": True})


GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"


def google_configured():
    return bool(os.getenv("GOOGLE_CLIENT_ID") and os.getenv("GOOGLE_CLIENT_SECRET"))


def google_redirect_uri():
    return request.url_root.rstrip("/") + "/auth/google/callback"


def _google_exchange_code(code):
    """Exchange the authorization code for tokens (patched in tests)."""
    params = urllib.parse.urlencode({
        "client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": google_redirect_uri(),
    }).encode()
    req = urllib.request.Request(
        GOOGLE_TOKEN_URL,
        data=params,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def _google_userinfo(access_token):
    """Fetch the signed-in user's profile (patched in tests)."""
    req = urllib.request.Request(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": "Bearer " + access_token},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


@app.route("/auth/google")
def google_login():
    if not google_configured():
        return redirect(url_for("home", auth_error="google_not_configured"))

    state = secrets.token_urlsafe(32)
    session["google_oauth_state"] = state

    params = urllib.parse.urlencode({
        "client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "redirect_uri": google_redirect_uri(),
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "prompt": "select_account",
        "access_type": "online",
    })
    return redirect(GOOGLE_AUTH_URL + "?" + params)


@app.route("/auth/google/callback")
def google_callback():
    if request.args.get("error"):
        return redirect(url_for("home", auth_error="google_denied"))

    code = request.args.get("code")
    state = request.args.get("state")
    if not code or not state or state != session.pop("google_oauth_state", None):
        return redirect(url_for("home", auth_error="google_state"))

    if not google_configured():
        return redirect(url_for("home", auth_error="google_not_configured"))

    try:
        tokens = _google_exchange_code(code)
        info = _google_userinfo(tokens.get("access_token") or "")
    except Exception as e:
        print("Google OAuth error:", str(e))
        return redirect(url_for("home", auth_error="google_state"))

    email = (info.get("email") or "").strip().lower()
    if not email or not info.get("email_verified"):
        return redirect(url_for("home", auth_error="google_email"))

    conn = get_db()
    user = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if not user:

        
        user = {"id": conn.execute(
            "INSERT INTO users (email, password_hash, verified) VALUES (?, ?, 1) RETURNING id",
            (email, generate_password_hash(secrets.token_urlsafe(24))),
        ).fetchone()["id"]}
        conn.commit()
    conn.close()

    session["user_id"] = user["id"]
    session.permanent = True
    return redirect(url_for("home"))
    

def email_configured():
    return bool(os.getenv("SMTP_HOST") and os.getenv("SMTP_USER") and os.getenv("SMTP_PASSWORD"))


def send_email(to, subject, body_text):
    """Send a plain-text email over SMTP; returns True on success.
    Works with Gmail app passwords (smtp.gmail.com:587)."""
    if not email_configured():
        return False
    import smtplib
    from email.mime.text import MIMEText

    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    pwd = os.getenv("SMTP_PASSWORD")
    mail_from = os.getenv("MAIL_FROM") or user

    msg = MIMEText(body_text, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = "AadidevGPT-0 <%s>" % mail_from
    msg["To"] = to

    try:
        with smtplib.SMTP(host, port, timeout=15) as server:
            server.starttls()
            server.login(user, pwd)
            server.sendmail(mail_from, [to], msg.as_string())
        return True
    except Exception as e:
        print("Email error:", str(e))
        return False


def _hash_token(token):
    return hashlib.sha256(token.encode()).hexdigest()


def _token_expiry():
    return (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()


def _token_valid(expires):
    if not expires:
        return False
    try:
        return datetime.fromisoformat(expires) > datetime.now(timezone.utc)
    except ValueError:
        return False


@app.route("/forgot-password", methods=["POST"])
def forgot_password():
    if _rate_limited(_rate_key() + ":forgot", 5, 300):
        return jsonify({"error": "Too many requests. Please try again in a few minutes."}), 429
    if not email_configured():
        return jsonify({"error": "Email isn't configured on this server yet."}), 503

    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()

    conn = get_db()
    user = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if user:
        token = secrets.token_urlsafe(32)
        conn.execute(
            "UPDATE users SET reset_token = ?, reset_expires = ? WHERE id = ?",
            (_hash_token(token), _token_expiry(), user["id"]),
        )
        conn.commit()
        link = request.url_root.rstrip("/") + "/reset-password?token=" + token
        send_email(
            email,
            "Reset your AadidevGPT-0 password",
            "Hi,\n\nYou asked to reset your AadidevGPT-0 password.\n"
            "Open this link within 1 hour to choose a new one:\n" + link +
            "\n\nIf you didn't ask for this, you can safely ignore this email.\n\n— AadidevGPT-0",
        )
    conn.close()

   
    return jsonify({"ok": True})


@app.route("/reset-password")
def reset_page():
    return render_template("reset.html", token=request.args.get("token", ""))


@app.route("/reset-password", methods=["POST"])
def reset_password():
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    new_password = data.get("new_password") or ""

    if len(new_password) < 6:
        return jsonify({"error": "Password must be at least 6 characters."}), 400

    conn = get_db()
    row = conn.execute(
        "SELECT id, reset_expires FROM users WHERE reset_token = ?",
        (_hash_token(token),),
    ).fetchone()
    if not row or not _token_valid(row["reset_expires"]):
        conn.close()
        return jsonify({"error": "This reset link is invalid or has expired."}), 400

    conn.execute(
        "UPDATE users SET password_hash = ?, reset_token = '', reset_expires = '' WHERE id = ?",
        (generate_password_hash(new_password), row["id"]),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/verify-email")
def verify_email():
    token = request.args.get("token", "")
    conn = get_db()
    row = conn.execute(
        "SELECT id, verify_expires FROM users WHERE verify_token = ?",
        (_hash_token(token),),
    ).fetchone()
    if not row or not _token_valid(row["verify_expires"]):
        conn.close()
        return redirect(url_for("home", auth_error="verify_expired"))

    conn.execute(
        "UPDATE users SET verified = 1, verify_token = '', verify_expires = '' WHERE id = ?",
        (row["id"],),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("home", verified=1))


@app.route("/resend-verification", methods=["POST"])
def resend_verification():
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401
    if not email_configured():
        return jsonify({"error": "Email isn't configured on this server yet."}), 503
    if user["verified"]:
        return jsonify({"ok": True, "message": "Your email is already verified."})

    token = secrets.token_urlsafe(32)
    conn = get_db()
    conn.execute(
        "UPDATE users SET verify_token = ?, verify_expires = ? WHERE id = ?",
        (_hash_token(token), _token_expiry(), user["id"]),
    )
    conn.commit()
    conn.close()

    link = request.url_root.rstrip("/") + "/verify-email?token=" + token
    send_email(
        user["email"],
        "Verify your AadidevGPT-0 email",
        "Hi,\n\nPlease confirm this email address for your AadidevGPT-0 account.\n"
        "Open this link within 1 hour:\n" + link +
        "\n\nIf you didn't create an account, you can ignore this email.\n\n— AadidevGPT-0",
    )
    return jsonify({"ok": True})


@app.route("/conversations", methods=["GET"])
def list_conversations():
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401

    q = (request.args.get("q") or "").strip()
    try:
        limit = min(max(int(request.args.get("limit", 50)), 1), 100)
        offset = max(int(request.args.get("offset", 0)), 0)
    except ValueError:
        limit, offset = 50, 0

    conn = get_db()
    if q:
        like = "%" + q + "%"
        rows = conn.execute(
            "SELECT DISTINCT c.id, c.title, c.pinned, c.created_at, c.updated_at "
            "FROM conversations c LEFT JOIN messages m ON m.conversation_id = c.id "
            "WHERE c.user_id = ? AND (c.title LIKE ? OR m.content LIKE ?) "
            "ORDER BY c.updated_at DESC, c.id DESC LIMIT ? OFFSET ?",
            (user["id"], like, like, limit, offset),
        ).fetchall()
        total = conn.execute(
            "SELECT COUNT(DISTINCT c.id) AS n FROM conversations c "
            "LEFT JOIN messages m ON m.conversation_id = c.id "
            "WHERE c.user_id = ? AND (c.title LIKE ? OR m.content LIKE ?)",
            (user["id"], like, like),
        ).fetchone()["n"]
    else:
        rows = conn.execute(
            "SELECT id, title, pinned, created_at, updated_at FROM conversations "
            "WHERE user_id = ? ORDER BY pinned DESC, updated_at DESC, id DESC LIMIT ? OFFSET ?",
            (user["id"], limit, offset),
        ).fetchall()
        total = conn.execute(
            "SELECT COUNT(*) AS n FROM conversations WHERE user_id = ?", (user["id"],)
        ).fetchone()["n"]
    conn.close()

    return jsonify({
        "conversations": [_serialize_conversation(r) for r in rows],
        "total": total,
        "has_more": offset + len(rows) < total,
    })


@app.route("/conversations/<int:cid>", methods=["GET"])
def get_conversation(cid):
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401

    conn = get_db()
    conv = conn.execute(
        "SELECT id, title, pinned FROM conversations WHERE id = ? AND user_id = ?",
        (cid, user["id"]),
    ).fetchone()
    if not conv:
        conn.close()
        return jsonify({"error": "not_found"}), 404

    rows = conn.execute(
        "SELECT id, role, content, tokens, created_at FROM messages "
        "WHERE conversation_id = ? ORDER BY id ASC",
        (cid,),
    ).fetchall()
    conn.close()

    return jsonify({
        "id": conv["id"],
        "title": conv["title"],
        "pinned": bool(conv["pinned"]),
        "messages": [
            {
                "id": r["id"],
                "role": r["role"],
                "content": r["content"],
                "tokens": r["tokens"],
                "created_at": r["created_at"],
            }
            for r in rows
        ],
    })


@app.route("/conversations/<int:cid>", methods=["PATCH"])
def update_conversation(cid):
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401

    data = request.get_json(silent=True) or {}

    conn = get_db()
    conv = conn.execute(
        "SELECT id FROM conversations WHERE id = ? AND user_id = ?",
        (cid, user["id"]),
    ).fetchone()
    if not conv:
        conn.close()
        return jsonify({"error": "not_found"}), 404

    if "title" in data:
        title = (data.get("title") or "").strip()[:120]
        if title:
            conn.execute("UPDATE conversations SET title = ? WHERE id = ?", (title, cid))

    if "pinned" in data:
        conn.execute(
            "UPDATE conversations SET pinned = ? WHERE id = ?",
            (1 if data.get("pinned") else 0, cid),
        )

    conn.commit()
    row = conn.execute(
        "SELECT id, title, pinned, created_at, updated_at FROM conversations WHERE id = ?",
        (cid,),
    ).fetchone()
    conn.close()

    return jsonify({"conversation": _serialize_conversation(row)})


@app.route("/conversations/<int:cid>", methods=["DELETE"])
def delete_conversation(cid):
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401

    conn = get_db()
    conv = conn.execute(
        "SELECT id FROM conversations WHERE id = ? AND user_id = ?",
        (cid, user["id"]),
    ).fetchone()
    if not conv:
        conn.close()
        return jsonify({"error": "not_found"}), 404

    conn.execute("DELETE FROM messages WHERE conversation_id = ?", (cid,))
    conn.execute("DELETE FROM conversations WHERE id = ?", (cid,))
    conn.commit()
    conn.close()

    return jsonify({"ok": True})


@app.route("/conversations/<int:cid>/export")
def export_conversation(cid):
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401

    fmt = (request.args.get("format") or "md").lower()
    if fmt not in ("md", "txt", "pdf"):
        fmt = "md"

    conn = get_db()
    conv = conn.execute(
        "SELECT id, title, created_at FROM conversations WHERE id = ? AND user_id = ?",
        (cid, user["id"]),
    ).fetchone()
    if not conv:
        conn.close()
        return jsonify({"error": "not_found"}), 404

    rows = conn.execute(
        "SELECT role, content FROM messages WHERE conversation_id = ? ORDER BY id ASC",
        (cid,),
    ).fetchall()
    conn.close()

    safe_name = "".join(c for c in conv["title"] if c.isalnum() or c in " -_")[:40].strip() or "chat"

    if fmt == "txt":
        lines = [conv["title"], "Exported from AadidevGPT-0 · " + (conv["created_at"] or ""), "", "=" * 40, ""]
        for r in rows:
            speaker = "You" if r["role"] == "user" else "AadidevGPT-0"
            lines.append(speaker + ":")
            lines.append("")
            lines.append(r["content"])
            lines.append("")
            lines.append("-" * 40)
            lines.append("")
        return (
            "\n".join(lines),
            200,
            {
                "Content-Type": "text/plain; charset=utf-8",
                "Content-Disposition": 'attachment; filename="aadev-%s.txt"' % safe_name,
            },
        )

    if fmt == "pdf":
        return _export_pdf(conv, rows, safe_name)

    # Default: Markdown.
    lines = [
        "# " + conv["title"],
        "",
        "Exported from AadidevGPT-0 · " + (conv["created_at"] or ""),
        "",
        "---",
        "",
    ]
    for r in rows:
        speaker = "You" if r["role"] == "user" else "AadidevGPT-0"
        lines.append("**" + speaker + ":**")
        lines.append("")
        lines.append(r["content"])
        lines.append("")

    return (
        "\n".join(lines),
        200,
        {
            "Content-Type": "text/markdown; charset=utf-8",
            "Content-Disposition": 'attachment; filename="aadev-%s.md"' % safe_name,
        },
    )


def _export_pdf(conv, rows, safe_name):
    """Build a minimal, dependency-free single-page PDF with the chat text."""
    def esc(s):
        return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    text_lines = []
    for r in rows:
        speaker = "You" if r["role"] == "user" else "AadidevGPT-0"
        text_lines.append(speaker + ":")
        text_lines.extend((r["content"] or "").split("\n"))
        text_lines.append("")

  
    wrapped = []
    for line in text_lines:
        while len(line) > 95:
            wrapped.append(line[:95])
            line = line[95:]
        wrapped.append(line)

    # A4 = 595 x 842 pt. 12 pt font, 14 pt line height.
    line_h = 14
    top = 800
    left = 50
    max_lines = int(top / line_h)

    pages = []
    chunk = wrapped[:max_lines]
    while chunk:
        pages.append(chunk)
        rest = wrapped[len(chunk):]
        if not rest:
            break
        # im leavig Leave room for a page number on the next page.
        chunk = rest[:max_lines - 1]

    objects = []
    content = []
    obj_id = 1

    def add(obj):
        nonlocal obj_id
        objects.append((obj_id, obj))
        obj_id += 1

    add("<< /Type /Catalog /Pages 2 0 R >>")  # 1
    add("<< /Type /Pages /Kids [3 0 R] /Count 1 >>")  # 2

    page_objs = []
    for pi, page_lines in enumerate(pages):
        stream = "BT /F1 12 Tf 14 TL"
        y = top
        for line in page_lines:
            stream += " 1 0 0 1 %d %d Tm (%s) Tj T*" % (left, y, esc(line))
            y -= line_h
        stream += " ET"
        content_id = obj_id + 1
        page_obj_id = obj_id + 2
        add("<< /Length %d >>\nstream\n%s\nendstream" % (len(stream), stream))
        add("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents %d 0 R >>" % content_id)
        page_objs.append(page_obj_id)

    objects[1] = (2, "<< /Type /Pages /Kids [%s] /Count %d >>" % (
        " ".join("%d 0 R" % p for p in page_objs), len(page_objs)))
    add("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")  # 4

    body = "%PDF-1.4\n"
    offsets = {}
    for oid, obj in objects:
        offsets[oid] = len(body)
        body += "%d 0 obj\n%s\nendobj\n" % (oid, obj)
    xref_pos = len(body)
    body += "xref\n0 %d\n" % (obj_id)
    body += "0000000000 65535 f \n"
    for oid in range(1, obj_id):
        body += "%010d 00000 n \n" % offsets[oid]
    body += "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF" % (obj_id, xref_pos)

    return (
        body.encode("latin-1", errors="replace"),
        200,
        {
            "Content-Type": "application/pdf",
            "Content-Disposition": 'attachment; filename="aadev-%s.pdf"' % safe_name,
        },
    )


@app.route("/memory", methods=["GET"])
def get_memory():
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401
    return jsonify({"ok": True, "memory": user["memory"] or ""})


@app.route("/memory", methods=["POST"])
def set_memory():
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401

    data = request.get_json(silent=True) or {}
    memory = (data.get("memory") or "").strip()[:4000]

    conn = get_db()
    conn.execute("UPDATE users SET memory = ? WHERE id = ?", (memory, user["id"]))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "memory": memory})


@app.route("/memory/remember", methods=["POST"])
def remember_fact():
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401

    data = request.get_json(silent=True) or {}
    fact = (data.get("fact") or "").strip()[:500]
    if not fact:
        return jsonify({"error": "Nothing to remember."}), 400

    existing = (user["memory"] or "").strip()
    memory = (existing + "\n- " + fact).strip()[:4000] if existing else "- " + fact

    conn = get_db()
    conn.execute("UPDATE users SET memory = ? WHERE id = ?", (memory, user["id"]))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "memory": memory})


@app.route("/memory/forget", methods=["POST"])
def forget_memory():
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401

    conn = get_db()
    conn.execute("UPDATE users SET memory = '' WHERE id = ?", (user["id"],))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

def _share_token_for(conn, cid):
    row = conn.execute(
        "SELECT token FROM shares WHERE conversation_id = ?", (cid,)
    ).fetchone()
    if row:
        return row["token"]
    token = secrets.token_urlsafe(12)
    conn.execute(
        "INSERT INTO shares (token, conversation_id) VALUES (?, ?)", (token, cid)
    )
    conn.commit()
    return token


@app.route("/conversations/<int:cid>/share", methods=["POST"])
def share_conversation(cid):
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401

    conn = get_db()
    conv = conn.execute(
        "SELECT id FROM conversations WHERE id = ? AND user_id = ?",
        (cid, user["id"]),
    ).fetchone()
    if not conv:
        conn.close()
        return jsonify({"error": "not_found"}), 404

    token = _share_token_for(conn, cid)
    conn.close()
    return jsonify({
        "ok": True,
        "url": request.url_root.rstrip("/") + "/share/" + token,
        "token": token,
    })


@app.route("/conversations/<int:cid>/share", methods=["DELETE"])
def unshare_conversation(cid):
    user = get_current_user()
    if not user:
        return jsonify({"error": "requires_auth"}), 401

    conn = get_db()
    conn.execute(
        "DELETE FROM shares WHERE conversation_id = ? AND conversation_id IN "
        "(SELECT id FROM conversations WHERE id = ? AND user_id = ?)",
        (cid, cid, user["id"]),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/share/<token>")
def view_share(token):
    conn = get_db()
    row = conn.execute(
        "SELECT conversation_id FROM shares WHERE token = ?", (token,)
    ).fetchone()
    if not row:
        conn.close()
        return render_template("share.html", error="This share link is invalid or has been removed."), 404

    conv = conn.execute(
        "SELECT title, created_at FROM conversations WHERE id = ?",
        (row["conversation_id"],),
    ).fetchone()
    if not conv:
        conn.close()
        return render_template("share.html", error="This conversation no longer exists."), 404

    msgs = conn.execute(
        "SELECT role, content, created_at FROM messages WHERE conversation_id = ? ORDER BY id ASC",
        (row["conversation_id"],),
    ).fetchall()
    conn.close()

    return render_template(
        "share.html",
        title=conv["title"],
        created_at=conv["created_at"],
        messages=[{"role": r["role"], "content": r["content"], "created_at": r["created_at"]} for r in msgs],
    )



@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json(silent=True)

    if not data:
        return jsonify({"error": "Please send a valid message."}), 400

    user_message = (data.get("message") or "").strip()

    if not user_message:
        return jsonify({"error": "Please enter a message."}), 400

    if client is None:
        return jsonify({
            "error": " AadidevGPT-0 is not configured yet: the server is missing its GROQ_API_KEY."
        }), 503

    if _rate_limited(_rate_key() + ":chat", 60, 60):
        return jsonify({"error": "You're sending messages too fast. Please slow down."}), 429

    user = get_current_user()
    logged_in = user is not None

    if logged_in:
        ok, payload = _resolve_context(user, data)
        if not ok:
            return jsonify(payload[1]), payload[0]
        conn, conversation_id, prompt, model_history, title, used = payload

        messages = build_model_messages(
            prompt, model_history, user["custom_instructions"], user["memory"],
            data.get("mode") or "general", user["project_name"], user["project_desc"],
        )

        try:
            completion = client.chat.completions.create(model=MODEL, messages=messages)
            bot_response = completion.choices[0].message.content or "⚠️ AadidevGPT-0 returned an empty reply. Please try again."
            tokens = getattr(getattr(completion, "usage", None), "total_tokens", 0) or 0
        except Exception as e:
            print("Groq API error:", str(e))
            conn.rollback()
            conn.close()
            return jsonify({
                "error": "⚠️ AadidevGPT-0 is having trouble connecting to the AI service right now."
            }), 500

        _insert_assistant(conn, conversation_id, bot_response, tokens)
        _consume_chat(conn, user["id"])
        conn.commit()
        conn.close()

        return jsonify({
            "response": bot_response,
            "remaining": remaining_after(used),
            "conversation_id": conversation_id,
            "title": title,
        })

    # Guest.
    if session.get("guest_chats", 0) >= FREE_GUEST_CHATS:
        return jsonify({
            "error": "requires_auth",
            "message": "Your free chat is used up. Sign in to keep chatting!",
            "daily_limit": DAILY_CHAT_LIMIT,
        }), 401

    history = sanitize_conversation(data.get("conversation") or data.get("history"))
    messages = build_model_messages(user_message, history, "")

    try:
        completion = client.chat.completions.create(model=MODEL, messages=messages)
        bot_response = completion.choices[0].message.content or "⚠️ AadidevGPT-0 returned an empty reply. Please try again."
    except Exception as e:
        print("Groq API error:", str(e))
        return jsonify({
            "error": "⚠️ AadidevGPT-0 is having trouble connecting to the AI service right now."
        }), 500

    session["guest_chats"] = session.get("guest_chats", 0) + 1
    return jsonify({
        "response": bot_response,
        "remaining": max(0, FREE_GUEST_CHATS - session["guest_chats"]),
        "conversation_id": None,
        "title": None,
    })


def _sse(obj):
    return "data: " + json.dumps(obj) + "\n\n"


@app.route("/chat/stream", methods=["POST"])
def chat_stream():
    data = request.get_json(silent=True) or {}

    if not data:
        return jsonify({"error": "Bad request."}), 400

    if client is None:
        return jsonify({"error": "AadidevGPT-0 is not configured yet: the server is missing its GROQ_API_KEY."}), 503

    if _rate_limited(_rate_key() + ":chat", 30, 60):
        return jsonify({"error": "You're sending messages too fast. Please slow down."}), 429

    user = get_current_user()
    logged_in = user is not None
    temporary = bool(data.get("temporary"))
    mode = data.get("mode") or "general"

    if temporary:
        if logged_in:
            if DAILY_CHAT_LIMIT > 0 and (daily_remaining(user) or 0) <= 0:
                return jsonify({
                    "error": "limit_reached",
                    "message": "You've used all " + str(DAILY_CHAT_LIMIT) + " of your chats for today. Come back tomorrow!",
                }), 403
         
            conn = get_db()
            _consume_chat(conn, user["id"])
            conn.commit()
            conn.close()
            remaining = daily_remaining(user)
            instructions = user["custom_instructions"] or ""
            memory = user["memory"] or ""
            return _stream_in_memory(data, instructions, memory, mode, remaining,
                                     user["project_name"], user["project_desc"])


        return _stream_guest(data)

    if logged_in:
        ok, payload = _resolve_context(user, data)
        if not ok:
            return jsonify(payload[1]), payload[0]
        conn, conversation_id, prompt, model_history, title, used = payload
        conn.commit()  

        model_messages = build_model_messages(
            prompt, model_history, user["custom_instructions"], user["memory"],
            mode, user["project_name"], user["project_desc"],
        )

        def generate():
            completed = False
            parts = []
            tokens = 0
            stream = None
            try:
                try:
                    stream = client.chat.completions.create(
                        model=MODEL,
                        messages=model_messages,
                        stream=True,
                        stream_options={"include_usage": True},
                    )
                except Exception:
                
                    stream = client.chat.completions.create(
                        model=MODEL, messages=model_messages, stream=True
                    )

                for chunk in stream:
                    choices = getattr(chunk, "choices", None) or []
                    usage = getattr(chunk, "usage", None)
                    if usage:
                        tokens = usage.total_tokens or 0
                    if not choices:
                        continue
                    delta = choices[0].delta
                    if delta is None:
                        continue
                    content = getattr(delta, "content", None)
                    if content:
                        parts.append(content)
                        yield _sse({"delta": content})

                text = "".join(parts).strip()
                if text:
                    msg_id, created_at = _insert_assistant(conn, conversation_id, text, tokens)
                    _consume_chat(conn, user["id"])
                    conn.commit()
                    completed = True
                    yield _sse({
                        "done": True,
                        "message_id": msg_id,
                        "created_at": created_at,
                        "conversation_id": conversation_id,
                        "title": title,
                        "remaining": remaining_after(used),
                        "tokens": tokens,
                    })
                else:
                    yield _sse({"error": "AadidevGPT-0 returned an empty reply. Please try again."})
            except Exception as e:
                print("Groq stream error:", str(e))
                if parts:
                    yield _sse({"error": "The reply was interrupted."})
            finally:
                if stream is not None:
                    try:
                        stream.close()
                    except Exception:
                        pass
                if not completed:
                    partial = "".join(parts).strip()
                    if partial:
                        _insert_assistant(conn, conversation_id, partial, 0)
                        _consume_chat(conn, user["id"])
                        try:
                            conn.commit()
                        except Exception:
                            pass
                    else:
                        conn.rollback() 
                try:
                    conn.close()
                except Exception:
                    pass

    else:
        return _stream_guest(data)

    response = Response(stream_with_context(generate()), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Connection"] = "keep-alive"
    response.headers["X-Accel-Buffering"] = "no"
    return response


def _stream_guest(data):
    """Guest chat: in-memory history only, no DB writes."""
    if session.get("guest_chats", 0) >= FREE_GUEST_CHATS:
        return jsonify({
            "error": "requires_auth",
            "message": "Your free chat is used up. Sign in to keep chatting!",
            "daily_limit": DAILY_CHAT_LIMIT,
        }), 401

    user_message = (data.get("message") or "").strip()
    if not user_message:
        return jsonify({"error": "Please enter a message."}), 400

    history = sanitize_conversation(data.get("conversation") or data.get("history"))
    model_messages = build_model_messages(user_message, history, "")


    session["guest_chats"] = session.get("guest_chats", 0) + 1

    def generate():
        parts = []
        tokens = 0
        stream = None
        try:
            try:
                stream = client.chat.completions.create(
                    model=MODEL,
                    messages=model_messages,
                    stream=True,
                    stream_options={"include_usage": True},
                )
            except Exception:
                stream = client.chat.completions.create(
                    model=MODEL, messages=model_messages, stream=True
                )

            for chunk in stream:
                choices = getattr(chunk, "choices", None) or []
                usage = getattr(chunk, "usage", None)
                if usage:
                    tokens = usage.total_tokens or 0
                if not choices:
                    continue
                delta = choices[0].delta
                if delta is None:
                    continue
                content = getattr(delta, "content", None)
                if content:
                    parts.append(content)
                    yield _sse({"delta": content})

            text = "".join(parts).strip()
            if text:
                yield _sse({
                    "done": True,
                    "message_id": None,
                    "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "conversation_id": None,
                    "title": None,
                    "remaining": max(0, FREE_GUEST_CHATS - session["guest_chats"]),
                    "tokens": tokens,
                })
            else:
                yield _sse({"error": "AadidevGPT-0 returned an empty reply. Please try again."})
        except GeneratorExit:
            pass
        except Exception as e:
            print("Groq stream error:", str(e))
            yield _sse({"error": "The reply was interrupted. Please try again."})
        finally:
            if stream is not None:
                try:
                    stream.close()
                except Exception:
                    pass

    response = Response(stream_with_context(generate()), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Connection"] = "keep-alive"
    response.headers["X-Accel-Buffering"] = "no"
    return response


def _stream_in_memory(data, instructions, memory, mode, remaining, project_name="", project_desc=""):
    """Temporary chat for a signed-in user: streams but saves nothing."""
    user_message = (data.get("message") or "").strip()
    if not user_message:
        return jsonify({"error": "Please enter a message."}), 400

    history = sanitize_conversation(data.get("conversation") or data.get("history"))
    model_messages = build_model_messages(user_message, history, instructions, memory, mode, project_name, project_desc)

    def generate():
        parts = []
        tokens = 0
        stream = None
        try:
            try:
                stream = client.chat.completions.create(
                    model=MODEL,
                    messages=model_messages,
                    stream=True,
                    stream_options={"include_usage": True},
                )
            except Exception:
                stream = client.chat.completions.create(
                    model=MODEL, messages=model_messages, stream=True
                )

            for chunk in stream:
                choices = getattr(chunk, "choices", None) or []
                usage = getattr(chunk, "usage", None)
                if usage:
                    tokens = usage.total_tokens or 0
                if not choices:
                    continue
                delta = choices[0].delta
                if delta is None:
                    continue
                content = getattr(delta, "content", None)
                if content:
                    parts.append(content)
                    yield _sse({"delta": content})

            text = "".join(parts).strip()
            if text:
                yield _sse({
                    "done": True,
                    "message_id": None,
                    "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "conversation_id": None,
                    "title": None,
                    "remaining": remaining,
                    "tokens": tokens,
                })
            else:
                yield _sse({"error": "AadidevGPT-0 returned an empty reply. Please try again."})
        except GeneratorExit:
            pass
        except Exception as e:
            print("Groq stream error:", str(e))
            yield _sse({"error": "The reply was interrupted. Please try again."})
        finally:
            if stream is not None:
                try:
                    stream.close()
                except Exception:
                    pass

    response = Response(stream_with_context(generate()), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Connection"] = "keep-alive"
    response.headers["X-Accel-Buffering"] = "no"
    return response


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=True,
    )
