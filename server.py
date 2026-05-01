#!/usr/bin/env python3
"""
Dudarev Motorsport — Таск-менеджер v4
Сообщения между пользователями, журнал активности, роли (admin/head/member),
прямые сообщения (messenger), аналитика и геймификация
"""

# -- deadline-pg-fix applied
import http.server
import json
import psycopg
# log-quiet-v1: заглушаем 'rolling back returned connection' INTRANS-warnings
import logging as _logging_quiet
_logging_quiet.getLogger('psycopg.pool').setLevel(_logging_quiet.ERROR)

from psycopg.rows import dict_row as _dict_row
import hashlib
import secrets
import time
import os
import mimetypes
import urllib.parse
import re
from datetime import datetime, timedelta, date

# Путь до БД: приоритет у env-переменной DB_PATH (Render Persistent Disk),
# fallback — локальный файл рядом со скриптом (для разработки).
DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "dm_tasks.db")
# Если DB_PATH указывает на директорию, которой ещё нет — создаём.
try:
    _db_dir = os.path.dirname(DB_PATH)
    if _db_dir and not os.path.exists(_db_dir):
        os.makedirs(_db_dir, exist_ok=True)
except Exception as _e:
    print("warn: не удалось создать директорию для БД:", _e)
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")

sessions = {}
user_last_seen = {}  # {user_id: timestamp}
typing_status = {}  # {typing_key: timestamp}
# Rate-limit: {ip: [timestamp, timestamp, ...]} — только неудачные попытки логина
login_attempts = {}
LOGIN_MAX_ATTEMPTS = 5       # попыток
LOGIN_WINDOW_SECONDS = 300   # окно 5 минут
# Rate-limit: {ip: [timestamp, timestamp, ...]} — только неудачные попытки логина
login_attempts = {}
LOGIN_MAX_ATTEMPTS = 5       # попыток
LOGIN_WINDOW_SECONDS = 300   # окно 5 минут

# -- round2v2-applied
def _safe_int(s, default=None):
    """Безопасное преобразование в int. None/bad → default."""
    if s is None: return default
    try: return int(s)
    except (ValueError, TypeError): return default


# -- rsw-v2 --
def _auto_status_for_task(data):
    """Если у задачи есть assigned_to, по умолчанию статус 'in_progress'."""
    s = (data.get("status") or "").strip()
    if s and s != "new":
        return s
    if data.get("assigned_to"):
        return "in_progress"
    return "new"


def _add_admin_watchers_for_head_self(conn, task_id, creator_id, title):
    """#4: если creator (head) поставил задачу СЕБЕ, добавить admin'ов в watchers."""
    try:
        u_role = conn.execute("SELECT role FROM users WHERE id=%s", (creator_id,)).fetchone()
        if not u_role or u_role.get("role") != "head":
            return
        admins = conn.execute("SELECT id FROM users WHERE role='admin'").fetchall()
        for a in admins:
            if a["id"] == creator_id:
                continue
            try:
                conn.execute("INSERT INTO task_watchers (task_id, user_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                             (task_id, a["id"]))
                conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (%s,%s,%s,%s)",
                             (a["id"], task_id, "watcher_added", f"Глава отдела поставил задачу себе: {title}"))
            except Exception: pass
    except Exception as e:
        print(f"_add_admin_watchers fail: {e}")


# -- rsw-v3 --
def hash_password(password, salt=None):
    if salt is None:
        salt = secrets.token_hex(16)
    hashed = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000)
    return salt + ":" + hashed.hex()

def verify_password(password, stored):
    salt = stored.split(":")[0]
    return hash_password(password, salt) == stored

_HTML_TAG_RE = re.compile(r'<[^>]+>')

def _sanitize_text(s):
    """Вырезать HTML-теги из пользовательского текста.
    Защита от XSS — даже если фронт где-то забыл экранировать.
    Оставляет одиночные < и > (например, в тексте "приоритет > 5").
    """
    if not s or not isinstance(s, str):
        return s
    return _HTML_TAG_RE.sub('', s)

# auto-watcher-v4: helper для назначения наблюдателя по умолчанию
def _ensure_auto_watcher(conn, task_id, creator_id, dept_id):
    """Глава отдела если есть и не сам создатель, иначе — Дударев."""
    try:
        if not task_id or not creator_id:
            return
        watcher_id = None
        if dept_id:
            row = conn.execute(
                "SELECT id FROM users WHERE department_id=%s AND role='head' AND id != %s AND COALESCE(is_active, TRUE) = TRUE ORDER BY id LIMIT 1",
                (dept_id, creator_id)
            ).fetchone()
            if row:
                watcher_id = row["id"]
        if not watcher_id:
            row = conn.execute(
                "SELECT id FROM users WHERE username='dudarev' AND id != %s LIMIT 1",
                (creator_id,)
            ).fetchone()
            if row:
                watcher_id = row["id"]
        if watcher_id:
            conn.execute(
                "INSERT INTO task_watchers (task_id, user_id) VALUES (%s, %s) ON CONFLICT (task_id, user_id) DO NOTHING",
                (task_id, watcher_id)
            )
    except Exception as _awe:
        print(f"[auto-watcher-v4] {_awe}")


# ratelimit-v1: защита /api/login от перебора
_RL_LOGIN_FAILS = {}  # ip -> [count, last_ts, lock_until_ts]
_RL_MAX_FAILS = 5
_RL_LOCK_SEC = 900       # 15 минут блок
_RL_RESET_SEC = 900      # 15 минут окно сброса счётчика

def _rl_check(ip):
    """Возвращает (allowed, retry_after_sec)."""
    rec = _RL_LOGIN_FAILS.get(ip)
    if not rec: return True, 0
    now = time.time()
    if rec[2] > now: return False, int(rec[2] - now)
    return True, 0

def _rl_fail(ip):
    now = time.time()
    rec = _RL_LOGIN_FAILS.get(ip, [0, 0.0, 0.0])
    if now - rec[1] > _RL_RESET_SEC:
        rec = [0, 0.0, 0.0]
    rec[0] += 1
    rec[1] = now
    if rec[0] >= _RL_MAX_FAILS:
        rec[2] = now + _RL_LOCK_SEC
    _RL_LOGIN_FAILS[ip] = rec

def _rl_ok(ip):
    _RL_LOGIN_FAILS.pop(ip, None)


class _SqliteCompatRow:
    """Строка с доступом и по имени колонки (row["col"]), и по индексу (row[0]).
    Имитирует sqlite3.Row, чтобы старый код работал без правок."""
    __slots__ = ("_d", "_v")
    def __init__(self, d, v):
        self._d = d
        self._v = v
    def __getitem__(self, k):
        if isinstance(k, int):
            return self._v[k]
        if k in self._d:
            return self._d[k]
        raise KeyError(k)
    def __contains__(self, k):
        return k in self._d
    def __iter__(self):
        return iter(self._v)
    def __len__(self):
        return len(self._v)
    def get(self, k, default=None):
        return self._d.get(k, default) if not isinstance(k, int) else (self._v[k] if 0 <= k < len(self._v) else default)
    def keys(self):
        return self._d.keys()
    def values(self):
        return self._v
    def items(self):
        return self._d.items()


def _sqlite_compat_row_factory(cursor):
    desc = cursor.description
    cols = [d.name for d in desc] if desc else []
    def make(values):
        return _SqliteCompatRow(dict(zip(cols, values)), values)
    return make


# Connection pool — переиспользуем соединения, не открываем новое на каждый запрос.
# Без пула каждое действие пользователя = TCP+TLS handshake к Neon (~200-400 ms).
from psycopg_pool import ConnectionPool as _ConnectionPool

_pg_pool = None

def _get_pg_pool():
    global _pg_pool
    if _pg_pool is not None:
        return _pg_pool
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL не задан. Укажи строку подключения Postgres (например, из Neon).")
    _pg_pool = _ConnectionPool(
        dsn,
        min_size=1,
        max_size=10,
        kwargs={"row_factory": _sqlite_compat_row_factory, "autocommit": False},
        # open позже (lazy), чтобы не падать при импорте если DSN невалиден
        open=True,
    )
    return _pg_pool


def get_db():
    """Возвращает соединение из пула. conn.close() вернёт обратно в пул."""
    pool = _get_pg_pool()
    conn = pool.getconn()
    # Обернём close: когда код вызывает conn.close(), соединение возвращается в пул,
    # а не реально закрывается. Это позволяет не трогать остальной код server.py.
    _orig_close = conn.close
    def _pool_release():
        try:
            pool.putconn(conn)
        except Exception:
            # Если пул не принял (например, соединение сломано) — реально закрываем
            try: _orig_close()
            except Exception: pass
    conn.close = _pool_release
    return conn

def load_sessions_from_db():
    """Загрузить все активные сессии из БД в память при старте сервера."""
    global sessions
    try:
        conn = get_db()
        rows = conn.execute("SELECT token, user_id, username, role FROM sessions_db").fetchall()
        sessions.clear()
        for r in rows:
            sessions[r["token"]] = {"id": r["user_id"], "username": r["username"], "role": r["role"]}
        conn.close()
        print(f"Loaded {len(sessions)} sessions from DB")
    except Exception as e:
        print(f"load_sessions_from_db: {e}")

def save_session_to_db(token, user_id, username, role):
    try:
        conn = get_db()
        conn.execute("INSERT INTO sessions_db (token, user_id, username, role) VALUES (%s,%s,%s,%s) ON CONFLICT (token) DO UPDATE SET user_id=EXCLUDED.user_id, username=EXCLUDED.username, role=EXCLUDED.role",
                     (token, user_id, username, role))
        conn.commit(); conn.close()
    except Exception as e:
        print(f"save_session_to_db: {e}")

def delete_session_from_db(token):
    try:
        conn = get_db()
        conn.execute("DELETE FROM sessions_db WHERE token=%s", (token,))
        conn.commit(); conn.close()
    except Exception as e:
        print(f"delete_session_from_db: {e}")

def _check_login_rate_limit(ip):
    """Возвращает True если запрос разрешён, False если превышен лимит."""
    import time
    now = time.time()
    attempts = login_attempts.get(ip, [])
    # Оставить только попытки в окне
    attempts = [t for t in attempts if now - t < LOGIN_WINDOW_SECONDS]
    login_attempts[ip] = attempts
    return len(attempts) < LOGIN_MAX_ATTEMPTS

def _record_login_failure(ip):
    import time
    login_attempts.setdefault(ip, []).append(time.time())

def _clear_login_attempts(ip):
    login_attempts.pop(ip, None)

SESSION_TTL_DAYS = 30  # Сессия живёт 30 дней с момента создания

def load_sessions_from_db():
    """Загрузить все активные сессии из БД в память при старте сервера.
    Просроченные (>SESSION_TTL_DAYS) — чистим автоматически."""
    global sessions
    try:
        conn = get_db()
        # Миграция: добавить created_at если нет
        try:
            conn.execute("SELECT created_at FROM sessions_db LIMIT 1")
        except Exception:
            try: conn.execute("ALTER TABLE sessions_db ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            except Exception: pass
            conn.commit()
        # Удалить просроченные
        try:
            conn.execute(
                "DELETE FROM sessions_db WHERE created_at IS NOT NULL "
                "AND datetime(created_at, '+' || %s || ' days') < datetime('now')",
                (SESSION_TTL_DAYS,)
            )
            conn.commit()
        except Exception as _e:
            print(f"[sessions] cleanup error: {_e}")
        rows = conn.execute("SELECT token, user_id, username, role FROM sessions_db").fetchall()
        sessions.clear()
        for r in rows:
            sessions[r["token"]] = {"id": r["user_id"], "username": r["username"], "role": r["role"]}
        conn.close()
        print(f"Loaded {len(sessions)} sessions from DB")
    except Exception as e:
        print(f"load_sessions_from_db: {e}")

def save_session_to_db(token, user_id, username, role):
    try:
        conn = get_db()
        conn.execute("INSERT INTO sessions_db (token, user_id, username, role) VALUES (%s,%s,%s,%s) ON CONFLICT (token) DO UPDATE SET user_id=EXCLUDED.user_id, username=EXCLUDED.username, role=EXCLUDED.role",
                     (token, user_id, username, role))
        conn.commit(); conn.close()
    except Exception as e:
        print(f"save_session_to_db: {e}")

def delete_session_from_db(token):
    try:
        conn = get_db()
        conn.execute("DELETE FROM sessions_db WHERE token=%s", (token,))
        conn.commit(); conn.close()
    except Exception as e:
        print(f"delete_session_from_db: {e}")

def _check_login_rate_limit(ip):
    """Возвращает True если запрос разрешён, False если превышен лимит."""
    import time
    now = time.time()
    attempts = login_attempts.get(ip, [])
    # Оставить только попытки в окне
    attempts = [t for t in attempts if now - t < LOGIN_WINDOW_SECONDS]
    login_attempts[ip] = attempts
    return len(attempts) < LOGIN_MAX_ATTEMPTS

def _record_login_failure(ip):
    import time
    login_attempts.setdefault(ip, []).append(time.time())

def _clear_login_attempts(ip):
    login_attempts.pop(ip, None)

def _run_ddl(cur, sql):
    """Запустить multi-statement DDL через отдельные execute'ы.
    psycopg v3 в extended protocol НЕ выполняет multi-statement в одном execute.
    Разбиваем по ';' и гоняем по одному. Пустые и комментарии пропускаем.
    """
    # Убираем однострочные -- комментарии, чтобы не мешали split'у по ;
    cleaned = re.sub(r'--[^\n]*', '', sql)
    for stmt in cleaned.split(';'):
        s = stmt.strip()
        if s:
            cur.execute(s)


def init_db():
    conn = get_db()
    conn.autocommit = True  # init_db: каждая команда сама по себе, чтобы один упавший SELECT не ломал всю транзакцию
    c = conn.cursor()
    _run_ddl(c, """
    CREATE TABLE IF NOT EXISTS departments (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        head_name TEXT,
        color TEXT DEFAULT '#1a1a1a',
        head_user_id INTEGER
    );
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        full_name TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        department_id INTEGER,
        role TEXT DEFAULT 'member',
        avatar_color TEXT DEFAULT '#1a1a1a',
        onboarding_done INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (department_id) REFERENCES departments(id)
    );
    CREATE TABLE IF NOT EXISTS tasks (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        description TEXT DEFAULT '',
        status TEXT DEFAULT 'new',
        priority TEXT DEFAULT 'medium',
        created_by INTEGER NOT NULL,
        assigned_to INTEGER,
        department_id INTEGER,
        deadline TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (created_by) REFERENCES users(id),
        FOREIGN KEY (assigned_to) REFERENCES users(id),
        FOREIGN KEY (department_id) REFERENCES departments(id)
    );
    CREATE TABLE IF NOT EXISTS task_watchers (
        id SERIAL PRIMARY KEY,
        task_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(id),
        UNIQUE(task_id, user_id)
    );
    CREATE TABLE IF NOT EXISTS task_coexecutors (
        id SERIAL PRIMARY KEY,
        task_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(id),
        UNIQUE(task_id, user_id)
    );
    CREATE TABLE IF NOT EXISTS comments (
        id SERIAL PRIMARY KEY,
        task_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS notifications (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        task_id INTEGER,
        type TEXT NOT NULL,
        message TEXT NOT NULL,
        is_read INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS task_messages (
        id SERIAL PRIMARY KEY,
        task_id INTEGER NOT NULL,
        sender_id INTEGER NOT NULL,
        recipient_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        is_read INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (sender_id) REFERENCES users(id),
        FOREIGN KEY (recipient_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS task_activity (
        id SERIAL PRIMARY KEY,
        task_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        details TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS direct_messages (
        id SERIAL PRIMARY KEY,
        sender_id INTEGER NOT NULL,
        recipient_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        is_read INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (sender_id) REFERENCES users(id),
        FOREIGN KEY (recipient_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS user_stats (
        user_id INTEGER UNIQUE NOT NULL,
        total_km REAL DEFAULT 0,
        level TEXT DEFAULT 'Босоногий',
        tasks_completed INTEGER DEFAULT 0,
        tasks_created INTEGER DEFAULT 0,
        comments_count INTEGER DEFAULT 0,
        streak_days INTEGER DEFAULT 0,
        last_active DATE,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS achievements (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        type TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT NOT NULL,
        icon TEXT,
        earned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        UNIQUE(user_id, type)
    );
    CREATE TABLE IF NOT EXISTS feedback (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        rating INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS group_chats (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        created_by INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        avatar_color TEXT DEFAULT '#6366f1'
    );
    CREATE TABLE IF NOT EXISTS group_chat_members (
        group_id INTEGER,
        user_id INTEGER,
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (group_id, user_id)
    );
    CREATE TABLE IF NOT EXISTS group_messages (
        id SERIAL PRIMARY KEY,
        group_id INTEGER NOT NULL,
        sender_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Migrate: add onboarding_done column if missing
    c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS onboarding_done INTEGER DEFAULT 0")

    # Migrate: add admin_onboarding_done column if missing
    c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS admin_onboarding_done INTEGER DEFAULT 0")

    # Fix: mark existing admin users as onboarding complete
    c.execute("UPDATE users SET onboarding_done=1, admin_onboarding_done=1 WHERE role='admin' AND admin_onboarding_done=0")

    # Migrate: add role column if missing
    c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS role TEXT DEFAULT 'member'")

    # Migrate: add parent_task_id column to tasks (for subtasks)
    c.execute("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS parent_task_id INTEGER DEFAULT NULL")

    # Migrate: add sort_order column to tasks (for subtask reordering)
    c.execute("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sort_order INTEGER DEFAULT 0")

    # Migrate: persistent sessions table (survives server restart)
    try:
        c.execute("SELECT token FROM sessions_db LIMIT 1")
    except:
        c.execute("""CREATE TABLE IF NOT EXISTS sessions_db (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

    # Migrate: persistent sessions table (survives server restart)
    try:
        c.execute("SELECT token FROM sessions_db LIMIT 1")
    except:
        c.execute("""CREATE TABLE IF NOT EXISTS sessions_db (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

    # Migrate: add attachment columns to comments (files, voice)
    c.execute("ALTER TABLE comments ADD COLUMN IF NOT EXISTS attachment_data TEXT DEFAULT NULL")
    c.execute("ALTER TABLE comments ADD COLUMN IF NOT EXISTS attachment_name TEXT DEFAULT NULL")
    c.execute("ALTER TABLE comments ADD COLUMN IF NOT EXISTS attachment_type TEXT DEFAULT NULL")
    # Migrate: add edited_at to comments (for "(изменено)" badge)
    try:
        c.execute("SELECT edited_at FROM comments LIMIT 1")
    except Exception:
        try: c.execute("ALTER TABLE comments ADD COLUMN edited_at TIMESTAMP DEFAULT NULL")
        except: pass
    # Migrate: task_reads — per-user last-read marker per task (for unread badge in chat)
    try:
        c.execute("SELECT task_id FROM task_reads LIMIT 1")
    except Exception:
        c.execute("""CREATE TABLE IF NOT EXISTS task_reads (
            task_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            last_read_comment_id INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (task_id, user_id)
        )""")
    # Migrate: add edited_at to comments (for "(изменено)" badge)
    try:
        c.execute("SELECT edited_at FROM comments LIMIT 1")
    except Exception:
        try: c.execute("ALTER TABLE comments ADD COLUMN edited_at TIMESTAMP DEFAULT NULL")
        except: pass
    # Migrate: task_reads — per-user last-read marker per task (for unread badge in chat)
    try:
        c.execute("SELECT task_id FROM task_reads LIMIT 1")
    except Exception:
        c.execute("""CREATE TABLE IF NOT EXISTS task_reads (
            task_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            last_read_comment_id INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (task_id, user_id)
        )""")

    # Migrate: create task_messages table if missing
    c.execute("""
    CREATE TABLE IF NOT EXISTS task_messages (
        id SERIAL PRIMARY KEY,
        task_id INTEGER NOT NULL,
        sender_id INTEGER NOT NULL,
        recipient_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        is_read INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (sender_id) REFERENCES users(id),
        FOREIGN KEY (recipient_id) REFERENCES users(id)
    );
    """)

    # Migrate: create task_activity table if missing
    c.execute("""
    CREATE TABLE IF NOT EXISTS task_activity (
        id SERIAL PRIMARY KEY,
        task_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        details TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(id)
    );
    """)

    # Migrate: create direct_messages table if missing
    c.execute("""
    CREATE TABLE IF NOT EXISTS direct_messages (
        id SERIAL PRIMARY KEY,
        sender_id INTEGER NOT NULL,
        recipient_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        is_read INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (sender_id) REFERENCES users(id),
        FOREIGN KEY (recipient_id) REFERENCES users(id)
    );
    """)

    # Migrate: create user_stats table if missing
    c.execute("""
    CREATE TABLE IF NOT EXISTS user_stats (
        user_id INTEGER UNIQUE NOT NULL,
        total_km REAL DEFAULT 0,
        level TEXT DEFAULT 'Босоногий',
        tasks_completed INTEGER DEFAULT 0,
        tasks_created INTEGER DEFAULT 0,
        comments_count INTEGER DEFAULT 0,
        streak_days INTEGER DEFAULT 0,
        last_active DATE,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)

    # Migrate: create achievements table if missing
    c.execute("""
    CREATE TABLE IF NOT EXISTS achievements (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        type TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT NOT NULL,
        icon TEXT,
        earned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        UNIQUE(user_id, type)
    );
    """)

    # Migrate: add car_override column to user_stats if missing
    c.execute("ALTER TABLE user_stats ADD COLUMN IF NOT EXISTS car_override TEXT DEFAULT ''")

    # Migrate: add switch_car permission if missing (table may not exist yet on fresh DB)
    try:
        existing_perms = [r[0] for r in c.execute("SELECT DISTINCT permission FROM role_permissions").fetchall()]
        if 'switch_car' not in existing_perms:
            c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES ('admin', 'switch_car', 1)")
            c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES ('head', 'switch_car', 0)")
            c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES ('member', 'switch_car', 0)")

        # Migrate: add new permissions if missing
        for perm, admin_val in [('manage_kanban', 1), ('view_all_departments', 1), ('view_feedback', 1)]:
            if perm not in existing_perms:
                c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES ('admin', %s, %s)", (perm, admin_val))
                c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES ('head', %s, 0)", (perm,))
                c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES ('member', %s, 0)", (perm,))
    except Exception:
        pass  # table will be created below and seeded with defaults

    # Migrate: add head_user_id column to departments if missing
    c.execute("ALTER TABLE departments ADD COLUMN IF NOT EXISTS head_user_id INTEGER")

    # Migrate: add avatar_url column if missing
    c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url TEXT DEFAULT ''")

    # Migrate: create group_chats table if missing
    _run_ddl(c, """
    CREATE TABLE IF NOT EXISTS group_chats (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        created_by INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        avatar_color TEXT DEFAULT '#6366f1'
    );
    CREATE TABLE IF NOT EXISTS group_chat_members (
        group_id INTEGER,
        user_id INTEGER,
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (group_id, user_id)
    );
    CREATE TABLE IF NOT EXISTS group_messages (
        id SERIAL PRIMARY KEY,
        group_id INTEGER NOT NULL,
        sender_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Migrate: add old_value and new_value columns to task_activity if missing
    c.execute("ALTER TABLE task_activity ADD COLUMN IF NOT EXISTS old_value TEXT")
    c.execute("ALTER TABLE task_activity ADD COLUMN IF NOT EXISTS new_value TEXT")

    # Create role_permissions table
    c.execute("""
    CREATE TABLE IF NOT EXISTS role_permissions (
        id SERIAL PRIMARY KEY,
        role TEXT NOT NULL,
        permission TEXT NOT NULL,
        allowed INTEGER DEFAULT 1,
        UNIQUE(role, permission)
    )
    """)

    # Seed default permissions if empty
    if c.execute("SELECT COUNT(*) FROM role_permissions").fetchone()[0] == 0:
        default_perms = {
            'admin': ['view_all_tasks','create_tasks','assign_tasks','comments','analytics','manage_users','manage_departments','delete_users','leaderboard','switch_car','manage_kanban','view_all_departments','view_feedback'],
            'head': ['view_all_tasks','create_tasks','assign_tasks','comments','analytics','leaderboard'],
            'member': ['create_tasks','assign_tasks','comments','leaderboard']
        }
        for role, perms in default_perms.items():
            all_perms = ['view_all_tasks','create_tasks','assign_tasks','comments','analytics','manage_users','manage_departments','delete_users','leaderboard','switch_car','manage_kanban','view_all_departments','view_feedback']
            for p in all_perms:
                allowed = 1 if p in perms else 0
                c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES (%s,%s,%s)", (role, p, allowed))

    # Seed departments
    existing = c.execute("SELECT COUNT(*) FROM departments").fetchone()[0]
    if existing == 0:
        for name, head, color in [
            ("Отдел продаж", "Лукьян", "#2563eb"),
            ("Склад", "Александр Дударев", "#059669"),
            ("Технический отдел", None, "#d97706"),
            ("Клуб", "Егор Паршин", "#7c3aed"),
        ]:
            c.execute("INSERT INTO departments (name, head_name, color) VALUES (%s,%s,%s)", (name, head, color))

    # Migrate: create funnel_stages table
    try:
        c.execute("SELECT 1 FROM funnel_stages LIMIT 1")
    except Exception:
        c.execute("""
            CREATE TABLE funnel_stages (
                id SERIAL PRIMARY KEY,
                key TEXT NOT NULL,
                label TEXT NOT NULL,
                color TEXT DEFAULT '#3b82f6',
                icon TEXT DEFAULT '',
                sort_order INTEGER DEFAULT 0,
                department_id INTEGER DEFAULT NULL,
                UNIQUE(key, department_id),
                FOREIGN KEY (department_id) REFERENCES departments(id)
            )
        """)
        # Default stages: Новый → В работе → Готово
        for sort_order, (key, label, color, icon) in enumerate([
            ('new', 'Новые', '#3b82f6', '📋'),
            ('in_progress', 'В работе', '#f59e0b', '⚡'),
            ('done', 'Готово', '#22c55e', '✅'),
        ]):
            c.execute("INSERT INTO funnel_stages (key, label, color, icon, sort_order, department_id) VALUES (%s,%s,%s,%s,%s,%s)",
                       (key, label, color, icon, sort_order, None))
        # Migrate existing tasks from 'review' to 'in_progress'
        c.execute("UPDATE tasks SET status='in_progress' WHERE status='review'")

    # Migrate: add department_id to funnel_stages (idempotent)
    c.execute("ALTER TABLE funnel_stages ADD COLUMN IF NOT EXISTS department_id INTEGER DEFAULT NULL")
    # FK на departments — в Postgres добавим отдельным constraint, если ещё нет.
    try:
        c.execute("ALTER TABLE funnel_stages ADD CONSTRAINT funnel_stages_dept_fk FOREIGN KEY (department_id) REFERENCES departments(id)")
    except Exception:
        pass
    # Copy existing global stages to each department (ON CONFLICT DO NOTHING — идемпотентно)
    depts = c.execute("SELECT id FROM departments").fetchall()
    global_stages = c.execute("SELECT key, label, color, icon, sort_order FROM funnel_stages WHERE department_id IS NULL").fetchall()
    for dept in depts:
        for s in global_stages:
            c.execute("INSERT INTO funnel_stages (key, label, color, icon, sort_order, department_id) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (s[0], s[1], s[2], s[3], s[4], dept[0]))

    # Migrate: fix funnel_stages — rename dept-suffixed keys and fix UNIQUE constraint
    # This fixes the bug where department stages had keys like 'new_dept1' but tasks used 'new'
    try:
        # Check if any dept-suffixed keys exist (e.g. new_dept1, in_progress_dept2)
        bad_keys = c.execute("SELECT id, key, department_id FROM funnel_stages WHERE key LIKE '%\\_dept%' ESCAPE '\\'").fetchall()
        if bad_keys:
            for row in bad_keys:
                # Strip the _deptN suffix: 'new_dept1' -> 'new', 'in_progress_dept2' -> 'in_progress'
                clean_key = re.sub(r'_dept\d+$', '', row[1])
                c.execute("UPDATE funnel_stages SET key=%s WHERE id=%s", (clean_key, row[0]))
            conn.commit()

        # Check if UNIQUE constraint is on (key) alone instead of (key, department_id)
        # by trying to see the table schema
        # В Postgres DDL нет в sqlite_master — смотрим в information_schema.
        # Для funnel_stages проверим: есть ли UNIQUE constraint на (key) без department_id.
        # Простое эмпирическое правило: если в таблице нет UNIQUE constraint вида
        # (key, department_id) — пересобираем.
        res = c.execute("""
            SELECT constraint_name FROM information_schema.table_constraints
            WHERE table_name='funnel_stages' AND constraint_type='UNIQUE'
        """).fetchall()
        # Смотрим какие столбцы включены в UNIQUE-констрейнты
        has_compound_unique = False
        for row in res:
            cn = row[0]
            cols = c.execute("""
                SELECT column_name FROM information_schema.constraint_column_usage
                WHERE constraint_name=%s
            """, (cn,)).fetchall()
            col_names = {r[0] for r in cols}
            if 'key' in col_names and 'department_id' in col_names:
                has_compound_unique = True
                break
        if not has_compound_unique:
            # Recreate table with correct UNIQUE constraint
            c.execute("ALTER TABLE funnel_stages RENAME TO funnel_stages_old")
            c.execute("""
                CREATE TABLE funnel_stages (
                    id SERIAL PRIMARY KEY,
                    key TEXT NOT NULL,
                    label TEXT NOT NULL,
                    color TEXT DEFAULT '#3b82f6',
                    icon TEXT DEFAULT '',
                    sort_order INTEGER DEFAULT 0,
                    department_id INTEGER DEFAULT NULL,
                    UNIQUE(key, department_id),
                    FOREIGN KEY (department_id) REFERENCES departments(id)
                )
            """)
            c.execute("""INSERT INTO funnel_stages (key, label, color, icon, sort_order, department_id)
                         SELECT key, label, color, icon, sort_order, department_id FROM funnel_stages_old""")
            c.execute("DROP TABLE funnel_stages_old")
            conn.commit()

        # Ensure every department has stages (fill missing ones from global)
        depts = c.execute("SELECT id FROM departments").fetchall()
        global_stages = c.execute("SELECT key, label, color, icon, sort_order FROM funnel_stages WHERE department_id IS NULL").fetchall()
        for dept in depts:
            existing = c.execute("SELECT key FROM funnel_stages WHERE department_id=%s", (dept[0],)).fetchall()
            existing_keys = {r[0] for r in existing}
            for s in global_stages:
                if s[0] not in existing_keys:
                    c.execute("INSERT INTO funnel_stages (key, label, color, icon, sort_order, department_id) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                        (s[0], s[1], s[2], s[3], s[4], dept[0]))
        conn.commit()
    except Exception as e:
        print(f"[Migration] funnel_stages fix: {e}")

    # ================================================================
    # Migration v2.6: Categories (Todoist-style) + normalize task.status
    # ================================================================
    # Creates: categories (hierarchical with parent_id), task_categories (M2M join)
    # Normalizes task.status to 3 fixed values: new / in_progress / done.
    # Any tasks with custom statuses get reset to 'new'; 'done' and 'in_progress' preserved.
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                color TEXT DEFAULT '#3b82f6',
                icon TEXT DEFAULT '',
                sort_order INTEGER DEFAULT 0,
                department_id INTEGER DEFAULT NULL,
                parent_id INTEGER DEFAULT NULL,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE CASCADE,
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS task_categories (
                task_id INTEGER NOT NULL,
                category_id INTEGER NOT NULL,
                PRIMARY KEY (task_id, category_id),
                FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
                FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
            )
        """)
        # Normalize task.status to the new fixed 3-value set.
        # Any status that is not one of the three gets converted to 'new'.
        c.execute("UPDATE tasks SET status='new' WHERE status NOT IN ('new', 'in_progress', 'done', 'cancelled')")
        conn.commit()
    except Exception as e:
        print(f"[Migration] categories v2.6: {e}")

    # Migrate: add is_deleted, edited_at to direct_messages
    try:
        c.execute("SELECT is_deleted FROM direct_messages LIMIT 1")
    except:
        try: c.execute("ALTER TABLE direct_messages ADD COLUMN is_deleted INTEGER DEFAULT 0")
        except: pass
    try:
        c.execute("SELECT edited_at FROM direct_messages LIMIT 1")
    except:
        try: c.execute("ALTER TABLE direct_messages ADD COLUMN edited_at TIMESTAMP DEFAULT NULL")
        except: pass
    try:
        c.execute("SELECT reply_to_id FROM direct_messages LIMIT 1")
    except:
        try: c.execute("ALTER TABLE direct_messages ADD COLUMN reply_to_id INTEGER DEFAULT NULL")
        except: pass
    try:
        c.execute("SELECT forwarded_from FROM direct_messages LIMIT 1")
    except:
        try: c.execute("ALTER TABLE direct_messages ADD COLUMN forwarded_from TEXT DEFAULT NULL")
        except: pass

    # Migrate: add is_deleted, edited_at to group_messages
    try:
        c.execute("SELECT is_deleted FROM group_messages LIMIT 1")
    except:
        try: c.execute("ALTER TABLE group_messages ADD COLUMN is_deleted INTEGER DEFAULT 0")
        except: pass
    try:
        c.execute("SELECT edited_at FROM group_messages LIMIT 1")
    except:
        try: c.execute("ALTER TABLE group_messages ADD COLUMN edited_at TIMESTAMP DEFAULT NULL")
        except: pass
    try:
        c.execute("SELECT reply_to_id FROM group_messages LIMIT 1")
    except:
        try: c.execute("ALTER TABLE group_messages ADD COLUMN reply_to_id INTEGER DEFAULT NULL")
        except: pass

    # Migrate: create message_reactions table
    c.execute("""CREATE TABLE IF NOT EXISTS message_reactions (
        id SERIAL PRIMARY KEY,
        message_type TEXT NOT NULL,
        message_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        emoji TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(message_type, message_id, user_id, emoji)
    )""")

    # Migrate: create pinned_messages table
    c.execute("""CREATE TABLE IF NOT EXISTS pinned_messages (
        id SERIAL PRIMARY KEY,
        message_type TEXT NOT NULL,
        message_id INTEGER NOT NULL,
        chat_type TEXT NOT NULL,
        chat_id TEXT NOT NULL,
        pinned_by INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Migrate: add taken_at column to tasks (tracks when task was taken into work)
    c.execute("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS taken_at TIMESTAMP DEFAULT NULL")

    # Migrate: add completed_at column to tasks (tracks when task was completed)
    c.execute("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP DEFAULT NULL")

    # Migrate: add version column to tasks (optimistic locking).
    # Каждый UPDATE инкрементит version; PUT /api/tasks/{id} проверяет if_version.
    c.execute("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 0")

    # Migrate: add km_awarded flag to tasks (чтобы не начислять km дважды
    # при последовательности done → new → done)
    c.execute("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS km_awarded INTEGER DEFAULT 0")

    # Seed admin
    if c.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
        c.execute("INSERT INTO users (username, full_name, password_hash, role, avatar_color, onboarding_done, admin_onboarding_done) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            ("admin", "Администратор", hash_password("admin123"), "admin", "#1a1a1a", 1, 1))

    # Seed команды DM: 6 аккаунтов, идемпотентно (INSERT OR IGNORE).
    # Пароли: <username>123. Пример: parshin / parshin123
    _dept_id = {}
    for row in c.execute("SELECT id, name FROM departments").fetchall():
        _dept_id[row[1]] = row[0]
    _seed_team = [
        # (username,    full_name,              role,     dept_name,        color)
        ("parshin",     "Егор Паршин",          "head",   "Клуб",           "#7c3aed"),
        ("chistovsky",  "Лукьян Чистовский",    "head",   "Отдел продаж",   "#2563eb"),
        ("dudarev",     "Александр Дударев",    "admin",  None,             "#dc2626"),
        ("serebrov",    "Егор Серебров",        "member", "Склад",          "#059669"),
        ("hripko",      "Николай Хрипко",       "member", "Отдел продаж",   "#0891b2"),
        ("selihin",     "Никита Селихин",       "member", "Отдел продаж",   "#f59e0b"),
    ]
    for username, full_name, role, dept_name, color in _seed_team:
        did = _dept_id.get(dept_name) if dept_name else None
        try:
            c.execute(
                "INSERT INTO users "
                "(username, full_name, password_hash, department_id, role, avatar_color, onboarding_done) "
                "VALUES (%s,%s,%s,%s,%s,%s,1) ON CONFLICT (username) DO NOTHING",
                (username, full_name, hash_password(username + "123"), did, role, color)
            )
        except Exception:
            pass

    # Привязать head_user_id в departments — чтобы "рук отдела" знал кто это
    try:
        for username, full_name, role, dept_name, color in _seed_team:
            if role == "head" and dept_name:
                u_row = c.execute("SELECT id FROM users WHERE username=%s", (username,)).fetchone()
                if u_row:
                    c.execute(
                        "UPDATE departments SET head_user_id=%s WHERE name=%s AND (head_user_id IS NULL OR head_user_id=0)",
                        (u_row[0], dept_name)
                    )
    except Exception:
        pass

    # user_stats для каждого
    for username, *_ in _seed_team:
        try:
            u_row = c.execute("SELECT id FROM users WHERE username=%s", (username,)).fetchone()
            if u_row:
                c.execute("INSERT INTO user_stats (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (u_row[0],))
        except Exception:
            pass

    conn.commit()
    conn.close()

    os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads"), exist_ok=True)

def generate_deadline_notifications():
    """Check for approaching deadlines and create notifications."""
    conn = get_db()
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")
    # Tasks due tomorrow
    tasks = conn.execute(
        "SELECT t.id, t.title, t.assigned_to, t.created_by FROM tasks t "
        "WHERE t.deadline = %s AND t.status NOT IN ('done','cancelled')", (tomorrow,)
    ).fetchall()
    for t in tasks:
        # Собираем всех причастных: исполнитель + автор + наблюдатели + соисполнители
        related = set(filter(None, [t["assigned_to"], t["created_by"]]))
        try:
            for w in conn.execute("SELECT user_id FROM task_watchers WHERE task_id=%s", (t["id"],)).fetchall():
                related.add(w["user_id"])
            for c in conn.execute("SELECT user_id FROM task_coexecutors WHERE task_id=%s", (t["id"],)).fetchall():
                related.add(c["user_id"])
        except Exception: pass
        for uid in related:
            existing = conn.execute(
                "SELECT id FROM notifications WHERE task_id=%s AND user_id=%s AND type='deadline_soon' AND date(created_at)=CURRENT_DATE", (t["id"], uid)
            ).fetchone()
            if not existing:
                conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (%s,%s,%s,%s)",
                    (uid, t["id"], "deadline_soon", f"Дедлайн завтра: {t['title']}"))
    # Overdue tasks
    tasks = conn.execute(
        "SELECT t.id, t.title, t.assigned_to, t.created_by FROM tasks t "
        "WHERE t.deadline < %s AND t.status NOT IN ('done','cancelled')", (today,)
    ).fetchall()
    for t in tasks:
        related = set(filter(None, [t["assigned_to"], t["created_by"]]))
        try:
            for w in conn.execute("SELECT user_id FROM task_watchers WHERE task_id=%s", (t["id"],)).fetchall():
                related.add(w["user_id"])
            for c in conn.execute("SELECT user_id FROM task_coexecutors WHERE task_id=%s", (t["id"],)).fetchall():
                related.add(c["user_id"])
        except Exception: pass
        for uid in related:
            existing = conn.execute(
                "SELECT id FROM notifications WHERE task_id=%s AND user_id=%s AND type='overdue' AND date(created_at)=CURRENT_DATE", (t["id"], uid)
            ).fetchone()
            if not existing:
                conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (%s,%s,%s,%s)",
                    (uid, t["id"], "overdue", f"Просрочена: {t['title']}"))
    conn.commit()
    conn.close()

def _notify_task_people(conn, task_id, message, ntype, exclude_uid=None, extra_uids=None):
    """Создать уведомление для всех, кто связан с задачей:
    assigned_to, created_by, watchers, coexecutors. Минус exclude_uid.
    extra_uids — дополнительно уведомить (например, @-упомянутых).
    """
    try:
        task = conn.execute("SELECT assigned_to, created_by FROM tasks WHERE id=%s", (task_id,)).fetchone()
    except Exception:
        task = None
    notify_ids = set()
    if task:
        if task["assigned_to"]: notify_ids.add(task["assigned_to"])
        if task["created_by"]: notify_ids.add(task["created_by"])
    try:
        for w in conn.execute("SELECT user_id FROM task_watchers WHERE task_id=%s", (task_id,)).fetchall():
            notify_ids.add(w["user_id"])
        for c in conn.execute("SELECT user_id FROM task_coexecutors WHERE task_id=%s", (task_id,)).fetchall():
            notify_ids.add(c["user_id"])
    except Exception:
        pass
    if extra_uids:
        for u in extra_uids:
            if u: notify_ids.add(u)
    if exclude_uid is not None:
        notify_ids.discard(exclude_uid)
    for nid in notify_ids:
        try:
            conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (%s,%s,%s,%s)",
                         (nid, task_id, ntype, message))
        except Exception:
            pass


def _safe_name(row, default="Пользователь"):
    """Безопасно достать full_name из row. Защита от KeyError/None."""
    try:
        if not row: return default
        n = row["full_name"] if "full_name" in row else None
        return n or default
    except Exception:
        return default


def _user_fullname(conn, u, default="Пользователь"):
    """Получить full_name по сессионному u={id,username,role}."""
    try:
        if not u or not u.get("id"):
            return default
        row = conn.execute("SELECT full_name FROM users WHERE id=%s", (u["id"],)).fetchone()
        if not row: return default
        return row["full_name"] if ("full_name" in row) else default
    except Exception:
        return default


def _parse_mentions(text, conn):
    """Найти в тексте @ИмяФамилия и вернуть список user_ids упомянутых."""
    if not text:
        return []
    import re as _re
    # '@Иван Петров' или '@Иван' — имена могут содержать кириллицу/латиницу/_/-/цифры
    # Берём блок после @ до конца имени — жадно матчим continuous тех же символов
    candidates = _re.findall(r"@([A-Za-zЀ-ӿ0-9_-]+(?:\s+[A-Za-zЀ-ӿ0-9_-]+)?)", text)
    if not candidates:
        return []
    ids = []
    try:
        all_users = conn.execute("SELECT id, full_name FROM users").fetchall()
    except Exception:
        return []
    user_by_name = {}
    for u in all_users:
        nm = (_user_fullname(conn, u) or "").strip().lower()
        user_by_name[nm] = u["id"]
        # Первое слово — имя — тоже считаем
        first = nm.split()[0] if nm else ""
        if first and first not in user_by_name:
            user_by_name[first] = u["id"]
    for c in candidates:
        c_low = c.strip().lower()
        if c_low in user_by_name:
            uid = user_by_name[c_low]
            if uid not in ids:
                ids.append(uid)
    return ids


# -- security-mega applied
def _safe_join(base, rel):
    """Защита от path traversal: rel не может выйти за пределы base."""
    try:
        base_abs = os.path.realpath(base)
        # отсечь leading slashes, .. и .
        rel_clean = rel.lstrip("/").replace("\\", "/")
        full = os.path.realpath(os.path.join(base_abs, rel_clean))
        if full != base_abs and not full.startswith(base_abs + os.sep):
            return None
        return full
    except Exception:
        return None


def _sanitize_text(s, maxlen=20000):
    """Вырезать опасные HTML-символы из user-input текста.
    Мягкая санитизация — оставляем < > & в тексте но без экранирования,
    фронт должен escape'ить перед innerHTML. Убираем null-bytes, ограничиваем длину.
    """
    if not isinstance(s, str):
        return s
    s = s.replace("\x00", "")
    if maxlen and len(s) > maxlen:
        s = s[:maxlen]
    return s


def _can_view_task(conn, user, task_id):
    """Может ли пользователь увидеть задачу? admin/head_of_dept/creator/assigned/coexec/watcher."""
    if not user or not user.get("id"):
        return False
    try:
        task = conn.execute("SELECT created_by, assigned_to, department_id FROM tasks WHERE id=%s", (task_id,)).fetchone()
        if not task:
            return False
        u_row = conn.execute("SELECT role, department_id FROM users WHERE id=%s", (user["id"],)).fetchone()
        if not u_row:
            return False
        if u_row["role"] == "admin":
            return True
        if u_row["role"] == "head" and u_row["department_id"] == task["department_id"]:
            return True
        if user["id"] in (task["created_by"], task["assigned_to"]):
            return True
        w = conn.execute("SELECT 1 FROM task_watchers WHERE task_id=%s AND user_id=%s", (task_id, user["id"])).fetchone()
        if w: return True
        c = conn.execute("SELECT 1 FROM task_coexecutors WHERE task_id=%s AND user_id=%s", (task_id, user["id"])).fetchone()
        if c: return True
    except Exception:
        return False
    return False


def _can_edit_task(conn, user, task_id):
    """Кто может ИЗМЕНЯТЬ задачу: admin, head отдела, автор, исполнитель, соисполнитель."""
    if not user or not user.get("id"):
        return False
    try:
        task = conn.execute("SELECT created_by, assigned_to, department_id FROM tasks WHERE id=%s", (task_id,)).fetchone()
        if not task:
            return False
        u_row = conn.execute("SELECT role, department_id FROM users WHERE id=%s", (user["id"],)).fetchone()
        if not u_row:
            return False
        if u_row["role"] == "admin":
            return True
        if u_row["role"] == "head" and u_row["department_id"] == task["department_id"]:
            return True
        if user["id"] in (task["created_by"], task["assigned_to"]):
            return True
        c = conn.execute("SELECT 1 FROM task_coexecutors WHERE task_id=%s AND user_id=%s", (task_id, user["id"])).fetchone()
        if c: return True
    except Exception:
        return False
    return False


def _can_delete_task(conn, user, task_id):
    """Кто может УДАЛИТЬ: только admin, head отдела, или автор задачи."""
    if not user or not user.get("id"):
        return False
    try:
        task = conn.execute("SELECT created_by, department_id FROM tasks WHERE id=%s", (task_id,)).fetchone()
        if not task:
            return False
        u_row = conn.execute("SELECT role, department_id FROM users WHERE id=%s", (user["id"],)).fetchone()
        if not u_row:
            return False
        if u_row["role"] == "admin":
            return True
        if u_row["role"] == "head" and u_row["department_id"] == task["department_id"]:
            return True
        if user["id"] == task["created_by"]:
            return True
    except Exception:
        return False
    return False


def log_activity(conn, task_id, user_id, action, details=None, old_value=None, new_value=None):
    """Log an activity to the task_activity table."""
    conn.execute("INSERT INTO task_activity (task_id, user_id, action, details, old_value, new_value) VALUES (%s,%s,%s,%s,%s,%s)",
        (task_id, user_id, action, details, old_value, new_value))

def _can_access_task(conn, user_id, task_id):
    """Проверка права доступа пользователя к задаче.
    Admin — любая задача. Head — задачи своего отдела + где он участник.
    Member — только где он создатель/исполнитель/наблюдатель/соисполнитель.
    Возвращает (task_row или None, allowed: bool).
    """
    task = conn.execute("SELECT id, created_by, assigned_to, department_id FROM tasks WHERE id=%s", (task_id,)).fetchone()
    if not task:
        return (None, False)
    user_row = conn.execute("SELECT role, department_id FROM users WHERE id=%s", (user_id,)).fetchone()
    if not user_row:
        return (task, False)
    role = user_row["role"]
    if role == "admin":
        return (task, True)
    if role == "head" and user_row["department_id"] == task["department_id"]:
        return (task, True)
    if user_id in (task["created_by"], task["assigned_to"]):
        return (task, True)
    is_watcher = conn.execute("SELECT 1 FROM task_watchers WHERE task_id=%s AND user_id=%s", (task_id, user_id)).fetchone()
    if is_watcher:
        return (task, True)
    is_coexec = conn.execute("SELECT 1 FROM task_coexecutors WHERE task_id=%s AND user_id=%s", (task_id, user_id)).fetchone()
    if is_coexec:
        return (task, True)
    return (task, False)

def calculate_working_hours(start_dt_str, end_dt_str):
    """Calculate working hours between two datetime strings, excluding non-working hours.
    Working hours: Mon-Fri 11:00-19:00 (8 hours), Sat 11:00-17:00 (6 hours), Sun is off.
    Returns a tuple: (total_hours, hours, minutes)
    """
    try:
        start = datetime.fromisoformat(start_dt_str.replace('Z', '+00:00'))
        end = datetime.fromisoformat(end_dt_str.replace('Z', '+00:00'))
    except:
        return 0, 0, 0

    total_minutes = 0
    current = start

    while current < end:
        weekday = current.weekday()  # 0=Mon, 6=Sun
        hour = current.hour

        # Skip non-working days (Sunday=6)
        if weekday == 6:
            current += timedelta(days=1)
            current = current.replace(hour=11, minute=0, second=0)
            continue

        # Define working hours
        if weekday < 5:  # Mon-Fri
            work_start, work_end = 11, 19
        else:  # Saturday
            work_start, work_end = 11, 17

        # Skip non-working hours
        if hour < work_start:
            current = current.replace(hour=work_start, minute=0, second=0)
            continue
        if hour >= work_end:
            current += timedelta(days=1)
            current = current.replace(hour=work_start, minute=0, second=0)
            continue

        # Count 1 minute of working time
        if current < end:
            total_minutes += 1
            current += timedelta(minutes=1)

    hours = total_minutes // 60
    minutes = total_minutes % 60
    return total_minutes / 60.0, hours, minutes

def get_level_from_km(total_km):
    """Calculate level name from total_km.
    ~240-360 km/day at 20 tasks. Progression takes ~3-4 months to Champion.
    """
    if total_km >= 25000:
        return "Чемпион"
    elif total_km >= 18000:
        return "Формула 1"
    elif total_km >= 12000:
        return "Формула 2"
    elif total_km >= 7000:
        return "Формула 3"
    elif total_km >= 3500:
        return "Водитель"
    elif total_km >= 1500:
        return "Байкер"
    elif total_km >= 500:
        return "Моноколёсник"
    elif total_km >= 100:
        return "Самокатчик"
    else:
        return "Босоногий"

def get_next_level(total_km):
    """Calculate next level info from total_km."""
    levels = [
        {"name": "Самокатчик", "km_needed": 100},
        {"name": "Моноколёсник", "km_needed": 500},
        {"name": "Байкер", "km_needed": 1500},
        {"name": "Водитель", "km_needed": 3500},
        {"name": "Формула 3", "km_needed": 7000},
        {"name": "Формула 2", "km_needed": 12000},
        {"name": "Формула 1", "km_needed": 18000},
        {"name": "Чемпион", "km_needed": 25000}
    ]
    for level in levels:
        if total_km < level["km_needed"]:
            return level
    return {"name": "Чемпион", "km_needed": 25000}

def ensure_user_stats(conn, user_id):
    """Ensure a user has a stats row."""
    existing = conn.execute("SELECT user_id FROM user_stats WHERE user_id=%s", (user_id,)).fetchone()
    if not existing:
        conn.execute("INSERT INTO user_stats (user_id, total_km, level, tasks_completed, tasks_created, comments_count, streak_days, last_active) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (user_id, 0, "Босоногий", 0, 0, 0, 0, str(date.today())))

def update_km(conn, user_id, km_amount):
    """Add km to a user's total and update level."""
    ensure_user_stats(conn, user_id)
    conn.execute("UPDATE user_stats SET total_km = total_km + %s WHERE user_id=%s", (km_amount, user_id))
    stats = conn.execute("SELECT total_km FROM user_stats WHERE user_id=%s", (user_id,)).fetchone()
    new_level = get_level_from_km(stats["total_km"])
    conn.execute("UPDATE user_stats SET level=%s WHERE user_id=%s", (new_level, user_id))

def check_and_award_achievements(conn, user_id):
    """Check and award achievements for a user."""
    stats = conn.execute("SELECT * FROM user_stats WHERE user_id=%s", (user_id,)).fetchone()
    if not stats:
        return

    achievements_to_award = []

    def _has(t):
        row = conn.execute("SELECT id FROM achievements WHERE user_id=%s AND type=%s", (user_id, t)).fetchone()
        return bool(row)

    # first_task: complete first task
    if stats["tasks_completed"] == 1 and not _has("first_task"):
        achievements_to_award.append(("first_task", "Первый старт", "Завершили первую задачу"))

    # speed_demon / department_star / early_bird — выдаются в обработчике завершения задачи

    # consistent: 7-day streak
    if stats["streak_days"] >= 7 and not _has("consistent"):
        achievements_to_award.append(("consistent", "Стабильность", "7-дневная активность"))

    # streak_master: 14-day streak (НОВОЕ)
    if stats["streak_days"] >= 14 and not _has("streak_master"):
        achievements_to_award.append(("streak_master", "Железная дисциплина", "14-дневная серия активности"))

    # team_player: create 10 tasks for others
    if stats["tasks_created"] >= 10 and not _has("team_player"):
        achievements_to_award.append(("team_player", "Командный игрок", "Создали 10 задач для других"))

    # social: leave 10 comments
    if stats["comments_count"] >= 10 and not _has("social"):
        achievements_to_award.append(("social", "Общительный", "10+ комментариев"))

    # commentator: leave 50 comments
    if stats["comments_count"] >= 50 and not _has("commentator"):
        achievements_to_award.append(("commentator", "Комментатор", "50+ комментариев"))

    # chat_hero: leave 100 comments (НОВОЕ)
    if stats["comments_count"] >= 100 and not _has("chat_hero"):
        achievements_to_award.append(("chat_hero", "Болтун", "100+ комментариев"))

    # marathon: complete 25 tasks
    if stats["tasks_completed"] >= 25 and not _has("marathon"):
        achievements_to_award.append(("marathon", "Марафон", "25+ задач завершено"))

    # half_century: 50 tasks completed (НОВОЕ)
    if stats["tasks_completed"] >= 50 and not _has("half_century"):
        achievements_to_award.append(("half_century", "Полусотня", "50+ задач завершено"))

    # century: complete 100 tasks
    if stats["tasks_completed"] >= 100 and not _has("century"):
        achievements_to_award.append(("century", "Сотня", "100+ задач завершено"))

    # legend: complete 500 tasks (НОВОЕ)
    if stats["tasks_completed"] >= 500 and not _has("legend"):
        achievements_to_award.append(("legend", "Легенда", "500+ задач завершено"))

    # leader: 500+ km (ФИКС — раньше был только во фронте)
    if (stats["total_km"] or 0) >= 500 and not _has("leader"):
        achievements_to_award.append(("leader", "Лидер", "Набрали 500 км"))

    # perfectionist: 10 tasks completed on-or-before deadline (ФИКС)
    if not _has("perfectionist"):
        ontime = conn.execute(
            "SELECT COUNT(*) as c FROM tasks "
            "WHERE assigned_to=%s AND status='done' AND deadline IS NOT NULL "
            "AND SUBSTRING(updated_at::text, 1, 10) <= deadline",
            (user_id,)
        ).fetchone()
        if ontime and (ontime["c"] or 0) >= 10:
            achievements_to_award.append(("perfectionist", "Перфекционист", "10 задач до дедлайна"))

    # priority_pro: 10 high-priority tasks completed (НОВОЕ)
    if not _has("priority_pro"):
        hp = conn.execute(
            "SELECT COUNT(*) as c FROM tasks "
            "WHERE assigned_to=%s AND status='done' AND priority='high'",
            (user_id,)
        ).fetchone()
        if hp and (hp["c"] or 0) >= 10:
            achievements_to_award.append(("priority_pro", "Тяжеловес", "10 задач высокого приоритета"))

    # weekly_hero: 10 tasks completed in the last 7 days (НОВОЕ)
    if not _has("weekly_hero"):
        wh = conn.execute(
            "SELECT COUNT(*) as c FROM tasks "
            "WHERE assigned_to=%s AND status='done' "
            "AND DATE(updated_at) >= DATE('now','-7 days')",
            (user_id,)
        ).fetchone()
        if wh and (wh["c"] or 0) >= 10:
            achievements_to_award.append(("weekly_hero", "Герой недели", "10+ задач за 7 дней"))

    for ach_type, ach_name, ach_desc in achievements_to_award:
        try:
            conn.execute("INSERT INTO achievements (user_id, type, name, description) VALUES (%s,%s,%s,%s)",
                (user_id, ach_type, ach_name, ach_desc))
        except psycopg.errors.UniqueViolation:
            pass

class TaskManagerHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): pass

    def _json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "https://dmtasks.ru")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False, default=str).encode("utf-8"))

    def _html(self, fp):
        try:
            with open(fp, "r", encoding="utf-8") as f: content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            self.end_headers()
            self.wfile.write(content.encode("utf-8"))
        except FileNotFoundError:
            self.send_response(404); self.end_headers()

    def _static(self, fp):
        try:
            mime, _ = mimetypes.guess_type(fp)
            with open(fp, "rb") as f: content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", mime or "application/octet-stream")
            self.send_header("Cache-Control", "public, max-age=604800")
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_response(404); self.end_headers()

    def _body(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        return json.loads(body.decode("utf-8")) if body else {}

    def _user(self):
        cookie = self.headers.get("Cookie", "")
        for part in cookie.split(";"):
            part = part.strip()
            if part.startswith("session="):
                token = part[8:]
                if token in sessions:
                    u = sessions[token]
                    user_last_seen[u["id"]] = datetime.now().isoformat()
                    return u
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
            if token in sessions:
                u = sessions[token]
                user_last_seen[u["id"]] = datetime.now().isoformat()
                return u
        return None

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "https://dmtasks.ru")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)

        # /api/ping — лёгкий health-check для keep-alive (UptimeRobot)
        # Быстро будит Neon compute и держит Render не уснувшим.
        if path == "/api/ping":
            try:
                conn = get_db()
                conn.execute("SELECT 1").fetchone()
                conn.close()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(b"pong")
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(f"err: {e}".encode())
            return

        if path.startswith("/static/"):
            safe = _safe_join(STATIC_DIR, path[8:])
            if not safe:
                self.send_response(403); self.end_headers(); return
            return self._static(safe)
        if path.startswith("/uploads/"):
            upload_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads"))
            # Разрешаем только имя файла (без подпапок и ../)
            requested = path[9:]
            safe_name = os.path.basename(requested)
            if not safe_name or safe_name != requested or safe_name.startswith("."):
                self.send_response(404); self.end_headers(); return
            filepath = os.path.abspath(os.path.join(upload_dir, safe_name))
            # Финальная проверка: путь должен быть внутри uploads
            if not filepath.startswith(upload_dir + os.sep):
                self.send_response(404); self.end_headers(); return
            if os.path.exists(filepath) and os.path.isfile(filepath):
                self.send_response(200)
                ext = filepath.rsplit(".", 1)[-1].lower()
                ct = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png", "gif": "image/gif", "webp": "image/webp"}.get(ext, "application/octet-stream")
                self.send_header("Content-Type", ct)
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Cache-Control", "public, max-age=86400")
                self.end_headers()
                with open(filepath, "rb") as f:
                    self.wfile.write(f.read())
                return
            self.send_response(404)
            self.end_headers()
            return
        if path in ("/", "/login", "/register", "/dashboard", "/kanban", "/funnel", "/list"):
            return self._html(os.path.join(TEMPLATES_DIR, "index.html"))

        # health-v1: простой ping-эндпоинт для мониторинга
        if path == "/health" or path == "/api/health":
            try:
                _c = get_db()
                _c.execute("SELECT 1").fetchone()
                _c.close()
                return self._json({"status": "ok"})
            except Exception as _e:
                return self._json({"status": "error", "detail": str(_e)}, 500)

        # yandex-verify-v1: подтверждение для webmaster.yandex.ru
        if path == "/yandex_0e03f0a7cb0a6df6.html":
            content = '<html><head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"></head><body>Verification: 0e03f0a7cb0a6df6</body></html>'
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=UTF-8")
            self.send_header("Content-Length", str(len(content.encode("utf-8"))))
            self.end_headers()
            self.wfile.write(content.encode("utf-8"))
            return

        if path == "/api/me":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            row = conn.execute("SELECT id, username, full_name, department_id, role, avatar_color, avatar_url, onboarding_done, admin_onboarding_done FROM users WHERE id=%s", (u["id"],)).fetchone()
            dept = None
            if row["department_id"]:
                d = conn.execute("SELECT * FROM departments WHERE id=%s", (row["department_id"],)).fetchone()
                dept = dict(d) if d else None
            # Fetch role permissions for current user's role
            role = row["role"] or "member"
            perm_rows = conn.execute("SELECT permission, allowed FROM role_permissions WHERE role=%s", (role,)).fetchall()
            my_perms = {p['permission']: bool(p['allowed']) for p in perm_rows}
            conn.close()
            return self._json({**dict(row), "department": dept, "permissions": my_perms})

        if path == "/api/stages":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            # Parse query params
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            dept_id = qs.get("department_id", [None])[0]
            conn = get_db()
            if dept_id:
                try:
                    dept_id = int(dept_id)
                    rows = conn.execute("SELECT * FROM funnel_stages WHERE department_id=%s ORDER BY sort_order", (dept_id,)).fetchall()
                except (ValueError, TypeError):
                    conn.close()
                    return self._json({"error": "invalid department_id"}, 400)
            else:
                # Return global stages (department_id IS NULL) for backward compat
                rows = conn.execute("SELECT * FROM funnel_stages WHERE department_id IS NULL ORDER BY sort_order").fetchall()
            conn.close()
            return self._json([dict(r) for r in rows])

        if path == "/api/departments":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            r = conn.execute(
                "SELECT d.*, u.full_name as head_user_name FROM departments d "
                "LEFT JOIN users u ON d.head_user_id = u.id ORDER BY d.name"
            ).fetchall()
            conn.close()
            return self._json([dict(d) for d in r])

        # ---------------- Categories (Todoist-style) ----------------
        if path == "/api/categories":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            # Return all categories the user can see. Include task counts per category
            # (only counting non-done tasks — done are hidden by default in the UI).
            rows = conn.execute("""
                SELECT c.*,
                    (SELECT COUNT(*) FROM task_categories tc JOIN tasks t ON tc.task_id = t.id
                     WHERE tc.category_id = c.id AND t.status != 'done' AND t.status != 'cancelled') AS task_count,
                    (SELECT COUNT(*) FROM task_categories tc JOIN tasks t ON tc.task_id = t.id
                     WHERE tc.category_id = c.id AND t.status = 'done') AS done_count
                FROM categories c
                ORDER BY c.parent_id IS NULL DESC, c.sort_order, c.name
            """).fetchall()
            conn.close()
            return self._json([dict(r) for r in rows])

        if path == "/api/users":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            r = conn.execute(
                "SELECT u.id, u.username, u.full_name, u.department_id, u.role, u.avatar_color, u.avatar_url, d.name as department_name "
                "FROM users u LEFT JOIN departments d ON u.department_id = d.id WHERE COALESCE(u.is_active, TRUE) = TRUE ORDER BY u.full_name /* users-filter-active-v2 */"
            ).fetchall()
            conn.close()
            return self._json([dict(u) for u in r])

        if path == "/api/admin/users":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            conn.close()
            if not user_role or user_role["role"] != "admin":
                return self._json({"error": "Forbidden: admin only"}, 403)
            conn = get_db()
            r = conn.execute(
                "SELECT u.id, u.username, u.full_name, u.department_id, u.role, u.avatar_color, d.name as department_name "
                "FROM users u LEFT JOIN departments d ON u.department_id = d.id ORDER BY u.full_name"
            ).fetchall()
            conn.close()
            return self._json([dict(u) for u in r])

        if path == "/api/admin/permissions":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            conn.close()
            if not user_role or user_role["role"] != "admin":
                return self._json({"error": "Forbidden"}, 403)
            conn = get_db()
            rows = conn.execute("SELECT role, permission, allowed FROM role_permissions").fetchall()
            conn.close()
            perms = {}
            for r in rows:
                if r['role'] not in perms:
                    perms[r['role']] = {}
                perms[r['role']][r['permission']] = bool(r['allowed'])
            return self._json({'permissions': perms})

        if path == "/api/metrics/employees":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            # Calculate average time from creation to taken_at, and taken_at to completed_at
            rows = conn.execute("""
                SELECT u.id, u.full_name, u.avatar_color, u.department_id,
                    d.name as department_name,
                    COUNT(CASE WHEN t.status = 'done' THEN 1 END) as completed_count,
                    COUNT(CASE WHEN t.status = 'in_progress' THEN 1 END) as in_progress_count,
                    COUNT(t.id) as total_assigned,
                    AVG(CASE WHEN t.taken_at IS NOT NULL
                        THEN (julianday(t.taken_at) - julianday(t.created_at)) * 24
                        END) as avg_hours_to_take,
                    AVG(CASE WHEN t.completed_at IS NOT NULL AND t.taken_at IS NOT NULL
                        THEN (julianday(t.completed_at) - julianday(t.taken_at)) * 24
                        END) as avg_hours_to_complete
                FROM users u
                LEFT JOIN tasks t ON t.assigned_to = u.id
                LEFT JOIN departments d ON u.department_id = d.id
                GROUP BY u.id
                ORDER BY completed_count DESC
            """).fetchall()
            conn.close()
            return self._json([dict(r) for r in rows])

# coexec-view-v1: получить мой override для конкретной задачи
        if path.startswith("/api/tasks/") and path.endswith("/my-coexec-view"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            try:
                task_id = int(path.split("/")[3])
            except:
                return self._json({"error": "bad task id"}, 400)
            conn = get_db()
            try:
                row = conn.execute(
                    "SELECT dept_funnel_id, deadline_override, priority_override FROM task_coexec_view WHERE task_id=%s AND user_id=%s",
                    (task_id, u["id"])
                ).fetchone()
                if row:
                    return self._json(dict(row))
                return self._json({"dept_funnel_id": None, "deadline_override": None, "priority_override": None})
            finally:
                conn.close()

                # coexec-view-v1: список моих coexec-задач с overrides
        if path == "/api/tasks/coexec-view":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            try:
                rows = conn.execute("""
                    SELECT t.id, t.title, t.description, t.status, t.priority, t.deadline,
                           t.department_id, t.created_by, t.assigned_to, t.parent_task_id,
                           cv.dept_funnel_id, cv.deadline_override, cv.priority_override,
                           u1.full_name as creator_name,
                           u2.full_name as assignee_name,
                           d.name as department_name, d.color as department_color,
                           (SELECT COUNT(*) FROM comments c WHERE c.task_id = t.id) as comment_count
                    FROM tasks t
                    INNER JOIN task_coexecutors tc ON tc.task_id = t.id
                    LEFT JOIN task_coexec_view cv ON cv.task_id = t.id AND cv.user_id = %s
                    LEFT JOIN users u1 ON t.created_by = u1.id
                    LEFT JOIN users u2 ON t.assigned_to = u2.id
                    LEFT JOIN departments d ON t.department_id = d.id
                    WHERE tc.user_id = %s
                      AND t.status != 'done' AND t.status != 'cancelled'
                      AND t.parent_task_id IS NULL
                    ORDER BY t.id DESC
                """, (u["id"], u["id"])).fetchall()
                return self._json([dict(r) for r in rows])
            except Exception as _e:
                print(f"coexec-view GET fail: {_e}")
                return self._json({"error": str(_e)}, 500)
            finally:
                conn.close()

        if path == "/api/tasks":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()

            # Get user's role and department
            user_row = conn.execute("SELECT role, department_id FROM users WHERE id=%s", (u["id"],)).fetchone()
            user_role = user_row["role"] if user_row else "member"
            user_dept = user_row["department_id"] if user_row else None

            query = """SELECT t.*, u1.full_name as creator_name, u2.full_name as assignee_name,
                d.name as department_name, d.color as department_color,
                (SELECT COUNT(*) FROM comments c WHERE c.task_id = t.id) as comment_count,
                (SELECT COUNT(*) FROM comments c WHERE c.task_id = t.id
                    AND c.user_id != %s
                    AND c.id > COALESCE((SELECT last_read_comment_id FROM task_reads
                                         WHERE task_id = t.id AND user_id = %s), 0)
                ) as unread_count
                FROM tasks t LEFT JOIN users u1 ON t.created_by = u1.id
                LEFT JOIN users u2 ON t.assigned_to = u2.id
                LEFT JOIN departments d ON t.department_id = d.id"""
            conds, params = [], [u["id"], u["id"]]  # for unread_count subquery

            # Role-based visibility filter
            if user_role == "admin":
                # Admin sees all tasks - no filter needed
                pass
            elif user_role == "head":
                # Head sees: tasks in their department + tasks they created + tasks assigned to them + tasks where they're a watcher + tasks where they're a coexecutor
                visibility = """(
                    t.department_id = %s OR
                    t.created_by = %s OR
                    t.assigned_to = %s OR
                    EXISTS (SELECT 1 FROM task_watchers tw WHERE tw.task_id = t.id AND tw.user_id = %s) OR
                    EXISTS (SELECT 1 FROM task_coexecutors tc WHERE tc.task_id = t.id AND tc.user_id = %s)
                )"""
                conds.append(visibility)
                params.extend([user_dept, u["id"], u["id"], u["id"], u["id"]])
            else:  # member
                # Member sees: tasks assigned to them + tasks they created + tasks where they're a watcher + tasks where they're a coexecutor
                visibility = """(
                    t.assigned_to = %s OR
                    t.created_by = %s OR
                    EXISTS (SELECT 1 FROM task_watchers tw WHERE tw.task_id = t.id AND tw.user_id = %s) OR
                    EXISTS (SELECT 1 FROM task_coexecutors tc WHERE tc.task_id = t.id AND tc.user_id = %s)
                )"""
                conds.append(visibility)
                params.extend([u["id"], u["id"], u["id"], u["id"]])

            # Additional query params filter on top of visibility
            if "department_id" in qs: conds.append("t.department_id = %s"); params.append(qs["department_id"][0])
            if "assigned_to" in qs: conds.append("t.assigned_to = %s"); params.append(qs["assigned_to"][0])
            if "status" in qs: conds.append("t.status = %s"); params.append(qs["status"][0])
            # Filter by category (M2M): if category_id is "none" → tasks without any category
            if "category_id" in qs:
                cat_val = qs["category_id"][0]
                if cat_val == "none":
                    conds.append("NOT EXISTS (SELECT 1 FROM task_categories tc WHERE tc.task_id = t.id)")
                else:
                    conds.append("EXISTS (SELECT 1 FROM task_categories tc WHERE tc.task_id = t.id AND tc.category_id = %s)")
                    params.append(cat_val)
            # Hide done tasks by default; include them only when ?include_done=1
            if qs.get("include_done", ["0"])[0] != "1":
                conds.append("t.status != 'done'")

            # Quick filters
            if "filter" in qs:
                filter_val = qs["filter"][0]
                if filter_val == "my_tasks":
                    conds.append("t.created_by = %s"); params.append(u["id"])
                elif filter_val == "assigned_to_me":
                    conds.append("t.assigned_to = %s"); params.append(u["id"])
                elif filter_val == "watching":
                    conds.append("EXISTS (SELECT 1 FROM task_watchers tw WHERE tw.task_id = t.id AND tw.user_id = %s)")
                    params.append(u["id"])

            if conds: query += " WHERE " + " AND ".join(conds)
            query += """ ORDER BY
                CASE WHEN t.status='done' THEN 1 WHEN t.status='cancelled' THEN 2 ELSE 0 END,
                CASE WHEN t.deadline IS NULL THEN 1 ELSE 0 END,
                t.deadline ASC,
                CASE t.priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 END,
                t.created_at DESC"""
            tasks = [dict(t) for t in conn.execute(query, params).fetchall()]
            # Attach watchers, coexecutors, categories
            for t in tasks:
                watchers = conn.execute(
                    "SELECT u.id, u.full_name, u.avatar_color FROM task_watchers tw JOIN users u ON tw.user_id=u.id WHERE tw.task_id=%s", (t["id"],)
                ).fetchall()
                t["watchers"] = [dict(w) for w in watchers]
                coexecs = conn.execute(
                    "SELECT u.id, u.full_name, u.avatar_color FROM task_coexecutors tc JOIN users u ON tc.user_id=u.id WHERE tc.task_id=%s", (t["id"],)
                ).fetchall()
                t["coexecutors"] = [dict(c) for c in coexecs]
                cats = conn.execute(
                    "SELECT c.id, c.name, c.color, c.icon FROM task_categories tc JOIN categories c ON tc.category_id=c.id WHERE tc.task_id=%s", (t["id"],)
                ).fetchall()
                t["categories"] = [dict(c) for c in cats]
            conn.close()
            return self._json(tasks)

        if path.startswith("/api/tasks/") and "/comments" in path:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            task_id = path.split("/")[3]
            conn = get_db()
            _t, _ok = _can_access_task(conn, u["id"], task_id)
            if not _t:
                conn.close(); return self._json({"error": "not found"}, 404)
            if not _ok:
                conn.close(); return self._json({"error": "forbidden"}, 403)
            r = conn.execute(
                "SELECT c.*, u.full_name as author_name, u.avatar_color FROM comments c "
                "JOIN users u ON c.user_id = u.id WHERE c.task_id = %s ORDER BY c.created_at ASC", (task_id,)
            ).fetchall()
            conn.close()
            return self._json([dict(c) for c in r])

        if path.startswith("/api/tasks/") and "/activity" in path:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            task_id = path.split("/")[3]
            conn = get_db()
            _t, _ok = _can_access_task(conn, u["id"], task_id)
            if not _t:
                conn.close(); return self._json({"error": "not found"}, 404)
            if not _ok:
                conn.close(); return self._json({"error": "forbidden"}, 403)
            r = conn.execute(
                "SELECT a.id, a.task_id, a.user_id, a.action, a.details, a.created_at, "
                "a.old_value, a.new_value, u.full_name as user_name, u.avatar_color "
                "FROM task_activity a "
                "JOIN users u ON a.user_id = u.id "
                "WHERE a.task_id = %s ORDER BY a.created_at ASC", (task_id,)
            ).fetchall()
            conn.close()
            return self._json([dict(a) for a in r])

        if path.startswith("/api/tasks/") and "/watchers" in path:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            task_id = path.split("/")[3]
            conn = get_db()
            _t, _ok = _can_access_task(conn, u["id"], task_id)
            if not _t:
                conn.close(); return self._json({"error": "not found"}, 404)
            if not _ok:
                conn.close(); return self._json({"error": "forbidden"}, 403)
            r = conn.execute(
                "SELECT u.id, u.full_name, u.avatar_color FROM task_watchers tw JOIN users u ON tw.user_id=u.id WHERE tw.task_id=%s", (task_id,)
            ).fetchall()
            conn.close()
            return self._json([dict(w) for w in r])

        if path.startswith("/api/tasks/") and path.count("/") == 3:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            task_id = path.split("/")[3]
            conn = get_db()
            _tr, _ok = _can_access_task(conn, u["id"], task_id)
            if not _tr:
                conn.close()
                return self._json({"error": "not found"}, 404)
            if not _ok:
                conn.close()
                return self._json({"error": "forbidden"}, 403)
            t = conn.execute(
                "SELECT t.*, u1.full_name as creator_name, u2.full_name as assignee_name, "
                "d.name as department_name, d.color as department_color "
                "FROM tasks t LEFT JOIN users u1 ON t.created_by = u1.id "
                "LEFT JOIN users u2 ON t.assigned_to = u2.id "
                "LEFT JOIN departments d ON t.department_id = d.id WHERE t.id = %s", (task_id,)
            ).fetchone()
            if not t:
                conn.close()
                return self._json({"error": "not found"}, 404)
            result = dict(t)
            watchers = conn.execute(
                "SELECT u.id, u.full_name, u.avatar_color FROM task_watchers tw JOIN users u ON tw.user_id=u.id WHERE tw.task_id=%s", (task_id,)
            ).fetchall()
            result["watchers"] = [dict(w) for w in watchers]
            coexecs = conn.execute(
                "SELECT u.id, u.full_name, u.avatar_color FROM task_coexecutors tc JOIN users u ON tc.user_id=u.id WHERE tc.task_id=%s", (task_id,)
            ).fetchall()
            result["coexecutors"] = [dict(c) for c in coexecs]
            cats = conn.execute(
                "SELECT c.id, c.name, c.color, c.icon FROM task_categories tc JOIN categories c ON tc.category_id=c.id WHERE tc.task_id=%s", (task_id,)
            ).fetchall()
            result["categories"] = [dict(c) for c in cats]
            conn.close()
            return self._json(result)

        if path == "/api/stats":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            total = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            new = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='new'").fetchone()[0]
            in_progress = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='in_progress'").fetchone()[0]
            review = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='review'").fetchone()[0]
            done = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='done'").fetchone()[0]
            overdue = conn.execute("SELECT COUNT(*) FROM tasks WHERE deadline < CURRENT_DATE::text AND status NOT IN ('done','cancelled')").fetchone()[0]
            conn.close()
            return self._json({"total": total, "new": new, "in_progress": in_progress, "review": review, "done": done, "overdue": overdue})

        if path == "/api/notifications":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            r = conn.execute(
                "SELECT * FROM notifications WHERE user_id=%s ORDER BY created_at DESC LIMIT 50", (u["id"],)
            ).fetchall()
            unread = conn.execute("SELECT COUNT(*) FROM notifications WHERE user_id=%s AND is_read=0", (u["id"],)).fetchone()[0]
            conn.close()
            return self._json({"items": [dict(n) for n in r], "unread": unread})

        if path == "/api/analytics":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role, department_id FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not user_role or user_role["role"] not in ["admin", "head"]:
                conn.close()
                return self._json({"error": "Forbidden: admin or head only"}, 403)

            # Build department filter
            dept_filter = ""
            dept_params = []
            if user_role["role"] == "head":
                dept_filter = " AND t.department_id = %s"
                dept_params = [user_role["department_id"]]

            # Basic stats
            total_tasks = conn.execute(f"SELECT COUNT(*) FROM tasks t WHERE 1=1{dept_filter}", dept_params).fetchone()[0]

            # Tasks by status
            tasks_by_status = {}
            for status in ["new", "in_progress", "review", "done", "cancelled"]:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM tasks t WHERE t.status = %s{dept_filter}",
                    [status] + dept_params
                ).fetchone()[0]
                tasks_by_status[status] = count

            # Tasks by priority
            tasks_by_priority = {}
            for priority in ["low", "medium", "high"]:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM tasks t WHERE t.priority = %s{dept_filter}",
                    [priority] + dept_params
                ).fetchone()[0]
                tasks_by_priority[priority] = count

            # Avg completion time for done tasks (in working hours)
            done_tasks = conn.execute(f"""
                SELECT created_at, updated_at FROM tasks t WHERE t.status = 'done'{dept_filter}
            """, dept_params).fetchall()

            if done_tasks:
                working_hours_list = []
                for task in done_tasks:
                    hours, _, _ = calculate_working_hours(task["created_at"], task["updated_at"])
                    working_hours_list.append(hours)
                avg_working_hours = sum(working_hours_list) / len(working_hours_list) if working_hours_list else 0
                avg_hours = int(avg_working_hours)
                avg_minutes = int((avg_working_hours - avg_hours) * 60)
                avg_completion_time_display = f"{avg_hours}ч {avg_minutes}м"
            else:
                avg_completion_time_display = "—"

            # For compatibility, also keep as decimal for now
            avg_completion_time = round(sum(working_hours_list) / len(working_hours_list) / 24, 2) if done_tasks else 0

            # Overdue count
            overdue_count = conn.execute(
                f"SELECT COUNT(*) FROM tasks t WHERE t.deadline < CURRENT_DATE::text AND t.status NOT IN ('done','cancelled'){dept_filter}",
                dept_params
            ).fetchone()[0]

            # Tasks by department
            tasks_by_dept = []
            if user_role["role"] == "admin":
                dept_tasks = conn.execute("""
                    SELECT d.name, COUNT(*) as count FROM tasks t
                    LEFT JOIN departments d ON t.department_id = d.id
                    GROUP BY t.department_id, d.name
                """).fetchall()
                tasks_by_dept = [{"department_name": d["name"], "count": d["count"]} for d in dept_tasks]
            else:
                dept_row = conn.execute("SELECT name FROM departments WHERE id=%s", [user_role["department_id"]]).fetchone()
                count = conn.execute(f"SELECT COUNT(*) FROM tasks t WHERE t.department_id = %s{dept_filter}", dept_params).fetchone()[0]
                if dept_row:
                    tasks_by_dept = [{"department_name": dept_row["name"], "count": count}]

            # Tasks by employee
            tasks_by_employee = []
            if user_role["role"] == "admin":
                emp_tasks = conn.execute("""
                    SELECT u.full_name, COUNT(CASE WHEN t.assigned_to = u.id THEN 1 END) as assigned_count,
                           COUNT(CASE WHEN t.assigned_to = u.id AND t.status = 'done' THEN 1 END) as completed_count
                    FROM users u LEFT JOIN tasks t ON t.assigned_to = u.id
                    WHERE u.role IN ('member', 'head')
                    GROUP BY u.id, u.full_name ORDER BY assigned_count DESC
                """).fetchall()
                tasks_by_employee = [{"full_name": e["full_name"], "assigned_count": e["assigned_count"], "completed_count": e["completed_count"]} for e in emp_tasks]
            else:
                emp_tasks = conn.execute("""
                    SELECT u.full_name, COUNT(CASE WHEN t.assigned_to = u.id THEN 1 END) as assigned_count,
                           COUNT(CASE WHEN t.assigned_to = u.id AND t.status = 'done' THEN 1 END) as completed_count
                    FROM users u LEFT JOIN tasks t ON t.assigned_to = u.id AND t.department_id = %s
                    WHERE u.role IN ('member', 'head') AND u.department_id = %s
                    GROUP BY u.id ORDER BY assigned_count DESC
                """, [user_role["department_id"], user_role["department_id"]]).fetchall()
                tasks_by_employee = [{"full_name": e["full_name"], "assigned_count": e["assigned_count"], "completed_count": e["completed_count"]} for e in emp_tasks]

            # Recent activity
            recent_activity = conn.execute(f"""
                SELECT a.*, u.full_name as user_name, t.title as task_title
                FROM task_activity a
                JOIN users u ON a.user_id = u.id
                LEFT JOIN tasks t ON a.task_id = t.id
                {' WHERE t.department_id = %s' if user_role['role'] == 'head' else ''}
                ORDER BY a.created_at DESC LIMIT 20
            """, dept_params if user_role['role'] == 'head' else []).fetchall()
            recent_activity = [dict(a) for a in recent_activity]

            conn.close()
            return self._json({
                "total_tasks": total_tasks,
                "tasks_by_status": tasks_by_status,
                "tasks_by_priority": tasks_by_priority,
                "avg_completion_time": avg_completion_time,
                "avg_completion_time_display": avg_completion_time_display,
                "overdue_count": overdue_count,
                "tasks_by_department": tasks_by_dept,
                "tasks_by_employee": tasks_by_employee,
                "recent_activity": recent_activity
            })

        if path == "/api/gamification/me":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            ensure_user_stats(conn, u["id"])
            # Recalculate level from km (handles legacy level names)
            stats_row = conn.execute("SELECT total_km FROM user_stats WHERE user_id=%s", (u["id"],)).fetchone()
            correct_level = get_level_from_km(stats_row["total_km"])
            conn.execute("UPDATE user_stats SET level=%s WHERE user_id=%s", (correct_level, u["id"]))
            conn.commit()
            stats = conn.execute("SELECT * FROM user_stats WHERE user_id=%s", (u["id"],)).fetchone()
            achievements = conn.execute(
                "SELECT id, type, name, description, icon, earned_at FROM achievements WHERE user_id=%s ORDER BY earned_at DESC",
                (u["id"],)
            ).fetchall()
            next_level = get_next_level(stats["total_km"])
            stats_dict = dict(stats)
            # Include car_override if set
            car_override = stats_dict.get('car_override', '') or ''
            # Check if user has switch_car permission
            role = u.get('role', 'member')
            perm_row = conn.execute("SELECT allowed FROM role_permissions WHERE role=%s AND permission='switch_car'", (role,)).fetchone()
            can_switch_car = bool(perm_row and perm_row['allowed'])
            conn.close()
            return self._json({
                "stats": stats_dict,
                "next_level": next_level,
                "achievements": [dict(a) for a in achievements],
                "car_override": car_override,
                "can_switch_car": can_switch_car
            })

        if path == "/api/gamification/leaderboard":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            # Recalculate all levels from km
            all_stats = conn.execute("SELECT user_id, total_km FROM user_stats").fetchall()
            for row in all_stats:
                correct = get_level_from_km(row["total_km"])
                conn.execute("UPDATE user_stats SET level=%s WHERE user_id=%s", (correct, row["user_id"]))
            conn.commit()
            leaderboard = conn.execute("""
                SELECT u.id, u.full_name, u.avatar_color, s.total_km, s.level, s.tasks_completed,
                       d.name as department_name, s.car_override
                FROM user_stats s
                JOIN users u ON s.user_id = u.id
                LEFT JOIN departments d ON u.department_id = d.id
                WHERE s.total_km > 0 OR s.tasks_completed > 0 OR s.tasks_created > 0
                ORDER BY s.total_km DESC LIMIT 100
            """).fetchall()
            conn.close()
            return self._json({"leaderboard": [dict(row) for row in leaderboard]})

        if path == "/api/users/online":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            now = datetime.now()
            online_users = {}
            for uid, last_seen in user_last_seen.items():
                try:
                    last = datetime.fromisoformat(last_seen)
                    diff = (now - last).total_seconds()
                    online_users[uid] = "online" if diff < 60 else ("away" if diff < 300 else "offline")
                except:
                    online_users[uid] = "offline"
            return self._json(online_users)

        if path == "/api/feedback":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            role = u.get("role", "member")
            # Check view_feedback permission
            if role != 'admin':
                conn_p = get_db()
                perm_row = conn_p.execute("SELECT allowed FROM role_permissions WHERE role=%s AND permission='view_feedback'", (role,)).fetchone()
                conn_p.close()
                if not perm_row or not perm_row['allowed']:
                    return self._json({"error": "forbidden"}, 403)
            conn = get_db()
            rows = conn.execute("""
                SELECT f.id, f.text, f.rating, f.created_at, u.full_name, u.username
                FROM feedback f JOIN users u ON f.user_id = u.id
                ORDER BY f.created_at DESC
            """).fetchall()
            conn.close()
            result = [{"id": r[0], "text": r[1], "rating": r[2], "created_at": r[3], "full_name": r[4], "username": r[5]} for r in rows]
            return self._json(result)

        self.send_response(404); self.end_headers()

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path

        if path == "/api/profile/avatar":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            content_type = self.headers.get("Content-Type", "")
            if "multipart/form-data" not in content_type:
                return self._json({"error": "Expected multipart/form-data"}, 400)

            # Parse boundary
            boundary = content_type.split("boundary=")[1].strip()
            length = int(self.headers.get("Content-Length", 0))
            # Лимит на размер запроса: 5 МБ
            AVATAR_MAX = 5 * 1024 * 1024
            if length <= 0 or length > AVATAR_MAX + 2048:
                return self._json({"error": "Файл слишком большой (макс 5 МБ)"}, 413)
            body = self.rfile.read(length)

            # Simple multipart parser
            parts = body.split(("--" + boundary).encode())
            file_data = None
            for part in parts:
                if b"Content-Disposition" in part and b"filename=" in part:
                    # Extract file data after double CRLF
                    header_end = part.find(b"\r\n\r\n")
                    if header_end != -1:
                        file_data = part[header_end + 4:]
                        # Remove trailing \r\n
                        if file_data.endswith(b"\r\n"):
                            file_data = file_data[:-2]

            if not file_data:
                return self._json({"error": "No file uploaded"}, 400)
            if len(file_data) > AVATAR_MAX:
                return self._json({"error": "Файл слишком большой (макс 5 МБ)"}, 413)

            # Проверка: файл должен быть картинкой (по magic-байтам)
            def _detect_image_ext(b):
                if b[:3] == b"\xff\xd8\xff": return "jpg"
                if b[:8] == b"\x89PNG\r\n\x1a\n": return "png"
                if b[:6] in (b"GIF87a", b"GIF89a"): return "gif"
                if b[:4] == b"RIFF" and b[8:12] == b"WEBP": return "webp"
                return None
            detected_ext = _detect_image_ext(file_data)
            if not detected_ext:
                return self._json({"error": "Формат не поддерживается. Разрешены JPEG, PNG, GIF, WEBP."}, 415)

            # Save file
            import uuid as uuid_mod
            ext = detected_ext
            filename = f"avatar_{u['id']}_{uuid_mod.uuid4().hex[:8]}.{ext}"
            upload_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
            os.makedirs(upload_dir, exist_ok=True)
            filepath = os.path.join(upload_dir, filename)

            # Delete old avatar
            conn = get_db()
            old_url = conn.execute("SELECT avatar_url FROM users WHERE id=%s", (u["id"],)).fetchone()
            if old_url and old_url["avatar_url"]:
                old_file = os.path.join(upload_dir, os.path.basename(old_url["avatar_url"]))
                if os.path.exists(old_file):
                    try: os.remove(old_file)
                    except: pass

            with open(filepath, "wb") as f:
                f.write(file_data)

            avatar_url = f"/uploads/{filename}"
            conn.execute("UPDATE users SET avatar_url=%s WHERE id=%s", (avatar_url, u["id"]))
            conn.commit()
            conn.close()
            return self._json({"ok": True, "avatar_url": avatar_url})

        data = self._body()

        # admin-post-fix-v2: создание пользователя (POST)
        if path == "/api/admin/users":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            row = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not row or row["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden: admin only"}, 403)
            username = (data.get("username") or "").strip()
            full_name = (data.get("full_name") or "").strip()
            password = data.get("password") or ""
            new_role = data.get("role") or "member"
            dept_id = data.get("department_id")
            if not username or not full_name or not password:
                conn.close()
                return self._json({"error": "username, full_name, password обязательны"}, 400)
            # password-policy-v1
            if len(password) < 6:
                conn.close()
                return self._json({"error": "пароль должен быть минимум 6 символов"}, 400)
            if new_role not in ("admin", "head", "member"):
                conn.close()
                return self._json({"error": "роль некорректна"}, 400)
            try: dept_id = int(dept_id) if dept_id else None
            except: dept_id = None
            exists = conn.execute("SELECT id FROM users WHERE username=%s", (username,)).fetchone()
            if exists:
                conn.close()
                return self._json({"error": f"пользователь {username} уже существует"}, 400)
            _salt = secrets.token_hex(16)
            _h = hashlib.pbkdf2_hmac("sha256", password.encode(), _salt.encode(), 100000)
            ph = _salt + ":" + _h.hex()
            try:
                new_id = conn.execute(
                    "INSERT INTO users (username, full_name, password_hash, role, department_id, avatar_color, onboarding_done, admin_onboarding_done) VALUES (%s,%s,%s,%s,%s,'#7c3aed',0,0) RETURNING id",
                    (username, full_name, ph, new_role, dept_id)
                ).fetchone()["id"]
                conn.execute("INSERT INTO user_stats (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING", (new_id,))
                conn.commit(); conn.close()
                return self._json({"ok": True, "id": new_id})
            except Exception as e:
                conn.close()
                return self._json({"error": f"ошибка создания: {e}"}, 500)

        if path == "/api/login":
            # ratelimit-v1: блок при переборе
            _ip = self.client_address[0] if hasattr(self, 'client_address') else 'unknown'
            _rl_allowed, _rl_retry = _rl_check(_ip)
            if not _rl_allowed:
                return self._json({"error": f"Слишком много попыток. Подожди {_rl_retry} сек."}, 429)
            # Rate-limit: максимум 5 неудач за 5 мин с одного IP
            ip = self.client_address[0] if self.client_address else "unknown"
            if not _check_login_rate_limit(ip):
                return self._json({"error": "Слишком много попыток. Попробуйте через 5 минут."}, 429)
            conn = get_db()
            user = conn.execute("SELECT * FROM users WHERE username = %s", (data.get("username", ""),)).fetchone()
            conn.close()
            if user and verify_password(data.get("password", ""), user["password_hash"]):
                _clear_login_attempts(ip)
                token = secrets.token_hex(32)
                sessions[token] = {"id": user["id"], "username": user["username"], "role": user["role"]}
                save_session_to_db(token, user["id"], user["username"], user["role"])
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Set-Cookie", f"session={token}; Path=/; HttpOnly; SameSite=Lax")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "token": token, "user": {"id": user["id"], "full_name": user["full_name"], "role": user["role"]}}, ensure_ascii=False).encode())
                return
            _record_login_failure(ip)
            _rl_fail(_ip); return self._json({"error": "Неверный логин или пароль"}, 401)

        if path == "/api/register":
            username = data.get("username", "").strip()
            full_name = _sanitize_text(data.get("full_name", "").strip())
            password = data.get("password", "")
            department_id = data.get("department_id")
            if not username or not full_name or not password:
                return self._json({"error": "Заполните все поля"}, 400)
            if len(password) < 4:
                return self._json({"error": "Пароль минимум 4 символа"}, 400)
            conn = get_db()
            if conn.execute("SELECT id FROM users WHERE username = %s", (username,)).fetchone():
                conn.close()
                return self._json({"error": "Пользователь уже существует"}, 400)
            colors = ["#2563eb", "#059669", "#d97706", "#7c3aed", "#dc2626", "#0891b2", "#4f46e5"]
            color = colors[hash(username) % len(colors)]
            c = conn.execute(
                "INSERT INTO users (username, full_name, password_hash, department_id, role, avatar_color, onboarding_done) VALUES (%s,%s,%s,%s,%s,%s,0) RETURNING id",
                (username, full_name, hash_password(password), department_id if department_id else None, "member", color))
            uid = c.fetchone()['id']
            ensure_user_stats(conn, uid)
            conn.commit(); conn.close()
            token = secrets.token_hex(32)
            sessions[token] = {"id": uid, "username": username, "role": "member"}
            save_session_to_db(token, uid, username, "member")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Set-Cookie", f"session={token}; Path=/; HttpOnly; SameSite=Lax")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "token": token}, ensure_ascii=False).encode())
            return

        if path == "/api/logout":
            cookie = self.headers.get("Cookie", "")
            for part in cookie.split(";"):
                part = part.strip()
                if part.startswith("session="):
                    token = part[8:]
                    sessions.pop(token, None)
                    delete_session_from_db(token)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Set-Cookie", "session=; Path=/; Max-Age=0")
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
            return

        if path == "/api/onboarding_done":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            conn.execute("UPDATE users SET onboarding_done=1 WHERE id=%s", (u["id"],))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path == "/api/admin_onboarding_done":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            conn.execute("UPDATE users SET admin_onboarding_done=1 WHERE id=%s", (u["id"],))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path == "/api/tasks":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            title = _sanitize_text(data.get("title", "").strip())
            if not title: return self._json({"error": "Введите название задачи"}, 400)
            # Лимиты длины (против спама и раздувания БД)
            if len(title) > 500:
                title = title[:500]
            _desc_val = data.get("description", "")
            if isinstance(_desc_val, str) and len(_desc_val) > 20000:
                data["description"] = _desc_val[:20000]
            conn = get_db()
            c = conn.execute(
                "INSERT INTO tasks (title, description, status, priority, created_by, assigned_to, department_id, deadline, parent_task_id, sort_order) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                (title, _sanitize_text(data.get("description", "")), _auto_status_for_task(data), data.get("priority", "medium"),
                 u["id"], data.get("assigned_to") or None, data.get("department_id") or None, data.get("deadline") or None,
                 data.get("parent_task_id") or None, int(data.get("sort_order") or 0)))
            task_id = c.fetchone()['id']
            # -- head-self-disabled-v1: ломает транзакцию для head, временно отключено
            # try: _add_admin_watchers_for_head_self(conn, task_id, u["id"], title)
            # except Exception as _e: print(f"head-self watcher fail: {_e}")

            # Log task creation with details
            log_activity(conn, task_id, u["id"], "task_created", title, new_value=title)

            # Award km for creating task
            update_km(conn, u["id"], 2)
            stats = conn.execute("SELECT tasks_created FROM user_stats WHERE user_id=%s", (u["id"],)).fetchone()
            conn.execute("UPDATE user_stats SET tasks_created = %s WHERE user_id=%s", (stats["tasks_created"] + 1, u["id"]))

            # Add watchers
            watchers = data.get("watchers", [])
            for wid in watchers:
                try: conn.execute("INSERT INTO task_watchers (task_id, user_id) VALUES (%s,%s)", (task_id, wid))
                except: pass

            # Default watchers: все админы + head отдела (идемпотентно)
            try:
                default_watcher_ids = set()
                # Админы
                for row in conn.execute("SELECT id FROM users WHERE role='admin'").fetchall():
                    default_watcher_ids.add(row["id"])
                # Head отдела, если задача в отделе
                dept_id_for_head = data.get("department_id")
                if dept_id_for_head:
                    for row in conn.execute(
                        "SELECT id FROM users WHERE role='head' AND department_id=%s",
                        (dept_id_for_head,)
                    ).fetchall():
                        default_watcher_ids.add(row["id"])
                # Не добавляем тех, кто и так участвует или уже в watchers
                skip_ids = {u["id"]}
                if data.get("assigned_to"):
                    try: skip_ids.add(int(data.get("assigned_to")))
                    except: pass
                for wid in default_watcher_ids:
                    if wid in skip_ids: continue
                    try:
                        conn.execute("INSERT INTO task_watchers (task_id, user_id) VALUES (%s,%s)", (task_id, wid))
                    except Exception:
                        pass
            except Exception:
                pass

            # Add coexecutors
            coexecutors = data.get("coexecutors", [])
            for cid in coexecutors:
                try: conn.execute("INSERT INTO task_coexecutors (task_id, user_id) VALUES (%s,%s)", (task_id, cid))
                except: pass

            # Auto-add creator as watcher if assigned to someone else
            assigned_to = data.get("assigned_to")
            if assigned_to and assigned_to != u["id"]:
                try:
                    conn.execute("INSERT INTO task_watchers (task_id, user_id) VALUES (%s,%s)", (task_id, u["id"]))
                except: pass
                # Notify assigned user
                conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (%s,%s,%s,%s)",
                    (assigned_to, task_id, "assigned", f"Вам назначена задача: {title}"))

            # Attach categories (Todoist-style M2M)
            category_ids = data.get("category_ids") or []
            if isinstance(category_ids, list):
                for cid in category_ids:
                    try:
                        conn.execute("INSERT INTO task_categories (task_id, category_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                                     (task_id, int(cid)))
                    except: pass

            conn.commit(); conn.close()
            return self._json({"ok": True, "id": task_id})

        if path.startswith("/api/tasks/") and path.endswith("/read"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            try:
                task_id = int(path.split("/")[3])
            except:
                return self._json({"error": "bad task id"}, 400)
            conn = get_db()
            _t, _ok = _can_access_task(conn, u["id"], task_id)
            if not _t:
                conn.close(); return self._json({"error": "not found"}, 404)
            if not _ok:
                conn.close(); return self._json({"error": "forbidden"}, 403)
            max_row = conn.execute("SELECT MAX(id) as m FROM comments WHERE task_id=%s", (task_id,)).fetchone()
            max_id = (max_row["m"] or 0) if max_row else 0
            conn.execute(
                "INSERT INTO task_reads (task_id, user_id, last_read_comment_id, updated_at) "
                "VALUES (%s,%s,%s, CURRENT_TIMESTAMP) "
                "ON CONFLICT(task_id, user_id) DO UPDATE SET "
                "last_read_comment_id=excluded.last_read_comment_id, updated_at=CURRENT_TIMESTAMP",
                (task_id, u["id"], max_id)
            )
            conn.commit(); conn.close()
            return self._json({"ok": True, "last_read": max_id})

        if path.startswith("/api/tasks/") and "/comments" in path:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            task_id = path.split("/")[3]
            text = _sanitize_text((data.get("text") or "").strip())
            attachment_data = data.get("attachment_data") or None
            attachment_name = data.get("attachment_name") or None
            attachment_type = data.get("attachment_type") or None
            # Accept comment if it has text OR an attachment
            if not text and not attachment_data:
                return self._json({"error": "Пустой комментарий"}, 400)
            # Лимит длины текста (10 000 симв)
            if len(text) > 10000:
                text = text[:10000]
            # Limit attachment size (base64 ~8MB raw = ~11MB encoded)
            if attachment_data and len(attachment_data) > 12_000_000:
                return self._json({"error": "Файл слишком большой (макс 8 МБ)"}, 400)
            conn = get_db()

            # Check access
            task = conn.execute("SELECT created_by, assigned_to, department_id FROM tasks WHERE id=%s", (task_id,)).fetchone()
            if not task:
                conn.close()
                return self._json({"error": "Task not found"}, 404)

            user_row = conn.execute("SELECT role, department_id FROM users WHERE id=%s", (u["id"],)).fetchone()
            is_admin = user_row["role"] == "admin"
            is_head = user_row["role"] == "head" and user_row["department_id"] == task["department_id"]
            is_participant = u["id"] in [task["created_by"], task["assigned_to"]]
            is_watcher = conn.execute("SELECT 1 FROM task_watchers WHERE task_id=%s AND user_id=%s", (task_id, u["id"])).fetchone()

            if not (is_admin or is_head or is_participant or is_watcher):
                conn.close()
                return self._json({"error": "Forbidden"}, 403)

            conn.execute(
                "INSERT INTO comments (task_id, user_id, text, attachment_data, attachment_name, attachment_type) VALUES (%s,%s,%s,%s,%s,%s)",
                (task_id, u["id"], text, attachment_data, attachment_name, attachment_type))

            # Log activity
            log_activity(conn, int(task_id), u["id"], "comment_added", text)

            # mention-coexec-v1: упоминание = добавить в соисполнители
            if text:
                _mentioned_ids = set()
                for mentioned_user in conn.execute("SELECT id, full_name FROM users").fetchall():
                    mention_pattern = "@" + mentioned_user["full_name"]
                    if mention_pattern in text and mentioned_user["id"] != u["id"]:
                        _mentioned_ids.add(mentioned_user["id"])
                for _mid in _mentioned_ids:
                    try:
                        conn.execute(
                            "INSERT INTO task_coexecutors (task_id, user_id) VALUES (%s,%s) ON CONFLICT (task_id, user_id) DO NOTHING",
                            (task_id, _mid)
                        )
                    except Exception as _ce:
                        print(f"[mention coexec] {_ce}")
                    conn.execute(
                        "INSERT INTO notifications (user_id, task_id, type, message) VALUES (%s,%s,%s,%s)",
                        (_mid, task_id, "mention",
                         f'{_user_fullname(conn, u)} упомянул вас в задаче и добавил соисполнителем'))

            # Award km for commenting
            update_km(conn, u["id"], 1)
            stats = conn.execute("SELECT comments_count FROM user_stats WHERE user_id=%s", (u["id"],)).fetchone()
            conn.execute("UPDATE user_stats SET comments_count = %s WHERE user_id=%s", (stats["comments_count"] + 1, u["id"]))

            # Check achievement for 50 comments
            try:
                check_and_award_achievements(conn, u["id"])
            except Exception as _e:
                print(f"achievements check failed: {_e}")
                try: conn.rollback()
                except: pass
            # first_comment — самый первый комментарий пользователя
            try:
                _first_check = conn.execute(
                    "SELECT id FROM achievements WHERE user_id=%s AND type='first_comment'", (u["id"],)
                ).fetchone()
                if not _first_check:
                    _cnt = conn.execute("SELECT COUNT(*) as c FROM comments WHERE user_id=%s", (u["id"],)).fetchone()
                    if _cnt and (_cnt["c"] or 0) >= 1:
                        conn.execute(
                            "INSERT INTO achievements (user_id, type, name, description) VALUES (%s,%s,%s,%s)",
                            (u["id"], "first_comment", "Первое слово", "Оставили первый комментарий")
                        )
            except Exception:
                pass

            # Notify: watchers + coexecutors + assignee + creator + @-упомянутые
            watchers = conn.execute("SELECT user_id FROM task_watchers WHERE task_id=%s", (task_id,)).fetchall()
            notify_ids = set([w["user_id"] for w in watchers])
            coexecs = conn.execute("SELECT user_id FROM task_coexecutors WHERE task_id=%s", (task_id,)).fetchall()
            notify_ids.update([c["user_id"] for c in coexecs])
            if task["assigned_to"]: notify_ids.add(task["assigned_to"])
            if task["created_by"]: notify_ids.add(task["created_by"])
            # @-упоминания попадают в notify отдельным типом
            mentioned_ids = set(_parse_mentions(text, conn))
            notify_ids.update(mentioned_ids)
            notify_ids.discard(u["id"])  # автор не получает собственного уведомления
            user_name = conn.execute("SELECT full_name FROM users WHERE id=%s", (u["id"],)).fetchone()
            task_title = conn.execute("SELECT title FROM tasks WHERE id=%s", (task_id,)).fetchone()
            for nid in notify_ids:
                ntype = "mention" if nid in mentioned_ids else "comment"
                prefix = "упомянул вас в" if nid in mentioned_ids else "прокомментировал"
                conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (%s,%s,%s,%s)",
                    (nid, task_id, ntype, f"{_safe_name(user_name)} {prefix}: {task_title['title']}"))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path.startswith("/api/tasks/") and "/watchers" in path:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            try: task_id = int(path.split("/")[3])
            except (ValueError, IndexError): return self._json({"error":"bad id"}, 400)
            watcher_ids = data.get("watcher_ids", [])
            conn = get_db()
            if not _can_edit_task(conn, u, task_id):
                conn.close()
                return self._json({"error":"Forbidden"}, 403)
            _tr, _ok = _can_access_task(conn, u["id"], task_id)
            if not _tr:
                conn.close(); return self._json({"error": "not found"}, 404)
            if not _ok:
                conn.close(); return self._json({"error": "forbidden"}, 403)

            # Get old watchers for activity log
            old_watchers = conn.execute("SELECT user_id FROM task_watchers WHERE task_id=%s", (task_id,)).fetchall()
            old_watcher_ids = set([w["user_id"] for w in old_watchers])
            new_watcher_ids = set(watcher_ids)
            added_watchers = new_watcher_ids - old_watcher_ids
            removed_watchers = old_watcher_ids - new_watcher_ids

            # Update watchers
            conn.execute("DELETE FROM task_watchers WHERE task_id=%s", (task_id,))
            for wid in watcher_ids:
                try: conn.execute("INSERT INTO task_watchers (task_id, user_id) VALUES (%s,%s)", (task_id, wid))
                except: pass

            # Log added watchers + send notifications
            for wid in added_watchers:
                user_name = conn.execute("SELECT full_name FROM users WHERE id=%s", (wid,)).fetchone()
                log_activity(conn, int(task_id), u["id"], "watcher_added", f"Добавлен наблюдатель: {_safe_name(user_name)}", new_value=_safe_name(user_name))
                # Notify the added watcher
                task_row = conn.execute("SELECT title FROM tasks WHERE id=%s", (task_id,)).fetchone()
                if task_row and wid != u["id"]:
                    conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (%s,%s,%s,%s)",
                        (wid, task_id, "watcher_added", f"Вы добавлены наблюдателем в задачу: {task_row['title']}"))
            # Log removed watchers
            for wid in removed_watchers:
                user_name = conn.execute("SELECT full_name FROM users WHERE id=%s", (wid,)).fetchone()
                log_activity(conn, int(task_id), u["id"], "watcher_removed", f"Убран наблюдатель: {_safe_name(user_name)}", old_value=_safe_name(user_name))

            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path.startswith("/api/tasks/") and "/coexecutors" in path:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            try: task_id = int(path.split("/")[3])
            except (ValueError, IndexError): return self._json({"error":"bad id"}, 400)
            coexecutor_ids = data.get("coexecutor_ids", [])
            conn = get_db()
            if not _can_edit_task(conn, u, task_id):
                conn.close()
                return self._json({"error":"Forbidden"}, 403)
            _tr, _ok = _can_access_task(conn, u["id"], task_id)
            if not _tr:
                conn.close(); return self._json({"error": "not found"}, 404)
            if not _ok:
                conn.close(); return self._json({"error": "forbidden"}, 403)
            old_coexecs = conn.execute("SELECT user_id FROM task_coexecutors WHERE task_id=%s", (task_id,)).fetchall()
            old_ids = set([c["user_id"] for c in old_coexecs])
            new_ids = set(coexecutor_ids)
            added = new_ids - old_ids
            removed = old_ids - new_ids
            conn.execute("DELETE FROM task_coexecutors WHERE task_id=%s", (task_id,))
            for cid in coexecutor_ids:
                try: conn.execute("INSERT INTO task_coexecutors (task_id, user_id) VALUES (%s,%s)", (task_id, cid))
                except: pass
            for cid in added:
                user_name = conn.execute("SELECT full_name FROM users WHERE id=%s", (cid,)).fetchone()
                log_activity(conn, int(task_id), u["id"], "coexecutor_added", f"Добавлен соисполнитель: {_safe_name(user_name)}", new_value=_safe_name(user_name))
                # Notify the added coexecutor
                task_row = conn.execute("SELECT title FROM tasks WHERE id=%s", (task_id,)).fetchone()
                conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (%s,%s,%s,%s)",
                    (cid, task_id, "coexecutor_added", f"Вы добавлены соисполнителем в задачу: {task_row['title']}"))
            for cid in removed:
                user_name = conn.execute("SELECT full_name FROM users WHERE id=%s", (cid,)).fetchone()
                log_activity(conn, int(task_id), u["id"], "coexecutor_removed", f"Убран соисполнитель: {_safe_name(user_name)}", old_value=_safe_name(user_name))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path == "/api/notifications/read":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            conn.execute("UPDATE notifications SET is_read=1 WHERE user_id=%s", (u["id"],))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path == "/api/gamification/car-override":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            car_level = data.get('car_level', '').strip()
            valid_levels = ['Босоногий','Самокатчик','Моноколёсник','Байкер','Водитель','Формула 3','Формула 2','Формула 1','Чемпион','']
            if car_level not in valid_levels:
                conn.close()
                return self._json({"error": "Неверный уровень"}, 400)
            # Admin can pick any level; others only unlocked ones
            role = u.get('role', 'member')
            if car_level and role != 'admin':
                level_thresholds = {'Босоногий':0,'Самокатчик':100,'Моноколёсник':500,'Байкер':1500,'Водитель':3500,'Формула 3':7000,'Формула 2':12000,'Формула 1':18000,'Чемпион':25000}
                ensure_user_stats(conn, u["id"])
                row = conn.execute("SELECT total_km FROM user_stats WHERE user_id=%s", (u["id"],)).fetchone()
                total_km = row['total_km'] if row else 0
                needed = level_thresholds.get(car_level, 999999)
                if total_km < needed:
                    conn.close()
                    return self._json({"error": "Этот персонаж ещё не разблокирован"}, 403)
            ensure_user_stats(conn, u["id"])
            conn.execute("UPDATE user_stats SET car_override=%s WHERE user_id=%s", (car_level, u["id"]))
            conn.commit()
            conn.close()
            return self._json({"ok": True, "car_override": car_level})

        # === Stage management ===
        if path == "/api/stages/add":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            dept_id = data.get("department_id")
            # Permission check: admin can do any, head can do their own dept
            if u["role"] == "admin":
                pass  # allowed
            elif u["role"] == "head":
                conn_tmp = get_db()
                dept = conn_tmp.execute("SELECT id FROM departments WHERE head_user_id=%s", (u["id"],)).fetchone()
                conn_tmp.close()
                if not dept or (dept_id and int(dept_id) != dept["id"]):
                    return self._json({"error": "Нет доступа"}, 403)
                if not dept_id:
                    dept_id = dept["id"]
            else:
                return self._json({"error": "Нет доступа"}, 403)
            label = _sanitize_text(data.get('label', '').strip())
            if not label: return self._json({"error": "Название обязательно"}, 400)
            key = data.get('key', '').strip()
            if not key:
                # Auto-generate unique key from label
                import time
                base_key = label.lower().replace(' ', '_')
                # Keep alphanumeric (including cyrillic) and underscores
                base_key = ''.join(c for c in base_key if c.isalnum() or c == '_')
                if not base_key:
                    base_key = 'stage'
                key = base_key + '_' + str(int(time.time()))
            color = data.get('color', '#3b82f6')
            icon = data.get('icon', '📋')
            conn = get_db()
            # Check uniqueness for this department
            if dept_id:
                dept_id = int(dept_id)
                existing = conn.execute("SELECT id FROM funnel_stages WHERE key=%s AND department_id=%s", (key, dept_id)).fetchone()
            else:
                existing = conn.execute("SELECT id FROM funnel_stages WHERE key=%s AND department_id IS NULL", (key,)).fetchone()
            if existing:
                conn.close()
                return self._json({"error": "Этап с таким ключом уже есть"}, 400)
            if dept_id:
                max_order = conn.execute("SELECT MAX(sort_order) FROM funnel_stages WHERE department_id=%s", (dept_id,)).fetchone()[0] or 0
            else:
                max_order = conn.execute("SELECT MAX(sort_order) FROM funnel_stages WHERE department_id IS NULL").fetchone()[0] or 0
            conn.execute("INSERT INTO funnel_stages (key, label, color, icon, sort_order, department_id) VALUES (%s,%s,%s,%s,%s,%s)",
                         (key, label, color, icon, max_order + 1, dept_id))
            conn.commit()
            if dept_id:
                new_stage = conn.execute("SELECT * FROM funnel_stages WHERE key=%s AND department_id=%s", (key, dept_id)).fetchone()
            else:
                new_stage = conn.execute("SELECT * FROM funnel_stages WHERE key=%s AND department_id IS NULL", (key,)).fetchone()
            conn.close()
            return self._json({"ok": True, "stage": dict(new_stage)})

        if path == "/api/stages/delete":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            stage_id = data.get('id')
            move_to = data.get('move_to_key', 'new')
            if not stage_id: return self._json({"error": "id обязателен"}, 400)
            conn = get_db()
            stage = conn.execute("SELECT * FROM funnel_stages WHERE id=%s", (int(stage_id),)).fetchone()
            if not stage:
                conn.close()
                return self._json({"error": "Этап не найден"}, 404)
            # Permission check: admin can delete any, head can delete their dept's
            if u["role"] == "admin":
                pass  # allowed
            elif u["role"] == "head":
                dept = conn.execute("SELECT id FROM departments WHERE head_user_id=%s", (u["id"],)).fetchone()
                if not dept or (stage['department_id'] and stage['department_id'] != dept["id"]):
                    conn.close()
                    return self._json({"error": "Нет доступа"}, 403)
            else:
                conn.close()
                return self._json({"error": "Нет доступа"}, 403)
            # Don't allow deleting if only 2 stages remain in this department
            if stage['department_id']:
                count = conn.execute("SELECT COUNT(*) FROM funnel_stages WHERE department_id=%s", (stage['department_id'],)).fetchone()[0]
            else:
                count = conn.execute("SELECT COUNT(*) FROM funnel_stages WHERE department_id IS NULL").fetchone()[0]
            if count <= 2:
                conn.close()
                return self._json({"error": "Нельзя удалить: минимум 2 этапа"}, 400)
            # Move tasks from deleted stage to move_to
            conn.execute("UPDATE tasks SET status=%s WHERE status=%s", (move_to, stage['key']))
            conn.execute("DELETE FROM funnel_stages WHERE id=%s", (int(stage_id),))
            conn.commit()
            conn.close()
            return self._json({"ok": True})

        if path == "/api/stages/update":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            stage_id = data.get('id')
            if not stage_id: return self._json({"error": "id обязателен"}, 400)
            conn = get_db()
            stage = conn.execute("SELECT * FROM funnel_stages WHERE id=%s", (int(stage_id),)).fetchone()
            if not stage:
                conn.close()
                return self._json({"error": "Этап не найден"}, 404)
            # Permission check: admin can update any, head can update their dept's
            if u["role"] == "admin":
                pass  # allowed
            elif u["role"] == "head":
                dept = conn.execute("SELECT id FROM departments WHERE head_user_id=%s", (u["id"],)).fetchone()
                if not dept or (stage['department_id'] and stage['department_id'] != dept["id"]):
                    conn.close()
                    return self._json({"error": "Нет доступа"}, 403)
            else:
                conn.close()
                return self._json({"error": "Нет доступа"}, 403)
            label = data.get('label', stage['label']).strip()
            color = data.get('color', stage['color'])
            icon = data.get('icon', stage['icon'])
            conn.execute("UPDATE funnel_stages SET label=%s, color=%s, icon=%s WHERE id=%s", (label, color, icon, int(stage_id)))
            conn.commit()
            conn.close()
            return self._json({"ok": True})

        if path == "/api/stages/reorder":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            order = data.get('order', [])  # list of stage IDs in new order
            if not order: return self._json({"error": "order обязателен"}, 400)
            conn = get_db()
            # Verify permissions: check all stages belong to same department and user can manage
            dept_ids = set()
            for sid in order:
                stage = conn.execute("SELECT department_id FROM funnel_stages WHERE id=%s", (int(sid),)).fetchone()
                if stage:
                    dept_ids.add(stage['department_id'])
            # All stages must belong to same department
            if len(dept_ids) > 1:
                conn.close()
                return self._json({"error": "stages from different departments"}, 400)
            dept_id = list(dept_ids)[0] if dept_ids else None
            # Permission check
            if u["role"] == "admin":
                pass  # allowed
            elif u["role"] == "head":
                dept = conn.execute("SELECT id FROM departments WHERE head_user_id=%s", (u["id"],)).fetchone()
                if not dept or (dept_id and dept_id != dept["id"]):
                    conn.close()
                    return self._json({"error": "Нет доступа"}, 403)
            else:
                conn.close()
                return self._json({"error": "Нет доступа"}, 403)
            for idx, sid in enumerate(order):
                conn.execute("UPDATE funnel_stages SET sort_order=%s WHERE id=%s", (idx, int(sid)))
            conn.commit()
            conn.close()
            return self._json({"ok": True})

        # ================================================================
        # Categories CRUD (Todoist-style)
        # ================================================================
        # Permission model: admin — any category; head of dept — categories in
        # their own department (or global ones where department_id IS NULL is
        # admin-only); regular members — read-only via GET /api/categories.
        def _can_edit_category(user, cat_dept_id):
            """Return True iff the given user can create/edit/delete a category
            scoped to the given department_id (None = global, admin only)."""
            if user.get("role") == "admin":
                return True
            if user.get("role") == "head":
                if cat_dept_id is None:
                    return False  # global categories = admin only
                conn_p = get_db()
                dept = conn_p.execute("SELECT id FROM departments WHERE head_user_id=%s", (user["id"],)).fetchone()
                conn_p.close()
                return bool(dept and dept["id"] == int(cat_dept_id))
            return False

        if path == "/api/categories":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            name = _sanitize_text((data.get("name") or "").strip())
            if not name:
                return self._json({"error": "Название обязательно"}, 400)
            dept_id = data.get("department_id")
            if dept_id is not None and dept_id != "":
                try:
                    dept_id = int(dept_id)
                except (TypeError, ValueError):
                    return self._json({"error": "Некорректный отдел"}, 400)
            else:
                dept_id = None
            if not _can_edit_category(u, dept_id):
                return self._json({"error": "Нет прав на создание категории в этом отделе"}, 403)
            parent_id = data.get("parent_id")
            if parent_id in ("", None, 0, "0"):
                parent_id = None
            else:
                try: parent_id = int(parent_id)
                except: parent_id = None
            color = data.get("color", "#3b82f6")
            icon = data.get("icon", "")
            conn = get_db()
            # If parent given — inherit its department to prevent cross-dept mess
            if parent_id:
                parent = conn.execute("SELECT department_id FROM categories WHERE id=%s", (parent_id,)).fetchone()
                if not parent:
                    conn.close()
                    return self._json({"error": "Родительская категория не найдена"}, 404)
                dept_id = parent["department_id"]
            max_order = conn.execute(
                "SELECT COALESCE(MAX(sort_order),0) FROM categories WHERE COALESCE(parent_id,0)=COALESCE(%s,0)",
                (parent_id,)
            ).fetchone()[0] or 0
            # last-insert-fixed-v1
            new_id = conn.execute(
                "INSERT INTO categories (name, color, icon, sort_order, department_id, parent_id, created_by) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                (name, color, icon, max_order + 1, dept_id, parent_id, u["id"])
            ).fetchone()["id"]
            # _aw5_call_inserted: auto-watcher v5
            try: _ensure_auto_watcher(conn, task_id, u["id"], data.get("department_id"))
            except Exception: pass
            conn.commit()
            row = conn.execute("SELECT * FROM categories WHERE id=%s", (new_id,)).fetchone()
            conn.close()
            return self._json({"ok": True, "category": dict(row)})

        if path == "/api/categories/reorder":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            order = data.get("order", [])
            if not isinstance(order, list) or not order:
                return self._json({"error": "order обязателен"}, 400)
            conn = get_db()
            for idx, cid in enumerate(order):
                try:
                    cid_int = int(cid)
                except (TypeError, ValueError):
                    continue
                cat = conn.execute("SELECT department_id FROM categories WHERE id=%s", (cid_int,)).fetchone()
                if not cat: continue
                if not _can_edit_category(u, cat["department_id"]):
                    conn.close()
                    return self._json({"error": "Нет прав"}, 403)
                conn.execute("UPDATE categories SET sort_order=%s WHERE id=%s", (idx, cid_int))
            conn.commit()
            conn.close()
            return self._json({"ok": True})

        if path == "/api/gamification/check":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            task_id = data.get("task_id")
            if not task_id:
                return self._json({"error": "Missing task_id"}, 400)

            task = conn.execute("SELECT created_at, deadline, assigned_to, km_awarded FROM tasks WHERE id=%s", (task_id,)).fetchone()
            if not task or task["assigned_to"] != u["id"]:
                conn.close()
                return self._json({"error": "Invalid task"}, 400)

            # Защита от дубля: если КМ уже начислены — выходим молча
            # (реопен-сценарий done → new → done не должен давать второй бонус)
            if task["km_awarded"]:
                conn.close()
                return self._json({"ok": True, "already_awarded": True})

            # Task completion award: +10 km
            update_km(conn, u["id"], 10)
            # Помечаем, что бонус уже выдан за эту задачу
            conn.execute("UPDATE tasks SET km_awarded=1 WHERE id=%s", (task_id,))

            # Complete before deadline bonus: +5 km
            if task["deadline"]:
                today = datetime.now().strftime("%Y-%m-%d")
                if today <= task["deadline"]:
                    update_km(conn, u["id"], 5)

            # Complete same day bonus: +3 km
            created_date = task["created_at"][:10]
            today = datetime.now().strftime("%Y-%m-%d")
            if created_date == today:
                update_km(conn, u["id"], 3)

            # Update task completion counter
            stats = conn.execute("SELECT tasks_completed FROM user_stats WHERE user_id=%s", (u["id"],)).fetchone()
            conn.execute("UPDATE user_stats SET tasks_completed = %s WHERE user_id=%s", (stats["tasks_completed"] + 1, u["id"]))

            # Speed demon: complete task same day as created
            if created_date == today:
                existing = conn.execute("SELECT id FROM achievements WHERE user_id=%s AND type='speed_demon'", (u["id"],)).fetchone()
                if not existing:
                    try:
                        conn.execute("INSERT INTO achievements (user_id, type, name, description) VALUES (%s,%s,%s,%s)",
                            (u["id"], "speed_demon", "Быстрый круг", "Завершите задачу в день создания"))
                    except psycopg.errors.UniqueViolation:
                        pass

            # Update streak_days
            user_stats = conn.execute("SELECT last_active, streak_days FROM user_stats WHERE user_id=%s", (u["id"],)).fetchone()
            today_date = datetime.now().strftime("%Y-%m-%d")
            if user_stats:
                last_active = user_stats["last_active"]
                current_streak = user_stats["streak_days"] or 0

                if last_active:
                    last_date = datetime.strptime(last_active[:10], "%Y-%m-%d").date()
                    today_obj = datetime.strptime(today_date, "%Y-%m-%d").date()
                    diff = (today_obj - last_date).days

                    if diff == 1:
                        # Increment streak if last active was yesterday
                        new_streak = current_streak + 1
                    elif diff > 1:
                        # Reset streak if gap > 1 day
                        new_streak = 1
                    else:
                        # Same day, don't change
                        new_streak = current_streak
                else:
                    new_streak = 1

                conn.execute("UPDATE user_stats SET last_active=%s, streak_days=%s WHERE user_id=%s",
                    (today_date, new_streak, u["id"]))

            # Check achievements
            try:
                check_and_award_achievements(conn, u["id"])
            except Exception as _e:
                print(f"achievements check failed: {_e}")
                try: conn.rollback()
                except: pass

            # Department star: complete 5 tasks in one day
            today_tasks = conn.execute(
                "SELECT COUNT(*) as cnt FROM tasks WHERE assigned_to=%s AND status='done' AND DATE(updated_at)=%s",
                (u["id"], today_date)
            ).fetchone()
            if today_tasks and today_tasks["cnt"] >= 5:
                existing = conn.execute("SELECT id FROM achievements WHERE user_id=%s AND type='department_star'", (u["id"],)).fetchone()
                if not existing:
                    try:
                        conn.execute("INSERT INTO achievements (user_id, type, name, description) VALUES (%s,%s,%s,%s)",
                            (u["id"], "department_star", "Звезда отдела", "5+ задач в один день"))
                    except psycopg.errors.UniqueViolation:
                        pass

            # Early bird: complete task before deadline
            if task["deadline"]:
                today = datetime.now().strftime("%Y-%m-%d")
                if today <= task["deadline"]:
                    existing = conn.execute("SELECT id FROM achievements WHERE user_id=%s AND type='early_bird'", (u["id"],)).fetchone()
                    if not existing:
                        try:
                            conn.execute("INSERT INTO achievements (user_id, type, name, description) VALUES (%s,%s,%s,%s)",
                                (u["id"], "early_bird", "Ранняя пташка", "Задача до дедлайна"))
                        except psycopg.errors.UniqueViolation:
                            pass

            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path == "/api/admin/departments":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not user_role or user_role["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden: admin only"}, 403)

            name = _sanitize_text(data.get("name", "").strip())
            if not name:
                conn.close()
                return self._json({"error": "Department name required"}, 400)

            # Check name uniqueness
            if conn.execute("SELECT id FROM departments WHERE name=%s", (name,)).fetchone():
                conn.close()
                return self._json({"error": "Department name must be unique"}, 400)

            color = data.get("color", "#1a1a1a")
            head_user_id = data.get("head_user_id")

            c = conn.execute("INSERT INTO departments (name, color, head_user_id) VALUES (%s,%s,%s) RETURNING id",
                           (name, color, head_user_id if head_user_id else None))
            dept_id = c.fetchone()['id']

            # If head_user_id provided, set user's role to 'head'
            if head_user_id:
                conn.execute("UPDATE users SET role=%s WHERE id=%s", ("head", head_user_id))

            conn.commit(); conn.close()
            return self._json({"ok": True, "id": dept_id})

        if path == "/api/admin/permissions":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not user_role or user_role["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden"}, 403)
            permissions = data.get('permissions', {})
            for role in ['admin', 'head', 'member']:
                if role in permissions:
                    for perm, allowed in permissions[role].items():
                        conn.execute(
                            "INSERT INTO role_permissions (role, permission, allowed) VALUES (%s,%s,%s) ON CONFLICT (role, permission) DO UPDATE SET allowed=EXCLUDED.allowed",
                            (role, perm, 1 if allowed else 0)
                        )
            conn.commit()
            conn.close()
            return self._json({"ok": True})

        if path == "/api/admin/clean-orphaned":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not user_role or user_role["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden"}, 403)
            # Delete tasks where assigned_to is set but user doesn't exist
            result = conn.execute(
                "DELETE FROM tasks WHERE assigned_to IS NOT NULL AND assigned_to NOT IN (SELECT id FROM users)"
            )
            deleted = result.rowcount
            conn.commit()
            conn.close()
            return self._json({"ok": True, "deleted": deleted})

        if path == "/api/feedback":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            text = _sanitize_text(data.get("text", "").strip())
            rating = data.get("rating", 0)
            if not text: return self._json({"error": "Текст обязателен"}, 400)
            conn = get_db()
            conn.execute("INSERT INTO feedback (user_id, text, rating) VALUES (%s,%s,%s)", (u["id"], text, rating))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        self.send_response(404); self.end_headers()

    def do_PATCH(self):
        # Делегируем PATCH в do_PUT (для /api/comments/{id})
        return self.do_PUT()

    def do_PUT(self):
        path = urllib.parse.urlparse(self.path).path
        data = self._body()

        # Редактирование комментария — автор или админ
        if path.startswith("/api/comments/") and path.count("/") == 3:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            try:
                comment_id = int(path.split("/")[3])
            except:
                return self._json({"error": "bad comment id"}, 400)
            new_text = _sanitize_text((data.get("text") or "").strip())
            if not new_text:
                return self._json({"error": "Пустой текст"}, 400)
            if len(new_text) > 10000:
                new_text = new_text[:10000]
            conn = get_db()
            row = conn.execute("SELECT user_id FROM comments WHERE id=%s", (comment_id,)).fetchone()
            if not row:
                conn.close(); return self._json({"error": "not found"}, 404)
            me = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            is_admin = bool(me) and me["role"] == "admin"
            if row["user_id"] != u["id"] and not is_admin:
                conn.close(); return self._json({"error": "forbidden"}, 403)
            conn.execute(
                "UPDATE comments SET text=%s, edited_at=CURRENT_TIMESTAMP WHERE id=%s",
                (new_text, comment_id)
            )
            conn.commit(); conn.close()
            return self._json({"ok": True})

        # self-password-v1: смена своего пароля
        if path == "/api/profile/password":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            old_password = data.get("old_password") or ""
            new_password = data.get("new_password") or ""
            if not old_password or not new_password:
                return self._json({"error": "Введите старый и новый пароль"}, 400)
            if len(new_password) < 4:
                return self._json({"error": "Новый пароль минимум 4 символа"}, 400)
            conn = get_db()
            row = conn.execute("SELECT password_hash FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not row:
                conn.close(); return self._json({"error": "пользователь не найден"}, 404)
            # Проверим старый пароль
            import hashlib, secrets
            try:
                salt, h_hex = (row["password_hash"] or "").split(":", 1)
                h_check = hashlib.pbkdf2_hmac("sha256", old_password.encode(), salt.encode(), 100000).hex()
                if h_check != h_hex:
                    conn.close(); return self._json({"error": "Старый пароль неверный"}, 400)
            except Exception:
                conn.close(); return self._json({"error": "Ошибка проверки пароля"}, 500)
            # Сохраняем новый
            new_            # password-policy-v1
            if not new_password or len(new_password) < 6:
                conn.close()
                return self._json({"error": "новый пароль должен быть минимум 6 символов"}, 400)
            salt = secrets.token_hex(16)
            new_h = hashlib.pbkdf2_hmac("sha256", new_password.encode(), new_salt.encode(), 100000)
            new_ph = new_salt + ":" + new_h.hex()
            conn.execute("UPDATE users SET password_hash=%s WHERE id=%s", (new_ph, u["id"]))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path == "/api/profile":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            full_name = _sanitize_text(data.get("full_name", "").strip())
            if not full_name: return self._json({"error": "Имя не может быть пустым"}, 400)
            conn = get_db()
            conn.execute("UPDATE users SET full_name=%s WHERE id=%s", (full_name, u["id"]))
            conn.commit()
            conn.close()
            return self._json({"ok": True})

        # admin-user-mgmt-v1: создание пользователя
        if path == "/api/admin/users":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not user_role or user_role["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden: admin only"}, 403)
            username = (data.get("username") or "").strip()
            full_name = (data.get("full_name") or "").strip()
            password = data.get("password") or ""
            new_role = data.get("role") or "member"
            dept_id = data.get("department_id")
            if not username or not full_name or not password:
                conn.close()
                return self._json({"error": "username, full_name, password обязательны"}, 400)
            if new_role not in ("admin", "head", "member"):
                conn.close()
                return self._json({"error": "роль некорректна"}, 400)
            try: dept_id = int(dept_id) if dept_id else None
            except: dept_id = None
            # Проверяем уникальность username
            exists = conn.execute("SELECT id FROM users WHERE username=%s", (username,)).fetchone()
            if exists:
                conn.close()
                return self._json({"error": f"пользователь {username} уже существует"}, 400)
            import hashlib, secrets
            salt = secrets.token_hex(16)
            h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000)
            ph = salt + ":" + h.hex()
            try:
                new_id = conn.execute(
                    "INSERT INTO users (username, full_name, password_hash, role, department_id, avatar_color, onboarding_done) VALUES (%s,%s,%s,%s,%s,'#7c3aed',1) RETURNING id",
                    (username, full_name, ph, new_role, dept_id)
                ).fetchone()["id"]
                conn.execute("INSERT INTO user_stats (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING", (new_id,))
                conn.commit(); conn.close()
                return self._json({"ok": True, "id": new_id})
            except Exception as e:
                conn.close()
                return self._json({"error": f"ошибка создания: {e}"}, 500)

        # admin-user-mgmt-v1: обновить имя/отдел
        if path.startswith("/api/admin/users/") and not path.endswith("/password") and not path.endswith("/role"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not user_role or user_role["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden: admin only"}, 403)
            try: target_id = int(path.split("/")[4])
            except:
                conn.close()
                return self._json({"error": "bad id"}, 400)
            updates = []
            params = []
            if "full_name" in data:
                updates.append("full_name=%s"); params.append((data["full_name"] or "").strip())
            if "department_id" in data:
                _did = data["department_id"]
                try: _did = int(_did) if _did else None
                except: _did = None
                updates.append("department_id=%s"); params.append(_did)
            if "username" in data:
                _un = (data["username"] or "").strip()
                if _un:
                    # проверим уникальность
                    exists = conn.execute("SELECT id FROM users WHERE username=%s AND id<>%s", (_un, target_id)).fetchone()
                    if exists:
                        conn.close()
                        return self._json({"error": f"username {_un} занят"}, 400)
                    updates.append("username=%s"); params.append(_un)
            if not updates:
                conn.close()
                return self._json({"error": "ничего не обновляем"}, 400)
            params.append(target_id)
            conn.execute(f"UPDATE users SET {', '.join(updates)} WHERE id=%s", params)
            conn.commit(); conn.close()
            return self._json({"ok": True})

        # admin-user-mgmt-v1: сбросить пароль
        if path.startswith("/api/admin/users/") and path.endswith("/password"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not user_role or user_role["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden: admin only"}, 403)
            try: target_id = int(path.split("/")[4])
            except:
                conn.close()
                return self._json({"error": "bad id"}, 400)
            new_password = data.get("password") or ""
            if not new_password or len(new_password) < 4:
                conn.close()
                return self._json({"error": "пароль минимум 4 символа"}, 400)
            import hashlib, secrets
            salt = secrets.token_hex(16)
            h = hashlib.pbkdf2_hmac("sha256", new_password.encode(), salt.encode(), 100000)
            ph = salt + ":" + h.hex()
            conn.execute("UPDATE users SET password_hash=%s WHERE id=%s", (ph, target_id))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path.startswith("/api/users/") and "/role" in path:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not user_role or user_role["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden: admin only"}, 403)
            user_id = path.split("/")[3]
            new_role = data.get("role")
            if new_role not in ["admin", "head", "member"]:
                conn.close()
                return self._json({"error": "Invalid role"}, 400)
            conn.execute("UPDATE users SET role=%s WHERE id=%s", (new_role, user_id))
            # -- rsw-v3 #1 dept_id + session sync при head
            _dept_id = (data or {}).get("department_id")
            if new_role == "head" and _dept_id:
                try:
                    _did = int(_dept_id)
                    conn.execute("UPDATE users SET department_id=%s WHERE id=%s", (_did, user_id))
                    conn.execute("UPDATE departments SET head_user_id=%s WHERE id=%s", (user_id, _did))
                except Exception as _e: print(f"head dept fail: {_e}")
            try:
                for _tk, _ses in list(sessions.items()):
                    if _ses.get("id") == user_id: _ses["role"] = new_role
            except Exception: pass
            conn.commit(); conn.close()
            return self._json({"ok": True})

        # coexec-view-v1: обновить мою воронку/дедлайн/приоритет
        if "/coexec-view" in path and path.startswith("/api/tasks/"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            try:
                task_id = int(path.split("/")[3])
            except:
                return self._json({"error": "bad task id"}, 400)
            conn = get_db()
            try:
                # Проверим что юзер действительно соисполнитель
                row = conn.execute(
                    "SELECT 1 FROM task_coexecutors WHERE task_id=%s AND user_id=%s",
                    (task_id, u["id"])
                ).fetchone()
                if not row:
                    return self._json({"error": "Вы не соисполнитель этой задачи"}, 403)
                # Поля в data: dept_funnel_id, deadline_override, priority_override
                _did = data.get("dept_funnel_id")
                if _did is not None and _did != "":
                    try: _did = int(_did)
                    except: _did = None
                else: _did = None
                _ddl = data.get("deadline_override") or None
                _prio = data.get("priority_override") or None
                conn.execute("""
                    INSERT INTO task_coexec_view (task_id, user_id, dept_funnel_id, deadline_override, priority_override)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (task_id, user_id) DO UPDATE SET
                        dept_funnel_id = EXCLUDED.dept_funnel_id,
                        deadline_override = EXCLUDED.deadline_override,
                        priority_override = EXCLUDED.priority_override
                """, (task_id, u["id"], _did, _ddl, _prio))
                conn.commit()
                return self._json({"ok": True})
            except Exception as _e:
                print(f"coexec-view PUT fail: {_e}")
                return self._json({"error": str(_e)}, 500)
            finally:
                conn.close()

        if path.startswith("/api/tasks/"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            task_id = path.split("/")[3]
            conn = get_db()
            _tr, _ok = _can_access_task(conn, u["id"], task_id)
            if not _tr:
                conn.close()
                return self._json({"error": "not found"}, 404)
            if not _ok:
                conn.close()
                return self._json({"error": "forbidden"}, 403)
            old_task = conn.execute("SELECT * FROM tasks WHERE id=%s", (task_id,)).fetchone()

            # Оптимистическая блокировка: если фронт прислал if_version —
            # сверяем с текущей версией задачи. При расхождении — 409.
            if old_task and "if_version" in data:
                try:
                    _client_ver = int(data.get("if_version"))
                except (ValueError, TypeError):
                    _client_ver = None
                try:
                    _cur_ver = int(old_task["version"]) if "version" in old_task.keys() else 0
                except (ValueError, TypeError, IndexError):
                    _cur_ver = 0
                if _client_ver is not None and _client_ver != _cur_ver:
                    _current = dict(old_task)
                    conn.close()
                    return self._json({
                        "error": "conflict",
                        "message": "Задача была изменена другим пользователем",
                        "current": _current,
                    }, 409)
                # if_version — служебное поле, не должно попасть в UPDATE
                data.pop("if_version", None)

            sets, params = [], []
            # Подзадача не может иметь отдел, отличный от родителя: это ломает
            # права доступа (юзеры родителя теряют подзадачу из видимости).
            # Если пользователь пытается сменить department_id у подзадачи —
            # игнорируем это поле тихо.
            if old_task and old_task.get("parent_task_id"):
                if "department_id" in data:
                    data.pop("department_id", None)
            for field in ["title", "description", "status", "priority", "assigned_to", "department_id", "deadline", "sort_order", "parent_task_id"]:
                if field in data:
                    sets.append(f"{field} = %s")
                    _val = data[field] if data[field] != "" else None
                    if field in ("title", "description") and isinstance(_val, str):
                        _val = _sanitize_text(_val)
                        # Лимиты длины
                        if field == "title" and len(_val) > 500: _val = _val[:500]
                        elif field == "description" and len(_val) > 20000: _val = _val[:20000]
                    params.append(_val)
            if sets:
                sets.append("updated_at = CURRENT_TIMESTAMP")
                sets.append("version = COALESCE(version, 0) + 1")
                params.append(task_id)
                conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id = %s", params)

                # Update taken_at and completed_at timestamps based on status changes
                if "status" in data:
                    new_status = data["status"]
                    if new_status == "in_progress":
                        conn.execute("UPDATE tasks SET taken_at = CURRENT_TIMESTAMP WHERE id = %s AND taken_at IS NULL", (task_id,))
                    elif new_status == "done":
                        # Только если ещё не выставлен — иначе реопен-сценарий
                        # затирает исходное время закрытия и ломает аналитику.
                        conn.execute("UPDATE tasks SET completed_at = CURRENT_TIMESTAMP WHERE id = %s AND completed_at IS NULL", (task_id,))
                    elif new_status == "new":
                        # Reset timestamps when task goes back to new
                        conn.execute("UPDATE tasks SET taken_at = NULL, completed_at = NULL WHERE id = %s", (task_id,))

                # Log all field changes with old/new values
                # Build status names from DB stages
                _stages = conn.execute("SELECT key, label FROM funnel_stages").fetchall()
                status_names = {s['key']: s['label'] for s in _stages}
                status_names.update({"cancelled": "Отменена", "todo": "Новая"})  # fallbacks
                priority_names = {"low":"Низкий","medium":"Средний","high":"Высокий","urgent":"Срочный"}
                if old_task:
                    for field in ["title", "description", "status", "priority", "assigned_to", "department_id", "deadline"]:
                        if field in data:
                            old_val = old_task[field]
                            new_val = data[field] if data[field] != "" else None
                            if str(old_val or "") != str(new_val or ""):
                                # Resolve human-readable values
                                if field == "status":
                                    ov = status_names.get(str(old_val), old_val or "—")
                                    nv = status_names.get(str(new_val), new_val or "—")
                                elif field == "priority":
                                    ov = priority_names.get(str(old_val), old_val or "—")
                                    nv = priority_names.get(str(new_val), new_val or "—")
                                elif field == "assigned_to":
                                    ov_user = conn.execute("SELECT full_name FROM users WHERE id=%s", (old_val,)).fetchone() if old_val else None
                                    nv_user = conn.execute("SELECT full_name FROM users WHERE id=%s", (new_val,)).fetchone() if new_val else None
                                    ov = ov_user["full_name"] if ov_user else "—"
                                    nv = nv_user["full_name"] if nv_user else "—"
                                elif field == "department_id":
                                    ov_dept = conn.execute("SELECT name FROM departments WHERE id=%s", (old_val,)).fetchone() if old_val else None
                                    nv_dept = conn.execute("SELECT name FROM departments WHERE id=%s", (new_val,)).fetchone() if new_val else None
                                    ov = ov_dept["name"] if ov_dept else "—"
                                    nv = nv_dept["name"] if nv_dept else "—"
                                else:
                                    ov = str(old_val) if old_val else "—"
                                    nv = str(new_val) if new_val else "—"
                                log_activity(conn, int(task_id), u["id"], f"{field}_changed", None, old_value=ov, new_value=nv)

                # Sync categories (Todoist-style M2M) if provided
                if "category_ids" in data and isinstance(data.get("category_ids"), list):
                    conn.execute("DELETE FROM task_categories WHERE task_id=%s", (task_id,))
                    for cid in data["category_ids"]:
                        try:
                            conn.execute("INSERT INTO task_categories (task_id, category_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                                         (task_id, int(cid)))
                        except: pass

                # Status change notifications
                if "status" in data and old_task and data["status"] != old_task["status"]:
                    msg = f"Статус изменён на «{status_names.get(data['status'], data['status'])}»: {old_task['title']}"
                    _notify_task_people(conn, task_id, msg, "status_change", exclude_uid=u["id"])

                # Assigned change — notify new assignee (и старому тоже, если был)
                if "assigned_to" in data and old_task and data["assigned_to"] != old_task["assigned_to"]:
                    new_assigned = data["assigned_to"]
                    if new_assigned and new_assigned != u["id"]:
                        try:
                            conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (%s,%s,%s,%s)",
                                (new_assigned, task_id, "assigned", f"Вам назначена задача: {old_task['title']}"))
                        except Exception: pass
                    # Уведомим старого исполнителя что с него сняли
                    if old_task["assigned_to"] and old_task["assigned_to"] != u["id"]:
                        try:
                            conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (%s,%s,%s,%s)",
                                (old_task["assigned_to"], task_id, "unassigned", f"С вас снята задача: {old_task['title']}"))
                        except Exception: pass

                # Deadline change — notify всем связанным
                if "deadline" in data and old_task and str(data.get("deadline") or "") != str(old_task["deadline"] or ""):
                    new_dl = data.get("deadline") or "—"
                    _notify_task_people(conn, task_id, f"Дедлайн изменён ({new_dl}): {old_task['title']}", "deadline_change", exclude_uid=u["id"])

                # Priority change — notify всем связанным
                if "priority" in data and old_task and data["priority"] != old_task["priority"]:
                    new_pri = priority_names.get(str(data["priority"]), data["priority"] or "—")
                    _notify_task_people(conn, task_id, f"Приоритет изменён на «{new_pri}»: {old_task['title']}", "priority_change", exclude_uid=u["id"])

                # Assignee change notification (рук отдела назначает задачу)
                if "assigned_to" in data and old_task:
                    try:
                        _new_a = int(data["assigned_to"]) if data["assigned_to"] else None
                    except (ValueError, TypeError):
                        _new_a = None
                    try:
                        _old_a = int(old_task["assigned_to"]) if old_task["assigned_to"] else None
                    except (ValueError, TypeError):
                        _old_a = None
                    if _new_a and _new_a != _old_a and _new_a != u["id"]:
                        conn.execute(
                            "INSERT INTO notifications (user_id, task_id, type, message) VALUES (%s,%s,%s,%s)",
                            (_new_a, task_id, "assigned",
                             f"Вам назначена задача: {old_task['title']}"))
                    # Прошлый исполнитель не должен терять контекст: делаем его watcher.
                    if _old_a and _old_a != _new_a:
                        conn.execute(
                            "INSERT INTO task_watchers (task_id, user_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                            (task_id, _old_a))

                # Deadline change notification: если руководитель подвинул дедлайн,
                # исполнитель должен узнать.
                if "deadline" in data and old_task:
                    _old_dl = old_task["deadline"] or ""
                    _new_dl = data.get("deadline") or ""
                    if str(_old_dl) != str(_new_dl) and old_task["assigned_to"] and old_task["assigned_to"] != u["id"]:
                        _msg_dl = (f"Дедлайн задачи «{old_task['title']}» изменён"
                                   if _new_dl else
                                   f"У задачи «{old_task['title']}» снят дедлайн")
                        conn.execute(
                            "INSERT INTO notifications (user_id, task_id, type, message) VALUES (%s,%s,%s,%s)",
                            (old_task["assigned_to"], task_id, "deadline_changed", _msg_dl))

                conn.commit()
            # Вернуть свежую версию, чтобы фронт мог обновить локальную копию
            _new_ver = 0
            try:
                _row = conn.execute("SELECT version FROM tasks WHERE id=%s", (task_id,)).fetchone()
                if _row: _new_ver = int(_row["version"] or 0)
            except Exception:
                _new_ver = 0
            conn.close()
            return self._json({"ok": True, "version": _new_ver})

        # ================================================================
        # Update category (Todoist-style)
        # ================================================================
        if path.startswith("/api/categories/"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            try:
                cat_id = int(path.split("/")[3])
            except (ValueError, IndexError):
                return self._json({"error": "bad id"}, 400)
            conn = get_db()
            cat = conn.execute("SELECT * FROM categories WHERE id=%s", (cat_id,)).fetchone()
            if not cat:
                conn.close()
                return self._json({"error": "Категория не найдена"}, 404)

            # Permission check using current department_id
            def _can(user, dept):
                if user.get("role") == "admin": return True
                if user.get("role") == "head":
                    if dept is None: return False
                    d = conn.execute("SELECT id FROM departments WHERE head_user_id=%s", (user["id"],)).fetchone()
                    return bool(d and d["id"] == int(dept))
                return False

            if not _can(u, cat["department_id"]):
                conn.close()
                return self._json({"error": "Нет прав на изменение этой категории"}, 403)

            sets, params = [], []
            if "name" in data:
                name = (data.get("name") or "").strip()
                if not name:
                    conn.close()
                    return self._json({"error": "Название обязательно"}, 400)
                sets.append("name=%s"); params.append(name)
            if "color" in data:
                sets.append("color=%s"); params.append(data.get("color") or "#3b82f6")
            if "icon" in data:
                sets.append("icon=%s"); params.append(data.get("icon") or "")
            if "parent_id" in data:
                pid = data.get("parent_id")
                if pid in ("", None, 0, "0"):
                    pid = None
                else:
                    try: pid = int(pid)
                    except: pid = None
                # Prevent self-parenting and simple cycles
                if pid == cat_id:
                    conn.close()
                    return self._json({"error": "Категория не может быть родителем самой себя"}, 400)
                if pid:
                    # Walk up the parent chain to prevent cycles
                    cur = pid
                    safety = 0
                    while cur is not None and safety < 100:
                        parent_row = conn.execute("SELECT parent_id FROM categories WHERE id=%s", (cur,)).fetchone()
                        if not parent_row: break
                        if parent_row["parent_id"] == cat_id:
                            conn.close()
                            return self._json({"error": "Цикл в дереве категорий"}, 400)
                        cur = parent_row["parent_id"]
                        safety += 1
                    # Inherit department from new parent
                    par = conn.execute("SELECT department_id FROM categories WHERE id=%s", (pid,)).fetchone()
                    if par:
                        sets.append("department_id=%s"); params.append(par["department_id"])
                sets.append("parent_id=%s"); params.append(pid)
            if "department_id" in data and "parent_id" not in data:
                # Only allow department change for top-level categories
                if cat["parent_id"] is None:
                    did = data.get("department_id")
                    if did in ("", None): did = None
                    else:
                        try: did = int(did)
                        except: did = None
                    # Must still have permission for the NEW department too
                    if not _can(u, did):
                        conn.close()
                        return self._json({"error": "Нет прав на целевой отдел"}, 403)
                    sets.append("department_id=%s"); params.append(did)
                    # Propagate to all descendants
                    # (collect descendants iteratively)
                    to_update = [cat_id]
                    descendants = []
                    while to_update:
                        next_batch = []
                        for pid_ in to_update:
                            children = conn.execute("SELECT id FROM categories WHERE parent_id=%s", (pid_,)).fetchall()
                            for ch in children:
                                descendants.append(ch["id"])
                                next_batch.append(ch["id"])
                        to_update = next_batch
                    for d_id in descendants:
                        conn.execute("UPDATE categories SET department_id=%s WHERE id=%s", (did, d_id))
            if sets:
                params.append(cat_id)
                conn.execute(f"UPDATE categories SET {', '.join(sets)} WHERE id=%s", params)
                conn.commit()
            row = conn.execute("SELECT * FROM categories WHERE id=%s", (cat_id,)).fetchone()
            conn.close()
            return self._json({"ok": True, "category": dict(row)})

        if path.startswith("/api/admin/departments/"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not user_role or user_role["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden: admin only"}, 403)

            dept_id = path.split("/")[4]
            dept = conn.execute("SELECT * FROM departments WHERE id=%s", (dept_id,)).fetchone()
            if not dept:
                conn.close()
                return self._json({"error": "Department not found"}, 404)

            # Update name and/or color if provided
            sets, params = [], []
            if "name" in data:
                name = data["name"].strip()
                if not name:
                    conn.close()
                    return self._json({"error": "Department name required"}, 400)
                # Check name uniqueness (but allow current name)
                existing = conn.execute("SELECT id FROM departments WHERE name=%s AND id!=%s", (name, dept_id)).fetchone()
                if existing:
                    conn.close()
                    return self._json({"error": "Department name must be unique"}, 400)
                sets.append("name = %s")
                params.append(name)

            if "color" in data:
                sets.append("color = %s")
                params.append(data["color"])

            # Handle head_user_id change
            if "head_user_id" in data:
                new_head_id = data["head_user_id"]
                old_head_id = dept["head_user_id"]

                # If different from current head, update roles
                if new_head_id and new_head_id != old_head_id:
                    # If there was an old head, revert them to member
                    if old_head_id:
                        conn.execute("UPDATE users SET role=%s WHERE id=%s", ("member", old_head_id))
                    # Set new head
                    conn.execute("UPDATE users SET role=%s WHERE id=%s", ("head", new_head_id))

                sets.append("head_user_id = %s")
                params.append(new_head_id)

            if sets:
                params.append(dept_id)
                conn.execute(f"UPDATE departments SET {', '.join(sets)} WHERE id = %s", params)
                conn.commit()

            conn.close()
            return self._json({"ok": True})

        # Edit direct message
        self.send_response(404); self.end_headers()

    def do_DELETE(self):
        path = urllib.parse.urlparse(self.path).path

        # Удалить комментарий — автор или админ
        # soft-delete-v4: деактивация юзера админом (is_active=FALSE + блок пароля)
        if path.startswith("/api/admin/users/") and path.count("/") == 4:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            try:
                target_id = int(path.rsplit("/", 1)[-1])
            except: return self._json({"error": "bad id"}, 400)
            conn = get_db()
            row = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not row or row["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden"}, 403)
            if target_id == u["id"]:
                conn.close()
                return self._json({"error": "себя нельзя удалять"}, 400)
            try:
                _rand_pwd = secrets.token_hex(16)
                _salt = secrets.token_hex(16)
                _h = hashlib.pbkdf2_hmac("sha256", _rand_pwd.encode(), _salt.encode(), 100000)
                _ph = _salt + ":" + _h.hex()
                conn.execute("UPDATE users SET is_active=FALSE, password_hash=%s WHERE id=%s", (_ph, target_id))
                conn.commit(); conn.close()
                return self._json({"ok": True, "deactivated": target_id})
            except Exception as e:
                conn.close()
                return self._json({"error": str(e)}, 500)

        if path.startswith("/api/comments/") and path.count("/") == 3:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            try:
                comment_id = int(path.split("/")[3])
            except:
                return self._json({"error": "bad comment id"}, 400)
            conn = get_db()
            row = conn.execute("SELECT user_id FROM comments WHERE id=%s", (comment_id,)).fetchone()
            if not row:
                conn.close(); return self._json({"error": "not found"}, 404)
            me = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            is_admin = bool(me) and me["role"] == "admin"
            if row["user_id"] != u["id"] and not is_admin:
                conn.close(); return self._json({"error": "forbidden"}, 403)
            conn.execute("DELETE FROM comments WHERE id=%s", (comment_id,))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path.startswith("/api/tasks/"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            task_id = path.split("/")[3]
            conn = get_db()
            # Удалять задачу могут только admin, head своего отдела или её создатель
            _tr = conn.execute("SELECT created_by, department_id FROM tasks WHERE id=%s", (task_id,)).fetchone()
            if not _tr:
                conn.close()
                return self._json({"error": "not found"}, 404)
            _ur = conn.execute("SELECT role, department_id FROM users WHERE id=%s", (u["id"],)).fetchone()
            _can_delete = bool(_ur) and (
                _ur["role"] == "admin"
                or (_ur["role"] == "head" and _ur["department_id"] == _tr["department_id"])
                or u["id"] == _tr["created_by"]
            )
            if not _can_delete:
                conn.close()
                return self._json({"error": "forbidden"}, 403)
            # Каскад: удаляем подзадачи вместе с родителем, чтобы не оставлять
            # сирот с битым parent_task_id (у SQLite нет FK ON DELETE CASCADE
            # из-за миграции ALTER TABLE).
            conn.execute("DELETE FROM tasks WHERE parent_task_id = %s", (task_id,))
            conn.execute("DELETE FROM tasks WHERE id = %s", (task_id,))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path.startswith("/api/users/"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            me = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not me or me["role"] != "admin":
                conn.close()
                return self._json({"error": "Только администратор может удалять пользователей"}, 403)
            user_id = path.split("/")[3]
            if int(user_id) == u["id"]:
                conn.close()
                return self._json({"error": "Нельзя удалить самого себя"}, 400)
            # Find all tasks created by this user (to clean up their dependencies)
            created_tasks = [r[0] for r in conn.execute("SELECT id FROM tasks WHERE created_by=%s", (user_id,)).fetchall()]
            for tid in created_tasks:
                conn.execute("DELETE FROM task_watchers WHERE task_id=%s", (tid,))
                conn.execute("DELETE FROM comments WHERE task_id=%s", (tid,))
                conn.execute("DELETE FROM task_messages WHERE task_id=%s", (tid,))
                conn.execute("DELETE FROM task_activity WHERE task_id=%s", (tid,))
                conn.execute("DELETE FROM notifications WHERE task_id=%s", (tid,))
            conn.execute("DELETE FROM tasks WHERE created_by=%s", (user_id,))
            # Delete user's other related data
            conn.execute("DELETE FROM task_watchers WHERE user_id=%s", (user_id,))
            conn.execute("DELETE FROM comments WHERE user_id=%s", (user_id,))
            conn.execute("DELETE FROM notifications WHERE user_id=%s", (user_id,))
            conn.execute("DELETE FROM task_messages WHERE sender_id=%s OR recipient_id=%s", (user_id, user_id))
            conn.execute("DELETE FROM task_activity WHERE user_id=%s", (user_id,))
            conn.execute("DELETE FROM direct_messages WHERE sender_id=%s OR recipient_id=%s", (user_id, user_id))
            conn.execute("DELETE FROM user_stats WHERE user_id=%s", (user_id,))
            conn.execute("DELETE FROM achievements WHERE user_id=%s", (user_id,))
            # Unassign from remaining tasks
            conn.execute("UPDATE tasks SET assigned_to=NULL WHERE assigned_to=%s", (user_id,))
            conn.execute("DELETE FROM users WHERE id=%s", (user_id,))
            conn.commit(); conn.close()
            # Remove from sessions
            to_remove = [k for k, v in sessions.items() if v.get("id") == int(user_id)]
            for k in to_remove: sessions.pop(k, None)
            return self._json({"ok": True})

        # ================================================================
        # Delete category (Todoist-style) — cascade via FK handles children
        # and task_categories rows. Tasks themselves remain.
        # ================================================================
        if path.startswith("/api/categories/"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            try:
                cat_id = int(path.split("/")[3])
            except (ValueError, IndexError):
                return self._json({"error": "bad id"}, 400)
            conn = get_db()
            cat = conn.execute("SELECT * FROM categories WHERE id=%s", (cat_id,)).fetchone()
            if not cat:
                conn.close()
                return self._json({"error": "Категория не найдена"}, 404)

            # Permission check
            dept = cat["department_id"]
            can = False
            if u.get("role") == "admin":
                can = True
            elif u.get("role") == "head" and dept is not None:
                d = conn.execute("SELECT id FROM departments WHERE head_user_id=%s", (u["id"],)).fetchone()
                can = bool(d and d["id"] == int(dept))
            if not can:
                conn.close()
                return self._json({"error": "Нет прав на удаление этой категории"}, 403)

            # CASCADE handles children categories AND task_categories rows.
            conn.execute("DELETE FROM categories WHERE id=%s", (cat_id,))
            conn.commit()
            conn.close()
            return self._json({"ok": True})

        if path.startswith("/api/admin/departments/"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            me = conn.execute("SELECT role FROM users WHERE id=%s", (u["id"],)).fetchone()
            if not me or me["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden: admin only"}, 403)

            dept_id = path.split("/")[4]
            dept = conn.execute("SELECT * FROM departments WHERE id=%s", (dept_id,)).fetchone()
            if not dept:
                conn.close()
                return self._json({"error": "Department not found"}, 404)

            # Set all users in this department to department_id=NULL
            conn.execute("UPDATE users SET department_id=NULL WHERE department_id=%s", (dept_id,))

            # Set all tasks with this department_id to NULL
            conn.execute("UPDATE tasks SET department_id=NULL WHERE department_id=%s", (dept_id,))

            # Delete the department
            conn.execute("DELETE FROM departments WHERE id=%s", (dept_id,))

            conn.commit(); conn.close()
            return self._json({"ok": True})

        # Delete entire conversation with a user
        self.send_response(404); self.end_headers()

if __name__ == "__main__":
    import ssl
    import subprocess
    init_db()
    load_sessions_from_db()
    generate_deadline_notifications()

    # Support PORT env variable for cloud hosting (Render, Railway, etc.)
    PORT_HTTP = int(os.environ.get("PORT", 8080))
    CLOUD_MODE = "PORT" in os.environ  # If PORT is set, we're on cloud hosting

    if CLOUD_MODE:
        # Cloud mode: HTTP only (hosting provides SSL)
        server_http = http.server.ThreadingHTTPServer(("0.0.0.0", PORT_HTTP), TaskManagerHandler)
        print(f"\n  ╔══════════════════════════════════════════╗")
        print(f"  ║  Dudarev Motorsport — Таск-менеджер v5   ║")
        print(f"  ║                                          ║")
        print(f"  ║  Cloud mode -> port {PORT_HTTP}                ║")
        print(f"  ╚══════════════════════════════════════════╝\n")
        try:
            server_http.serve_forever()
        except KeyboardInterrupt:
            print("\nСервер остановлен.")
            server_http.server_close()
    else:
        # Local mode: HTTP + HTTPS
        PORT_HTTPS = 8443

        CERT_DIR = os.path.dirname(os.path.abspath(__file__))
        CERT_FILE = os.path.join(CERT_DIR, "cert.pem")
        KEY_FILE = os.path.join(CERT_DIR, "key.pem")

        def generate_cert():
            print("Генерация SSL-сертификата с SAN для localhost...")
            conf_path = os.path.join(CERT_DIR, "openssl.cnf")
            with open(conf_path, "w") as f:
                f.write("[req]\ndefault_bits = 2048\nprompt = no\ndefault_md = sha256\n")
                f.write("distinguished_name = dn\nx509_extensions = v3_req\n")
                f.write("[dn]\nCN = Dudarev Motorsport Task Manager\nO = Dudarev Motorsport\n")
                f.write("[v3_req]\nbasicConstraints = CA:TRUE\nkeyUsage = digitalSignature, keyEncipherment\n")
                f.write("subjectAltName = @alt_names\n[alt_names]\n")
                f.write("DNS.1 = localhost\nDNS.2 = *.localhost\nIP.1 = 127.0.0.1\nIP.2 = ::1\n")
            subprocess.run([
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", KEY_FILE, "-out", CERT_FILE,
                "-days", "3650", "-nodes", "-config", conf_path
            ], check=True, capture_output=True)
            try: os.remove(conf_path)
            except: pass
            print("SSL-сертификат создан.")

        if not os.path.exists(CERT_FILE) or not os.path.exists(KEY_FILE):
            generate_cert()

        server_http = http.server.ThreadingHTTPServer(("0.0.0.0", PORT_HTTP), TaskManagerHandler)
        server_https = http.server.ThreadingHTTPServer(("0.0.0.0", PORT_HTTPS), TaskManagerHandler)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(CERT_FILE, KEY_FILE)
        server_https.socket = ctx.wrap_socket(server_https.socket, server_side=True)

        print(f"\n  ╔══════════════════════════════════════════╗")
        print(f"  ║  Dudarev Motorsport — Таск-менеджер v5   ║")
        print(f"  ║                                          ║")
        print(f"  ║  HTTP  -> http://localhost:{PORT_HTTP}        ║")
        print(f"  ║  HTTPS -> https://localhost:{PORT_HTTPS}     ║")
        print(f"  ║                                          ║")
        print(f"  ║  Рекомендуем: http://localhost:{PORT_HTTP}    ║")
        print(f"  ╚══════════════════════════════════════════╝\n")

        import threading
        t1 = threading.Thread(target=server_https.serve_forever, daemon=True)
        t1.start()
        try:
            server_http.serve_forever()
        except KeyboardInterrupt:
            print("\nСервер остановлен.")
            server_http.server_close()
            server_https.server_close()
