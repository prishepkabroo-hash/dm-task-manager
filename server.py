#!/usr/bin/env python3
"""
Dudarev Motorsport — Таск-менеджер v4
Сообщения между пользователями, журнал активности, роли (admin/head/member),
прямые сообщения (messenger), аналитика и геймификация
"""

import http.server
import json
import sqlite3
import hashlib
import secrets
import os
import mimetypes
import urllib.parse
import re
from datetime import datetime, timedelta, date

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dm_tasks.db")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")

sessions = {}
user_last_seen = {}  # {user_id: timestamp}
typing_status = {}  # {typing_key: timestamp}

def hash_password(password, salt=None):
    if salt is None:
        salt = secrets.token_hex(16)
    hashed = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000)
    return salt + ":" + hashed.hex()

def verify_password(password, stored):
    salt = stored.split(":")[0]
    return hash_password(password, salt) == stored

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS departments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        head_name TEXT,
        color TEXT DEFAULT '#1a1a1a',
        head_user_id INTEGER,
        FOREIGN KEY (head_user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(id),
        UNIQUE(task_id, user_id)
    );
    CREATE TABLE IF NOT EXISTS task_coexecutors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(id),
        UNIQUE(task_id, user_id)
    );
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        details TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS direct_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        rating INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS group_chats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER NOT NULL,
        sender_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Migrate: add onboarding_done column if missing
    try:
        c.execute("SELECT onboarding_done FROM users LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE users ADD COLUMN onboarding_done INTEGER DEFAULT 0")

    # Migrate: add admin_onboarding_done column if missing
    try:
        c.execute("SELECT admin_onboarding_done FROM users LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE users ADD COLUMN admin_onboarding_done INTEGER DEFAULT 0")

    # Fix: mark existing admin users as onboarding complete
    c.execute("UPDATE users SET onboarding_done=1, admin_onboarding_done=1 WHERE role='admin' AND admin_onboarding_done=0")

    # Migrate: add role column if missing
    try:
        c.execute("SELECT role FROM users LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'member'")

    # Migrate: add parent_task_id column to tasks (for subtasks)
    try:
        c.execute("SELECT parent_task_id FROM tasks LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE tasks ADD COLUMN parent_task_id INTEGER DEFAULT NULL")

    # Migrate: add sort_order column to tasks (for subtask reordering)
    try:
        c.execute("SELECT sort_order FROM tasks LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE tasks ADD COLUMN sort_order INTEGER DEFAULT 0")

    # Migrate: add attachment columns to comments (files, voice)
    try:
        c.execute("SELECT attachment_data FROM comments LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE comments ADD COLUMN attachment_data TEXT DEFAULT NULL")
    try:
        c.execute("SELECT attachment_name FROM comments LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE comments ADD COLUMN attachment_name TEXT DEFAULT NULL")
    try:
        c.execute("SELECT attachment_type FROM comments LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE comments ADD COLUMN attachment_type TEXT DEFAULT NULL")

    # Migrate: create task_messages table if missing
    try:
        c.execute("SELECT 1 FROM task_messages LIMIT 1")
    except sqlite3.OperationalError:
        c.executescript("""
        CREATE TABLE task_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    try:
        c.execute("SELECT 1 FROM task_activity LIMIT 1")
    except sqlite3.OperationalError:
        c.executescript("""
        CREATE TABLE task_activity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    try:
        c.execute("SELECT 1 FROM direct_messages LIMIT 1")
    except sqlite3.OperationalError:
        c.executescript("""
        CREATE TABLE direct_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    try:
        c.execute("SELECT 1 FROM user_stats LIMIT 1")
    except sqlite3.OperationalError:
        c.executescript("""
        CREATE TABLE user_stats (
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
    try:
        c.execute("SELECT 1 FROM achievements LIMIT 1")
    except sqlite3.OperationalError:
        c.executescript("""
        CREATE TABLE achievements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    try:
        c.execute("SELECT car_override FROM user_stats LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE user_stats ADD COLUMN car_override TEXT DEFAULT ''")

    # Migrate: add switch_car permission if missing
    existing_perms = [r[0] for r in c.execute("SELECT DISTINCT permission FROM role_permissions").fetchall()]
    if 'switch_car' not in existing_perms:
        c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES ('admin', 'switch_car', 1)")
        c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES ('head', 'switch_car', 0)")
        c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES ('member', 'switch_car', 0)")

    # Migrate: add new permissions if missing
    for perm, admin_val in [('manage_kanban', 1), ('view_all_departments', 1), ('view_feedback', 1)]:
        if perm not in existing_perms:
            c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES ('admin', ?, ?)", (perm, admin_val))
            c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES ('head', ?, 0)", (perm,))
            c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES ('member', ?, 0)", (perm,))

    # Migrate: add head_user_id column to departments if missing
    try:
        c.execute("SELECT head_user_id FROM departments LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE departments ADD COLUMN head_user_id INTEGER")

    # Migrate: add avatar_url column if missing
    try:
        c.execute("SELECT avatar_url FROM users LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE users ADD COLUMN avatar_url TEXT DEFAULT ''")

    # Migrate: create group_chats table if missing
    try:
        c.execute("SELECT 1 FROM group_chats LIMIT 1")
    except sqlite3.OperationalError:
        c.executescript("""
        CREATE TABLE group_chats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            avatar_color TEXT DEFAULT '#6366f1'
        );
        CREATE TABLE group_chat_members (
            group_id INTEGER,
            user_id INTEGER,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (group_id, user_id)
        );
        CREATE TABLE group_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            sender_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

    # Migrate: add old_value and new_value columns to task_activity if missing
    try:
        c.execute("SELECT old_value FROM task_activity LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE task_activity ADD COLUMN old_value TEXT")
        c.execute("ALTER TABLE task_activity ADD COLUMN new_value TEXT")

    # Create role_permissions table
    c.execute("""
    CREATE TABLE IF NOT EXISTS role_permissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                c.execute("INSERT INTO role_permissions (role, permission, allowed) VALUES (?,?,?)", (role, p, allowed))

    # Seed departments
    existing = c.execute("SELECT COUNT(*) FROM departments").fetchone()[0]
    if existing == 0:
        for name, head, color in [
            ("Отдел продаж", "Лукьян", "#2563eb"),
            ("Склад", "Александр Дударев", "#059669"),
            ("Технический отдел", None, "#d97706"),
            ("Клуб", "Егор Паршин", "#7c3aed"),
        ]:
            c.execute("INSERT INTO departments (name, head_name, color) VALUES (?,?,?)", (name, head, color))

    # Migrate: create funnel_stages table
    try:
        c.execute("SELECT 1 FROM funnel_stages LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("""
            CREATE TABLE funnel_stages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            c.execute("INSERT INTO funnel_stages (key, label, color, icon, sort_order, department_id) VALUES (?,?,?,?,?,?)",
                       (key, label, color, icon, sort_order, None))
        # Migrate existing tasks from 'review' to 'in_progress'
        c.execute("UPDATE tasks SET status='in_progress' WHERE status='review'")

    # Migrate: add department_id to funnel_stages
    try:
        c.execute("SELECT department_id FROM funnel_stages LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE funnel_stages ADD COLUMN department_id INTEGER DEFAULT NULL")
        try:
            c.execute("ALTER TABLE funnel_stages ADD FOREIGN KEY (department_id) REFERENCES departments(id)")
        except:
            pass
        # Copy existing global stages to each department (same keys, different department_id)
        depts = c.execute("SELECT id FROM departments").fetchall()
        global_stages = c.execute("SELECT key, label, color, icon, sort_order FROM funnel_stages WHERE department_id IS NULL").fetchall()
        for dept in depts:
            for s in global_stages:
                c.execute("INSERT OR IGNORE INTO funnel_stages (key, label, color, icon, sort_order, department_id) VALUES (?,?,?,?,?,?)",
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
                c.execute("UPDATE funnel_stages SET key=? WHERE id=?", (clean_key, row[0]))
            conn.commit()

        # Check if UNIQUE constraint is on (key) alone instead of (key, department_id)
        # by trying to see the table schema
        schema = c.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='funnel_stages'").fetchone()
        if schema and 'UNIQUE(key)' in schema[0].replace(' ', '') and 'UNIQUE(key,department_id)' not in schema[0].replace(' ', ''):
            # Recreate table with correct UNIQUE constraint
            c.execute("ALTER TABLE funnel_stages RENAME TO funnel_stages_old")
            c.execute("""
                CREATE TABLE funnel_stages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            existing = c.execute("SELECT key FROM funnel_stages WHERE department_id=?", (dept[0],)).fetchall()
            existing_keys = {r[0] for r in existing}
            for s in global_stages:
                if s[0] not in existing_keys:
                    c.execute("INSERT OR IGNORE INTO funnel_stages (key, label, color, icon, sort_order, department_id) VALUES (?,?,?,?,?,?)",
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_type TEXT NOT NULL,
        message_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        emoji TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(message_type, message_id, user_id, emoji)
    )""")

    # Migrate: create pinned_messages table
    c.execute("""CREATE TABLE IF NOT EXISTS pinned_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_type TEXT NOT NULL,
        message_id INTEGER NOT NULL,
        chat_type TEXT NOT NULL,
        chat_id TEXT NOT NULL,
        pinned_by INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Seed admin
    if c.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
        c.execute("INSERT INTO users (username, full_name, password_hash, role, avatar_color, onboarding_done, admin_onboarding_done) VALUES (?,?,?,?,?,?,?)",
            ("admin", "Администратор", hash_password("admin123"), "admin", "#1a1a1a", 1, 1))

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
        "WHERE t.deadline = ? AND t.status NOT IN ('done','cancelled')", (tomorrow,)
    ).fetchall()
    for t in tasks:
        for uid in set(filter(None, [t["assigned_to"], t["created_by"]])):
            existing = conn.execute(
                "SELECT id FROM notifications WHERE task_id=? AND user_id=? AND type='deadline_soon' AND date(created_at)=date('now')", (t["id"], uid)
            ).fetchone()
            if not existing:
                conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (?,?,?,?)",
                    (uid, t["id"], "deadline_soon", f"Дедлайн завтра: {t['title']}"))
    # Overdue tasks
    tasks = conn.execute(
        "SELECT t.id, t.title, t.assigned_to, t.created_by FROM tasks t "
        "WHERE t.deadline < ? AND t.status NOT IN ('done','cancelled')", (today,)
    ).fetchall()
    for t in tasks:
        for uid in set(filter(None, [t["assigned_to"], t["created_by"]])):
            existing = conn.execute(
                "SELECT id FROM notifications WHERE task_id=? AND user_id=? AND type='overdue' AND date(created_at)=date('now')", (t["id"], uid)
            ).fetchone()
            if not existing:
                conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (?,?,?,?)",
                    (uid, t["id"], "overdue", f"Просрочена: {t['title']}"))
    conn.commit()
    conn.close()

def log_activity(conn, task_id, user_id, action, details=None, old_value=None, new_value=None):
    """Log an activity to the task_activity table."""
    conn.execute("INSERT INTO task_activity (task_id, user_id, action, details, old_value, new_value) VALUES (?,?,?,?,?,?)",
        (task_id, user_id, action, details, old_value, new_value))

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
    existing = conn.execute("SELECT user_id FROM user_stats WHERE user_id=?", (user_id,)).fetchone()
    if not existing:
        conn.execute("INSERT INTO user_stats (user_id, total_km, level, tasks_completed, tasks_created, comments_count, streak_days, last_active) VALUES (?,?,?,?,?,?,?,?)",
            (user_id, 0, "Босоногий", 0, 0, 0, 0, str(date.today())))

def update_km(conn, user_id, km_amount):
    """Add km to a user's total and update level."""
    ensure_user_stats(conn, user_id)
    conn.execute("UPDATE user_stats SET total_km = total_km + ? WHERE user_id=?", (km_amount, user_id))
    stats = conn.execute("SELECT total_km FROM user_stats WHERE user_id=?", (user_id,)).fetchone()
    new_level = get_level_from_km(stats["total_km"])
    conn.execute("UPDATE user_stats SET level=? WHERE user_id=?", (new_level, user_id))

def check_and_award_achievements(conn, user_id):
    """Check and award achievements for a user."""
    stats = conn.execute("SELECT * FROM user_stats WHERE user_id=?", (user_id,)).fetchone()
    if not stats:
        return

    achievements_to_award = []

    # first_task: complete first task
    if stats["tasks_completed"] == 1:
        achievements_to_award.append(("first_task", "Первый старт", "Завершили первую задачу"))

    # speed_demon: complete task same day (check in the calling function logic)
    # This is handled when a task is marked done

    # consistent: 7-day streak
    if stats["streak_days"] >= 7:
        existing = conn.execute("SELECT id FROM achievements WHERE user_id=? AND type='consistent'", (user_id,)).fetchone()
        if not existing:
            achievements_to_award.append(("consistent", "Стабильность", "7-дневная активность"))

    # team_player: create 10 tasks for others
    if stats["tasks_created"] >= 10:
        existing = conn.execute("SELECT id FROM achievements WHERE user_id=? AND type='team_player'", (user_id,)).fetchone()
        if not existing:
            achievements_to_award.append(("team_player", "Командный игрок", "Создали 10 задач для других"))

    # commentator: leave 50 comments
    if stats["comments_count"] >= 50:
        existing = conn.execute("SELECT id FROM achievements WHERE user_id=? AND type='commentator'", (user_id,)).fetchone()
        if not existing:
            achievements_to_award.append(("commentator", "Комментатор", "50+ комментариев"))

    # century: complete 100 tasks
    if stats["tasks_completed"] >= 100:
        existing = conn.execute("SELECT id FROM achievements WHERE user_id=? AND type='century'", (user_id,)).fetchone()
        if not existing:
            achievements_to_award.append(("century", "Сотня", "100+ задач завершено"))

    # marathon: complete 25 tasks
    if stats["tasks_completed"] >= 25:
        existing = conn.execute("SELECT id FROM achievements WHERE user_id=? AND type='marathon'", (user_id,)).fetchone()
        if not existing:
            achievements_to_award.append(("marathon", "Марафон", "25+ задач завершено"))

    # social: leave 10 comments
    if stats["comments_count"] >= 10:
        existing = conn.execute("SELECT id FROM achievements WHERE user_id=? AND type='social'", (user_id,)).fetchone()
        if not existing:
            achievements_to_award.append(("social", "Общительный", "10+ комментариев"))

    for ach_type, ach_name, ach_desc in achievements_to_award:
        try:
            conn.execute("INSERT INTO achievements (user_id, type, name, description) VALUES (?,?,?,?)",
                (user_id, ach_type, ach_name, ach_desc))
        except sqlite3.IntegrityError:
            pass

class TaskManagerHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): pass

    def _json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
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
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)

        if path.startswith("/static/"):
            return self._static(os.path.join(STATIC_DIR, path[8:]))
        if path.startswith("/uploads/"):
            upload_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
            filepath = os.path.join(upload_dir, path[9:])
            if os.path.exists(filepath):
                self.send_response(200)
                ext = filepath.rsplit(".", 1)[-1].lower()
                ct = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png", "gif": "image/gif", "webp": "image/webp"}.get(ext, "application/octet-stream")
                self.send_header("Content-Type", ct)
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

        if path == "/api/me":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            row = conn.execute("SELECT id, username, full_name, department_id, role, avatar_color, avatar_url, onboarding_done, admin_onboarding_done FROM users WHERE id=?", (u["id"],)).fetchone()
            dept = None
            if row["department_id"]:
                d = conn.execute("SELECT * FROM departments WHERE id=?", (row["department_id"],)).fetchone()
                dept = dict(d) if d else None
            # Fetch role permissions for current user's role
            role = row["role"] or "member"
            perm_rows = conn.execute("SELECT permission, allowed FROM role_permissions WHERE role=?", (role,)).fetchall()
            my_perms = {p['permission']: bool(p['allowed']) for p in perm_rows}
            conn.close()
            return self._json({**dict(row), "department": dept, "permissions": my_perms})

        if path == "/api/stages":
            u = self._user()
            # Parse query params
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            dept_id = qs.get("department_id", [None])[0]
            conn = get_db()
            if dept_id:
                try:
                    dept_id = int(dept_id)
                    rows = conn.execute("SELECT * FROM funnel_stages WHERE department_id=? ORDER BY sort_order", (dept_id,)).fetchall()
                except (ValueError, TypeError):
                    conn.close()
                    return self._json({"error": "invalid department_id"}, 400)
            else:
                # Return global stages (department_id IS NULL) for backward compat
                rows = conn.execute("SELECT * FROM funnel_stages WHERE department_id IS NULL ORDER BY sort_order").fetchall()
            conn.close()
            return self._json([dict(r) for r in rows])

        if path == "/api/departments":
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
            conn = get_db()
            r = conn.execute(
                "SELECT u.id, u.username, u.full_name, u.department_id, u.role, u.avatar_color, u.avatar_url, d.name as department_name "
                "FROM users u LEFT JOIN departments d ON u.department_id = d.id ORDER BY u.full_name"
            ).fetchall()
            conn.close()
            return self._json([dict(u) for u in r])

        if path == "/api/admin/users":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=?", (u["id"],)).fetchone()
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
            user_role = conn.execute("SELECT role FROM users WHERE id=?", (u["id"],)).fetchone()
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

        if path == "/api/tasks":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()

            # Get user's role and department
            user_row = conn.execute("SELECT role, department_id FROM users WHERE id=?", (u["id"],)).fetchone()
            user_role = user_row["role"] if user_row else "member"
            user_dept = user_row["department_id"] if user_row else None

            query = """SELECT t.*, u1.full_name as creator_name, u2.full_name as assignee_name,
                d.name as department_name, d.color as department_color
                FROM tasks t LEFT JOIN users u1 ON t.created_by = u1.id
                LEFT JOIN users u2 ON t.assigned_to = u2.id
                LEFT JOIN departments d ON t.department_id = d.id"""
            conds, params = [], []

            # Role-based visibility filter
            if user_role == "admin":
                # Admin sees all tasks - no filter needed
                pass
            elif user_role == "head":
                # Head sees: tasks in their department + tasks they created + tasks assigned to them + tasks where they're a watcher + tasks where they're a coexecutor
                visibility = """(
                    t.department_id = ? OR
                    t.created_by = ? OR
                    t.assigned_to = ? OR
                    EXISTS (SELECT 1 FROM task_watchers tw WHERE tw.task_id = t.id AND tw.user_id = ?) OR
                    EXISTS (SELECT 1 FROM task_coexecutors tc WHERE tc.task_id = t.id AND tc.user_id = ?)
                )"""
                conds.append(visibility)
                params.extend([user_dept, u["id"], u["id"], u["id"], u["id"]])
            else:  # member
                # Member sees: tasks assigned to them + tasks they created + tasks where they're a watcher + tasks where they're a coexecutor
                visibility = """(
                    t.assigned_to = ? OR
                    t.created_by = ? OR
                    EXISTS (SELECT 1 FROM task_watchers tw WHERE tw.task_id = t.id AND tw.user_id = ?) OR
                    EXISTS (SELECT 1 FROM task_coexecutors tc WHERE tc.task_id = t.id AND tc.user_id = ?)
                )"""
                conds.append(visibility)
                params.extend([u["id"], u["id"], u["id"], u["id"]])

            # Additional query params filter on top of visibility
            if "department_id" in qs: conds.append("t.department_id = ?"); params.append(qs["department_id"][0])
            if "assigned_to" in qs: conds.append("t.assigned_to = ?"); params.append(qs["assigned_to"][0])
            if "status" in qs: conds.append("t.status = ?"); params.append(qs["status"][0])
            # Filter by category (M2M): if category_id is "none" → tasks without any category
            if "category_id" in qs:
                cat_val = qs["category_id"][0]
                if cat_val == "none":
                    conds.append("NOT EXISTS (SELECT 1 FROM task_categories tc WHERE tc.task_id = t.id)")
                else:
                    conds.append("EXISTS (SELECT 1 FROM task_categories tc WHERE tc.task_id = t.id AND tc.category_id = ?)")
                    params.append(cat_val)
            # Hide done tasks by default; include them only when ?include_done=1
            if qs.get("include_done", ["0"])[0] != "1":
                conds.append("t.status != 'done'")

            # Quick filters
            if "filter" in qs:
                filter_val = qs["filter"][0]
                if filter_val == "my_tasks":
                    conds.append("t.created_by = ?"); params.append(u["id"])
                elif filter_val == "assigned_to_me":
                    conds.append("t.assigned_to = ?"); params.append(u["id"])
                elif filter_val == "watching":
                    conds.append("EXISTS (SELECT 1 FROM task_watchers tw WHERE tw.task_id = t.id AND tw.user_id = ?)")
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
                    "SELECT u.id, u.full_name, u.avatar_color FROM task_watchers tw JOIN users u ON tw.user_id=u.id WHERE tw.task_id=?", (t["id"],)
                ).fetchall()
                t["watchers"] = [dict(w) for w in watchers]
                coexecs = conn.execute(
                    "SELECT u.id, u.full_name, u.avatar_color FROM task_coexecutors tc JOIN users u ON tc.user_id=u.id WHERE tc.task_id=?", (t["id"],)
                ).fetchall()
                t["coexecutors"] = [dict(c) for c in coexecs]
                cats = conn.execute(
                    "SELECT c.id, c.name, c.color, c.icon FROM task_categories tc JOIN categories c ON tc.category_id=c.id WHERE tc.task_id=?", (t["id"],)
                ).fetchall()
                t["categories"] = [dict(c) for c in cats]
            conn.close()
            return self._json(tasks)

        if path.startswith("/api/tasks/") and "/comments" in path:
            task_id = path.split("/")[3]
            conn = get_db()
            r = conn.execute(
                "SELECT c.*, u.full_name as author_name, u.avatar_color FROM comments c "
                "JOIN users u ON c.user_id = u.id WHERE c.task_id = ? ORDER BY c.created_at ASC", (task_id,)
            ).fetchall()
            conn.close()
            return self._json([dict(c) for c in r])

        if path.startswith("/api/tasks/") and "/activity" in path:
            task_id = path.split("/")[3]
            conn = get_db()
            r = conn.execute(
                "SELECT a.id, a.task_id, a.user_id, a.action, a.details, a.created_at, "
                "a.old_value, a.new_value, u.full_name as user_name, u.avatar_color "
                "FROM task_activity a "
                "JOIN users u ON a.user_id = u.id "
                "WHERE a.task_id = ? ORDER BY a.created_at ASC", (task_id,)
            ).fetchall()
            conn.close()
            return self._json([dict(a) for a in r])

        if path.startswith("/api/tasks/") and "/watchers" in path:
            task_id = path.split("/")[3]
            conn = get_db()
            r = conn.execute(
                "SELECT u.id, u.full_name, u.avatar_color FROM task_watchers tw JOIN users u ON tw.user_id=u.id WHERE tw.task_id=?", (task_id,)
            ).fetchall()
            conn.close()
            return self._json([dict(w) for w in r])

        if path.startswith("/api/tasks/") and path.count("/") == 3:
            task_id = path.split("/")[3]
            conn = get_db()
            t = conn.execute(
                "SELECT t.*, u1.full_name as creator_name, u2.full_name as assignee_name, "
                "d.name as department_name, d.color as department_color "
                "FROM tasks t LEFT JOIN users u1 ON t.created_by = u1.id "
                "LEFT JOIN users u2 ON t.assigned_to = u2.id "
                "LEFT JOIN departments d ON t.department_id = d.id WHERE t.id = ?", (task_id,)
            ).fetchone()
            if not t:
                conn.close()
                return self._json({"error": "not found"}, 404)
            result = dict(t)
            watchers = conn.execute(
                "SELECT u.id, u.full_name, u.avatar_color FROM task_watchers tw JOIN users u ON tw.user_id=u.id WHERE tw.task_id=?", (task_id,)
            ).fetchall()
            result["watchers"] = [dict(w) for w in watchers]
            coexecs = conn.execute(
                "SELECT u.id, u.full_name, u.avatar_color FROM task_coexecutors tc JOIN users u ON tc.user_id=u.id WHERE tc.task_id=?", (task_id,)
            ).fetchall()
            result["coexecutors"] = [dict(c) for c in coexecs]
            cats = conn.execute(
                "SELECT c.id, c.name, c.color, c.icon FROM task_categories tc JOIN categories c ON tc.category_id=c.id WHERE tc.task_id=?", (task_id,)
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
            overdue = conn.execute("SELECT COUNT(*) FROM tasks WHERE deadline < date('now') AND status NOT IN ('done','cancelled')").fetchone()[0]
            conn.close()
            return self._json({"total": total, "new": new, "in_progress": in_progress, "review": review, "done": done, "overdue": overdue})

        if path == "/api/notifications":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            r = conn.execute(
                "SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC LIMIT 50", (u["id"],)
            ).fetchall()
            unread = conn.execute("SELECT COUNT(*) FROM notifications WHERE user_id=? AND is_read=0", (u["id"],)).fetchone()[0]
            conn.close()
            return self._json({"items": [dict(n) for n in r], "unread": unread})

        if path == "/api/analytics":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role, department_id FROM users WHERE id=?", (u["id"],)).fetchone()
            if not user_role or user_role["role"] not in ["admin", "head"]:
                conn.close()
                return self._json({"error": "Forbidden: admin or head only"}, 403)

            # Build department filter
            dept_filter = ""
            dept_params = []
            if user_role["role"] == "head":
                dept_filter = " AND t.department_id = ?"
                dept_params = [user_role["department_id"]]

            # Basic stats
            total_tasks = conn.execute(f"SELECT COUNT(*) FROM tasks t WHERE 1=1{dept_filter}", dept_params).fetchone()[0]

            # Tasks by status
            tasks_by_status = {}
            for status in ["new", "in_progress", "review", "done", "cancelled"]:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM tasks t WHERE t.status = ?{dept_filter}",
                    [status] + dept_params
                ).fetchone()[0]
                tasks_by_status[status] = count

            # Tasks by priority
            tasks_by_priority = {}
            for priority in ["low", "medium", "high"]:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM tasks t WHERE t.priority = ?{dept_filter}",
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
                f"SELECT COUNT(*) FROM tasks t WHERE t.deadline < date('now') AND t.status NOT IN ('done','cancelled'){dept_filter}",
                dept_params
            ).fetchone()[0]

            # Tasks by department
            tasks_by_dept = []
            if user_role["role"] == "admin":
                dept_tasks = conn.execute("""
                    SELECT d.name, COUNT(*) as count FROM tasks t
                    LEFT JOIN departments d ON t.department_id = d.id
                    GROUP BY t.department_id
                """).fetchall()
                tasks_by_dept = [{"department_name": d["name"], "count": d["count"]} for d in dept_tasks]
            else:
                dept_row = conn.execute("SELECT name FROM departments WHERE id=?", [user_role["department_id"]]).fetchone()
                count = conn.execute(f"SELECT COUNT(*) FROM tasks t WHERE t.department_id = ?{dept_filter}", dept_params).fetchone()[0]
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
                    GROUP BY u.id ORDER BY assigned_count DESC
                """).fetchall()
                tasks_by_employee = [{"full_name": e["full_name"], "assigned_count": e["assigned_count"], "completed_count": e["completed_count"]} for e in emp_tasks]
            else:
                emp_tasks = conn.execute("""
                    SELECT u.full_name, COUNT(CASE WHEN t.assigned_to = u.id THEN 1 END) as assigned_count,
                           COUNT(CASE WHEN t.assigned_to = u.id AND t.status = 'done' THEN 1 END) as completed_count
                    FROM users u LEFT JOIN tasks t ON t.assigned_to = u.id AND t.department_id = ?
                    WHERE u.role IN ('member', 'head') AND u.department_id = ?
                    GROUP BY u.id ORDER BY assigned_count DESC
                """, [user_role["department_id"], user_role["department_id"]]).fetchall()
                tasks_by_employee = [{"full_name": e["full_name"], "assigned_count": e["assigned_count"], "completed_count": e["completed_count"]} for e in emp_tasks]

            # Recent activity
            recent_activity = conn.execute(f"""
                SELECT a.*, u.full_name as user_name, t.title as task_title
                FROM task_activity a
                JOIN users u ON a.user_id = u.id
                LEFT JOIN tasks t ON a.task_id = t.id
                {' WHERE t.department_id = ?' if user_role['role'] == 'head' else ''}
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
            stats_row = conn.execute("SELECT total_km FROM user_stats WHERE user_id=?", (u["id"],)).fetchone()
            correct_level = get_level_from_km(stats_row["total_km"])
            conn.execute("UPDATE user_stats SET level=? WHERE user_id=?", (correct_level, u["id"]))
            conn.commit()
            stats = conn.execute("SELECT * FROM user_stats WHERE user_id=?", (u["id"],)).fetchone()
            achievements = conn.execute(
                "SELECT id, type, name, description, icon, earned_at FROM achievements WHERE user_id=? ORDER BY earned_at DESC",
                (u["id"],)
            ).fetchall()
            next_level = get_next_level(stats["total_km"])
            stats_dict = dict(stats)
            # Include car_override if set
            car_override = stats_dict.get('car_override', '') or ''
            # Check if user has switch_car permission
            role = u.get('role', 'member')
            perm_row = conn.execute("SELECT allowed FROM role_permissions WHERE role=? AND permission='switch_car'", (role,)).fetchone()
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
                conn.execute("UPDATE user_stats SET level=? WHERE user_id=?", (correct, row["user_id"]))
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
                perm_row = conn_p.execute("SELECT allowed FROM role_permissions WHERE role=? AND permission='view_feedback'", (role,)).fetchone()
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

            # Save file
            import uuid as uuid_mod
            ext = "jpg"
            filename = f"avatar_{u['id']}_{uuid_mod.uuid4().hex[:8]}.{ext}"
            upload_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
            os.makedirs(upload_dir, exist_ok=True)
            filepath = os.path.join(upload_dir, filename)

            # Delete old avatar
            conn = get_db()
            old_url = conn.execute("SELECT avatar_url FROM users WHERE id=?", (u["id"],)).fetchone()
            if old_url and old_url["avatar_url"]:
                old_file = os.path.join(upload_dir, os.path.basename(old_url["avatar_url"]))
                if os.path.exists(old_file):
                    try: os.remove(old_file)
                    except: pass

            with open(filepath, "wb") as f:
                f.write(file_data)

            avatar_url = f"/uploads/{filename}"
            conn.execute("UPDATE users SET avatar_url=? WHERE id=?", (avatar_url, u["id"]))
            conn.commit()
            conn.close()
            return self._json({"ok": True, "avatar_url": avatar_url})

        data = self._body()

        if path == "/api/login":
            conn = get_db()
            user = conn.execute("SELECT * FROM users WHERE username = ?", (data.get("username", ""),)).fetchone()
            conn.close()
            if user and verify_password(data.get("password", ""), user["password_hash"]):
                token = secrets.token_hex(32)
                sessions[token] = {"id": user["id"], "username": user["username"], "role": user["role"]}
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Set-Cookie", f"session={token}; Path=/; HttpOnly; SameSite=Lax")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "token": token, "user": {"id": user["id"], "full_name": user["full_name"], "role": user["role"]}}, ensure_ascii=False).encode())
                return
            return self._json({"error": "Неверный логин или пароль"}, 401)

        if path == "/api/register":
            username = data.get("username", "").strip()
            full_name = data.get("full_name", "").strip()
            password = data.get("password", "")
            department_id = data.get("department_id")
            if not username or not full_name or not password:
                return self._json({"error": "Заполните все поля"}, 400)
            if len(password) < 4:
                return self._json({"error": "Пароль минимум 4 символа"}, 400)
            conn = get_db()
            if conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone():
                conn.close()
                return self._json({"error": "Пользователь уже существует"}, 400)
            colors = ["#2563eb", "#059669", "#d97706", "#7c3aed", "#dc2626", "#0891b2", "#4f46e5"]
            color = colors[hash(username) % len(colors)]
            c = conn.execute(
                "INSERT INTO users (username, full_name, password_hash, department_id, role, avatar_color, onboarding_done) VALUES (?,?,?,?,?,?,0)",
                (username, full_name, hash_password(password), department_id if department_id else None, "member", color))
            uid = c.lastrowid
            ensure_user_stats(conn, uid)
            conn.commit(); conn.close()
            token = secrets.token_hex(32)
            sessions[token] = {"id": uid, "username": username, "role": "member"}
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
                    sessions.pop(part[8:], None)
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
            conn.execute("UPDATE users SET onboarding_done=1 WHERE id=?", (u["id"],))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path == "/api/admin_onboarding_done":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            conn.execute("UPDATE users SET admin_onboarding_done=1 WHERE id=?", (u["id"],))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path == "/api/tasks":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            title = data.get("title", "").strip()
            if not title: return self._json({"error": "Введите название задачи"}, 400)
            conn = get_db()
            c = conn.execute(
                "INSERT INTO tasks (title, description, status, priority, created_by, assigned_to, department_id, deadline, parent_task_id, sort_order) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (title, data.get("description", ""), data.get("status", "new"), data.get("priority", "medium"),
                 u["id"], data.get("assigned_to") or None, data.get("department_id") or None, data.get("deadline") or None,
                 data.get("parent_task_id") or None, int(data.get("sort_order") or 0)))
            task_id = c.lastrowid

            # Log task creation with details
            log_activity(conn, task_id, u["id"], "task_created", title, new_value=title)

            # Award km for creating task
            update_km(conn, u["id"], 2)
            stats = conn.execute("SELECT tasks_created FROM user_stats WHERE user_id=?", (u["id"],)).fetchone()
            conn.execute("UPDATE user_stats SET tasks_created = ? WHERE user_id=?", (stats["tasks_created"] + 1, u["id"]))

            # Add watchers
            watchers = data.get("watchers", [])
            for wid in watchers:
                try: conn.execute("INSERT INTO task_watchers (task_id, user_id) VALUES (?,?)", (task_id, wid))
                except: pass

            # Add coexecutors
            coexecutors = data.get("coexecutors", [])
            for cid in coexecutors:
                try: conn.execute("INSERT INTO task_coexecutors (task_id, user_id) VALUES (?,?)", (task_id, cid))
                except: pass

            # Auto-add creator as watcher if assigned to someone else
            assigned_to = data.get("assigned_to")
            if assigned_to and assigned_to != u["id"]:
                try:
                    conn.execute("INSERT INTO task_watchers (task_id, user_id) VALUES (?,?)", (task_id, u["id"]))
                except: pass
                # Notify assigned user
                conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (?,?,?,?)",
                    (assigned_to, task_id, "assigned", f"Вам назначена задача: {title}"))

            # Attach categories (Todoist-style M2M)
            category_ids = data.get("category_ids") or []
            if isinstance(category_ids, list):
                for cid in category_ids:
                    try:
                        conn.execute("INSERT OR IGNORE INTO task_categories (task_id, category_id) VALUES (?,?)",
                                     (task_id, int(cid)))
                    except: pass

            conn.commit(); conn.close()
            return self._json({"ok": True, "id": task_id})

        if path.startswith("/api/tasks/") and "/comments" in path:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            task_id = path.split("/")[3]
            text = (data.get("text") or "").strip()
            attachment_data = data.get("attachment_data") or None
            attachment_name = data.get("attachment_name") or None
            attachment_type = data.get("attachment_type") or None
            # Accept comment if it has text OR an attachment
            if not text and not attachment_data:
                return self._json({"error": "Пустой комментарий"}, 400)
            # Limit attachment size (base64 ~8MB raw = ~11MB encoded)
            if attachment_data and len(attachment_data) > 12_000_000:
                return self._json({"error": "Файл слишком большой (макс 8 МБ)"}, 400)
            conn = get_db()

            # Check access
            task = conn.execute("SELECT created_by, assigned_to, department_id FROM tasks WHERE id=?", (task_id,)).fetchone()
            if not task:
                conn.close()
                return self._json({"error": "Task not found"}, 404)

            user_row = conn.execute("SELECT role, department_id FROM users WHERE id=?", (u["id"],)).fetchone()
            is_admin = user_row["role"] == "admin"
            is_head = user_row["role"] == "head" and user_row["department_id"] == task["department_id"]
            is_participant = u["id"] in [task["created_by"], task["assigned_to"]]
            is_watcher = conn.execute("SELECT 1 FROM task_watchers WHERE task_id=? AND user_id=?", (task_id, u["id"])).fetchone()

            if not (is_admin or is_head or is_participant or is_watcher):
                conn.close()
                return self._json({"error": "Forbidden"}, 403)

            conn.execute(
                "INSERT INTO comments (task_id, user_id, text, attachment_data, attachment_name, attachment_type) VALUES (?,?,?,?,?,?)",
                (task_id, u["id"], text, attachment_data, attachment_name, attachment_type))

            # Log activity
            log_activity(conn, int(task_id), u["id"], "comment_added", text)

            # Award km for commenting
            update_km(conn, u["id"], 1)
            stats = conn.execute("SELECT comments_count FROM user_stats WHERE user_id=?", (u["id"],)).fetchone()
            conn.execute("UPDATE user_stats SET comments_count = ? WHERE user_id=?", (stats["comments_count"] + 1, u["id"]))

            # Check achievement for 50 comments
            check_and_award_achievements(conn, u["id"])

            # Notify watchers and assignee about new comment
            watchers = conn.execute("SELECT user_id FROM task_watchers WHERE task_id=?", (task_id,)).fetchall()
            notify_ids = set([w["user_id"] for w in watchers])
            if task["assigned_to"]: notify_ids.add(task["assigned_to"])
            if task["created_by"]: notify_ids.add(task["created_by"])
            notify_ids.discard(u["id"])  # Don't notify the commenter
            user_name = conn.execute("SELECT full_name FROM users WHERE id=?", (u["id"],)).fetchone()
            task_title = conn.execute("SELECT title FROM tasks WHERE id=?", (task_id,)).fetchone()
            for nid in notify_ids:
                conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (?,?,?,?)",
                    (nid, task_id, "comment", f"{user_name['full_name']} прокомментировал: {task_title['title']}"))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path.startswith("/api/tasks/") and "/watchers" in path:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            task_id = path.split("/")[3]
            watcher_ids = data.get("watcher_ids", [])
            conn = get_db()

            # Get old watchers for activity log
            old_watchers = conn.execute("SELECT user_id FROM task_watchers WHERE task_id=?", (task_id,)).fetchall()
            old_watcher_ids = set([w["user_id"] for w in old_watchers])
            new_watcher_ids = set(watcher_ids)
            added_watchers = new_watcher_ids - old_watcher_ids
            removed_watchers = old_watcher_ids - new_watcher_ids

            # Update watchers
            conn.execute("DELETE FROM task_watchers WHERE task_id=?", (task_id,))
            for wid in watcher_ids:
                try: conn.execute("INSERT INTO task_watchers (task_id, user_id) VALUES (?,?)", (task_id, wid))
                except: pass

            # Log added watchers + send notifications
            for wid in added_watchers:
                user_name = conn.execute("SELECT full_name FROM users WHERE id=?", (wid,)).fetchone()
                log_activity(conn, int(task_id), u["id"], "watcher_added", f"Добавлен наблюдатель: {user_name['full_name']}", new_value=user_name['full_name'])
                # Notify the added watcher
                task_row = conn.execute("SELECT title FROM tasks WHERE id=?", (task_id,)).fetchone()
                if task_row and wid != u["id"]:
                    conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (?,?,?,?)",
                        (wid, task_id, "watcher_added", f"Вы добавлены наблюдателем в задачу: {task_row['title']}"))
            # Log removed watchers
            for wid in removed_watchers:
                user_name = conn.execute("SELECT full_name FROM users WHERE id=?", (wid,)).fetchone()
                log_activity(conn, int(task_id), u["id"], "watcher_removed", f"Убран наблюдатель: {user_name['full_name']}", old_value=user_name['full_name'])

            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path.startswith("/api/tasks/") and "/coexecutors" in path:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            task_id = path.split("/")[3]
            coexecutor_ids = data.get("coexecutor_ids", [])
            conn = get_db()
            old_coexecs = conn.execute("SELECT user_id FROM task_coexecutors WHERE task_id=?", (task_id,)).fetchall()
            old_ids = set([c["user_id"] for c in old_coexecs])
            new_ids = set(coexecutor_ids)
            added = new_ids - old_ids
            removed = old_ids - new_ids
            conn.execute("DELETE FROM task_coexecutors WHERE task_id=?", (task_id,))
            for cid in coexecutor_ids:
                try: conn.execute("INSERT INTO task_coexecutors (task_id, user_id) VALUES (?,?)", (task_id, cid))
                except: pass
            for cid in added:
                user_name = conn.execute("SELECT full_name FROM users WHERE id=?", (cid,)).fetchone()
                log_activity(conn, int(task_id), u["id"], "coexecutor_added", f"Добавлен соисполнитель: {user_name['full_name']}", new_value=user_name['full_name'])
                # Notify the added coexecutor
                task_row = conn.execute("SELECT title FROM tasks WHERE id=?", (task_id,)).fetchone()
                conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (?,?,?,?)",
                    (cid, task_id, "coexecutor_added", f"Вы добавлены соисполнителем в задачу: {task_row['title']}"))
            for cid in removed:
                user_name = conn.execute("SELECT full_name FROM users WHERE id=?", (cid,)).fetchone()
                log_activity(conn, int(task_id), u["id"], "coexecutor_removed", f"Убран соисполнитель: {user_name['full_name']}", old_value=user_name['full_name'])
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path == "/api/notifications/read":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            conn.execute("UPDATE notifications SET is_read=1 WHERE user_id=?", (u["id"],))
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
                row = conn.execute("SELECT total_km FROM user_stats WHERE user_id=?", (u["id"],)).fetchone()
                total_km = row['total_km'] if row else 0
                needed = level_thresholds.get(car_level, 999999)
                if total_km < needed:
                    conn.close()
                    return self._json({"error": "Этот персонаж ещё не разблокирован"}, 403)
            ensure_user_stats(conn, u["id"])
            conn.execute("UPDATE user_stats SET car_override=? WHERE user_id=?", (car_level, u["id"]))
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
                dept = conn_tmp.execute("SELECT id FROM departments WHERE head_user_id=?", (u["id"],)).fetchone()
                conn_tmp.close()
                if not dept or (dept_id and int(dept_id) != dept["id"]):
                    return self._json({"error": "Нет доступа"}, 403)
                if not dept_id:
                    dept_id = dept["id"]
            else:
                return self._json({"error": "Нет доступа"}, 403)
            label = data.get('label', '').strip()
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
                existing = conn.execute("SELECT id FROM funnel_stages WHERE key=? AND department_id=?", (key, dept_id)).fetchone()
            else:
                existing = conn.execute("SELECT id FROM funnel_stages WHERE key=? AND department_id IS NULL", (key,)).fetchone()
            if existing:
                conn.close()
                return self._json({"error": "Этап с таким ключом уже есть"}, 400)
            if dept_id:
                max_order = conn.execute("SELECT MAX(sort_order) FROM funnel_stages WHERE department_id=?", (dept_id,)).fetchone()[0] or 0
            else:
                max_order = conn.execute("SELECT MAX(sort_order) FROM funnel_stages WHERE department_id IS NULL").fetchone()[0] or 0
            conn.execute("INSERT INTO funnel_stages (key, label, color, icon, sort_order, department_id) VALUES (?,?,?,?,?,?)",
                         (key, label, color, icon, max_order + 1, dept_id))
            conn.commit()
            if dept_id:
                new_stage = conn.execute("SELECT * FROM funnel_stages WHERE key=? AND department_id=?", (key, dept_id)).fetchone()
            else:
                new_stage = conn.execute("SELECT * FROM funnel_stages WHERE key=? AND department_id IS NULL", (key,)).fetchone()
            conn.close()
            return self._json({"ok": True, "stage": dict(new_stage)})

        if path == "/api/stages/delete":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            stage_id = data.get('id')
            move_to = data.get('move_to_key', 'new')
            if not stage_id: return self._json({"error": "id обязателен"}, 400)
            conn = get_db()
            stage = conn.execute("SELECT * FROM funnel_stages WHERE id=?", (int(stage_id),)).fetchone()
            if not stage:
                conn.close()
                return self._json({"error": "Этап не найден"}, 404)
            # Permission check: admin can delete any, head can delete their dept's
            if u["role"] == "admin":
                pass  # allowed
            elif u["role"] == "head":
                dept = conn.execute("SELECT id FROM departments WHERE head_user_id=?", (u["id"],)).fetchone()
                if not dept or (stage['department_id'] and stage['department_id'] != dept["id"]):
                    conn.close()
                    return self._json({"error": "Нет доступа"}, 403)
            else:
                conn.close()
                return self._json({"error": "Нет доступа"}, 403)
            # Don't allow deleting if only 2 stages remain in this department
            if stage['department_id']:
                count = conn.execute("SELECT COUNT(*) FROM funnel_stages WHERE department_id=?", (stage['department_id'],)).fetchone()[0]
            else:
                count = conn.execute("SELECT COUNT(*) FROM funnel_stages WHERE department_id IS NULL").fetchone()[0]
            if count <= 2:
                conn.close()
                return self._json({"error": "Нельзя удалить: минимум 2 этапа"}, 400)
            # Move tasks from deleted stage to move_to
            conn.execute("UPDATE tasks SET status=? WHERE status=?", (move_to, stage['key']))
            conn.execute("DELETE FROM funnel_stages WHERE id=?", (int(stage_id),))
            conn.commit()
            conn.close()
            return self._json({"ok": True})

        if path == "/api/stages/update":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            stage_id = data.get('id')
            if not stage_id: return self._json({"error": "id обязателен"}, 400)
            conn = get_db()
            stage = conn.execute("SELECT * FROM funnel_stages WHERE id=?", (int(stage_id),)).fetchone()
            if not stage:
                conn.close()
                return self._json({"error": "Этап не найден"}, 404)
            # Permission check: admin can update any, head can update their dept's
            if u["role"] == "admin":
                pass  # allowed
            elif u["role"] == "head":
                dept = conn.execute("SELECT id FROM departments WHERE head_user_id=?", (u["id"],)).fetchone()
                if not dept or (stage['department_id'] and stage['department_id'] != dept["id"]):
                    conn.close()
                    return self._json({"error": "Нет доступа"}, 403)
            else:
                conn.close()
                return self._json({"error": "Нет доступа"}, 403)
            label = data.get('label', stage['label']).strip()
            color = data.get('color', stage['color'])
            icon = data.get('icon', stage['icon'])
            conn.execute("UPDATE funnel_stages SET label=?, color=?, icon=? WHERE id=?", (label, color, icon, int(stage_id)))
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
                stage = conn.execute("SELECT department_id FROM funnel_stages WHERE id=?", (int(sid),)).fetchone()
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
                dept = conn.execute("SELECT id FROM departments WHERE head_user_id=?", (u["id"],)).fetchone()
                if not dept or (dept_id and dept_id != dept["id"]):
                    conn.close()
                    return self._json({"error": "Нет доступа"}, 403)
            else:
                conn.close()
                return self._json({"error": "Нет доступа"}, 403)
            for idx, sid in enumerate(order):
                conn.execute("UPDATE funnel_stages SET sort_order=? WHERE id=?", (idx, int(sid)))
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
                dept = conn_p.execute("SELECT id FROM departments WHERE head_user_id=?", (user["id"],)).fetchone()
                conn_p.close()
                return bool(dept and dept["id"] == int(cat_dept_id))
            return False

        if path == "/api/categories":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            name = (data.get("name") or "").strip()
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
                parent = conn.execute("SELECT department_id FROM categories WHERE id=?", (parent_id,)).fetchone()
                if not parent:
                    conn.close()
                    return self._json({"error": "Родительская категория не найдена"}, 404)
                dept_id = parent["department_id"]
            max_order = conn.execute(
                "SELECT COALESCE(MAX(sort_order),0) FROM categories WHERE COALESCE(parent_id,0)=COALESCE(?,0)",
                (parent_id,)
            ).fetchone()[0] or 0
            conn.execute(
                "INSERT INTO categories (name, color, icon, sort_order, department_id, parent_id, created_by) VALUES (?,?,?,?,?,?,?)",
                (name, color, icon, max_order + 1, dept_id, parent_id, u["id"])
            )
            new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.commit()
            row = conn.execute("SELECT * FROM categories WHERE id=?", (new_id,)).fetchone()
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
                cat = conn.execute("SELECT department_id FROM categories WHERE id=?", (cid_int,)).fetchone()
                if not cat: continue
                if not _can_edit_category(u, cat["department_id"]):
                    conn.close()
                    return self._json({"error": "Нет прав"}, 403)
                conn.execute("UPDATE categories SET sort_order=? WHERE id=?", (idx, cid_int))
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

            task = conn.execute("SELECT created_at, deadline, assigned_to FROM tasks WHERE id=?", (task_id,)).fetchone()
            if not task or task["assigned_to"] != u["id"]:
                conn.close()
                return self._json({"error": "Invalid task"}, 400)

            # Task completion award: +10 km
            update_km(conn, u["id"], 10)

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
            stats = conn.execute("SELECT tasks_completed FROM user_stats WHERE user_id=?", (u["id"],)).fetchone()
            conn.execute("UPDATE user_stats SET tasks_completed = ? WHERE user_id=?", (stats["tasks_completed"] + 1, u["id"]))

            # Speed demon: complete task same day as created
            if created_date == today:
                existing = conn.execute("SELECT id FROM achievements WHERE user_id=? AND type='speed_demon'", (u["id"],)).fetchone()
                if not existing:
                    try:
                        conn.execute("INSERT INTO achievements (user_id, type, name, description) VALUES (?,?,?,?)",
                            (u["id"], "speed_demon", "Быстрый круг", "Завершите задачу в день создания"))
                    except sqlite3.IntegrityError:
                        pass

            # Update streak_days
            user_stats = conn.execute("SELECT last_active, streak_days FROM user_stats WHERE user_id=?", (u["id"],)).fetchone()
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

                conn.execute("UPDATE user_stats SET last_active=?, streak_days=? WHERE user_id=?",
                    (today_date, new_streak, u["id"]))

            # Check achievements
            check_and_award_achievements(conn, u["id"])

            # Department star: complete 5 tasks in one day
            today_tasks = conn.execute(
                "SELECT COUNT(*) as cnt FROM tasks WHERE assigned_to=? AND status='done' AND DATE(updated_at)=?",
                (u["id"], today_date)
            ).fetchone()
            if today_tasks and today_tasks["cnt"] >= 5:
                existing = conn.execute("SELECT id FROM achievements WHERE user_id=? AND type='department_star'", (u["id"],)).fetchone()
                if not existing:
                    try:
                        conn.execute("INSERT INTO achievements (user_id, type, name, description) VALUES (?,?,?,?)",
                            (u["id"], "department_star", "Звезда отдела", "5+ задач в один день"))
                    except sqlite3.IntegrityError:
                        pass

            # Early bird: complete task before deadline
            if task["deadline"]:
                today = datetime.now().strftime("%Y-%m-%d")
                if today <= task["deadline"]:
                    existing = conn.execute("SELECT id FROM achievements WHERE user_id=? AND type='early_bird'", (u["id"],)).fetchone()
                    if not existing:
                        try:
                            conn.execute("INSERT INTO achievements (user_id, type, name, description) VALUES (?,?,?,?)",
                                (u["id"], "early_bird", "Ранняя пташка", "Задача до дедлайна"))
                        except sqlite3.IntegrityError:
                            pass

            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path == "/api/admin/departments":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=?", (u["id"],)).fetchone()
            if not user_role or user_role["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden: admin only"}, 403)

            name = data.get("name", "").strip()
            if not name:
                conn.close()
                return self._json({"error": "Department name required"}, 400)

            # Check name uniqueness
            if conn.execute("SELECT id FROM departments WHERE name=?", (name,)).fetchone():
                conn.close()
                return self._json({"error": "Department name must be unique"}, 400)

            color = data.get("color", "#1a1a1a")
            head_user_id = data.get("head_user_id")

            c = conn.execute("INSERT INTO departments (name, color, head_user_id) VALUES (?,?,?)",
                           (name, color, head_user_id if head_user_id else None))
            dept_id = c.lastrowid

            # If head_user_id provided, set user's role to 'head'
            if head_user_id:
                conn.execute("UPDATE users SET role=? WHERE id=?", ("head", head_user_id))

            conn.commit(); conn.close()
            return self._json({"ok": True, "id": dept_id})

        if path == "/api/admin/permissions":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=?", (u["id"],)).fetchone()
            if not user_role or user_role["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden"}, 403)
            permissions = data.get('permissions', {})
            for role in ['admin', 'head', 'member']:
                if role in permissions:
                    for perm, allowed in permissions[role].items():
                        conn.execute(
                            "INSERT OR REPLACE INTO role_permissions (role, permission, allowed) VALUES (?,?,?)",
                            (role, perm, 1 if allowed else 0)
                        )
            conn.commit()
            conn.close()
            return self._json({"ok": True})

        if path == "/api/admin/clean-orphaned":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=?", (u["id"],)).fetchone()
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
            text = data.get("text", "").strip()
            rating = data.get("rating", 0)
            if not text: return self._json({"error": "Текст обязателен"}, 400)
            conn = get_db()
            conn.execute("INSERT INTO feedback (user_id, text, rating) VALUES (?,?,?)", (u["id"], text, rating))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        self.send_response(404); self.end_headers()

    def do_PUT(self):
        path = urllib.parse.urlparse(self.path).path
        data = self._body()

        if path == "/api/profile":
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            full_name = data.get("full_name", "").strip()
            if not full_name: return self._json({"error": "Имя не может быть пустым"}, 400)
            conn = get_db()
            conn.execute("UPDATE users SET full_name=? WHERE id=?", (full_name, u["id"]))
            conn.commit()
            conn.close()
            return self._json({"ok": True})

        if path.startswith("/api/users/") and "/role" in path:
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=?", (u["id"],)).fetchone()
            if not user_role or user_role["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden: admin only"}, 403)
            user_id = path.split("/")[3]
            new_role = data.get("role")
            if new_role not in ["admin", "head", "member"]:
                conn.close()
                return self._json({"error": "Invalid role"}, 400)
            conn.execute("UPDATE users SET role=? WHERE id=?", (new_role, user_id))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path.startswith("/api/tasks/"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            task_id = path.split("/")[3]
            conn = get_db()
            old_task = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            sets, params = [], []
            for field in ["title", "description", "status", "priority", "assigned_to", "department_id", "deadline", "sort_order", "parent_task_id"]:
                if field in data:
                    sets.append(f"{field} = ?")
                    params.append(data[field] if data[field] != "" else None)
            if sets:
                sets.append("updated_at = CURRENT_TIMESTAMP")
                params.append(task_id)
                conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id = ?", params)

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
                                    ov_user = conn.execute("SELECT full_name FROM users WHERE id=?", (old_val,)).fetchone() if old_val else None
                                    nv_user = conn.execute("SELECT full_name FROM users WHERE id=?", (new_val,)).fetchone() if new_val else None
                                    ov = ov_user["full_name"] if ov_user else "—"
                                    nv = nv_user["full_name"] if nv_user else "—"
                                elif field == "department_id":
                                    ov_dept = conn.execute("SELECT name FROM departments WHERE id=?", (old_val,)).fetchone() if old_val else None
                                    nv_dept = conn.execute("SELECT name FROM departments WHERE id=?", (new_val,)).fetchone() if new_val else None
                                    ov = ov_dept["name"] if ov_dept else "—"
                                    nv = nv_dept["name"] if nv_dept else "—"
                                else:
                                    ov = str(old_val) if old_val else "—"
                                    nv = str(new_val) if new_val else "—"
                                log_activity(conn, int(task_id), u["id"], f"{field}_changed", None, old_value=ov, new_value=nv)

                # Sync categories (Todoist-style M2M) if provided
                if "category_ids" in data and isinstance(data.get("category_ids"), list):
                    conn.execute("DELETE FROM task_categories WHERE task_id=?", (task_id,))
                    for cid in data["category_ids"]:
                        try:
                            conn.execute("INSERT OR IGNORE INTO task_categories (task_id, category_id) VALUES (?,?)",
                                         (task_id, int(cid)))
                        except: pass

                # Status change notifications
                if "status" in data and old_task and data["status"] != old_task["status"]:
                    msg = f"Статус изменён на «{status_names.get(data['status'], data['status'])}»: {old_task['title']}"
                    watchers = conn.execute("SELECT user_id FROM task_watchers WHERE task_id=?", (task_id,)).fetchall()
                    notify_ids = set([w["user_id"] for w in watchers])
                    coexecs = conn.execute("SELECT user_id FROM task_coexecutors WHERE task_id=?", (task_id,)).fetchall()
                    notify_ids.update([c["user_id"] for c in coexecs])
                    if old_task["assigned_to"]: notify_ids.add(old_task["assigned_to"])
                    if old_task["created_by"]: notify_ids.add(old_task["created_by"])
                    notify_ids.discard(u["id"])
                    for nid in notify_ids:
                        conn.execute("INSERT INTO notifications (user_id, task_id, type, message) VALUES (?,?,?,?)",
                            (nid, task_id, "status_change", msg))

                conn.commit()
            conn.close()
            return self._json({"ok": True})

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
            cat = conn.execute("SELECT * FROM categories WHERE id=?", (cat_id,)).fetchone()
            if not cat:
                conn.close()
                return self._json({"error": "Категория не найдена"}, 404)

            # Permission check using current department_id
            def _can(user, dept):
                if user.get("role") == "admin": return True
                if user.get("role") == "head":
                    if dept is None: return False
                    d = conn.execute("SELECT id FROM departments WHERE head_user_id=?", (user["id"],)).fetchone()
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
                sets.append("name=?"); params.append(name)
            if "color" in data:
                sets.append("color=?"); params.append(data.get("color") or "#3b82f6")
            if "icon" in data:
                sets.append("icon=?"); params.append(data.get("icon") or "")
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
                        parent_row = conn.execute("SELECT parent_id FROM categories WHERE id=?", (cur,)).fetchone()
                        if not parent_row: break
                        if parent_row["parent_id"] == cat_id:
                            conn.close()
                            return self._json({"error": "Цикл в дереве категорий"}, 400)
                        cur = parent_row["parent_id"]
                        safety += 1
                    # Inherit department from new parent
                    par = conn.execute("SELECT department_id FROM categories WHERE id=?", (pid,)).fetchone()
                    if par:
                        sets.append("department_id=?"); params.append(par["department_id"])
                sets.append("parent_id=?"); params.append(pid)
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
                    sets.append("department_id=?"); params.append(did)
                    # Propagate to all descendants
                    # (collect descendants iteratively)
                    to_update = [cat_id]
                    descendants = []
                    while to_update:
                        next_batch = []
                        for pid_ in to_update:
                            children = conn.execute("SELECT id FROM categories WHERE parent_id=?", (pid_,)).fetchall()
                            for ch in children:
                                descendants.append(ch["id"])
                                next_batch.append(ch["id"])
                        to_update = next_batch
                    for d_id in descendants:
                        conn.execute("UPDATE categories SET department_id=? WHERE id=?", (did, d_id))
            if sets:
                params.append(cat_id)
                conn.execute(f"UPDATE categories SET {', '.join(sets)} WHERE id=?", params)
                conn.commit()
            row = conn.execute("SELECT * FROM categories WHERE id=?", (cat_id,)).fetchone()
            conn.close()
            return self._json({"ok": True, "category": dict(row)})

        if path.startswith("/api/admin/departments/"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            user_role = conn.execute("SELECT role FROM users WHERE id=?", (u["id"],)).fetchone()
            if not user_role or user_role["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden: admin only"}, 403)

            dept_id = path.split("/")[4]
            dept = conn.execute("SELECT * FROM departments WHERE id=?", (dept_id,)).fetchone()
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
                existing = conn.execute("SELECT id FROM departments WHERE name=? AND id!=?", (name, dept_id)).fetchone()
                if existing:
                    conn.close()
                    return self._json({"error": "Department name must be unique"}, 400)
                sets.append("name = ?")
                params.append(name)

            if "color" in data:
                sets.append("color = ?")
                params.append(data["color"])

            # Handle head_user_id change
            if "head_user_id" in data:
                new_head_id = data["head_user_id"]
                old_head_id = dept["head_user_id"]

                # If different from current head, update roles
                if new_head_id and new_head_id != old_head_id:
                    # If there was an old head, revert them to member
                    if old_head_id:
                        conn.execute("UPDATE users SET role=? WHERE id=?", ("member", old_head_id))
                    # Set new head
                    conn.execute("UPDATE users SET role=? WHERE id=?", ("head", new_head_id))

                sets.append("head_user_id = ?")
                params.append(new_head_id)

            if sets:
                params.append(dept_id)
                conn.execute(f"UPDATE departments SET {', '.join(sets)} WHERE id = ?", params)
                conn.commit()

            conn.close()
            return self._json({"ok": True})

        # Edit direct message
        self.send_response(404); self.end_headers()

    def do_DELETE(self):
        path = urllib.parse.urlparse(self.path).path
        if path.startswith("/api/tasks/"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            task_id = path.split("/")[3]
            conn = get_db()
            conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            conn.commit(); conn.close()
            return self._json({"ok": True})

        if path.startswith("/api/users/"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            me = conn.execute("SELECT role FROM users WHERE id=?", (u["id"],)).fetchone()
            if not me or me["role"] != "admin":
                conn.close()
                return self._json({"error": "Только администратор может удалять пользователей"}, 403)
            user_id = path.split("/")[3]
            if int(user_id) == u["id"]:
                conn.close()
                return self._json({"error": "Нельзя удалить самого себя"}, 400)
            # Find all tasks created by this user (to clean up their dependencies)
            created_tasks = [r[0] for r in conn.execute("SELECT id FROM tasks WHERE created_by=?", (user_id,)).fetchall()]
            for tid in created_tasks:
                conn.execute("DELETE FROM task_watchers WHERE task_id=?", (tid,))
                conn.execute("DELETE FROM comments WHERE task_id=?", (tid,))
                conn.execute("DELETE FROM task_messages WHERE task_id=?", (tid,))
                conn.execute("DELETE FROM task_activity WHERE task_id=?", (tid,))
                conn.execute("DELETE FROM notifications WHERE task_id=?", (tid,))
            conn.execute("DELETE FROM tasks WHERE created_by=?", (user_id,))
            # Delete user's other related data
            conn.execute("DELETE FROM task_watchers WHERE user_id=?", (user_id,))
            conn.execute("DELETE FROM comments WHERE user_id=?", (user_id,))
            conn.execute("DELETE FROM notifications WHERE user_id=?", (user_id,))
            conn.execute("DELETE FROM task_messages WHERE sender_id=? OR recipient_id=?", (user_id, user_id))
            conn.execute("DELETE FROM task_activity WHERE user_id=?", (user_id,))
            conn.execute("DELETE FROM direct_messages WHERE sender_id=? OR recipient_id=?", (user_id, user_id))
            conn.execute("DELETE FROM user_stats WHERE user_id=?", (user_id,))
            conn.execute("DELETE FROM achievements WHERE user_id=?", (user_id,))
            # Unassign from remaining tasks
            conn.execute("UPDATE tasks SET assigned_to=NULL WHERE assigned_to=?", (user_id,))
            conn.execute("DELETE FROM users WHERE id=?", (user_id,))
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
            cat = conn.execute("SELECT * FROM categories WHERE id=?", (cat_id,)).fetchone()
            if not cat:
                conn.close()
                return self._json({"error": "Категория не найдена"}, 404)

            # Permission check
            dept = cat["department_id"]
            can = False
            if u.get("role") == "admin":
                can = True
            elif u.get("role") == "head" and dept is not None:
                d = conn.execute("SELECT id FROM departments WHERE head_user_id=?", (u["id"],)).fetchone()
                can = bool(d and d["id"] == int(dept))
            if not can:
                conn.close()
                return self._json({"error": "Нет прав на удаление этой категории"}, 403)

            # CASCADE handles children categories AND task_categories rows.
            conn.execute("DELETE FROM categories WHERE id=?", (cat_id,))
            conn.commit()
            conn.close()
            return self._json({"ok": True})

        if path.startswith("/api/admin/departments/"):
            u = self._user()
            if not u: return self._json({"error": "unauthorized"}, 401)
            conn = get_db()
            me = conn.execute("SELECT role FROM users WHERE id=?", (u["id"],)).fetchone()
            if not me or me["role"] != "admin":
                conn.close()
                return self._json({"error": "Forbidden: admin only"}, 403)

            dept_id = path.split("/")[4]
            dept = conn.execute("SELECT * FROM departments WHERE id=?", (dept_id,)).fetchone()
            if not dept:
                conn.close()
                return self._json({"error": "Department not found"}, 404)

            # Set all users in this department to department_id=NULL
            conn.execute("UPDATE users SET department_id=NULL WHERE department_id=?", (dept_id,))

            # Set all tasks with this department_id to NULL
            conn.execute("UPDATE tasks SET department_id=NULL WHERE department_id=?", (dept_id,))

            # Delete the department
            conn.execute("DELETE FROM departments WHERE id=?", (dept_id,))

            conn.commit(); conn.close()
            return self._json({"ok": True})

        # Delete entire conversation with a user
        self.send_response(404); self.end_headers()

if __name__ == "__main__":
    import ssl
    import subprocess
    init_db()
    generate_deadline_notifications()

    # Support PORT env variable for cloud hosting (Render, Railway, etc.)
    PORT_HTTP = int(os.environ.get("PORT", 8080))
    CLOUD_MODE = "PORT" in os.environ  # If PORT is set, we're on cloud hosting

    if CLOUD_MODE:
        # Cloud mode: HTTP only (hosting provides SSL)
        server_http = http.server.HTTPServer(("0.0.0.0", PORT_HTTP), TaskManagerHandler)
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

        server_http = http.server.HTTPServer(("0.0.0.0", PORT_HTTP), TaskManagerHandler)
        server_https = http.server.HTTPServer(("0.0.0.0", PORT_HTTPS), TaskManagerHandler)
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
