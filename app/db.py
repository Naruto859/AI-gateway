"""SQLite store for settings, proxies, keywords, and request logs.

All secrets (API key, proxy list) live here in a local file (data.db) that is
gitignored — never in source or memory.
"""
import os
import time
import sqlite3
import threading

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE, "data.db")

_lock = threading.Lock()
_conn = None

# columns that update_proxy is allowed to set
_PROXY_COLS = {
    "url", "label", "enabled", "status", "exit_ip", "latency_ms",
    "fail_count", "success_count", "last_used", "last_checked", "note",
}

DEFAULT_SETTINGS = {
    "endpoint": "https://agentrouter.org",       # upstream base; request path is appended
    "gateway_key": "",                            # key clients must present to THIS gateway
    "upstream_key": "",                           # key the gateway sends upstream (defaults to gateway_key)
    "require_client_key": "1",                    # 1 = clients must send the gateway_key
    "admin_password": "claude",                   # dashboard password (CHANGE THIS)
    "model_note": "claude-opus-4-8",              # informational
    "max_retries": "4",                           # max proxies to try per request
    "connect_timeout": "20",
    "read_timeout": "300",
    "user_agent": "claude-cli/1.0.60 (external, cli)",
}


def conn():
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _init(_conn)
    return _conn


def _init(c):
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS proxies (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            url           TEXT UNIQUE NOT NULL,
            label         TEXT    DEFAULT '',
            enabled       INTEGER DEFAULT 1,
            status        TEXT    DEFAULT 'unknown',   -- unknown|ok|banned|unhealthy
            exit_ip       TEXT    DEFAULT '',
            latency_ms    INTEGER DEFAULT 0,
            fail_count    INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            last_used     REAL    DEFAULT 0,
            last_checked  REAL    DEFAULT 0,
            note          TEXT    DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS keywords (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            value       TEXT NOT NULL,
            mode        TEXT    DEFAULT 'redact',      -- redact|block
            replacement TEXT    DEFAULT '[REDACTED]',
            enabled     INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS logs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ts         REAL,
            method     TEXT,
            path       TEXT,
            status     INTEGER,
            proxy      TEXT,
            attempts   INTEGER,
            stream     INTEGER,
            redactions INTEGER,
            ms         INTEGER,
            note       TEXT
        );
        """
    )
    c.commit()
    # seed default settings (only missing keys)
    for k, v in DEFAULT_SETTINGS.items():
        c.execute("INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)", (k, v))
    c.commit()


# ----- settings -----
def get_setting(key, default=""):
    with _lock:
        row = conn().execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key, value):
    with _lock:
        c = conn()
        c.execute(
            "INSERT INTO settings(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        c.commit()


def get_all_settings():
    with _lock:
        rows = conn().execute("SELECT key, value FROM settings").fetchall()
    return {r["key"]: r["value"] for r in rows}


# ----- proxies -----
def list_proxies():
    with _lock:
        return [dict(r) for r in conn().execute(
            "SELECT * FROM proxies ORDER BY id").fetchall()]


def get_proxy(pid):
    with _lock:
        row = conn().execute("SELECT * FROM proxies WHERE id=?", (pid,)).fetchone()
    return dict(row) if row else None


def add_proxy(url, label=""):
    with _lock:
        c = conn()
        c.execute("INSERT OR IGNORE INTO proxies(url, label) VALUES (?, ?)", (url, label))
        c.commit()
        return c.total_changes


def bulk_add(urls):
    added = 0
    with _lock:
        c = conn()
        for u in urls:
            before = c.total_changes
            c.execute("INSERT OR IGNORE INTO proxies(url) VALUES (?)", (u,))
            added += (c.total_changes - before)
        c.commit()
    return added


def update_proxy(pid, **fields):
    fields = {k: v for k, v in fields.items() if k in _PROXY_COLS}
    if not fields:
        return
    cols = ", ".join(f"{k}=?" for k in fields)
    vals = list(fields.values()) + [pid]
    with _lock:
        c = conn()
        c.execute(f"UPDATE proxies SET {cols} WHERE id=?", vals)
        c.commit()


def delete_proxy(pid):
    with _lock:
        c = conn()
        c.execute("DELETE FROM proxies WHERE id=?", (pid,))
        c.commit()


# ----- keywords -----
def list_keywords():
    with _lock:
        return [dict(r) for r in conn().execute(
            "SELECT * FROM keywords ORDER BY id").fetchall()]


def add_keyword(value, mode="redact", replacement="[REDACTED]"):
    with _lock:
        c = conn()
        cur = c.execute(
            "INSERT INTO keywords(value, mode, replacement) VALUES (?, ?, ?)",
            (value, mode, replacement or "[REDACTED]"),
        )
        c.commit()
        return cur.lastrowid


def update_keyword(kid, **fields):
    allowed = {"value", "mode", "replacement", "enabled"}
    fields = {k: v for k, v in fields.items() if k in allowed}
    if not fields:
        return
    cols = ", ".join(f"{k}=?" for k in fields)
    vals = list(fields.values()) + [kid]
    with _lock:
        c = conn()
        c.execute(f"UPDATE keywords SET {cols} WHERE id=?", vals)
        c.commit()


def delete_keyword(kid):
    with _lock:
        c = conn()
        c.execute("DELETE FROM keywords WHERE id=?", (kid,))
        c.commit()


# ----- logs -----
def add_log(**f):
    with _lock:
        c = conn()
        c.execute(
            "INSERT INTO logs(ts, method, path, status, proxy, attempts, stream, redactions, ms, note) "
            "VALUES (:ts, :method, :path, :status, :proxy, :attempts, :stream, :redactions, :ms, :note)",
            {
                "ts": f.get("ts", time.time()),
                "method": f.get("method", ""),
                "path": f.get("path", ""),
                "status": f.get("status", 0),
                "proxy": f.get("proxy", ""),
                "attempts": f.get("attempts", 0),
                "stream": f.get("stream", 0),
                "redactions": f.get("redactions", 0),
                "ms": f.get("ms", 0),
                "note": f.get("note", ""),
            },
        )
        # keep last 500
        c.execute("DELETE FROM logs WHERE id NOT IN (SELECT id FROM logs ORDER BY id DESC LIMIT 500)")
        c.commit()


def recent_logs(limit=100):
    with _lock:
        return [dict(r) for r in conn().execute(
            "SELECT * FROM logs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]


def clear_logs():
    with _lock:
        c = conn()
        c.execute("DELETE FROM logs")
        c.commit()
