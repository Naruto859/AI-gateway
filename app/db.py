"""SQLite store for settings, proxies, keywords, and request logs.

All secrets (API key, proxy list) live here in a local file (data.db) that is
gitignored — never in source or memory.
"""
import os
import time
import sqlite3
import threading

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE, "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "data.db")

_lock = threading.Lock()
_conn = None

# columns that update_proxy is allowed to set
_PROXY_COLS = {
    "url", "label", "ptype", "enabled", "status", "exit_ip", "latency_ms",
    "fail_count", "success_count", "last_used", "last_checked", "note",
}

DEFAULT_SETTINGS = {
    "endpoint": "https://agentrouter.org",       # upstream base; request path is appended
    "gateway_key": "",                            # key clients must present to THIS gateway
    "upstream_key": "",                           # key the gateway sends upstream (defaults to gateway_key)
    "require_client_key": "1",                    # 1 = clients must send the gateway_key
    "admin_password": "12345678",                 # dashboard password (CHANGE THIS)
    "model_note": "claude-opus-4-8",              # informational
    "max_retries": "10",                          # max attempts per request (same proxy or rotation)
    "connect_timeout": "20",                      # seconds to establish the proxy connection
    "read_timeout": "1200",                       # seconds to wait for upstream bytes (long generations)
    "user_agent": "claude-cli/2.1.177 (external, cli)",  # Claude Code fingerprint (agentrouter requires this)
    "dedicated_proxy_id": "",                     # pin ALL requests to one proxy ("" = first enabled)
    "dedicated_strict": "0",                      # legacy; unused when auto_rotation drives selection
    "auto_rotation": "0",                         # 1 = rotate across enabled proxies on fail; 0 = single dedicated
    "hot_pool_enabled": "1",                      # 1 = hot pool engine active; 0 = off (for premium proxies)
    "hot_pool_api_test": "1",                     # 1 = HTTP API GET test; 0 = simple TCP connection test
    "hedging_concurrency": "3",                   # Number of proxies to race in parallel for a request
    "hot_pool_size": "100",                       # number of verified proxies to keep in hot pool
    "hot_pool_refresh": "5",                      # seconds between hot pool refresh cycles
    "hot_pool_test_timeout": "3.0",               # timeout in seconds for hot pool tests
    "hot_pool_concurrency": "20",                 # max concurrent proxy tests during hot pool refresh
    # --- retry backoff (Claude Code SDK formula: min(initial*2^n, max) + jitter) ---
    "retry_initial_delay": "1.0",                 # seconds before first retry
    "retry_max_delay": "8.0",                     # cap on the backoff delay
    "failover_5xx_threshold": "3",                # consecutive 5xx errors before assuming endpoint is down
    "global_model_override": "",                  # Override model id globally
    "claude_mimicry": "1",                        # 1 = Full A-to-Z node/claude-cli mimicry
    # --- TCP keepalive on the proxy tunnel (covers slow upstream "thinking") ---
    "keepalive_idle": "30",                       # seconds idle before first probe
    "keepalive_intvl": "5",                       # seconds between probes
    "keepalive_cnt": "174",                       # probe count (idle + intvl*cnt = total coverage)
}


def conn():
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.execute('PRAGMA journal_mode=WAL')
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
            ptype         TEXT    DEFAULT 'http',        -- http|https|socks5|socks4
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
            note       TEXT,
            req_body   TEXT
        );
        CREATE TABLE IF NOT EXISTS endpoints (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            url       TEXT UNIQUE NOT NULL,
            api_mode  TEXT    DEFAULT 'anthropic_messages',  -- anthropic_messages|chat_completions
            api_key   TEXT    DEFAULT '',                    -- '' = use global upstream_key
            enabled   INTEGER DEFAULT 1,
            priority  INTEGER DEFAULT 0,                     -- lower = tried first
            status    TEXT    DEFAULT 'unknown',
            note      TEXT    DEFAULT '',
            model_override TEXT DEFAULT '',
            failover_trigger_keywords TEXT DEFAULT '500,501,502,503,504'
        );
        CREATE TABLE IF NOT EXISTS api_keys (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT UNIQUE NOT NULL,   -- e.g. "mugen_a3f9k2x8q1"
            key        TEXT NOT NULL,          -- the secret clients send (== name here)
            label      TEXT    DEFAULT '',     -- human note ("my phone", "n8n")
            created_at REAL    DEFAULT 0,
            last_used  REAL    DEFAULT 0,
            hit_count  INTEGER DEFAULT 0,
            enabled    INTEGER DEFAULT 1
        );
        """
    )
    c.commit()
    # seed default settings (only missing keys)
    for k, v in DEFAULT_SETTINGS.items():
        c.execute("INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)", (k, v))
    c.commit()
    # --- migrations: add columns introduced after first deploy ---
    cols = [r[1] for r in c.execute("PRAGMA table_info(proxies)").fetchall()]
    if "ptype" not in cols:
        c.execute("ALTER TABLE proxies ADD COLUMN ptype TEXT DEFAULT 'http'")
        # infer type from existing url scheme
        for row in c.execute("SELECT id, url FROM proxies").fetchall():
            u = row[1] or ""
            t = "http"
            for scheme in ("socks5h", "socks5", "socks4", "https", "http"):
                if u.startswith(scheme + "://"):
                    t = "socks5" if scheme == "socks5h" else scheme
                    break
            c.execute("UPDATE proxies SET ptype=? WHERE id=?", (t, row[0]))
    # endpoints: add is_primary + name introduced for primary-select / labels
    ecols = [r[1] for r in c.execute("PRAGMA table_info(endpoints)").fetchall()]
    if "is_primary" not in ecols:
        c.execute("ALTER TABLE endpoints ADD COLUMN is_primary INTEGER DEFAULT 0")
    if "name" not in ecols:
        c.execute("ALTER TABLE endpoints ADD COLUMN name TEXT DEFAULT ''")
    if "model_override" not in ecols:
        c.execute("ALTER TABLE endpoints ADD COLUMN model_override TEXT DEFAULT ''")
    if "failover_trigger_keywords" not in ecols:
        c.execute("ALTER TABLE endpoints ADD COLUMN failover_trigger_keywords TEXT DEFAULT '500,501,502,503,504'")
    # logs: add ip / model / detail for the log-detail view
    lcols = [r[1] for r in c.execute("PRAGMA table_info(logs)").fetchall()]
    if "ip" not in lcols:
        c.execute("ALTER TABLE logs ADD COLUMN ip TEXT DEFAULT ''")
    if "model" not in lcols:
        c.execute("ALTER TABLE logs ADD COLUMN model TEXT DEFAULT ''")
    if "detail" not in lcols:
        c.execute("ALTER TABLE logs ADD COLUMN detail TEXT DEFAULT ''")
    if "endpoint" not in lcols:
        c.execute("ALTER TABLE logs ADD COLUMN endpoint TEXT DEFAULT ''")
    if "source" not in lcols:
        # '' = real routed traffic, 'test' = endpoint test/chat, 'agent' = embedded agent
        c.execute("ALTER TABLE logs ADD COLUMN source TEXT DEFAULT ''")
    if "req_body" not in lcols:
        c.execute("ALTER TABLE logs ADD COLUMN req_body TEXT DEFAULT ''")
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
def list_proxies(limit=None):
    with _lock:
        if limit:
            return [dict(r) for r in conn().execute("SELECT * FROM proxies ORDER BY id LIMIT ?", (limit,)).fetchall()]
        return [dict(r) for r in conn().execute(
            "SELECT * FROM proxies ORDER BY id").fetchall()]

def count_proxies(enabled_only=False):
    with _lock:
        if enabled_only:
            return conn().execute("SELECT COUNT(*) FROM proxies WHERE enabled=1").fetchone()[0]
        return conn().execute("SELECT COUNT(*) FROM proxies").fetchone()[0]

def proxy_counts():
    with _lock:
        rows = conn().execute("SELECT status, COUNT(*) as cnt FROM proxies GROUP BY status").fetchall()
        total = conn().execute("SELECT COUNT(*) FROM proxies").fetchone()[0]
        enabled = conn().execute("SELECT COUNT(*) FROM proxies WHERE enabled=1").fetchone()[0]
        res = {"total": total, "enabled": enabled, "ok": 0, "banned": 0, "unhealthy": 0, "unknown": 0}
        for r in rows:
            if r["status"] in res:
                res[r["status"]] = r["cnt"]
        return res

def get_best_proxies(limit=10, exclude_ids=None):
    if not exclude_ids:
        exclude_ids = [-1]
    placeholders = ",".join("?" for _ in exclude_ids)
    query = f"""
        SELECT * FROM proxies 
        WHERE enabled=1 AND status != 'banned' AND id NOT IN ({placeholders})
        ORDER BY 
            CASE status
                WHEN 'ok' THEN 0
                WHEN 'unknown' THEN 1
                WHEN 'unhealthy' THEN 2
                ELSE 3
            END ASC,
            latency_ms ASC
        LIMIT ?
    """
    with _lock:
        params = tuple(exclude_ids) + (limit,)
        return [dict(r) for r in conn().execute(query, params).fetchall()]

def get_batch_test_candidates(limit=10):
    query = """
        SELECT * FROM proxies
        ORDER BY 
            CASE 
                WHEN enabled=1 AND status IN ('ok', 'unknown') THEN 0
                WHEN enabled=1 AND status IN ('unhealthy', 'banned') THEN 1
                ELSE 2
            END ASC,
            last_checked ASC
        LIMIT ?
    """
    with _lock:
        return [dict(r) for r in conn().execute(query, (limit,)).fetchall()]

def get_hot_pool_candidates(limit=50):
    """Get top proxy candidates for hot pool testing, sorted by latency.
    
    Includes ALL statuses so the hot pool can rediscover recovered proxies.
    """
    query = """
        SELECT * FROM proxies
        ORDER BY 
            CASE status
                WHEN 'ok' THEN 0
                WHEN 'unknown' THEN 1
                WHEN 'unhealthy' THEN 2
                WHEN 'banned' THEN 3
                ELSE 4
            END ASC,
            latency_ms ASC
        LIMIT ?
    """
    with _lock:
        return [dict(r) for r in conn().execute(query, (limit,)).fetchall()]

def delete_old_disabled_proxies(cutoff_time):
    with _lock:
        c = conn()
        cur = c.execute("DELETE FROM proxies WHERE enabled=0 AND last_checked > 0 AND last_checked < ?", (cutoff_time,))
        c.commit()
        return cur.rowcount


def get_proxy(pid):
    with _lock:
        row = conn().execute("SELECT * FROM proxies WHERE id=?", (pid,)).fetchone()
    return dict(row) if row else None


def add_proxy(url, label="", ptype="http"):
    with _lock:
        c = conn()
        c.execute("INSERT OR IGNORE INTO proxies(url, label, ptype) VALUES (?, ?, ?)", (url, label, ptype))
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
            "INSERT INTO logs(ts, method, path, status, proxy, attempts, stream, redactions, ms, note, ip, model, detail, endpoint, source, req_body) "
            "VALUES (:ts, :method, :path, :status, :proxy, :attempts, :stream, :redactions, :ms, :note, :ip, :model, :detail, :endpoint, :source, :req_body)",
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
                "ip": f.get("ip", ""),
                "model": f.get("model", ""),
                "detail": f.get("detail", ""),
                "endpoint": f.get("endpoint", ""),
                "source": f.get("source", ""),
                "req_body": f.get("req_body", ""),
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


# ----- endpoints (multi-provider failover) -----
def list_endpoints():
    with _lock:
        return [dict(r) for r in conn().execute(
            "SELECT * FROM endpoints ORDER BY priority, id").fetchall()]


def add_endpoint(url, api_mode="anthropic_messages", api_key="", model_override=""):
    with _lock:
        c = conn()
        # new endpoint goes last in priority
        nxt = c.execute("SELECT COALESCE(MAX(priority),0)+1 FROM endpoints").fetchone()[0]
        c.execute("INSERT OR IGNORE INTO endpoints(url, api_mode, api_key, model_override, priority) VALUES (?,?,?,?,?)",
                  (url, api_mode, api_key, model_override, nxt))
        c.commit()
        return c.total_changes


def update_endpoint(eid, **fields):
    allowed_ENDPOINT_COLS = {"url", "api_mode", "api_key", "enabled", "priority", "status", "note", "is_primary", "name", "model_override", "failover_trigger_keywords"}
    fields = {k: v for k, v in fields.items() if k in allowed_ENDPOINT_COLS}
    if not fields:
        return
    cols = ", ".join(f"{k}=?" for k in fields)
    with _lock:
        c = conn()
        c.execute(f"UPDATE endpoints SET {cols} WHERE id=?", list(fields.values()) + [eid])
        c.commit()


def delete_endpoint(eid):
    with _lock:
        c = conn()
        c.execute("DELETE FROM endpoints WHERE id=?", (eid,))
        c.commit()


def set_primary_endpoint(eid):
    """Mark one endpoint primary (clears the flag on all others). eid=0 -> none."""
    with _lock:
        c = conn()
        c.execute("UPDATE endpoints SET is_primary=0")
        if eid:
            c.execute("UPDATE endpoints SET is_primary=1, enabled=1 WHERE id=?", (eid,))
        c.commit()


# ----- api keys (client-facing keys the gateway accepts) -----
def list_api_keys():
    with _lock:
        return [dict(r) for r in conn().execute(
            "SELECT * FROM api_keys ORDER BY id DESC").fetchall()]


def add_api_key(name, key, label=""):
    with _lock:
        c = conn()
        c.execute(
            "INSERT OR IGNORE INTO api_keys(name, key, label, created_at) VALUES (?,?,?,?)",
            (name, key, label, time.time()))
        c.commit()
        return c.total_changes


def delete_api_key(kid):
    with _lock:
        c = conn()
        c.execute("DELETE FROM api_keys WHERE id=?", (kid,))
        c.commit()


def get_api_key_by_value(key):
    with _lock:
        row = conn().execute("SELECT * FROM api_keys WHERE key=? AND enabled=1", (key,)).fetchone()
    return dict(row) if row else None


def touch_api_key(kid):
    with _lock:
        c = conn()
        c.execute("UPDATE api_keys SET last_used=?, hit_count=hit_count+1 WHERE id=?",
                  (time.time(), kid))
        c.commit()


# ----- computed stats for the dashboard -----
def stats(window_sec=86400):
    """Real success rate / avg latency / counts over the recent window."""
    import time as _t
    cutoff = _t.time() - window_sec
    with _lock:
        rows = conn().execute(
            "SELECT status, attempts, ms, stream FROM logs WHERE ts>=? AND COALESCE(source,'')=''", (cutoff,)).fetchall()
    # only count real model calls (POST messages -> stream/buffered/assembled), skip 404 probes
    real = [r for r in rows if r["status"] and r["status"] != 404]
    total = len(real)
    ok = sum(1 for r in real if 200 <= (r["status"] or 0) < 300)
    fails = total - ok
    oks_ms = [r["ms"] for r in real if 200 <= (r["status"] or 0) < 300 and r["ms"]]
    avg_ms = round(sum(oks_ms) / len(oks_ms)) if oks_ms else 0
    return {
        "total": total,
        "ok": ok,
        "fails": fails,
        "success_pct": round(ok / total * 100, 1) if total else 0.0,
        "avg_ms": avg_ms,
        "window_sec": window_sec,
    }
