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
    # --- latency-tuned defaults -------------------------------------------------
    # Measured over 24h of live traffic: 1-attempt requests averaged 33s while
    # 4-6 attempt requests averaged 247-471s. Almost all latency is burned in
    # failed retries, not in generation — so the defaults below are chosen to fail
    # fast and move on, rather than to keep grinding one bad path.
    "max_retries": "3",                           # attempts per endpoint before moving to the next.
                                                  # Kept low on purpose: measured, a 6th
                                                  # attempt averaged 471s and rarely saved
                                                  # the request. Every endpoint is still
                                                  # tried before the client sees an error
    "connect_timeout": "8",                       # seconds to establish the proxy tunnel; a live proxy
                                                  # answers in 1-3s, so a longer wait only pays for
                                                  # dead ones (45 dead-proxy hits in the sample day)
    "read_timeout": "1200",                       # seconds to wait for upstream bytes (long generations)
    "write_timeout": "120",                       # seconds to wait for downstream data upload (e.g., large POST payloads)
    "pool_timeout": "20",                         # seconds to wait for an available connection from the internal pool
    "keepalive_ping_interval": "10",              # seconds between SSE keep-alive pings to prevent client disconnect
    "admin_test_timeout": "45",                   # seconds to wait when manually testing an endpoint via Dashboard
    "scanner_tcp_timeout": "3.0",                 # seconds the background proxy scanner waits for a TCP connection
    "cleanup_loop_interval": "3600",              # seconds between database cleanup sweeps (deleting stale/dead proxies)
    "preflight_get_timeout": "2.0",               # seconds for rapid preflight GET tests
    "direct_test_timeout": "25.0",                # seconds for deep proxy validation tests
    "user_agent": "claude-cli/2.1.177 (external, cli)",  # Claude Code fingerprint (agentrouter requires this)
    "dedicated_proxy_id": "",                     # pin ALL requests to one proxy ("" = first enabled)
    "dedicated_strict": "0",                      # legacy; unused when auto_rotation drives selection
    "auto_rotation": "0",                         # 1 = rotate across enabled proxies on fail; 0 = single dedicated
    "hot_pool_enabled": "1",                      # 1 = hot pool engine active; 0 = off (for premium proxies)
    "hot_pool_api_test": "1",                     # 1 = HTTP API GET test; 0 = simple TCP connection test
    "hedging_concurrency": "5",                   # proxies raced in parallel; first tunnel wins. Higher
                                                  # = lower connect latency. Safe to raise now that an
                                                  # attempt only consumes the winner + genuine failures
    "hot_pool_size": "100",                       # number of verified proxies to keep in hot pool
    "hot_pool_refresh": "5",                      # seconds between hot pool refresh cycles
    "hot_pool_test_timeout": "3.0",               # timeout in seconds for hot pool tests
    "hot_pool_concurrency": "20",                 # max concurrent proxy tests during hot pool refresh
    "dedicated_cooldown": "60",                   # seconds a dedicated proxy (phone/scrape.do)
                                                  # is skipped after a connect-level failure.
                                                  # It has no DB row, so without this every
                                                  # request re-discovers that the phone is off
    "proxy_scanner_enabled": "1",                 # 1 = background TCP scanner active (UI toggle read this before it existed)
    "proxy_scanner_interval": "5",
    "proxy_scanner_batch": "150",
    "global_model_override": "",                  # Override model id globally
    "claude_mimicry": "1",                        # 1 = Full A-to-Z node/claude-cli mimicry
    # --- retry backoff (Claude Code SDK formula: min(initial*2^n, max) + jitter) ---
    "retry_initial_delay": "0.5",                 # seconds before first retry. Only applies to upstream
    "retry_max_delay": "4.0",                     # pressure (5xx/429); proxy-local failures rotate to a
                                                  # fresh exit IP and skip the wait entirely
    "failover_5xx_threshold": "2",                # consecutive 5xx before abandoning this endpoint. 2 is
                                                  # enough of a signal and saves a whole slow attempt
    # Suggested phrases offered in the UI for per-key rotation. Providers word an
    # exhausted balance differently: agentrouter says "pre-consume quota failed" and
    # later "Budget pool quota has been exhausted"; Chinese relays say 额度不足. The
    # substring "quota" covers the first two on its own.
    "key_rotate_hint": "insufficient quota, quota, 额度不足, 402, insufficient balance",
    # --- TCP keepalive on the proxy tunnel (covers slow upstream "thinking") ---
    # Probes start sooner and repeat faster than the old 30s/5s: the residential
    # tunnel goes idle while the model "thinks", and an idle-dropped tunnel showed up
    # as RemoteProtocolError 48 times in the sample day — the single largest
    # non-upstream failure cause.
    "keepalive_idle": "15",                       # seconds idle before first probe
    "keepalive_intvl": "3",                       # seconds between probes
    "keepalive_cnt": "200",                       # probe count (idle + intvl*cnt = total coverage)
    # --- routing-fix toggles (Ciel, 2026-09-02) ---------------------------------
    # Boss's requirement: "mujhe har cheez ka toggle chahiye, dynamic hona
    # chahiye". Each fix below changed how a failure is CLASSIFIED, so each one
    # gets its own switch — default ON (the measured-correct behaviour), set to
    # "0" to fall back to the old behaviour without editing or redeploying code.
    # Read live per request, so a toggle takes effect with no restart.
    "fx_status_over_contenttype": "1",             # trust the HTTP status when an upstream
                                                   # returns 400 with content-type
                                                   # text/event-stream (new-api does this).
                                                   # OFF = the old guard, which fed that JSON
                                                   # error into the SSE assembler and blamed
                                                   # the proxy for "incomplete"
    "fx_status_only_keywords": "1",                # numeric failover keywords match the HTTP
                                                   # STATUS only. OFF = old substring match on
                                                   # the whole body, where an upstream request
                                                   # id like ...518501558... matched rule "501"
    "fx_size_refusal_stop": "1",                   # a "context window is full" refusal stops
                                                   # THIS endpoint's proxy/key retries (other
                                                   # providers are still tried). OFF = spend the
                                                   # full budget re-sending identical bytes
    "fx_proxy_not_endpoint": "1",                  # WAF/ConnectError/ReadError/truncated count
                                                   # against the PROXY, not the provider. OFF =
                                                   # old behaviour, where 2 bad proxies retired
                                                   # a healthy endpoint via failover_5xx_threshold
    "fx_refusal_is_answer": "1",                   # stop_reason=refusal with empty content is a
                                                   # complete answer. OFF = treat it as a
                                                   # truncated stream and retry everywhere
    "fx_method_passthrough": "1",                  # GET/HEAD (e.g. /v1/models) go upstream as
                                                   # GET. OFF = old behaviour, forwarded as POST
                                                   # and every provider answered 404
    "fx_strict_waf": "1",
    # Send one SSE keepalive before contacting any upstream, so a CDN in front
    # of the gateway cannot 524 while waiting for the first byte. Measured: CF
    # held headers until 70s on a large request and aborts at ~100s.
    "fx_early_ping": "1",                          # only a real challenge page counts as WAF.
                                                   # OFF = any text/html response counts, which
                                                   # mislabelled ordinary nginx error pages
}


_ENDPOINTS_DDL = """
CREATE TABLE endpoints_new (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    url                       TEXT NOT NULL,
    api_mode                  TEXT    DEFAULT 'anthropic_messages',
    api_key                   TEXT    DEFAULT '',
    enabled                   INTEGER DEFAULT 1,
    priority                  INTEGER DEFAULT 0,
    status                    TEXT    DEFAULT 'unknown',
    note                      TEXT    DEFAULT '',
    model_override            TEXT    DEFAULT '',
    is_primary                INTEGER DEFAULT 0,
    name                      TEXT    DEFAULT '',
    failover_trigger_keywords TEXT    DEFAULT '500,501,502,503,504,524,401,403,unauthorized',
    endpoint_failover_keywords TEXT   DEFAULT 'Thinking,model_not_found,invalid_api_key,content-blocked,content_filter',
    scrape_do_token           TEXT    DEFAULT '',
    custom_proxies            TEXT    DEFAULT '[]',
    proxy_priority            TEXT    DEFAULT '[]',
    proxy_fallback            INTEGER DEFAULT 1,
    key_failover_keywords     TEXT    DEFAULT '',
    extra_keys                TEXT    DEFAULT '[]'
)
"""


def _drop_endpoint_url_unique(c):
    """Rebuild `endpoints` without UNIQUE(url).

    SQLite cannot drop a column constraint, so the table has to be recreated. One
    provider legitimately appears several times — one row per API key, because quota is
    metered per key upstream.
    """
    idx = [r[1] for r in c.execute("PRAGMA index_list(endpoints)").fetchall()]
    if not any(i.startswith("sqlite_autoindex_endpoints") for i in idx):
        return
    old_cols = [r[1] for r in c.execute("PRAGMA table_info(endpoints)").fetchall()]
    new_cols = [l.strip().split()[0] for l in _ENDPOINTS_DDL.splitlines()
                if l.startswith("    ") and not l.strip().startswith(")")]
    shared = [col for col in new_cols if col in old_cols]
    collist = ", ".join(shared)
    # python-sqlite3 opens an implicit transaction before DML, so an explicit BEGIN here
    # raises "cannot start a transaction within a transaction" — which the previous
    # version swallowed, leaving the UNIQUE index quietly in place. Commit what is
    # pending, switch to autocommit for the rebuild, then restore.
    prev_isolation = c.isolation_level
    try:
        c.commit()
        c.isolation_level = None
        c.execute("PRAGMA foreign_keys=off")
        c.execute("DROP TABLE IF EXISTS endpoints_new")
        c.execute(_ENDPOINTS_DDL)
        c.execute(f"INSERT INTO endpoints_new({collist}) SELECT {collist} FROM endpoints")
        c.execute("DROP TABLE endpoints")
        c.execute("ALTER TABLE endpoints_new RENAME TO endpoints")
    except Exception as exc:
        # Booting matters more than the rebuild; the only cost of failing here is that
        # duplicate URLs stay rejected. Log it so it is not silent.
        try:
            c.execute("DROP TABLE IF EXISTS endpoints_new")
        except Exception:
            pass
        print(f"[db] endpoints rebuild skipped: {type(exc).__name__}: {exc}")
    finally:
        try:
            c.execute("PRAGMA foreign_keys=on")
        except Exception:
            pass
        c.isolation_level = prev_isolation


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
            req_body   TEXT,
            req_id     TEXT DEFAULT '',   -- same value for every attempt of one request
            final      INTEGER DEFAULT 0  -- 1 on the row that decided the outcome
        );
        CREATE TABLE IF NOT EXISTS endpoints (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            -- NOT unique: providers hand out several keys per account, each with its
            -- own quota, so the same base URL legitimately appears more than once.
            url       TEXT NOT NULL,
            api_mode  TEXT    DEFAULT 'anthropic_messages',  -- anthropic_messages|chat_completions
            api_key   TEXT    DEFAULT '',                    -- '' = use global upstream_key
            enabled   INTEGER DEFAULT 1,
            priority  INTEGER DEFAULT 0,                     -- lower = tried first
            status    TEXT    DEFAULT 'unknown',
            note      TEXT    DEFAULT '',
            model_override TEXT DEFAULT '',
            failover_trigger_keywords TEXT DEFAULT '500,501,502,503,504,524,401,403,unauthorized',
            endpoint_failover_keywords TEXT DEFAULT 'Thinking,model_not_found,invalid_api_key,content-blocked,content_filter'
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
    lcols = [r[1] for r in c.execute("PRAGMA table_info(logs)").fetchall()]
    if "req_id" not in lcols:
        c.execute("ALTER TABLE logs ADD COLUMN req_id TEXT DEFAULT ''")
    if "final" not in lcols:
        c.execute("ALTER TABLE logs ADD COLUMN final INTEGER DEFAULT 0")
        # Backfill: attempts resets to 1 on each new request, and the last row before
        # the next reset is that request's outcome.
        c.execute("""
            UPDATE logs SET final=1 WHERE id IN (
              SELECT id FROM (
                SELECT id, attempts,
                       LEAD(attempts) OVER (ORDER BY id) nxt
                FROM logs WHERE COALESCE(source,'')=''
              ) WHERE nxt IS NULL OR nxt<=1
            )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_logs_final ON logs(final, ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_logs_reqid ON logs(req_id)")
    if "failover_trigger_keywords" not in ecols:
        c.execute("ALTER TABLE endpoints ADD COLUMN failover_trigger_keywords TEXT DEFAULT '500,501,502,503,504,524,401,403,unauthorized'")
    if "endpoint_failover_keywords" not in ecols:
        c.execute("ALTER TABLE endpoints ADD COLUMN endpoint_failover_keywords TEXT DEFAULT 'Thinking,model_not_found,invalid_api_key,content-blocked,content_filter'")
    if "scrape_do_token" not in ecols:
        c.execute("ALTER TABLE endpoints ADD COLUMN scrape_do_token TEXT DEFAULT ''")
    if "custom_proxies" not in ecols:
        c.execute("ALTER TABLE endpoints ADD COLUMN custom_proxies TEXT DEFAULT '[]'")
    if "proxy_priority" not in ecols:
        c.execute("ALTER TABLE endpoints ADD COLUMN proxy_priority TEXT DEFAULT '[]'")
    if "proxy_fallback" not in ecols:
        c.execute("ALTER TABLE endpoints ADD COLUMN proxy_fallback INTEGER DEFAULT 1")
    if "key_failover_keywords" not in ecols:
        # Third failover tier. Empty means "any error moves to the next key for this
        # provider" — which is what someone adding a second key almost always wants,
        # so it works with no configuration. Non-empty narrows it to these markers.
        c.execute("ALTER TABLE endpoints ADD COLUMN key_failover_keywords TEXT DEFAULT ''")
    if "extra_keys" not in ecols:
        # JSON array of additional API keys for this same provider/URL. Each key
        # carries its own quota upstream, so exhausting one should move to the next
        # rather than abandoning the provider.
        c.execute("ALTER TABLE endpoints ADD COLUMN extra_keys TEXT DEFAULT '[]'")
    if "fx_flags" not in ecols:
        # Per-endpoint failure-diagnosis overrides as a JSON object, e.g.
        # {"fx_size_refusal_stop": "0"}. Boss's point (2026-09-02): these describe
        # how a PARTICULAR provider's failures should be read, so they belong to
        # the endpoint, not to the global proxy settings. A premium/official
        # endpoint wants fewer restrictions than a free relay. Empty = inherit
        # every rule from the global settings row.
        c.execute("ALTER TABLE endpoints ADD COLUMN fx_flags TEXT DEFAULT ''")

    _drop_endpoint_url_unique(c)

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
    """Read a setting, falling back to DEFAULT_SETTINGS before the caller's default.

    Call sites used to carry their own literal fallback (`get_setting("max_retries",
    "10")`), so a tuned default here could be silently overridden by a stale literal
    somewhere else. DEFAULT_SETTINGS is now the single source of truth.
    """
    with _lock:
        row = conn().execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    if row is not None:
        return row["value"]
    return DEFAULT_SETTINGS.get(key, default)


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
    """All settings, with DEFAULT_SETTINGS filling any key the DB has never stored.

    forward() reads its timeouts and retry budget from this dict with its own inline
    fallbacks (`s.get("max_retries", "4")`), which quietly diverged from
    DEFAULT_SETTINGS. Merging here keeps one source of truth.
    """
    with _lock:
        rows = conn().execute("SELECT key, value FROM settings").fetchall()
    merged = dict(DEFAULT_SETTINGS)
    merged.update({r["key"]: r["value"] for r in rows})
    return merged


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

def prune_proxies(mode, min_fails=3):
    """Delete proxies that are known bad. Returns how many rows were removed.

    Deliberately narrow: each mode names a condition that can be checked from stored
    data, so the assistant cannot be talked into a broad DELETE. An unrecognised mode
    removes nothing.

      unhealthy   status is unhealthy or banned
      failing     fail_count >= min_fails AND never succeeded
      unroutable  addresses that can never reach the internet from this host
    """
    if mode == "unhealthy":
        sql = "DELETE FROM proxies WHERE status IN ('unhealthy','banned')"
        params = ()
    elif mode == "failing":
        sql = ("DELETE FROM proxies WHERE COALESCE(fail_count,0) >= ? "
               "AND COALESCE(success_count,0) = 0")
        params = (max(1, int(min_fails)),)
    elif mode == "unroutable":
        sql = ("DELETE FROM proxies WHERE url LIKE '%0.0.0.0%' OR url LIKE '%127.0.0.%' "
               "OR url LIKE '%localhost%' OR url LIKE '%://192.168.%' OR url LIKE '%://10.%' "
               "OR url LIKE '%://172.16.%' OR url LIKE '%://172.17.%' "
               "OR url LIKE '%://169.254.%' OR url LIKE '%:0'")
        params = ()
    else:
        return 0
    with _lock:
        c = conn()
        cur = c.execute(sql, params)
        c.commit()
        return cur.rowcount


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

def get_untested_proxies(limit=50):
    query = "SELECT * FROM proxies WHERE status = 'unknown' AND enabled = 1 ORDER BY RANDOM() LIMIT ?"
    with _lock:
        return [dict(r) for r in conn().execute(query, (limit,)).fetchall()]

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

def delete_dead_proxies(cutoff_time):
    with _lock:
        c = conn()
        # Delete banned/unhealthy outright
        cur = c.execute("DELETE FROM proxies WHERE status IN ('banned', 'unhealthy')")
        # Delete stale 'ok' proxies (not checked in last 24h)
        c.execute("DELETE FROM proxies WHERE status = 'ok' AND last_checked > 0 AND last_checked < ?", (cutoff_time,))
        c.commit()
        return cur.rowcount

def delete_old_disabled_proxies(cutoff_time):
    with _lock:
        c = conn()
        cur = c.execute("DELETE FROM proxies WHERE enabled=0 AND last_checked > 0 AND last_checked < ?", (cutoff_time,))
        c.commit()
        return cur.rowcount


def search_proxies(q="", limit=100, offset=0, status=""):
    """Server-side proxy search/paging.

    The dashboard used to fetch a flat first-200 slice while displaying the true
    total, so filtering or "Show all" quietly missed the other 15k rows. Searching
    and paging now happen in SQL against the whole table.
    """
    where, params = [], []
    if q:
        where.append("(url LIKE ? OR COALESCE(status,'') LIKE ? OR COALESCE(exit_ip,'') LIKE ?)")
        like = f"%{q}%"
        params += [like, like, like]
    if status:
        where.append("status = ?")
        params.append(status)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    with _lock:
        c = conn()
        total = c.execute(f"SELECT COUNT(*) FROM proxies {clause}", params).fetchone()[0]
        rows = c.execute(
            f"SELECT * FROM proxies {clause} ORDER BY "
            "CASE status WHEN 'ok' THEN 0 WHEN 'unknown' THEN 1 WHEN 'unhealthy' THEN 2 ELSE 3 END, "
            "latency_ms, id LIMIT ? OFFSET ?",
            params + [max(1, min(int(limit), 500)), max(0, int(offset))]).fetchall()
    return {"proxies": [dict(r) for r in rows], "matched": total}


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
            "INSERT INTO logs(ts, method, path, status, proxy, attempts, stream, redactions, ms, note, ip, model, detail, endpoint, source, req_body, req_id, final) "
            "VALUES (:ts, :method, :path, :status, :proxy, :attempts, :stream, :redactions, :ms, :note, :ip, :model, :detail, :endpoint, :source, :req_body, :req_id, :final)",
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
                "req_id": f.get("req_id", ""),
                "final": 1 if f.get("final") else 0,
            },
        )
        # keep last 500
        c.execute("DELETE FROM logs WHERE id NOT IN (SELECT id FROM logs ORDER BY id DESC LIMIT 500)")
        c.commit()


def recent_logs(limit=100, after_id=None):
    """Recent log rows, newest first.

    `after_id` returns only rows newer than that id, which lets the dashboard poll
    frequently without re-sending the whole list — the live view used to wait for the
    12s full-state refresh (which also re-serialised every setting, endpoint and 200
    proxies) just to show a new line.

    req_body is excluded: it can be 3 KB per row and the list view never shows it.
    The detail modal reads it via log_detail().
    """
    cols = ("id, ts, method, path, status, proxy, attempts, stream, redactions, ms, "
            "note, ip, model, endpoint, source, detail, req_id, final")
    with _lock:
        c = conn()
        if after_id:
            rows = c.execute(f"SELECT {cols} FROM logs WHERE id>? ORDER BY id DESC LIMIT ?",
                             (int(after_id), limit)).fetchall()
        else:
            rows = c.execute(f"SELECT {cols} FROM logs ORDER BY id DESC LIMIT ?",
                             (limit,)).fetchall()
    return [dict(r) for r in rows]


def log_detail(log_id):
    """Full row including req_body, for the detail modal only."""
    with _lock:
        r = conn().execute("SELECT * FROM logs WHERE id=?", (int(log_id),)).fetchone()
    return dict(r) if r else None


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


def add_endpoint(url, api_mode="anthropic_messages", api_key="", model_override="", name=""):
    """Insert an endpoint and return (added, endpoint_id).

    The same url may be added repeatedly — one row per API key, since providers meter
    quota per key. Previously a UNIQUE(url) index silently dropped the second one while
    the caller reported success.

    The old return value was `c.total_changes`, which counts every change since the
    connection was opened, so the API answered things like {"added": 11990}.
    """
    with _lock:
        c = conn()
        count = c.execute("SELECT COUNT(*) FROM endpoints").fetchone()[0]
        is_primary = 1 if count == 0 else 0
        nxt = c.execute("SELECT COALESCE(MAX(priority),0)+1 FROM endpoints").fetchone()[0]
        cur = c.execute(
            "INSERT INTO endpoints(url, api_mode, api_key, model_override, priority, is_primary, name) "
            "VALUES (?,?,?,?,?,?,?)",
            (url, api_mode, api_key, model_override, nxt, is_primary, name or ""))
        c.commit()
        return (1 if cur.rowcount else 0), cur.lastrowid


def update_endpoint(eid, **fields):
    allowed_ENDPOINT_COLS = {"url", "api_mode", "api_key", "enabled", "priority", "status", "note", "is_primary", "name", "model_override", "failover_trigger_keywords", "endpoint_failover_keywords", "scrape_do_token", "custom_proxies", "proxy_priority", "proxy_fallback", "extra_keys", "key_failover_keywords", "fx_flags"}
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
    """Make one endpoint the first routing target.

    `priority` is the single source of truth for try-order, so promoting an
    endpoint means moving it to priority 0 and pushing everything else down.
    is_primary stays in sync purely as a display flag, which keeps the dashboard
    order and the forwarder's real order from ever disagreeing. eid=0 -> none.
    """
    with _lock:
        c = conn()
        c.execute("UPDATE endpoints SET is_primary=0")
        if eid:
            rest = [r[0] for r in c.execute(
                "SELECT id FROM endpoints WHERE id!=? ORDER BY priority, id", (eid,)).fetchall()]
            c.execute("UPDATE endpoints SET is_primary=1, enabled=1, priority=0 WHERE id=?", (eid,))
            for i, other in enumerate(rest, start=1):
                c.execute("UPDATE endpoints SET priority=? WHERE id=?", (i, other))
        c.commit()


def reorder_endpoints(ids):
    """Persist an explicit try-order: ids[0] is tried first, ids[1] next, ...

    The first ENABLED id also becomes primary so the star in the dashboard always
    marks the endpoint that actually receives the first request. Any endpoint not
    named in `ids` keeps its relative order after the listed ones.
    """
    with _lock:
        c = conn()
        known = [r[0] for r in c.execute("SELECT id FROM endpoints ORDER BY priority, id").fetchall()]
        seen, order = set(), []
        for eid in ids:
            try:
                eid = int(eid)
            except (TypeError, ValueError):
                continue
            if eid in known and eid not in seen:
                seen.add(eid)
                order.append(eid)
        order += [e for e in known if e not in seen]
        for i, eid in enumerate(order):
            c.execute("UPDATE endpoints SET priority=? WHERE id=?", (i, eid))
        enabled = {r[0] for r in c.execute("SELECT id FROM endpoints WHERE enabled=1").fetchall()}
        first = next((e for e in order if e in enabled), None)
        c.execute("UPDATE endpoints SET is_primary=0")
        if first is not None:
            c.execute("UPDATE endpoints SET is_primary=1 WHERE id=?", (first,))
        c.commit()
        return order


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


def endpoint_stats(window_sec=86400):
    """Per-endpoint routed-traffic outcome, keyed by the endpoint's display name.

    Powers the dashboard's routing-transparency panel: for each upstream the
    operator can see how many real requests it served, how many it rejected, its
    median-ish latency, and when it was last touched — so "which endpoint is
    actually being used" stops being a guess.

    Counted per ATTEMPT here on purpose: this panel answers "how does this endpoint
    behave when the router tries it", which is an attempt-level question. Whole-request
    latency lives in stats() instead.
    """
    cutoff = time.time() - window_sec
    with _lock:
        rows = conn().execute(
            "SELECT endpoint, status, ms, ts FROM logs "
            "WHERE ts>=? AND COALESCE(source,'')='' AND COALESCE(endpoint,'')!=''",
            (cutoff,)).fetchall()
    agg = {}
    for r in rows:
        st = r["status"] or 0
        # 404 = path the upstream doesn't implement (v1/props, count_tokens);
        # not a routing failure, so it must not pollute the success rate.
        if st == 404:
            continue
        a = agg.setdefault(r["endpoint"], {"served": 0, "failed": 0, "_ms": [], "last_ts": 0})
        if 200 <= st < 300:
            a["served"] += 1
            if r["ms"]:
                a["_ms"].append(r["ms"])
        else:
            a["failed"] += 1
        a["last_ts"] = max(a["last_ts"], r["ts"] or 0)
    out = {}
    for name, a in agg.items():
        total = a["served"] + a["failed"]
        ms = sorted(a["_ms"])
        out[name] = {
            "served": a["served"],
            "failed": a["failed"],
            "total": total,
            "success_pct": round(a["served"] / total * 100, 1) if total else 0.0,
            "avg_ms": round(sum(ms) / len(ms)) if ms else 0,
            "median_ms": ms[len(ms) // 2] if ms else 0,
            "last_ts": a["last_ts"],
        }
    return out


# ----- computed stats for the dashboard -----
def stats(window_sec=86400):
    """What the CLIENT experienced, one row per request — not per attempt.

    The old version aggregated every log row, so a request that retried 7 times was
    counted as 7 "requests" (measured: 266 rows for 52 real requests, a 5.1x
    inflation, and success rate read 12% when it was really 63%). Worse, only 2xx rows
    carry a latency, so the minutes burned by a FAILED request never entered the
    average at all — which is exactly why the dashboard said 146s while requests were
    really taking ~171s, and why a 10-minute failure appeared to cost nothing.

    Latency here is the full wall-clock time the client waited, including every retry
    and every endpoint switch, for successes AND failures.
    """
    import time as _t
    cutoff = _t.time() - window_sec
    with _lock:
        rows = conn().execute(
            "SELECT status, attempts, ms, stream FROM logs "
            "WHERE ts>=? AND COALESCE(source,'')='' AND final=1 AND COALESCE(status,0)!=404",
            (cutoff,)).fetchall()
    total = len(rows)
    ok = sum(1 for r in rows if 200 <= (r["status"] or 0) < 300)
    fails = total - ok
    all_ms = sorted(r["ms"] for r in rows if r["ms"])
    ok_ms = sorted(r["ms"] for r in rows if 200 <= (r["status"] or 0) < 300 and r["ms"])
    fail_ms = sorted(r["ms"] for r in rows if not (200 <= (r["status"] or 0) < 300) and r["ms"])
    attempts = [r["attempts"] for r in rows if r["attempts"]]

    def _avg(v):
        return round(sum(v) / len(v)) if v else 0

    def _pct(v, q):
        return v[min(len(v) - 1, int(len(v) * q))] if v else 0

    return {
        "total": total,
        "ok": ok,
        "fails": fails,
        "success_pct": round(ok / total * 100, 1) if total else 0.0,
        # avg_ms stays the headline number but now spans successes AND failures
        "avg_ms": _avg(all_ms),
        "median_ms": _pct(all_ms, 0.5),
        "p95_ms": _pct(all_ms, 0.95),
        "worst_ms": all_ms[-1] if all_ms else 0,
        "avg_ok_ms": _avg(ok_ms),
        "avg_fail_ms": _avg(fail_ms),
        "avg_attempts": round(sum(attempts) / len(attempts), 2) if attempts else 0,
        "first_try_pct": round(sum(1 for a in attempts if a == 1) / len(attempts) * 100, 1) if attempts else 0.0,
        "window_sec": window_sec,
    }
