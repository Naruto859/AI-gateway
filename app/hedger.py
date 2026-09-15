import os
import asyncio
import socket
import time
import urllib.parse
import logging
from . import db

log = logging.getLogger("gateway.hedger")

# ---------------------------------------------------------------------------
# Race outcome side-channel.
#
# The hedger races N proxies and knows exactly which one won the CONNECT and
# which ones failed outright — but that knowledge used to die here. The caller
# (forwarder) therefore attributed every success/failure to `candidates[0]`,
# so a fast proxy could be marked bad for a slow one's failure and the pool's
# health data slowly became fiction.
#
# The caller now passes `x-hedge-id: <token>` alongside `x-hedge-proxies`, and
# reads the outcome back with `take_outcome(token)`. Same process, so a plain
# dict is enough; entries are one-shot and time-bounded so a client that
# disconnects mid-race cannot leak memory.
# ---------------------------------------------------------------------------
_OUTCOMES = {}
_OUTCOME_TTL = 600  # seconds; generous — a slow generation still finishes first


def _record_outcome(token, winner, failed):
    if not token:
        return
    _prune_outcomes()
    _OUTCOMES[token] = {"winner": winner, "failed": list(failed), "ts": time.time()}


def _prune_outcomes():
    if len(_OUTCOMES) < 256:
        return
    cutoff = time.time() - _OUTCOME_TTL
    for k in [k for k, v in _OUTCOMES.items() if v["ts"] < cutoff]:
        _OUTCOMES.pop(k, None)


def take_outcome(token):
    """Pop the race outcome for `token`.

    Returns {"winner": url|None, "failed": [url, ...]} or None if the request
    never went through the hedger (single-candidate attempts bypass it).
    """
    if not token:
        return None
    o = _OUTCOMES.pop(token, None)
    if o is None:
        return None
    return {"winner": o["winner"], "failed": o["failed"]}


async def test_proxy(proxy_url, target_host, target_port):
    """Attempt to establish a TCP tunnel through the proxy.

    Returns (reader, writer, proxy_url) on success, or ("__failed__", proxy_url)
    on a definite failure. Losing the race is NOT a failure — those tasks are
    cancelled and never report, so a merely-slower proxy keeps its good record.
    """
    parsed = urllib.parse.urlparse(proxy_url if "://" in proxy_url else f"http://{proxy_url}")
    proxy_host = parsed.hostname or parsed.path.split(":")[0]
    proxy_port = parsed.port or (1080 if "socks" in parsed.scheme else 3128)
    scheme = parsed.scheme.lower()

    timeout_sec = float(db.get_setting("connect_timeout", "10.0") or 10.0)

    writer = None
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(proxy_host, proxy_port), timeout=timeout_sec)
        # TCP keepalive on the hedger's OWN socket to the proxy (Ciel, 2026-09-05).
        #
        # forwarder._keepalive_opts() only reaches the socket httpx opens — and when
        # more than one proxy is raced, that socket goes to 127.0.0.1:<hedger>, i.e.
        # loopback, which never idles out. The leg that CAN be idle-dropped is this
        # one, and it had no keepalive at all: while a model "thinks" for a minute
        # before its first SSE byte, a residential/free proxy is free to drop a
        # silent tunnel, and the resulting clean EOF is indistinguishable from the
        # provider cutting the stream.
        #
        # Values come from the same dashboard settings as the direct path, so one
        # place tunes both. Wrapped: TCP_KEEP* are Linux-specific and a proxy socket
        # is not worth an exception.
        try:
            sock = writer.get_extra_info("socket")
            if sock is not None:
                idle = int(float(db.get_setting("keepalive_idle", "30") or 30))
                intvl = int(float(db.get_setting("keepalive_intvl", "5") or 5))
                cnt = int(float(db.get_setting("keepalive_cnt", "174") or 174))
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                for opt, val in ((getattr(socket, "TCP_KEEPIDLE", None), idle),
                                 (getattr(socket, "TCP_KEEPINTVL", None), intvl),
                                 (getattr(socket, "TCP_KEEPCNT", None), cnt)):
                    if opt is not None:
                        sock.setsockopt(socket.IPPROTO_TCP, opt, val)
        except (OSError, TypeError, ValueError):
            pass

        if "socks" in scheme:
            # Basic SOCKS5 handshake (no auth)
            writer.write(b"\x05\x01\x00")
            await writer.drain()
            resp = await asyncio.wait_for(reader.read(2), timeout=timeout_sec)
            if not resp or resp[0] != 5 or resp[1] != 0:
                writer.close()
                return ("__failed__", proxy_url)

            # Connect request
            host_bytes = target_host.encode('utf-8')
            req = b"\x05\x01\x00\x03" + bytes([len(host_bytes)]) + host_bytes + target_port.to_bytes(2, 'big')
            writer.write(req)
            await writer.drain()

            resp = await asyncio.wait_for(reader.read(10), timeout=timeout_sec)
            if not resp or resp[1] != 0:
                writer.close()
                return ("__failed__", proxy_url)
        else:
            # HTTP CONNECT
            req = f"CONNECT {target_host}:{target_port} HTTP/1.1\r\nHost: {target_host}:{target_port}\r\n\r\n"
            writer.write(req.encode())
            await writer.drain()

            # Read until \r\n\r\n
            resp = b""
            while b"\r\n\r\n" not in resp:
                chunk = await asyncio.wait_for(reader.read(1024), timeout=timeout_sec)
                if not chunk:
                    break
                resp += chunk

            if b" 200 " not in resp.split(b"\r\n")[0]:
                writer.close()
                return ("__failed__", proxy_url)

        # Success! We have a tunnel.
        return reader, writer, proxy_url
    except Exception:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        return ("__failed__", proxy_url)


async def race_proxies(proxies, target_host, target_port):
    """First proxy to complete a tunnel wins. Returns (winner_or_None, failed_urls)."""
    if not proxies:
        return None, []

    tasks = [asyncio.create_task(test_proxy(p, target_host, target_port)) for p in proxies]
    winner = None
    failed = []

    for coro in asyncio.as_completed(tasks):
        try:
            result = await coro
        except Exception:
            continue
        if result is None:
            continue
        if result[0] == "__failed__":
            failed.append(result[1])
            continue
        winner = result
        break

    # Cancel the stragglers. They neither won nor failed, so they are left
    # unjudged — being slower than the winner says nothing about health.
    for t in tasks:
        if not t.done():
            t.cancel()
    await asyncio.sleep(0)

    # Harvest tasks that had already finished when the winner appeared. A proxy
    # that refused CONNECT is genuinely bad and must be recorded even though the
    # race was already decided — as_completed stops early, so without this pass a
    # dead proxy raced alongside a good one keeps a clean record forever. Also
    # closes any second tunnel that succeeded, so no socket is leaked.
    for t in tasks:
        if not t.done() or t.cancelled():
            continue
        try:
            r = t.result()
        except Exception:
            continue
        if not r:
            continue
        if r[0] == "__failed__":
            if r[1] not in failed:
                failed.append(r[1])
        elif winner is None or r[2] != winner[2]:
            try:
                r[1].close()
            except Exception:
                pass

    return winner, failed

async def pipe(r, w):
    try:
        while True:
            data = await r.read(8192)
            if not data:
                break
            w.write(data)
            await w.drain()
    except Exception:
        pass
    finally:
        w.close()

async def handle_client(reader, writer):
    request_line = await reader.readline()
    if not request_line:
        writer.close()
        return

    parts = request_line.decode('utf-8').split()
    if len(parts) < 3:
        writer.close()
        return

    method, url, proto = parts[0], parts[1], parts[2]
    log.info(f"Hedger received: {method} {url}")

    headers = b""
    proxies_to_race = []
    hedge_id = ""

    while True:
        line = await reader.readline()
        if not line or line == b"\r\n":
            headers += b"\r\n"
            break

        lower_line = line.lower()
        if lower_line.startswith(b"x-hedge-proxies:"):
            # Extract proxies from header and DO NOT forward this header
            proxies_str = line.decode('utf-8').split(":", 1)[1].strip()
            proxies_to_race = [p.strip() for p in proxies_str.split(",") if p.strip()]
        elif lower_line.startswith(b"x-hedge-id:"):
            # Correlation token for the outcome side-channel; also not forwarded.
            hedge_id = line.decode('utf-8').split(":", 1)[1].strip()
        else:
            headers += line

    if method == "CONNECT":
        host, port = url.split(":")
        port = int(port)
    else:
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        port = parsed.port or 80

    if not proxies_to_race:
        # Fallback if header is missing
        from . import proxy_pool
        concurrency = int(db.get_setting("hedging_concurrency", "3"))
        db_proxies = proxy_pool.ordered_for_request(concurrency)
        proxies_to_race = [p["url"] for p in db_proxies]

    winner, failed = await race_proxies(proxies_to_race, host, port)

    if not winner:
        log.warning(f"❌ All {len(proxies_to_race)} proxies failed to connect to {host}:{port}")
        _record_outcome(hedge_id, None, failed)
        writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
        await writer.drain()
        writer.close()
        return

    winner_reader, winner_writer, winning_url = winner
    _record_outcome(hedge_id, winning_url, failed)
    log.info(f"🏎️ Race winner: {winning_url} for {host}:{port}"
             + (f" ({len(failed)} failed)" if failed else ""))
    
    if method == "CONNECT":
        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await writer.drain()
    else:
        winner_writer.write(request_line + headers)
        await winner_writer.drain()
        
    await asyncio.gather(
        pipe(reader, winner_writer),
        pipe(winner_reader, writer)
    )

def hedger_port():
    """Loopback port for the hedging CONNECT proxy.

    Configurable so two instances can run on one host without fighting over 9090
    (the live container has its own network namespace, but a second host-side
    deployment does not).
    """
    try:
        return int(os.environ.get("HEDGER_PORT", "9090"))
    except ValueError:
        return 9090


async def start_server():
    port = hedger_port()
    server = await asyncio.start_server(handle_client, '127.0.0.1', port)
    log.info("Hedging proxy server listening on 127.0.0.1:%d", port)
    async with server:
        await server.serve_forever()
