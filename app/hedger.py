import asyncio
import time
import urllib.parse
import logging
from . import db

log = logging.getLogger("gateway.hedger")

async def test_proxy(proxy_url, target_host, target_port):
    """Attempt to establish a TCP tunnel through the proxy."""
    parsed = urllib.parse.urlparse(proxy_url if "://" in proxy_url else f"http://{proxy_url}")
    proxy_host = parsed.hostname or parsed.path.split(":")[0]
    proxy_port = parsed.port or (1080 if "socks" in parsed.scheme else 3128)
    scheme = parsed.scheme.lower()
    
    timeout_sec = float(db.get_setting("connect_timeout", "10.0") or 10.0)
    
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(proxy_host, proxy_port), timeout=timeout_sec)
        
        if "socks" in scheme:
            # Basic SOCKS5 handshake (no auth)
            writer.write(b"\x05\x01\x00")
            await writer.drain()
            resp = await asyncio.wait_for(reader.read(2), timeout=timeout_sec)
            if not resp or resp[0] != 5 or resp[1] != 0:
                writer.close()
                return None
                
            # Connect request
            host_bytes = target_host.encode('utf-8')
            req = b"\x05\x01\x00\x03" + bytes([len(host_bytes)]) + host_bytes + target_port.to_bytes(2, 'big')
            writer.write(req)
            await writer.drain()
            
            resp = await asyncio.wait_for(reader.read(10), timeout=timeout_sec)
            if not resp or resp[1] != 0:
                writer.close()
                return None
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
                return None

        # Success! We have a tunnel.
        return reader, writer, proxy_url
    except Exception as e:
        return None

async def race_proxies(proxies, target_host, target_port):
    if not proxies:
        return None
        
    tasks = [asyncio.create_task(test_proxy(p, target_host, target_port)) for p in proxies]
    winner = None
    
    for coro in asyncio.as_completed(tasks):
        try:
            result = await coro
            if result is not None:
                winner = result
                break
        except Exception:
            pass
            
    for t in tasks:
        if not t.done():
            t.cancel()
            
    return winner

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
    
    winner = await race_proxies(proxies_to_race, host, port)
    
    if not winner:
        log.warning(f"❌ All {len(proxies_to_race)} proxies failed to connect to {host}:{port}")
        writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
        await writer.drain()
        writer.close()
        return
        
    winner_reader, winner_writer, winning_url = winner
    log.info(f"🏎️ Race winner: {winning_url} for {host}:{port}")
    
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

async def start_server():
    server = await asyncio.start_server(handle_client, '127.0.0.1', 9090)
    log.info("Hedging proxy server listening on 127.0.0.1:9090")
    async with server:
        await server.serve_forever()
