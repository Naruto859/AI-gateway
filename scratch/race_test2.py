import asyncio
import time
import sqlite3
import urllib.parse

DB_PATH = "/root/AI-gateway-v7/data/data.db"

async def test_proxy_connection(proxy_url, target_host, target_port):
    parsed = urllib.parse.urlparse(proxy_url if "://" in proxy_url else f"http://{proxy_url}")
    proxy_host = parsed.hostname or parsed.path.split(":")[0]
    proxy_port = parsed.port or (1080 if "socks" in parsed.scheme else 3128)
    scheme = parsed.scheme.lower()
    
    start_time = time.time()
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(proxy_host, proxy_port), timeout=3.0)
        
        if "socks" in scheme:
            writer.write(b"\x05\x01\x00")
            await writer.drain()
            resp = await asyncio.wait_for(reader.read(2), timeout=3.0)
            if not resp or resp[0] != 5 or resp[1] != 0:
                writer.close()
                return None
                
            host_bytes = target_host.encode('utf-8')
            req = b"\x05\x01\x00\x03" + bytes([len(host_bytes)]) + host_bytes + target_port.to_bytes(2, 'big')
            writer.write(req)
            await writer.drain()
            
            resp = await asyncio.wait_for(reader.read(10), timeout=3.0)
            if not resp or resp[1] != 0:
                writer.close()
                return None
        else:
            req = f"CONNECT {target_host}:{target_port} HTTP/1.1\r\nHost: {target_host}:{target_port}\r\n\r\n"
            writer.write(req.encode())
            await writer.drain()
            
            resp = b""
            while b"\r\n\r\n" not in resp:
                chunk = await asyncio.wait_for(reader.read(1024), timeout=3.0)
                if not chunk:
                    break
                resp += chunk
                
            if b"200 Connection" not in resp:
                writer.close()
                return None

        latency = int((time.time() - start_time) * 1000)
        writer.close()
        await writer.wait_closed()
        return {"url": proxy_url, "latency_ms": latency}
    except Exception:
        return None

async def race_proxies(proxies, target_host, target_port):
    tasks = [asyncio.create_task(test_proxy_connection(p, target_host, target_port)) for p in proxies]
    start_time = time.time()
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
            
    total_time = int((time.time() - start_time) * 1000)
    if winner:
        print(f"✅ WINNER in {total_time}ms -> {winner['url']} (Latency: {winner['latency_ms']}ms)")
    else:
        print(f"❌ All proxies failed.")

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT url FROM proxies WHERE enabled=1 AND status = 'ok' ORDER BY latency_ms ASC LIMIT 20").fetchall()
    conn.close()
    
    proxies = [r["url"] for r in rows]
    
    print("Test 1: Racing 5 proxies to google.com")
    asyncio.run(race_proxies(proxies[:5], "google.com", 443))
    
    print("Test 2: Racing 5 proxies to agentrouter.org")
    asyncio.run(race_proxies(proxies[5:10], "agentrouter.org", 443))

if __name__ == "__main__":
    main()
