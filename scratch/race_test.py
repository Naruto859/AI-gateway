import asyncio
import time
import sqlite3
import urllib.parse
import json

DB_PATH = "/root/AI-gateway-v7/data/data.db"

async def test_proxy_connection(proxy_url, target_host, target_port):
    """Attempt to establish a TCP tunnel through the proxy."""
    parsed = urllib.parse.urlparse(proxy_url if "://" in proxy_url else f"http://{proxy_url}")
    proxy_host = parsed.hostname or parsed.path.split(":")[0]
    proxy_port = parsed.port or (1080 if "socks" in parsed.scheme else 3128)
    scheme = parsed.scheme.lower()
    
    start_time = time.time()
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(proxy_host, proxy_port), timeout=5.0)
        
        if "socks" in scheme:
            # Basic SOCKS5 handshake (no auth)
            writer.write(b"\x05\x01\x00")
            await writer.drain()
            resp = await asyncio.wait_for(reader.read(2), timeout=5.0)
            if not resp or resp[0] != 5 or resp[1] != 0:
                writer.close()
                return None
                
            # Connect request
            host_bytes = target_host.encode('utf-8')
            req = b"\x05\x01\x00\x03" + bytes([len(host_bytes)]) + host_bytes + target_port.to_bytes(2, 'big')
            writer.write(req)
            await writer.drain()
            
            resp = await asyncio.wait_for(reader.read(10), timeout=5.0)
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
                chunk = await asyncio.wait_for(reader.read(1024), timeout=5.0)
                if not chunk:
                    break
                resp += chunk
                
            if b"200 Connection" not in resp:
                writer.close()
                return None

        # Success! We have a tunnel.
        latency = int((time.time() - start_time) * 1000)
        
        # Close the tunnel immediately since this is just a test
        writer.close()
        await writer.wait_closed()
        
        return {"url": proxy_url, "latency_ms": latency}
    except Exception as e:
        return None

async def race_proxies(proxies, target_host, target_port):
    print(f"Racing {len(proxies)} proxies to {target_host}:{target_port}...")
    
    # Create tasks
    tasks = [asyncio.create_task(test_proxy_connection(p, target_host, target_port)) for p in proxies]
    
    start_time = time.time()
    winner = None
    
    # Wait for the first task to return a non-None result
    for coro in asyncio.as_completed(tasks):
        try:
            result = await coro
            if result is not None:
                winner = result
                break
        except Exception:
            pass
            
    # Cancel remaining tasks
    for t in tasks:
        if not t.done():
            t.cancel()
            
    total_time = int((time.time() - start_time) * 1000)
    
    if winner:
        print(f"✅ Winner found in {total_time}ms: {winner['url']} (Proxy latency: {winner['latency_ms']}ms)")
    else:
        print(f"❌ All {len(proxies)} proxies failed or timed out.")

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Get 10 random enabled proxies that aren't banned
    rows = conn.execute("SELECT url FROM proxies WHERE enabled=1 AND status = 'ok' ORDER BY latency_ms ASC LIMIT 10").fetchall()
    conn.close()
    
    proxies = [r["url"] for r in rows]
    
    # Run the race 3 times with different batch sizes
    asyncio.run(race_proxies(proxies[:3], "agentrouter.org", 443))
    print("---")
    asyncio.run(race_proxies(proxies[3:6], "agentrouter.org", 443))
    print("---")
    asyncio.run(race_proxies(proxies[6:], "agentrouter.org", 443))

if __name__ == "__main__":
    main()
