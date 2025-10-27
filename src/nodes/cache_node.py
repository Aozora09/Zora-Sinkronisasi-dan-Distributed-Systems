# src/nodes/cache_node.py
import os
import asyncio
import logging
import sys
from aiohttp import web, ClientSession, ClientResponse
from aiohttp_retry import RetryClient, ExponentialRetry
from collections import OrderedDict
from typing import Any

# --- Konfigurasi Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("CacheNode")

# --- Implementasi Cache (MESI + LRU) ---
class LruCache:
    def __init__(self, capacity: int = 10):
        self.capacity = capacity
        self.cache = OrderedDict()
        logger.info(f"LRU Cache dibuat dengan kapasitas {self.capacity}")
        
        # Metrics
        self.metrics_hits = 0
        self.metrics_misses = 0

    def _get(self, key: str):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def read(self, key: str):
        logger.info(f"Membaca '{key}'...")
        data = self._get(key)
        
        if data is None or data['state'] == 'I':
            logger.info(f"Cache MISS untuk '{key}' (State: {data.get('state') if data else 'None'})")
            self.metrics_misses += 1
            return None
        
        logger.info(f"Cache HIT untuk '{key}'. State: {data['state']}, Value: {data['value']}")
        self.metrics_hits += 1
        return data

    def write(self, key: str, value: Any):
        logger.info(f"Menulis '{key}' = {value}...")
        
        if key not in self.cache and len(self.cache) >= self.capacity:
            old_key, old_data = self.cache.popitem(last=False)
            logger.info(f"Cache penuh. Mengeluarkan LRU: '{old_key}'")
        
        self.cache[key] = {'state': 'M', 'value': value}
        self.cache.move_to_end(key)
        logger.info(f"Cache WRITE untuk '{key}'. State diatur ke 'Modified'")
        
    def invalidate(self, key: str):
        data = self._get(key)
        if data and data['state'] != 'I':
            logger.info(f"INVALIDATE diterima untuk '{key}'. State diubah ke 'Invalid'.")
            data['state'] = 'I'
            
    def share(self, key: str):
        data = self._get(key)
        if data and data['state'] in ('M', 'E'):
            logger.info(f"SHARE diterima untuk '{key}'. State diubah ke 'Shared'.")
            data['state'] = 'S'
            return data
        elif data and data['state'] == 'S':
            return data
        return None

# --- Logika Server Cache ---

async def main():
    node_id = os.getenv("NODE_ID", "cache-node-1")
    internal_port = int(os.getenv("INTERNAL_PORT", 8000))
    peers_env = os.getenv("PEERS", "")
    
    peers = [peer for peer in peers_env.split(",") if peer]
    
    cache = LruCache(capacity=10)
    
    retry_options = ExponentialRetry(attempts=3)
    http_client = RetryClient(ClientSession(), retry_options=retry_options)

    # --- Fungsi "Bus" (Komunikasi Antar Node) ---
    
    async def send_invalidate_to_peer(peer, key):
        """Helper coroutine untuk mengirim 1 invalidate."""
        try:
            url = f"http://{peer}:{internal_port}/bus/invalidate/{key}"
            async with http_client.post(url, timeout=0.5) as response:
                return await response.json()
        except Exception as e:
            logger.debug(f"Gagal invalidate {peer}: {e}")
            return None

    async def broadcast_invalidate(key: str):
        """Mengirim 'BusRdX' (Invalidate) ke semua rekan."""
        logger.info(f"[{node_id}] Menyiarkan INVALIDATE untuk '{key}' ke {peers}")
        tasks = []
        for peer in peers:
            tasks.append(asyncio.create_task(send_invalidate_to_peer(peer, key)))
        await asyncio.gather(*tasks)

    async def send_read_to_peer(peer, key):
        """Helper coroutine untuk mengirim 1 BusRd."""
        try:
            url = f"http://{peer}:{internal_port}/bus/read/{key}"
            async with http_client.get(url, timeout=0.5) as response:
                if response.status == 200:
                    return await response.json()
                return None
        except Exception as e:
            logger.debug(f"Gagal read dari {peer}: {e}")
            return None

    async def broadcast_read(key: str):
        """Mengirim 'BusRd' (Read) ke semua rekan."""
        logger.info(f"[{node_id}] Menyiarkan READ untuk '{key}' ke {peers}")
        tasks = []
        for peer in peers:
            tasks.append(asyncio.create_task(send_read_to_peer(peer, key)))
            
        results = await asyncio.gather(*tasks)
        
        for data in results:
            if data and data.get('value'):
                logger.info(f"READ HIT dari rekan. State: {data['state']}, Value: {data['value']}")
                cache.cache[key] = {'state': 'S', 'value': data['value']}
                cache.cache.move_to_end(key)
                return cache.cache[key]
        
        return None

    # === Definisi REST API ===
    
    async def handle_read(request):
        key = request.match_info.get('key')
        data = cache.read(key)
        
        if data:
            return web.json_response(data)
        
        data_from_peer = await broadcast_read(key)
        if data_from_peer:
            return web.json_response(data_from_peer)
        
        logger.info(f"MISS di semua cache. Mengambil dari 'Memori Utama'...")
        new_value = f"data_dari_memori_untuk_{key}"
        cache.cache[key] = {'state': 'E', 'value': new_value}
        cache.cache.move_to_end(key)
        return web.json_response(cache.cache[key])

    async def handle_write(request):
        try:
            key = request.match_info.get('key')
            data = await request.json()
            value = data.get('value')
            
            if value is None:
                return web.json_response({"error": "Butuh 'value'"}, status=400)
            
            await broadcast_invalidate(key)
            cache.write(key, value)
            return web.json_response(cache.cache[key])
            
        except Exception as e:
            logger.error(f"Error di handle_write: {e}")
            return web.json_response({"error": str(e)}, status=500)

    # --- ENDPOINT INTERNAL UNTUK BUS ---
    
    async def handle_bus_invalidate(request):
        key = request.match_info.get('key')
        cache.invalidate(key)
        return web.json_response({"status": "invalidated"})
        
    async def handle_bus_read(request):
        key = request.match_info.get('key')
        data = cache.share(key)
        if data:
            return web.json_response(data)
        else:
            return web.json_response({"error": "not_found"}, status=404)

    async def handle_status(request):
        json_safe_cache = {}
        for k, v in cache.cache.items():
            json_safe_cache[k] = v
            if 'owners' in v and isinstance(v['owners'], set):
                 json_safe_cache[k]['owners'] = list(v['owners'])

        hit_ratio = 0.0
        total_reads = cache.metrics_hits + cache.metrics_misses
        if total_reads > 0:
            hit_ratio = (cache.metrics_hits / total_reads) * 100

        return web.json_response({
            "node_id": node_id,
            "type": "cache_node",
            "cache_size": len(cache.cache),
            "cache_content": json_safe_cache,
            "metrics": {
                "hits": cache.metrics_hits,
                "misses": cache.metrics_misses,
                "total_reads": total_reads,
                "hit_ratio_percent": round(hit_ratio, 2)
            }
        })

    # === Setup Aiohttp App ===
    app = web.Application()
    app.add_routes([
        web.get("/cache/{key}", handle_read),
        web.post("/cache/{key}", handle_write),
        web.get("/status", handle_status),
        web.post("/bus/invalidate/{key}", handle_bus_invalidate),
        web.get("/bus/read/{key}", handle_bus_read)
    ])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", internal_port)

    logger.info(f"[{node_id}] Server AIOHTTP (Cache Node) berjalan di http://0.0.0.0:{internal_port}")
    await site.start()
    
    try:
        await asyncio.Event().wait()
    finally:
        await http_client.close()


if __name__ == "__main__":
    try:
        node_id = os.getenv('NODE_ID', 'cache-node-X')
        logging.info(f"[{node_id}] Memulai Node (Cache Coherence)...")
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Crash di main: {e}")