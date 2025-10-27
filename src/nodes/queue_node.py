import os
import asyncio
import logging
import sys
import hashlib
import bisect
from aiohttp import web
import redis.asyncio as redis

# --- Konfigurasi Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("QueueNode")

# --- Implementasi Consistent Hashing ---
class ConsistentHashRing:
    def __init__(self, nodes=None, replicas=3):
        self.replicas = replicas
        self.ring = dict()
        self._sorted_keys = []
        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node: str):
        for i in range(self.replicas):
            virtual_key = f"{node}#{i}"
            key_hash = self._hash(virtual_key)
            self.ring[key_hash] = node
            bisect.insort(self._sorted_keys, key_hash)

    def get_node(self, key: str) -> str:
        if not self.ring:
            return None
        key_hash = self._hash(key)
        idx = bisect.bisect(self._sorted_keys, key_hash)
        if idx == len(self._sorted_keys):
            idx = 0
        target_hash = self._sorted_keys[idx]
        return self.ring[target_hash]

# --- Logika Server Antrian ---
async def main():
    node_id = os.getenv("NODE_ID", "queue-node-1")
    internal_port = int(os.getenv("INTERNAL_PORT", 8000))
    redis_nodes = ['redis-1', 'redis-2', 'redis-3']
    hash_ring = ConsistentHashRing(nodes=redis_nodes)

    redis_pools = {}
    for node_name in redis_nodes:
        try:
            pool = redis.ConnectionPool.from_url(f"redis://{node_name}:6379")
            redis_pools[node_name] = pool
            logger.info(f"[{node_id}] Membuat koneksi pool untuk {node_name}")
        except Exception as e:
            logger.error(f"[{node_id}] Gagal membuat koneksi pool untuk {node_name}: {e}")

    # === Definisi REST API ===

    async def handle_queue_produce(request):
        """Producer mengirim pesan ke 'queue_name'."""
        try:
            data = await request.json()
            queue_name = data.get('queue_name')
            message_body = data.get('message', {})

            if not queue_name or not message_body:
                return web.json_response({"error": "Butuh 'queue_name' dan 'message'"}, status=400)

            target_node_name = hash_ring.get_node(queue_name)
            if target_node_name not in redis_pools:
                return web.json_response({"error": f"Node Redis {target_node_name} tidak tersedia"}, status=500)

            logger.info(f"[{node_id}] Merutekan pesan untuk '{queue_name}' ke -> {target_node_name}")
            try:
                r = redis.Redis(connection_pool=redis_pools[target_node_name])
                message_id = await r.xadd(queue_name, message_body)
                await r.close()
                return web.json_response({
                    "status": "ok",
                    "routed_to": target_node_name,
                    "message_id": message_id.decode('utf-8')
                })
            except Exception as e:
                logger.error(f"[{node_id}] Gagal XADD ke {target_node_name}: {e}")
                return web.json_response({"error": f"Gagal mengirim ke Redis node {target_node_name}"}, status=500)

        except Exception as e:
            logger.error(f"[{node_id}] Error di handle_queue_produce: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def handle_create_consumer_group(request):
        """Membuat consumer group untuk sebuah stream (antrian)."""
        try:
            data = await request.json()
            queue_name = data.get('queue_name')
            group_name = data.get('group_name', 'grup_pekerja')

            if not queue_name:
                return web.json_response({"error": "Butuh 'queue_name'"}, status=400)

            target_node_name = hash_ring.get_node(queue_name)
            if target_node_name not in redis_pools:
                return web.json_response({"error": f"Node Redis {target_node_name} tidak tersedia"}, status=500)

            logger.info(f"Mencoba membuat consumer group '{group_name}' untuk '{queue_name}' di {target_node_name}")
            try:
                r = redis.Redis(connection_pool=redis_pools[target_node_name])
                await r.xgroup_create(queue_name, group_name, id='0', mkstream=True)
                await r.close()
                return web.json_response({"status": "ok", "message": f"Grup '{group_name}' dibuat untuk '{queue_name}' di {target_node_name}"})
            except Exception as e:
                logger.warning(f"Gagal membuat grup (mungkin sudah ada): {e}")
                return web.json_response({"status": "warning", "message": str(e)}, status=409)
        
        except Exception as e:
            logger.error(f"Error di handle_create_consumer_group: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def handle_queue_consume(request):
        """Consumer mengambil pesan dari 'queue_name'."""
        try:
            data = await request.json()
            queue_name = data.get('queue_name')
            group_name = data.get('group_name', 'grup_pekerja')
            consumer_id = data.get('consumer_id', 'consumer_1')

            if not queue_name or not consumer_id:
                return web.json_response({"error": "Butuh 'queue_name' dan 'consumer_id'"}, status=400)

            target_node_name = hash_ring.get_node(queue_name)
            if target_node_name not in redis_pools:
                return web.json_response({"error": f"Node Redis {target_node_name} tidak tersedia"}, status=500)

            logger.info(f"[{consumer_id}] mencoba mengambil pesan dari '{queue_name}' di {target_node_name}")
            try:
                r = redis.Redis(connection_pool=redis_pools[target_node_name])
                # Ambil 1 pesan, block selama 5 detik
                response = await r.xreadgroup(group_name, consumer_id, {queue_name: '>'}, count=1, block=5000)
                await r.close()

                if not response:
                    return web.json_response({"status": "empty", "message": "Tidak ada pesan baru"})

                stream, messages = response[0]
                message_id, message_body = messages[0]
                
                decoded_body = {k.decode('utf-8'): v.decode('utf-8') for k, v in message_body.items()}
                
                return web.json_response({
                    "status": "ok",
                    "queue": stream.decode('utf-8'),
                    "message_id": message_id.decode('utf-8'),
                    "message": decoded_body
                })
            except Exception as e:
                logger.error(f"Gagal XREADGROUP: {e}")
                return web.json_response({"error": f"Gagal membaca dari Redis: {e}"}, status=500)

        except Exception as e:
            logger.error(f"Error di handle_queue_consume: {e}")
            return web.json_response({"error": str(e)}, status=500)
            
    async def handle_status(request):
        """Endpoint status sederhana untuk queue node."""
        return web.json_response({
            "node_id": node_id,
            "type": "queue_node",
            "redis_nodes": redis_nodes
        })

    # === Setup Aiohttp App ===
    app = web.Application()
    app.add_routes([
        web.post("/queue/produce", handle_queue_produce),
        web.post("/queue/create-group", handle_create_consumer_group),
        web.post("/queue/consume", handle_queue_consume),
        web.get("/status", handle_status)
    ])
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", internal_port)
    logger.info(f"[{node_id}] Server AIOHTTP (Queue Node) berjalan di http://0.0.0.0:{internal_port}")
    await site.start()

    # Tahan server agar tetap berjalan
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        node_id = os.getenv('NODE_ID', 'queue-node-X')
        logging.info(f"[{node_id}] Memulai Node (Queue Manager)...")
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown manual...")
