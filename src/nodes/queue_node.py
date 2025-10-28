import os
import asyncio
import logging
import sys
import hashlib
import bisect
import ssl
from aiohttp import web
# Import redis.asyncio dan exceptions dari redis utama
import redis.asyncio as redis
import redis.exceptions

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger("QueueNode")

class ConsistentHashRing:
    """Implementasi Consistent Hashing sederhana."""
    def __init__(self, nodes=None, replicas=3):
        self.replicas = replicas
        self.ring = dict()
        self._sorted_keys = []
        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key: str) -> int:
        """Menggunakan MD5 hash untuk kunci."""
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node: str):
        """Menambahkan node (dengan replica) ke ring."""
        for i in range(self.replicas):
            virtual_key = f"{node}#{i}"
            key_hash = self._hash(virtual_key)
            self.ring[key_hash] = node
            # Masukkan hash ke list terurut
            bisect.insort(self._sorted_keys, key_hash)
        logger.debug(f"Node '{node}' ditambahkan ke ring dengan {self.replicas} replica.")

    def get_node(self, key: str) -> str | None:
        """Mendapatkan node yang bertanggung jawab untuk kunci."""
        if not self.ring:
            logger.warning("ConsistentHashRing kosong, tidak bisa mendapatkan node.")
            return None
        key_hash = self._hash(key)
        # Cari posisi insertion point untuk hash kunci
        idx = bisect.bisect_left(self._sorted_keys, key_hash)
        # Wrap around jika hash kunci lebih besar dari semua hash node
        if idx == len(self._sorted_keys):
            idx = 0
        target_hash = self._sorted_keys[idx]
        target_node = self.ring[target_hash]
        logger.debug(f"Kunci '{key}' (hash: {key_hash}) -> node '{target_node}' (hash: {target_hash})")
        return target_node

async def main():
    node_id = os.getenv("NODE_ID", "queue-node-1")
    internal_port = int(os.getenv("INTERNAL_PORT", 8000))
    # Ambil daftar node Redis dari env var
    redis_nodes_env = os.getenv("REDIS_NODES", "redis-1") # Default ke redis-1 jika tidak diset
    redis_nodes = [name.strip() for name in redis_nodes_env.split(',') if name.strip()]
    logger.info(f"Menginisialisasi Queue Node '{node_id}' dengan Redis nodes: {redis_nodes}")

    # Ambil path sertifikat
    cert_file = os.getenv("CERT_FILE")
    key_file = os.getenv("KEY_FILE")

    hash_ring = ConsistentHashRing(nodes=redis_nodes)

    # --- Inisialisasi Pool Redis (Tanpa Ping Eksplisit di Startup) ---
    redis_pools = {}
    successful_connections = []
    logger.info(f"Mencoba membuat koneksi pool Redis untuk: {redis_nodes}")
    for node_name in redis_nodes:
        pool = None # Inisialisasi pool ke None
        try:
            # Buat connection pool, asumsikan sukses jika tidak ada error
            pool = redis.ConnectionPool.from_url(
                f"redis://{node_name}:6379",
                decode_responses=True,        # Otomatis decode hasil ke string
                socket_connect_timeout=3,     # Timeout koneksi awal 3 detik
                socket_keepalive=True,        # Aktifkan keepalive TCP
                health_check_interval=30      # Cek koneksi idle setiap 30 detik
            )
            # --- BLOK PING DIHAPUS ---

            # Jika from_url tidak error, anggap pool siap
            redis_pools[node_name] = pool
            successful_connections.append(node_name)
            logger.info(f"[{node_id}] Koneksi pool Redis untuk '{node_name}' berhasil diinisialisasi.")

        except redis.exceptions.ConnectionError as e:
            logger.error(f"[{node_id}] GAGAL KONEKSI AWAL ke Redis node '{node_name}': {e}. Pool TIDAK akan dibuat.")
        except Exception as e:
            logger.error(f"[{node_id}] ERROR TIDAK DIKENAL saat inisialisasi pool Redis '{node_name}': {e}", exc_info=True)
            # Jika pool sempat dibuat tapi gagal, coba disconnect
            if 'pool' in locals() and pool:
                try: await pool.disconnect()
                except Exception: pass
    # --- Akhir Inisialisasi Pool Redis ---

    # Validasi koneksi
    if len(successful_connections) != len(redis_nodes):
         logger.warning(f"PERINGATAN: Tidak semua node Redis ({redis_nodes}) berhasil diinisialisasi poolnya. Hanya: {successful_connections}.")
    else:
         logger.info(f"Berhasil menginisialisasi pool untuk semua node Redis: {successful_connections}")

    # === Definisi Handler REST API (dengan Audit Logging & Perbaikan Exception) ===

    async def handle_queue_produce(request):
        peer_ip = request.remote or "unknown"; queue_name = "unknown"
        try:
            data = await request.json(); queue_name = data.get('queue_name')
            message_body = data.get('message', {}); logger.info(f"Menerima /queue/produce dari {peer_ip} untuk Queue='{queue_name}'")
            if not queue_name or not isinstance(message_body, dict) or not message_body:
                logger.warning(f"Bad req /produce {peer_ip}: Butuh 'queue_name' & 'message'(dict non-empty). Diterima q='{queue_name}', msg_type='{type(message_body)}'")
                return web.json_response({"error": "Butuh 'queue_name' dan 'message' (dict non-empty)"}, status=400)
            target_node_name = hash_ring.get_node(queue_name)
            # Periksa jika target node valid DAN ada di pool yang berhasil terhubung
            if not target_node_name or target_node_name not in redis_pools:
                logger.error(f"Routing err '{queue_name}' {peer_ip}: Redis target '{target_node_name}' tdk tersedia/connect.")
                return web.json_response({"error": f"Node Redis {target_node_name} tidak tersedia"}, status=503) # Service Unavailable
            logger.info(f"[{node_id}] Route msg '{queue_name}' ({peer_ip}) -> {target_node_name}")
            redis_client = None # Inisialisasi
            try:
                # Dapatkan koneksi dari pool
                redis_client = redis.Redis(connection_pool=redis_pools[target_node_name])
                # Pastikan message_body adalah dict string:string
                sanitized_body = {str(k): str(v) for k, v in message_body.items()}
                message_id = await redis_client.xadd(queue_name, sanitized_body)
                logger.info(f"Msg '{queue_name}' SUKSES -> {target_node_name} (MsgID: {message_id})")
                return web.json_response({"status": "ok", "routed_to": target_node_name, "message_id": message_id}) # message_id sudah string
            # Tangkap ConnectionError spesifik jika pool disconnect tiba-tiba
            except redis.exceptions.ConnectionError as conn_err:
                 logger.error(f"Conn Err XADD {target_node_name} Q='{queue_name}' ({peer_ip}): {conn_err}", exc_info=False) # Log lebih singkat
                 return web.json_response({"error": f"Koneksi Redis {target_node_name} gagal"}, status=503)
            except Exception as e:
                logger.error(f"Fail XADD {target_node_name} '{queue_name}' ({peer_ip}): {e}", exc_info=True)
                return web.json_response({"error": f"Gagal Redis {target_node_name}"}, status=500)
            finally:
                # Selalu tutup koneksi client setelah dipakai
                if redis_client: await redis_client.aclose()
        except Exception as e:
            logger.error(f"Error handle_produce {peer_ip} (Q: {queue_name}): {e}", exc_info=True)
            return web.json_response({"error": "Internal server error"}, status=500)

    async def handle_create_consumer_group(request):
        peer_ip = request.remote or "unknown"; queue_name = "unknown"; group_name = "unknown"
        try:
            data = await request.json(); queue_name = data.get('queue_name'); group_name = data.get('group_name', 'default_group')
            logger.info(f"Menerima /q/create-group {peer_ip} Q='{queue_name}', G='{group_name}'")
            if not queue_name: logger.warning(f"Bad req /create-group {peer_ip}: 'queue_name' missing."); return web.json_response({"error": "Butuh 'queue_name'"}, status=400)

            target_node_name = hash_ring.get_node(queue_name)
            if not target_node_name or target_node_name not in redis_pools: logger.error(f"Routing err /create-group '{queue_name}' {peer_ip}: Redis target '{target_node_name}' tdk tersedia."); return web.json_response({"error": f"Node Redis {target_node_name} tidak tersedia"}, status=503)

            logger.info(f"Try XGROUP CREATE '{group_name}' '{queue_name}' @ {target_node_name}")
            redis_client = None
            try:
                redis_client = redis.Redis(connection_pool=redis_pools[target_node_name])
                # mkstream=True akan membuat stream jika belum ada
                await redis_client.xgroup_create(queue_name, group_name, id='0', mkstream=True);
                logger.info(f"Group '{group_name}' SUKSES created '{queue_name}' @ {target_node_name}")
                return web.json_response({"status": "ok", "message": f"Grup '{group_name}' dibuat '{queue_name}' @ {target_node_name}"})
            except redis.exceptions.ResponseError as e: # Tangkap dari redis.exceptions
                 if "BUSYGROUP Consumer Group name already exists" in str(e):
                      logger.warning(f"Group '{group_name}' exists '{queue_name}' @ {target_node_name} (req {peer_ip}).")
                      return web.json_response({"status": "ok", "message": f"Grup '{group_name}' sudah ada."}, status=200) # Anggap sukses jika sudah ada
                 else: logger.error(f"Redis err XGROUP CREATE '{group_name}' @ {target_node_name}: {e}", exc_info=True); return web.json_response({"error": f"Gagal Redis: {e}"}, status=500)
            except redis.exceptions.ConnectionError as conn_err:
                 logger.error(f"Conn Err XGROUP CREATE {target_node_name} Q='{queue_name}' G='{group_name}' ({peer_ip}): {conn_err}", exc_info=False)
                 return web.json_response({"error": f"Koneksi Redis {target_node_name} gagal"}, status=503)
            except Exception as e:
                logger.error(f"Error XGROUP CREATE '{group_name}' @ {target_node_name}: {e}", exc_info=True)
                return web.json_response({"error": f"Gagal Redis: {e}"}, status=500)
            finally:
                if redis_client: await redis_client.aclose()
        except Exception as e:
            logger.error(f"Error handle_create_group {peer_ip} (Q: {queue_name}): {e}", exc_info=True)
            return web.json_response({"error": "Internal server error"}, status=500)

    async def handle_queue_consume(request):
        peer_ip = request.remote or "unknown"; queue_name = "unknown"; consumer_id = "unknown"; group_name = "unknown"
        try:
            data = await request.json(); queue_name = data.get('queue_name'); group_name = data.get('group_name', 'default_group'); consumer_id = data.get('consumer_id')
            logger.info(f"Menerima /q/consume {peer_ip} (C='{consumer_id}') Q='{queue_name}', G='{group_name}'")
            if not queue_name or not consumer_id: logger.warning(f"Bad req /consume {peer_ip}: Butuh 'queue_name' & 'consumer_id'."); return web.json_response({"error": "Butuh 'queue_name' dan 'consumer_id'"}, status=400)

            target_node_name = hash_ring.get_node(queue_name)
            if not target_node_name or target_node_name not in redis_pools: logger.error(f"Routing err /consume '{queue_name}' {peer_ip}: Redis target '{target_node_name}' tdk tersedia."); return web.json_response({"error": f"Node Redis {target_node_name} tidak tersedia"}, status=503)

            count = data.get('count', 1)
            block_ms = data.get('block_ms', 5000) # Default block 5 detik

            logger.info(f"[{consumer_id}] try XREADGROUP '{queue_name}' G='{group_name}' @ {target_node_name}")
            redis_client = None
            try:
                redis_client = redis.Redis(connection_pool=redis_pools[target_node_name])
                # '>' berarti hanya pesan baru yang belum pernah dikirim ke consumer manapun di grup ini
                response = await redis_client.xreadgroup(group_name, consumer_id, {queue_name: '>'}, count=count, block=block_ms)

                if not response:
                    logger.debug(f"[{consumer_id}] No new msg '{queue_name}' G='{group_name}' @ {target_node_name}.")
                    return web.json_response({"status": "empty", "message": "Tidak ada pesan baru", "messages": []})

                # Proses respons
                stream_name, messages_raw = response[0]
                processed_messages = []
                for msg_id_raw, msg_body_raw in messages_raw:
                     processed_messages.append({"message_id": msg_id_raw, "message": msg_body_raw})
                     logger.info(f"[{consumer_id}] SUKSES consume msg {msg_id_raw} '{queue_name}' @ {target_node_name}.")

                # TODO: Implementasi XACK setelah client memproses pesan

                return web.json_response({"status": "ok", "queue": stream_name, "messages": processed_messages})

            except redis.exceptions.ResponseError as e: # Tangkap dari redis.exceptions
                 # Error umum jika grup tidak ada atau stream tidak ada
                 if "NOGROUP No such key" in str(e) or "NOGROUP No such consumer group" in str(e):
                      logger.warning(f"Group '{group_name}' or stream '{queue_name}' not found @ {target_node_name} for {consumer_id} (req {peer_ip}). Create group first.")
                      return web.json_response({"error": f"Group '{group_name}' atau queue '{queue_name}' belum dibuat. Panggil /queue/create-group dulu."}, status=404) # Not Found
                 else:
                      logger.error(f"Redis err XREADGROUP '{queue_name}' C='{consumer_id}' @ {target_node_name}: {e}", exc_info=True)
                      return web.json_response({"error": f"Gagal Redis: {e}"}, status=500)
            except redis.exceptions.ConnectionError as conn_err:
                 logger.error(f"Conn Err XREADGROUP {target_node_name} Q='{queue_name}' G='{group_name}' C='{consumer_id}' ({peer_ip}): {conn_err}", exc_info=False)
                 return web.json_response({"error": f"Koneksi Redis {target_node_name} gagal"}, status=503)
            except Exception as e:
                logger.error(f"Error XREADGROUP '{queue_name}' C='{consumer_id}' @ {target_node_name}: {e}", exc_info=True)
                return web.json_response({"error": f"Gagal Redis: {e}"}, status=500)
            finally:
                if redis_client: await redis_client.aclose() # Selalu tutup client

        except Exception as e:
            logger.error(f"Error handle_consume {peer_ip} (Q: {queue_name}, C: {consumer_id}): {e}", exc_info=True)
            return web.json_response({"error": "Internal server error"}, status=500)

    async def handle_status(request):
        peer_ip = request.remote or "unknown"; logger.debug(f"Menerima /status req {peer_ip}")
        # Status koneksi pool (hanya yang berhasil diinisialisasi)
        connected_pools = list(redis_pools.keys())
        return web.json_response({
            "node_id": node_id,
            "type": "queue_node",
            "redis_nodes_configured": redis_nodes,
            "redis_pools_connected": connected_pools
        })

    # === Setup Aiohttp App ===
    app = web.Application()
    app.add_routes([
        web.post("/queue/produce", handle_queue_produce),
        web.post("/queue/create-group", handle_create_consumer_group),
        web.post("/queue/consume", handle_queue_consume),
        web.get("/status", handle_status)
    ])

    # --- Konfigurasi SSL Server ---
    ssl_context_server = None
    if cert_file and key_file:
        try:
            ssl_context_server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ssl_context_server.load_cert_chain(certfile=cert_file, keyfile=key_file)
            logger.info(f"SSL Context server (queue) dibuat. Cert: {cert_file}")
        except Exception as e: logger.error(f"Gagal SSL Context server: {e}"); ssl_context_server = None
    else: logger.warning("CERT/KEY file tdk diset. Queue server HTTP.")
    # --- Akhir SSL Server ---

    # --- PERBAIKAN: Pindahkan on_shutdown SEBELUM runner.setup() ---
    async def cleanup_redis_pools(app_instance): # Ubah nama argumen
        logger.info("Menutup koneksi pool Redis saat shutdown...")
        disconnect_tasks = []
        # Gunakan redis_pools dari scope luar
        for node_name, pool in redis_pools.items():
            logger.debug(f"Menjadwalkan penutupan pool {node_name}")
            if pool: disconnect_tasks.append(pool.disconnect())
        try:
            # Tunggu semua disconnect selesai dengan timeout
            await asyncio.wait_for(asyncio.gather(*disconnect_tasks, return_exceptions=True), timeout=5.0)
            logger.info("Semua pool Redis berhasil ditutup.")
        except asyncio.TimeoutError: logger.warning("Timeout saat menunggu penutupan pool Redis.")
        except Exception as e: logger.error(f"Error saat menutup pool Redis: {e}")

    app.on_shutdown.append(cleanup_redis_pools)
    # --- AKHIR PERBAIKAN ---

    runner = web.AppRunner(app)
    await runner.setup() # Setup setelah on_shutdown ditambahkan
    site = web.TCPSite(runner, "0.0.0.0", internal_port, ssl_context=ssl_context_server)

    protocol = "HTTPS" if ssl_context_server else "HTTP"
    logger.info(f"[{node_id}] Server AIOHTTP ({protocol}) (Queue Node) on {protocol.lower()}://0.0.0.0:{internal_port}")

    try:
         await site.start()
         # Tahan server agar tetap berjalan sampai dihentikan (misal oleh Ctrl+C)
         await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
         logger.info("Menerima sinyal shutdown...")
    except Exception as e:
        logger.error(f"Error saat startup/runtime queue server: {e}", exc_info=True)
    finally:
         logger.info("Memulai proses shutdown...")
         await runner.cleanup() # Ini akan memicu on_shutdown
         logger.info("Queue server shutdown selesai.")


if __name__ == "__main__":
    try:
        node_id = os.getenv('NODE_ID', 'queue-node-X')
        logging.info(f"[{node_id}] Memulai Node (Queue Manager)...")
        # Handle loop policy Windows jika perlu (biasanya tidak masalah di Python 3.8+)
        # if sys.platform == 'win32':
        #     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown manual terdeteksi.")
    except Exception as e:
        logger.critical(f"Critical error di __main__ queue_node: {e}", exc_info=True)

