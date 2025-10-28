# src/nodes/cache_node.py
import os
import asyncio
import logging
import sys
import ssl # <-- Tambah import ssl
import json # <-- Import json
import time
import uuid # <-- Import uuid
from aiohttp import web, ClientSession, ClientResponse, TCPConnector # <-- TCPConnector
import aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry
from collections import OrderedDict
from typing import Any, Tuple

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
        self.cache = OrderedDict() # Key: str, Value: {'state': str, 'value': Any}
        logger.info(f"LRU Cache dibuat dengan kapasitas {self.capacity}")

        # Metrics
        self.metrics_hits = 0
        self.metrics_misses = 0
        self.metrics_invalidations_sent = 0
        self.metrics_invalidations_received = 0
        self.metrics_bus_reads_sent = 0
        self.metrics_bus_reads_served = 0

    def _get(self, key: str) -> dict | None:
        """Helper internal: ambil data dan pindahkan ke akhir jika ada."""
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def read(self, key: str) -> dict | None:
        """Membaca data dari cache. Mengembalikan data jika valid (E, M, S)."""
        logger.info(f"Membaca '{key}' dari cache lokal...")
        data = self._get(key)

        if data is None or data['state'] == 'I':
            log_state = data.get('state') if data else 'None'
            logger.info(f"Cache MISS lokal untuk '{key}' (State: {log_state})")
            self.metrics_misses += 1
            return None

        logger.info(f"Cache HIT lokal untuk '{key}'. State: {data['state']}, Value: {data['value']}")
        self.metrics_hits += 1
        return data

    def write(self, key: str, value: Any) -> dict:
        """Menulis data ke cache. Selalu set state ke 'Modified'."""
        logger.info(f"Menulis '{key}' = {value} ke cache lokal...")

        if key not in self.cache and len(self.cache) >= self.capacity:
            old_key, _ = self.cache.popitem(last=False)
            logger.info(f"Cache penuh. Mengeluarkan LRU: '{old_key}'")

        new_data = {'state': 'M', 'value': value}
        self.cache[key] = new_data
        self.cache.move_to_end(key)
        logger.info(f"Cache WRITE lokal untuk '{key}'. State diatur ke 'Modified'")
        return new_data

    def invalidate(self, key: str):
        """Menerima sinyal invalidate. Set state ke 'Invalid'."""
        # Gunakan _get agar LRU terupdate jika dibaca setelah invalidate (opsional)
        data = self._get(key)
        if data and data['state'] != 'I':
            logger.info(f"INVALIDATE diterima untuk '{key}'. State: {data['state']} -> 'Invalid'.")
            data['state'] = 'I'
            self.metrics_invalidations_received += 1
        elif data and data['state'] == 'I':
             logger.debug(f"INVALIDATE diterima untuk '{key}', state sudah 'Invalid'.")
        else:
             logger.debug(f"INVALIDATE diterima untuk '{key}', tapi key tidak ada di cache.")


    def share(self, key: str) -> dict | None:
        """Menerima sinyal BusRd. Jika state M/E, ubah ke S dan kembalikan data."""
        data = self._get(key)
        if data and data['state'] in ('M', 'E'):
            old_state = data['state']
            data['state'] = 'S'
            self.metrics_bus_reads_served += 1
            logger.info(f"SHARE (BusRd response) untuk '{key}'. State: {old_state} -> 'Shared'. Mengirim data.")
            return data # Kembalikan data dengan state baru
        elif data and data['state'] == 'S':
            # Jika sudah S, cukup kembalikan data (tidak perlu log state change)
            self.metrics_bus_reads_served += 1
            logger.debug(f"SHARE (BusRd response) untuk '{key}'. State sudah 'Shared'. Mengirim data.")
            return data
        elif data and data['state'] == 'I':
             logger.warning(f"SHARE (BusRd response) ditolak untuk '{key}'. State 'Invalid'.")
             return None # Jangan kirim data jika Invalid
        else:
             logger.warning(f"SHARE (BusRd response) ditolak untuk '{key}'. Key tidak ditemukan.")
             return None # Key tidak ada

# --- Logika Server Cache ---

async def main():
    node_id = os.getenv("NODE_ID", "cache-node-1")
    internal_port = int(os.getenv("INTERNAL_PORT", 8000))
    peers_env = os.getenv("PEERS", "")

    # Ambil path sertifikat
    cert_file = os.getenv("CERT_FILE")
    key_file = os.getenv("KEY_FILE")
    ca_file = os.getenv("CA_FILE")

    peers = [peer for peer in peers_env.split(",") if peer]
    cache = LruCache(capacity=10) # Ambil kapasitas dari Env Var? Misal: int(os.getenv("CACHE_CAPACITY", 10))

    # --- Konfigurasi SSL Context untuk Client (Bus) ---
    ssl_context_client = None
    use_https_client = False
    if ca_file:
        try:
            ssl_context_client = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_file)
            # Jika ingin mTLS client->server (cache node lain):
            # if cert_file and key_file:
            #     ssl_context_client.load_cert_chain(certfile=cert_file, keyfile=key_file)
            logger.info(f"SSL Context client (bus) dibuat, CA: {ca_file}")
            use_https_client = True
        except Exception as e:
            logger.error(f"Gagal membuat SSL Context client: {e}. Bus akan pakai HTTP.")
    else:
        logger.warning("CA_FILE tidak diset untuk client bus. Komunikasi bus akan menggunakan HTTP.")

    connector = TCPConnector(ssl=ssl_context_client if use_https_client else None)
    client_session = ClientSession(connector=connector)
    # --- Akhir Konfigurasi SSL Client ---

    retry_options = ExponentialRetry(attempts=3, start_timeout=0.2)
    http_client = RetryClient(client_session, retry_options=retry_options)

    # Tentukan protokol untuk URL peer
    peer_protocol = "https://" if use_https_client else "http://"

    # --- Fungsi "Bus" (Komunikasi Antar Node via HTTPS/HTTP) ---
    async def send_invalidate_to_peer(peer, key) -> bool:
        """Helper coroutine untuk mengirim 1 invalidate. Return True jika sukses."""
        url = f"{peer_protocol}{peer}:{internal_port}/bus/invalidate/{key}"
        try:
            logger.debug(f"Mengirim invalidate '{key}' ke {url}")
            async with http_client.post(url, timeout=0.5) as response:
                if response.status == 200:
                    # body = await response.json() # Opsional: cek body jika perlu
                    return True
                else:
                    logger.warning(f"Gagal invalidate {peer} untuk '{key}', status: {response.status}")
                    return False
        except ssl.SSLError as e: logger.error(f"SSL Error saat invalidate ke {peer} ({url}): {e}")
        except aiohttp.ClientConnectorCertificateError as e: logger.error(f"Certificate Error saat invalidate ke {peer} ({url}): {e}")
        except aiohttp.ClientConnectorError as e: logger.warning(f"Connection Error saat invalidate ke {peer} ({url}): {e}")
        except asyncio.TimeoutError: logger.warning(f"Timeout saat invalidate ke {peer} ({url})")
        except Exception as e: logger.error(f"Exception saat invalidate {peer} untuk '{key}' ({url}): {e}", exc_info=False) # Jangan terlalu verbose
        return False

    async def broadcast_invalidate(key: str):
        """Mengirim 'BusRdX' (Invalidate) ke semua rekan."""
        logger.info(f"[{node_id}] Menyiarkan INVALIDATE untuk '{key}' ke {peers}")
        cache.metrics_invalidations_sent += len(peers)
        tasks = [asyncio.create_task(send_invalidate_to_peer(peer, key)) for peer in peers]
        if tasks: await asyncio.gather(*tasks) # Tunggu semua selesai

    async def send_read_to_peer(peer, key) -> dict | None:
        """Helper coroutine untuk mengirim 1 BusRd. Return data jika ditemukan."""
        url = f"{peer_protocol}{peer}:{internal_port}/bus/read/{key}"
        try:
            logger.debug(f"Mengirim bus read '{key}' ke {url}")
            async with http_client.get(url, timeout=0.5) as response:
                if response.status == 200:
                    return await response.json()
                # Status 404 dianggap normal (peer tidak punya atau invalid)
                elif response.status != 404:
                     logger.warning(f"Gagal bus read dari {peer} untuk '{key}', status: {response.status}")
                return None
        except ssl.SSLError as e: logger.error(f"SSL Error saat bus read ke {peer} ({url}): {e}")
        except aiohttp.ClientConnectorCertificateError as e: logger.error(f"Certificate Error saat bus read ke {peer} ({url}): {e}")
        except aiohttp.ClientConnectorError as e: logger.warning(f"Connection Error saat bus read ke {peer} ({url}): {e}")
        except asyncio.TimeoutError: logger.warning(f"Timeout saat bus read ke {peer} ({url})")
        except Exception as e: logger.error(f"Exception saat bus read dari {peer} untuk '{key}' ({url}): {e}", exc_info=False)
        return None

    async def broadcast_read(key: str) -> dict | None:
        """Mengirim 'BusRd' (Read) ke semua rekan. Return data pertama yg ditemukan."""
        logger.info(f"[{node_id}] Menyiarkan READ (BusRd) untuk '{key}' ke {peers}")
        cache.metrics_bus_reads_sent += len(peers)
        tasks = [asyncio.create_task(send_read_to_peer(peer, key)) for peer in peers]
        if not tasks: return None

        # Tunggu hasil pertama yang valid (non-None)
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        data_found = None
        for task in done:
            try:
                result = task.result()
                if result and result.get('value') is not None:
                    peer_state = result.get('state', 'Unknown')
                    logger.info(f"READ HIT dari rekan via BusRd. State rekan: {peer_state}, Value: {result['value']}")
                    # Set state lokal ke Shared
                    cache.cache[key] = {'state': 'S', 'value': result['value']}
                    cache.cache.move_to_end(key) # Update LRU
                    data_found = cache.cache[key]
                    break # Ambil hasil pertama saja
            except Exception as e:
                 logger.warning(f"Error saat memproses hasil BusRd: {e}") # Harusnya tidak terjadi

        # Batalkan task yang belum selesai
        for task in pending: task.cancel()
        if pending: await asyncio.gather(*pending, return_exceptions=True) # Tunggu pembatalan

        if data_found: return data_found

        logger.info(f"READ MISS di semua rekan via BusRd untuk '{key}'")
        return None

    # === Definisi Handler REST API (dengan Audit Logging) ===
    async def handle_read(request):
        key = request.match_info.get('key')
        peer_ip = request.remote or "unknown"
        logger.info(f"Menerima READ request dari {peer_ip} untuk key '{key}'") # Audit Log
        data = cache.read(key) # read() sudah punya log HIT/MISS internal
        if data: return web.json_response(data)
        data_from_peer = await broadcast_read(key)
        if data_from_peer: return web.json_response(data_from_peer)
        logger.info(f"CACHE MISS GLOBAL untuk '{key}'. Mensimulasikan fetch dari memori utama.") # Audit Log
        new_value = f"data_dari_memori_untuk_{key}_{int(time.time())}"
        if key not in cache.cache and len(cache.cache) >= cache.capacity:
            old_k, _ = cache.cache.popitem(last=False); logger.info(f"Cache penuh saat fetch memori. Mengeluarkan LRU: '{old_k}'")
        cache.cache[key] = {'state': 'E', 'value': new_value}
        cache.cache.move_to_end(key)
        logger.info(f"Data '{key}' ditulis ke cache lokal dari memori utama. State: E") # Audit Log
        return web.json_response(cache.cache[key])

    async def handle_write(request):
        peer_ip = request.remote or "unknown"
        key = request.match_info.get('key')
        value = "N/A" # Default for logging
        try:
            data = await request.json()
            value = data.get('value')
            logger.info(f"Menerima WRITE request dari {peer_ip} untuk key '{key}' = '{value}'") # Audit Log
            if value is None:
                logger.warning(f"Bad request WRITE dari {peer_ip} untuk '{key}': 'value' tidak ada.") # Audit Log
                return web.json_response({"error": "Butuh 'value'"}, status=400)
            await broadcast_invalidate(key) # Siarkan invalidate SEBELUM menulis
            written_data = cache.write(key, value) # Tulis lokal (write() sudah punya log)
            return web.json_response(written_data)
        except json.JSONDecodeError:
             logger.warning(f"Bad request WRITE dari {peer_ip} untuk '{key}': Invalid JSON")
             return web.json_response({"error": "Invalid JSON body"}, status=400)
        except Exception as e:
            logger.error(f"Error di handle_write dari {peer_ip} untuk '{key}' = '{value}': {e}", exc_info=True) # Log Error
            return web.json_response({"error": "Internal server error"}, status=500)

    # --- Handler Internal untuk Bus ---
    async def handle_bus_invalidate(request):
        key = request.match_info.get('key')
        peer_ip = request.remote or "unknown"
        logger.info(f"Menerima BUS INVALIDATE dari {peer_ip} untuk key '{key}'") # Audit Log Bus
        cache.invalidate(key) # invalidate() sudah punya log internal
        return web.json_response({"status": "invalidated"})

    async def handle_bus_read(request):
        key = request.match_info.get('key')
        peer_ip = request.remote or "unknown"
        logger.info(f"Menerima BUS READ (BusRd) dari {peer_ip} untuk key '{key}'") # Audit Log Bus
        data = cache.share(key) # share() sudah punya log internal jika state berubah
        if data: return web.json_response(data)
        else:
            logger.warning(f"BUS READ MISS dari {peer_ip} untuk '{key}' (data tidak ada atau Invalid)") # Audit Log Bus Miss
            return web.json_response({"error": "not_found_or_invalid"}, status=404)

    async def handle_status(request):
         peer_ip = request.remote or "unknown"
         logger.debug(f"Menerima /status request dari {peer_ip}") # Log level debug
         json_safe_cache = {k: v for k, v in cache.cache.items()} # OrderedDict aman untuk JSON
         hit_ratio = 0.0
         total_reads = cache.metrics_hits + cache.metrics_misses
         if total_reads > 0: hit_ratio = (cache.metrics_hits / total_reads) * 100
         return web.json_response({
             "node_id": node_id, "type": "cache_node", "cache_size": len(cache.cache),
             "cache_capacity": cache.capacity, "cache_content": json_safe_cache,
             "metrics": {
                 "hits": cache.metrics_hits, "misses": cache.metrics_misses,
                 "total_reads": total_reads, "hit_ratio_percent": round(hit_ratio, 2),
                 "invalidations_sent": cache.metrics_invalidations_sent,
                 "invalidations_received": cache.metrics_invalidations_received,
                 "bus_reads_sent": cache.metrics_bus_reads_sent,
                 "bus_reads_served": cache.metrics_bus_reads_served
             }})

    # === Setup Aiohttp App ===
    app = web.Application()
    app.add_routes([
        web.get("/cache/{key}", handle_read), web.post("/cache/{key}", handle_write),
        web.get("/status", handle_status),
        web.post("/bus/invalidate/{key}", handle_bus_invalidate), web.get("/bus/read/{key}", handle_bus_read)
    ])

    # --- Konfigurasi SSL Context untuk Server ---
    ssl_context_server = None
    use_https_server = False
    if cert_file and key_file:
        try:
            ssl_context_server = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH) # Server tidak perlu client auth di mode ini
            ssl_context_server.load_cert_chain(certfile=cert_file, keyfile=key_file)
            # Opsi mTLS:
            # if ca_file:
            #     ssl_context_server.load_verify_locations(cafile=ca_file)
            #     ssl_context_server.verify_mode = ssl.CERT_REQUIRED
            logger.info(f"SSL Context server (cache) dibuat. Cert: {cert_file}")
            use_https_server = True
        except Exception as e:
            logger.error(f"Gagal membuat SSL Context server: {e}. Cache server berjalan di HTTP.")
    else:
        logger.warning("CERT_FILE/KEY_FILE tidak diset. Cache server berjalan di HTTP.")
    # --- Akhir Konfigurasi SSL Server ---

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", internal_port, ssl_context=ssl_context_server)

    server_protocol = "HTTPS" if use_https_server else "HTTP"
    logger.info(f"[{node_id}] Server AIOHTTP ({server_protocol}) (Cache Node) berjalan di {server_protocol.lower()}://0.0.0.0:{internal_port}")

    # Jalankan server web
    server_task = None
    try:
         server_task = asyncio.create_task(site.start())
         # Tahan server agar tetap berjalan
         await asyncio.Event().wait()
    except asyncio.CancelledError:
         logger.info("Cache server task dibatalkan.")
    except Exception as e:
         logger.error(f"Error saat startup cache server: {e}", exc_info=True)
    finally:
         logger.info("Memulai shutdown cache server...")
         # Hentikan server web
         await runner.cleanup()
         # Tutup client session
         await http_client.close()
         logger.info("Shutdown cache server selesai.")


if __name__ == "__main__":
    try:
        node_id = os.getenv('NODE_ID', 'cache-node-X')
        logging.info(f"[{node_id}] Memulai Node (Cache Coherence)...")
        # Handle KeyboardInterrupt dengan lebih baik
        loop = asyncio.get_event_loop()
        main_task = loop.create_task(main())
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        logging.info("Menerima KeyboardInterrupt, memulai shutdown...")
    except Exception as e:
        logging.critical(f"Critical error in cache_node main: {e}", exc_info=True)
    finally:
         # Pastikan loop event ditutup
         loop = asyncio.get_event_loop()
         if loop.is_running():
              loop.stop()
         # loop.close() # Hindari jika akan run lagi

