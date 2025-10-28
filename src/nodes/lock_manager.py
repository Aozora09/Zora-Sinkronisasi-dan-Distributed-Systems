# src/nodes/lock_manager.py
import json
import os
import asyncio
import logging
import sys
import ssl # <-- Tambah import ssl
from aiohttp import web
# Pastikan path import benar
# (Asumsi struktur folder: project_root/src/nodes/...)
try:
     # Jika dijalankan dari root (misal: python -m src.nodes.lock_manager)
     from src.nodes.base_node import RaftNode
except ImportError:
     # Jika dijalankan langsung dari folder nodes (misal: python lock_manager.py)
     # atau untuk pytest
     from base_node import RaftNode


# Konfigurasi logging (diatur oleh base_node, tapi atur format logger ini)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger("LockManager")

async def main():
    node_id = os.getenv("NODE_ID", "node1")
    peers_env = os.getenv("PEERS", "")
    internal_port = int(os.getenv("INTERNAL_PORT", 8000))

    # Ambil path sertifikat dari env var (DI DALAM CONTAINER)
    cert_file_path = os.getenv("CERT_FILE") # Misal: /etc/ssl/certs/myapp/node1.crt
    key_file_path = os.getenv("KEY_FILE")   # Misal: /etc/ssl/certs/myapp/node1.key
    ca_file_path = os.getenv("CA_FILE")     # Misal: /etc/ssl/certs/myapp/ca.crt (Untuk mTLS jika diaktifkan server)

    # --- Konfigurasi SSL Context untuk Server ---
    ssl_context_server = None
    use_https = False
    if cert_file_path and key_file_path:
        try:
            # Server context: Membutuhkan cert & key server
            ssl_context_server = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH) # Minta client auth (mTLS)? TIDAK defaultnya
            ssl_context_server.load_cert_chain(certfile=cert_file_path, keyfile=key_file_path)

            # Jika ingin mTLS (memverifikasi client cert dari CA kita):
            # if ca_file_path:
            #     try:
            #         ssl_context_server.load_verify_locations(cafile=ca_file_path)
            #         ssl_context_server.verify_mode = ssl.CERT_REQUIRED
            #         logger.info(f"mTLS diaktifkan. Server akan memverifikasi client cert menggunakan CA: {ca_file_path}")
            #     except FileNotFoundError:
            #         logger.error(f"CA file untuk mTLS tidak ditemukan: {ca_file_path}. mTLS dinonaktifkan.")
            #         ssl_context_server.verify_mode = ssl.CERT_NONE
            #     except ssl.SSLError as e_ca:
            #         logger.error(f"SSL Error saat memuat CA untuk mTLS: {e_ca}. mTLS dinonaktifkan.")
            #         ssl_context_server.verify_mode = ssl.CERT_NONE
            # else:
            #     ssl_context_server.verify_mode = ssl.CERT_NONE # Hanya enkripsi server
            #     logger.info("mTLS tidak diaktifkan (CA_FILE tidak diset untuk server).")

            logger.info(f"SSL Context server dibuat. Cert: {cert_file_path}, Key: {key_file_path}")
            use_https = True
        except FileNotFoundError:
            logger.error(f"File sertifikat/kunci server tidak ditemukan: Cert='{cert_file_path}', Key='{key_file_path}'. Server akan berjalan di HTTP.")
        except ssl.SSLError as e:
            logger.error(f"SSL Error saat memuat sertifikat server: {e}. Server akan berjalan di HTTP.")
        except Exception as e:
            logger.error(f"Error membuat SSL context server: {e}. Server akan berjalan di HTTP.")
    else:
        logger.warning("CERT_FILE atau KEY_FILE tidak diset di env. Server akan berjalan di HTTP.")
    # --- Akhir Konfigurasi SSL Context Server ---

    # Tentukan URL peers berdasarkan apakah client Raft akan menggunakan HTTPS atau HTTP
    # (Ini ditentukan oleh base_node.py berdasarkan CA_FILE client)
    # Kita tetap perlu membuat peers dict, tapi base_node yang menentukan protokol akhirnya.
    # Asumsikan base_node akan mencoba HTTPS jika CA_FILE diset.
    client_will_use_https = bool(os.getenv("CA_FILE")) # Cek apakah CA client diset
    peer_protocol = "https://" if client_will_use_https else "http://"
    peers = {}
    for peer_id in peers_env.split(","):
        peer_id = peer_id.strip() # Hapus spasi
        if peer_id and peer_id != node_id:
            # Buat URL awal berdasarkan asumsi client
            peers[peer_id] = f"{peer_protocol}{peer_id}:{internal_port}"

    # Inisialisasi RaftNode
    node = RaftNode(node_id=node_id, peers=peers, port=internal_port)

    # === Definisi Handler API (dengan Audit Logging) ===
    async def handle_vote(request):
        """Menerima RPC /vote dari node lain."""
        peer_ip = request.remote or "unknown"
        candidate_id = "unknown" # Default
        try:
            payload_with_mac = await request.json()
            candidate_id = payload_with_mac.get("data", {}).get("candidate_id", "unknown")
            logger.info(f"Menerima /vote dari {peer_ip} (Kandidat: {candidate_id})") # Audit Log
            response_dict = await node.handle_vote_rpc(payload_with_mac)
            logger.info(f"Membalas /vote ke {peer_ip} (Kandidat: {candidate_id}): granted={response_dict.get('data',{}).get('vote_granted')}") # Audit Log
            return web.json_response(response_dict)
        except json.JSONDecodeError:
             logger.error(f"Error JSON decode di handle_vote dari {peer_ip} (Kandidat: {candidate_id})")
             # Kirim respons error generik dengan MAC
             error_data = {"term": node.current_term, "vote_granted": False}
             error_bytes = json.dumps(error_data, sort_keys=True).encode('utf-8')
             return web.json_response({"data": error_data, "mac": node._generate_mac(error_bytes)}, status=400) # Bad request
        except Exception as e:
            logger.error(f"Error di handle_vote dari {peer_ip} (Kandidat: {candidate_id}): {e}", exc_info=True) # Log Error detail
            # Kirim respons error generik
            error_data = {"term": node.current_term, "vote_granted": False}
            error_bytes = json.dumps(error_data, sort_keys=True).encode('utf-8')
            return web.json_response({"data": error_data, "mac": node._generate_mac(error_bytes)}, status=500)

    async def handle_append_entries(request):
        """Menerima RPC /append-entries dari leader."""
        peer_ip = request.remote or "unknown"
        leader_id = "unknown" # Default
        try:
            payload_with_mac = await request.json()
            leader_id = payload_with_mac.get("data", {}).get("leader_id", "unknown")
            num_entries = len(payload_with_mac.get("data", {}).get("entries", []))
            logger.info(f"Menerima /append-entries dari {peer_ip} (Leader: {leader_id}, #Entries: {num_entries})") # Audit Log
            response_dict = await node.handle_append_entries_rpc(payload_with_mac)
            logger.info(f"Membalas /append-entries ke {peer_ip} (Leader: {leader_id}): success={response_dict.get('data',{}).get('success')}") # Audit Log
            return web.json_response(response_dict)
        except json.JSONDecodeError:
             logger.error(f"Error JSON decode di handle_append_entries dari {peer_ip} (Leader: {leader_id})")
             error_data = {"term": node.current_term, "success": False}
             error_bytes = json.dumps(error_data, sort_keys=True).encode('utf-8')
             return web.json_response({"data": error_data, "mac": node._generate_mac(error_bytes)}, status=400)
        except Exception as e:
            logger.error(f"Error di handle_append_entries dari {peer_ip} (Leader: {leader_id}): {e}", exc_info=True)
            # Kirim respons error generik
            error_data = {"term": node.current_term, "success": False}
            error_bytes = json.dumps(error_data, sort_keys=True).encode('utf-8')
            return web.json_response({"data": error_data, "mac": node._generate_mac(error_bytes)}, status=500)


    async def handle_status(request):
        peer_ip = request.remote or "unknown"
        logger.debug(f"Menerima /status request dari {peer_ip}") # Log level debug
        try:
            return web.json_response(node.status_dict())
        except Exception as e:
            logger.error(f"Error saat mengambil status: {e}", exc_info=True)
            return web.json_response({"error": "Gagal mengambil status internal"}, status=500)


    async def handle_client_request(request):
        """Endpoint untuk client mengirim command (acquire/release)."""
        peer_ip = request.remote or "unknown"
        command = "invalid_format" # Default untuk logging error
        try:
            data = await request.json()
            cmd_type = data.get('type')
            lock_name = data.get('lock_name')
            client_id = data.get('client_id')
            logger.info(f"Menerima /client-request dari {peer_ip}: Type={cmd_type}, Lock={lock_name}, Client={client_id}") # Audit Log

            if not all([cmd_type, lock_name, client_id]):
                logger.warning(f"Bad request /client-request dari {peer_ip}: Format salah") # Audit Log Error
                return web.json_response({"error": "Format salah. Butuh 'type', 'lock_name', 'client_id'"}, status=400)

            # --- Perbaikan: Cek leader_id sebelum redirect ---
            current_leader_id = node.leader_id # Ambil snapshot
            if node.state != 'leader':
                logger.warning(f"Menolak /client-request dari {peer_ip} (bukan Leader). Hint: {current_leader_id}") # Audit Log Penolakan
                # Berikan hint URL leader
                leader_url_hint = None
                if current_leader_id: # Hanya jika leader ID diketahui
                    leader_url_hint = node.peers.get(current_leader_id)
                    # Jika leader adalah diri sendiri (N=1 tapi belum jadi leader?) atau hint tidak ada di peers
                    if not leader_url_hint and current_leader_id == node.node_id:
                        # Kita tidak tahu pasti protokol leader jika belum terpilih,
                        # tapi jika kita tahu leader_id adalah diri sendiri, kita bisa coba tebak.
                        # Asumsikan protokol client Raft yang akan digunakan
                        protocol = "https://" if isinstance(getattr(node.http_session, 'connector', None).ssl, ssl.SSLContext) else "http://"
                        leader_url_hint = f"{protocol}{node.host}:{node.port}" # Jarang terjadi tapi mungkin

                return web.json_response(
                    {"error": "Bukan Leader", "leader_id": current_leader_id, "leader_hint": leader_url_hint},
                    status=400 # 400 Bad Request lebih cocok
                )
            # --- Akhir Perbaikan Cek Leader ---

            if cmd_type == "ACQUIRE":
                mode = data.get('mode', 'exclusive').upper() # Pastikan UPPERCASE
                if mode not in ["EXCLUSIVE", "SHARED"]:
                     logger.warning(f"Bad request /client-request ACQUIRE dari {peer_ip}: Mode tidak valid '{mode}'")
                     return web.json_response({"error": f"Mode tidak valid: {mode}. Harus 'exclusive' atau 'shared'."}, status=400)
                command = f"ACQUIRE_{mode} {lock_name} {client_id}"
            elif cmd_type == "RELEASE":
                command = f"RELEASE {lock_name} {client_id}"
            else:
                logger.warning(f"Bad request /client-request dari {peer_ip}: Tipe command tidak dikenal '{cmd_type}'") # Audit Log Error
                return web.json_response({"error": "Tipe command tidak dikenal"}, status=400)

            logger.info(f"Meneruskan client command ke RaftNode: {command}") # Audit Log
            result = await node.client_request(command)

            # Tentukan status code berdasarkan hasil
            status_code = 200 if result.get("success") else 400 # Default 400 jika gagal
            message = result.get("message", "")
            if "timeout" in message.lower(): status_code = 503 # Service Unavailable jika timeout Raft internal
            elif "deadlock" in message.lower(): status_code = 409 # Conflict jika deadlock
            elif message == "Bukan Leader": status_code = 400 # Jika state berubah saat request diproses

            logger.info(f"Hasil /client-request dari {peer_ip} ({command}): Success={result.get('success')}, Msg='{message}', Status={status_code}") # Audit Log Hasil
            # Kembalikan respons yang lebih detail
            response_body = {"status": "ok" if result.get("success") else "error", "message": message}
            if not result.get("success") and "leader_hint" in result:
                 response_body["leader_id"] = result.get("leader_id")
                 response_body["leader_hint"] = result.get("leader_hint")
            return web.json_response(response_body, status=status_code)

        except json.JSONDecodeError:
            logger.warning(f"Bad request /client-request dari {peer_ip}: Invalid JSON")
            return web.json_response({"error": "Invalid JSON body"}, status=400)
        except Exception as e:
            logger.error(f"Error di handle_client_request dari {peer_ip} (Command: {command}): {e}", exc_info=True)
            return web.json_response({"error": "Internal server error"}, status=500)

    # --- Alias-friendly endpoints (compatibility) ---
    # /lock/acquire and /lock/release adalah wrapper untuk demo/manual curl
    # Mereka meniru format input yang mungkin lebih intuitif
    async def handle_lock_acquire_alias(request):
        peer_ip = request.remote or "unknown"
        logger.info(f"Menerima alias /lock/acquire dari {peer_ip}")
        try:
            data = await request.json()
        except json.JSONDecodeError:
            logger.warning(f"Bad alias request /lock/acquire dari {peer_ip}: Invalid JSON")
            return web.json_response({"error": "Invalid JSON"}, status=400)

        resource = data.get("resource") or data.get("lock_name")
        client_id = data.get("client") or data.get("client_id")
        mode = data.get("mode", "exclusive") # Default ke exclusive

        if not resource or not client_id:
            logger.warning(f"Bad alias request /lock/acquire dari {peer_ip}: Missing 'resource' or 'client'")
            return web.json_response({"error": "Butuh 'resource' (atau 'lock_name') dan 'client' (atau 'client_id')"}, status=400)

        # Terjemahkan ke format /client-request
        client_request_payload = {
            "type": "ACQUIRE",
            "lock_name": resource,
            "client_id": client_id,
            "mode": mode
        }
        # Buat request internal ke handler utama
        # Ini kurang ideal, lebih baik panggil node.client_request langsung
        # tapi untuk demo alias, kita bisa forward request
        # NOTE: Ini tidak akan berfungsi jika handler utama butuh info request asli
        # Lebih baik langsung panggil node.client_request

        mode_upper = mode.upper()
        if mode_upper not in ["EXCLUSIVE", "SHARED"]:
             logger.warning(f"Bad alias request /lock/acquire dari {peer_ip}: Mode tidak valid '{mode}'")
             return web.json_response({"error": f"Mode tidak valid: {mode}. Harus 'exclusive' atau 'shared'."}, status=400)

        cmd = f"ACQUIRE_{mode_upper} {resource} {client_id}"
        logger.info(f"[alias] Meneruskan ke RaftNode: {cmd}")
        result = await node.client_request(cmd) # Panggil fungsi RaftNode langsung

        status_code = 200 if result.get("success") else 400
        message = result.get("message", "")
        if "timeout" in message.lower(): status_code = 503
        elif "deadlock" in message.lower(): status_code = 409
        elif message == "Bukan Leader": status_code = 400

        logger.info(f"Hasil alias /lock/acquire dari {peer_ip}: Success={result.get('success')}, Msg='{message}', Status={status_code}")
        response_body = {"status": "ok" if result.get("success") else "error", "message": message}
        if not result.get("success") and "leader_hint" in result:
             response_body["leader_id"] = result.get("leader_id")
             response_body["leader_hint"] = result.get("leader_hint")
        return web.json_response(response_body, status=status_code)


    async def handle_lock_release_alias(request):
        peer_ip = request.remote or "unknown"
        logger.info(f"Menerima alias /lock/release dari {peer_ip}")
        try:
            data = await request.json()
        except json.JSONDecodeError:
            logger.warning(f"Bad alias request /lock/release dari {peer_ip}: Invalid JSON")
            return web.json_response({"error": "Invalid JSON"}, status=400)

        resource = data.get("resource") or data.get("lock_name")
        client_id = data.get("client") or data.get("client_id")
        if not resource or not client_id:
            logger.warning(f"Bad alias request /lock/release dari {peer_ip}: Missing 'resource' or 'client'")
            return web.json_response({"error": "Butuh 'resource' (atau 'lock_name') dan 'client' (atau 'client_id')"}, status=400)

        # Langsung panggil node.client_request
        cmd = f"RELEASE {resource} {client_id}"
        logger.info(f"[alias] Meneruskan ke RaftNode: {cmd}")
        result = await node.client_request(cmd)

        status_code = 200 if result.get("success") else 400
        message = result.get("message", "")
        if "timeout" in message.lower(): status_code = 503
        elif message == "Bukan Leader": status_code = 400

        logger.info(f"Hasil alias /lock/release dari {peer_ip}: Success={result.get('success')}, Msg='{message}', Status={status_code}")
        response_body = {"status": "ok" if result.get("success") else "error", "message": message}
        if not result.get("success") and "leader_hint" in result:
             response_body["leader_id"] = result.get("leader_id")
             response_body["leader_hint"] = result.get("leader_hint")
        return web.json_response(response_body, status=status_code)

    async def list_local_locks_alias(request):
        """Expose current in-memory state machine for quick debug (read-only)."""
        peer_ip = request.remote or "unknown"
        logger.debug(f"Menerima alias /locks dari {peer_ip}")
        try:
            # Gunakan node.status_dict() untuk presentasi JSON-safe
            status = node.status_dict()
            return web.json_response({"state_machine": status.get("state_machine", {}), "node_id": status.get("node_id")})
        except Exception as e:
            logger.error(f"Error di list_local_locks_alias: {e}", exc_info=True)
            return web.json_response({"error": "Gagal mengambil state machine lokal"}, status=500)


    # === Setup Aiohttp App ===
    app = web.Application()
    app['node_instance'] = node # Simpan instance node jika perlu diakses handler lain

    app.add_routes([
        web.post("/vote", handle_vote),
        web.post("/append-entries", handle_append_entries),
        web.get("/status", handle_status),
        web.post("/client-request", handle_client_request),
        # Endpoint alias untuk kemudahan demo/curl
        web.post("/lock/acquire", handle_lock_acquire_alias),
        web.post("/lock/release", handle_lock_release_alias),
        web.get("/locks", list_local_locks_alias) # Endpoint GET untuk melihat state
    ])

    # === Cleanup saat Shutdown ===
    async def cleanup_node(app_instance):
         logger.info("Signal shutdown diterima, menghentikan node Raft...")
         # Ambil instance node dari app
         raft_node_instance = app_instance.get('node_instance')
         if raft_node_instance:
             await raft_node_instance.stop()
         else:
             logger.warning("Instance RaftNode tidak ditemukan di app saat cleanup.")

    app.on_shutdown.append(cleanup_node)

    # === Start Server & Node ===
    runner = web.AppRunner(app)
    await runner.setup()
    # Gunakan ssl_context_server yang sudah dibuat sebelumnya
    site = web.TCPSite(runner, node.host, node.port, ssl_context=ssl_context_server)

    server_protocol = "HTTPS" if use_https else "HTTP"
    logger.info(f"[{node.node_id}] Server AIOHTTP ({server_protocol}) (Lock Manager) berjalan di {server_protocol.lower()}://{node.host}:{node.port}")

    # Jalankan server web DAN node Raft secara bersamaan
    node_task = None
    site_task = None
    try:
        # Mulai node Raft DULU
        logger.info("Memulai background task RaftNode...")
        await node.start() # start() sekarang non-blocking
        node_task = asyncio.gather(node._election_task, node._heartbeat_task, node._applier_task) # Kumpulkan task utama Raft

        # Beri sedikit waktu untuk node Raft mulai (opsional tapi aman)
        await asyncio.sleep(0.5)

        logger.info("Memulai server web aiohttp...")
        await site.start()
        site_task = asyncio.current_task() # Dapatkan task server web saat ini

        # ----- PERBAIKAN: Jaga agar main() tetap berjalan -----
        # Gunakan asyncio.Event untuk menunggu sinyal shutdown (misal dari Ctrl+C)
        shutdown_event = asyncio.Event()
        await shutdown_event.wait()
        # ---------------------------------------------------

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Menerima sinyal shutdown (KeyboardInterrupt/Cancelled)...")
    except Exception as e:
        logger.error(f"Error fatal saat startup/runtime: {e}", exc_info=True)
    finally:
        logger.info("Memulai shutdown terstruktur...")

        # 1. Hentikan server web dulu agar tidak terima request baru
        logger.info("Menghentikan server web aiohttp...")
        await runner.cleanup() # Ini juga akan memanggil on_shutdown (cleanup_node)
        if site_task and not site_task.done():
            site_task.cancel()
            try: await site_task
            except asyncio.CancelledError: pass

        # 2. Hentikan node Raft (seharusnya sudah dipanggil oleh on_shutdown, tapi panggil lagi untuk safety)
        if node:
            await node.stop()

        # 3. Cancel task Raft utama jika masih ada
        if node_task and not node_task.done():
            node_task.cancel()
            try: await node_task
            except asyncio.CancelledError: pass

        logger.info("Shutdown lock_manager selesai.")


if __name__ == "__main__":
    try:
        node_id = os.getenv('NODE_ID', 'nodeX')
        logging.info(f"[{node_id}] Memulai Proses Utama (Lock Manager)...")
        # Handle loop policy Windows jika perlu (biasanya tidak masalah di Python 3.8+)
        # if sys.platform == 'win32':
        #     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Proses utama dihentikan oleh KeyboardInterrupt.")
    except Exception as e:
        logging.critical(f"Critical error di __main__: {e}", exc_info=True)
    finally:
        logging.info("Proses utama lock_manager berakhir.")
        # Loop event akan ditutup otomatis oleh asyncio.run()
