# src/nodes/lock_manager.py
import json
import os
import asyncio
import logging
import sys
import ssl # <-- Tambah import ssl
from aiohttp import web
# Pastikan path import benar
from src.nodes.base_node import RaftNode

# Konfigurasi logging (diatur oleh base_node, tapi atur format logger ini)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger("LockManager")

async def main():
    node_id = os.getenv("NODE_ID", "node1")
    peers_env = os.getenv("PEERS", "")
    internal_port = int(os.getenv("INTERNAL_PORT", 8000))

    # Ambil path sertifikat dari env var
    cert_file = os.getenv("CERT_FILE")
    key_file = os.getenv("KEY_FILE")
    ca_file = os.getenv("CA_FILE") # Untuk mTLS jika diaktifkan

    # --- Konfigurasi SSL Context untuk Server ---
    ssl_context = None
    use_https = False
    if cert_file and key_file:
        try:
            ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH) # Minta client auth (mTLS)?
            ssl_context.load_cert_chain(certfile=cert_file, keyfile=key_file)
            # Jika ingin mTLS (memverifikasi client cert dari CA kita):
            # if ca_file:
            #     ssl_context.load_verify_locations(cafile=ca_file)
            #     ssl_context.verify_mode = ssl.CERT_REQUIRED
            # else:
            #     ssl_context.verify_mode = ssl.CERT_NONE # Hanya enkripsi server
            logger.info(f"SSL Context server dibuat. Cert: {cert_file}, Key: {key_file}")
            use_https = True
        except FileNotFoundError:
            logger.error(f"File sertifikat/kunci server tidak ditemukan: {cert_file} / {key_file}. Server akan berjalan di HTTP.")
        except ssl.SSLError as e:
            logger.error(f"SSL Error saat memuat sertifikat server: {e}. Server akan berjalan di HTTP.")
        except Exception as e:
            logger.error(f"Error membuat SSL context server: {e}. Server akan berjalan di HTTP.")
    else:
        logger.warning("CERT_FILE atau KEY_FILE tidak diset. Server akan berjalan di HTTP.")
    # --- Akhir Konfigurasi SSL Context Server ---

    # Tentukan URL peers berdasarkan apakah server ini HTTPS atau HTTP
    protocol = "https://" if use_https else "http://"
    peers = {}
    for peer_id in peers_env.split(","):
        if peer_id and peer_id != node_id:
            peers[peer_id] = f"{protocol}{peer_id}:{internal_port}"

    # Inisialisasi RaftNode dengan peers yang sesuai (https/http)
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
        except Exception as e:
            logger.error(f"Error di handle_append_entries dari {peer_ip} (Leader: {leader_id}): {e}", exc_info=True)
            # Kirim respons error generik
            error_data = {"term": node.current_term, "success": False}
            error_bytes = json.dumps(error_data, sort_keys=True).encode('utf-8')
            return web.json_response({"data": error_data, "mac": node._generate_mac(error_bytes)}, status=500)


    async def handle_status(request):
        peer_ip = request.remote or "unknown"
        logger.debug(f"Menerima /status request dari {peer_ip}") # Log level debug
        return web.json_response(node.status_dict())

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

            if node.state != 'leader':
                logger.warning(f"Menolak /client-request dari {peer_ip} (bukan Leader). Hint: {node.leader_id}") # Audit Log Penolakan
                # Berikan hint URL leader (sudah https/http dari init node)
                leader_url_hint = node.peers.get(node.leader_id) if node.leader_id in node.peers else None
                if not leader_url_hint and node.leader_id == node.node_id: # Jika leader adalah diri sendiri (N=1?)
                     leader_url_hint = f"{protocol}{node.host}:{node.port}" # Jarang terjadi tapi mungkin

                return web.json_response(
                    {"error": "Bukan Leader", "leader_id": node.leader_id, "leader_hint": leader_url_hint},
                    status=400 # 400 Bad Request lebih cocok
                )

            if cmd_type == "ACQUIRE":
                mode = data.get('mode', 'exclusive')
                command = f"ACQUIRE_{mode.upper()} {lock_name} {client_id}"
            elif cmd_type == "RELEASE":
                command = f"RELEASE {lock_name} {client_id}"
            else:
                logger.warning(f"Bad request /client-request dari {peer_ip}: Tipe command tidak dikenal '{cmd_type}'") # Audit Log Error
                return web.json_response({"error": "Tipe command tidak dikenal"}, status=400)

            logger.info(f"Meneruskan client command ke RaftNode: {command}") # Audit Log
            result = await node.client_request(command)

            status_code = 200 if result.get("success") else 400 # Default 400 jika gagal
            if "timeout" in result.get("message", ""): status_code = 503 # Service Unavailable jika timeout Raft internal
            elif "Deadlock" in result.get("message", ""): status_code = 409 # Conflict jika deadlock

            logger.info(f"Hasil /client-request dari {peer_ip} ({command}): Success={result.get('success')}, Msg='{result.get('message')}', Status={status_code}") # Audit Log Hasil
            return web.json_response({"status": "ok" if result.get("success") else "error", "message": result.get("message")}, status=status_code)

        except json.JSONDecodeError:
             logger.warning(f"Bad request /client-request dari {peer_ip}: Invalid JSON")
             return web.json_response({"error": "Invalid JSON body"}, status=400)
        except Exception as e:
            logger.error(f"Error di handle_client_request dari {peer_ip} (Command: {command}): {e}", exc_info=True)
            return web.json_response({"error": "Internal server error"}, status=500)

    # === Setup Aiohttp App ===
    app = web.Application()
    app['node_instance'] = node

    app.add_routes([
        web.post("/vote", handle_vote),
        web.post("/append-entries", handle_append_entries),
        web.get("/status", handle_status),
        web.post("/client-request", handle_client_request)
    ])

    runner = web.AppRunner(app)
    await runner.setup()
    # Gunakan ssl_context yang sudah dibuat sebelumnya
    site = web.TCPSite(runner, node.host, node.port, ssl_context=ssl_context)

    server_protocol = "HTTPS" if use_https else "HTTP"
    logger.info(f"[{node.node_id}] Server AIOHTTP ({server_protocol}) (Lock Manager) berjalan di {server_protocol.lower()}://{node.host}:{node.port}")

    # Jalankan server web DAN node Raft secara bersamaan
    node_task = None
    try:
        # Mulai node Raft DULU agar siap saat server mulai terima request
        node_task = asyncio.create_task(node.start())
        # Beri sedikit waktu untuk node Raft mulai
        await asyncio.sleep(0.1)
        await site.start()
        # Tunggu node Raft selesai (jika stop dipanggil atau error)
        await node_task
    except asyncio.CancelledError:
        logger.info("Main task dibatalkan.")
    except Exception as e:
         logger.error(f"Error fatal saat startup: {e}", exc_info=True)
    finally:
        logger.info("Memulai shutdown...")
        # Hentikan node Raft jika masih berjalan
        if node_task and not node_task.done():
            node_task.cancel()
            await node.stop() # Panggil stop secara eksplisit
            try:
                await node_task # Tunggu pembatalan
            except asyncio.CancelledError:
                 pass # Diharapkan
        elif node:
             await node.stop() # Jika node_task sudah selesai tapi node belum stop

        await runner.cleanup() # Cleanup server web
        logger.info("Shutdown selesai.")


if __name__ == "__main__":
    try:
        node_id = os.getenv('NODE_ID', 'nodeX')
        logging.info(f"[{node_id}] Memulai Node (Lock Manager)...")
        # Handle KeyboardInterrupt dengan lebih baik
        loop = asyncio.get_event_loop()
        main_task = loop.create_task(main())
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        logging.info("Menerima KeyboardInterrupt, memulai shutdown...")
        # Pembatalan task utama akan ditangani oleh blok finally di main()
    except Exception as e:
         logging.critical(f"Critical error in __main__: {e}", exc_info=True)
    finally:
         # Pastikan loop event ditutup
         loop = asyncio.get_event_loop()
         if loop.is_running():
              loop.stop()
         # loop.close() # Hindari close jika akan run lagi (misal di test)

