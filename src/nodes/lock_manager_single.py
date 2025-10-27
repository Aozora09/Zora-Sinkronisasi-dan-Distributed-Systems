import os
import asyncio
import logging
import sys
from aiohttp import web
# Pastikan import benar
from src.nodes.base_node import RaftNode 

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("LockManager")


async def main():
    node_id = os.getenv("NODE_ID", "node1")
    peers_env = os.getenv("PEERS", "")
    internal_port = int(os.getenv("INTERNAL_PORT", 8000))

    peers = {
        peer_id: f"http://{peer_id}:{internal_port}"
        for peer_id in peers_env.split(",")
        if peer_id and peer_id != node_id
    }

    # Gunakan RaftNode (yang berisi logika Raft)
    node = RaftNode(node_id=node_id, peers=peers, port=internal_port)

    # === Definisi REST API ===

    # --- Definisikan SEMUA handler SEBELUM 'app.add_routes' ---
    
    async def handle_vote(request):
        """Endpoint menerima vote request."""
        try:
            data = await request.json()
            # "Bongkar" JSON dan teruskan sebagai argumen
            response_dict = await node.handle_vote_rpc(
                candidate_id=data.get("candidateId"), 
                candidate_term=data.get("term"),
                candidate_log_index=data.get("lastLogIndex", -1),
                candidate_log_term=data.get("lastLogTerm", 0)
            )
            return web.json_response(response_dict)
        except Exception as e:
            logger.error(f"Error di handle_vote: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def handle_append_entries(request):
        """Endpoint menerima AppendEntries (heartbeat atau log)."""
        try:
            data = await request.json()
            # "Bongkar" JSON dan teruskan sebagai argumen
            response_dict = await node.handle_append_entries_rpc(
                leader_id=data.get("leaderId"), 
                leader_term=data.get("term"),
                prev_log_index=data.get("prevLogIndex", -1),
                prev_log_term=data.get("prevLogTerm", 0),
                entries=data.get("entries", []),
                leader_commit=data.get("leaderCommit", -1)
            )
            return web.json_response(response_dict)
        except Exception as e:
            logger.error(f"Error di handle_append_entries: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def handle_status(request):
        """Debug endpoint."""
        return web.json_response(node.status_dict()) 

    async def handle_client_request(request):
        """Endpoint untuk client."""
        # Akses instance node dari aplikasi
        node_instance: RaftNode = request.app['node_instance']
        
        if node_instance.state != 'leader':
            logger.warning(f"Menolak /client-request (bukan Leader).")
            return web.json_response({"error": "Bukan Leader", "leader_id": node_instance.leader_id}, status=400)

        try:
            data = await request.json()
            cmd_type = data.get('type') 
            lock_name = data.get('lock_name')
            client_id = data.get('client_id')
            
            if not all([cmd_type, lock_name, client_id]):
                return web.json_response({"error": "Format salah. Butuh 'type', 'lock_name', 'client_id'"}, status=400)
            
            if cmd_type == "ACQUIRE":
                mode = data.get('mode', 'exclusive')
                command = f"ACQUIRE_{mode.upper()} {lock_name} {client_id}"
            elif cmd_type == "RELEASE":
                command = f"RELEASE {lock_name} {client_id}"
            else:
                return web.json_response({"error": "Tipe command tidak dikenal"}, status=400)

            logger.info(f"Menerima /client-request: {command}")
            result = await node_instance.client_request(command) 
            
            if result.get("success"):
                return web.json_response({"status": "ok", "message": result.get("message")})
            else:
                # 400 Bad Request jika ditolak (misal: deadlock)
                # 503 Service Unavailable jika timeout
                status_code = 400 if "Deadlock" in result.get("message", "") else 503
                return web.json_response({"status": "error", "message": result.get("message")}, status=status_code)
                
        except Exception as e:
            logger.error(f"Error di handle_client_request: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    # === Setup Aiohttp App ===
    app = web.Application()
    app['node_instance'] = node 
    
    # --- RUTE RAFT YANG BENAR ---
    app.add_routes([
        web.post("/vote", handle_vote),
        web.post("/append-entries", handle_append_entries),
        web.get("/status", handle_status),
        web.post("/client-request", handle_client_request)
    ])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, node.host, node.port)

    logger.info(f"[{node.node_id}] Server AIOHTTP (Lock Manager - Raft Mode) berjalan di http://{node.host}:{node.port}")
    
    # Jalankan server web DAN node Raft secara bersamaan
    try:
        await asyncio.gather(
            site.start(), 
            node.start()
        )
    except asyncio.CancelledError:
        logger.info("Main task dibatalkan.")
    finally:
        await runner.cleanup()
        await node.stop()


if __name__ == "__main__":
    try:
        node_id = os.getenv('NODE_ID', 'nodeX')
        logging.info(f"[{node_id}] Memulai Node (Lock Manager - Raft Mode)...")
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown manual...")