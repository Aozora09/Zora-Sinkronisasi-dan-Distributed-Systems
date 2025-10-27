import os
import asyncio
import logging
import sys
from aiohttp import web
# Pastikan base_node.py ada di PYTHONPATH atau di folder yang benar
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
    
    node = RaftNode(node_id=node_id, peers=peers, port=internal_port)

    # === Definisi REST API (HANYA UNTUK RAFT) ===

    async def handle_vote(request):
        """Menerima RPC /vote dari node lain."""
        try:
            # PERBAIKAN: Ambil seluruh payload JSON
            payload_with_mac = await request.json()
            
            # PERBAIKAN: Teruskan seluruh dict ke method RaftNode
            response_dict = await node.handle_vote_rpc(payload_with_mac)
            
            return web.json_response(response_dict)
        except Exception as e:
            logger.error(f"Error di handle_vote: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def handle_append_entries(request):
        """Menerima RPC /append-entries dari leader."""
        try:
            # PERBAIKAN: Ambil seluruh payload JSON
            payload_with_mac = await request.json()
            
            # PERBAIKAN: Teruskan seluruh dict ke method RaftNode
            response_dict = await node.handle_append_entries_rpc(payload_with_mac)
            
            return web.json_response(response_dict)
        except Exception as e:
            logger.error(f"Error di handle_append_entries: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def handle_status(request):
        """Endpoint status untuk memantau node."""
        return web.json_response(node.status_dict())

    async def handle_client_request(request):
        """Endpoint untuk client mengirim command (acquire/release)."""
        if node.state != 'leader':
            logger.warning(f"Menolak /client-request (bukan Leader).")
            # Redirect ke leader jika diketahui
            leader_host = node.peers.get(node.leader_id, node.leader_id) # Coba cari URL-nya
            return web.json_response(
                {"error": "Bukan Leader", "leader_id": node.leader_id, "leader_hint": leader_host}, 
                status=400 # 400 Bad Request sering dipakai, atau 503 Service Unavailable
            )
            
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
            result = await node.client_request(command)
            
            if result.get("success"):
                return web.json_response({"status": "ok", "message": result.get("message")})
            else:
                # Kirim status 400 (Bad Request) jika permintaan ditolak (misal: deadlock)
                return web.json_response({"status": "error", "message": result.get("message")}, status=400)
                
        except Exception as e:
            logger.error(f"Error di handle_client_request: {e}")
            return web.json_response({"error": str(e)}, status=500)

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
    site = web.TCPSite(runner, node.host, node.port)
    
    logger.info(f"[{node.node_id}] Server AIOHTTP (Lock Manager) berjalan di http://{node.host}:{node.port}")
    
    # Menjalankan server web DAN node Raft secara bersamaan
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
        logging.info(f"[{node_id}] Memulai Node (Lock Manager)...")
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown manual...")
