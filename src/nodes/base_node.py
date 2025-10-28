# src/nodes/base_node.py
import asyncio
import hashlib
import hmac
import time
import random
import logging
from typing import Dict, Optional, List, Any
import json
import os
import ssl # <-- Tambah import ssl

import aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")

class RaftNode:
    """
    Implementasi Raft Node dengan MAC untuk otentikasi.
    Update: Menambahkan HTTPS client dan audit logging.
    """

    def __init__(self, node_id: str, peers: Dict[str, str], host: str = "0.0.0.0", port: int = 8000):
        self.node_id = node_id
        # Peers sekarang berisi URL https://
        self.peers = peers.copy()
        self.host = host
        self.port = int(port)

        # === RAFT STATE ===
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.log: List[Dict[str, Any]] = []
        self.commit_index: int = -1
        self.last_applied: int = -1
        self.state: str = "follower"
        self.leader_id: Optional[str] = None
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # === STATE MACHINE ===
        self.state_machine: Dict[str, Any] = {}
        self.wait_queue: Dict[str, set] = {}
        self.commit_events: Dict[int, Dict] = {}

        # === Timers & Tasks ===
        self.heartbeat_interval: float = 1.0
        self._election_min = 5.0
        self._election_max = 10.0
        self.election_timeout = self._rand_election_timeout()
        self._last_heartbeat_ts = time.time()
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._applier_task: Optional[asyncio.Task] = None

        self.http_session: Optional[aiohttp.ClientSession] = None
        self.votes_received = set()
        self.logger = logging.getLogger(self.node_id)

        # --- LOGIKA MAC ---
        mac_key_str = os.getenv("SHARED_MAC_KEY", "default_insecure_key")
        if len(mac_key_str) < 16:
            self.logger.warning("SHARED_MAC_KEY terlalu pendek! Harap set di .env")
        self.mac_key = mac_key_str.encode('utf-8')
        # --- AKHIR LOGIKA MAC ---

        self.logger.info(f"Initialized. peers={list(self.peers.keys())}, port={self.port}")

    def _rand_election_timeout(self) -> float:
        return random.uniform(self._election_min, self._election_max)

    # --- FUNGSI HELPER MAC ---
    def _generate_mac(self, message_bytes: bytes) -> str:
        return hmac.new(self.mac_key, message_bytes, hashlib.sha256).hexdigest()

    def _verify_mac(self, message_bytes: bytes, received_mac: str) -> bool:
        if not received_mac: return False
        expected_mac = self._generate_mac(message_bytes)
        return hmac.compare_digest(expected_mac, received_mac)
    # --- AKHIR HELPER MAC ---

    # ---------------------------
    # Lifecycle: start / stop
    # ---------------------------
    async def start(self):
        """Memulai semua background task dan HTTPS client."""
        # --- Konfigurasi SSL Context untuk Client ---
        ca_file = os.getenv("CA_FILE")
        cert_file = os.getenv("CERT_FILE") # Client cert (untuk mTLS jika server minta)
        key_file = os.getenv("KEY_FILE")   # Client key (untuk mTLS jika server minta)
        ssl_context = None
        use_https = False # Flag apakah kita pakai HTTPS atau fallback ke HTTP

        if ca_file:
            try:
                ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_file)
                # Jika ingin mTLS (client juga kirim sertifikat):
                # if cert_file and key_file:
                #     ssl_context.load_cert_chain(certfile=cert_file, keyfile=key_file)
                self.logger.info(f"SSL Context client dibuat, CA: {ca_file}")
                use_https = True
            except FileNotFoundError:
                self.logger.error(f"File sertifikat CA tidak ditemukan: {ca_file}. Client akan menggunakan HTTP.")
            except ssl.SSLError as e:
                 self.logger.error(f"SSL Error saat memuat sertifikat client: {e}. Client akan menggunakan HTTP.")
            except Exception as e:
                 self.logger.error(f"Error membuat SSL context client: {e}. Client akan menggunakan HTTP.")
        else:
            self.logger.warning("CA_FILE tidak diset. Komunikasi client akan menggunakan HTTP.")

        # Buat connector dengan SSL context (jika valid)
        connector = aiohttp.TCPConnector(ssl=ssl_context if use_https else None)
        client_session = aiohttp.ClientSession(connector=connector)
        # --- Akhir Konfigurasi SSL Context ---

        retry_options = ExponentialRetry(attempts=5, start_timeout=0.5, max_timeout=10)
        self.http_session = RetryClient(client_session, retry_options=retry_options) # Gunakan session (HTTPS or HTTP)

        # Ubah URL peers jika tidak pakai HTTPS
        if not use_https:
             self.peers = {pid: url.replace("https://", "http://") for pid, url in self.peers.items()}
             self.logger.warning("Menjalankan client dalam mode HTTP.")

        self.logger.info(f"Node {self.node_id} starting as {self.state}")
        loop = asyncio.get_running_loop()
        self._election_task = loop.create_task(self._election_timer_loop())
        self._heartbeat_task = loop.create_task(self._append_entries_loop())
        self._applier_task = loop.create_task(self._commit_log_applier())

        try:
            await asyncio.gather(self._election_task, self._heartbeat_task, self._applier_task)
        except asyncio.CancelledError:
            pass
        finally:
            if self.http_session:
                await self.http_session.close() # Close session (termasuk connector)
                self.http_session = None
            self.logger.info(f"Node {self.node_id} stopped.")

    async def stop(self):
        if self._election_task: self._election_task.cancel()
        if self._heartbeat_task: self._heartbeat_task.cancel()
        if self._applier_task: self._applier_task.cancel()

    # ---------------------------
    # Background Tasks (Timers)
    # ---------------------------
    async def _election_timer_loop(self):
        while True:
            await asyncio.sleep(0.15)
            if self.state == "leader": continue
            elapsed = time.time() - self._last_heartbeat_ts
            if elapsed >= self.election_timeout:
                self.logger.info(f"Election timeout ({elapsed:.2f}s >= {self.election_timeout:.2f}s) — start election")
                await self._start_election()

    async def _append_entries_loop(self):
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            if self.state == "leader":
                self.logger.debug(f"(Leader) Mengirim AppendEntries (heartbeat) ke {list(self.peers.keys())}")
                tasks = []
                for peer_id in self.peers.keys():
                    if peer_id not in self.next_index: self.next_index[peer_id] = len(self.log)
                    tasks.append(self._replicate_log_to_peer(peer_id))
                if tasks: await asyncio.gather(*tasks, return_exceptions=True)

    async def _commit_log_applier(self):
        while True:
            await asyncio.sleep(0.1)
            if self.commit_index > self.last_applied:
                for i in range(self.last_applied + 1, self.commit_index + 1):
                    if i >= len(self.log):
                        self.logger.error(f"FATAL: commit_index {self.commit_index} > log length {len(self.log)}")
                        continue
                    entry = self.log[i]
                    command = entry['command']
                    self.logger.info(f"Menerapkan log [{i}] ke state machine: {command}")
                    try:
                        cmd_parts = command.split(' ')
                        cmd_type = cmd_parts[0]
                        lock_name = cmd_parts[1]
                        client_id = cmd_parts[2]
                        # ... (Logika state machine sama) ...
                        if cmd_type == "ACQUIRE_EXCLUSIVE":
                            if lock_name in self.state_machine:
                                self.logger.warning(f"Gagal ACQUIRE_EXCLUSIVE: Lock '{lock_name}' sudah diambil. {client_id} menunggu.")
                                if lock_name not in self.wait_queue: self.wait_queue[lock_name] = set()
                                self.wait_queue[lock_name].add(client_id)
                            else:
                                self.state_machine[lock_name] = {'mode': 'exclusive', 'owners': {client_id}}
                                self.logger.info(f"Lock '{lock_name}' diambil (exclusive) oleh {client_id}.")
                        elif cmd_type == "ACQUIRE_SHARED":
                             if lock_name not in self.state_machine:
                                self.state_machine[lock_name] = {'mode': 'shared', 'owners': {client_id}}
                                self.logger.info(f"Lock '{lock_name}' diambil (shared) oleh {client_id}.")
                             elif self.state_machine[lock_name]['mode'] == 'shared':
                                self.state_machine[lock_name]['owners'].add(client_id)
                                self.logger.info(f"Lock '{lock_name}' ditambah (shared) oleh {client_id}.")
                             else: # Dipegang exclusive
                                self.logger.warning(f"Gagal ACQUIRE_SHARED: Lock '{lock_name}' dipegang exclusive. {client_id} menunggu.")
                                if lock_name not in self.wait_queue: self.wait_queue[lock_name] = set()
                                self.wait_queue[lock_name].add(client_id)
                        elif cmd_type == "RELEASE":
                             if lock_name not in self.state_machine:
                                self.logger.warning(f"Gagal RELEASE: Lock '{lock_name}' tidak ada.")
                             elif client_id not in self.state_machine[lock_name]['owners']:
                                self.logger.warning(f"Gagal RELEASE: {client_id} tidak memegang lock '{lock_name}'.")
                             else:
                                self.state_machine[lock_name]['owners'].remove(client_id)
                                self.logger.info(f"{client_id} melepaskan lock '{lock_name}'.")
                                if not self.state_machine[lock_name]['owners']:
                                    del self.state_machine[lock_name]
                                    self.logger.info(f"Lock '{lock_name}' sekarang bebas.")
                        self.logger.debug(f"State machine: {self.state_machine}")
                        self.logger.debug(f"Wait queue: {self.wait_queue}")
                    except Exception as e:
                        self.logger.error(f"Gagal menerapkan command '{command}': {e}")
                    self.last_applied = i
                    if i in self.commit_events:
                        self.commit_events[i]['success'] = True
                        self.commit_events[i]['event'].set()

    # ---------------------------
    # Election Flow
    # ---------------------------
    async def _start_election(self):
        self.state = "candidate"
        self.current_term += 1
        term_of_this_election = self.current_term
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self._last_heartbeat_ts = time.time()
        self.election_timeout = self._rand_election_timeout()
        self.logger.info(f"Becoming CANDIDATE for term={self.current_term}")
        total_nodes = len(self.peers) + 1
        majority = total_nodes // 2 + 1
        self.logger.info(f"Membutuhkan {majority} suara untuk menang (dari {total_nodes} node).")
        tasks = [self._request_vote(peer_id, base_url) for peer_id, base_url in self.peers.items()]
        results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []
        if self.state != "candidate" or self.current_term != term_of_this_election:
            self.logger.info(f"Term berubah saat voting. Pemilu dibatalkan.")
            return
        for r, peer_id in zip(results, list(self.peers.keys())):
            if isinstance(r, Exception): self.logger.debug(f"Vote request ke {peer_id} error: {r}")
            elif r: self.votes_received.add(peer_id)
        self.logger.info(f"Pemilu selesai. Menerima total {len(self.votes_received)} suara.")
        if len(self.votes_received) >= majority: await self._become_leader()
        else: self.logger.info("Kalah pemilu.")

    async def _request_vote(self, peer_id: str, base_url: str) -> bool:
        """Kirim RPC RequestVote ke peer (HTTPS or HTTP)."""
        if not self.http_session: return False
        url = f"{base_url.rstrip('/')}/vote" # base_url sudah https/http dari start()
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]['term'] if last_log_index >= 0 else 0
        payload_data = {"candidate_id": self.node_id, "term": self.current_term, "last_log_index": last_log_index, "last_log_term": last_log_term}
        payload_bytes = json.dumps(payload_data, sort_keys=True).encode('utf-8')
        payload_with_mac = {"data": payload_data, "mac": self._generate_mac(payload_bytes)}
        self.logger.debug(f"Mengirim RequestVote (term {self.current_term}) ke {url}")
        try:
            async with self.http_session.post(url, json=payload_with_mac, timeout=3.0) as resp:
                if resp.status == 200:
                    response_with_mac = await resp.json()
                    response_data = response_with_mac.get("data")
                    response_mac = response_with_mac.get("mac")
                    if response_data is None or response_mac is None:
                         self.logger.error(f"Balasan /vote dari {peer_id} tidak lengkap")
                         return False
                    response_bytes = json.dumps(response_data, sort_keys=True).encode('utf-8')
                    if not self._verify_mac(response_bytes, response_mac):
                         self.logger.error(f"Balasan /vote dari {peer_id} GAGAL VERIFIKASI MAC!")
                         return False
                    if response_data.get("term", 0) > self.current_term: await self._become_follower(response_data.get("term"))
                    return response_data.get("vote_granted", False)
                else:
                    self.logger.warning(f"Vote request ke {peer_id} gagal, status: {resp.status}, body: {await resp.text()}")
                    return False
        except ssl.SSLError as e: self.logger.error(f"SSL Error saat request vote ke {peer_id}: {e}")
        except aiohttp.ClientConnectorCertificateError as e: self.logger.error(f"Certificate Verification Error saat request vote ke {peer_id}: {e}")
        except aiohttp.ClientConnectorError as e: self.logger.error(f"Connection Error saat request vote ke {peer_id}: {e}")
        except asyncio.TimeoutError: self.logger.warning(f"Timeout saat request vote ke {peer_id}")
        except Exception as e: self.logger.error(f"Vote request ke {peer_id} exception: {e}", exc_info=True)
        return False

    # ---------------------------
    # Log Replication Flow
    # ---------------------------
    async def _replicate_log_to_peer(self, peer_id: str):
        """Satu task per peer untuk mengirim AppendEntries (HTTPS or HTTP)."""
        if not self.http_session or self.state != "leader": return
        if peer_id not in self.peers: return # Peer tidak ada
        url = f"{self.peers[peer_id].rstrip('/')}/append-entries" # URL sudah https/http
        if peer_id not in self.next_index: self.next_index[peer_id] = len(self.log)
        next_idx = self.next_index[peer_id]
        prev_log_index = next_idx - 1
        prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0
        entries = self.log[next_idx:]
        payload_data = {"leader_id": self.node_id, "term": self.current_term, "prev_log_index": prev_log_index, "prev_log_term": prev_log_term, "entries": entries, "leader_commit": self.commit_index}
        payload_bytes = json.dumps(payload_data, sort_keys=True).encode('utf-8')
        payload_with_mac = {"data": payload_data, "mac": self._generate_mac(payload_bytes)}
        self.logger.debug(f"Mengirim AppendEntries (prevIdx={prev_log_index}, #entries={len(entries)}) ke {url}")
        try:
            async with self.http_session.post(url, json=payload_with_mac, timeout=3.0) as resp:
                if resp.status == 200:
                    response_with_mac = await resp.json()
                    response_data = response_with_mac.get("data")
                    response_mac = response_with_mac.get("mac")
                    if response_data is None or response_mac is None:
                         self.logger.error(f"Balasan /append-entries dari {peer_id} tidak lengkap")
                         return
                    response_bytes = json.dumps(response_data, sort_keys=True).encode('utf-8')
                    if not self._verify_mac(response_bytes, response_mac):
                         self.logger.error(f"Balasan /append-entries dari {peer_id} GAGAL VERIFIKASI MAC!")
                         return
                    peer_term = response_data.get("term", 0)
                    if peer_term > self.current_term:
                        self.logger.info(f"Peer {peer_id} punya term lebih tinggi {peer_term} -> mundur.")
                        await self._become_follower(peer_term)
                        return
                    if response_data.get("success", False):
                        new_next_index = prev_log_index + 1 + len(entries)
                        self.next_index[peer_id] = new_next_index
                        self.match_index[peer_id] = new_next_index - 1
                        self.logger.debug(f"Replikasi ke {peer_id} berhasil. nextIndex={self.next_index[peer_id]}, matchIndex={self.match_index[peer_id]}")
                        await self._check_for_commit()
                    else:
                        self.logger.warning(f"Replikasi ke {peer_id} gagal (term:{peer_term}, log mismatch?). Mundur 1.")
                        self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)
                else:
                    self.logger.warning(f"AppendEntries ke {peer_id} gagal, status: {resp.status}, body: {await resp.text()}")
        except ssl.SSLError as e: self.logger.error(f"SSL Error saat append entries ke {peer_id}: {e}")
        except aiohttp.ClientConnectorCertificateError as e: self.logger.error(f"Certificate Verification Error saat append entries ke {peer_id}: {e}")
        except aiohttp.ClientConnectorError as e: self.logger.error(f"Connection Error saat append entries ke {peer_id}: {e}")
        except asyncio.TimeoutError: self.logger.warning(f"Timeout saat append entries ke {peer_id}")
        except Exception as e: self.logger.error(f"AppendEntries ke {peer_id} exception: {e}", exc_info=True)

    async def _check_for_commit(self):
        """Cek apakah ada log baru yang bisa di-commit."""
        if self.state != "leader": return
        total_nodes = len(self.peers) + 1
        majority = total_nodes // 2 + 1
        for n in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[n]['term'] != self.current_term: continue
            count = 1 + sum(1 for pid in self.peers if self.match_index.get(pid, -1) >= n)
            if count >= majority:
                if n > self.commit_index:
                    self.commit_index = n
                    self.logger.info(f"🎉 Log index {n} di-commit oleh mayoritas ({count}).")
                break

    # ---------------------------
    # State Transitions Helpers
    # ---------------------------
    async def _become_leader(self):
        self.state = "leader"
        self.leader_id = self.node_id
        self.logger.info(f"🏆 Node {self.node_id} WON election and is now LEADER (term={self.current_term})")
        log_len = len(self.log)
        for peer_id in self.peers:
            self.next_index[peer_id] = log_len
            self.match_index[peer_id] = -1
        self.logger.info("(Leader) Mengirim heartbeat pertama...")
        tasks = [self._replicate_log_to_peer(peer_id) for peer_id in self.peers.keys()]
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)

    async def _become_follower(self, new_term: int):
        self.state = "follower"
        self.current_term = new_term
        self.voted_for = None
        self.leader_id = None
        self._last_heartbeat_ts = time.time()
        self.election_timeout = self._rand_election_timeout()
        self.logger.info(f"Node {self.node_id} -> follower (term={self.current_term}), new election_timeout={self.election_timeout:.2f}s")

    # ---------------------------
    # Public RPC Handlers
    # ---------------------------
    async def handle_vote_rpc(self, payload_with_mac: Dict) -> Dict:
        """Logika untuk membalas RPC /vote (DENGAN VERIFIKASI MAC)."""
        candidate_id = payload_with_mac.get("data", {}).get("candidate_id", "unknown")
        candidate_term = payload_with_mac.get("data", {}).get("term", "unknown")
        self.logger.info(f"Menerima RequestVote RPC dari {candidate_id} (term: {candidate_term})")
        # ... (logika verifikasi MAC dan voting sama) ...
        received_data = payload_with_mac.get("data")
        received_mac = payload_with_mac.get("mac")
        if received_data is None or received_mac is None:
            self.logger.error("RPC /vote tidak lengkap (missing data/mac)")
            return {"data": {"term": self.current_term, "vote_granted": False}, "mac": "invalid"}
        data_bytes = json.dumps(received_data, sort_keys=True).encode('utf-8')
        if not self._verify_mac(data_bytes, received_mac):
            self.logger.error("RPC /vote GAGAL VERIFIKASI MAC!")
            return {"data": {"term": self.current_term, "vote_granted": False}, "mac": "invalid"}
        candidate_id = received_data.get("candidate_id") # Ambil lagi setelah verifikasi
        candidate_term = received_data.get("term")
        candidate_log_index = received_data.get("last_log_index", -1)
        candidate_log_term = received_data.get("last_log_term", 0)
        vote_granted = False
        if candidate_term < self.current_term:
            self.logger.debug(f"Menolak vote {candidate_id} (term kadaluwarsa {candidate_term})")
        elif candidate_term > self.current_term:
            await self._become_follower(candidate_term)
        if candidate_term == self.current_term:
            if (self.voted_for is None or self.voted_for == candidate_id):
                our_last_log_index = len(self.log) - 1
                our_last_log_term = self.log[our_last_log_index]['term'] if our_last_log_index >= 0 else 0
                candidate_is_up_to_date = (candidate_log_term > our_last_log_term or (candidate_log_term == our_last_log_term and candidate_log_index >= our_last_log_index))
                if candidate_is_up_to_date:
                    self.logger.info(f"Memberi suara untuk {candidate_id} (Term {self.current_term})")
                    vote_granted = True
                    self.voted_for = candidate_id
                    self._last_heartbeat_ts = time.time() # Reset timer
                else: self.logger.warning(f"Menolak vote {candidate_id} (log-nya ketinggalan)")
            else: self.logger.warning(f"Menolak vote {candidate_id} (sudah vote {self.voted_for})")
        # --- Akhir logika voting ---
        response_data = {"term": self.current_term, "vote_granted": vote_granted}
        response_bytes = json.dumps(response_data, sort_keys=True).encode('utf-8')
        response_mac = self._generate_mac(response_bytes)
        self.logger.info(f"Membalas RequestVote ke {candidate_id}: granted={vote_granted}, term={self.current_term}")
        return {"data": response_data, "mac": response_mac}

    async def handle_append_entries_rpc(self, payload_with_mac: Dict) -> Dict:
        """Logika untuk membalas RPC /append-entries (DENGAN VERIFIKASI MAC)."""
        leader_id = payload_with_mac.get("data", {}).get("leader_id", "unknown")
        leader_term = payload_with_mac.get("data", {}).get("term", "unknown")
        prev_idx = payload_with_mac.get("data", {}).get("prev_log_index", "-")
        num_entries = len(payload_with_mac.get("data", {}).get("entries", []))
        self.logger.info(f"Menerima AppendEntries RPC dari {leader_id} (term: {leader_term}, prevIdx: {prev_idx}, #entries: {num_entries})")
        # ... (logika verifikasi MAC dan append entries sama) ...
        received_data = payload_with_mac.get("data")
        received_mac = payload_with_mac.get("mac")
        if received_data is None or received_mac is None:
            self.logger.error("RPC /append-entries tidak lengkap (missing data/mac)")
            return {"data": {"term": self.current_term, "success": False}, "mac": "invalid"}
        data_bytes = json.dumps(received_data, sort_keys=True).encode('utf-8')
        if not self._verify_mac(data_bytes, received_mac):
            self.logger.error("RPC /append-entries GAGAL VERIFIKASI MAC!")
            return {"data": {"term": self.current_term, "success": False}, "mac": "invalid"}
        leader_id = received_data.get("leader_id") # Ambil lagi
        leader_term = received_data.get("term")
        prev_log_index = received_data.get("prev_log_index", -1)
        prev_log_term = received_data.get("prev_log_term", 0)
        entries = received_data.get("entries", [])
        leader_commit = received_data.get("leader_commit", -1)
        success = False
        is_new_leader = False # Reset flag lokal
        if leader_term < self.current_term:
            self.logger.warning(f"Menolak AppendEntries dari {leader_id} (term kadaluwarsa {leader_term})")
        else:
            if leader_term > self.current_term or self.state == "candidate":
                await self._become_follower(leader_term)
                is_new_leader = True # Tandai leader baru
            if leader_term == self.current_term:
                self.leader_id = leader_id
                self._last_heartbeat_ts = time.time() # Reset timer
                self.logger.debug("Menerima heartbeat/RPC valid, timer di-reset.")
            if prev_log_index >= 0:
                if len(self.log) <= prev_log_index or self.log[prev_log_index]['term'] != prev_log_term:
                    self.logger.warning(f"Gagal Log Match di index {prev_log_index} (Term: {prev_log_term}). Log kita[{prev_log_index}]: {'Tidak ada' if len(self.log) <= prev_log_index else self.log[prev_log_index]['term'] }")
                    success = False
                else: success = True
            else: success = True
            if success:
                if entries:
                    conflict_found = False
                    first_new_entry_index = prev_log_index + 1
                    log_len = len(self.log)
                    for i, entry in enumerate(entries):
                        current_index = first_new_entry_index + i
                        if current_index >= log_len or self.log[current_index]['term'] != entry['term']:
                            self.logger.info(f"Konflik log terdeteksi di index {current_index}. Memotong log.")
                            self.log = self.log[:current_index]
                            self.log.extend(entries[i:])
                            conflict_found = True
                            break
                    if not conflict_found and first_new_entry_index + len(entries) > log_len:
                        new_entries_start_index = log_len - first_new_entry_index
                        if new_entries_start_index < len(entries): # Pastikan ada entri baru
                           self.log.extend(entries[new_entries_start_index:])
                    self.logger.info(f"Log sekarang memiliki {len(self.log)} entri setelah AppendEntries.")
                if leader_commit > self.commit_index:
                    new_commit_index = min(leader_commit, len(self.log) - 1)
                    if new_commit_index > self.commit_index:
                         self.commit_index = new_commit_index
                         self.logger.debug(f"Commit index diperbarui ke {self.commit_index}")
        # --- Akhir logika append ---
        response_data = {"term": self.current_term, "success": success}
        response_bytes = json.dumps(response_data, sort_keys=True).encode('utf-8')
        response_mac = self._generate_mac(response_bytes)
        self.logger.info(f"Membalas AppendEntries ke {leader_id}: success={success}, term={self.current_term}")
        return {"data": response_data, "mac": response_mac}

    async def client_request(self, command: str) -> Dict:
        """Dipanggil oleh handler /client-request."""
        client_id, lock_name, action = "unknown", "unknown", "unknown"
        try:
            parts = command.split(' ')
            action = parts[0]
            lock_name = parts[1]
            client_id = parts[2]
        except IndexError: pass
        self.logger.info(f"Client request diterima: Action={action}, Lock={lock_name}, Client={client_id}")

        if self.state != "leader":
            self.logger.warning(f"Menolak client request (bukan leader): {command}")
            return {"success": False, "message": "Bukan Leader"}

        try: # Pindahkan deadlock check ke sini
            if action.startswith("ACQUIRE"):
                if self.check_for_deadlock(client_id, lock_name):
                    self.logger.error(f"DEADLOCK DETECTED for request: {command}")
                    return {"success": False, "message": "Deadlock terdeteksi, permintaan ditolak"}
        except Exception as e:
            self.logger.error(f"Error parsing/deadlock check di client_request: {e}")
            return {"success": False, "message": "Command parse error or deadlock check failed"}

        log_entry = {'term': self.current_term, 'command': command}
        self.log.append(log_entry)
        log_index = len(self.log) - 1
        self.logger.info(f"Leader menambahkan log baru [{log_index}]: {command}")
        commit_event = asyncio.Event()
        self.commit_events[log_index] = {'event': commit_event, 'success': False}

        tasks = []
        for peer_id in self.peers.keys():
            if peer_id not in self.next_index: self.next_index[peer_id] = len(self.log)
            if peer_id not in self.match_index: self.match_index[peer_id] = -1
            tasks.append(self._replicate_log_to_peer(peer_id))

        replication_finished_event = asyncio.Event()
        async def run_replication():
            try: # Tambah try-except di task
                if tasks: await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                 self.logger.error(f"Exception during replication task: {e}", exc_info=True)
            finally: # Pastikan event di-set
                replication_finished_event.set()

        replication_task = asyncio.create_task(run_replication())

        if not self.peers: await self._check_for_commit()

        self.logger.info(f"Menunggu log [{log_index}] di-commit...")
        commit_wait_task = asyncio.create_task(commit_event.wait())
        all_tasks_to_cleanup = {commit_wait_task, replication_task}

        try:
            await asyncio.wait_for(commit_wait_task, timeout=5.0)
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout menunggu commit event untuk log [{log_index}], Command: {command}")
            if log_index in self.commit_events: del self.commit_events[log_index]
            return {"success": False, "message": "Perintah gagal di-commit (timeout menunggu event)"}
        finally:
            if not replication_task.done(): replication_task.cancel()
            await asyncio.gather(*all_tasks_to_cleanup, return_exceptions=True)

        result = self.commit_events.pop(log_index, {})
        success_status = result.get('success', False)
        if success_status:
            self.logger.info(f"Client request SUKSES (committed & applied): LogIndex={log_index}, Command: {command}")
            return {"success": True, "message": "Perintah berhasil di-commit"}
        else:
            self.logger.error(f"Client request GAGAL (setelah commit event?): LogIndex={log_index}, Command: {command}")
            return {"success": False, "message": "Perintah gagal diterapkan setelah commit"}

    def check_for_deadlock(self, client_id, lock_name):
        """Deteksi deadlock sederhana menggunakan Wait-for-Graph (WFG)."""
        self.logger.info(f"Mengecek deadlock untuk {client_id} yang meminta {lock_name}...")
        if lock_name not in self.state_machine: return False
        holders = self.state_machine[lock_name].get('owners', set())
        if not holders: return False
        wait_for_graph = {}
        for current_lock, waiting_clients in self.wait_queue.items():
            if current_lock in self.state_machine:
                holding_clients = self.state_machine[current_lock].get('owners', set())
                for waiting_client in waiting_clients:
                    if waiting_client not in wait_for_graph: wait_for_graph[waiting_client] = set()
                    wait_for_graph[waiting_client].update(holding_clients)
        if client_id not in wait_for_graph: wait_for_graph[client_id] = set()
        wait_for_graph[client_id].update(holders)
        path = set(); visited = set()
        def has_cycle(user):
            path.add(user)
            visited.add(user)
            waited_users = wait_for_graph.get(user, set())
            for waited_user in waited_users:
                if waited_user in path:
                    self.logger.error(f"!!! DEADLOCK TERDETEKSI !!! Siklus: {list(path)} -> {waited_user}")
                    return True
                if waited_user not in visited:
                    if has_cycle(waited_user): return True
            path.remove(user)
            return False
        return has_cycle(client_id)

    def status_dict(self) -> Dict:
        """Return status untuk endpoint /status (JSON-safe)."""
        json_safe_state_machine = {ln: {'mode': ld.get('mode'), 'owners': list(ld.get('owners', set()))} for ln, ld in self.state_machine.items()}
        last_heartbeat_age = round(time.time() - self._last_heartbeat_ts, 2) if self.state != 'leader' else 0.0
        return {"node_id": self.node_id, "state": self.state, "term": self.current_term, "leader_id": self.leader_id, "voted_for": self.voted_for, "peers": list(self.peers.keys()), "election_timeout": round(self.election_timeout, 2), "last_heartbeat_age": last_heartbeat_age, "log_length": len(self.log), "commit_index": self.commit_index, "last_applied": self.last_applied, "next_index": self.next_index if self.state == 'leader' else {}, "match_index": self.match_index if self.state == 'leader' else {}, "state_machine": json_safe_state_machine}

