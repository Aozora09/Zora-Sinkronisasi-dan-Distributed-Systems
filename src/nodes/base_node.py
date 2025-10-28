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
    Update 2: Menonaktifkan check_hostname client.
    Update 3: Meningkatkan timeout client_request.
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
        mac_key_str = os.getenv("SHARED_MAC_KEY", "default_insecure_key_for_raft_demo") # Kunci lebih panjang sedikit
        if len(mac_key_str) < 16:
            self.logger.warning("SHARED_MAC_KEY terlalu pendek! Harap set di .env atau environment")
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
        ca_file = os.getenv("CA_FILE") # Path ke CA cert di dalam container
        # cert_file = os.getenv("CERT_FILE") # Client cert (untuk mTLS jika server minta)
        # key_file = os.getenv("KEY_FILE")   # Client key (untuk mTLS jika server minta)
        ssl_context = None
        use_https = False # Flag apakah kita pakai HTTPS atau fallback ke HTTP

        if ca_file:
            try:
                ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_file)
                # --- PERBAIKAN: Nonaktifkan pemeriksaan hostname ---
                ssl_context.check_hostname = False
                # ---------------------------------------------------
                # Jika ingin mTLS (client juga kirim sertifikat):
                # if cert_file and key_file:
                #     ssl_context.load_cert_chain(certfile=cert_file, keyfile=key_file)
                self.logger.info(f"SSL Context client dibuat, CA: {ca_file}, Hostname Check: OFF") # Log hostname check
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
        connector = aiohttp.TCPConnector(ssl=ssl_context if use_https else False) # Explisit False jika tidak https
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
            # Gather task utama. Jangan await di sini agar start bisa kembali.
            # await asyncio.gather(self._election_task, self._heartbeat_task, self._applier_task)
            pass # Biarkan task berjalan di background
        except asyncio.CancelledError:
            pass
        # Finally block akan dijalankan saat stop() dipanggil

    async def stop(self):
        """Menghentikan semua background task dan menutup session."""
        self.logger.info(f"Stopping node {self.node_id}...")
        tasks_to_cancel = []
        if self._election_task and not self._election_task.done(): tasks_to_cancel.append(self._election_task)
        if self._heartbeat_task and not self._heartbeat_task.done(): tasks_to_cancel.append(self._heartbeat_task)
        if self._applier_task and not self._applier_task.done(): tasks_to_cancel.append(self._applier_task)

        for task in tasks_to_cancel:
            task.cancel()

        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        if self.http_session:
            await self.http_session.close() # Close session (termasuk connector)
            self.http_session = None

        # Hapus event yang mungkin masih menunggu
        for index in list(self.commit_events.keys()):
            if index in self.commit_events:
                 # Set event agar task wait_for tidak menggantung selamanya saat shutdown
                 self.commit_events[index]['event'].set()
        self.commit_events.clear()

        self.logger.info(f"Node {self.node_id} stopped.")


    # ---------------------------
    # Background Tasks (Timers)
    # ---------------------------
    async def _election_timer_loop(self):
        try:
            while True:
                await asyncio.sleep(0.15) # Cek lebih sering
                if self.state == "leader": continue

                elapsed = time.time() - self._last_heartbeat_ts
                if elapsed >= self.election_timeout:
                    self.logger.info(f"Election timeout ({elapsed:.2f}s >= {self.election_timeout:.2f}s) — start election")
                    await self._start_election()
        except asyncio.CancelledError:
            self.logger.debug("Election timer loop cancelled.")
        except Exception as e:
            self.logger.error(f"Error in election timer loop: {e}", exc_info=True)


    async def _append_entries_loop(self):
        try:
            while True:
                # Cek state dulu sebelum sleep
                if self.state == "leader":
                    self.logger.debug(f"(Leader) Mengirim AppendEntries (heartbeat) ke {list(self.peers.keys())}")
                    tasks = []
                    for peer_id in self.peers.keys():
                        if peer_id not in self.next_index: self.next_index[peer_id] = len(self.log)
                        # Jangan await di sini, kumpulkan task
                        tasks.append(self._replicate_log_to_peer(peer_id))

                    if tasks:
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        # Log hasil jika ada error
                        for peer_id, result in zip(self.peers.keys(), results):
                            if isinstance(result, Exception):
                                self.logger.debug(f"Heartbeat/Replication to {peer_id} resulted in error: {result}")
                # Sleep setelah mengirim (atau jika bukan leader)
                await asyncio.sleep(self.heartbeat_interval)
        except asyncio.CancelledError:
            self.logger.debug("Append entries loop cancelled.")
        except Exception as e:
            self.logger.error(f"Error in append entries loop: {e}", exc_info=True)

    async def _commit_log_applier(self):
        try:
            while True:
                applied_something = False
                current_commit_index = self.commit_index # Ambil snapshot
                current_last_applied = self.last_applied # Ambil snapshot

                if current_commit_index > current_last_applied:
                    start_apply_index = current_last_applied + 1
                    end_apply_index = current_commit_index + 1

                    self.logger.debug(f"Applier check: commit_index={current_commit_index}, last_applied={current_last_applied}. Applying range [{start_apply_index}, {end_apply_index}).")

                    for i in range(start_apply_index, end_apply_index):
                        if i >= len(self.log):
                            self.logger.error(f"FATAL: commit_index {current_commit_index} points beyond log length {len(self.log)} at index {i}")
                            # Mungkin perlu logic recovery atau stop di sini?
                            break # Hentikan loop apply untuk iterasi ini

                        entry = self.log[i]
                        command = entry.get('command') # Gunakan get untuk keamanan
                        if not command:
                            self.logger.error(f"Log entry at index {i} has no 'command' field: {entry}")
                            continue # Lanjut ke entry berikutnya

                        self.logger.info(f"Menerapkan log [{i}] ke state machine: {command}")
                        try:
                            cmd_parts = command.split(' ')
                            if len(cmd_parts) < 3:
                                self.logger.error(f"Invalid command format in log [{i}]: '{command}'. Skipping.")
                                continue

                            cmd_type = cmd_parts[0]
                            lock_name = cmd_parts[1]
                            client_id = cmd_parts[2]

                            # === Logika State Machine ===
                            if cmd_type == "ACQUIRE_EXCLUSIVE":
                                if lock_name in self.state_machine:
                                    # Lock sudah ada, cek pemilik
                                    current_owners = self.state_machine[lock_name].get('owners', set())
                                    current_mode = self.state_machine[lock_name].get('mode')
                                    self.logger.warning(f"Gagal ACQUIRE_EXCLUSIVE: Lock '{lock_name}' sudah diambil ({current_mode} by {list(current_owners)}). {client_id} menunggu.")
                                    # Tambahkan ke wait queue jika belum ada
                                    if lock_name not in self.wait_queue: self.wait_queue[lock_name] = set()
                                    self.wait_queue[lock_name].add(client_id)
                                else:
                                    # Lock belum ada, ambil
                                    self.state_machine[lock_name] = {'mode': 'exclusive', 'owners': {client_id}}
                                    self.logger.info(f"Lock '{lock_name}' diambil (exclusive) oleh {client_id}.")

                            elif cmd_type == "ACQUIRE_SHARED":
                                if lock_name not in self.state_machine:
                                    # Lock belum ada, ambil shared
                                    self.state_machine[lock_name] = {'mode': 'shared', 'owners': {client_id}}
                                    self.logger.info(f"Lock '{lock_name}' diambil (shared) oleh {client_id}.")
                                elif self.state_machine[lock_name]['mode'] == 'shared':
                                    # Lock sudah shared, tambahkan owner
                                    self.state_machine[lock_name]['owners'].add(client_id)
                                    self.logger.info(f"Lock '{lock_name}' ditambah (shared) oleh {client_id}. Owners: {list(self.state_machine[lock_name]['owners'])}")
                                else: # Dipegang exclusive
                                    current_owner = list(self.state_machine[lock_name].get('owners', set()))
                                    self.logger.warning(f"Gagal ACQUIRE_SHARED: Lock '{lock_name}' dipegang exclusive oleh {current_owner}. {client_id} menunggu.")
                                    # Tambahkan ke wait queue
                                    if lock_name not in self.wait_queue: self.wait_queue[lock_name] = set()
                                    self.wait_queue[lock_name].add(client_id)

                            elif cmd_type == "RELEASE":
                                if lock_name not in self.state_machine:
                                    self.logger.warning(f"Gagal RELEASE: Lock '{lock_name}' tidak ada.")
                                elif client_id not in self.state_machine[lock_name].get('owners', set()):
                                    owners = list(self.state_machine[lock_name].get('owners', set()))
                                    self.logger.warning(f"Gagal RELEASE: {client_id} tidak memegang lock '{lock_name}'. Owners saat ini: {owners}")
                                else:
                                    # Lepaskan lock
                                    self.state_machine[lock_name]['owners'].remove(client_id)
                                    remaining_owners = list(self.state_machine[lock_name]['owners'])
                                    self.logger.info(f"{client_id} melepaskan lock '{lock_name}'. Sisa owners: {remaining_owners}")

                                    # Jika tidak ada owner lagi, hapus lock & cek wait queue
                                    if not self.state_machine[lock_name]['owners']:
                                        del self.state_machine[lock_name]
                                        self.logger.info(f"Lock '{lock_name}' sekarang bebas.")
                                        # TODO: Logika untuk 'membangunkan' client dari wait_queue
                                        # Ini kompleks: siapa yang dibangunkan? Bagaimana urutannya?
                                        # Untuk demo sederhana, kita bisa abaikan ini dulu.
                                        if lock_name in self.wait_queue and self.wait_queue[lock_name]:
                                            self.logger.info(f"Wait queue untuk '{lock_name}' tidak kosong: {list(self.wait_queue[lock_name])}. Perlu logika 'wake-up'.")
                                            # Hapus saja wait queue untuk lock ini agar tidak menumpuk?
                                            # del self.wait_queue[lock_name]

                            else:
                                self.logger.error(f"Command type tidak dikenal '{cmd_type}' di log [{i}]. Skipping.")
                                continue # Lanjut ke entry berikutnya
                            # === Akhir Logika State Machine ===

                            self.logger.debug(f"State machine: {self.state_machine}")
                            self.logger.debug(f"Wait queue: {self.wait_queue}")

                        except Exception as e:
                            self.logger.error(f"Gagal menerapkan command '{command}' di log [{i}]: {e}", exc_info=True)
                            # Haruskah kita berhenti di sini? Tergantung desain fault tolerance.
                            # Untuk sekarang, kita lanjut apply entry berikutnya.

                        # Update last_applied HANYA jika apply berhasil (atau kita putuskan untuk lanjut meski error)
                        self.last_applied = i
                        applied_something = True

                        # Set event commit HANYA setelah berhasil apply
                        if i in self.commit_events:
                            try:
                                self.commit_events[i]['success'] = True
                                self.commit_events[i]['event'].set()
                                self.logger.debug(f"Commit event for index {i} set successfully.")
                            except Exception as e_set:
                                self.logger.error(f"Error setting commit event for index {i}: {e_set}")
                                # Hapus event jika gagal di-set agar tidak menyebabkan deadlock di client_request
                                if i in self.commit_events: del self.commit_events[i]
                    # Akhir loop for apply

                # Sleep hanya jika tidak ada yang di-apply di iterasi ini
                if not applied_something:
                    await asyncio.sleep(0.05) # Sleep sebentar jika tidak ada kerjaan
                # else:
                #     self.logger.debug("Applied logs, checking immediately for more.")

        except asyncio.CancelledError:
            self.logger.debug("Commit log applier loop cancelled.")
        except Exception as e:
            self.logger.error(f"Error in commit log applier loop: {e}", exc_info=True)


    # ---------------------------
    # Election Flow
    # ---------------------------
    async def _start_election(self):
        self.state = "candidate"
        self.current_term += 1
        term_of_this_election = self.current_term # Simpan term saat ini
        self.voted_for = self.node_id
        self.votes_received = {self.node_id} # Kandidat otomatis vote diri sendiri
        self._last_heartbeat_ts = time.time() # Reset timer SEGERA
        self.election_timeout = self._rand_election_timeout() # Randomize lagi

        self.logger.info(f"Becoming CANDIDATE for term={self.current_term}")

        total_nodes = len(self.peers) + 1
        majority = total_nodes // 2 + 1
        self.logger.info(f"Membutuhkan {majority} suara untuk menang (dari {total_nodes} node).")

        # Kirim RequestVote ke semua peer secara paralel
        tasks = [self._request_vote(peer_id, base_url) for peer_id, base_url in self.peers.items()]

        # Kumpulkan hasil (atau exception)
        results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []

        # --- PENTING: Cek State & Term SETELAH gather ---
        # Jika state berubah (misal jadi follower karena term lebih tinggi) atau
        # term berubah saat menunggu hasil vote, batalkan pemilu ini.
        if self.state != "candidate" or self.current_term != term_of_this_election:
            self.logger.info(f"State/Term berubah saat voting (State: {self.state}, Term: {self.current_term} vs {term_of_this_election}). Pemilu term {term_of_this_election} dibatalkan.")
            return # Keluar dari fungsi _start_election

        # Proses hasil vote (hanya jika state & term masih valid)
        for r, peer_id in zip(results, list(self.peers.keys())):
            if isinstance(r, Exception):
                self.logger.warning(f"Vote request ke {peer_id} error: {r}") # Naikkan level log
            elif r: # Jika hasilnya True (vote granted)
                self.votes_received.add(peer_id)
                self.logger.info(f"Menerima suara dari {peer_id}. Total suara: {len(self.votes_received)}")

        # Cek kemenangan (lagi, cek state & term dulu!)
        if self.state == "candidate" and self.current_term == term_of_this_election:
            self.logger.info(f"Pemilu term {term_of_this_election} selesai. Menerima total {len(self.votes_received)} suara ({self.votes_received}).")
            if len(self.votes_received) >= majority:
                await self._become_leader()
            else:
                self.logger.info(f"Kalah pemilu term {term_of_this_election}. Tetap candidate, timer akan reset/restart.")
                # Tidak perlu _become_follower di sini, biarkan timer berjalan lagi
        # else: (sudah ditangani di cek state/term di atas)
            # self.logger.info(f"State/Term berubah setelah vote diproses. Hasil pemilu term {term_of_this_election} diabaikan.")


    async def _request_vote(self, peer_id: str, base_url: str) -> bool:
        """Kirim RPC RequestVote ke peer (HTTPS or HTTP)."""
        if not self.http_session: return False
        url = f"{base_url.rstrip('/')}/vote" # base_url sudah https/http dari start()

        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]['term'] if last_log_index >= 0 else 0

        payload_data = {
            "candidate_id": self.node_id,
            "term": self.current_term,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term
        }
        payload_bytes = json.dumps(payload_data, sort_keys=True).encode('utf-8')
        payload_with_mac = {"data": payload_data, "mac": self._generate_mac(payload_bytes)}

        self.logger.debug(f"Mengirim RequestVote (term {self.current_term}) ke {url}")
        try:
            # Gunakan timeout yang lebih pendek untuk vote?
            async with self.http_session.post(url, json=payload_with_mac, timeout=2.0) as resp:
                if resp.status == 200:
                    response_with_mac = await resp.json()
                    response_data = response_with_mac.get("data")
                    response_mac = response_with_mac.get("mac")

                    if response_data is None or response_mac is None:
                        self.logger.error(f"Balasan /vote dari {peer_id} tidak lengkap (missing data/mac)")
                        return False

                    response_bytes = json.dumps(response_data, sort_keys=True).encode('utf-8')
                    if not self._verify_mac(response_bytes, response_mac):
                        self.logger.error(f"Balasan /vote dari {peer_id} GAGAL VERIFIKASI MAC!")
                        return False

                    # Proses balasan setelah MAC valid
                    peer_term = response_data.get("term", 0)
                    vote_granted = response_data.get("vote_granted", False)

                    if peer_term > self.current_term:
                        self.logger.info(f"Peer {peer_id} punya term {peer_term} > {self.current_term}. Mundur ke follower.")
                        await self._become_follower(peer_term)
                        return False # Vote tidak relevan lagi jika kita mundur

                    # Jika term sama, cek apakah vote diberikan
                    if vote_granted:
                        self.logger.debug(f"Vote DIBERIKAN oleh {peer_id} untuk term {self.current_term}")
                        return True
                    else:
                        self.logger.debug(f"Vote DITOLAK oleh {peer_id} untuk term {self.current_term} (Term peer: {peer_term})")
                        return False
                else:
                    # Handle status error non-200
                    try: body = await resp.text()
                    except Exception: body = "(cannot read body)"
                    self.logger.warning(f"Vote request ke {peer_id} gagal, status: {resp.status}, body: {body}")
                    return False

        # --- Exception Handling Lebih Spesifik ---
        except ssl.SSLError as e: self.logger.error(f"SSL Error saat request vote ke {peer_id} ({url}): {e}")
        except aiohttp.ClientConnectorCertificateError as e: self.logger.error(f"Certificate Verification Error saat request vote ke {peer_id} ({url}): {e}")
        except aiohttp.ClientConnectorError as e: self.logger.error(f"Connection Error saat request vote ke {peer_id} ({url}): {e}") # Error, bukan Warning
        except asyncio.TimeoutError: self.logger.warning(f"Timeout saat request vote ke {peer_id} ({url})")
        except aiohttp.ClientError as e: # Tangkap exception aiohttp lainnya
             self.logger.error(f"AIOHTTP Client Error saat request vote ke {peer_id} ({url}): {e}", exc_info=False) # Jangan terlalu verbose
        except Exception as e:
            self.logger.error(f"Unexpected Exception saat request vote ke {peer_id} ({url}): {e}", exc_info=True) # Log full traceback
        return False

    # ---------------------------
    # Log Replication Flow
    # ---------------------------
    async def _replicate_log_to_peer(self, peer_id: str):
        """Satu task per peer untuk mengirim AppendEntries (HTTPS or HTTP)."""
        # Cek state SEBELUM melakukan apapun
        if self.state != "leader": return
        if not self.http_session: return
        if peer_id not in self.peers: return # Peer tidak ada

        url = f"{self.peers[peer_id].rstrip('/')}/append-entries" # URL sudah https/http

        # Pastikan next_index ada untuk peer ini
        if peer_id not in self.next_index:
             self.logger.warning(f"next_index for peer {peer_id} not initialized. Setting to log length {len(self.log)}")
             self.next_index[peer_id] = len(self.log)

        next_idx = self.next_index[peer_id]
        prev_log_index = next_idx - 1
        prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 and prev_log_index < len(self.log) else 0

        # Ambil entries HANYA jika next_idx valid
        entries = self.log[next_idx:] if next_idx <= len(self.log) else []
        if next_idx > len(self.log):
             self.logger.warning(f"next_index[{peer_id}] ({next_idx}) is beyond log length ({len(self.log)}). Sending heartbeat only.")

        payload_data = {
            "leader_id": self.node_id,
            "term": self.current_term,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": entries,
            "leader_commit": self.commit_index
        }
        payload_bytes = json.dumps(payload_data, sort_keys=True).encode('utf-8')
        payload_with_mac = {"data": payload_data, "mac": self._generate_mac(payload_bytes)}

        log_msg_detail = f"prevIdx={prev_log_index}, prevTerm={prev_log_term}, #entries={len(entries)}, ldrCommit={self.commit_index}"
        self.logger.debug(f"Mengirim AppendEntries ({log_msg_detail}) ke {url}")

        try:
            # Gunakan timeout yang wajar
            async with self.http_session.post(url, json=payload_with_mac, timeout=2.0) as resp:
                # --- Cek state LAGI setelah await ---
                if self.state != "leader":
                    self.logger.info(f"State changed while sending AppendEntries to {peer_id}. Aborting.")
                    return

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
                    success = response_data.get("success", False)

                    if peer_term > self.current_term:
                        self.logger.info(f"Peer {peer_id} punya term {peer_term} > {self.current_term} via AppendEntries reply. Mundur.")
                        await self._become_follower(peer_term)
                        return # Berhenti memproses balasan ini

                    # Hanya proses jika term masih sama dan kita masih leader
                    if self.state == "leader" and peer_term == self.current_term:
                        if success:
                            # Replikasi berhasil, update nextIndex dan matchIndex
                            new_next_index = next_idx + len(entries) # next_idx sebelumnya
                            new_match_index = new_next_index - 1
                            # Update hanya jika benar-benar bertambah (mencegah mundur jika ada reordering)
                            self.next_index[peer_id] = max(self.next_index.get(peer_id, 0), new_next_index)
                            self.match_index[peer_id] = max(self.match_index.get(peer_id, -1), new_match_index)

                            self.logger.debug(f"Replikasi ke {peer_id} SUKSES. nextIndex={self.next_index[peer_id]}, matchIndex={self.match_index[peer_id]}")
                            # Panggil _check_for_commit SETELAH matchIndex diupdate
                            await self._check_for_commit()
                        else:
                            # Replikasi gagal (kemungkinan log mismatch)
                            self.logger.warning(f"Replikasi ke {peer_id} GAGAL (Term:{peer_term}). Mundurkan nextIndex.")
                            # Mundurkan nextIndex secara agresif? Atau satu per satu?
                            # Strategi sederhana: mundur satu, coba lagi di heartbeat berikutnya
                            self.next_index[peer_id] = max(0, self.next_index.get(peer_id, 1) - 1)
                            # Pertimbangkan: jika prev_log_index terlalu jauh ke belakang, peer mungkin butuh snapshot.
                else:
                    # Handle status error non-200
                    try: body = await resp.text()
                    except Exception: body = "(cannot read body)"
                    self.logger.warning(f"AppendEntries ke {peer_id} gagal, status: {resp.status}, body: {body}")
                    # Mungkin perlu menandai peer ini sebagai 'unreachable' sementara?

        # --- Exception Handling Lebih Spesifik ---
        except ssl.SSLError as e: self.logger.error(f"SSL Error saat append entries ke {peer_id} ({url}): {e}")
        except aiohttp.ClientConnectorCertificateError as e: self.logger.error(f"Certificate Verification Error saat append entries ke {peer_id} ({url}): {e}")
        except aiohttp.ClientConnectorError as e: self.logger.error(f"Connection Error saat append entries ke {peer_id} ({url}): {e}") # Error
        except asyncio.TimeoutError: self.logger.warning(f"Timeout saat append entries ke {peer_id} ({url})")
        except aiohttp.ClientError as e: # Tangkap exception aiohttp lainnya
             self.logger.error(f"AIOHTTP Client Error saat append entries ke {peer_id} ({url}): {e}", exc_info=False)
        except Exception as e:
            self.logger.error(f"Unexpected Exception saat append entries ke {peer_id} ({url}): {e}", exc_info=True)


    async def _check_for_commit(self):
        """Cek apakah ada log baru yang bisa di-commit."""
        # Hanya leader yang bisa commit
        if self.state != "leader": return

        total_nodes = len(self.peers) + 1
        majority = total_nodes // 2 + 1

        # Kandidat commit potensial: dari commit_index + 1 sampai akhir log
        # Kita iterasi dari belakang log ke depan (lebih efisien)
        potential_commit_index = -1 # Default jika tidak ada yang bisa di-commit
        log_len = len(self.log)

        for n in range(log_len - 1, self.commit_index, -1):
            # Hanya commit log dari term saat ini (aturan Raft)
            if self.log[n]['term'] == self.current_term:
                # Hitung berapa banyak node (termasuk diri sendiri) yang sudah mereplikasi log index n
                count = 1 + sum(1 for pid in self.peers if self.match_index.get(pid, -1) >= n)

                if count >= majority:
                    # Ditemukan index tertinggi yang sudah direplikasi mayoritas
                    potential_commit_index = n
                    break # Kita hanya perlu yang tertinggi

        # Jika ditemukan index baru yang bisa di-commit
        if potential_commit_index > self.commit_index:
            old_commit_index = self.commit_index
            self.commit_index = potential_commit_index
            self.logger.info(f"🎉 Commit index diperbarui dari {old_commit_index} ke {self.commit_index}.")
            # Applier task akan mengambil perubahan ini di loop berikutnya.
        # else:
            # self.logger.debug(f"Check commit: Tidak ada index baru untuk di-commit (current={self.commit_index}). Match indices: {self.match_index}")

    # ---------------------------
    # State Transitions Helpers
    # ---------------------------
    async def _become_leader(self):
        # Pastikan kita masih candidate dari term yang benar sebelum jadi leader
        # (Meskipun sudah dicek di _start_election, double check)
        if self.state != "candidate":
            self.logger.warning(f"Attempted to become leader, but state is {self.state}. Aborting.")
            return

        self.state = "leader"
        self.leader_id = self.node_id
        self.logger.info(f"🏆 Node {self.node_id} WON election and is now LEADER (term={self.current_term})")

        # Inisialisasi nextIndex dan matchIndex untuk semua peer
        log_len = len(self.log)
        for peer_id in self.peers:
            self.next_index[peer_id] = log_len
            self.match_index[peer_id] = -1 # Awalnya belum ada yang cocok

        self.logger.info("(Leader) Mengirim heartbeat pertama...")
        # Kirim AppendEntries (heartbeat) SEGERA untuk menegaskan kepemimpinan
        # Gunakan create_task agar tidak memblok transisi state
        tasks = [asyncio.create_task(self._replicate_log_to_peer(peer_id)) for peer_id in self.peers.keys()]
        # Tidak perlu await gather di sini, biarkan berjalan di background
        # Jika ingin memastikan heartbeat pertama terkirim, bisa gather tapi mungkin lambat
        # if tasks: await asyncio.gather(*tasks, return_exceptions=True)

    async def _become_follower(self, new_term: int):
        old_state = self.state
        old_term = self.current_term
        self.state = "follower"
        # Update term HANYA jika new_term > current_term
        if new_term > self.current_term:
             self.current_term = new_term
             self.voted_for = None # Reset vote jika term baru
             self.leader_id = None # Lupakan leader lama jika term baru
             term_change_msg = f"Term diperbarui ke {self.current_term}."
        else:
            # Jika new_term sama atau lebih kecil (jarang terjadi, tapi defensif)
            # Jangan update term, jangan reset voted_for
             term_change_msg = f"Term tetap {self.current_term}."
             # Jika kita mundur dari leader ke follower di term yang sama?
             if old_state == "leader": self.leader_id = None

        self._last_heartbeat_ts = time.time() # Reset timer
        self.election_timeout = self._rand_election_timeout() # Randomize lagi

        self.logger.info(f"Node {self.node_id}: {old_state}({old_term}) -> follower. {term_change_msg} New election_timeout={self.election_timeout:.2f}s")


    # ---------------------------
    # Public RPC Handlers
    # ---------------------------
    async def handle_vote_rpc(self, payload_with_mac: Dict) -> Dict:
        """Logika untuk membalas RPC /vote (DENGAN VERIFIKASI MAC)."""
        candidate_id_log = payload_with_mac.get("data", {}).get("candidate_id", "unknown")
        candidate_term_log = payload_with_mac.get("data", {}).get("term", "unknown")
        self.logger.info(f"Menerima RequestVote RPC dari {candidate_id_log} (term: {candidate_term_log})")

        # --- Verifikasi MAC ---
        received_data = payload_with_mac.get("data")
        received_mac = payload_with_mac.get("mac")
        if received_data is None or received_mac is None:
            self.logger.error("RPC /vote tidak lengkap (missing data/mac)")
            # Tetap balas dengan MAC valid agar client tidak bingung
            resp_data = {"term": self.current_term, "vote_granted": False}
            resp_bytes = json.dumps(resp_data, sort_keys=True).encode('utf-8')
            return {"data": resp_data, "mac": self._generate_mac(resp_bytes)}

        data_bytes = json.dumps(received_data, sort_keys=True).encode('utf-8')
        if not self._verify_mac(data_bytes, received_mac):
            self.logger.error("RPC /vote GAGAL VERIFIKASI MAC!")
            # Balas dengan MAC invalid agar client tahu ada masalah? Atau MAC valid?
            # Konsisten dengan AppendEntries, balas dengan MAC valid tapi vote ditolak.
            resp_data = {"term": self.current_term, "vote_granted": False}
            resp_bytes = json.dumps(resp_data, sort_keys=True).encode('utf-8')
            return {"data": resp_data, "mac": self._generate_mac(resp_bytes)}

        # --- Logika Voting (Setelah MAC valid) ---
        candidate_id = received_data.get("candidate_id")
        candidate_term = received_data.get("term")
        candidate_log_index = received_data.get("last_log_index", -1)
        candidate_log_term = received_data.get("last_log_term", 0)
        vote_granted = False

        # 1. Reply false if term < currentTerm (§5.1)
        if candidate_term < self.current_term:
            self.logger.debug(f"Menolak vote {candidate_id} (term kadaluwarsa {candidate_term} < {self.current_term})")
            vote_granted = False
        # Jika term kandidat > term kita, kita jadi follower DULU
        elif candidate_term > self.current_term:
            self.logger.info(f"Menerima term {candidate_term} > {self.current_term} dari vote {candidate_id}. Mundur.")
            await self._become_follower(candidate_term)
            # Setelah mundur, term kita = candidate_term. Lanjut ke cek vote.

        # Jika term sama (atau kita baru saja mundur ke term ini)
        if candidate_term == self.current_term:
            # Cek votedFor (§5.2, §5.4)
            # Kondisi: (belum vote ATAU sudah vote kandidat yang sama) DAN log kandidat up-to-date
            can_vote_for_candidate = (self.voted_for is None or self.voted_for == candidate_id)

            if can_vote_for_candidate:
                # Cek log up-to-date (§5.4.1)
                our_last_log_index = len(self.log) - 1
                our_last_log_term = self.log[our_last_log_index]['term'] if our_last_log_index >= 0 else 0

                # Logika up-to-date: (term kandidat lebih baru) ATAU (term sama TAPI index kandidat >= index kita)
                candidate_is_up_to_date = (
                    candidate_log_term > our_last_log_term or
                    (candidate_log_term == our_last_log_term and candidate_log_index >= our_last_log_index)
                )

                if candidate_is_up_to_date:
                    self.logger.info(f"Memberi suara untuk {candidate_id} (Term {self.current_term})")
                    vote_granted = True
                    self.voted_for = candidate_id
                    self._last_heartbeat_ts = time.time() # Reset election timer KARENA kita memberi vote
                else:
                    self.logger.warning(f"Menolak vote {candidate_id} (log-nya ketinggalan: cand=[{candidate_log_term},{candidate_log_index}] vs our=[{our_last_log_term},{our_last_log_index}])")
                    vote_granted = False
            else:
                self.logger.warning(f"Menolak vote {candidate_id} (sudah vote untuk {self.voted_for} di term {self.current_term})")
                vote_granted = False

        # --- Buat Respons ---
        response_data = {"term": self.current_term, "vote_granted": vote_granted}
        response_bytes = json.dumps(response_data, sort_keys=True).encode('utf-8')
        response_mac = self._generate_mac(response_bytes)
        self.logger.info(f"Membalas RequestVote ke {candidate_id}: granted={vote_granted}, term={self.current_term}")
        return {"data": response_data, "mac": response_mac}


    async def handle_append_entries_rpc(self, payload_with_mac: Dict) -> Dict:
        """Logika untuk membalas RPC /append-entries (DENGAN VERIFIKASI MAC)."""
        # Log awal (sebelum verifikasi MAC)
        leader_id_log = payload_with_mac.get("data", {}).get("leader_id", "unknown")
        leader_term_log = payload_with_mac.get("data", {}).get("term", "unknown")
        prev_idx_log = payload_with_mac.get("data", {}).get("prev_log_index", "-")
        num_entries_log = len(payload_with_mac.get("data", {}).get("entries", []))
        log_detail = f"term: {leader_term_log}, prevIdx: {prev_idx_log}, #entries: {num_entries_log}"
        self.logger.info(f"Menerima AppendEntries RPC dari {leader_id_log} ({log_detail})")

        # --- Verifikasi MAC ---
        received_data = payload_with_mac.get("data")
        received_mac = payload_with_mac.get("mac")
        if received_data is None or received_mac is None:
            self.logger.error("RPC /append-entries tidak lengkap (missing data/mac)")
            resp_data = {"term": self.current_term, "success": False}
            resp_bytes = json.dumps(resp_data, sort_keys=True).encode('utf-8')
            return {"data": resp_data, "mac": self._generate_mac(resp_bytes)}

        data_bytes = json.dumps(received_data, sort_keys=True).encode('utf-8')
        if not self._verify_mac(data_bytes, received_mac):
            self.logger.error("RPC /append-entries GAGAL VERIFIKASI MAC!")
            resp_data = {"term": self.current_term, "success": False}
            resp_bytes = json.dumps(resp_data, sort_keys=True).encode('utf-8')
            return {"data": resp_data, "mac": self._generate_mac(resp_bytes)}

        # --- Logika AppendEntries (Setelah MAC valid) ---
        leader_id = received_data.get("leader_id")
        leader_term = received_data.get("term")
        prev_log_index = received_data.get("prev_log_index", -1)
        prev_log_term = received_data.get("prev_log_term", 0)
        entries = received_data.get("entries", [])
        leader_commit = received_data.get("leader_commit", -1)
        success = False

        # 1. Reply false if term < currentTerm (§5.1)
        if leader_term < self.current_term:
            self.logger.warning(f"Menolak AppendEntries dari {leader_id} (term kadaluwarsa {leader_term} < {self.current_term})")
            success = False
        else:
            # Jika term leader >= term kita:
            # - Reset election timer
            # - Jika kita candidate/leader, mundur jadi follower
            # - Update term jika term leader lebih baru
            self._last_heartbeat_ts = time.time() # Reset timer KARENA pesan valid dari leader term >= kita
            self.logger.debug(f"Heartbeat/RPC valid dari {leader_id} diterima, timer di-reset.")

            if leader_term > self.current_term:
                self.logger.info(f"Menerima term {leader_term} > {self.current_term} dari AppendEntries {leader_id}. Mundur.")
                await self._become_follower(leader_term)
            elif self.state == "candidate":
                self.logger.info(f"Menerima AppendEntries dari leader {leader_id} di term {leader_term} saat candidate. Mundur.")
                await self._become_follower(leader_term) # Mundur ke follower di term yang sama
            elif self.state == "leader" and leader_term == self.current_term:
                 # Sangat jarang: dua leader di term sama? Mundur saja.
                 self.logger.warning(f"Menerima AppendEntries dari leader {leader_id} di term {leader_term} saat SAYA JUGA leader? Mundur.")
                 await self._become_follower(leader_term)


            # Setelah memastikan kita follower di term yang benar:
            self.leader_id = leader_id # Catat siapa leadernya

            # 2. Reply false if log doesn’t contain an entry at prevLogIndex
            #    whose term matches prevLogTerm (§5.3)
            log_match = False
            if prev_log_index == -1: # Kasus awal (log kosong)
                log_match = True
            elif prev_log_index < len(self.log) and self.log[prev_log_index]['term'] == prev_log_term:
                log_match = True

            if not log_match:
                log_term_at_prev = self.log[prev_log_index]['term'] if prev_log_index < len(self.log) else 'None'
                self.logger.warning(f"Gagal Log Match di index {prev_log_index}. Leader term: {prev_log_term}, Log kita: {log_term_at_prev} (Log len: {len(self.log)})")
                success = False
            else:
                # Log cocok sampai prevLogIndex
                success = True

                # 3. If an existing entry conflicts with a new one (same index
                #    but different terms), delete the existing entry and all that
                #    follow it (§5.3)
                # 4. Append any new entries not already in the log
                if entries:
                    log_len = len(self.log)
                    for i, entry in enumerate(entries):
                        current_index = prev_log_index + 1 + i
                        if current_index >= log_len:
                            # Semua entri setelah ini adalah baru, append saja
                            self.log.extend(entries[i:])
                            self.logger.info(f"Menambahkan {len(entries[i:])} entri baru mulai dari index {current_index}. Log len sekarang: {len(self.log)}")
                            break # Selesai append
                        elif self.log[current_index]['term'] != entry['term']:
                            # Konflik ditemukan! Hapus entry ini dan semua setelahnya
                            self.logger.warning(f"Konflik log terdeteksi di index {current_index} (Term kita: {self.log[current_index]['term']} vs Leader: {entry['term']}). Memotong log.")
                            self.log = self.log[:current_index]
                            # Append sisa entri dari leader
                            self.log.extend(entries[i:])
                            self.logger.info(f"Setelah potong & append, log len sekarang: {len(self.log)}")
                            break # Selesai append
                        # else: Entry sama, lanjut ke berikutnya

                # 5. If leaderCommit > commitIndex, set commitIndex =
                # min(leaderCommit, index of last new entry)
                if leader_commit > self.commit_index:
                    # Index of last new entry (jika ada) adalah len(self.log) - 1
                    # Jika tidak ada entry baru, index of last new entry tetap sama seperti sebelumnya
                    last_entry_index = len(self.log) - 1
                    new_commit_index = min(leader_commit, last_entry_index)

                    if new_commit_index > self.commit_index:
                        old_commit_index = self.commit_index
                        self.commit_index = new_commit_index
                        self.logger.info(f"Commit index diperbarui dari {old_commit_index} ke {self.commit_index} via leader ({leader_id}) commit {leader_commit}")
                        # Applier task akan mengambil ini

        # --- Buat Respons ---
        response_data = {"term": self.current_term, "success": success}
        response_bytes = json.dumps(response_data, sort_keys=True).encode('utf-8')
        response_mac = self._generate_mac(response_bytes)
        self.logger.info(f"Membalas AppendEntries ke {leader_id}: success={success}, term={self.current_term}")
        return {"data": response_data, "mac": response_mac}


    async def client_request(self, command: str) -> Dict:
        """Dipanggil oleh handler /client-request."""
        # Log awal
        client_id, lock_name, action = "unknown", "unknown", "unknown"
        try:
            parts = command.split(' ')
            action = parts[0]
            lock_name = parts[1]
            client_id = parts[2]
        except IndexError: pass
        self.logger.info(f"Client request diterima: Action={action}, Lock={lock_name}, Client={client_id}")

        # Hanya leader yang boleh proses
        if self.state != "leader":
            self.logger.warning(f"Menolak client request (bukan leader): {command}")
            # Berikan hint URL leader (sudah https/http dari init node)
            leader_url_hint = self.peers.get(self.leader_id) if self.leader_id in self.peers else None
            if not leader_url_hint and self.leader_id == self.node_id: # Kasus N=1?
                protocol = "https://" if isinstance(self.http_session.connector.ssl, ssl.SSLContext) else "http://"
                leader_url_hint = f"{protocol}{self.host}:{self.port}" # Jarang terjadi
            return {"success": False, "message": "Bukan Leader", "leader_id": self.leader_id, "leader_hint": leader_url_hint}


        # --- Periksa Deadlock (SEBELUM menambahkan ke log) ---
        # TODO: Perlu penyesuaian logika deadlock jika state machine dipisah
        try:
            if action.startswith("ACQUIRE"):
                if self.check_for_deadlock(client_id, lock_name):
                    self.logger.error(f"DEADLOCK DETECTED for request: {command}")
                    return {"success": False, "message": "Deadlock terdeteksi, permintaan ditolak"}
        except Exception as e:
            self.logger.error(f"Error parsing/deadlock check di client_request: {e}")
            return {"success": False, "message": "Command parse error or deadlock check failed"}

        # --- Tambahkan ke Log & Siapkan Event ---
        log_entry = {'term': self.current_term, 'command': command}
        self.log.append(log_entry)
        log_index = len(self.log) - 1
        self.logger.info(f"Leader menambahkan log baru [{log_index}]: {command}")

        # Buat event untuk menunggu commit & apply
        commit_event = asyncio.Event()
        self.commit_events[log_index] = {'event': commit_event, 'success': False}

        # --- Trigger Replikasi (Tidak perlu await di sini) ---
        # Panggil replicate untuk SEMUA peer (termasuk yang mungkin lambat/offline)
        replication_tasks = []
        if self.peers:
             self.logger.debug(f"Memicu replikasi untuk log [{log_index}] ke {list(self.peers.keys())}")
             for peer_id in self.peers.keys():
                  # Buat task agar tidak memblok
                  replication_tasks.append(asyncio.create_task(self._replicate_log_to_peer(peer_id)))
        else:
             # Kasus N=1: Langsung cek commit
             self.logger.debug("N=1, langsung cek commit setelah append log.")
             await self._check_for_commit() # Commit index akan naik

        # --- Tunggu Commit & Apply (dengan Timeout) ---
        self.logger.info(f"Menunggu log [{log_index}] di-commit dan di-apply...")
        commit_wait_task = asyncio.create_task(commit_event.wait())

        # Kumpulkan semua task yang perlu dibersihkan
        all_tasks_to_cleanup = {commit_wait_task}.union(set(replication_tasks))

        try:
            # ----- PERBAIKAN: Tingkatkan Timeout -----
            await asyncio.wait_for(commit_wait_task, timeout=20.0)
            # ---------------------------------------

            # Jika wait_for berhasil (event di-set oleh applier)
            result_info = self.commit_events.get(log_index, {})
            success_status = result_info.get('success', False)

            if success_status:
                self.logger.info(f"Client request SUKSES (committed & applied): LogIndex={log_index}, Command: {command}")
                return {"success": True, "message": "Perintah berhasil di-commit"}
            else:
                # Jarang terjadi: event di-set tapi success=False? (Mungkin error di applier?)
                self.logger.error(f"Client request GAGAL ANEH (event set tapi !success): LogIndex={log_index}, Command: {command}")
                return {"success": False, "message": "Perintah gagal diterapkan setelah commit (internal error)"}

        except asyncio.TimeoutError:
            self.logger.error(f"Timeout ({20.0}s) menunggu commit/apply event untuk log [{log_index}], Command: {command}")
            # Event tidak di-set, perintah belum selesai diterapkan
            return {"success": False, "message": f"Perintah gagal di-commit/apply dalam {20.0}s (timeout)"}
        except asyncio.CancelledError:
             self.logger.warning(f"Client request wait for log {log_index} cancelled.")
             return {"success": False, "message": "Request dibatalkan"}
        except Exception as e:
            self.logger.error(f"Error tak terduga saat menunggu commit event log {log_index}: {e}", exc_info=True)
            return {"success": False, "message": f"Error internal saat menunggu commit: {e}"}

        finally:
            # --- Cleanup ---
            # Hapus event dari dictionary
            if log_index in self.commit_events:
                del self.commit_events[log_index]

            # Batalkan task replikasi yang mungkin masih berjalan
            for task in replication_tasks:
                 if not task.done():
                     task.cancel()
            # Tunggu semua task (wait & replikasi) selesai/dibatalkan
            if all_tasks_to_cleanup:
                 await asyncio.gather(*all_tasks_to_cleanup, return_exceptions=True)
            self.logger.debug(f"Cleanup selesai untuk client request log {log_index}")


    def check_for_deadlock(self, client_id, lock_name):
        """Deteksi deadlock sederhana menggunakan Wait-for-Graph (WFG)."""
        self.logger.info(f"Mengecek deadlock untuk {client_id} yang meminta {lock_name}...")

        # Jika lock tidak ada, tidak mungkin deadlock karena request ini
        if lock_name not in self.state_machine:
            self.logger.debug(f"Deadlock check: Lock '{lock_name}' tidak ada, aman.")
            return False

        # Siapa yang memegang lock yang diminta client_id?
        holders = self.state_machine[lock_name].get('owners', set())
        if not holders:
            # Seharusnya tidak terjadi jika lock ada di state_machine, tapi cek saja
            self.logger.debug(f"Deadlock check: Lock '{lock_name}' ada tapi tidak ada owner? Aman.")
            return False

        # Jika client_id sudah memegang lock (misal: reentrant shared lock), bukan deadlock
        if client_id in holders:
             # Perlu cek mode? Jika exclusive minta exclusive lagi -> error, tapi bukan deadlock
             # Jika shared minta shared -> OK
             # Jika shared minta exclusive -> mungkin deadlock
             current_mode = self.state_machine[lock_name].get('mode')
             num_holders = len(holders)
             requested_mode_is_exclusive = True # Asumsi dari `action.startswith("ACQUIRE")`

             if current_mode == 'shared' and requested_mode_is_exclusive and num_holders > 1:
                  # Client pegang shared, minta exclusive, TAPI ada shared holder lain -> potensial deadlock
                  pass # Lanjut cek WFG
             elif current_mode == 'shared' and not requested_mode_is_exclusive:
                  self.logger.debug(f"Deadlock check: {client_id} sudah pegang shared '{lock_name}', minta shared lagi. Aman.")
                  return False
             else: # Kasus lain (exclusive minta exclusive?) anggap aman dari deadlock, mungkin error logic lain
                  self.logger.debug(f"Deadlock check: {client_id} sudah pegang '{lock_name}'. Bukan deadlock.")
                  return False


        # --- Bangun Wait-for-Graph (WFG) ---
        # Key: client yang menunggu, Value: set client yang dipegang lock-nya
        wait_for_graph: Dict[str, set] = {}

        # 1. Tambahkan edge dari client saat ini ke pemegang lock yang diminta
        wait_for_graph[client_id] = holders.copy()
        self.logger.debug(f"WFG Init: {client_id} waits for {list(holders)}")

        # 2. Iterasi semua lock di wait_queue untuk edge lainnya
        for waiting_lock, waiting_clients in self.wait_queue.items():
            # Siapa yang memegang waiting_lock ini?
            if waiting_lock in self.state_machine:
                holding_clients = self.state_machine[waiting_lock].get('owners', set())
                if holding_clients:
                    for waiting_client in waiting_clients:
                        # Tambahkan edge: waiting_client -> holding_clients
                        if waiting_client not in wait_for_graph:
                            wait_for_graph[waiting_client] = set()
                        # Jangan tambahkan edge ke diri sendiri
                        wait_for_graph[waiting_client].update(hc for hc in holding_clients if hc != waiting_client)
                        if wait_for_graph[waiting_client]: # Log jika ada edge baru
                             self.logger.debug(f"WFG Edge: {waiting_client} waits for {list(wait_for_graph[waiting_client])} (via lock '{waiting_lock}')")

        # --- Deteksi Siklus (Cycle Detection using DFS) ---
        path = set() # Nodes currently in the recursion stack
        visited = set() # Nodes that have been fully explored

        def has_cycle(user):
            path.add(user)
            visited.add(user)
            self.logger.debug(f"DFS Cycle Check: Visiting {user}, Path: {list(path)}")

            waited_users = wait_for_graph.get(user, set())
            for waited_user in waited_users:
                if waited_user in path:
                    # Siklus terdeteksi!
                    self.logger.error(f"!!! DEADLOCK TERDETEKSI !!! Siklus: {' -> '.join(list(path))} -> {waited_user}")
                    return True
                if waited_user not in visited:
                    if has_cycle(waited_user):
                        return True # Propagate cycle found deeper

            path.remove(user) # Backtrack
            self.logger.debug(f"DFS Cycle Check: Finished {user}")
            return False

        # Mulai deteksi siklus dari client yang meminta lock
        if has_cycle(client_id):
            return True
        else:
            self.logger.info(f"Deadlock check untuk {client_id} meminta '{lock_name}': Tidak ada siklus terdeteksi.")
            return False


    def status_dict(self) -> Dict:
        """Return status untuk endpoint /status (JSON-safe)."""
        # Buat salinan state machine yang aman untuk JSON
        json_safe_state_machine = {}
        for lock_name, lock_data in self.state_machine.items():
            json_safe_state_machine[lock_name] = {
                'mode': lock_data.get('mode'),
                'owners': sorted(list(lock_data.get('owners', set()))) # Urutkan agar konsisten
            }

        last_heartbeat_age = round(time.time() - self._last_heartbeat_ts, 2) if self.state != 'leader' else 0.0

        # Salinan next/match index (hanya relevan jika leader)
        next_idx = self.next_index.copy() if self.state == 'leader' else {}
        match_idx = self.match_index.copy() if self.state == 'leader' else {}

        return {
            "node_id": self.node_id,
            "state": self.state,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "voted_for": self.voted_for,
            "peers": sorted(list(self.peers.keys())), # Urutkan
            "election_timeout_setting": f"{self._election_min}-{self._election_max}s",
            "current_election_timeout": round(self.election_timeout, 2),
            "last_leader_contact_ago": f"{last_heartbeat_age}s",
            "log_length": len(self.log),
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "leader_state": { # Info khusus leader
                 "next_index": next_idx,
                 "match_index": match_idx
            } if self.state == 'leader' else None,
            "state_machine": json_safe_state_machine,
            "wait_queue": {lock: sorted(list(clients)) for lock, clients in self.wait_queue.items()} # Urutkan
        }

