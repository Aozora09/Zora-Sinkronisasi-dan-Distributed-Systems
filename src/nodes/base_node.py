# src/nodes/base_node.py

import asyncio

import hashlib

import hmac

import time

import random

import logging

from typing import Dict, Optional, List, Any

import json # Untuk serialisasi payload

import os



import aiohttp

from aiohttp_retry import RetryClient, ExponentialRetry



# Konfigurasi logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")





class RaftNode:

    """

    Implementasi Raft Node dengan MAC untuk otentikasi (persiapan PBFT).

    """



    def __init__(self, node_id: str, peers: Dict[str, str], host: str = "0.0.0.0", port: int = 8000):

        self.node_id = node_id

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

        """Menghasilkan MAC (HMAC-SHA256) untuk pesan."""

        return hmac.new(self.mac_key, message_bytes, hashlib.sha256).hexdigest()



    def _verify_mac(self, message_bytes: bytes, received_mac: str) -> bool:

        """Memverifikasi MAC yang diterima."""

        if not received_mac: # Handle jika MAC tidak ada

             return False

        expected_mac = self._generate_mac(message_bytes)

        return hmac.compare_digest(expected_mac, received_mac)

    # --- AKHIR HELPER MAC ---



    # ---------------------------

    # Lifecycle: start / stop

    # ---------------------------

    async def start(self):

        """Memulai semua background task dan HTTP client."""

        retry_options = ExponentialRetry(attempts=5, start_timeout=0.5, max_timeout=10)

        self.http_session = RetryClient(aiohttp.ClientSession(), retry_options=retry_options)

       

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

                await self.http_session.close()

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

            if self.state == "leader":

                continue

           

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

                    tasks.append(self._replicate_log_to_peer(peer_id))

                await asyncio.gather(*tasks, return_exceptions=True)



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



                        if cmd_type == "ACQUIRE_EXCLUSIVE":

                            if lock_name in self.state_machine:

                                self.logger.warning(f"Gagal ACQUIRE_EXCLUSIVE: Lock '{lock_name}' sudah diambil. {client_id} menunggu.")

                                if lock_name not in self.wait_queue:

                                    self.wait_queue[lock_name] = set()

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

                                if lock_name not in self.wait_queue:

                                    self.wait_queue[lock_name] = set()

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



        # Mayoritas dihitung dari jumlah total node (peers + diri sendiri)

        total_nodes = len(self.peers) + 1

        majority = total_nodes // 2 + 1

        self.logger.info(f"Membutuhkan {majority} suara untuk menang (dari {total_nodes} node).")



        tasks = []

        for peer_id, base_url in self.peers.items():

            tasks.append(self._request_vote(peer_id, base_url))

       

        results = await asyncio.gather(*tasks, return_exceptions=True)

       

        if self.state != "candidate" or self.current_term != term_of_this_election:

            self.logger.info(f"Term berubah saat voting (dari {term_of_this_election} ke {self.current_term}). Pemilu dibatalkan.")

            return



        for r, (peer_id) in zip(results, list(self.peers.keys())):

            if isinstance(r, Exception):

                self.logger.debug(f"Vote request ke {peer_id} error: {r}")

            elif r: # r adalah True (vote granted)

                self.votes_received.add(peer_id)



        self.logger.info(f"Pemilu selesai. Menerima total {len(self.votes_received)} suara.")



        if len(self.votes_received) >= majority:

            await self._become_leader()

        else:

            self.logger.info("Kalah pemilu.")



    async def _request_vote(self, peer_id: str, base_url: str) -> bool:

        """Kirim RPC RequestVote ke peer (DENGAN MAC)."""

        if not self.http_session: return False

       

        last_log_index = len(self.log) - 1

        last_log_term = self.log[last_log_index]['term'] if last_log_index >= 0 else 0

       

        payload_data = {

            "candidate_id": self.node_id,

            "term": self.current_term,

            "last_log_index": last_log_index,

            "last_log_term": last_log_term

        }

        payload_bytes = json.dumps(payload_data, sort_keys=True).encode('utf-8')

       

        payload_with_mac = {

            "data": payload_data,

            "mac": self._generate_mac(payload_bytes)

        }

       

        url = f"{base_url.rstrip('/')}/vote"

        try:

            async with self.http_session.post(url, json=payload_with_mac, timeout=2.0) as resp:

                if resp.status == 200:

                    response_with_mac = await resp.json()

                    # --- Verifikasi MAC Balasan ---

                    response_data = response_with_mac.get("data")

                    response_mac = response_with_mac.get("mac")

                    if response_data is None or response_mac is None:

                         self.logger.error(f"Balasan /vote dari {peer_id} tidak lengkap")

                         return False

                    response_bytes = json.dumps(response_data, sort_keys=True).encode('utf-8')

                    if not self._verify_mac(response_bytes, response_mac):

                         self.logger.error(f"Balasan /vote dari {peer_id} GAGAL VERIFIKASI MAC!")

                         return False

                    # --- Akhir Verifikasi ---

                   

                    if response_data.get("term", 0) > self.current_term:

                        await self._become_follower(response_data.get("term"))

                    return response_data.get("vote_granted", False)

        except Exception as e:

            self.logger.debug(f"Vote request ke {peer_id} failed: {e}")

        return False



    # ---------------------------

    # Log Replication Flow

    # ---------------------------

    async def _replicate_log_to_peer(self, peer_id: str):

        """Satu task per peer untuk mengirim AppendEntries (DENGAN MAC)."""

        if not self.http_session or self.state != "leader":

            return



        next_index = self.next_index[peer_id]

        prev_log_index = next_index - 1

        prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0

        entries = self.log[next_index:]



        payload_data = {

            "leader_id": self.node_id,

            "term": self.current_term,

            "prev_log_index": prev_log_index,

            "prev_log_term": prev_log_term,

            "entries": entries,

            "leader_commit": self.commit_index

        }

        payload_bytes = json.dumps(payload_data, sort_keys=True).encode('utf-8')

       

        payload_with_mac = {

            "data": payload_data,

            "mac": self._generate_mac(payload_bytes)

        }

       

        url = f"{self.peers[peer_id].rstrip('/')}/append-entries"

        try:

            async with self.http_session.post(url, json=payload_with_mac, timeout=2.0) as resp:

                if resp.status == 200:

                    response_with_mac = await resp.json()

                    # --- Verifikasi MAC Balasan ---

                    response_data = response_with_mac.get("data")

                    response_mac = response_with_mac.get("mac")

                    if response_data is None or response_mac is None:

                         self.logger.error(f"Balasan /append-entries dari {peer_id} tidak lengkap")

                         return

                    response_bytes = json.dumps(response_data, sort_keys=True).encode('utf-8')

                    if not self._verify_mac(response_bytes, response_mac):

                         self.logger.error(f"Balasan /append-entries dari {peer_id} GAGAL VERIFIKASI MAC!")

                         return

                    # --- Akhir Verifikasi ---



                    peer_term = response_data.get("term", 0)

                    if peer_term > self.current_term:

                        self.logger.info(f"Peer {peer_id} punya term lebih tinggi {peer_term} -> mundur.")

                        await self._become_follower(peer_term)

                        return



                    if response_data.get("success", False):

                        self.next_index[peer_id] = len(self.log)

                        self.match_index[peer_id] = len(self.log) - 1

                        self.logger.debug(f"Replikasi ke {peer_id} berhasil. nextIndex={self.next_index[peer_id]}")

                        await self._check_for_commit()

                    else:

                        self.logger.warning(f"Replikasi ke {peer_id} gagal (log mismatch). Mundur 1.")

                        self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)

                else:

                    self.logger.debug(f"AppendEntries ke {peer_id} return status {resp.status}")

        except Exception as e:

            self.logger.debug(f"AppendEntries ke {peer_id} failed: {e}")



    async def _check_for_commit(self):

        """Cek apakah ada log baru yang bisa di-commit."""

        if self.state != "leader":

            return



        total_nodes = len(self.peers) + 1

        majority = total_nodes // 2 + 1

       

        for n in range(len(self.log) - 1, self.commit_index, -1):

            if self.log[n]['term'] != self.current_term:

                continue

               

            count = 1 + sum(1 for peer in self.peers if self.match_index[peer] >= n)

           

            if count >= majority:

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



        for peer_id in self.peers:

            self.next_index[peer_id] = len(self.log)

            self.match_index[peer_id] = -1

       

        await self._append_entries_loop() # Kirim heartbeat pertama



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

       

        received_data = payload_with_mac.get("data")

        received_mac = payload_with_mac.get("mac")

       

        if received_data is None or received_mac is None:

            self.logger.error("RPC /vote tidak lengkap (missing data/mac)")

            return {"data": {"term": self.current_term, "vote_granted": False}, "mac": "invalid"} # Kirim MAC invalid



        data_bytes = json.dumps(received_data, sort_keys=True).encode('utf-8')

        if not self._verify_mac(data_bytes, received_mac):

            self.logger.error("RPC /vote GAGAL VERIFIKASI MAC!")

            return {"data": {"term": self.current_term, "vote_granted": False}, "mac": "invalid"}



        # Jika MAC valid, proses data seperti biasa

        candidate_id = received_data.get("candidate_id")

        candidate_term = received_data.get("term")

        candidate_log_index = received_data.get("last_log_index", -1)

        candidate_log_term = received_data.get("last_log_term", 0)



        vote_granted = False

        if candidate_term < self.current_term:

            self.logger.debug(f"Menolak vote {candidate_id} (term kadaluwarsa {candidate_term})")

       

        elif candidate_term > self.current_term:

            await self._become_follower(candidate_term)

            # Logika jatuh ke bawah...



        # Cek hanya jika term cocok

        if candidate_term == self.current_term:

            if (self.voted_for is None or self.voted_for == candidate_id):

                our_last_log_index = len(self.log) - 1

                our_last_log_term = self.log[our_last_log_index]['term'] if our_last_log_index >= 0 else 0

               

                candidate_is_up_to_date = (

                    candidate_log_term > our_last_log_term or

                    (candidate_log_term == our_last_log_term and candidate_log_index >= our_last_log_index)

                )

               

                if candidate_is_up_to_date:

                    self.logger.info(f"Memberi suara untuk {candidate_id} (Term {self.current_term})")

                    vote_granted = True

                    self.voted_for = candidate_id

                    self._last_heartbeat_ts = time.time()

                else:

                    self.logger.warning(f"Menolak vote {candidate_id} (log-nya ketinggalan)")

            else:

                 self.logger.warning(f"Menolak vote {candidate_id} (sudah vote {self.voted_for})")



        # Bungkus balasan dengan MAC

        response_data = {"term": self.current_term, "vote_granted": vote_granted}

        response_bytes = json.dumps(response_data, sort_keys=True).encode('utf-8')

        return {"data": response_data, "mac": self._generate_mac(response_bytes)}



    async def handle_append_entries_rpc(self, payload_with_mac: Dict) -> Dict:

        """Logika untuk membalas RPC /append-entries (DENGAN VERIFIKASI MAC)."""

       

        received_data = payload_with_mac.get("data")

        received_mac = payload_with_mac.get("mac")



        if received_data is None or received_mac is None:

            self.logger.error("RPC /append-entries tidak lengkap (missing data/mac)")

            return {"data": {"term": self.current_term, "success": False}, "mac": "invalid"}



        data_bytes = json.dumps(received_data, sort_keys=True).encode('utf-8')

        if not self._verify_mac(data_bytes, received_mac):

            self.logger.error("RPC /append-entries GAGAL VERIFIKASI MAC!")

            return {"data": {"term": self.current_term, "success": False}, "mac": "invalid"}

           

        # Jika MAC valid, proses data

        leader_id = received_data.get("leader_id")

        leader_term = received_data.get("term")

        prev_log_index = received_data.get("prev_log_index", -1)

        prev_log_term = received_data.get("prev_log_term", 0)

        entries = received_data.get("entries", [])

        leader_commit = received_data.get("leader_commit", -1)

       

        success = False

        if leader_term < self.current_term:

            self.logger.warning(f"Menolak AppendEntries dari {leader_id} (term kadaluwarsa {leader_term})")

       

        else: # leader_term >= self.current_term

            if leader_term > self.current_term:

                await self._become_follower(leader_term)

           

            if self.state == "candidate":

                await self._become_follower(leader_term)



            self.leader_id = leader_id

            self._last_heartbeat_ts = time.time()

            self.logger.debug("Menerima heartbeat, timer di-reset.")



            # Log matching check

            if prev_log_index >= 0:

                if len(self.log) <= prev_log_index or self.log[prev_log_index]['term'] != prev_log_term:

                    self.logger.warning(f"Gagal Log Match di index {prev_log_index}. Log kita: {len(self.log)}")

                    success = False # Kirim false agar leader mundur

                else: # Log cocok sampai prev_log_index

                    success = True

            else: # Log pertama

                success = True



            if success:

                if entries:

                    self.logger.info(f"Menerima {len(entries)} entri baru dari {leader_id}.")

                    self.log = self.log[:prev_log_index + 1] + entries

                    self.logger.info(f"Log sekarang memiliki {len(self.log)} entri.")



                if leader_commit > self.commit_index:

                    self.commit_index = min(leader_commit, len(self.log) - 1)

                    self.logger.debug(f"Commit index diperbarui ke {self.commit_index}")

       

        # Bungkus balasan dengan MAC

        response_data = {"term": self.current_term, "success": success}

        response_bytes = json.dumps(response_data, sort_keys=True).encode('utf-8')

        return {"data": response_data, "mac": self._generate_mac(response_bytes)}





    async def client_request(self, command: str) -> Dict:

        """Dipanggil oleh handler /client-request."""

        if self.state != "leader":

            return {"success": False, "message": "Bukan Leader"}



        try:

            cmd_parts = command.split(' ')

            cmd_type = cmd_parts[0]

            lock_name = cmd_parts[1]

            client_id = cmd_parts[2]

           

            if cmd_type.startswith("ACQUIRE"):

                if self.check_for_deadlock(client_id, lock_name):

                    self.logger.error(f"Permintaan {command} ditolak karena menyebabkan DEADLOCK.")

                    return {"success": False, "message": "Deadlock terdeteksi, permintaan ditolak"}

        except Exception as e:

            self.logger.error(f"Error parsing command di client_request: {e}")

            return {"success": False, "message": "Command parse error"}



        log_entry = {'term': self.current_term, 'command': command}

        self.log.append(log_entry)

        log_index = len(self.log) - 1

       

        self.logger.info(f"Leader menambahkan log baru [{log_index}]: {command}")



        commit_event = asyncio.Event()

        self.commit_events[log_index] = {'event': commit_event, 'success': False}

       

        # Memicu replikasi

        tasks = []

        for peer_id in self.peers.keys():

            tasks.append(self._replicate_log_to_peer(peer_id))

        await asyncio.gather(*tasks, return_exceptions=True)



        self.logger.info(f"Menunggu log [{log_index}] di-commit...")

        try:

            await asyncio.wait_for(commit_event.wait(), timeout=5.0)

        except asyncio.TimeoutError:

            self.logger.error(f"Timeout menunggu commit untuk log [{log_index}]")

            if log_index in self.commit_events:

                del self.commit_events[log_index]

            return {"success": False, "message": "Perintah gagal di-commit (timeout)"}



        result = self.commit_events.pop(log_index, {})

        if result.get('success', False):

            self.logger.info(f"Log [{log_index}] berhasil di-commit dan diterapkan.")

            return {"success": True, "message": "Perintah berhasil di-commit"}

        else:

            return {"success": False, "message": "Perintah gagal diterapkan"}



    def check_for_deadlock(self, client_id, lock_name):

        """Deteksi deadlock sederhana menggunakan Wait-for-Graph (WFG)."""

        self.logger.info(f"Mengecek deadlock untuk {client_id} yang meminta {lock_name}...")



        if lock_name not in self.state_machine:

            return False

       

        holders = self.state_machine[lock_name].get('owners', set())

        if not holders:

            return False



        wait_for_graph = {}

        for lock, waiting_clients in self.wait_queue.items():

            if lock in self.state_machine:

                holding_clients = self.state_machine[lock].get('owners', set())

                for waiting_client in waiting_clients:

                    if waiting_client not in wait_for_graph:

                        wait_for_graph[waiting_client] = set()

                    wait_for_graph[waiting_client].update(holding_clients)



        if client_id not in wait_for_graph:

            wait_for_graph[client_id] = set()

        wait_for_graph[client_id].update(holders)



        path = set()

        visited = set()



        def has_cycle(user):

            path.add(user)

            if user not in wait_for_graph:

                path.remove(user)

                return False

               

            for waited_user in wait_for_graph[user]:

                if waited_user in path:

                    self.logger.error(f"!!! DEADLOCK TERDETEKSI !!! Siklus: {path} -> {waited_user}")

                    return True

                if waited_user not in visited:

                    visited.add(waited_user)

                    if has_cycle(waited_user):

                        return True

            path.remove(user)

            return False



        return has_cycle(client_id)



    def status_dict(self) -> Dict:

        """Return status untuk endpoint /status (JSON-safe)."""

       

        json_safe_state_machine = {}

        for lock_name, lock_data in self.state_machine.items():

            json_safe_state_machine[lock_name] = {

                'mode': lock_data.get('mode'),

                'owners': list(lock_data.get('owners', set()))

            }

       

        return {

            "node_id": self.node_id,

            "state": self.state,

            "term": self.current_term,

            "leader_id": self.leader_id,

            "voted_for": self.voted_for,

            "peers": list(self.peers.keys()),

            "election_timeout": round(self.election_timeout, 2),

            "last_heartbeat_age": round(time.time() - self._last_heartbeat_ts, 2),

            "log_length": len(self.log),

            "commit_index": self.commit_index,

            "last_applied": self.last_applied,

            "state_machine": json_safe_state_machine

        }