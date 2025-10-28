# test/unit/test_raft_node.py
from typing import Dict
import pytest
import asyncio
import json
import time
from unittest.mock import AsyncMock, patch, MagicMock
# Pastikan path import ini sesuai dengan struktur folder Anda
from src.nodes.base_node import RaftNode

# Tandai semua tes di file ini sebagai asyncio
pytestmark = pytest.mark.asyncio

@pytest.fixture
def minimal_node():
    """Fixture untuk membuat instance RaftNode dasar (N=3)."""
    peers = {"node2": "http://node2:8000", "node3": "http://node3:8000"}
    node = RaftNode(node_id="node1", peers=peers)
    node.http_session = AsyncMock()
    # Inisialisasi next/match index jika node leader
    node.next_index = {"node2": 0, "node3": 0}
    node.match_index = {"node2": -1, "node3": -1}
    # Mock task background agar tidak berjalan otomatis saat start
    node._applier_task = AsyncMock()
    node._election_task = AsyncMock()
    node._heartbeat_task = AsyncMock()
    # Patch time.time agar bisa dikontrol dalam tes
    with patch('time.time', return_value=time.time()) as mock_time:
         node._time = mock_time # Simpan mock time
         yield node # Serahkan node ke tes


@pytest.fixture
def single_node():
    """Fixture untuk instance RaftNode tunggal (N=1)."""
    node = RaftNode(node_id="node1", peers={})
    node.http_session = AsyncMock()
    # Mock task background
    node._applier_task = AsyncMock()
    node._election_task = AsyncMock()
    node._heartbeat_task = AsyncMock()
    with patch('time.time', return_value=time.time()) as mock_time:
         node._time = mock_time
         yield node


async def test_initial_state(minimal_node):
    """Tes state awal node."""
    assert minimal_node.state == "follower"
    assert minimal_node.current_term == 0
    assert minimal_node.voted_for is None
    assert minimal_node.commit_index == -1
    assert minimal_node.last_applied == -1

async def test_become_follower(minimal_node):
    """Tes transisi ke state follower."""
    minimal_node.current_term = 1
    minimal_node.state = "candidate"
    await minimal_node._become_follower(new_term=2)
    assert minimal_node.state == "follower"
    assert minimal_node.current_term == 2
    assert minimal_node.voted_for is None

# --- Tes Logika RPC (handle_vote_rpc) ---

def _create_vote_request(node: RaftNode, term: int, candidate_id: str = "node2", log_idx: int = -1, log_term: int = 0) -> Dict:
    """Helper untuk membuat payload vote request yang valid dengan MAC."""
    payload_data = {
        "candidate_id": candidate_id,
        "term": term,
        "last_log_index": log_idx,
        "last_log_term": log_term
    }
    payload_bytes = json.dumps(payload_data, sort_keys=True).encode('utf-8')
    return {
        "data": payload_data,
        "mac": node._generate_mac(payload_bytes)
    }

async def test_handle_vote_rpc_grant_vote(minimal_node):
    """Tes: Memberikan suara ke kandidat yang valid di term baru."""
    payload = _create_vote_request(minimal_node, term=1)
    response = await minimal_node.handle_vote_rpc(payload)

    assert response["data"]["vote_granted"] is True
    assert response["data"]["term"] == 1
    assert minimal_node.current_term == 1
    assert minimal_node.voted_for == "node2"

async def test_handle_vote_rpc_deny_stale_term(minimal_node):
    """Tes: Menolak suara untuk kandidat dengan term lama."""
    minimal_node.current_term = 2 # Node ini sudah di term 2
    payload = _create_vote_request(minimal_node, term=1)
    response = await minimal_node.handle_vote_rpc(payload)

    assert response["data"]["vote_granted"] is False
    assert response["data"]["term"] == 2 # Balas dengan term saat ini

async def test_handle_vote_rpc_deny_stale_log(minimal_node):
    """Tes: Menolak suara karena log kandidat ketinggalan."""
    minimal_node.current_term = 1
    minimal_node.log = [{'term': 1, 'command': 'cmd1'}] # Log node ini lebih baru

    # Kandidat di term 1, tapi lognya kosong
    payload = _create_vote_request(minimal_node, term=1, log_idx=-1, log_term=0)
    response = await minimal_node.handle_vote_rpc(payload)

    assert response["data"]["vote_granted"] is False
    assert minimal_node.voted_for is None

async def test_handle_vote_rpc_invalid_mac(minimal_node):
    """Tes: Menolak request dengan MAC yang tidak valid."""
    payload = _create_vote_request(minimal_node, term=1)
    payload["mac"] = "mac_yang_salah_123" # MAC tidak valid

    response = await minimal_node.handle_vote_rpc(payload)

    assert response["data"]["vote_granted"] is False
    assert response["mac"] == "invalid"

# --- Tes Logika Commit (N=3 vs N=1) ---

async def test_check_commit_majority_n3(minimal_node):
    """Tes commit_index naik ketika mayoritas (N=3) tercapai."""
    minimal_node.state = "leader"
    minimal_node.current_term = 1
    minimal_node.log = [{'term': 1, 'command': 'cmd1'}] # Log index 0
    minimal_node.commit_index = -1
    # Asumsikan mayoritas peer (node2) sudah cocok
    minimal_node.match_index = {"node2": 0, "node3": -1}
    # Total = 2 (self + node2) >= majority (2)

    await minimal_node._check_for_commit()

    assert minimal_node.commit_index == 0 # Commit index harus naik ke 0

async def test_check_commit_no_majority_n3(minimal_node):
    """Tes commit_index TIDAK naik jika mayoritas (N=3) belum tercapai."""
    minimal_node.state = "leader"
    minimal_node.current_term = 1
    minimal_node.log = [{'term': 1, 'command': 'cmd1'}]
    minimal_node.commit_index = -1
    # Hanya diri sendiri yang cocok
    minimal_node.match_index = {"node2": -1, "node3": -1}
    # Total = 1 (self) < majority (2)

    await minimal_node._check_for_commit()

    assert minimal_node.commit_index == -1 # Commit index tetap -1

async def test_check_commit_single_node_n1(single_node):
    """Tes commit_index naik di node tunggal (N=1)."""
    single_node.state = "leader"
    single_node.current_term = 1
    single_node.log = [{'term': 1, 'command': 'cmd1'}]
    single_node.commit_index = -1
    single_node.match_index = {} # Tidak ada peer
    # Total = 1 (self) >= majority (1)

    await single_node._check_for_commit()

    assert single_node.commit_index == 0 # Harus naik

# --- Tes Perbaikan Universal client_request ---

@patch('src.nodes.base_node.RaftNode._replicate_log_to_peer', new_callable=AsyncMock)
async def test_client_request_n1_commits(mock_replicate, single_node):
    """Tes client_request di N=1 (HARUS berhasil commit)."""
    single_node.state = "leader"
    single_node.current_term = 1
    command = "ACQUIRE_EXCLUSIVE lock1 client1"

    # Jalankan client_request sebagai task
    request_task = asyncio.create_task(single_node.client_request(command))

    # Beri sedikit waktu agar _check_for_commit (dipanggil oleh client_request) berjalan
    await asyncio.sleep(0.01)

    # Verifikasi bahwa _check_for_commit menaikkan commit_index
    assert single_node.commit_index == 0

    # Simulasikan _commit_log_applier yang melihat commit_index baru
    # dan men-set event
    assert 0 in single_node.commit_events, "Commit event for index 0 was not created"
    single_node.commit_events[0]['success'] = True
    single_node.commit_events[0]['event'].set()

    # Sekarang tunggu hasil client_request (seharusnya tidak timeout)
    result = await asyncio.wait_for(request_task, timeout=1.0) # Beri sedikit waktu

    # Cek hasil
    assert result["success"] is True
    assert result["message"] == "Perintah berhasil di-commit"
    assert len(single_node.log) == 1
    assert single_node.log[0]['command'] == command

    # Pastikan replikasi TIDAK dipanggil karena peers kosong
    mock_replicate.assert_not_called()


@patch('src.nodes.base_node.RaftNode._replicate_log_to_peer', new_callable=AsyncMock)
async def test_client_request_n3_waits_for_commit(mock_replicate, minimal_node):
    """Tes client_request di N=3 (HARUS timeout jika replikasi tidak selesai)."""
    minimal_node.state = "leader"
    minimal_node.current_term = 1
    command = "ACQUIRE_EXCLUSIVE lock1 client1"

    # Mock _replicate_log_to_peer agar 'menggantung' (tidak memanggil _check_for_commit)
    replication_started = asyncio.Event()
    async def slow_replication(*args, **kwargs):
        replication_started.set()
        await asyncio.sleep(10) # Tidur lebih lama dari timeout client_request

    mock_replicate.side_effect = slow_replication

    # Jalankan client_request dan HARAPKAN TimeoutError dari wait_for internalnya
    # Tidak perlu `with pytest.raises` karena kita menangkap timeout di dalam client_request
    result = await minimal_node.client_request(command)

    # Cek hasil (harus gagal karena timeout internal)
    assert result["success"] is False
    assert "timeout" in result["message"] # Pesan error harus mengandung timeout

    # Cek state setelah timeout
    assert mock_replicate.call_count == 2 # Replikasi harus dipanggil
    await replication_started.wait() # Pastikan replikasi sempat dimulai
    assert len(minimal_node.log) == 1 # Log ditambahkan
    # TAPI commit_index BELUM naik karena _check_for_commit (dalam _replicate) tidak terpanggil
    assert minimal_node.commit_index == -1
    # Event tidak di-set (atau sudah dihapus oleh blok finally/except)
    assert 0 not in minimal_node.commit_events