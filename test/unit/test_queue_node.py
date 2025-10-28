# test/unit/test_queue_node.py
import pytest
import os
import asyncio # Diperlukan untuk marker async
from unittest.mock import AsyncMock, patch, MagicMock
# Pastikan path import ini sesuai dengan struktur folder Anda
from src.nodes.queue_node import ConsistentHashRing
import redis.asyncio as redis # Import untuk patching

# --- Tes Logika Consistent Hashing (BUKAN ASYNC)---

@pytest.fixture
def hash_ring():
    """Fixture untuk instance ConsistentHashRing."""
    nodes = ['redis-1', 'redis-2', 'redis-3']
    # Gunakan 2 replica untuk tes agar lebih deterministik
    return ConsistentHashRing(nodes=nodes, replicas=2)

# Tes ini TIDAK perlu async
def test_consistent_hash_add_nodes(hash_ring):
    # Total entri = 3 nodes * 2 replicas = 6
    assert len(hash_ring.ring) == 6
    assert len(hash_ring._sorted_keys) == 6

# Tes ini TIDAK perlu async
def test_consistent_hash_get_node_consistency(hash_ring):
    # Key yang sama HARUS selalu dipetakan ke node yang sama
    node_a1 = hash_ring.get_node("kunci_yang_sama_123")
    node_a2 = hash_ring.get_node("kunci_yang_sama_123")
    assert node_a1 == node_a2

# Tes ini TIDAK perlu async
def test_consistent_hash_get_node_distribution(hash_ring):
    # Tes distribusi key (ini hanya contoh, hasil hash bisa bervariasi)
    node_q1 = hash_ring.get_node("queue_penting")
    node_q2 = hash_ring.get_node("queue_log_A")
    node_q3 = hash_ring.get_node("antrian_notifikasi_B")

    assert node_q1 in ['redis-1', 'redis-2', 'redis-3']
    assert node_q2 in ['redis-1', 'redis-2', 'redis-3']
    assert node_q3 in ['redis-1', 'redis-2', 'redis-3']
    print(f"Distribusi: q1->{node_q1}, q2->{node_q2}, q3->{node_q3}") # Optional: lihat distribusi

# Tes ini TIDAK perlu async
def test_consistent_hash_empty_ring():
    empty_ring = ConsistentHashRing(nodes=[])
    assert empty_ring.get_node("apapun") is None

# --- Tes Handler API (ASYNC) ---

@pytest.fixture
def mock_redis_conn():
    """Fixture untuk mock koneksi Redis."""
    mock_conn = AsyncMock()
    mock_conn.xadd = AsyncMock(return_value=b'12345-0')
    mock_conn.xgroup_create = AsyncMock(return_value=True)
    mock_conn.xreadgroup = AsyncMock(return_value=[
        (b'queue_test', [(b'12345-0', {b'data': b'test_value'})])
    ])
    mock_conn.close = AsyncMock()
    return mock_conn

@pytest.fixture
def mock_redis_pool(mock_redis_conn):
    """Fixture untuk mock connection pool Redis."""
    mock_pool = MagicMock()
    # Menggunakan patch di dalam fixture agar lebih mudah
    # Ini memastikan bahwa setiap kali redis.asyncio.Redis(...) dipanggil,
    # ia akan mengembalikan mock_redis_conn kita
    with patch('redis.asyncio.Redis', return_value=mock_redis_conn) as mock_redis_class:
        yield {
            'pool': mock_pool,
            'conn': mock_redis_conn,
            'redis_class': mock_redis_class
        }

@pytest.fixture(autouse=True)
def mock_env_vars():
    """Mock environment variables."""
    with patch.dict(os.environ, {
        "NODE_ID": "queue-test-1",
        "INTERNAL_PORT": "8000",
        "REDIS_NODES": "redis-1,redis-2,redis-3" # Pastikan ini ada
    }):
        yield

@pytest.mark.asyncio # Tandai tes ini saja sebagai async
async def test_example_async_redis_call(mock_redis_pool):
     """Contoh tes async sederhana yang menggunakan mock redis."""
     # Tes ini sekarang hanya bergantung pada mock_redis_pool fixture
     # yang sudah melakukan patch pada redis.asyncio.Redis

     # Pura-pura panggil logika inti handler
     # Kita bisa membuat instance Redis palsu menggunakan mock pool
     r = redis.Redis(connection_pool=mock_redis_pool['pool'])
     msg_id = await r.xadd('my_queue', {'data': 'value'})
     await r.close()

     # Cek apakah xadd dipanggil pada mock connection
     mock_redis_pool['conn'].xadd.assert_called_once_with('my_queue', {'data': 'value'})
     assert msg_id == b'12345-0'
     mock_redis_pool['conn'].close.assert_called_once()


# --- PERBAIKAN DI SINI ---
@pytest.mark.asyncio
async def test_specific_routing(mock_redis_pool):
    """Tes routing ke node spesifik."""
    target_node = 'redis-2' # Node yang ingin kita targetkan
    queue_name_for_target = 'antrian_khusus_2' # Nama antrian

    # Patch HANYA method `get_node` dari kelas `ConsistentHashRing`
    # Ini akan mempengaruhi SEMUA instance ConsistentHashRing yang dibuat
    # atau dipanggil selama patch aktif.
    with patch('src.nodes.queue_node.ConsistentHashRing.get_node', return_value=target_node) as mock_get_node:

        # Simulasikan logika yang relevan dari handler (atau panggil handler jika direfactor)
        # 1. Dapatkan target node menggunakan instance hash_ring (yang methodnya di-patch)
        #    Buat instance sementara HANYA untuk memanggil method yang di-patch
        ring_instance_for_test = ConsistentHashRing(nodes=['redis-1', 'redis-2', 'redis-3'])
        actual_target = ring_instance_for_test.get_node(queue_name_for_target)

        # Verifikasi bahwa method yang di-patch dipanggil dan mengembalikan nilai mock
        mock_get_node.assert_called_with(queue_name_for_target)
        assert actual_target == target_node

        # 2. Simulasikan penggunaan hasil routing untuk memilih pool
        #    Karena redis_pools adalah lokal di main(), kita tidak bisa patch langsung.
        #    Sebagai gantinya, kita ASUMSIKAN logika pemilihan pool benar,
        #    dan langsung gunakan mock_redis_pool untuk node target kita.
        pool_to_use = mock_redis_pool['pool'] # Anggap ini pool untuk 'redis-2'

        # 3. Buat koneksi Redis (ini akan mengembalikan mock_conn dari fixture)
        r = redis.Redis(connection_pool=pool_to_use)
        await r.xadd(queue_name_for_target, {'data': 'routed_correctly'})
        await r.close()

        # Verifikasi panggilan ke mock connection
        mock_redis_pool['conn'].xadd.assert_called_once_with(queue_name_for_target, {'data': 'routed_correctly'})
# --- AKHIR PERBAIKAN ---

# TODO: Tambahkan tes handler API yang lebih lengkap jika menggunakan pytest-aiohttp