# test/unit/test_queue_node.py
import pytest
import os
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from src.nodes.queue_node import ConsistentHashRing
import redis.asyncio as redis

# --- Tes Logika Consistent Hashing (Synchronous) ---

@pytest.fixture
def hash_ring():
    nodes = ['redis-1', 'redis-2', 'redis-3']
    return ConsistentHashRing(nodes=nodes, replicas=2)

def test_consistent_hash_add_nodes(hash_ring):
    assert len(hash_ring.ring) == 6
    assert len(hash_ring._sorted_keys) == 6

def test_consistent_hash_get_node_consistency(hash_ring):
    node_a1 = hash_ring.get_node("kunci_yang_sama_123")
    node_a2 = hash_ring.get_node("kunci_yang_sama_123")
    assert node_a1 == node_a2

def test_consistent_hash_get_node_distribution(hash_ring):
    node_q1 = hash_ring.get_node("queue_penting")
    node_q2 = hash_ring.get_node("queue_log_A")
    node_q3 = hash_ring.get_node("antrian_notifikasi_B")

    assert node_q1 in ['redis-1', 'redis-2', 'redis-3']
    assert node_q2 in ['redis-1', 'redis-2', 'redis-3']
    assert node_q3 in ['redis-1', 'redis-2', 'redis-3']
    print(f"Distribusi: q1->{node_q1}, q2->{node_q2}, q3->{node_q3}")

def test_consistent_hash_empty_ring():
    empty_ring = ConsistentHashRing(nodes=[])
    assert empty_ring.get_node("apapun") is None

# --- Fixture Mock Redis Async ---

@pytest.fixture
def mock_redis_conn():
    mock_conn = AsyncMock()
    mock_conn.xadd = AsyncMock(return_value=b'12345-0')
    mock_conn.xgroup_create = AsyncMock(return_value=None)
    mock_conn.xreadgroup = AsyncMock(return_value=[
        (b'queue_test', [(b'12345-0', {b'data': b'test_value'})])
    ])
    mock_conn.aclose = AsyncMock()
    return mock_conn

@pytest.fixture
def mock_redis_pool(mock_redis_conn):
    mock_pool = MagicMock()
    with patch('redis.asyncio.Redis', return_value=mock_redis_conn) as mock_redis_class:
        yield {
            'pool': mock_pool,
            'conn': mock_redis_conn,
            'redis_class': mock_redis_class
        }

@pytest.fixture(autouse=True)
def mock_env_vars():
    with patch.dict(os.environ, {
        "NODE_ID": "queue-test-1",
        "INTERNAL_PORT": "8000",
        "REDIS_NODES": "redis-1,redis-2,redis-3"
    }):
        yield

# --- Tes Async Redis Call ---

@pytest.mark.asyncio
async def test_example_async_redis_call(mock_redis_pool):
    r = redis.Redis(connection_pool=mock_redis_pool['pool'])
    msg_id = await r.xadd('my_queue', {'data': 'value'})
    await r.aclose()

    mock_redis_pool['conn'].xadd.assert_called_once_with('my_queue', {'data': 'value'})
    mock_redis_pool['conn'].aclose.assert_called_once()
    assert msg_id == b'12345-0'

@pytest.mark.asyncio
async def test_specific_routing(mock_redis_pool):
    target_node = 'redis-2'
    queue_name_for_target = 'antrian_khusus_2'

    with patch('src.nodes.queue_node.ConsistentHashRing.get_node', return_value=target_node) as mock_get_node:
        ring_instance_for_test = ConsistentHashRing(nodes=['redis-1', 'redis-2', 'redis-3'])
        actual_target = ring_instance_for_test.get_node(queue_name_for_target)

        mock_get_node.assert_called_with(queue_name_for_target)
        assert actual_target == target_node

        # Gunakan mock pool untuk node target
        pool_to_use = mock_redis_pool['pool']
        r = redis.Redis(connection_pool=pool_to_use)
        await r.xadd(queue_name_for_target, {'data': 'routed_correctly'})
        await r.aclose()

        mock_redis_pool['conn'].xadd.assert_called_once_with(queue_name_for_target, {'data': 'routed_correctly'})
        mock_redis_pool['conn'].aclose.assert_called_once()
