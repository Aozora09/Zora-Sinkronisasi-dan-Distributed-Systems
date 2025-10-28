# test/unit/test_cache_node.py
import pytest
# Pastikan path import ini sesuai dengan struktur folder Anda
from src.nodes.cache_node import LruCache

@pytest.fixture
def cache_instance():
    """Fixture untuk instance LruCache dengan kapasitas 3."""
    cache = LruCache(capacity=3)
    # Reset metrics untuk setiap tes
    cache.metrics_hits = 0
    cache.metrics_misses = 0
    return cache

def test_lru_cache_initial(cache_instance):
    assert cache_instance.capacity == 3
    assert len(cache_instance.cache) == 0

def test_lru_cache_write_within_capacity(cache_instance):
    cache_instance.write('a', 1)
    cache_instance.write('b', 2)
    assert len(cache_instance.cache) == 2
    assert cache_instance.cache['a'] == {'state': 'M', 'value': 1}
    assert cache_instance.cache['b'] == {'state': 'M', 'value': 2}
    # Cek urutan (paling baru di akhir)
    assert list(cache_instance.cache.keys()) == ['a', 'b']

def test_lru_cache_write_exceed_capacity(cache_instance):
    cache_instance.write('a', 1)
    cache_instance.write('b', 2)
    cache_instance.write('c', 3)
    cache_instance.write('d', 4) # Ini harus mengeluarkan 'a'

    assert len(cache_instance.cache) == 3
    assert 'a' not in cache_instance.cache
    assert 'd' in cache_instance.cache
    assert list(cache_instance.cache.keys()) == ['b', 'c', 'd'] # 'a' terdepak

def test_lru_cache_read_hit_moves_to_end(cache_instance):
    cache_instance.write('a', 1)
    cache_instance.write('b', 2)
    cache_instance.write('c', 3)
    # Set state A menjadi Shared untuk tes read
    cache_instance.cache['a']['state'] = 'S'

    assert list(cache_instance.cache.keys()) == ['a', 'b', 'c']
    result = cache_instance.read('a')

    assert result == {'state': 'S', 'value': 1}
    # 'a' harus pindah ke akhir (paling baru digunakan)
    assert list(cache_instance.cache.keys()) == ['b', 'c', 'a']
    assert cache_instance.metrics_hits == 1
    assert cache_instance.metrics_misses == 0

def test_lru_cache_read_miss_invalid_state(cache_instance):
    cache_instance.write('a', 1)
    cache_instance.cache['a']['state'] = 'I' # Set ke Invalid
    result = cache_instance.read('a')

    assert result is None
    assert cache_instance.metrics_hits == 0
    assert cache_instance.metrics_misses == 1
    # Membaca 'I' tidak boleh mengubah urutan LRU
    assert list(cache_instance.cache.keys()) == ['a']

def test_lru_cache_read_miss_not_found(cache_instance):
    result = cache_instance.read('x') # Key tidak ada
    assert result is None
    assert cache_instance.metrics_hits == 0
    assert cache_instance.metrics_misses == 1

def test_lru_cache_invalidate(cache_instance):
    cache_instance.write('a', 1)
    cache_instance.cache['a']['state'] = 'S' # Misal state Shared
    cache_instance.invalidate('a')

    assert cache_instance.cache['a']['state'] == 'I'
    # Invalidate tidak boleh mengubah urutan LRU
    assert list(cache_instance.cache.keys()) == ['a']

def test_lru_cache_share_from_modified(cache_instance):
    cache_instance.write('a', 1) # State awal 'M'
    result = cache_instance.share('a')

    assert cache_instance.cache['a']['state'] == 'S'
    assert result['state'] == 'S'

def test_lru_cache_share_from_exclusive(cache_instance):
    cache_instance.write('a', 1)
    cache_instance.cache['a']['state'] = 'E' # Misal state Exclusive
    result = cache_instance.share('a')

    assert cache_instance.cache['a']['state'] == 'S'
    assert result['state'] == 'S'

def test_lru_cache_share_from_shared(cache_instance):
    cache_instance.write('a', 1)
    cache_instance.cache['a']['state'] = 'S' # State sudah Shared
    result = cache_instance.share('a')

    assert cache_instance.cache['a']['state'] == 'S'
    assert result['state'] == 'S'