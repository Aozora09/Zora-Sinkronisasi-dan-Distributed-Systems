import pytest
import asyncio
import aiohttp
import ssl # <-- Import ssl
import time
import os
import uuid # <-- Import uuid
import json # <-- Import json

# Tandai semua tes di file ini sebagai asyncio
pytestmark = pytest.mark.asyncio

# Alamat node cache (HTTPS)
CACHE1_PORT = os.getenv("CACHE1_PORT", "10001")
CACHE2_PORT = os.getenv("CACHE2_PORT", "10002")
CACHE3_PORT = os.getenv("CACHE3_PORT", "10003")
CACHE_NODE_URLS = [
    f"https://localhost:{CACHE1_PORT}",
    f"https://localhost:{CACHE2_PORT}",
    f"https://localhost:{CACHE3_PORT}",
]

# Path ke CA certificate (relatif dari root proyek)
CA_CERT_PATH = "certs/ca.crt"

# --- Helper Functions (URL akan pakai https) ---
async def write_cache(session: aiohttp.ClientSession, node_url: str, key: str, value: str) -> tuple[int, dict | str]:
    payload = {"value": value}
    try:
        async with session.post(f"{node_url}/cache/{key}", json=payload, timeout=3) as resp:
            print(f"WRITE {key}={value} to {node_url}: Status {resp.status}")
            response_body = await resp.json() if resp.content_type == 'application/json' else await resp.text()
            print(f"  Resp: {response_body}")
            return resp.status, response_body
    except Exception as e: print(f"Error WRITE {node_url}: {e}"); return 500, str(e)

async def read_cache(session: aiohttp.ClientSession, node_url: str, key: str) -> tuple[int, dict | str]:
    try:
        async with session.get(f"{node_url}/cache/{key}", timeout=3) as resp:
            print(f"READ {key} from {node_url}: Status {resp.status}")
            response_body = await resp.json() if resp.content_type == 'application/json' else await resp.text()
            return resp.status, response_body
    except Exception as e: print(f"Error READ {node_url}: {e}"); return 500, str(e)

async def get_cache_entry_state(session: aiohttp.ClientSession, node_url: str, key: str) -> str | None:
    try:
        async with session.get(f"{node_url}/status", timeout=1.5) as resp:
            if resp.status == 200:
                data = await resp.json()
                entry = data.get("cache_content", {}).get(key)
                return entry.get("state") if entry else None
            else: return None # Anggap None jika status gagal
    except Exception: return None # Anggap None jika error
# --- Akhir Helper Functions ---

# --- Test Cases ---
async def test_cache_write_read_coherence():
    """Tes alur: Tulis ke node1, Baca dari node2 -> state jadi Shared (via HTTPS)."""
    key = f"cache_key_{int(time.time())}"; value1 = f"value_{uuid.uuid4()}"
    node1_url = CACHE_NODE_URLS[0]; node2_url = CACHE_NODE_URLS[1]

    # --- Konfigurasi SSL Context ---
    ssl_context = None
    try:
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=CA_CERT_PATH)
        # --- PERBAIKAN: Nonaktifkan pemeriksaan hostname ---
        ssl_context.check_hostname = False
        # ---------------------------------------------------
        print(f"SSL Context tes: CA={CA_CERT_PATH}, Hostname Check: OFF")
    except FileNotFoundError: pytest.fail(f"CA cert not found: {CA_CERT_PATH}")
    except Exception as e: pytest.fail(f"SSL context error: {e}")
    # -----------------------------

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        # 1. Tulis data ke node1
        status_write, body_write = await write_cache(session, node1_url, key, value1)
        assert status_write == 200; assert isinstance(body_write, dict)
        assert body_write.get("state") in ["M", "E"]
        await asyncio.sleep(0.5)
        state_node1 = await get_cache_entry_state(session, node1_url, key)
        assert state_node1 in ["M", "E"]

        # 2. Baca data dari node2
        print(f"\nREAD {key} from {node2_url}...")
        status_read2, body_read2 = await read_cache(session, node2_url, key)
        assert status_read2 == 200; assert isinstance(body_read2, dict)
        assert body_read2.get("value") == value1
        read_state_node2 = body_read2.get("state")
        print(f"  State read node2: {read_state_node2}")
        assert read_state_node2 in ["S", "E"]
        await asyncio.sleep(0.7)

        # 3. Verifikasi state Shared
        print("\nVerifikasi state Shared...")
        state_node1_after = await get_cache_entry_state(session, node1_url, key)
        state_node2_after = await get_cache_entry_state(session, node2_url, key)
        print(f"  State node1: {state_node1_after}, State node2: {state_node2_after}")
        assert state_node1_after == "S", f"Node1 state post-read salah: {state_node1_after}"
        assert state_node2_after == "S", f"Node2 state post-read salah: {state_node2_after}"
        print("Verifikasi state Shared OK.")

async def test_cache_write_invalidate_coherence():
    """Tes alur: Tulis ke node1, Tulis ke node2 -> state di node1 jadi Invalid (via HTTPS)."""
    key = f"cache_key_{int(time.time())}"; value1 = f"value1_{uuid.uuid4()}"; value2 = f"value2_{uuid.uuid4()}"
    node1_url = CACHE_NODE_URLS[0]; node2_url = CACHE_NODE_URLS[1]

    # --- Konfigurasi SSL Context ---
    ssl_context = None
    try:
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=CA_CERT_PATH)
        # --- PERBAIKAN: Nonaktifkan pemeriksaan hostname ---
        ssl_context.check_hostname = False
        # ---------------------------------------------------
        print(f"SSL Context tes: CA={CA_CERT_PATH}, Hostname Check: OFF")
    except FileNotFoundError: pytest.fail(f"CA cert not found: {CA_CERT_PATH}")
    except Exception as e: pytest.fail(f"SSL context error: {e}")
    # -----------------------------

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        # 1. Tulis ke node1
        status_write1, _ = await write_cache(session, node1_url, key, value1)
        assert status_write1 == 200; await asyncio.sleep(0.2)

        # 2. Baca dari node 2 -> state jadi Shared
        await read_cache(session, node2_url, key); await asyncio.sleep(0.7)
        print("\nVerifikasi state Shared...")
        assert await get_cache_entry_state(session, node1_url, key) == "S"
        assert await get_cache_entry_state(session, node2_url, key) == "S"

        # 3. Tulis BARU ke node2 -> invalidate node1
        print(f"\nWRITE {key}={value2} -> {node2_url}...")
        status_write2, body_write2 = await write_cache(session, node2_url, key, value2)
        assert status_write2 == 200; assert isinstance(body_write2, dict)
        assert body_write2.get("state") == "M"; assert body_write2.get("value") == value2
        print("\nMenunggu invalidate...")
        await asyncio.sleep(1.2) # Waktu bus HTTPS

        # 4. Verifikasi state Invalid di node1
        print("\nVerifikasi state Invalid...")
        state_node1_final = await get_cache_entry_state(session, node1_url, key)
        state_node2_final = await get_cache_entry_state(session, node2_url, key)
        print(f"  State node1: {state_node1_final}, State node2: {state_node2_final}")
        assert state_node1_final == "I", f"Node1 state post-invalidate salah: {state_node1_final}"
        assert state_node2_final in ["M", "E"], f"Node2 state post-write salah: {state_node2_final}"
        print("Verifikasi state Invalid OK.")

# TODO: Tambahkan tes HTTPS lainnya

