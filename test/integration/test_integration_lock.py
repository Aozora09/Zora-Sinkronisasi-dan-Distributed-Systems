import pytest
import asyncio
import aiohttp
import ssl # <-- Import ssl
import time
import os
import json # <-- Import json

# Tandai semua tes di file ini sebagai asyncio
pytestmark = pytest.mark.asyncio

# Alamat node lock manager (HTTPS)
NODE1_PORT = os.getenv("NODE1_PORT", "8001")
NODE2_PORT = os.getenv("NODE2_PORT", "8002")
NODE3_PORT = os.getenv("NODE3_PORT", "8003")
LOCK_NODE_URLS = [
    f"https://localhost:{NODE1_PORT}",
    f"https://localhost:{NODE2_PORT}",
    f"https://localhost:{NODE3_PORT}",
]

# Path ke CA certificate (relatif dari root proyek)
CA_CERT_PATH = "certs/ca.crt"

# --- Helper Functions (Tetap sama, URL akan pakai https dari list) ---
async def find_leader(session: aiohttp.ClientSession) -> str | None:
    leader_found = None; node_states = {}
    for url in LOCK_NODE_URLS:
        try:
            async with session.get(f"{url}/status", timeout=1.5) as resp:
                if resp.status == 200:
                    data = await resp.json(); state = data.get('state'); term = data.get('term')
                    node_states[url] = f"State: {state}, Term: {term}"
                    if state == "leader": leader_found = url
                else: node_states[url] = f"Status {resp.status}"
        except asyncio.TimeoutError: node_states[url] = "Timeout"; print(f"Timeout {url}")
        except aiohttp.ClientConnectorError as e: node_states[url] = f"ConnErr ({type(e)})"; print(f"ConnErr {url}: {e}")
        except ssl.SSLError as e: node_states[url] = f"SSLErr ({type(e)})"; print(f"SSLErr {url}: {e}")
        except Exception as e: node_states[url] = f"Err: {e}"; print(f"Err {url}: {e}")
    print("--- Status Node ---"); [print(f"  {u}: {s}") for u, s in node_states.items()]; print("-------------------")
    if leader_found: print(f"Leader: {leader_found}"); return leader_found
    else: print("Leader NOT found!"); return None

async def acquire_lock(session: aiohttp.ClientSession, leader_url: str, lock_name: str, client_id: str, mode: str = "exclusive") -> bool:
    payload = {"type": "ACQUIRE", "mode": mode, "lock_name": lock_name, "client_id": client_id}
    try:
        async with session.post(f"{leader_url}/client-request", json=payload, timeout=10) as resp:
            print(f"ACQUIRE {lock_name} ({mode}) by {client_id}: Status {resp.status}")
            response_body = await resp.text()
            if resp.status == 200:
                try: result = json.loads(response_body); print(f"  Resp: {result}"); return result.get("status") == "ok"
                except json.JSONDecodeError: print(f"  Non-JSON: {response_body}"); return False
            else: print(f"  Error Body: {response_body}"); return False
    except asyncio.TimeoutError: print(f"TIMEOUT ACQUIRE {lock_name}"); return False
    except Exception as e: print(f"Error ACQUIRE {lock_name}: {e}"); return False

async def release_lock(session: aiohttp.ClientSession, leader_url: str, lock_name: str, client_id: str) -> bool:
    payload = {"type": "RELEASE", "lock_name": lock_name, "client_id": client_id}
    try:
        async with session.post(f"{leader_url}/client-request", json=payload, timeout=10) as resp:
            print(f"RELEASE {lock_name} by {client_id}: Status {resp.status}")
            response_body = await resp.text()
            if resp.status == 200:
                 try: result = json.loads(response_body); print(f"  Resp: {result}"); return result.get("status") == "ok"
                 except json.JSONDecodeError: print(f"  Non-JSON: {response_body}"); return False
            else: print(f"  Error Body: {response_body}"); return False
    except asyncio.TimeoutError: print(f"TIMEOUT RELEASE {lock_name}"); return False
    except Exception as e: print(f"Error RELEASE {lock_name}: {e}"); return False

async def get_lock_status(session: aiohttp.ClientSession, node_url: str, lock_name: str) -> dict | str | None:
    try:
        async with session.get(f"{node_url}/status", timeout=1.5) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("state_machine", {}).get(lock_name)
            else: return "error_status"
    except Exception as e:
        # Kurangi verbosity error di sini
        # print(f"Error get lock status {node_url}: {type(e)}")
        return "error_comm"
# --- Akhir Helper Functions ---

# --- Test Cases ---
async def test_lock_acquire_release_single_client():
    """Tes alur dasar: acquire lalu release oleh client yang sama (via HTTPS)."""
    lock_name = f"integration_lock_{int(time.time())}"
    client_id = "client_A_https"

    # --- Konfigurasi SSL Context untuk Client Test ---
    ssl_context = None
    try:
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=CA_CERT_PATH)
        # --- PERBAIKAN: Nonaktifkan pemeriksaan hostname ---
        ssl_context.check_hostname = False
        # ---------------------------------------------------
        print(f"SSL Context tes: CA={CA_CERT_PATH}, Hostname Check: OFF")
    except FileNotFoundError: pytest.fail(f"CA cert not found: {CA_CERT_PATH}")
    except Exception as e: pytest.fail(f"SSL context error: {e}")
    # ---------------------------------------------

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        leader_url = None; print("Mencari leader...")
        for i in range(15):
            print(f"Leader attempt {i+1}...")
            leader_url = await find_leader(session)
            if leader_url: break
            await asyncio.sleep(1)
        assert leader_url is not None, "Gagal menemukan leader Raft"

        # 1. Acquire lock
        print(f"\nACQUIRE {lock_name} -> {leader_url}")
        acquired = await acquire_lock(session, leader_url, lock_name, client_id)
        assert acquired is True, f"Gagal acquire {lock_name}"
        await asyncio.sleep(0.5)

        # Verifikasi state ACQUIRE
        print("\nVerifikasi state ACQUIRE (retry)...")
        retry_timeout_acquire = time.time() + 5.0; all_nodes_acquired = False; last_states_acquire = {}
        while time.time() < retry_timeout_acquire:
            all_acquired_this_check = True; current_states_acquire = {}
            for node_url in LOCK_NODE_URLS:
                lock_state = await get_lock_status(session, node_url, lock_name)
                current_states_acquire[node_url] = lock_state
                if lock_state is None or lock_state == "error_status" or lock_state == "error_comm" or lock_state.get("owners") != [client_id]:
                    print(f"  Wait ACQUIRE {node_url} (state: {lock_state})")
                    all_acquired_this_check = False; break
            last_states_acquire = current_states_acquire
            if all_acquired_this_check: all_nodes_acquired = True; print("  Semua node OK."); break
            await asyncio.sleep(0.3)
        if not all_nodes_acquired: print(f"State ACQUIRE timeout: {last_states_acquire}")
        assert all_nodes_acquired, f"State ACQUIRE {lock_name} tidak konsisten"
        print("Verifikasi state ACQUIRE OK.")

        # 2. Release lock
        print(f"\nRELEASE {lock_name} -> {leader_url}")
        released = await release_lock(session, leader_url, lock_name, client_id)
        assert released is True, f"Gagal release {lock_name}"

        # Verifikasi state RELEASE
        print("\nVerifikasi state RELEASE (retry)...")
        retry_timeout_release = time.time() + 5.0; all_nodes_released = False; last_states_release = {}
        while time.time() < retry_timeout_release:
            all_released_this_check = True; current_states_release = {}
            for node_url in LOCK_NODE_URLS:
                lock_state = await get_lock_status(session, node_url, lock_name)
                current_states_release[node_url] = lock_state
                if lock_state is not None and lock_state not in ["error_status", "error_comm"]:
                    all_released_this_check = False
                    print(f"  Wait RELEASE {node_url} (state: {lock_state})")
                    break
            last_states_release = current_states_release
            if all_released_this_check: all_nodes_released = True; print("  Semua node OK."); break
            await asyncio.sleep(0.3)
        if not all_nodes_released: print(f"State RELEASE timeout: {last_states_release}")
        assert all_nodes_released, f"Lock {lock_name} tidak hilang"
        print("Verifikasi state RELEASE OK.")

# TODO: Tambahkan tes HTTPS lainnya

