import pytest
import asyncio
import aiohttp
import ssl # <-- Import ssl
import time
import uuid
import os
import json # <-- Import json

# Tandai semua tes di file ini sebagai asyncio
pytestmark = pytest.mark.asyncio

# Alamat node queue (HTTPS)
QUEUE_NODE_PORT = os.getenv("QUEUE_NODE_PORT", "9101")
QUEUE_NODE_URL = f"https://localhost:{QUEUE_NODE_PORT}"

# Path ke CA certificate (relatif dari root proyek)
# Pastikan file ini ada di root proyek Anda, BUKAN di dalam folder docker/
CA_CERT_PATH = "certs/ca.crt"

# --- Helper Functions (Tetap sama) ---
async def produce_message(session: aiohttp.ClientSession, queue_name: str, message: dict) -> tuple[int, dict | str]:
    payload = {"queue_name": queue_name, "message": message}
    try:
        async with session.post(f"{QUEUE_NODE_URL}/queue/produce", json=payload, timeout=5) as resp:
            print(f"PRODUCE to {queue_name}: Status {resp.status}")
            response_body = await resp.json() if resp.content_type == 'application/json' else await resp.text()
            print(f"  Response: {response_body}")
            return resp.status, response_body
    except Exception as e:
        print(f"Error saat PRODUCE ke {queue_name}: {e}")
        return 500, str(e)

async def create_group(session: aiohttp.ClientSession, queue_name: str, group_name: str) -> int:
    payload = {"queue_name": queue_name, "group_name": group_name}
    last_status = 500
    try:
        for _ in range(3):
            async with session.post(f"{QUEUE_NODE_URL}/queue/create-group", json=payload, timeout=3) as resp:
                last_status = resp.status
                print(f"CREATE GROUP {group_name} for {queue_name}: Status {resp.status}")
                if resp.status == 200 or resp.status == 409:
                    print(f"  Response: {await resp.text()}")
                    return resp.status
            await asyncio.sleep(0.5)
        return last_status
    except Exception as e:
        print(f"Error saat CREATE GROUP {group_name}: {e}")
        return 500

async def consume_message(session: aiohttp.ClientSession, queue_name: str, group_name: str, consumer_id: str) -> tuple[int, dict | str | None]:
    payload = {"queue_name": queue_name, "group_name": group_name, "consumer_id": consumer_id}
    try:
        async with session.post(f"{QUEUE_NODE_URL}/queue/consume", json=payload, timeout=7) as resp:
            print(f"CONSUME from {queue_name} by {consumer_id}: Status {resp.status}")
            if resp.status == 200:
                result = await resp.json()
                print(f"  Response: {result}")
                return resp.status, result
            else:
                body = await resp.text()
                print(f"  Response Body: {body}")
                return resp.status, body
    except Exception as e:
        print(f"Error saat CONSUME from {queue_name}: {e}")
        return 500, str(e)
# --- Akhir Helper Functions ---

# --- Test Cases ---
async def test_queue_produce_consume_flow():
    """Tes alur dasar: produce -> create group -> consume (via HTTPS)."""
    queue_name = f"integration_queue_{int(time.time())}"
    group_name = "test_group_https"
    consumer_id = "consumer_https_1"
    message_content = {"data": f"test_message_{uuid.uuid4()}"}

    # --- Konfigurasi SSL Context untuk Client Test ---
    ssl_context = None
    try:
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=CA_CERT_PATH)
        # --- PERBAIKAN: Nonaktifkan pemeriksaan hostname ---
        ssl_context.check_hostname = False
        # ---------------------------------------------------
        print(f"SSL Context untuk tes dibuat, CA: {CA_CERT_PATH}, Hostname Check: OFF")
    except FileNotFoundError:
        pytest.fail(f"File CA certificate tidak ditemukan di {CA_CERT_PATH}.")
    except Exception as e:
        pytest.fail(f"Gagal membuat SSL context untuk tes: {e}")
    # ---------------------------------------------

    # --- Buat ClientSession DENGAN SSL Context ---
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        # -----------------------------------------

        # 1. Produce message
        status_prod, body_prod = await produce_message(session, queue_name, message_content)
        assert status_prod == 200, f"Produce gagal: {body_prod}" # Tampilkan body jika gagal
        assert isinstance(body_prod, dict)
        assert body_prod.get("status") == "ok"
        message_id = body_prod.get("message_id")
        assert message_id is not None

        await asyncio.sleep(0.1)

        # 2. Create consumer group (boleh gagal jika sudah ada)
        status_group = await create_group(session, queue_name, group_name)
        assert status_group in [200, 409] # OK or Conflict

        # 3. Consume message
        status_cons, body_cons = await consume_message(session, queue_name, group_name, consumer_id)
        assert status_cons == 200, f"Consume gagal: {body_cons}" # Tampilkan body jika gagal
        assert isinstance(body_cons, dict)
        assert body_cons.get("status") == "ok"
        consumed_messages = body_cons.get("messages", [])
        assert len(consumed_messages) == 1
        assert consumed_messages[0].get("message_id") == message_id
        assert consumed_messages[0].get("message") == message_content

        # 4. Coba consume lagi (seharusnya kosong)
        status_cons_empty, body_cons_empty = await consume_message(session, queue_name, group_name, consumer_id)
        assert status_cons_empty == 200, f"Consume empty gagal: {body_cons_empty}" # Tampilkan body jika gagal
        assert isinstance(body_cons_empty, dict)
        assert body_cons_empty.get("status") == "empty"

# TODO: Tambahkan tes HTTPS lainnya (multiple consumers, distribution)

