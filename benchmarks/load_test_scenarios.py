import random
from locust import HttpUser, task, between, events # Import events
import logging # Import logging

# Setup basic logging for Locust messages if needed
# logging.basicConfig(level=logging.INFO)

@events.request.add_listener
def handle_request_failure(request_type, name, response_time, response_length, response,
                           context, exception, start_time, url, **kwargs):
    """
    Listener untuk mencatat detail saat request gagal (terutama status 0).
    """
    if exception:
        # Log exception jika ada (misal: ConnectionError, SSLError, Timeout)
        logging.error(f"Request {name} FAILED due to exception: {exception}")
        # Tandai sebagai failure di statistik Locust (seharusnya sudah otomatis, tapi untuk safety)
        if response:
             response.failure(f"Exception: {exception}")
    elif response and response.status_code == 0:
        # Jika status 0, log sebagai connection issue
        logging.error(f"Request {name} FAILED with status 0 (Connection/Network issue?)")
        # Tandai sebagai failure
        response.failure("Status 0 - Connection/Network issue")
    # Anda bisa menambahkan penanganan status code lain di sini jika perlu

class RaftUser(HttpUser):
    """
    User simulasi untuk menguji Raft Lock Manager via HTTPS.
    """
    # Setiap "user" simulasi akan menunggu antara 0.5 s.d. 2 detik
    # sebelum menjalankan tugas berikutnya.
    wait_time = between(0.5, 2.0)
    # Atribut host bisa diset di sini atau via command line --host
    # host = "https://localhost:8001" # Contoh jika diset di kode

    @task
    def acquire_and_release_lock(self):
        """
        Mensimulasikan user mencoba acquire lock eksklusif, lalu release.
        """
        # Kita buat ID klien dan nama lock secara acak
        # agar tidak terjadi deadlock palsu dan mendistribusikan beban
        client_id = f"locust_user_{random.randint(1, 50000)}" # Range lebih besar
        lock_name = f"lock_{random.randint(1, 100)}" # Lebih banyak variasi lock

        # --- Request ACQUIRE ---
        acquire_body = {
            "type": "ACQUIRE",
            "lock_name": lock_name,
            "client_id": client_id,
            "mode": "exclusive"
        }

        acquired_successfully = False
        logging.debug(f"User {client_id}: Attempting ACQUIRE {lock_name}") # Log Debug
        # Kirim request POST ke endpoint /client-request
        # 'catch_response=True' penting agar kita bisa menandai kegagalan
        # 'verify=False' untuk melewati verifikasi SSL (hostname mismatch)
        with self.client.post(
            "/client-request",
            json=acquire_body,
            catch_response=True,
            verify=False # <-- Lewati verifikasi SSL
        ) as response:
            logging.debug(f"User {client_id}: ACQUIRE {lock_name} status {response.status_code}") # Log Debug status
            if response.status_code == 200:
                try:
                    # Cek isi response jika perlu
                    # result = response.json()
                    # logging.debug(f"User {client_id}: ACQUIRE {lock_name} success response: {result}")
                    response.success()
                    acquired_successfully = True # Tandai untuk release
                except Exception as e:
                     # Jika response 200 tapi JSON error?
                     logging.error(f"User {client_id}: ACQUIRE {lock_name} - JSON decode error on 200 OK: {e}. Body: {response.text}")
                     response.failure(f"JSON decode error on 200 OK: {e}")

            # Handle redirect (400) jika kena follower
            elif response.status_code == 400:
                 try:
                      error_data = response.json()
                      if "leader_hint" in error_data:
                           logging.warning(f"User {client_id}: ACQUIRE {lock_name} hit follower. Leader hint: {error_data.get('leader_id')}")
                           # Anggap failure agar Locust tahu ada masalah routing/target
                           response.failure(f"Hit Follower, Leader: {error_data.get('leader_id')}")
                      else:
                           # Mungkin error 400 lain (misal deadlock)
                           logging.error(f"User {client_id}: ACQUIRE {lock_name} failed (400): {response.text}")
                           response.failure(f"Acquire Failed (400): {response.text}")
                 except Exception:
                      # Jika response 400 tapi bukan JSON
                      logging.error(f"User {client_id}: ACQUIRE {lock_name} failed (400, Non-JSON): {response.text}")
                      response.failure(f"Acquire Failed (400, Non-JSON): {response.text}")
            # Handle timeout/server error (5xx)
            elif response.status_code >= 500:
                 logging.error(f"User {client_id}: ACQUIRE {lock_name} failed ({response.status_code}): {response.text}")
                 response.failure(f"Acquire Failed ({response.status_code}): {response.text}")

            else:
                # Handle status code lain yang tidak diharapkan
                logging.error(f"User {client_id}: ACQUIRE {lock_name} unexpected status: {response.status_code}. Body: {response.text}")
                response.failure(f"Unexpected status: {response.status_code}")

        # --- Request RELEASE (Hanya jika ACQUIRE berhasil) ---
        if acquired_successfully:
            # Beri jeda sedikit sebelum release (opsional, simulasi work time)
            # time.sleep(random.uniform(0.1, 0.5))

            release_body = {
                "type": "RELEASE",
                "lock_name": lock_name,
                "client_id": client_id
            }
            logging.debug(f"User {client_id}: Attempting RELEASE {lock_name}") # Log Debug
            # Kirim request RELEASE
            with self.client.post(
                "/client-request",
                json=release_body,
                catch_response=True,
                verify=False # <-- Lewati verifikasi SSL
            ) as release_response:
                logging.debug(f"User {client_id}: RELEASE {lock_name} status {release_response.status_code}") # Log Debug status
                if release_response.status_code == 200:
                    release_response.success()
                # Handle error (misal: 400 jika leader berganti atau lock tidak dipegang?)
                elif release_response.status_code == 400:
                    logging.error(f"User {client_id}: RELEASE {lock_name} failed (400): {release_response.text}")
                    release_response.failure(f"Release Failed (400): {release_response.text}")
                elif release_response.status_code >= 500:
                    logging.error(f"User {client_id}: RELEASE {lock_name} failed ({release_response.status_code}): {release_response.text}")
                    release_response.failure(f"Release Failed ({release_response.status_code}): {release_response.text}")
                else:
                    logging.error(f"User {client_id}: RELEASE {lock_name} unexpected status: {release_response.status_code}. Body: {release_response.text}")
                    release_response.failure(f"Unexpected status: {release_response.status_code}")

# --- (Opsional) Menjalankan Locust dari script jika diinginkan ---
# if __name__ == "__main__":
#     import os
#     # Contoh menjalankan via script (lebih jarang dipakai)
#     # Anda perlu menginstal locust sebagai library
#     # os.system("locust -f benchmarks/load_test_scenarios.py --host https://localhost:8001 --web-host 127.0.0.1")
#     pass
