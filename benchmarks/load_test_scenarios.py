# benchmarks/load_test_scenarios.py
import random
from locust import HttpUser, task, between

class RaftUser(HttpUser):
    # Setiap "user" simulasi akan menunggu antara 0.5 s.d. 2 detik
    # sebelum menjalankan tugas berikutnya.
    wait_time = between(0.5, 2.0)

    @task
    def acquire_and_release_lock(self):
        # Kita buat ID klien dan nama lock secara acak
        # agar tidak terjadi deadlock
        client_id = f"locust_user_{random.randint(1, 5000)}"
        lock_name = f"lock_{random.randint(1, 50)}"
        
        body = {
            "type": "ACQUIRE",
            "lock_name": lock_name,
            "client_id": client_id,
            "mode": "exclusive"
        }
        
        # Kirim request POST ke endpoint /client-request
        # 'catch_response=True' penting agar kita bisa menandai kegagalan
        with self.client.post("/client-request", json=body, catch_response=True) as response:
            if response.status_code == 200:
                # Jika sukses (200 OK), tandai sebagai sukses
                response.success()
            else:
                # Jika gagal (misal: 400 Bad Request karena kena Follower),
                # tandai sebagai kegagalan
                response.failure(f"Gagal, status: {response.status_code}")