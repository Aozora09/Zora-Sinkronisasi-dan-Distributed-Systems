# Panduan Deployment: Sistem Sinkronisasi Terdistribusi

Proyek ini mengimplementasikan tiga pilar sistem terdistribusi:

1.  **(Sub-tugas A)** _Distributed Lock Manager_ menggunakan algoritma konsensus **Raft**.
2.  **(Sub-tugas B)** _Distributed Queue System_ menggunakan **Consistent Hashing** dengan _backend_ Redis Streams.
3.  **(Sub-tugas C)** _Distributed Cache Coherence_ menggunakan protokol **MESI**.

Semua _service_ dibangun sebagai _microservice_ Python `asyncio`, diamankan dengan **HTTPS/TLS**, dan diorkestrasi menggunakan Docker Compose.

---

## 🚀 Panduan Deployment

Panduan ini akan memandu Anda dalam menjalankan seluruh sistem terdistribusi (Raft, Queue, dan Cache) di lingkungan lokal Anda menggunakan Docker.

### Prasyarat

* **Docker Desktop:** Pastikan Anda telah menginstal dan menjalankan Docker Desktop.
* **WSL 2 (Untuk Windows):** Sangat direkomendasikan untuk menggunakan backend WSL 2 untuk Docker Desktop.
* **Git:** Untuk meng-clone repositori.
* **Terminal:** (PowerShell, Bash, CMD, dll.).
* **OpenSSL (atau Docker):** Diperlukan **satu kali** untuk membuat sertifikat TLS. Anda bisa menginstal OpenSSL atau menggunakan perintah `docker run ... openssl ...` (lihat bagian "Membuat Sertifikat").

### 1. Setup Awal

1.  **Clone Repositori:**
    ```bash
    git clone <URL_REPOSITORI_ANDA>
    cd <NAMA_FOLDER_PROYEK>
    ```

2.  **Konfigurasi Lingkungan (`.env`):**
    * Di folder _root_ proyek, salin file `env.example` menjadi `.env`.
        ```bash
        cp .env.example .env
        ```
    * Buka file `.env` dan pastikan isinya sesuai. Kunci `SHARED_MAC_KEY` digunakan untuk otentikasi pesan Raft.
        ```dotenv
        # Port internal yang akan didengarkan oleh server Python (HTTPS)
        INTERNAL_PORT=8000

        # Kunci rahasia bersama untuk MAC (minimal 16 karakter)
        SHARED_MAC_KEY=mysecretpresharedkey123456
        ```

3.  **Membuat Sertifikat TLS (Lakukan Sekali):**
    * Buat folder `certs` di _root_ proyek: `mkdir certs`.
    * **Gunakan Docker untuk membuat sertifikat** (tidak perlu instal OpenSSL di host):
        * Buka terminal di **root** proyek.
        * **Buat CA:**
            ```powershell
            # PowerShell
            docker run --rm -v "${PWD}/certs:/certs" python:3.10-slim openssl genpkey -algorithm RSA -out /certs/ca.key
            docker run --rm -v "${PWD}/certs:/certs" -it python:3.10-slim openssl req -new -x509 -key /certs/ca.key -out /certs/ca.crt -days 365 -subj "/CN=MyLocalCA/O=DistributedSystem/C=ID"
            ```
            *(Ganti `${PWD}` dengan `%cd%` jika menggunakan CMD)*.
        * **Buat Sertifikat Node (ulangi 7x):** Ganti `node1` dengan nama node lain (`node2`, `node3`, `queue-node-1`, `cache-node-1`, `cache-node-2`, `cache-node-3`).
            ```powershell
            # PowerShell Example for node1
            $env:NODE_NAME="node1"
            docker run --rm -v "${PWD}/certs:/certs" python:3.10-slim openssl genpkey -algorithm RSA -out /certs/$env:NODE_NAME.key
            docker run --rm -v "${PWD}/certs:/certs" -it python:3.10-slim openssl req -new -key /certs/$env:NODE_NAME.key -out /certs/$env:NODE_NAME.csr -subj "/CN=$env:NODE_NAME/O=DistributedSystem/C=ID"
            docker run --rm -v "${PWD}/certs:/certs" python:3.10-slim openssl x509 -req -in /certs/$env:NODE_NAME.csr -CA /certs/ca.crt -CAkey /certs/ca.key -CAcreateserial -out /certs/$env:NODE_NAME.crt -days 365
            ```
            *(Gunakan `set NODE_NAME=node1` dan `%NODE_NAME%` jika menggunakan CMD)*.
    * Setelah selesai, folder `certs` akan berisi `ca.crt`, `ca.key`, dan pasangan `.crt`/`.key` untuk setiap node.

### 2. Menjalankan Sistem

Semua _service_ (Raft, Queue, Cache, Redis) diorkestrasi menggunakan file `docker-compose.yml` yang ada di folder `docker/`.

1.  **Navigasi ke Folder Docker:**
    Buka terminal dan navigasikan ke folder `docker/`.
    ```bash
    cd docker/
    ```

2.  **Bangun (Build) dan Jalankan (Up):**
    Jalankan perintah berikut. Ini akan membangun _image_ Docker (menggunakan `Dockerfile.node` dan menyalin sertifikat), lalu memulai semua kontainer. Opsi `--force-recreate` memastikan konfigurasi terbaru diterapkan.
    ```bash
    # PowerShell (titik koma)
    docker compose down; docker compose up --build --force-recreate -d
    ```
    ```bash
    # Bash/CMD (&&)
    docker compose down && docker compose up --build --force-recreate -d
    ```
    *(Opsi `-d` menjalankan kontainer di _background_)*.

3.  **Melihat Log (Opsional):**
    Untuk melihat _output log_ dari semua _service_ (termasuk pemilihan _leader_ Raft dan log audit):
    ```bash
    docker compose logs -f
    ```
    Tekan `Ctrl + C` untuk keluar dari mode _log streaming_.

4.  **Sistem Berjalan:**
    Setelah beberapa saat (sekitar 10-20 detik untuk pemilihan _leader_ Raft), sistem akan stabil dan siap menerima permintaan API melalui **HTTPS** pada port yang dipetakan di `docker-compose.yml`:
    * Lock Manager (Raft): `https://localhost:8001`, `https://localhost:8002`, `https://localhost:8003`
    * Queue Node: `https://localhost:9101`
    * Cache Nodes: `https://localhost:10001`, `https://localhost:10002`, `https://localhost:10003`

    *(Saat mengakses dari browser atau `curl`, Anda mungkin perlu menerima peringatan keamanan karena sertifikatnya _self-signed_ atau menggunakan flag seperti `curl -k` / `Invoke-WebRequest -SkipCertificateCheck`)*.

### 3. Menghentikan Sistem

Untuk menghentikan dan membersihkan semua kontainer dan _network_:

1.  Pastikan Anda berada di folder `docker/`.
2.  Jalankan:
    ```bash
    docker compose down
    ```

---

## 🧐 Troubleshooting

Berikut adalah beberapa _error_ umum dan solusinya:

| Error / Gejala                                                              | Penyebab                                                                                                                                                    | Solusi                                                                                                                                                                                                            |
| :-------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Terminal "Menggantung"** (Hanya `Attaching...`)                            | **Silent Crash.** Biasanya `IndentationError` di file `.py` atau `Dockerfile` salah (misal: `CMD` bukan di akhir).                                          | 1. **Format Ulang:** Gunakan `Shift+Alt+F` di VS Code pada file `.py`.<br/>2. **Verifikasi `Dockerfile`:** Pastikan `CMD [...]` adalah baris _terakhir_.                                                     |
| Kontainer keluar dengan `code 0` atau `code 1` segera setelah start         | **`CMD` atau `command:` salah.** Bisa karena `CMD` hilang/tertumpuk di Dockerfile, atau `command:` di `docker-compose.yml` salah path/modul.                  | 1. Pindahkan `CMD [...]` ke akhir `Dockerfile`.<br/>2. Pastikan `command: python -m src.nodes.nama_modul` di `docker-compose.yml` benar dan sejajar indentasinya.                                               |
| `404 Not Found` (Saat akses `/queue/...` atau `/cache/...`)                 | **`command:` override gagal.** Node tersebut salah menjalankan `lock_manager.py` (default `CMD`) bukan skripnya sendiri.                                       | Perbaiki **indentasi** baris `command: ...` di `docker-compose.yml` agar sejajar dengan `build:`, `ports:`, dll.                                                                                        |
| `SSLError`, `CERTIFICATE_VERIFY_FAILED`, `Hostname mismatch` (di log/client) | **Masalah Sertifikat/TLS.** Bisa karena file sertifikat tidak ditemukan, CA tidak dipercaya, atau nama host tidak cocok (misal: akses `localhost` tapi cert untuk `node1`). | 1. Pastikan folder `certs` lengkap dan disalin dengan benar di `Dockerfile`.<br/>2. Pastikan path `CERT_FILE`, `KEY_FILE`, `CA_FILE` benar di `docker-compose.yml`.<br/>3. Saat tes/akses dari luar, gunakan opsi `-k` (curl) / `-SkipCertificateCheck` (PS) atau konfigurasi *client* agar percaya CA / menonaktifkan *hostname check*. |
| `Connection Refused` atau `Timeout` (Saat akses API)                       | **Kontainer belum siap/crash.** Server aiohttp belum selesai start, atau kontainer crash sebelum server siap.                                                  | 1. Tunggu lebih lama setelah `docker compose up`.<br/>2. Periksa log spesifik kontainer (`docker compose logs nama_service`) untuk melihat *error* saat startup.                                                    |
| `BUSYGROUP Consumer Group name already exists`                              | **Bukan Error.** Respons normal Redis jika grup sudah dibuat sebelumnya.                                                                               | Abaikan pesan ini dan lanjutkan pengujian _consume_.                                                                                                                                                              |