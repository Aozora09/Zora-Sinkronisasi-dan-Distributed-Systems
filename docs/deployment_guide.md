# Proyek Sistem Sinkronisasi Terdistribusi

Proyek ini mengimplementasikan tiga pilar sistem terdistribusi:

1.  **(Sub-tugas A)** _Distributed Lock Manager_ menggunakan algoritma konsensus **Raft**.
2.  **(Sub-tugas B)** _Distributed Queue System_ menggunakan **Consistent Hashing** dengan _backend_ Redis Streams.
3.  **(Sub-tugas C)** _Distributed Cache Coherence_ menggunakan protokol **MESI**.

Semua _service_ dibangun sebagai _microservice_ Python `asyncio` dan diorkestrasi menggunakan Docker Compose.

---

## 🚀 Panduan Deployment

Panduan ini akan memandu Anda dalam menjalankan seluruh sistem terdistribusi (Raft, Queue, dan Cache) di lingkungan lokal Anda menggunakan Docker.

### Prasyarat

- **Docker Desktop:** Pastikan Anda telah menginstal dan menjalankan Docker Desktop di sistem operasi Anda (Windows/macOS/Linux).
- **WSL 2 (Untuk Windows):** Sangat direkomendasikan untuk menggunakan backend WSL 2 untuk Docker Desktop untuk performa terbaik.
- **Git:** Untuk meng-clone repositori.
- **Terminal:** (PowerShell, Bash, atau terminal pilihan Anda).

### 1. Konfigurasi Lingkungan

Sistem ini memerlukan beberapa variabel lingkungan untuk berjalan.

1.  **Salin File `.env`:**
    Di folder _root_ proyek, salin file `env.example` menjadi file baru bernama `.env`.

    ```bash
    cp .env.example .env
    ```

2.  **Verifikasi `.env`:**
    Pastikan file `.env` Anda berisi port internal. (Anda bisa biarkan _default_-nya).
    ```env
    INTERNAL_PORT=8000
    ```

### 2. Menjalankan Sistem

Semua _service_ (Raft, Queue, Cache, Redis) diorkestrasi menggunakan `docker-compose`.

1.  **Navigasi ke Folder Docker:**
    Buka terminal dan navigasikan ke folder `docker/` di dalam proyek ini.

    ```bash
    cd docker/
    ```

2.  **Bangun (Build) dan Jalankan (Up):**
    Jalankan perintah berikut. Perintah ini akan "membersihkan" (down), "membangun ulang" (build), dan "memaksa pembuatan ulang" (force-recreate) semua kontainer untuk memastikan Anda menjalankan kode terbaru.

    _(Gunakan perintah ini untuk PowerShell)_:

    ```powershell
    docker-compose down; docker-compose up --build --force-recreate
    ```

    _(Gunakan perintah ini untuk Bash/CMD)_:

    ```bash
    docker-compose down && docker-compose up --build --force-recreate
    ```

3.  **Sistem Berjalan:**
    Terminal Anda akan menampilkan log _streaming_ dari semua 8 _service_ (3 Raft, 1 Queue, 3 Cache, 3 Redis). Anda akan melihat pemilihan _Leader_ Raft terjadi, dan setelah itu, sistem akan stabil dan siap menerima permintaan API.

### 3. Menghentikan Sistem

Untuk menghentikan dan membersihkan semua kontainer dan _network_:

1.  Tekan `Ctrl + C` di terminal `docker-compose`.
2.  Jalankan:
    ```bash
    docker-compose down
    ```

---

## 🧐 Troubleshooting

Berikut adalah beberapa _error_ umum yang mungkin Anda temui saat menjalankan atau menguji sistem.

| Error / Gejala                                                      | Penyebab                                                                                                                                                           | Solusi                                                                                                                                                                                                   |
| :------------------------------------------------------------------ | :----------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Terminal "Menggantung"** (Hanya `Attaching...` dan tidak ada log) | **Silent Crash.** Ini adalah `IndentationError` (spasi buruk) di file `.py` ATAU `bug` `Dockerfile` (misal: `CMD` bukan di akhir).                                 | 1. **Format Ulang:** Buka semua file `.py` di VS Code, `Ctrl+A`, lalu `Shift+Alt+F` untuk memformat kode.<br/>2. **Verifikasi `Dockerfile.node`:** Pastikan `CMD [...]` adalah baris _terakhir_ di file. |
| `exited with code 0` (Hanya untuk `node1`, `node2`, `node3`)        | **`CMD` Hilang.** Ini terjadi karena `Dockerfile.node` memiliki `RUN` _setelah_ `CMD`.                                                                             | Pindahkan `CMD ["python", "-m", "src.nodes.lock_manager"]` ke **baris terakhir** di `Dockerfile.node`.                                                                                                   |
| `404 Not Found` (Saat menguji `queue-node` atau `cache-node`)       | **`command:` salah.** `docker-compose.yml` gagal menimpa (override) `CMD` default. Node tersebut menjalankan `lock_manager.py` (salah) alih-alih skrip yang benar. | Perbaiki **indentasi** baris `command: python -m src.nodes.queue_node` di `docker-compose.yml` agar sejajar dengan `build:`, `ports:`, dll.                                                              |
| `500 Internal Server Error` (Saat menguji Cache)                    | **Bug Kode.** Kemungkinan besar _bug_ serialisasi (mencoba mengubah `set()` ke JSON) atau _bug_ `asyncio` (`a coroutine was expected`).                            | Pastikan `status_dict` di `cache_node.py` mengubah `set` menjadi `list()`, dan pastikan semua panggilan `asyncio.create_task` membungkus _coroutine_ yang valid.                                         |
| `Invalid URI: Invalid port specified` (Saat menguji Raft)           | **Kesalahan Pengguna.** Anda menyalin-tempel `[LEADER_PORT]` alih-alih menggantinya dengan port Leader yang sebenarnya (misal: `8001`, `8002`, atau `8003`).       | Lihat log `docker-compose up` untuk menemukan _Leader_ yang menang (misal `node2`) dan gunakan port-nya (misal `8002`) di perintah `Invoke-WebRequest` Anda.                                             |
| `BUSYGROUP Consumer Group name already exists`                      | **Bukan Error.** Ini adalah respons sukses yang memberi tahu Anda bahwa _consumer group_ sudah dibuat di pengujian sebelumnya.                                     | Anda bisa mengabaikan _error_ ini dan melanjutkan pengujian _consume_.                                                                                                                                   |
