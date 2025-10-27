# Sistem Sinkronisasi Terdistribusi

Proyek ini adalah implementasi sistem terdistribusi skala kecil yang ditulis dalam Python `asyncio` dan diorkestrasi menggunakan Docker Compose. Sistem ini memenuhi semua *Core Requirements* dari tugas, termasuk:

* **Sub-tugas A:** *Distributed Lock Manager* menggunakan implementasi algoritma konsensus **Raft** dari awal.
* **Sub-tugas B:** *Distributed Queue System* menggunakan **Consistent Hashing** dengan *backend* Redis Streams.
* **Sub-tugas C:** *Distributed Cache Coherence* menggunakan simulasi protokol **MESI**.
* **Sub-tugas D:** *Containerization* dari semua *microservice* menggunakan **Docker** dan **Docker Compose**.

---

## 📚 Dokumentasi Lengkap

Dokumentasi teknis lengkap dan laporan performa dapat ditemukan di dalam folder `docs/` dan `benchmarks/`.

* **[Arsitektur Sistem & Penjelasan Algoritma (docs/architecture.md)](docs/architecture.md)**
    * Termasuk diagram arsitektur Mermaid dan penjelasan rinci tentang implementasi Raft, Consistent Hashing, dan MESI.

* **[Dokumentasi API - OpenAPI 3.0 (docs/api_spec.yaml)](docs/api_spec.yaml)**
    * Spesifikasi OpenAPI/Swagger lengkap yang mendefinisikan semua *endpoint* HTTP untuk *service* Lock Manager, Queue, dan Cache.

* **[Laporan Analisis Performa (report.pdf)](report.pdf)**
    * Laporan PDF yang berisi *gambar diagram*, *benchmark* performa `locust`, membandingkan *throughput* dan *latency* dari skenario *single-node* vs. *distributed 3-node*.

---

## 🚀 Panduan Deployment

Panduan ini akan memandu Anda dalam menjalankan seluruh sistem terdistribusi (Raft, Queue, dan Cache) di lingkungan lokal Anda menggunakan Docker.

### Prasyarat

* **Docker Desktop:** Pastikan Anda telah menginstal dan menjalankan Docker Desktop.
* **WSL 2 (Untuk Windows):** Sangat direkomendasikan untuk menggunakan backend WSL 2.
* **Terminal:** (PowerShell atau Bash).

### 1. Konfigurasi Lingkungan

Sistem ini memerlukan beberapa variabel lingkungan untuk berjalan.

1.  **Salin File `.env`:**
    Di folder *root* proyek ini, salin file `env.example` menjadi file baru bernama `.env`.
    ```bash
    cp .env.example .env
    ```

2.  **Verifikasi `.env`:**
    Pastikan file `.env` Anda berisi port internal *default*:
    ```env
    INTERNAL_PORT=8000
    ```

### 2. Menjalankan Sistem

Semua 11 *service* (3 Raft, 1 Queue, 3 Cache, 3 Redis, 1 Locust) diorkestrasi menggunakan `docker-compose`.

1.  **Navigasi ke Folder Docker:**
    Buka terminal dan navigasikan ke folder `docker/`.
    ```bash
    cd docker/
    ```

2.  **Bangun (Build) dan Jalankan (Up):**
    Jalankan perintah berikut untuk membersihkan, membangun ulang, dan menjalankan semua kontainer.

    *(Gunakan perintah ini untuk PowerShell)*:
    ```powershell
    docker-compose down; docker-compose up --build --force-recreate
    ```

    *(Gunakan perintah ini untuk Bash/CMD)*:
    ```bash
    docker-compose down && docker-compose up --build --force-recreate
    ```

3.  **Sistem Berjalan:**
    Terminal Anda akan menampilkan log *streaming* dari semua *service*. Anda akan melihat pemilihan *Leader* Raft terjadi, dan setelah itu, sistem akan stabil dan siap menerima permintaan API.

### 3. Menghentikan Sistem

Untuk menghentikan dan membersihkan semua kontainer dan *network*:
1.  Tekan `Ctrl + C` di terminal `docker-compose`.
2.  Jalankan:
    ```bash
    docker-compose down
    ```

---

## 🧐 Panduan Troubleshooting

Berikut adalah beberapa *error* umum yang mungkin Anda temui:

| Error / Gejala | Penyebab | Solusi |
| :--- | :--- | :--- |
| **Terminal "Menggantung"** (Hanya `Attaching...` dan tidak ada log) | **Silent Crash.** Ini adalah `IndentationError` (spasi buruk) di file `.py` ATAU `bug` `Dockerfile` (misal: `CMD` bukan di akhir). | 1. **Format Ulang:** Buka semua file `.py` di VS Code (`Ctrl+A` -> `Shift+Alt+F`).<br/>2. **Verifikasi `Dockerfile.node`:** Pastikan `CMD [...]` adalah baris *terakhir* di file. |
| `exited with code 0` (Hanya untuk `node1`, `node2`, `node3`) | **`CMD` Hilang.** Ini terjadi karena `Dockerfile.node` memiliki `RUN` *setelah* `CMD`. | Pindahkan `CMD ["python", "-m", "src.nodes.lock_manager"]` ke **baris terakhir** di `Dockerfile.node`. |
| `404 Not Found` (Saat menguji `queue-node` atau `cache-node`) | **`command:` salah.** `docker-compose.yml` gagal menimpa (override) `CMD` default. Node tersebut menjalankan `lock_manager.py` (salah) alih-alih skrip yang benar. | Perbaiki **indentasi** baris `command: python -m src.nodes.queue_node` di `docker-compose.yml` agar sejajar dengan `build:`, `ports:`, dll. |
| `500 Internal Server Error` (Saat menguji Cache) | **Bug Kode.** Kemungkinan besar *bug* serialisasi (mencoba mengubah `set()` ke JSON) atau *bug* `asyncio` (`a coroutine was expected`). | Pastikan `status_dict` di `cache_node.py` mengubah `set` menjadi `list()`, dan pastikan semua panggilan `asyncio.create_task` membungkus *coroutine* yang valid. |
| `Invalid URI: Invalid port specified` (Saat menguji Raft) | **Kesalahan Pengguna.** Anda menyalin-tempel `[LEADER_PORT]` alih-alih menggantinya dengan port Leader yang sebenarnya (misal: `8001`, `8002`, atau `8003`). | Lihat log `docker-compose up` untuk menemukan *Leader* yang menang (misal `node2`) dan gunakan port-nya (misal `8002`) di perintah `Invoke-WebRequest` Anda. |
| `BUSYGROUP Consumer Group name already exists` | **Bukan Error.** Ini adalah respons sukses yang memberi tahu Anda bahwa *consumer group* sudah dibuat di pengujian sebelumnya. | Anda bisa mengabaikan *error* ini dan melanjutkan pengujian *consume*. |