# Arsitektur Sistem Sinkronisasi Terdistribusi

Sistem ini dirancang sebagai arsitektur _microservice_ yang berjalan di dalam satu jaringan Docker (`raft-net`). Setiap _service_ utama (Lock Manager, Queue, Cache) berjalan sebagai kontainer independen, mengimplementasikan algoritma terdistribusi yang berbeda untuk memenuhi persyaratan tugas. **Komunikasi antar _service_ Python diamankan menggunakan HTTPS/TLS.**

---

## Diagram Arsitektur

Diagram ini menunjukkan 9 _service_ yang berjalan secara bersamaan dan bagaimana mereka berinteraksi.

- **Sub-tugas A (Biru):** Cluster Raft 3-node untuk _Distributed Lock_.
- **Sub-tugas B (Hijau/Merah):** 1 _Queue Router_ yang menggunakan _Consistent Hashing_ untuk mempartisi data ke 3 _node_ Redis.
- **Sub-tugas C (Kuning):** Cluster Cache 3-node yang mengimplementasikan koherensi MESI.

code mermaid diagram:

```mermaid
graph TD
    subgraph "Client (Pengguna - PowerShell)"
        U(User / Invoke-WebRequest)
    end

    subgraph "Docker Network (raft-net)"

        %% Sub-tugas A: Raft
        subgraph "Sub-tugas A: Raft Distributed Lock Manager (Port 8001-8003)"
            direction LR
            N1("Node Raft (node1:8001)")
            N2("Node Raft (node2:8002)")
            N3("Node Raft (node3:8003)")

            %% Komunikasi Raft
            N1 -- "Raft RPC (/vote, /append-entries)" --- N2
            N2 -- "Raft RPC (/vote, /append-entries)" --- N3
            N3 -- "Raft RPC (/vote, /append-entries)" --- N1
        end

        %% Sub-tugas B: Queue
        subgraph "Sub-tugas B: Distributed Queue System (Port 9101)"
            direction TB
            QN("Queue Router (queue-node-1:9101)")

            subgraph "Penyimpanan Redis (Persistence)"
                direction LR
                R1("Redis 1 (redis-1)")
                R2("Redis 2 (redis-2)")
                R3("Redis 3 (redis-3)")
            end

            %% Komunikasi Queue
            QN -- "Consistent Hashing (Perintah XADD)" --> R1
            QN -- "Consistent Hashing (Perintah XADD)" --> R2
            QN -- "Consistent Hashing (Perintah XADD)" --> R3
        end

        %% Sub-tugas C: Cache
        subgraph "Sub-tugas C: Distributed Cache Coherence (Port 10001-10003)"
            direction LR
            C1("Cache Node 1 (cache-node-1:10001)")
            C2("Cache Node 2 (cache-node-2:10002)")
            C3("Cache Node 3 (cache-node-3:10003)")

            %% Komunikasi MESI Bus
            C1 -- "MESI Bus (/bus/invalidate, /bus/read)" --- C2
            C2 -- "MESI Bus (/bus/invalidate, /bus/read)" --- C3
            C3 -- "MESI Bus (/bus/invalidate, /bus/read)" --- C1
        end
    end

    %% Tautan Komunikasi Client ke API
    U -- "API Call: /client-request" --> N1
    U -- "API Call: /client-request" --> N2
    U -- "API Call: /client-request" --> N3

    User -- "API Call: /queue/produce" --> QN

    User -- "API Call: /cache/{key}" --> C1
    User -- "API Call: /cache/{key}" --> C2
    User -- "API Call: /cache/{key}" --> C3

    %% Styling
    style User fill:#f8f9fa,stroke:#333,stroke-width:2px
    style N1 fill:#cce5ff,stroke:#333,stroke-width:2px
    style N2 fill:#cce5ff,stroke:#333,stroke-width:2px
    style N3 fill:#cce5ff,stroke:#333,stroke-width:2px
    style QN fill:#d4edda,stroke:#333,stroke-width:2px
    style R1 fill:#f8d7da,stroke:#333,stroke-width:2px
    style R2 fill:#f8d7da,stroke:#333,stroke-width:2px
    style R3 fill:#f8d7da,stroke:#333,stroke-width:2px
    style C1 fill:#fff3cd,stroke:#333,stroke-width:2px
    style C2 fill:#fff3cd,stroke:#333,stroke-width:2px
    style C3 fill:#fff3cd,stroke:#333,stroke-width:2px
end
```

---

## Penjelasan Algoritma & Implementasi

Sistem ini terdiri dari tiga _microservice_ utama yang masing-masing mengimplementasikan algoritma sistem terdistribusi yang berbeda, ditambah lapisan keamanan dasar.

### 1. Sub-tugas A: Raft Distributed Lock Manager

_Service_ ini adalah _state machine_ terdistribusi yang mengelola _shared_ dan _exclusive lock_. Untuk memastikan semua node (replika) setuju pada _state_ (status) _lock_ yang sama, saya mengimplementasikan algoritma konsensus **Raft** dari awal, berdasarkan paper "In Search of an Understandable Consensus Algorithm" [cite: 601-1640].

Logika Raft memecah masalah konsensus menjadi tiga bagian:

1.  **Leader Election:** Sistem dimulai dengan semua node sebagai `Follower`. Setiap `Follower` memiliki _timer_ pemilihan acak [cite: 911-912]. Jika tidak menerima _heartbeat_ (`AppendEntries` kosong) dari `Leader` sebelum _timer_-nya habis, ia menjadi `Candidate` [cite: 896-897], menaikkan _term_, dan meminta suara (`RequestVote` RPC) dari _peer_. Jika ia menerima suara dari mayoritas, ia menjadi `Leader`.
2.  **Log Replication:** Hanya `Leader` yang menangani permintaan _client_. Perintah ditambahkan ke log `Leader` dan direplikasi ke _Follower_ melalui `AppendEntries` RPC.
3.  **Safety & Commit:** Entri log dianggap **committed** setelah direplikasi ke mayoritas _server_. **Election Restriction** memastikan `Leader` baru memiliki semua entri yang sudah di-_commit_ [cite: 1083, 1086-1087].

_State machine_ kemudian menerapkan log yang sudah di-_commit_ secara berurutan, mengelola status _lock_ (`exclusive`/`shared`, `owners`) dan antrian tunggu (`wait_queue`). **Deteksi Deadlock** diimplementasikan di `Leader` sebelum menambahkan perintah `ACQUIRE` ke log, menggunakan analisis _wait-for-graph_ untuk menolak permintaan yang akan menyebabkan siklus tunggu.

### 2. Sub-tugas B: Distributed Queue (Consistent Hashing)

_Service_ ini berfungsi sebagai _router_ yang menerima _request_ API (produce/consume) dan meneruskannya ke _node_ Redis yang sesuai berdasarkan **Consistent Hashing**.

1.  **"Hash Ring":** Ketiga _node_ Redis (`redis-1` hingga `redis-3`) dipetakan ke beberapa titik pada sebuah "cincin" virtual menggunakan _hash_ dari nama _node_ ditambah _suffix replica_.
2.  **Perutean Pesan:** Saat _request_ `produce` untuk `queue_name` diterima, _hash_ dari `queue_name` dihitung. _Node_ Redis pertama yang ditemukan searah jarum jam dari posisi _hash_ tersebut di cincin menjadi target penyimpanan pesan.
3.  **Toleransi Kegagalan (Redis):** Jika satu _node_ Redis gagal, _consistent hashing_ memastikan hanya kunci yang sebelumnya dipetakan ke _node_ tersebut yang akan didistribusikan ulang ke _node_ berikutnya di cincin, meminimalkan gangguan.
4.  **At-Least-Once Delivery & Persistence:** Penggunaan **Redis Streams** (`XADD`, `XGROUP CREATE`, `XREADGROUP`) oleh _service_ ini menyediakan persistensi pesan (ditangani oleh Redis) dan mekanisme dasar untuk jaminan pengiriman setidaknya sekali melalui _consumer groups_.

### 3. Sub-tugas C: Cache Coherence (MESI)

_Service_ ini mensimulasikan cluster _cache_ terdistribusi (3 _node_) yang menjaga koherensi data menggunakan variasi protokol **MESI**.

Setiap _node_ (`cache-node-1` hingga `cache-node-3`) menyimpan data dalam _cache_ LRU lokal. Setiap entri _cache_ memiliki _state_: **M**odified, **E**xclusive, **S**hared, atau **I**nvalid. Komunikasi antar _node_ untuk menjaga koherensi terjadi melalui "bus" internal (HTTPS `POST /bus/invalidate/{key}` dan `GET /bus/read/{key}`).

**Alur Kerja Utama:**

- **PrRd (Processor Read):** Saat `GET /cache/{key}` diterima:
  - Jika _hit_ lokal (state M, E, S): Data dikembalikan.
  - Jika _miss_ lokal atau state I: Node mengirim `BusRd` (via HTTPS) ke semua _peer_.
    - Jika _peer_ merespons dengan data: _Peer_ pengirim mengubah state-nya ke S. Node penerima memuat data dengan state S.
    - Jika tidak ada _peer_ yang merespons: Node mengambil data dari "memori utama" (simulasi) dan memuatnya dengan state E.
- **PrWr (Processor Write):** Saat `POST /cache/{key}` diterima:
  - Node mengirim `BusInvalidate` (via HTTPS) ke semua _peer_.
  - Node menulis data baru ke _cache_ lokal dan menyetel state ke M.
- **BusRd Response:** Node yang menerima `BusRd` dan memiliki data valid (M, E, S) akan mengirimkan data kembali dan mengubah state lokalnya ke S.
- **BusInvalidate Response:** Node yang menerima `BusInvalidate` akan mengubah state kunci yang relevan menjadi I.
- **LRU Replacement:** Jika _cache_ penuh saat data baru perlu dimasukkan, entri yang paling lama tidak diakses akan dikeluarkan.

### 4. Keamanan (Fitur Bonus)

Lapisan keamanan dasar telah ditambahkan untuk meningkatkan robustitas sistem:

1.  **Enkripsi Komunikasi (HTTPS/TLS):**

    - Seluruh komunikasi berbasis HTTP antar _node_ Python (Raft RPC, Cache Bus) dan antara _client_ eksternal ke API _endpoint_ kini dienkripsi menggunakan TLS (HTTPS).
    - Sertifikat X.509 _self-signed_ dibuat untuk setiap _node_ aplikasi, ditandatangani oleh Certificate Authority (CA) lokal yang juga dibuat untuk keperluan pengujian.
    - Server `aiohttp` dikonfigurasi untuk menggunakan sertifikat ini. _Client_ `aiohttp` dikonfigurasi untuk memvalidasi sertifikat _server_ menggunakan CA lokal, memastikan koneksi terenkripsi dan terotentikasi (dasar).

2.  **Audit Logging:**
    - Detail _logging_ ditingkatkan secara signifikan di semua _handler_ API dan fungsi internal.
    - Informasi penting seperti IP sumber _request_, _timestamp_, aksi yang diminta, parameter kunci, hasil operasi (termasuk _error_), dan target _routing_ (jika relevan) dicatat untuk keperluan _tracing_ dan _debugging_.

Implementasi keamanan ini memberikan perlindungan dasar terhadap penyadapan (_eavesdropping_) dan modifikasi data saat transit antar _service_, serta meningkatkan kemampuan observasi sistem.
