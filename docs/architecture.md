# Arsitektur Sistem Sinkronisasi Terdistribusi

Sistem ini dirancang sebagai arsitektur _microservice_ yang berjalan di dalam satu jaringan Docker (`raft-net`). Setiap _service_ utama (Lock Manager, Queue, Cache) berjalan sebagai kontainer independen, mengimplementasikan algoritma terdistribusi yang berbeda untuk memenuhi persyaratan tugas.

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

## Penjelasan Algoritma

Sistem ini terdiri dari tiga _microservice_ utama yang masing-masing mengimplementasikan algoritma sistem terdistribusi yang berbeda untuk memenuhi _core requirements_.

### 1. Sub-tugas A: Raft Distributed Lock Manager

_Service_ ini adalah _state machine_ terdistribusi yang mengelola _shared_ dan _exclusive lock_. [cite_start]Untuk memastikan semua node (replika) setuju pada _state_ (status) _lock_ yang sama, saya mengimplementasikan algoritma konsensus **Raft** dari awal, berdasarkan paper "In Search of an Understandable Consensus Algorithm" [cite: 601-1640].

[cite_start]Logika Raft memecah masalah konsensus menjadi tiga bagian[cite: 740]:

1.  **Leader Election (Pemilihan Pemimpin):** Sistem dimulai dengan semua node sebagai `Follower`. [cite_start]Setiap `Follower` memiliki _timer_ pemilihan acak (antara 5-10 detik) [cite: 911-912]. [cite_start]Jika seorang `Follower` tidak menerima _heartbeat_ dari `Leader` sebelum _timer_-nya habis, ia akan berubah menjadi `Candidate` [cite: 896-897], menaikkan _term_ (periode), dan meminta suara (`RequestVote` RPC) dari _peer_ lain. [cite_start]Jika ia menerima suara dari mayoritas (misalnya, 2 dari 3 node), ia menjadi `Leader`[cite: 901].

2.  [cite_start]**Log Replication (Replikasi Log):** `Leader` adalah satu-satunya yang bertanggung jawab untuk menangani permintaan _client_[cite: 736]. [cite_start]Ketika `Leader` menerima perintah (misal: `ACQUIRE_EXCLUSIVE lock_A user1`), ia menambahkannya ke log-nya sendiri [cite: 959] [cite_start]dan mengirimkan `AppendEntries` RPC ke semua _Follower_ untuk memaksa log mereka sama[cite: 959, 981].

3.  [cite_start]**Safety & Commit:** Sebuah entri log dianggap **committed** (aman untuk dieksekusi) hanya setelah berhasil direplikasi ke mayoritas _server_[cite: 968]. [cite_start]Untuk mencegah data yang salah, Raft memberlakukan **Election Restriction**: seorang _Candidate_ tidak bisa memenangkan pemilu kecuali log-nya "setidaknya sama terbarunya" dengan log mayoritas _cluster_ [cite: 1083, 1086-1087]. Ini menjamin bahwa `Leader` baru selalu memiliki semua entri yang sudah di-_commit_.

_State machine_ di atas Raft kemudian menerapkan `ACQUIRE` dan `RELEASE` secara berurutan, sambil melacak "grafik tunggu" (wait-for-graph) untuk mendeteksi dan menolak transaksi yang akan menyebabkan _deadlock_.

### 2. Sub-tugas B: Distributed Queue (Consistent Hashing)

_Service_ ini adalah _router_ antrian cerdas yang menggunakan **Consistent Hashing** untuk mempartisi antrian (`queue_name`) ke beberapa _node_ Redis.

1.  **Konsep "Hash Ring":** Bayangkan sebuah cincin atau lingkaran. Ketiga _node_ Redis kita (`redis-1`, `redis-2`, `redis-3`) ditempatkan di beberapa titik di sepanjang cincin ini (menggunakan _virtual replicas_ untuk distribusi yang lebih baik).
2.  **Perutean Pesan:** Ketika `queue-node-1` menerima permintaan untuk mengirim pesan ke `"antrian_A"`, ia akan menghitung _hash_ dari `"antrian_A"`. Ia kemudian menempatkan _hash_ itu di cincin dan "berjalan" searah jarum jam hingga menemukan _node_ Redis pertama (misalnya `redis-2`). Semua pesan untuk `"antrian_A"` akan dirutekan ke `redis-2`. Pesan untuk `"antrian_B"` mungkin akan di-_hash_ ke lokasi lain dan mendarat di `redis-3`.
3.  **Toleransi Kegagalan:** Keuntungan dari _consistent hashing_ adalah jika `redis-2` mati, hanya kunci-kunci yang dimilikinya yang akan dipetakan ulang ke _node_ berikutnya di cincin (`redis-3`), tanpa mengganggu kunci-kunci yang sudah ada di `redis-1`.
4.  **At-Least-Once Delivery:** Untuk memenuhi syarat persistensi dan jaminan pengiriman, saya menggunakan **Redis Streams**. _Producer_ menggunakan `XADD` untuk menambah pesan. _Consumer_ menggunakan `XGROUP CREATE` dan `XREADGROUP` untuk membaca pesan. Ini memastikan bahwa jika _consumer_ mati sebelum mengkonfirmasi (`XACK`) sebuah pesan, pesan itu tetap ada dan dapat diproses oleh _consumer_ lain.

### 3. Sub-tugas C: Cache Coherence (MESI)

_Service_ ini mensimulasikan sekelompok _cache_ CPU terdistribusi yang menjaga koherensi data menggunakan protokol **MESI**.

saya mengimplementasikan 3 _node_ _cache_ (`cache-node-1`, `cache-node-2`, `cache-node-3`) yang saling berkomunikasi melalui "bus" internal (menggunakan _endpoint_ HTTP `/bus/read` dan `/bus/invalidate`).

Setiap baris _cache_ (misal: `"alamat_A"`) di setiap _node_ dapat memiliki satu dari empat _state_:

1.  **M (Modified):** _Cache_ ini memiliki data terbaru; data di "memori" sudah basi.
2.  **E (Exclusive):** _Cache_ ini adalah _satu-satunya_ yang memiliki salinan data ini, dan data ini bersih (sama dengan memori).
3.  **S (Shared):** _Cache_ ini memiliki data, dan _setidaknya satu node cache lain_ juga memilikinya.
4.  **I (Invalid):** Salinan data di _cache_ ini sudah basi dan tidak boleh digunakan.

**Skenario Alur Kerja (Sesuai Pengujian):**

1.  **Cache Miss (Read):** `cache-node-1` membaca `"alamat_A"`. Terjadi _cache miss_. Node mengambil data dari "memori utama" (simulasi) dan menyetel _state_-nya ke **E (Exclusive)**.
2.  **Read by Other (E -> S):** `cache-node-2` membaca `"alamat_A"`. Terjadi _cache miss_. Ia mengirim `BusRd` (Bus Read) ke semua _peer_. `cache-node-1` menerima `BusRd`, mengirimkan datanya, dan mengubah _state_-nya dari 'E' menjadi **S (Shared)**. `cache-node-2` memuat data dengan _state_ **S (Shared)**.
3.  **Write (S -> M & Invalidate):** `cache-node-1` menulis data baru ke `"alamat_A"`. Ia segera mengirimkan `BusInvalidate` ke semua _peer_ dan mengubah _state_ lokalnya menjadi **M (Modified)**.
4.  **Invalidation:** `cache-node-2` menerima `BusInvalidate` dan mengubah _state_ `"alamat_A"` miliknya menjadi **I (Invalid)**, memastikan ia tidak akan membaca data yang basi.

Untuk _cache replacement_, saya menggunakan kebijakan **LRU (Least Recently Used)** yang diimplementasikan menggunakan `OrderedDict` dari Python.
