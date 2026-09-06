# offsplit

`offsplit.py` adalah CLI Linux sederhana untuk memecah file biner menjadi frame offline dan menyatukannya kembali.

Default format sekarang adalah MessagePack (`OFFSPLIT/2`) agar payload biner bisa disimpan langsung tanpa base64. Format text lama (`OFFSPLIT/1`) tetap tersedia untuk inspeksi manual.

Format text setiap part:

```text
OFFSPLIT/1
HEADER {"chunk_size":1048576,"created_utc":"...","filename":"data.bin","format":"OFFSPLIT/1","sha256":"...","size":123,"total_frames":1,"transfer_id":"..."}
FRAME 0 123 deadbeef <payload-base64>
```

- `HEADER` berisi metadata file asli.
- `FRAME` berisi nomor urut, panjang data asli, CRC32 per frame, dan payload base64.
- Payload base64 membuat frame tetap aman untuk data biner apa pun.
- Format MessagePack memakai satu manifest `.ofm` untuk metadata lengkap dan banyak frame `.ofs` yang hanya membawa `transfer_id`, `metadata_hash`, urutan, panjang, CRC32, dan payload biner mentah.
- Saat `join`, tool memvalidasi CRC tiap frame, frame hilang/duplikat, ukuran akhir, dan SHA256 file akhir.
- `split` dan `join` mendukung resume secara default.

## Pakai

```bash
chmod +x offsplit.py
./offsplit.py split file.bin -o parts -s 1m
./offsplit.py join parts -o file-restored.bin
```

Command terpadu untuk memilih mode transfer:

```bash
./offsplit.py transfer file.bin --mode offline --parts parts -s 1m
./offsplit.py transfer file.bin --mode http --url http://alamat-tujuan:8080 --parts parts -s 1m
./offsplit.py transfer file.bin --mode websocket --url ws://alamat-tujuan:8090 --parts parts -s 1m
./offsplit.py transfer file.bin --mode udp --url udp://alamat-tujuan:9000 --parts parts -s 16k
./offsplit.py transfer file.bin --mode quic --url quic://alamat-tujuan:9443 --parts parts -s 1m
./offsplit.py transfer file.bin --mode mqtt --url mqtt://broker:1883 --topic offsplit --parts parts -s 64k
```

Mode `offline` hanya membuat parts untuk dicopy manual. Mode `http`, `websocket`, `udp`, `quic`, dan `mqtt` akan split dulu ke `--parts`, lalu langsung mengirim lewat transport yang dipilih.

Pilih format:

```bash
./offsplit.py split file.bin -o parts -s 1m --format msgpack
./offsplit.py split file.bin -o parts -s 1m --format text
```

`join` melakukan autodetect, jadi tidak perlu menyebut format:

```bash
./offsplit.py join parts -o file-restored.bin
```

Ukuran chunk mendukung `b`, `k`, `kb`, `m`, `mb`, `g`, dan `gb`.

## Pause / Resume

Kalau proses `split` berhenti, jalankan command yang sama lagi:

```bash
./offsplit.py split file.bin -o parts -s 1m
```

Frame `.ofs` yang sudah ada dan valid akan di-skip. Frame yang hilang atau rusak akan ditulis ulang.

Kalau proses `join` berhenti, tool meninggalkan file sementara:

```text
file-restored.bin.partial
```

Jalankan command yang sama lagi:

```bash
./offsplit.py join parts -o file-restored.bin
```

Tool akan memvalidasi isi `.partial` terhadap frame awal, lalu melanjutkan dari frame berikutnya. Resume tetap bisa dilakukan walaupun sudah lama, selama file `.partial` dan part `.ofs` masih ada.

Gunakan `--no-resume` kalau ingin mulai ulang dari nol.

## HTTP Transfer dan UI

Jalankan receiver di mesin tujuan:

```bash
./offsplit.py serve --host 0.0.0.0 --port 8080 --inbox inbox --completed completed
```

Buka UI di browser:

```text
http://localhost:8080
```

Di UI web, panel `Transfer Mode` bisa memilih `Offline`, `HTTP`, `WebSocket`, `UDP`, `QUIC`, atau `MQTT`. Isi path file sumber, folder parts, chunk size, lalu isi target URL jika memakai mode jaringan.

Kirim parts MessagePack dari mesin pengirim:

```bash
./offsplit.py send parts http://alamat-tujuan:8080
```

Receiver akan menyimpan `.ofm` dan `.ofs` di `inbox`. UI menampilkan progress frame yang sudah diterima, jumlah frame hilang, status validasi, dan tombol `Join` untuk menyatukan file ke folder `completed`.

Kalau transfer terputus, jalankan `send` lagi. Sender akan bertanya status ke receiver dan hanya mengirim frame yang belum ada:

```bash
./offsplit.py send parts http://alamat-tujuan:8080
```

Untuk jaringan publik, pakai token:

```bash
./offsplit.py serve --host 0.0.0.0 --port 8080 --token rahasia
./offsplit.py send parts http://alamat-tujuan:8080 --token rahasia
```

Endpoint HTTP utama:

```text
POST /api/upload/manifest
POST /api/upload/frame
GET  /api/status/<transfer_id>
GET  /api/transfers
POST /api/join/<transfer_id>
POST /api/split
```

Catatan: gunakan HTTPS/reverse proxy kalau dipakai lewat internet publik.

## WebSocket Transfer

WebSocket cocok untuk transfer langsung antar komputer/server dengan koneksi dua arah dan ACK per frame.

Receiver:

```bash
./offsplit.py ws-serve --host 0.0.0.0 --port 8090 --inbox ws-inbox --completed ws-completed
```

Sender:

```bash
./offsplit.py ws-send parts ws://alamat-tujuan:8090
```

Kalau transfer terputus, jalankan `ws-send` lagi. Receiver mengirim status frame yang masih hilang, lalu sender hanya mengirim bagian yang belum ada.

Pakai token jika dibuka ke jaringan:

```bash
./offsplit.py ws-serve --host 0.0.0.0 --port 8090 --token rahasia
./offsplit.py ws-send parts ws://alamat-tujuan:8090 --token rahasia
```

Default `ws-serve` akan auto-join setelah semua frame lengkap. Gunakan `--no-join` kalau hanya ingin menerima frame dulu.

## UDP Transfer

UDP cocok untuk LAN/private network. Tool ini menambahkan reliability sederhana: manifest/status, ACK per frame, retry, complete, dan auto-join.

Receiver:

```bash
./offsplit.py udp-serve --host 0.0.0.0 --port 9000 --inbox udp-inbox --completed udp-completed
```

Sender:

```bash
./offsplit.py udp-send parts udp://alamat-tujuan:9000
```

Atau lewat command terpadu:

```bash
./offsplit.py transfer file.bin --mode udp --url udp://alamat-tujuan:9000 --parts parts -s 16k
```

Gunakan chunk kecil untuk UDP agar datagram tidak terlalu besar:

```bash
./offsplit.py split file.bin -o parts -s 16k --format msgpack
```

## QUIC Transfer

QUIC cocok untuk internet modern karena membawa reliability, congestion control, dan TLS. Mode ini membutuhkan module Python `aioquic`. Jika server tidak diberi `--cert` dan `--key`, tool membuat self-signed certificate sementara; client default menerima self-signed certificate. Gunakan `--secure` pada `quic-send` untuk verifikasi certificate.

Receiver:

```bash
./offsplit.py quic-serve --host 0.0.0.0 --port 9443 --inbox quic-inbox --completed quic-completed
```

Sender:

```bash
./offsplit.py quic-send parts quic://alamat-tujuan:9443
```

Atau lewat command terpadu:

```bash
./offsplit.py transfer file.bin --mode quic --url quic://alamat-tujuan:9443 --parts parts -s 1m
```

## MQTT Transfer

MQTT cocok jika transfer lewat broker, misalnya untuk jaringan IoT, edge device, atau host yang tidak bisa koneksi langsung. Adapter MQTT di tool ini memakai MQTT 3.1.1 basic over TCP tanpa dependency Python tambahan, tetapi tetap membutuhkan broker MQTT seperti Mosquitto.

Receiver:

```bash
./offsplit.py mqtt-recv mqtt://broker:1883 --topic offsplit --inbox mqtt-inbox --completed mqtt-completed
```

Sender:

```bash
./offsplit.py mqtt-send parts mqtt://broker:1883 --topic offsplit
```

Dengan username/password broker:

```bash
./offsplit.py mqtt-recv mqtt://broker:1883 --topic offsplit --username user --password pass
./offsplit.py mqtt-send parts mqtt://broker:1883 --topic offsplit --username user --password pass
```

Topic yang dipakai:

```text
offsplit/<transfer_id>/manifest
offsplit/<transfer_id>/frame/<seq>
```

`mqtt-send` publish manifest dan frame dengan QoS 1. `mqtt-recv` subscribe ke `offsplit/#`, menyimpan frame yang valid, dan default-nya auto-join setelah semua frame lengkap. Gunakan `--no-join` untuk mode receive-only, atau `--once` agar receiver keluar setelah satu transfer selesai.

Untuk MQTT, gunakan chunk lebih kecil jika broker punya batas payload, misalnya:

```bash
./offsplit.py split file.bin -o parts -s 64k --format msgpack
```
