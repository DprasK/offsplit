# Offsplit Android

Versi Android native untuk fungsi inti `offsplit.py`.

## Fitur

- Pilih file sumber dari storage Android.
- Split file menjadi part `.ofs` format text `OFFSPLIT/1`.
- Pilih folder parts lalu join kembali ke file asli.
- Validasi CRC32 per frame saat membaca part.
- Validasi ukuran akhir dan SHA256 setelah join.

## Batasan versi awal

- Belum mendukung format MessagePack `OFFSPLIT/2` (`.ofm` + `.ofs`) dari CLI Python.
- Belum membawa mode HTTP, WebSocket, UDP, QUIC, atau MQTT.
- App ini fokus pada mode offline dulu agar bisa berjalan native tanpa Python runtime tambahan.

## Cara Buka

1. Buka Android Studio.
2. Pilih `Open`.
3. Arahkan ke folder `E:\isolated codex\OffsplitAndroid`.
4. Tunggu Gradle sync.
5. Jalankan ke emulator atau perangkat Android.

## Build APK

Dari PowerShell:

```powershell
cd "E:\isolated codex\OffsplitAndroid"
.\build-apk.ps1
```

Jika memakai Android Studio:

1. Buka folder `E:\isolated codex\OffsplitAndroid`.
2. Tunggu Gradle sync selesai.
3. Pilih `Build > Build Bundle(s) / APK(s) > Build APK(s)`.
4. APK debug biasanya muncul di `app\build\outputs\apk\debug\app-debug.apk`.

## Cara Pakai

1. Tap `Pilih File Sumber`.
2. Tap `Pilih Folder Output Parts`.
3. Isi ukuran chunk, contoh `512k` atau `1m`.
4. Tap `Split`.
5. Untuk menggabungkan lagi, pilih folder parts dan folder hasil join.
6. Tap `Join`.

## Catatan Kompatibilitas

Part yang dibuat app Android ini memakai format text `OFFSPLIT/1`, sehingga bisa dibaca oleh `offsplit.py join`.

Untuk membuat part dari CLI yang akan dibaca app Android:

```bash
./offsplit.py split file.bin -o parts -s 1m --format text
```
