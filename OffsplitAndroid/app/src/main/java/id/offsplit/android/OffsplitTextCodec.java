package id.offsplit.android;

import android.content.ContentResolver;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.provider.DocumentsContract;
import android.provider.OpenableColumns;
import android.util.Base64;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.util.zip.CRC32;

final class OffsplitTextCodec {
    interface ProgressListener {
        void onProgress(String message);
    }

    static final class SplitResult {
        final String filename;
        final int totalFrames;
        final String transferId;

        SplitResult(String filename, int totalFrames, String transferId) {
            this.filename = filename;
            this.totalFrames = totalFrames;
            this.transferId = transferId;
        }
    }

    static final class JoinResult {
        final String filename;
        final long bytesWritten;
        final int totalFrames;

        JoinResult(String filename, long bytesWritten, int totalFrames) {
            this.filename = filename;
            this.bytesWritten = bytesWritten;
            this.totalFrames = totalFrames;
        }
    }

    private static final String TEXT_MAGIC = "OFFSPLIT/1";

    private OffsplitTextCodec() {
    }

    static int parseSize(String value) {
        String input = value.trim().toLowerCase(Locale.US);
        if (input.isEmpty()) {
            throw new IllegalArgumentException("Ukuran chunk kosong");
        }

        int split = 0;
        while (split < input.length() && Character.isDigit(input.charAt(split))) {
            split++;
        }
        if (split == 0) {
            throw new IllegalArgumentException("Ukuran chunk harus angka, contoh 512k atau 1m");
        }

        long number = Long.parseLong(input.substring(0, split));
        String unit = input.substring(split);
        long multiplier;
        switch (unit) {
            case "":
            case "b":
                multiplier = 1L;
                break;
            case "k":
            case "kb":
                multiplier = 1024L;
                break;
            case "m":
            case "mb":
                multiplier = 1024L * 1024L;
                break;
            case "g":
            case "gb":
                multiplier = 1024L * 1024L * 1024L;
                break;
            default:
                throw new IllegalArgumentException("Unit ukuran tidak dikenal: " + unit);
        }

        long size = number * multiplier;
        if (size <= 0 || size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Ukuran chunk harus antara 1 byte dan 2 GB");
        }
        return (int) size;
    }

    static SplitResult split(Context context, Uri sourceUri, Uri outputTreeUri, int chunkSize, ProgressListener progress)
            throws Exception {
        ContentResolver resolver = context.getContentResolver();
        String filename = getDisplayName(resolver, sourceUri);
        long fileSize = getSize(resolver, sourceUri);
        if (fileSize < 0) {
            throw new IllegalStateException("Ukuran file sumber tidak bisa dibaca oleh Android");
        }
        String sourceSha = sha256(resolver, sourceUri);
        String transferId = sha256((filename + "\0" + fileSize + "\0" + sourceSha).getBytes(StandardCharsets.UTF_8))
                .substring(0, 16);
        int totalFrames = fileSize == 0 ? 1 : (int) ((fileSize + chunkSize - 1) / chunkSize);

        JSONObject metadata = new JSONObject();
        metadata.put("format", TEXT_MAGIC);
        metadata.put("transfer_id", transferId);
        metadata.put("filename", filename);
        metadata.put("size", fileSize);
        metadata.put("sha256", sourceSha);
        metadata.put("chunk_size", chunkSize);
        metadata.put("total_frames", totalFrames);
        metadata.put("created_utc", utcNow());

        String prefix = safeStem(filename);
        byte[] buffer = new byte[chunkSize];
        try (InputStream input = resolver.openInputStream(sourceUri)) {
            if (input == null) {
                throw new IllegalStateException("File sumber tidak bisa dibuka");
            }

            for (int sequence = 0; sequence < totalFrames; sequence++) {
                int read = readChunk(input, buffer, sequence == totalFrames - 1);
                byte[] chunk = Arrays.copyOf(buffer, read);
                String partName = prefix + "." + String.format(Locale.US, "%06d", sequence) + ".ofs";
                Uri partUri = createOrReplace(resolver, outputTreeUri, "application/octet-stream", partName);
                writePart(resolver, partUri, metadata, sequence, chunk);
                progress.onProgress("Split frame " + (sequence + 1) + "/" + totalFrames);
            }
        }

        return new SplitResult(filename, totalFrames, transferId);
    }

    static JoinResult join(Context context, Uri partsTreeUri, Uri outputTreeUri, ProgressListener progress) throws Exception {
        ContentResolver resolver = context.getContentResolver();
        List<DocumentEntry> parts = new ArrayList<>();
        for (DocumentEntry entry : listChildren(resolver, partsTreeUri)) {
            if (entry.name.endsWith(".ofs")) {
                parts.add(entry);
            }
        }
        parts.sort(Comparator.comparing(entry -> entry.name));
        if (parts.isEmpty()) {
            throw new IllegalStateException("Folder parts tidak berisi file .ofs");
        }

        ParsedPart first = readPart(resolver, parts.get(0).uri);
        JSONObject metadata = first.metadata;
        String filename = metadata.getString("filename");
        long expectedSize = metadata.getLong("size");
        String expectedSha = metadata.getString("sha256");
        int totalFrames = metadata.getInt("total_frames");

        if (parts.size() < totalFrames) {
            throw new IllegalStateException("Frame belum lengkap: ada " + parts.size() + " dari " + totalFrames);
        }

        Uri outputUri = createOrReplace(resolver, outputTreeUri, "application/octet-stream", filename);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        long bytesWritten = 0;

        try (OutputStream output = resolver.openOutputStream(outputUri, "w")) {
            if (output == null) {
                throw new IllegalStateException("File hasil tidak bisa dibuat");
            }

            ParsedPart current = first;
            for (int index = 0; index < totalFrames; index++) {
                if (index > 0) {
                    current = readPart(resolver, parts.get(index).uri);
                }
                validateMetadata(metadata, current.metadata);
                if (current.sequence != index) {
                    throw new IllegalStateException("Urutan frame tidak cocok. Harusnya " + index + ", dapat " + current.sequence);
                }
                output.write(current.data);
                digest.update(current.data);
                bytesWritten += current.data.length;
                progress.onProgress("Join frame " + (index + 1) + "/" + totalFrames);
            }
        }

        String actualSha = toHex(digest.digest());
        if (bytesWritten != expectedSize) {
            throw new IllegalStateException("Ukuran hasil tidak cocok. Harusnya " + expectedSize + ", dapat " + bytesWritten);
        }
        if (!actualSha.equals(expectedSha)) {
            throw new IllegalStateException("SHA256 hasil tidak cocok");
        }

        return new JoinResult(filename, bytesWritten, totalFrames);
    }

    private static int readChunk(InputStream input, byte[] buffer, boolean mayBeEmpty) throws Exception {
        int offset = 0;
        while (offset < buffer.length) {
            int read = input.read(buffer, offset, buffer.length - offset);
            if (read == -1) {
                break;
            }
            offset += read;
        }
        if (offset == 0 && !mayBeEmpty) {
            throw new IllegalStateException("Data file berhenti sebelum semua frame dibaca");
        }
        return offset;
    }

    private static void writePart(ContentResolver resolver, Uri partUri, JSONObject metadata, int sequence, byte[] chunk)
            throws Exception {
        CRC32 crc32 = new CRC32();
        crc32.update(chunk);
        String payload = Base64.encodeToString(chunk, Base64.NO_WRAP);

        StringBuilder builder = new StringBuilder();
        builder.append(TEXT_MAGIC).append('\n');
        builder.append("HEADER ").append(toAsciiJson(metadata)).append('\n');
        builder.append("FRAME ")
                .append(sequence).append(' ')
                .append(chunk.length).append(' ')
                .append(String.format(Locale.US, "%08x", crc32.getValue())).append(' ')
                .append(payload).append('\n');

        try (OutputStream output = resolver.openOutputStream(partUri, "w")) {
            if (output == null) {
                throw new IllegalStateException("Part tidak bisa ditulis");
            }
            output.write(builder.toString().getBytes(StandardCharsets.US_ASCII));
        }
    }

    private static ParsedPart readPart(ContentResolver resolver, Uri uri) throws Exception {
        try (InputStream input = resolver.openInputStream(uri);
             BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.US_ASCII))) {
            String magic = reader.readLine();
            String headerLine = reader.readLine();
            String frameLine = reader.readLine();
            if (!TEXT_MAGIC.equals(magic) || headerLine == null || frameLine == null) {
                throw new IllegalStateException("Format part tidak valid");
            }
            if (!headerLine.startsWith("HEADER ") || !frameLine.startsWith("FRAME ")) {
                throw new IllegalStateException("Header/frame part tidak valid");
            }

            JSONObject metadata = new JSONObject(headerLine.substring("HEADER ".length()));
            String[] pieces = frameLine.split(" ", 5);
            if (pieces.length != 5) {
                throw new IllegalStateException("Frame part tidak lengkap");
            }

            int sequence = Integer.parseInt(pieces[1]);
            int expectedLength = Integer.parseInt(pieces[2]);
            long expectedCrc = Long.parseLong(pieces[3], 16);
            byte[] data = Base64.decode(pieces[4], Base64.DEFAULT);

            CRC32 crc32 = new CRC32();
            crc32.update(data);
            if (data.length != expectedLength) {
                throw new IllegalStateException("Panjang frame " + sequence + " tidak cocok");
            }
            if (crc32.getValue() != expectedCrc) {
                throw new IllegalStateException("CRC frame " + sequence + " tidak cocok");
            }
            return new ParsedPart(metadata, sequence, data);
        }
    }

    private static void validateMetadata(JSONObject expected, JSONObject actual) throws Exception {
        String[] keys = {"format", "transfer_id", "filename", "size", "sha256", "chunk_size", "total_frames"};
        for (String key : keys) {
            if (!String.valueOf(expected.get(key)).equals(String.valueOf(actual.get(key)))) {
                throw new IllegalStateException("Metadata frame tidak konsisten pada " + key);
            }
        }
    }

    private static String sha256(ContentResolver resolver, Uri uri) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] buffer = new byte[1024 * 1024];
        try (InputStream input = resolver.openInputStream(uri)) {
            if (input == null) {
                throw new IllegalStateException("File tidak bisa dibuka");
            }
            int read;
            while ((read = input.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return toHex(digest.digest());
    }

    private static String sha256(byte[] data) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return toHex(digest.digest(data));
    }

    private static String toHex(byte[] data) {
        StringBuilder builder = new StringBuilder(data.length * 2);
        for (byte value : data) {
            builder.append(String.format(Locale.US, "%02x", value & 0xff));
        }
        return builder.toString();
    }

    private static String safeStem(String name) {
        String stem = name.replaceAll("[^A-Za-z0-9._-]+", "_").replaceAll("^[._]+|[._]+$", "");
        return stem.isEmpty() ? "data" : stem;
    }

    private static String toAsciiJson(JSONObject metadata) {
        String json = metadata.toString();
        StringBuilder builder = new StringBuilder(json.length());
        for (int index = 0; index < json.length(); index++) {
            char value = json.charAt(index);
            if (value <= 0x7f) {
                builder.append(value);
            } else {
                builder.append(String.format(Locale.US, "\\u%04x", (int) value));
            }
        }
        return builder.toString();
    }

    private static String utcNow() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US);
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        return format.format(new Date());
    }

    private static String getDisplayName(ContentResolver resolver, Uri uri) {
        try (Cursor cursor = resolver.query(uri, new String[]{OpenableColumns.DISPLAY_NAME}, null, null, null)) {
            if (cursor != null && cursor.moveToFirst()) {
                int index = cursor.getColumnIndex(OpenableColumns.DISPLAY_NAME);
                if (index >= 0) {
                    return cursor.getString(index);
                }
            }
        }
        return "data.bin";
    }

    private static long getSize(ContentResolver resolver, Uri uri) {
        try (Cursor cursor = resolver.query(uri, new String[]{OpenableColumns.SIZE}, null, null, null)) {
            if (cursor != null && cursor.moveToFirst()) {
                int index = cursor.getColumnIndex(OpenableColumns.SIZE);
                if (index >= 0 && !cursor.isNull(index)) {
                    return cursor.getLong(index);
                }
            }
        }
        return -1L;
    }

    private static Uri createOrReplace(ContentResolver resolver, Uri treeUri, String mimeType, String displayName)
            throws Exception {
        Uri existing = findChild(resolver, treeUri, displayName);
        if (existing != null) {
            DocumentsContract.deleteDocument(resolver, existing);
        }
        Uri parentUri = DocumentsContract.buildDocumentUriUsingTree(treeUri, DocumentsContract.getTreeDocumentId(treeUri));
        Uri created = DocumentsContract.createDocument(resolver, parentUri, mimeType, displayName);
        if (created == null) {
            throw new IllegalStateException("Gagal membuat file: " + displayName);
        }
        return created;
    }

    private static Uri findChild(ContentResolver resolver, Uri treeUri, String displayName) {
        for (DocumentEntry entry : listChildren(resolver, treeUri)) {
            if (entry.name.equals(displayName)) {
                return entry.uri;
            }
        }
        return null;
    }

    private static List<DocumentEntry> listChildren(ContentResolver resolver, Uri treeUri) {
        List<DocumentEntry> entries = new ArrayList<>();
        Uri childrenUri = DocumentsContract.buildChildDocumentsUriUsingTree(
                treeUri,
                DocumentsContract.getTreeDocumentId(treeUri)
        );
        String[] projection = {
                DocumentsContract.Document.COLUMN_DOCUMENT_ID,
                DocumentsContract.Document.COLUMN_DISPLAY_NAME
        };

        try (Cursor cursor = resolver.query(childrenUri, projection, null, null, null)) {
            if (cursor == null) {
                return entries;
            }
            while (cursor.moveToNext()) {
                String documentId = cursor.getString(0);
                String name = cursor.getString(1);
                Uri uri = DocumentsContract.buildDocumentUriUsingTree(treeUri, documentId);
                entries.add(new DocumentEntry(name, uri));
            }
        }
        return entries;
    }

    private static final class DocumentEntry {
        final String name;
        final Uri uri;

        DocumentEntry(String name, Uri uri) {
            this.name = name;
            this.uri = uri;
        }
    }

    private static final class ParsedPart {
        final JSONObject metadata;
        final int sequence;
        final byte[] data;

        ParsedPart(JSONObject metadata, int sequence, byte[] data) {
            this.metadata = metadata;
            this.sequence = sequence;
            this.data = data;
        }
    }
}
