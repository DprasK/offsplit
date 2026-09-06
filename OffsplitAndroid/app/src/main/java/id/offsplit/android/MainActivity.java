package id.offsplit.android;

import android.app.Activity;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.view.Gravity;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.TextView;

import java.util.Locale;

public class MainActivity extends Activity {
    private static final int REQ_SOURCE_FILE = 10;
    private static final int REQ_SPLIT_FOLDER = 11;
    private static final int REQ_JOIN_PARTS_FOLDER = 12;
    private static final int REQ_JOIN_OUTPUT_FOLDER = 13;

    private Uri sourceFileUri;
    private Uri splitOutputFolderUri;
    private Uri joinPartsFolderUri;
    private Uri joinOutputFolderUri;

    private TextView sourceLabel;
    private TextView splitFolderLabel;
    private TextView joinPartsLabel;
    private TextView joinOutputLabel;
    private TextView statusLabel;
    private EditText chunkInput;
    private Button splitButton;
    private Button joinButton;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(buildView());
        refreshButtons();
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (resultCode != RESULT_OK || data == null || data.getData() == null) {
            return;
        }

        Uri uri = data.getData();
        int flags = data.getFlags()
                & (Intent.FLAG_GRANT_READ_URI_PERMISSION | Intent.FLAG_GRANT_WRITE_URI_PERMISSION);
        try {
            getContentResolver().takePersistableUriPermission(uri, flags);
        } catch (SecurityException ignored) {
            // Some providers grant temporary access only.
        }

        if (requestCode == REQ_SOURCE_FILE) {
            sourceFileUri = uri;
            sourceLabel.setText(shortUri(uri));
        } else if (requestCode == REQ_SPLIT_FOLDER) {
            splitOutputFolderUri = uri;
            splitFolderLabel.setText(shortUri(uri));
        } else if (requestCode == REQ_JOIN_PARTS_FOLDER) {
            joinPartsFolderUri = uri;
            joinPartsLabel.setText(shortUri(uri));
        } else if (requestCode == REQ_JOIN_OUTPUT_FOLDER) {
            joinOutputFolderUri = uri;
            joinOutputLabel.setText(shortUri(uri));
        }
        refreshButtons();
    }

    private View buildView() {
        int padding = dp(18);
        ScrollView scrollView = new ScrollView(this);
        LinearLayout root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);
        root.setPadding(padding, padding, padding, padding);
        root.setBackgroundColor(0xFFF7F7F2);
        scrollView.addView(root);

        TextView title = text("Offsplit Android", 26, 0xFF18221D);
        title.setGravity(Gravity.START);
        root.addView(title);

        TextView subtitle = text("Split dan join file offline memakai format text OFFSPLIT/1.", 15, 0xFF4F5D55);
        subtitle.setPadding(0, dp(6), 0, dp(18));
        root.addView(subtitle);

        sourceLabel = label("Belum pilih file sumber");
        splitFolderLabel = label("Belum pilih folder output parts");
        joinPartsLabel = label("Belum pilih folder parts");
        joinOutputLabel = label("Belum pilih folder hasil join");
        statusLabel = label("Siap.");
        statusLabel.setTextColor(0xFF2E7D5B);

        root.addView(sectionTitle("Split File"));
        root.addView(button("Pilih File Sumber", v -> pickFile()));
        root.addView(sourceLabel);
        root.addView(button("Pilih Folder Output Parts", v -> pickTree(REQ_SPLIT_FOLDER)));
        root.addView(splitFolderLabel);

        chunkInput = new EditText(this);
        chunkInput.setSingleLine(true);
        chunkInput.setText("1m");
        chunkInput.setHint("Ukuran chunk, contoh 512k atau 1m");
        chunkInput.setTextSize(16);
        chunkInput.setPadding(0, dp(8), 0, dp(12));
        root.addView(chunkInput);

        splitButton = button("Split", v -> runSplit());
        root.addView(splitButton);

        root.addView(spacer(22));
        root.addView(sectionTitle("Join Parts"));
        root.addView(button("Pilih Folder Parts", v -> pickTree(REQ_JOIN_PARTS_FOLDER)));
        root.addView(joinPartsLabel);
        root.addView(button("Pilih Folder Hasil Join", v -> pickTree(REQ_JOIN_OUTPUT_FOLDER)));
        root.addView(joinOutputLabel);

        joinButton = button("Join", v -> runJoin());
        root.addView(joinButton);

        root.addView(spacer(22));
        root.addView(sectionTitle("Status"));
        root.addView(statusLabel);
        return scrollView;
    }

    private void pickFile() {
        Intent intent = new Intent(Intent.ACTION_OPEN_DOCUMENT);
        intent.addCategory(Intent.CATEGORY_OPENABLE);
        intent.setType("*/*");
        intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION | Intent.FLAG_GRANT_PERSISTABLE_URI_PERMISSION);
        startActivityForResult(intent, REQ_SOURCE_FILE);
    }

    private void pickTree(int requestCode) {
        Intent intent = new Intent(Intent.ACTION_OPEN_DOCUMENT_TREE);
        intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION
                | Intent.FLAG_GRANT_WRITE_URI_PERMISSION
                | Intent.FLAG_GRANT_PERSISTABLE_URI_PERMISSION);
        startActivityForResult(intent, requestCode);
    }

    private void runSplit() {
        setBusy(true);
        status("Mulai split...");
        new Thread(() -> {
            try {
                int chunkSize = OffsplitTextCodec.parseSize(chunkInput.getText().toString());
                OffsplitTextCodec.SplitResult result = OffsplitTextCodec.split(
                        this,
                        sourceFileUri,
                        splitOutputFolderUri,
                        chunkSize,
                        this::status
                );
                status(String.format(Locale.US,
                        "Split selesai: %s menjadi %d frame. Transfer ID: %s",
                        result.filename,
                        result.totalFrames,
                        result.transferId));
            } catch (Exception exc) {
                status("Split gagal: " + exc.getMessage());
            } finally {
                runOnUiThread(() -> setBusy(false));
            }
        }).start();
    }

    private void runJoin() {
        setBusy(true);
        status("Mulai join...");
        new Thread(() -> {
            try {
                OffsplitTextCodec.JoinResult result = OffsplitTextCodec.join(
                        this,
                        joinPartsFolderUri,
                        joinOutputFolderUri,
                        this::status
                );
                status(String.format(Locale.US,
                        "Join selesai: %s (%d byte, %d frame).",
                        result.filename,
                        result.bytesWritten,
                        result.totalFrames));
            } catch (Exception exc) {
                status("Join gagal: " + exc.getMessage());
            } finally {
                runOnUiThread(() -> setBusy(false));
            }
        }).start();
    }

    private Button button(String text, View.OnClickListener listener) {
        Button button = new Button(this);
        button.setText(text);
        button.setAllCaps(false);
        button.setTextSize(15);
        button.setOnClickListener(listener);
        button.setPadding(0, dp(8), 0, dp(8));
        return button;
    }

    private TextView sectionTitle(String text) {
        TextView view = text(text, 19, 0xFF18221D);
        view.setPadding(0, dp(8), 0, dp(8));
        return view;
    }

    private TextView label(String text) {
        TextView view = text(text, 14, 0xFF4F5D55);
        view.setPadding(0, dp(4), 0, dp(10));
        return view;
    }

    private TextView text(String text, int sp, int color) {
        TextView view = new TextView(this);
        view.setText(text);
        view.setTextSize(sp);
        view.setTextColor(color);
        view.setLineSpacing(0, 1.1f);
        return view;
    }

    private View spacer(int heightDp) {
        View view = new View(this);
        view.setLayoutParams(new LinearLayout.LayoutParams(1, dp(heightDp)));
        return view;
    }

    private void setBusy(boolean busy) {
        splitButton.setEnabled(!busy && sourceFileUri != null && splitOutputFolderUri != null);
        joinButton.setEnabled(!busy && joinPartsFolderUri != null && joinOutputFolderUri != null);
    }

    private void refreshButtons() {
        if (splitButton != null && joinButton != null) {
            setBusy(false);
        }
    }

    private void status(String message) {
        runOnUiThread(() -> statusLabel.setText(message));
    }

    private int dp(int value) {
        return Math.round(value * getResources().getDisplayMetrics().density);
    }

    private String shortUri(Uri uri) {
        String value = uri.toString();
        if (value.length() <= 72) {
            return value;
        }
        return value.substring(0, 34) + "..." + value.substring(value.length() - 34);
    }
}
