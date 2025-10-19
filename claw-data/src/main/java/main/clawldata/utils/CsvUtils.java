package main.clawldata.utils;

import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.CSVWriter;
import main.clawldata.module.SjcRecord;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class CsvUtils {

    public static Path appendToSingle(String filePath, List<SjcRecord> rows) throws IOException {
        if (rows == null || rows.isEmpty()) return null;

        Path path = Paths.get(filePath);
        Files.createDirectories(path.getParent());
        boolean exists = Files.exists(path);

        // Mở writer append + lock file để an toàn khi có nhiều tiến trình (tuỳ chọn)
        try (FileChannel ch = FileChannel.open(path,
                exists ? new StandardOpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE}
                        : new StandardOpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE});
             BufferedWriter w = new BufferedWriter(java.nio.channels.Channels.newWriter(ch, java.nio.charset.StandardCharsets.UTF_8))) {

            // Ghi header nếu file mới
            if (!exists) {
                w.write("date,type,buy,sell,payload");
                w.newLine();
            }

            // Append từng dòng
            for (SjcRecord r : rows) {
                // escape đơn giản cho dấu phẩy/nháy kép
                String date = esc(r.getDate());
                String type = esc(r.getType());
                String buy  = esc(r.getBuy());
                String sell = esc(r.getSell());
                String payload = esc(r.getPayload());
                w.write(String.join(",", date, type, buy, sell, payload));
                w.newLine();
            }
            w.flush();
        }
        return path;
    }

    private static String esc(String s) {
        if (s == null) return "";
        boolean needQuote = s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r");
        String v = s.replace("\"", "\"\"");
        return needQuote ? "\"" + v + "\"" : v;
    }
}
