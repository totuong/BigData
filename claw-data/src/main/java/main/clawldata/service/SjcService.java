package main.clawldata.service;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kong.unirest.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import main.clawldata.config.AppProps;
import main.clawldata.config.FileProgressStore;
import main.clawldata.module.SjcRecord;
import main.clawldata.utils.CsvUtils;
import static  main.clawldata.utils.DateUtil.*;

import main.clawldata.utils.JsonUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@Component
@RequiredArgsConstructor
public class SjcService {

    private final AppProps props;
    private final ObjectMapper mapper = new ObjectMapper();
    private final FileProgressStore progress;

    public void genCsv() {
        LocalDate end = "TODAY".equalsIgnoreCase(props.getToDate())
                ? LocalDate.now(ZoneId.of("Asia/Bangkok"))
                : LocalDate.parse(props.getToDate(), DF);

        LocalDate defaultStart = end.minusDays(Math.max(1, props.getDays()) - 1L);

        LocalDate last = progress.loadLastSuccess();            // null nếu chưa có
        LocalDate start = (last == null) ? defaultStart : last.plusDays(1);

        if (start.isAfter(end)) {
            log.info("Nothing to do. Already crawled up to {}", last == null ? "N/A" : last.format(DF));
            return;
        }

        log.info("Incremental crawl (single CSV) from {} to {}. Checkpoint: {}",
                start.format(DF), end.format(DF), last == null ? "none" : last.format(DF));

        final long baseDelay = Math.max(0L, props.getDelayMs());

        LocalDate cur = start;
        while (!cur.isAfter(end)) {
            try {
                // 1) Gọi API theo ngày
                List<SjcRecord> rows = fetchByDate(cur);

                // 2) Ghi append vào 1 file nếu có dữ liệu
                if (!rows.isEmpty()) {
                    Path out = CsvUtils.appendToSingle(props.getCsvFile(), rows);
                    log.info("[{}] appended {} rows -> {}", cur.format(DF), rows.size(), out);
                } else {
                    log.info("[{}] no data", cur.format(DF));
                }

                // 3) Cập nhật checkpoint SAU khi xử lý xong ngày hiện tại
                progress.saveLastSuccess(cur);

            } catch (Exception e) {
                log.warn("[{}] error: {}", cur.format(DF), e.toString());
                // Không cập nhật checkpoint khi lỗi để lần sau còn thử lại ngày này
            }

            // 4) Throttle với jitter
            long jitter = ThreadLocalRandom.current().nextLong(-baseDelay / 3, baseDelay / 3 + 1);
            long sleepMs = Math.max(0L, baseDelay + jitter);
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.info("Interrupted, stop.");
                break;
            }

            cur = cur.plusDays(1);
        }

        log.info("Crawl done.");
    }

    public void genJson() {
        LocalDate end = "TODAY".equalsIgnoreCase(props.getToDate())
                ? LocalDate.now(ZoneId.of("Asia/Bangkok"))
                : LocalDate.parse(props.getToDate(), DF);

        LocalDate defaultStart = end.minusDays(Math.max(1, props.getDays()) - 1L);
        LocalDate last = progress.loadLastSuccess(); // null nếu chưa có
        LocalDate start = (last == null) ? defaultStart : last.plusDays(1);

        if (start.isAfter(end)) {
            log.info("✅ Nothing to do. Already crawled up to {}",
                    last == null ? "N/A" : last.format(DF));
            return;
        }

        log.info("🚀 Incremental crawl (JSON only) from {} → {}. Checkpoint: {}",
                start.format(DF), end.format(DF),
                last == null ? "none" : last.format(DF));

        final long baseDelay = Math.max(0L, props.getDelayMs());

        // 🔹 Lấy đường dẫn JSON từ cấu hình
        Path jsonFile = Path.of(props.getJsonFile());
        if (!Files.exists(jsonFile.getParent())) {
            try {
                Files.createDirectories(jsonFile.getParent());
            } catch (IOException e) {
                log.error("❌ Cannot create output directory: {}", jsonFile.getParent(), e);
                return;
            }
        }


        LocalDate cur = start;
        while (!cur.isAfter(end)) {
            try {
                // 1️⃣ Gọi API lấy dữ liệu theo ngày
                List<SjcRecord> rows = fetchByDate(cur);

                // 2️⃣ Ghi JSON nếu có dữ liệu
                if (!rows.isEmpty()) {
                    JsonUtils.appendToSingle(jsonFile, rows);
                    log.info("[{}] ✅ Appended {} rows → {}", cur.format(DF), rows.size(), jsonFile);
                } else {
                    log.info("[{}] ⚠️ No data", cur.format(DF));
                }

                // 3️⃣ Cập nhật checkpoint sau khi ghi xong
                progress.saveLastSuccess(cur);

            } catch (Exception e) {
                log.warn("[{}] ⚠️ Error: {}", cur.format(DF), e.getMessage(), e);
                // không cập nhật checkpoint để thử lại sau
            }

            // 4️⃣ Tạm nghỉ giữa các ngày crawl (có jitter)
            long jitter = ThreadLocalRandom.current().nextLong(-baseDelay / 3, baseDelay / 3 + 1);
            long sleepMs = Math.max(0L, baseDelay + jitter);
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.info("🛑 Interrupted, stopping crawl.");
                break;
            }

            cur = cur.plusDays(1);
        }

        log.info("🎉 Crawl done. JSON saved at: {}", jsonFile.toAbsolutePath());
    }



    private List<SjcRecord> fetchByDate(LocalDate date) throws Exception {
        String dmy = date.format(java.time.format.DateTimeFormatter.ofPattern("dd/MM/yyyy"));

        HttpResponse<String> res = Unirest.post(props.getEndpoint())
                .header("User-Agent", props.getUserAgent())
                .header("Referer", props.getReferer())
                .header("Content-Type", ContentType.APPLICATION_FORM_URLENCODED.getMimeType())
                .body("method=GetSJCGoldPriceByDate&toDate=" + urlEncode(dmy))
                .asString();

        if (!res.isSuccess()) {
            throw new IllegalStateException("HTTP " + res.getStatus() + " on " + dmy);
        }

        String body = res.getBody();
        JsonNode root;
        try {
            root = mapper.readTree(body);
        } catch (Exception ex) {
            // đôi khi server trả HTML lỗi – log & bỏ qua
            log.warn("Non-JSON response on {}: {}", dmy, body != null ? body.substring(0, Math.min(200, body.length())) : "null");
            return List.of();
        }

        return flattenToRecords(dmy, root);
    }

    private static String urlEncode(String s) {
        try {
            return java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            return s;
        }
    }

    private List<SjcRecord> flattenToRecords(String dmy, JsonNode root) {
        List<SjcRecord> out = new ArrayList<>();

        if (root.isArray()) {
            for (JsonNode n : root) out.add(nodeToRecord(dmy, n));
            return out;
        }

        for (String key : List.of("data", "items", "result", "Result", "Rows", "rows", "List")) {
            JsonNode arr = root.get(key);
            if (arr != null && arr.isArray()) {
                for (JsonNode n : arr) out.add(nodeToRecord(dmy, n));
                return out;
            }
        }

        out.add(nodeToRecord(dmy, root));
        return out;
    }

    private SjcRecord nodeToRecord(String dmy, JsonNode n) {
        String type = str(n, "type", "Type", "Loai", "Ten", "Product", "Name");
        String buy = str(n, "buy", "Buy", "Mua", "bid", "Bid");
        String sell = str(n, "sell", "Sell", "Ban", "ask", "Ask");

        String payload = n.isValueNode() ? n.asText() : n.toString();

        return SjcRecord.builder()
                .date(dmy)
                .type(type)
                .buy(buy)
                .sell(sell)
                .payload(payload)
                .build();
    }

    private String str(JsonNode n, String... keys) {
        for (String k : keys) {
            JsonNode v = n.get(k);
            if (v != null && !v.isNull() && !v.isMissingNode()) {
                String s = v.asText();
                if (s != null && !s.isBlank()) return s.trim();
            }
        }
        // tìm numeric field phổ biến
        Optional<Map.Entry<String, JsonNode>> num = iterable(n.fields())
                .filter(e -> e.getKey().toLowerCase().contains("buy") || e.getKey().toLowerCase().contains("sell"))
                .findFirst();
        return num.map(e -> e.getValue().asText()).orElse(null);
    }

    private static <T> Stream<Map.Entry<String, T>> iterable(Iterator<Map.Entry<String, T>> it) {
        Iterable<Map.Entry<String, T>> iterable = () -> it;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
