package main.clawldata.service;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kong.unirest.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import main.clawldata.config.AppProps;
import main.clawldata.module.SjcRecord;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.StreamSupport;

@Slf4j
@Component
@RequiredArgsConstructor
public class SjcService {

    private final AppProps props;
    private final ObjectMapper mapper = new ObjectMapper();

    public List<SjcRecord> fetchByDate(LocalDate date) throws Exception {
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

    private static <T> java.util.stream.Stream<Map.Entry<String, T>> iterable(Iterator<Map.Entry<String, T>> it) {
        Iterable<Map.Entry<String, T>> iterable = () -> it;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
