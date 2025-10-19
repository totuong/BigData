package main.clawldata.worker;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import main.clawldata.config.AppProps;
import main.clawldata.config.FileProgressStore;
import main.clawldata.module.SjcRecord;
import main.clawldata.service.SjcService;
import main.clawldata.utils.CsvUtils;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
@RequiredArgsConstructor
public class SjcCrawlerJob {

    private static final DateTimeFormatter DF = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    private final AppProps props;
    private final SjcService service;
    private final FileProgressStore progress;

    public void runOnce() {
        // cho phép dùng "TODAY" để tiện cấu hình
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
                List<SjcRecord> rows = service.fetchByDate(cur);

                // 2) Ghi append vào 1 file nếu có dữ liệu
                if (!rows.isEmpty()) {
                    Path out = CsvUtils.appendToSingle(props.getSingleFile(), rows);
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
}
