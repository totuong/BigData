package main.clawldata.worker;

import jakarta.annotation.PostConstruct;
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


    private final SjcService service;

    @PostConstruct
    public void genCsv() {
        log.info("🚀 Starting SjcCrawlerJob to crawl SJC data and generate CSV...");

        try {
            service.genJson();
            log.info("✅ SjcCrawlerJob completed successfully.");
        } catch (Exception e) {
            log.error("❌ SjcCrawlerJob failed with exception: ", e);
        }
    }
}
