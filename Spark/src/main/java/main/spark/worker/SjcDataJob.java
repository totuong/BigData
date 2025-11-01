package main.spark.worker;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import main.spark.service.SparkJobService;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SjcDataJob {
    private final SparkJobService sparkJobService;

    @PostConstruct
    public void sync() {
        log.info("🚀 Bắt đầu đồng bộ dữ liệu SJC từ HDFS...");
        sparkJobService.syncSJC();
        log.info("✅ Hoàn thành đồng bộ dữ liệu SJC.");
        log.info("🚀 Bắt đầu đồng bộ dữ liệu PNJ...");
        sparkJobService.syncNestedPrices();
        log.info("✅ Hoàn thành đồng bộ dữ liệu PNJ");
    }

}
