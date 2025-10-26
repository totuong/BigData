package main.clawldata.config;

import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "app")
public class AppProps {
    private String endpoint;
    private String toDate;     // "18/10/2025"
    private int days;          // 365
    private String outputDir;
    private String userAgent;
    private String referer;
    private long delayMs;
    private String csvFile;
    private String jsonFile;
    @Getter
    private String progressFile; // ví dụ: data/sjc/_progress.json
}
