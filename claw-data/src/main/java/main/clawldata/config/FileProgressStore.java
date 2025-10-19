package main.clawldata.config;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.file.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Slf4j
@Component
public class FileProgressStore {

    private static final DateTimeFormatter DF = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private final ObjectMapper mapper = new ObjectMapper();

    // cho phép đổi qua application.yml (nếu muốn), còn mặc định để đây
    private final Path file = Paths.get("data/sjc/_progress.json");

    @Data
    public static class Progress {
        private String lastSuccessDate; // "dd/MM/yyyy"
    }

    /** Trả về ngày cuối đã crawl xong, hoặc null nếu chưa có. */
    public LocalDate loadLastSuccess() {
        try {
            if (!Files.exists(file)) return null;
            byte[] b = Files.readAllBytes(file);
            Progress p = mapper.readValue(b, Progress.class);
            if (p == null || p.getLastSuccessDate() == null || p.getLastSuccessDate().isBlank()) return null;
            return LocalDate.parse(p.getLastSuccessDate(), DF);
        } catch (Exception e) {
            log.warn("Cannot read progress {}: {}", file, e.toString());
            return null;
        }
    }

    /** Ghi ngày cuối đã crawl xong. */
    public void saveLastSuccess(LocalDate date) {
        try {
            Files.createDirectories(file.getParent());
            Progress p = new Progress();
            p.setLastSuccessDate(date.format(DF));
            byte[] b = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(p);
            Files.write(file, b, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception e) {
            log.warn("Cannot write progress {}: {}", file, e.toString());
        }
    }
}
