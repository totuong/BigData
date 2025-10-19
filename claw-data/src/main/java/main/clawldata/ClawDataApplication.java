package main.clawldata;

import lombok.RequiredArgsConstructor;
import main.clawldata.worker.SjcCrawlerJob;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor
public class ClawDataApplication implements CommandLineRunner {

    private final SjcCrawlerJob job;

    public static void main(String[] args) {
        SpringApplication.run(ClawDataApplication.class, args);
    }

    @Override
    public void run(String... args) {
        job.runOnce(); // chạy một mạch 1 năm theo cấu hình
    }
}
