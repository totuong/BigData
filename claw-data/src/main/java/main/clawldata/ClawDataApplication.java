package main.clawldata;

import lombok.RequiredArgsConstructor;
import main.clawldata.worker.SjcCrawlerJob;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@RequiredArgsConstructor
@EnableScheduling
public class ClawDataApplication{

    public static void main(String[] args) {
        SpringApplication.run(ClawDataApplication.class, args);
    }

}
