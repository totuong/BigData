package main.spark.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spark.master}")
    private String master;

    @Value("${spark.hdfs-uri}")
    private String hdfsUri;

    @Bean
    public SparkSession sparkSession() {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        System.setProperty("HADOOP_HOME", "C:\\hadoop");

        return SparkSession.builder()
                .appName("HDFS to Oracle Job")
                .master(master)
                .config("spark.hadoop.fs.defaultFS", hdfsUri)
                .config("spark.ui.enabled", "false")           // 🚫 Tắt UI
                .config("spark.ui.showConsoleProgress", "false") // 🚫 Không hiển thị progress bar (cũng dùng servlet)
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse") // để tránh lỗi permission
                .getOrCreate();
    }
}
