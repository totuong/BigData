package main.spark.config;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class SparkConfig {
    private final SparkProps props;

    @Bean
    public SparkSession sparkSession() {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        System.setProperty("HADOOP_HOME", "C:\\hadoop");

        return SparkSession.builder()
                .appName("HDFS to Oracle Job")
                .master(props.getMaster())
                .config("spark.hadoop.fs.defaultFS", props.getHdfsUri())
                .config("spark.ui.enabled", "false")           // 🚫 Tắt UI
                .config("spark.ui.showConsoleProgress", "false") // 🚫 Không hiển thị progress bar (cũng dùng servlet)
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse") // để tránh lỗi permission
                .getOrCreate();
    }
}
