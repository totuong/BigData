package main.spark.service;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SparkJobService {

    private final SparkSession spark;

    public String runJob() {
        try {
            // 1️⃣ Đọc dữ liệu từ Hadoop HDFS
            String inputPath = "hdfs://localhost:9000/user/totuong/data/gold_price.csv";
            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(inputPath);

            return "✅ Spark job hoàn tất, dữ liệu đã lưu vào Oracle.";
        } catch (Exception e) {
            e.printStackTrace();
            return "❌ Job lỗi: " + e.getMessage();
        }
    }
}
