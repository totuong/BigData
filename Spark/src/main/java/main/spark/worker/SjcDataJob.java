package main.spark.worker;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

@Slf4j
@Component
public class SjcDataJob {

    @PostConstruct
    public void sync() {
        log.info("🚀 Starting SjcDataJob to read JSON from HDFS and save to DB...");

        // 1️⃣ Khởi tạo SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SjcDataToDatabase")
                .master("local[*]")
                .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.38.88:9000")
                .getOrCreate();

        String hdfsPath = "hdfs://192.168.38.88:9000/user/totuong/data/sjc_prices.json";

        // 2️⃣ Đọc JSON (multiline vì file là mảng JSON)
        Dataset<Row> df = spark.read()
                .option("multiline", "true")
                .json(hdfsPath);

        log.info("✅ Schema ban đầu:");
        df.printSchema();

        Dataset<Row> cleaned = df
                .withColumn("buy_value",
                        regexp_replace(col("buy"), ",", "").cast("double"))
                .withColumn("sell_value",
                        regexp_replace(col("sell"), ",", "").cast("double"))
                .withColumn("branch_name", col("payload.BranchName"))
                .withColumn("type_name", col("payload.TypeName"))
                .select("date", "type", "buy_value", "sell_value", "branch_name", "type_name");

        log.info("✅ Dữ liệu sau khi làm sạch:");
        cleaned.show(10, false);

        // 4️⃣ Cấu hình kết nối JDBC
        String jdbcUrl = "jdbc:postgresql://192.168.38.10:5432/bigdata";

        log.info("✅ Đã ghi dữ liệu vào bảng 'sjc_prices' thành công!");

        spark.stop();
    }

}
