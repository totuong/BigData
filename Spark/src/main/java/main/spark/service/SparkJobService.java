package main.spark.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import main.spark.module.entity.*;
import main.spark.repository.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

@Service
@RequiredArgsConstructor
@Slf4j
public class SparkJobService {

    private final SparkSession spark;
    private final GoldPriceFactRepository factRepository;
    private final SourceDimensionRepository sourceDimensionRepository;
    private final GoldTypeDimensionRepository goldTypeDimensionRepository;
    private final LocationDimensionRepository locationDimensionRepository;
    private final TimeDimensionRepository timeDimensionRepository;
    private final GoldTypeService goldTypeService;

    private final String hdfsPath = "hdfs://192.168.38.88:9000/user/totuong/data/sjc_prices.json";
    final DateTimeFormatter DF = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    public void syncSJC() {
        log.info("🚀 Bắt đầu đọc dữ liệu từ HDFS: {}", hdfsPath);

        // 1️⃣ Đọc file JSON
        Dataset<Row> df = spark.read()
                .option("multiline", "true")
                .json(hdfsPath);

        log.info("✅ Schema ban đầu:");
        df.printSchema();

        // 2️⃣ Làm sạch dữ liệu
        Dataset<Row> cleaned = df
                .withColumn("buy_value", regexp_replace(col("buy"), ",", "").cast("double"))
                .withColumn("sell_value", regexp_replace(col("sell"), ",", "").cast("double"))
                .withColumn("branch_name", col("payload.BranchName"))
                .withColumn("type_name", col("payload.TypeName"))
                .withColumn("type_name", col("payload.TypeName"))
                .select("date", "type", "buy_value", "sell_value", "branch_name", "type_name");

        log.info("✅ Đã làm sạch dữ liệu, tổng số dòng: {}", cleaned.count());

        // 3️⃣ Convert sang List<GoldPriceFact>
        List<Row> rows = cleaned.collectAsList();
        List<GoldPriceFact> facts = rows.stream().map(row -> {
            try {
                GoldPriceFact fact = new GoldPriceFact();
                fact.setBuyPrice(row.getAs("buy_value"));
                fact.setSellPrice(row.getAs("sell_value"));
                fact.setUnit("VND");

                // 🔹 SourceDimension
                SourceDimension src = sourceDimensionRepository.findBySourceName("SJC")
                        .orElseGet(() -> {
                            SourceDimension s = new SourceDimension();
                            s.setSourceName("SJC");
                            s.setSourceUrl("https://sjc.com.vn");
                            s.setDescription("Giá vàng SJC chính thức");
                            return sourceDimensionRepository.save(s);
                        });
                fact.setSourceDimension(src);

                // 🔹 GoldTypeDimension
                String typeName = row.getAs("type_name");
                GoldTypeDimension type = goldTypeService.getOrCreateGoldType(typeName);
                fact.setGoldTypeDimension(type);

                // 🔹 LocationDimension
                String branch = row.getAs("branch_name");
                LocationDimension loc = locationDimensionRepository.findByCity(branch)
                        .orElseGet(() -> {
                            LocationDimension l = new LocationDimension();
                            l.setCity(branch);
                            l.setRegion("VN");
                            return locationDimensionRepository.save(l);
                        });
                fact.setLocationDimension(loc);

                // 🔹 TimeDimension
                String dateStr = row.getAs("date");         // ví dụ "27/10/2025"
                if (dateStr == null || dateStr.isBlank()) {
                    log.warn("⚠️ Bỏ qua bản ghi vì thiếu field 'date'");
                    return null;
                }

// Parse LocalDate
                LocalDate d = LocalDate.parse(dateStr, DF);

// Tạo ID = yyyyMMdd
                long dateId = Long.parseLong(d.format(DateTimeFormatter.BASIC_ISO_DATE)); // 20251027

                TimeDimension time = timeDimensionRepository.findById(dateId)
                        .orElseGet(() -> {
                            TimeDimension t = new TimeDimension();
                            t.setId(dateId);
                            t.setDateTime(d);                         // ✅ Gán DATE để tránh NULL
                            t.setDay(dateStr);                    // "27/10/2025"
                            t.setMonth(String.format("%02d", d.getMonthValue()));
                            t.setYear(String.valueOf(d.getYear()));

                            int m = d.getMonthValue();
                            t.setQuarter(m <= 3 ? "Q1" : m <= 6 ? "Q2" : m <= 9 ? "Q3" : "Q4");

                            return timeDimensionRepository.save(t);
                        });

                fact.setTimeDimension(time);

                return fact;

            } catch (Exception e) {
                log.warn("⚠️ Lỗi parse record: {}", e.getMessage());
                return null;
            }
        }).filter(f -> f != null).collect(Collectors.toList());

        // 4️⃣ Lưu toàn bộ vào DB
        factRepository.saveAll(facts);
        log.info("💾 Đã lưu {} bản ghi vào bảng GOLD_PRICE_FACT", facts.size());
    }
}
