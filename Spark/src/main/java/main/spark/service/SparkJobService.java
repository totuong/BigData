package main.spark.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import main.spark.module.entity.*;
import main.spark.repository.GoldPriceFactRepository;
import main.spark.repository.LocationDimensionRepository;
import main.spark.repository.SourceDimensionRepository;
import main.spark.repository.TimeDimensionRepository;
import main.spark.util.LocationMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

@Service
@RequiredArgsConstructor
@Slf4j
public class SparkJobService {

    private final SparkSession spark;
    private final GoldPriceFactRepository factRepository;
    private final SourceDimensionRepository sourceDimensionRepository;
    //    private final GoldTypeDimensionRepository goldTypeDimensionRepository;
    private final LocationDimensionRepository locationDimensionRepository;
    private final TimeDimensionRepository timeDimensionRepository;
    private final GoldTypeService goldTypeService;

    private final String hdfsPath = "hdfs://192.168.38.88:9000/user/totuong/data/sjc_prices.json";
    private final String hdfsPathPnj = "hdfs://192.168.38.88:9000/user/totuong/data/pnj_gold.json";
    final DateTimeFormatter DF = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final int BATCH_SIZE = 500;

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
                .select("date", "type", "buy_value", "sell_value", "branch_name", "type_name");

        long totalRows = cleaned.count();
        log.info("✅ Đã làm sạch dữ liệu, tổng số dòng: {}", totalRows);

        // 3️⃣ Duyệt tuần tự từng dòng và lưu batch
        Iterator<Row> iter = cleaned.toLocalIterator();
        List<GoldPriceFact> buffer = new ArrayList<>();
        AtomicInteger counter = new AtomicInteger();

        while (iter.hasNext()) {
            Row row = iter.next();
            GoldPriceFact fact = convertRowToEntity(row);
            if (fact != null) {
                buffer.add(fact);
                counter.incrementAndGet();
            }

            if (buffer.size() >= BATCH_SIZE) {
                saveBatch(buffer, counter.get());
            }
        }

        // 4️⃣ Lưu nốt phần dư
        if (!buffer.isEmpty()) {
            saveBatch(buffer, counter.get());
        }

        log.info("💾 Hoàn tất đồng bộ, tổng cộng lưu {} bản ghi.", counter.get());
    }
    private void saveBatch(List<GoldPriceFact> buffer, int counter) {
        try {
            factRepository.saveAll(buffer);
            factRepository.flush();
            log.info("✅ Đã lưu {} bản ghi (tổng cộng).", counter);
        } catch (Exception e) {
            log.error("❌ Lỗi khi lưu batch: {}", e.getMessage());
        } finally {
            buffer.clear();
        }
    }
    private GoldPriceFact convertRowToEntity(Row row) {
        try {
            GoldPriceFact fact = new GoldPriceFact();

            Double buyValue = row.getAs("buy_value");
            Double sellValue = row.getAs("sell_value");
            fact.setBuyPrice(buyValue * 1000);
            fact.setSellPrice(sellValue * 1000);
            fact.setUnit("VNĐ/Lượng");

            // 🔹 SourceDimension
            SourceDimension src = sourceDimensionRepository.findBySourceName("SJC")
                    .orElseGet(() -> {
                        SourceDimension s = new SourceDimension();
                        s.setSourceName("SJC");
                        s.setSourceUrl("https://sjc.com.vn");
                        s.setDescription("Giá vàng SJC chính thức");
                        return s;
                    });
            src.setSourceUrl("https://sjc.com.vn");
            src.setDescription("Giá vàng SJC chính thức");
            sourceDimensionRepository.save(src);
            fact.setSourceDimension(src);

            // 🔹 GoldTypeDimension
            String typeName = row.getAs("type_name");
            GoldTypeDimension type = goldTypeService.getOrCreateGoldType(typeName);
            fact.setGoldTypeDimension(type);

            // 🔹 LocationDimension
            String branch = row.getAs("branch_name");
            if (branch == null) branch = "";

            branch = branch.trim()
                    .replaceAll("\\s+", " ")
                    .toLowerCase();

            String city = "Unknown";
            String region = "Unknown";

// Tìm trong map các tỉnh/thành
            for (Map.Entry<String, String> e : LocationMap.regionMap.entrySet()) {
                if (branch.contains(e.getKey())) {
                    city = LocationMap.capitalizeWords(e.getKey());
                    region = e.getValue();
                    break;
                }
            }


            if (branch.contains("bắc")) region = "Miền Bắc";
            else if (branch.contains("trung")) region = "Miền Trung";
            else if (branch.contains("nam")) region = "Miền Nam";

            final String finalCity = city;
            final String finalRegion = region;

            Optional<LocationDimension> existingLoc;
            if (finalCity != null && finalRegion != null) {
                existingLoc = locationDimensionRepository.findByCityIsAndRegionIs(finalCity, finalRegion);
            } else if (!Objects.equals(finalCity, "Unknown")) {
                existingLoc = locationDimensionRepository.findByCityIs(finalCity);
            } else if (!Objects.equals(finalRegion, "Unknown")) {
                existingLoc = locationDimensionRepository.findByRegionIs(finalRegion);
            } else {
                existingLoc = Optional.empty();
            }

            LocationDimension loc = existingLoc.orElseGet(LocationDimension::new);
            loc.setCity(finalCity);
            loc.setRegion(finalRegion);
            locationDimensionRepository.save(loc);
            fact.setLocationDimension(loc);

            // 🔹 TimeDimension
            String dateStr = row.getAs("date");         // ví dụ "27/10/2025"
            if (dateStr == null || dateStr.isBlank()) {
                log.warn("⚠️ Bỏ qua bản ghi vì thiếu field 'date'");
                return null;
            }

            LocalDate d = LocalDate.parse(dateStr, DF);
            Integer day = d.getDayOfMonth();
            Integer month = d.getMonthValue();
            Integer year = d.getYear();


            TimeDimension time = timeDimensionRepository.findByDayIsAndMonthIsAndYearIsAndHourIs(day, month, year, 12)
                    .orElseGet(() -> {
                        TimeDimension t = new TimeDimension();
                        t.setDateTime(d);
                        t.setDay(day);
                        t.setMonth(month);
                        t.setYear(year);
                        t.setHour(12);
                        return timeDimensionRepository.save(t);
                    });

            fact.setTimeDimension(time);

            return fact;

        } catch (Exception e) {
            log.warn("⚠️ Lỗi parse record: {}", e.getMessage());
            return null;
        }
    }


    public void syncNestedPrices() {
        log.info("🚀 Đọc JSON nested: {}", hdfsPath);

        // 1) Đọc file: là 1 mảng JSON duy nhất -> cần multiline=true
        Dataset<Row> raw = spark.read()
                .option("multiline", "true")
                .json(hdfsPath);

        raw.printSchema();

        // 2) Bung các tầng: locations[] -> gold_type[] -> data[]
        Dataset<Row> flat = raw
                .withColumn("date_raw", col("date")) // "yyyyMMdd"
                .withColumn("location", org.apache.spark.sql.functions.explode(col("data.locations")))
                .withColumn("gold", org.apache.spark.sql.functions.explode(col("location.gold_type")))
                .withColumn("point", org.apache.spark.sql.functions.explode(col("gold.data")))
                .select(
                        col("date_raw"),
                        col("location.name").alias("branch_name"),
                        col("gold.name").alias("type_name"),
                        col("point.gia_mua").alias("buy_raw"),
                        col("point.gia_ban").alias("sell_raw"),
                        col("point.updated_at").alias("updated_at") // "dd/MM/yyyy HH:mm:ss"
                );

        long total = flat.count();
        log.info("✅ Flatten xong, tổng dòng: {}", total);

        // 3) Duyệt streaming + lưu theo batch
        final int BATCH = 500;
        Iterator<Row> iter = flat.toLocalIterator();
        List<GoldPriceFact> buffer = new java.util.ArrayList<>(BATCH);
        int counter = 0;

        while (iter.hasNext()) {
            Row r = iter.next();
            GoldPriceFact fact = convertNestedRowToEntity(r);
            if (fact != null) {
                buffer.add(fact);
                counter++;
            }
            if (buffer.size() >= BATCH) {
                saveBatch(buffer, counter);
            }
        }
        if (!buffer.isEmpty()) {
            saveBatch(buffer, counter);
        }

        log.info("🏁 Hoàn tất syncNestedPrices, đã lưu {} bản ghi.", counter);
    }
    private GoldPriceFact convertNestedRowToEntity(Row row) {
        try {
            String buyStr  = row.getAs("buy_raw");   // ví dụ "66.100" hoặc "52.650"
            String sellStr = row.getAs("sell_raw");
            String branch  = row.getAs("branch_name"); // "TPHCM", "Miền Tây", "Hà Nội", ...
            String type    = row.getAs("type_name");   // "PNJ", "SJC", ...
            String dateRaw = row.getAs("date_raw");    // "yyyyMMdd" -> "20221001"
            String updated = row.getAs("updated_at");  // "dd/MM/yyyy HH:mm:ss"

            if (dateRaw == null || buyStr == null || sellStr == null) return null;

            // Chuẩn hoá số: "66.400" hoặc "26,620" -> về đơn vị nghìn, rồi *1000 => VNĐ
            double buyVnd  = parsePriceVnd(buyStr);
            double sellVnd = parsePriceVnd(sellStr);

            GoldPriceFact fact = new GoldPriceFact();
            fact.setBuyPrice(buyVnd);
            fact.setSellPrice(sellVnd);
            fact.setUnit("VNĐ/Lượng");

            // SourceDimension: PNJ/SJC là loại vàng, còn nguồn bạn có thể đặt "Crawl PNJ/SJC"
            SourceDimension src = sourceDimensionRepository.findBySourceName("Crawl PNJ/SJC")
                    .orElseGet(() -> {
                        SourceDimension s = new SourceDimension();
                        s.setSourceName("Crawl PNJ/SJC");
                        s.setSourceUrl("N/A");
                        s.setDescription("Dữ liệu crawl PNJ/SJC theo khu vực/giờ");
                        return sourceDimensionRepository.save(s);
                    });
            fact.setSourceDimension(src);

            // GoldTypeDimension theo "type_name" (PNJ, SJC, …)
            GoldTypeDimension goldType = goldTypeService.getOrCreateGoldType(type);
            fact.setGoldTypeDimension(goldType);

            // LocationDimension: city/region từ branch
            String normBranch = java.util.Optional.ofNullable(branch).orElse("").trim().toLowerCase();
            String city = "Unknown";
            String region = "Unknown";

            for (Map.Entry<String, String> e : LocationMap.regionMap.entrySet()) {
                if (normBranch.contains(e.getKey().toLowerCase())) {
                    city = LocationMap.capitalizeWords(e.getKey());
                    region = e.getValue();
                    break;
                }
            }
            // Các nhãn vùng trực tiếp
            if (normBranch.contains("miền tây"))    { region = "Miền Tây";    city = "Unknown"; }
            if (normBranch.contains("tây nguyên"))  { region = "Tây Nguyên";  city = "Unknown"; }
            if (normBranch.contains("miền bắc"))    { region = "Miền Bắc";    city = "Unknown"; }
            if (normBranch.contains("miền trung"))  { region = "Miền Trung";  city = "Unknown"; }
            if (normBranch.contains("miền nam") || normBranch.contains("tphcm") || normBranch.contains("tp.hcm")) {
                if (city.equals("Unknown")) city = "TPHCM";
                region = "Miền Nam";
            }

            Optional<LocationDimension> existingLoc;
            if (!"Unknown".equals(city) && !"Unknown".equals(region)) {
                existingLoc = locationDimensionRepository.findByCityIsAndRegionIs(city, region);
            } else if (!"Unknown".equals(city)) {
                existingLoc = locationDimensionRepository.findByCityIs(city);
            } else if (!"Unknown".equals(region)) {
                existingLoc = locationDimensionRepository.findByRegionIs(region);
            } else {
                existingLoc = Optional.empty();
            }
            String finalCity = city;
            String finalRegion = region;
            LocationDimension loc = existingLoc.orElseGet(() -> {
                LocationDimension l = new LocationDimension();
                l.setCity(finalCity);
                l.setRegion(finalRegion);
                return locationDimensionRepository.save(l);
            });
            fact.setLocationDimension(loc);

            // TimeDimension: lấy ngày từ "yyyyMMdd", giờ từ updated_at nếu có, fallback 12h
            java.time.LocalDate d = java.time.LocalDate.parse(dateRaw, java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
            int hour = 12;
            if (updated != null && !updated.isBlank()) {
                try {
                    java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(
                            updated,
                            java.time.format.DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
                    );
                    hour = ldt.getHour();
                } catch (Exception ignore) {}
            }

            int finalHour = hour;
            TimeDimension time = timeDimensionRepository
                    .findByDayIsAndMonthIsAndYearIsAndHourIs(d.getDayOfMonth(), d.getMonthValue(), d.getYear(), hour)
                    .orElseGet(() -> {
                        TimeDimension t = new TimeDimension();
                        t.setDateTime(d);
                        t.setDay(d.getDayOfMonth());
                        t.setMonth(d.getMonthValue());
                        t.setYear(d.getYear());
                        t.setHour(finalHour);
                        return timeDimensionRepository.save(t);
                    });
            fact.setTimeDimension(time);

            return fact;
        } catch (Exception e) {
            log.warn("⚠️ convertNestedRowToEntity lỗi: {}", e.getMessage());
            return null;
        }
    }
    private long parsePriceVnd(String s) {
        if (s == null) return 0L;
        // bỏ dấu . , và khoảng trắng
        String digits = s.replace(".", "").replace(",", "").replace(" ", "");
        if (digits.isEmpty() || !digits.matches("\\d+")) return 0L;
        long thousands = Long.parseLong(digits); // ví dụ "66400" nghìn
        return thousands * 1000L;                // -> 66,400,000 VNĐ
    }
}
