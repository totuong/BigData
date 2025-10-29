package main.spark.service;

import lombok.extern.slf4j.Slf4j;
import main.spark.module.entity.GoldTypeDimension;
import main.spark.repository.GoldTypeDimensionRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class GoldTypeService {

    private final GoldTypeDimensionRepository goldTypeDimensionRepository;

    public GoldTypeService(GoldTypeDimensionRepository goldTypeDimensionRepository) {
        this.goldTypeDimensionRepository = goldTypeDimensionRepository;
    }

    private static final Map<String, String> KARAT_TO_PURITY = Map.ofEntries(
            Map.entry("24k", "99.99"),
            Map.entry("23k", "95.8"),
            Map.entry("22k", "91.6"),
            Map.entry("21k", "87.5"),
            Map.entry("18k", "75.0"),
            Map.entry("16k", "68.0"),
            Map.entry("14k", "58.3"),
            Map.entry("12k", "50.0"),
            Map.entry("9k", "37.5"),
            Map.entry("8k", "33.3"),
            Map.entry("10k", "41.66"),
            Map.entry("kg", "99.99"),
            Map.entry("10l", "99.99"),
            Map.entry("1l", "99.99")
    );

    /**
     * 🔹 Nhận diện độ tinh khiết (purity)
     */
    public String extractPurity(String typeName) {
        if (typeName == null) return "Unknown";
        String name = typeName.toLowerCase();

        // Ưu tiên phát hiện carat
        for (var entry : KARAT_TO_PURITY.entrySet()) {
            if (name.contains(entry.getKey())) return entry.getValue();
        }

        // Phát hiện theo ký hiệu phần trăm hoặc chuỗi
        if (name.contains("9999") || name.contains("99,99") || name.contains("99.99") || name.contains("999.9"))
            return "99.99";
        if (name.contains("999") || name.contains("99,9") || name.contains("99.9")) return "99.9";
        if (name.contains("992")) return "99.2";
        if (name.contains("99")) return "99.0";
        if (name.contains("75")) return "75.0";
        if (name.contains("68")) return "68.0";
        if (name.contains("650")) return "65.0";
        if (name.contains("61")) return "61.0";
        if (name.contains("58")) return "58.3";
        if (name.contains("585")) return "58.5";
        if (name.contains("50")) return "50.0";
        if (name.contains("416")) return "41.66";
        if (name.contains("41")) return "41.7";

        return "Unknown";
    }

    /**
     * 🔹 Nhận diện loại sản phẩm (category)
     */
    public String extractCategory(String typeName) {
        if (typeName == null) return "toher";
        String name = typeName.toLowerCase();

        if (name.contains("sjc") && (name.contains("1l") || name.contains("10l") || name.contains("kg")))
            return "gold_bar";
        if (name.contains("nhẫn") || name.contains("chỉ"))
            return "ring";
        if (name.contains("nữ trang") || name.contains("trang sức") || name.contains("pnj") || name.contains("doji"))
            return "jewelry";
        return "other";
    }

    /**
     * 🔹 Chuẩn hoá & lưu hoặc lấy từ DB
     */
    public GoldTypeDimension getOrCreateGoldType(String typeName) {
        String purity = extractPurity(typeName);
        String category = extractCategory(typeName);

        List<GoldTypeDimension> goldTypeDimensions = goldTypeDimensionRepository.findByTypeName(typeName);
        GoldTypeDimension goldTypeDimension;
        if (goldTypeDimensions.isEmpty()) {
            goldTypeDimension = new GoldTypeDimension();
            goldTypeDimension.setTypeName(typeName);
            log.info("🆕 Tạo mới GoldTypeDimension: {} [{} - {}]", typeName, category, purity);
        } else {
            goldTypeDimension = goldTypeDimensions.get(0);
        }
        goldTypeDimension.setPurity(purity);
        goldTypeDimension.setCategory(category);
        goldTypeDimensionRepository.save(goldTypeDimension);
        return goldTypeDimension;
    }
}
