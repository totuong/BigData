package main.spark.service;

import lombok.extern.slf4j.Slf4j;
import main.spark.module.entity.GoldTypeDimension;
import main.spark.repository.GoldTypeDimensionRepository;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class GoldTypeService {

    private final GoldTypeDimensionRepository goldTypeDimensionRepository;

    public GoldTypeService(GoldTypeDimensionRepository goldTypeDimensionRepository) {
        this.goldTypeDimensionRepository = goldTypeDimensionRepository;
    }

    private static final Map<String, String> KARAT_TO_PURITY = Map.ofEntries(
            Map.entry("24k", "9999"),
            Map.entry("23k", "958"),
            Map.entry("22k", "916"),
            Map.entry("21k", "875"),
            Map.entry("18k", "750"),
            Map.entry("16k", "680"),
            Map.entry("14k", "583"),
            Map.entry("12k", "500"),
            Map.entry("10k", "417")
    );

    /** 🔹 Nhận diện độ tinh khiết (purity) */
    public String extractPurity(String typeName) {
        if (typeName == null) return "9999";
        String name = typeName.toLowerCase();

        // Ưu tiên phát hiện carat
        for (var entry : KARAT_TO_PURITY.entrySet()) {
            if (name.contains(entry.getKey())) return entry.getValue();
        }

        // Phát hiện theo ký hiệu phần trăm hoặc chuỗi
        if (name.contains("9999") || name.contains("99,99") || name.contains("99.99")) return "9999";
        if (name.contains("99")) return "990";
        if (name.contains("75")) return "750";
        if (name.contains("68")) return "680";
        if (name.contains("61")) return "610";
        if (name.contains("58")) return "583";
        if (name.contains("50")) return "500";
        if (name.contains("41")) return "417";

        return "9999";
    }

    /** 🔹 Nhận diện loại sản phẩm (category) */
    public String extractCategory(String typeName) {
        if (typeName == null) return "Other";
        String name = typeName.toLowerCase();

        if (name.contains("sjc") && (name.contains("1l") || name.contains("10l") || name.contains("kg")))
            return "Gold Bar";
        if (name.contains("nhẫn") || name.contains("chỉ"))
            return "Gold Ring";
        if (name.contains("nữ trang") || name.contains("trang sức") || name.contains("pnj") || name.contains("doji"))
            return "Jewelry";
        return "Other";
    }

    /** 🔹 Chuẩn hoá & lưu hoặc lấy từ DB */
    public GoldTypeDimension getOrCreateGoldType(String typeName) {
        String purity = extractPurity(typeName);
        String category = extractCategory(typeName);

        return goldTypeDimensionRepository.findByTypeName(typeName)
                .orElseGet(() -> {
                    GoldTypeDimension t = new GoldTypeDimension();
                    t.setTypeName(typeName);
                    t.setPurity(purity);
                    t.setCategory(category);
                    log.info("🆕 Tạo mới GoldTypeDimension: {} [{} - {}]", typeName, category, purity);
                    return goldTypeDimensionRepository.save(t);
                });
    }
}
