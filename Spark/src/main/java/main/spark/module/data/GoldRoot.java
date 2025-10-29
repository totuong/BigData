package main.spark.module.data;

import lombok.Data;

import java.util.List;

@Data
public class GoldRoot {
    private String date;
    private GoldData data;
}

@Data
class GoldData {
    private List<Location> locations;
}

@Data
class Location {
    private String name;                   // "TPHCM", "Hà Nội", ...
    private List<GoldType> gold_type;      // Danh sách loại vàng
}

@Data
class GoldType {
    private String name;                   // "PNJ", "SJC", ...
    private List<GoldPoint> data;          // Dữ liệu từng lần cập nhật
}

@Data
class GoldPoint {
    private String gia_ban;
    private String gia_mua;
    private String updated_at;
}
