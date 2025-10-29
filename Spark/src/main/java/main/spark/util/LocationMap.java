package main.spark.util;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LocationMap {
   public static Map<String, String> regionMap = new LinkedHashMap<>() {{
       // --- Miền Bắc ---
       put("hà nội", "Miền Bắc");
       put("hn", "Miền Bắc");
       put("hải phòng", "Miền Bắc");
       put("quảng ninh", "Miền Bắc");
       put("bắc giang", "Miền Bắc");
       put("bắc ninh", "Miền Bắc");
       put("bắc kạn", "Miền Bắc");
       put("cao bằng", "Miền Bắc");
       put("lạng sơn", "Miền Bắc");
       put("thái nguyên", "Miền Bắc");
       put("tuyên quang", "Miền Bắc");
       put("phú thọ", "Miền Bắc");
       put("vĩnh phúc", "Miền Bắc");
       put("hòa bình", "Miền Bắc");
       put("hà nam", "Miền Bắc");
       put("nam định", "Miền Bắc");
       put("ninh bình", "Miền Bắc");
       put("thái bình", "Miền Bắc");
       put("hà giang", "Miền Bắc");
       put("lai châu", "Miền Bắc");
       put("điện biên", "Miền Bắc");
       put("sơn la", "Miền Bắc");
       put("lào cai", "Miền Bắc");
       put("yên bái", "Miền Bắc");

       // --- Miền Trung ---
       put("đà nẵng", "Miền Trung");
       put("dn", "Miền Trung");
       put("quảng bình", "Miền Trung");
       put("quảng trị", "Miền Trung");
       put("thừa thiên huế", "Miền Trung");
       put("huế", "Miền Trung");
       put("nghệ an", "Miền Trung");
       put("hà tĩnh", "Miền Trung");
       put("quảng nam", "Miền Trung");
       put("quảng ngãi", "Miền Trung");
       put("bình định", "Miền Trung");
       put("phú yên", "Miền Trung");
       put("khánh hòa", "Miền Trung");
       put("nha trang", "Miền Trung");
       put("ninh thuận", "Miền Trung");
       put("bình thuận", "Miền Trung");
       put("kon tum", "Miền Trung");
       put("gia lai", "Miền Trung");
       put("đắk lắk", "Miền Trung");
       put("đắk nông", "Miền Trung");
       put("lâm đồng", "Miền Trung");

       // --- Miền Nam ---
       put("hồ chí minh", "Miền Nam");
       put("hcm", "Miền Nam");
       put("sài gòn", "Miền Nam");
       put("bình dương", "Miền Nam");
       put("bình phước", "Miền Nam");
       put("đồng nai", "Miền Nam");
       put("tây ninh", "Miền Nam");
       put("bà rịa vũng tàu", "Miền Nam");
       put("vũng tàu", "Miền Nam");
       put("cần thơ", "Miền Nam");
       put("an giang", "Miền Nam");
       put("kiên giang", "Miền Nam");
       put("đồng tháp", "Miền Nam");
       put("vĩnh long", "Miền Nam");
       put("trà vinh", "Miền Nam");
       put("bến tre", "Miền Nam");
       put("long an", "Miền Nam");
       put("hậu giang", "Miền Nam");
       put("sóc trăng", "Miền Nam");
       put("bạc liêu", "Miền Nam");
       put("cà mau", "Miền Nam");
   }};

    public static String capitalizeWords(String input) {
        if (input == null || input.isEmpty()) return input;
        return Arrays.stream(input.split("\\s+"))
                .map(w -> w.substring(0, 1).toUpperCase() + w.substring(1))
                .collect(Collectors.joining(" "));
    }
}
