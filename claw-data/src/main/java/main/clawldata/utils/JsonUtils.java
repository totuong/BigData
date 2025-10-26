package main.clawldata.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.file.*;
import java.util.List;

public class JsonUtils {
    private static final ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    // Ghi append vào file JSON (dạng mảng)
    public static synchronized Path appendToSingle(Path file, List<?> rows) throws Exception {
        if (rows == null || rows.isEmpty()) return file;

        // Nếu file chưa tồn tại -> tạo mảng mới
        if (!Files.exists(file)) {
            mapper.writeValue(file.toFile(), rows);
            return file;
        }

        // Nếu đã có file -> đọc toàn bộ, append thêm rồi ghi lại
        List<Object> existing = mapper.readValue(file.toFile(), List.class);
        existing.addAll(rows);
        mapper.writeValue(file.toFile(), existing);

        return file;
    }
}
