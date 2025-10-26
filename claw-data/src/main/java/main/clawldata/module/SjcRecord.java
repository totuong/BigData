package main.clawldata.module;

import com.fasterxml.jackson.annotation.JsonRawValue;
import com.opencsv.bean.CsvBindByName;
import lombok.*;

@Data @NoArgsConstructor @AllArgsConstructor @Builder
public class SjcRecord {
    @CsvBindByName(column = "date")
    private String date;

    @CsvBindByName(column = "type")
    private String type;

    @CsvBindByName(column = "buy")
    private String buy;

    @CsvBindByName(column = "sell")
    private String sell;

    @CsvBindByName(column = "payload")
    @JsonRawValue
    private String payload;
}
