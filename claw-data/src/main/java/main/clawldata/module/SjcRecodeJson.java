package main.clawldata.module;

import lombok.Data;

import java.util.Map;

@Data
public class SjcRecodeJson {
    private String date;
    private String type;

    private String buy;
    private String sell;
    private Map<String, Object> payload;
}
