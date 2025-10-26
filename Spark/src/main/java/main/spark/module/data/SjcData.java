package main.spark.module.data;

import lombok.Data;

@Data
public class SjcData {
    private String date;
    private String type;
    private Double buy;
    private Double sell;
    private Payload payload;

    @Data
    public static class Payload {
        private Long Id;
        private String TypeName;
        private String BranchName;
        private String Buy;
        private Double BuyValue;
        private String Sell;
        private Double SellValue;
        private String BuyDiffer;
        private Double BuyDifferValue;
        private String SellDiffer;
        private Double SellDifferValue;
        private String GroupDate;
    }

}

