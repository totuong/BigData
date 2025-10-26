package main.spark.module.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "GOLD_PRICE_FACT")
public class GoldPriceFact {
    @Id
    private Long id;
    private Long sourceId;
    private Long typeId;
    private Long LocationId;
    private Long timeId;
    private Double buyPrice;
    private Double sellPrice;
    private Double priceChange;
    private String unit;
    private LocalDateTime recordedAt;

    @ManyToOne
    private SourceDimension sourceDimension;
    @ManyToOne
    private GoldTypeDimension goldTypeDimension;
    @ManyToOne
    private LocationDimension locationDimension;
    @ManyToOne
    private TimeDimension timeDimension;

}
