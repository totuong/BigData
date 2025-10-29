package main.spark.module.entity;

import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "GOLD_PRICE_FACT")
public class GoldPriceFact {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    //    private Long sourceId;
//    private Long typeId;
//    private Long locationId;
//    private Long timeId;
    private Double buyPrice;
    private Double sellPrice;
    private String unit;
    private Integer isDeleted = 0;
    @CreationTimestamp
    private LocalDateTime recordedAt;

    @ManyToOne
    @JoinColumn(name = "source_id")
    private SourceDimension sourceDimension;
    @ManyToOne
    @JoinColumn(name = "type_id")
    private GoldTypeDimension goldTypeDimension;
    @ManyToOne
    @JoinColumn(name = "location_id")
    private LocationDimension locationDimension;
    @ManyToOne
    @JoinColumn(name = "time_id")
    private TimeDimension timeDimension;

}
