package main.spark.module.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Data
@Entity
@Table(name = "GOLD_TPE_DIMENSION")
public class GoldTypeDimension {
    @Id
    private Long id;
    private String typeName;
    private String purity;
    private String category;
}
