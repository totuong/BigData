package main.spark.module.entity;

import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "GOLD_TYPE_DIMENSION")
public class GoldTypeDimension {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String typeName;
    private String purity;
    private String category;
}
