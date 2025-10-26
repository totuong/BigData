package main.spark.module.entity;

import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "SOURCE_DIMENSION")
public class SourceDimension {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String sourceName;
    private String sourceUrl;
    private String description;
}
