package main.spark.module.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Data
@Entity
@Table(name = "SOURCE_DIMENSION")
public class SourceDimension {
    @Id
    private Long id;
    private String sourceName;
    private String sourceUrl;
    private String description;
}
