package main.spark.module.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Data
@Entity
@Table(name = "LOCATION_DIMENSION")
public class LocationDimension {
    @Id
    private Long id;
    private String city;
    private String region;
}
