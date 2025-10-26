package main.spark.module.entity;

import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "LOCATION_DIMENSION")
public class LocationDimension {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String city;
    private String region;
}
