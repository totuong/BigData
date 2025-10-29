package main.spark.module.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDate;

@Data
@Entity
@Table(name = "TIME_DIMENSION")
public class TimeDimension {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private Integer day;
    private Integer month;
    private Integer year;
    private Integer hour;
    private LocalDate dateTime;
}
