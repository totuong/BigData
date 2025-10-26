package main.spark.module.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDate;

@Data
@Entity
@Table(name = "TIME_DIMENSION")
public class TimeDimension {
    @Id
    private Long id;
    private String day;
    private String month;
    private String quarter;
    private String year;
    private LocalDate dateTime;
}
