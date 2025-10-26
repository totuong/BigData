package main.spark.module.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

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
}
