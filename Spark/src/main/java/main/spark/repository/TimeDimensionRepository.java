package main.spark.repository;

import main.spark.module.entity.TimeDimension;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface TimeDimensionRepository extends JpaRepository<TimeDimension, Long> {
    Optional<TimeDimension> findByDay(Integer dateStr);
    Optional<TimeDimension> findByDayIsAndMonthIsAndYearIsAndHourIs(Integer day,Integer month,Integer year,Integer hour);
}
