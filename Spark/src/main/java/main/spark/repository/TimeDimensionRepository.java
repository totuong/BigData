package main.spark.repository;

import main.spark.module.entity.TimeDimension;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TimeDimensionRepository extends JpaRepository<TimeDimension, Long> {
}
