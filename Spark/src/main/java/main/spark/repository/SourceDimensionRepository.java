package main.spark.repository;

import main.spark.module.entity.SourceDimension;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;


@Repository
public interface SourceDimensionRepository extends JpaRepository<SourceDimension, Long> {
}
