package main.spark.repository;

import main.spark.module.entity.SourceDimension;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;


@Repository
public interface SourceDimensionRepository extends JpaRepository<SourceDimension, Long> {
    Optional<SourceDimension> findBySourceName(String sourceName);
    Optional<SourceDimension> findBySourceUrl(String sourceName);


}
