package main.spark.repository;

import main.spark.module.entity.GoldTypeDimension;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface GoldTypeDimensionRepository extends JpaRepository<GoldTypeDimension, Long> {
   Optional<GoldTypeDimension> findByTypeName(String s);
}
