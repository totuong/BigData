package main.spark.repository;

import main.spark.module.entity.LocationDimension;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;


@Repository
public interface LocationDimensionRepository extends JpaRepository<LocationDimension, Long> {
    Optional<LocationDimension> findByCityIs(String branch);
    Optional<LocationDimension> findByRegionIs(String region);
    Optional<LocationDimension> findByCityIsAndRegionIs(String branch,String region);
}
