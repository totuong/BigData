package main.spark.repository;


import main.spark.module.entity.GoldPriceFact;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface GoldPriceFactRepository extends JpaRepository<GoldPriceFact, Long> {

}
