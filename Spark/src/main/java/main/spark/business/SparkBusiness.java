package main.spark.business;

import lombok.RequiredArgsConstructor;
import main.spark.service.GoldTypeService;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SparkBusiness {
    private final GoldTypeService goldTypeService;

}
