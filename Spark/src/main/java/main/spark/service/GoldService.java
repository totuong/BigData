package main.spark.service;

import lombok.RequiredArgsConstructor;
import main.spark.repository.GoldPriceFactRepository;
import org.jvnet.hk2.annotations.Service;

@Service
@RequiredArgsConstructor
public class GoldService {
    private final GoldPriceFactRepository goldPriceFactRepository;

}
