package org.hackctl.order.orderlakehouse.ingestor.kafka.hdfs.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderLakeHouseIngestorKafkaToHdfsController {
    @GetMapping("/test")
    public String test() {
        return "Hello, World!";
    }
}