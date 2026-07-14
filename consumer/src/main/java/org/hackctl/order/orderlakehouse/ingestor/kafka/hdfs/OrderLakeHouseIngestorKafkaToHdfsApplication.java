package org.hackctl.order.orderlakehouse.ingestor.kafka.hdfs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderLakeHouseIngestorKafkaToHdfsApplication {

    public static void main(String[] args) {
        SpringApplication.run(
            OrderLakeHouseIngestorKafkaToHdfsApplication.class,
            args
        );
    }
}
