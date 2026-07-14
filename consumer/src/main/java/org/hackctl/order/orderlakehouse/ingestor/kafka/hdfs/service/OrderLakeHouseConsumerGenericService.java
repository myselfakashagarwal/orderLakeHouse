package org.hackctl.order.orderlakehouse.ingestor.kafka.hdfs.service;

import lombok.extern.slf4j.Slf4j;
import org.hackctl.order.orderlakehouse.ingestor.kafka.hdfs.dto.OrderRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class OrderLakeHouseConsumerGenericService {

    @Autowired
    private HdfsService hdfsService;

    /**
     * Listens to all topics matching 9-digit pattern (businessId = topic name).
     * Key = stateId (set by producer for partition routing).
     */
    @KafkaListener(
        topicPattern = "\\d{9}",
        groupId = "orderLakeHouseIngestorGroup",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload OrderRequest order,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_KEY) String key
    ) {
        log.info("Received order | topic={} key={} businessId={} regionId={} stateId={}",
                topic, key,
                order.getBusinessId(),
                order.getRegionId(),
                order.getStateId());
        try {
            hdfsService.writeOrder(order);
        } catch (Exception e) {
            log.error("Failed to write order to HDFS | topic={} key={} error={}",
                    topic, key, e.getMessage(), e);
        }
    }
}