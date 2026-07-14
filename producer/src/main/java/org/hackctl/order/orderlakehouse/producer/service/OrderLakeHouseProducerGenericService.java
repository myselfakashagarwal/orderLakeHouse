package org.hackctl.order.orderlakehouse.producer.service;

import lombok.extern.slf4j.Slf4j;
import org.hackctl.order.orderlakehouse.producer.dto.OrderRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class OrderLakeHouseProducerGenericService {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void sendOrder(OrderRequest orderRequest) {
        String topic = orderRequest.getBusinessId();

        String key = orderRequest.getStateId();

        log.info(
            "Sending order to dynamic topic (businessId): {} with key (stateId): {}",
            topic,
            key
        );

        kafkaTemplate.send(topic, key, orderRequest);
    }
}
