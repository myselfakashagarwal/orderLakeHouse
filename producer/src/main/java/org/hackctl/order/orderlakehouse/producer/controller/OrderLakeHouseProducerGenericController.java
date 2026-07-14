package org.hackctl.order.orderlakehouse.producer.controller;

import jakarta.validation.Valid;
import org.hackctl.order.orderlakehouse.producer.dto.OrderRequest;
import org.hackctl.order.orderlakehouse.producer.service.OrderLakeHouseProducerGenericService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderLakeHouseProducerGenericController {

    @Autowired
    private OrderLakeHouseProducerGenericService producerService;

    @GetMapping("/test")
    public String hello() {
        return "The producer is up";
    }

    @PostMapping("/api/v1/order")
    public ResponseEntity<String> placeOrder(
        @Valid @RequestBody OrderRequest request
    ) {
        producerService.sendOrder(request);

        return ResponseEntity.ok(
            String.format(
                "Order accepted and forwarded to Topic: %s (Business) with Key: %s (State)",
                request.getBusinessId(),
                request.getStateId()
            )
        );
    }
}
